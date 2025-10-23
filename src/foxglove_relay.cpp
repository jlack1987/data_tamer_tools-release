#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <rcl_interfaces/msg/log.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp/qos.hpp>
#include <rclcpp/serialization.hpp>
#include <rclcpp/serialized_message.hpp>

#include <data_tamer_msgs/msg/schema.hpp>
#include <data_tamer_msgs/msg/schemas.hpp>
#include <data_tamer_msgs/msg/snapshot.hpp>
#include <data_tamer_parser/data_tamer_parser.hpp>
#include <data_tamer_tools/helpers.hpp>

#include <foxglove/foxglove.hpp>
#include <foxglove/server.hpp>

#include <cstdlib>
#include <unordered_map>
#include <string>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <chrono>
#include <nlohmann/json.hpp>

namespace data_tamer_tools
{

static foxglove::schemas::Log::LogLevel toFoxgloveLevel(uint8_t ros_level)
{
    using FG = foxglove::schemas::Log::LogLevel;
    // ROS: 10=DEBUG,20=INFO,30=WARN,40=ERROR,50=FATAL
    if (ros_level >= 50)
    {
        return FG::FATAL;
    }
    if (ros_level >= 40)
    {
        return FG::ERROR;
    }
    if (ros_level >= 30)
    {
        return FG::WARNING;
    }
    if (ros_level >= 20)
    {
        return FG::INFO;
    }
    if (ros_level >= 10)
    {
        return FG::DEBUG;
    }
    return FG::UNKNOWN;
}

struct ChannelInfo
{
    std::string topic;                              // ROS2 topic (for reference)
    std::string json_schema;                        // JSON Schema text
    DataTamerParser::Schema parsed_schema;          // Parsed DataTamer schema
    std::shared_ptr<foxglove::RawChannel> channel;  // Foxglove channel (json)
    rclcpp::Time last_seen{ 0, 0 };
    ProtoRuntime proto_runtime;

    std::mutex pb_mutex;
    std::unique_ptr<google::protobuf::Message> m;
    std::unordered_map<std::string, const google::protobuf::FieldDescriptor*> field_by_name;
};

// Hash (uint64) -> ChannelInfo
class SchemaRegistry
{
  public:
    // Update last_seen for a specific hash
    void touch(uint64_t hash, const rclcpp::Time& t)
    {
        std::scoped_lock lk(m_);
        std::unordered_map<uint64_t, std::shared_ptr<ChannelInfo>>::iterator it = map_.find(hash);
        if (it != map_.end() && it->second)
        {
            it->second->last_seen = t;
        }
    }

    std::shared_ptr<ChannelInfo> get_mutable(uint64_t hash)
    {
        std::scoped_lock lk(m_);
        auto it = map_.find(hash);
        return (it == map_.end()) ? nullptr : it->second;
    }

    // Close channels that are idle past TTL, but DO NOT erase the entry.
    // Returns number of channels closed.
    size_t soft_sweep_idle_channels(const rclcpp::Time& now, const rclcpp::Duration& ttl)
    {
        std::scoped_lock lk(m_);
        size_t closed = 0;
        for (auto& [hash, info] : map_)
        {
            if (!info)
                continue;
            if (info->last_seen.nanoseconds() == 0)
                continue;  // never saw data
            const rclcpp::Duration age = now - info->last_seen;
            if (age > ttl)
            {
                if (info->channel)
                {
                    info->channel.reset();  // close Foxglove channel; keep schema & topic
                    ++closed;
                }
            }
        }
        return closed;
    }

    size_t size() const
    {
        std::scoped_lock lk(m_);
        return map_.size();
    }

    bool has(uint64_t hash) const
    {
        std::scoped_lock lk(m_);
        return map_.count(hash) != 0;
    }

    bool try_emplace(uint64_t h, std::shared_ptr<ChannelInfo> info)
    {
        std::scoped_lock lk(m_);
        std::pair<std::unordered_map<uint64_t, std::shared_ptr<ChannelInfo>>::iterator, bool> result = map_.emplace(h, std::move(info));
        return result.second;
    }

    void emplace(uint64_t hash, std::shared_ptr<ChannelInfo> info)
    {
        std::scoped_lock lk(m_);
        map_.emplace(hash, std::move(info));
    }

    std::shared_ptr<const ChannelInfo> get(uint64_t hash) const
    {
        std::scoped_lock lk(m_);
        std::unordered_map<uint64_t, std::shared_ptr<ChannelInfo>>::const_iterator it = map_.find(hash);
        return (it == map_.end()) ? nullptr : it->second;
    }

  private:
    mutable std::mutex m_;
    std::unordered_map<uint64_t, std::shared_ptr<ChannelInfo>> map_;
};

// --- DataTamer -> JSON helpers ----------------------------------------------

// --- Bridge node -------------------------------------------------------------

class DtRos2ToFoxgloveBridge : public rclcpp::Node
{
  public:
    DtRos2ToFoxgloveBridge(const rclcpp::NodeOptions& o) : rclcpp::Node("foxglove_relay", o)
    {
        using std::chrono::milliseconds;

        // Foxglove WebSocket server
        foxglove::WebSocketServerOptions opts;
        opts.host = declare_parameter<std::string>("host", "127.0.0.1");

        rcl_interfaces::msg::ParameterDescriptor port_description;
        port_description.type = rclcpp::ParameterType::PARAMETER_INTEGER;
        port_description.name = "port";
        port_description.description = "Foxglove web socket port";
        port_description.integer_range.resize(1);
        port_description.integer_range[0].from_value = 0;
        port_description.integer_range[0].to_value = 65535;
        port_description.integer_range[0].step = 1;
        port_description.read_only = true;
        opts.port = declare_parameter(port_description.name, 8765, port_description);

        foxglove::FoxgloveResult<foxglove::WebSocketServer> serverResult = foxglove::WebSocketServer::create(std::move(opts));
        if (!serverResult.has_value())
        {
            RCLCPP_FATAL(get_logger(), "Foxglove server error: %s", foxglove::strerror(serverResult.error()));
            throw std::runtime_error("failed to create foxglove server");
        }
        server_.emplace(std::move(serverResult.value()));
        RCLCPP_INFO(get_logger(), "Foxglove WebSocket up at ws://%s:%u", opts.host.c_str(), server_->port());

        rcl_interfaces::msg::ParameterDescriptor eviction_ttl_description;
        eviction_ttl_description.type = rclcpp::ParameterType::PARAMETER_INTEGER;
        eviction_ttl_description.name = "eviction_ttl_sec";
        eviction_ttl_description.description = "Time to live for stale publishers";
        eviction_ttl_description.integer_range.resize(1);
        eviction_ttl_description.integer_range[0].from_value = 1;
        eviction_ttl_description.integer_range[0].to_value = 86400;  // 24 hours in seconds
        eviction_ttl_description.integer_range[0].step = 1;
        eviction_ttl_description.read_only = true;
        eviction_ttl_ = rclcpp::Duration::from_seconds(declare_parameter<int>("eviction_ttl_sec", 900, eviction_ttl_description));  // 15 min defaultefault

        rcl_interfaces::msg::ParameterDescriptor eviction_period_description;
        eviction_period_description.type = rclcpp::ParameterType::PARAMETER_INTEGER;
        eviction_period_description.name = "eviction_period_sec";
        eviction_period_description.description = "Check for stale publishers every N seconds";
        eviction_period_description.integer_range.resize(1);
        eviction_period_description.integer_range[0].from_value = 1;
        eviction_period_description.integer_range[0].to_value = 86400;  // 24 hours in seconds
        eviction_period_description.integer_range[0].step = 1;
        eviction_period_description.read_only = true;
        // Eviction timer
        int eviction_period_sec = declare_parameter<int>("eviction_period_sec", 30, eviction_period_description);
        eviction_timer_ = create_wall_timer(std::chrono::seconds(eviction_period_sec), std::bind(&DtRos2ToFoxgloveBridge::evictStalePublishers, this));

        rcl_interfaces::msg::ParameterDescriptor discovery_period_description;
        discovery_period_description.type = rclcpp::ParameterType::PARAMETER_INTEGER;
        discovery_period_description.name = "discovery_sec";
        discovery_period_description.description = "Period for (re)discovering all Snapshot topics";
        discovery_period_description.integer_range.resize(1);
        discovery_period_description.integer_range[0].from_value = 1;
        discovery_period_description.integer_range[0].to_value = 60;  // 1 minute in seconds
        discovery_period_description.integer_range[0].step = 1;
        discovery_period_description.read_only = true;
        // Periodically (re)discover all Snapshot topics and subscribe dynamically.
        int discovery_sec = declare_parameter<int>("discovery_sec", 5, discovery_period_description);
        discovery_timer_ = create_wall_timer(std::chrono::seconds(discovery_sec), std::bind(&DtRos2ToFoxgloveBridge::discoverSnapshotTopics, this));

        // --- ROSOUT options ---
        rcl_interfaces::msg::ParameterDescriptor rosout_enable_desc;
        rosout_enable_desc.type = rclcpp::ParameterType::PARAMETER_BOOL;
        rosout_enable_desc.name = "enable_rosout";
        rosout_enable_desc.description = "Relay /rosout to Foxglove using foxglove.schemas.Log";
        rosout_enable_desc.read_only = true;
        const bool enable_rosout = declare_parameter<bool>("enable_rosout", true, rosout_enable_desc);

        rcl_interfaces::msg::ParameterDescriptor rosout_topic_desc;
        rosout_topic_desc.type = rclcpp::ParameterType::PARAMETER_STRING;
        rosout_topic_desc.name = "rosout_topic";
        rosout_topic_desc.description = "ROS topic to subscribe for logs";
        rosout_topic_desc.read_only = true;
        const std::string rosout_topic = declare_parameter<std::string>("rosout_topic", "/rosout", rosout_topic_desc);

        rcl_interfaces::msg::ParameterDescriptor rosout_min_lvl_desc;
        rosout_min_lvl_desc.type = rclcpp::ParameterType::PARAMETER_INTEGER;
        rosout_min_lvl_desc.name = "rosout_min_level";
        rosout_min_lvl_desc.description = "Minimum ROS log level to relay (10=DEBUG,20=INFO,30=WARN,40=ERROR,50=FATAL)";
        rosout_min_lvl_desc.read_only = true;
        rosout_min_lvl_desc.integer_range.resize(1);
        rosout_min_lvl_desc.integer_range[0].from_value = 10;
        rosout_min_lvl_desc.integer_range[0].to_value = 50;
        rosout_min_lvl_desc.integer_range[0].step = 10;
        rosout_min_level_ = declare_parameter<int>("rosout_min_level", 20, rosout_min_lvl_desc);

        if (enable_rosout)
        {
            // Create a typed Log channel (uses the built-in Log schema)
            auto chRes = foxglove::schemas::LogChannel::create("/rosout");
            if (!chRes.has_value())
            {
                RCLCPP_WARN(get_logger(), "Failed to create LogChannel /rosout: %s", foxglove::strerror(chRes.error()));
            }
            else
            {
                rosout_chan_.emplace(std::move(chRes.value()));
                RCLCPP_INFO(get_logger(), "Relaying logs from '%s' to Foxglove Log channel /rosout (min level %d)", rosout_topic.c_str(), rosout_min_level_);
                // Best-effort QoS is fine for logs
                auto qos = rclcpp::QoS(rclcpp::KeepLast(200)).best_effort();
                rosout_sub_ =
                    create_subscription<rcl_interfaces::msg::Log>(rosout_topic, qos, [this](rcl_interfaces::msg::Log::ConstSharedPtr msg) { this->onRosout(*msg); });
            }
        }

        use_protobuf_ = declare_parameter<bool>("use_protobuf", true);
    }

  private:
    void onSchemas(const data_tamer_msgs::msg::Schemas& msg)
    {
        for (const data_tamer_msgs::msg::Schema& s : msg.schemas)
            onSchema(s);
    }

    void onSchema(const data_tamer_msgs::msg::Schema& msg)
    {
        if (registry_.has(msg.hash))
        {
            registry_.touch(msg.hash, this->now());
            RCLCPP_INFO(get_logger(), "Schema already registered for hash=%lu", (unsigned long)msg.hash);
            return;
        }

        std::shared_ptr<ChannelInfo> info = std::make_shared<ChannelInfo>();
        info->parsed_schema = DataTamerParser::BuilSchemaFromText(msg.schema_text);
        info->topic = (msg.channel_name.empty() || msg.channel_name[0] == '/') ? msg.channel_name : ("/" + msg.channel_name);

        foxglove::Schema schema{};
        std::string message_encoding;
        if (use_protobuf_)
        {
            info->proto_runtime = data_tamer_tools::buildProto(info->parsed_schema);

            schema.encoding = "protobuf";
            schema.name = info->proto_runtime.full_type;
            schema.data = reinterpret_cast<const std::byte*>(info->proto_runtime.fdset_bytes.data());
            schema.data_len = info->proto_runtime.fdset_bytes.size();
            message_encoding = "protobuf";
        }
        else
        {
            info->json_schema = data_tamer_tools::convertToJSONSchema(info->parsed_schema).dump();
            schema.encoding = "jsonschema";
            schema.data = reinterpret_cast<const std::byte*>(info->json_schema.data());
            schema.data_len = info->json_schema.size();
            message_encoding = "json";
        }

        auto chanRes = foxglove::RawChannel::create(info->topic, message_encoding, std::move(schema));
        if (!chanRes.has_value())
        {
            RCLCPP_ERROR(get_logger(), "Failed to create channel '%s'", info->topic.c_str());
            return;
        }
        info->channel = std::make_shared<foxglove::RawChannel>(std::move(chanRes.value()));
        info->last_seen = this->now();  // start the clock
        // Atomically publish it in the registry
        if (!registry_.try_emplace(msg.hash, info))
        {
            // Another thread won the race; just drop our extra channel.
            return;
        }

        RCLCPP_INFO(get_logger(), "Registered Foxglove channel '%s' (hash=%lu)", msg.channel_name.c_str(), (unsigned long)msg.hash);
    }

    // (Re)subscribe to every topic whose type == data_tamer_msgs/msg/Snapshot.
    void discoverSnapshotTopics()
    {
        // QoS: schemas are latched (Transient Local) so late-joiners get them once.
        rclcpp::QoS qos_schema = rclcpp::QoS(rclcpp::KeepLast(10)).transient_local().reliable();
        rclcpp::QoS qos_snap = rclcpp::QoS(rclcpp::KeepLast(100)).reliable();

        const std::map<std::string, std::vector<std::string>> topics = this->get_topic_names_and_types();
        for (const auto& [name, types] : topics)
        {
            for (const auto& t : types)
            {
                // 1) Snapshots
                if (t == "data_tamer_msgs/msg/Snapshot")
                {
                    if (snapshot_subs_.count(name) == 0)
                    {
                        rclcpp::Subscription<data_tamer_msgs::msg::Snapshot>::SharedPtr sub = create_subscription<data_tamer_msgs::msg::Snapshot>(
                            name, qos_snap, [this, topic = name](data_tamer_msgs::msg::Snapshot::ConstSharedPtr msg) { this->onSnapshot(topic, *msg); });
                        snapshot_subs_.emplace(name, sub);
                        RCLCPP_INFO(get_logger(), "Subscribed to Snapshot: %s", name.c_str());
                    }
                }

                // 2) Single Schema
                if (t == "data_tamer_msgs/msg/Schema")
                {
                    if (schema_subs_.count(name) == 0)
                    {
                        rclcpp::Subscription<data_tamer_msgs::msg::Schema>::SharedPtr sub = create_subscription<data_tamer_msgs::msg::Schema>(
                            name, qos_schema, [this](data_tamer_msgs::msg::Schema::ConstSharedPtr msg) { this->onSchema(*msg); });
                        schema_subs_.emplace(name, sub);
                        RCLCPP_INFO(get_logger(), "Subscribed to Schema: %s", name.c_str());
                    }
                }

                // 3) Batch Schemas
                if (t == "data_tamer_msgs/msg/Schemas")
                {
                    if (schemas_subs_.count(name) == 0)
                    {
                        rclcpp::Subscription<data_tamer_msgs::msg::Schemas>::SharedPtr sub = create_subscription<data_tamer_msgs::msg::Schemas>(
                            name, qos_schema, [this](data_tamer_msgs::msg::Schemas::ConstSharedPtr msg) { this->onSchemas(*msg); });
                        schemas_subs_.emplace(name, sub);
                        RCLCPP_INFO(get_logger(), "Subscribed to Schemas: %s", name.c_str());
                    }
                }
            }
        }
    }

    void onSnapshot(const std::string& topic, const data_tamer_msgs::msg::Snapshot& msg)
    {
        std::shared_ptr<ChannelInfo> ch = registry_.get_mutable(msg.schema_hash);
        if (!ch)
        {
            RCLCPP_DEBUG_THROTTLE(get_logger(), *this->get_clock(), 2000, "No schema yet for hash=%lu (topic=%s)", (unsigned long)msg.schema_hash, topic.c_str());
            return;
        }

        // Take a local copy to avoid races with the sweeper resetting ch->channel
        auto chan = ch->channel;
        if (use_protobuf_)
        {
            if (!chan)
            {
                foxglove::Schema sch{};
                sch.encoding = "protobuf";
                sch.name = ch->proto_runtime.full_type;
                sch.data = reinterpret_cast<const std::byte*>(ch->proto_runtime.fdset_bytes.data());
                sch.data_len = ch->proto_runtime.fdset_bytes.size();
                foxglove::FoxgloveResult<foxglove::RawChannel> r = foxglove::RawChannel::create(ch->topic, "protobuf", std::move(sch));
                if (!r.has_value())
                {
                    RCLCPP_WARN(get_logger(), "Recreate proto chan failed for '%s'", ch->topic.c_str());
                    return;
                }
                ch->channel = std::make_shared<foxglove::RawChannel>(std::move(r.value()));
                chan = ch->channel;
            }

            const google::protobuf::Message* proto = ch->proto_runtime.factory->GetPrototype(ch->proto_runtime.desc);
            if (!proto)
            {
                RCLCPP_WARN(get_logger(), "No prototype for %s", ch->proto_runtime.full_type.c_str());
                return;
            }

            if (!ch->m)
            {
                ch->m.reset(proto->New());  // allocate once
            }
            std::lock_guard<std::mutex> lk(ch->pb_mutex);
            google::protobuf::Message* m = ch->m.get();
            m->Clear();  // reuse memory

            DataTamerParser::SnapshotView sv;
            sv.schema_hash = msg.schema_hash;
            sv.timestamp = msg.timestamp_nsec;
            sv.active_mask = { msg.active_mask.data(), msg.active_mask.size() };
            sv.payload = { msg.payload.data(), msg.payload.size() };

            thread_local std::string bytes;
            if (!data_tamer_tools::encodeSnapshot(ch->parsed_schema, sv, ch->proto_runtime, bytes, m))
            {
                RCLCPP_ERROR(get_logger(), "Failed to encode snapshot");
                return;
            }

            ch->channel->log(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size(), msg.timestamp_nsec);
        }
        else
        {
            // Re-create Foxglove channel lazily if soft-evicted
            if (!chan)
            {
                foxglove::Schema s{};
                s.encoding = "jsonschema";
                s.data = reinterpret_cast<const std::byte*>(ch->json_schema.data());
                s.data_len = ch->json_schema.size();

                const std::string resolved_topic = (!ch->topic.empty() && ch->topic[0] == '/') ? ch->topic : ("/" + ch->topic);

                foxglove::FoxgloveResult<foxglove::RawChannel> r = foxglove::RawChannel::create(resolved_topic, "json", std::move(s));
                if (!r.has_value())
                {
                    RCLCPP_WARN(get_logger(), "Re-create channel failed for '%s'", resolved_topic.c_str());
                    return;
                }
                ch->channel = std::make_shared<foxglove::RawChannel>(std::move(r.value()));
                chan = ch->channel;  // refresh local copy
                RCLCPP_INFO(get_logger(), "Recreated Foxglove channel '%s'", resolved_topic.c_str());
            }

            std::string json_text = data_tamer_tools::serializeSnapshotToJson(ch->parsed_schema, msg.active_mask, msg.payload,
                                                                              std::chrono::nanoseconds{ static_cast<int64_t>(msg.timestamp_nsec) })
                                        .dump();

            chan->log(reinterpret_cast<const std::byte*>(json_text.data()), json_text.size(), msg.timestamp_nsec);
        }

        registry_.touch(msg.schema_hash, this->now());
    }

  private:
    std::optional<foxglove::WebSocketServer> server_;
    rclcpp::TimerBase::SharedPtr discovery_timer_;

    std::unordered_map<std::string, rclcpp::Subscription<data_tamer_msgs::msg::Snapshot>::SharedPtr> snapshot_subs_;
    std::unordered_map<std::string, rclcpp::Subscription<data_tamer_msgs::msg::Schema>::SharedPtr> schema_subs_;
    std::unordered_map<std::string, rclcpp::Subscription<data_tamer_msgs::msg::Schemas>::SharedPtr> schemas_subs_;

    SchemaRegistry registry_;

    rclcpp::TimerBase::SharedPtr eviction_timer_;
    rclcpp::Duration eviction_ttl_{ 0, 0 };

    std::optional<foxglove::schemas::LogChannel> rosout_chan_;
    rclcpp::Subscription<rcl_interfaces::msg::Log>::SharedPtr rosout_sub_;
    int rosout_min_level_{ 20 };  // INFO default
    std::mutex namer_mtx_;

    bool use_protobuf_{ false };

    void evictStalePublishers()
    {
        const size_t closed = registry_.soft_sweep_idle_channels(this->now(), eviction_ttl_);
        if (closed > 0)
        {
            RCLCPP_INFO(get_logger(), "GC: soft-evicted (closed) %zu Foxglove channels idle > %.0f s; registry size: %zu", closed, eviction_ttl_.seconds(),
                        registry_.size());
        }
    }

    static const char* levelToString(uint8_t lvl)
    {
        using rcl_interfaces::msg::Log;
        switch (lvl)
        {
            case Log::DEBUG:
                return "DEBUG";
            case Log::INFO:
                return "INFO";
            case Log::WARN:
                return "WARN";
            case Log::ERROR:
                return "ERROR";
            case Log::FATAL:
                return "FATAL";
            default:
                return "UNKNOWN";
        }
    }

    void onRosout(const rcl_interfaces::msg::Log& m)
    {
        if (static_cast<int>(m.level) < rosout_min_level_)
        {
            return;  // filtered
        }
        if (!rosout_chan_.has_value())
        {
            return;  // not enabled / failed to init
        }

        // Build foxglove.schemas.Log
        foxglove::schemas::Log ev{};
        ev.timestamp = foxglove::schemas::Timestamp{ static_cast<uint32_t>(m.stamp.sec), static_cast<uint32_t>(m.stamp.nanosec) };
        ev.level = toFoxgloveLevel(m.level);
        ev.message = m.msg;
        ev.name = m.name;  // node name
        ev.file = m.file;
        ev.line = static_cast<uint32_t>(m.line);

        // Log it (use ROS time as the log_time too, to keep everything consistent)
        const uint64_t log_time_ns = static_cast<uint64_t>(m.stamp.sec) * 1000000000ULL + static_cast<uint64_t>(m.stamp.nanosec);
        (void)rosout_chan_->log(ev, log_time_ns);
    }
};
}  // namespace data_tamer_tools
#include <rclcpp_components/register_node_macro.hpp>
RCLCPP_COMPONENTS_REGISTER_NODE(data_tamer_tools::DtRos2ToFoxgloveBridge)
