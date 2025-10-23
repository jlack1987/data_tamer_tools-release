// dt_snapshot_recorder.cpp
#include <rclcpp/rclcpp.hpp>
#include <data_tamer_msgs/msg/schema.hpp>
#include <data_tamer_msgs/msg/schemas.hpp>
#include <data_tamer_msgs/msg/snapshot.hpp>
#include <fstream>
#include <unordered_map>

struct CapturedSchema
{
    std::string text;
};

struct BinHeader
{
    uint64_t schema_hash;  // little-endian
    uint64_t timestamp_ns;
    uint32_t active_len;
    uint32_t payload_len;
};

int main(int argc, char** argv)
{
    rclcpp::init(argc, argv);
    auto node = std::make_shared<rclcpp::Node>("dt_snapshot_recorder");

    const std::string schema_topic = node->declare_parameter<std::string>("schema_topic", "/data_tamer/schema");
    const std::string schemas_topic = node->declare_parameter<std::string>("schemas_topic", "/data_tamer/schemas");
    const std::string snapshot_topic = node->declare_parameter<std::string>("snapshot_topic", "/data_tamer/snapshot");
    const std::string out_prefix = node->declare_parameter<std::string>("out_prefix", "capture");

    rclcpp::QoS qos_schema = rclcpp::QoS(rclcpp::KeepLast(10)).transient_local().reliable();
    rclcpp::QoS qos_snap = rclcpp::QoS(rclcpp::KeepLast(100)).reliable();

    std::unordered_map<uint64_t, CapturedSchema> schemas;

    auto on_schema = [&](const data_tamer_msgs::msg::Schema& s)
    {
        schemas[s.hash] = CapturedSchema{ s.schema_text };
        // Write schema text now so we always have it:
        std::ofstream fs(out_prefix + "." + std::to_string(s.hash) + ".schema.txt", std::ios::binary);
        fs.write(s.schema_text.data(), static_cast<std::streamsize>(s.schema_text.size()));
        RCLCPP_INFO(node->get_logger(), "Captured schema hash=%lu", (unsigned long)s.hash);
    };

    auto sub_schema =
        node->create_subscription<data_tamer_msgs::msg::Schema>(schema_topic, qos_schema, [=](data_tamer_msgs::msg::Schema::ConstSharedPtr m) { on_schema(*m); });

    auto sub_schemas =
        node->create_subscription<data_tamer_msgs::msg::Schemas>(schemas_topic, qos_schema,
                                                                 [=, &schemas](data_tamer_msgs::msg::Schemas::ConstSharedPtr m)
                                                                 {
                                                                     for (auto& s : m->schemas)
                                                                     {
                                                                         schemas[s.hash] = CapturedSchema{ s.schema_text };
                                                                         std::ofstream fs(out_prefix + "." + std::to_string(s.hash) + ".schema.txt", std::ios::binary);
                                                                         fs.write(s.schema_text.data(), static_cast<std::streamsize>(s.schema_text.size()));
                                                                     }
                                                                     RCLCPP_INFO(node->get_logger(), "Captured %zu schemas (batch)", m->schemas.size());
                                                                 });

    auto sub_snapshot = node->create_subscription<data_tamer_msgs::msg::Snapshot>(snapshot_topic, qos_snap,
                                                                                  [=, &schemas](data_tamer_msgs::msg::Snapshot::ConstSharedPtr m)
                                                                                  {
                                                                                      auto it = schemas.find(m->schema_hash);
                                                                                      if (it == schemas.end())
                                                                                      {
                                                                                          RCLCPP_WARN(node->get_logger(), "No schema yet for hash=%lu; waiting...",
                                                                                                      (unsigned long)m->schema_hash);
                                                                                          return;
                                                                                      }
                                                                                      // Write one binary record
                                                                                      const std::string base = out_prefix + "." + std::to_string(m->schema_hash);
                                                                                      std::ofstream fb(base + ".snapshot.bin", std::ios::binary);

                                                                                      BinHeader hdr{};
                                                                                      hdr.schema_hash = m->schema_hash;
                                                                                      hdr.timestamp_ns = static_cast<uint64_t>(m->timestamp_nsec);
                                                                                      hdr.active_len = static_cast<uint32_t>(m->active_mask.size());
                                                                                      hdr.payload_len = static_cast<uint32_t>(m->payload.size());

                                                                                      fb.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
                                                                                      fb.write(reinterpret_cast<const char*>(m->active_mask.data()), hdr.active_len);
                                                                                      fb.write(reinterpret_cast<const char*>(m->payload.data()), hdr.payload_len);

                                                                                      RCLCPP_INFO(node->get_logger(), "Wrote %s.{schema.txt,snapshot.bin}", base.c_str());
                                                                                      rclcpp::shutdown();  // capture one, then exit
                                                                                  });

    rclcpp::spin(node);
    rclcpp::shutdown();
    return 0;
}
