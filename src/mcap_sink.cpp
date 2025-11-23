#include <data_tamer_tools/helpers.hpp>
#include <data_tamer_tools/sinks/mcap_sink.hpp>

#include <mcap/writer.hpp>
#include <mcap/reader.hpp>

#include <nlohmann/json.hpp>
#include <sstream>
#include <mutex>
#include <chrono>
#include <ctime>
#include <iomanip>

#define MCAP_IMPLEMENTATION

namespace data_tamer_tools
{

// ----------- ctor helpers -----------

mcap::McapWriterOptions McapSink::makeOptions(Format fmt, Compression comp, std::optional<uint64_t> chunk)
{
    mcap::McapWriterOptions o(fmt == Format::Protobuf ? "protobuf" : "json");
    if (chunk.has_value() && *chunk == 0)
    {
        o.noChunking = true;
        o.chunkSize = mcap::DefaultChunkSize;  // ignored when noChunking=true
    }
    else
    {
        o.chunkSize = chunk.value_or(mcap::DefaultChunkSize);
    }
    o.compression = (comp == Compression::Zstd) ? mcap::Compression::Zstd : (comp == Compression::Lz4) ? mcap::Compression::Lz4 : mcap::Compression::None;
    return o;
}

// Minimal DT -> Parser schema converter so we can reuse buildProto/encodeSnapshot.
DataTamerParser::Schema McapSink::toParserSchema(const DataTamer::Schema& s)
{
    DataTamerParser::Schema out;
    out.channel_name = s.channel_name;
    out.hash = s.hash;  // keep the same so naming stays stable

    out.fields.reserve(s.fields.size());
    for (const auto& f : s.fields)
    {
        DataTamerParser::TypeField pf;
        pf.field_name = f.field_name;
        // Map basic type enums by value (DT and Parser enums match in your tree)
        switch (f.type)
        {
            case DataTamer::BasicType::BOOL:
                pf.type = DataTamerParser::BasicType::BOOL;
                break;
            case DataTamer::BasicType::CHAR:
                pf.type = DataTamerParser::BasicType::CHAR;
                break;
            case DataTamer::BasicType::INT8:
                pf.type = DataTamerParser::BasicType::INT8;
                break;
            case DataTamer::BasicType::UINT8:
                pf.type = DataTamerParser::BasicType::UINT8;
                break;
            case DataTamer::BasicType::INT16:
                pf.type = DataTamerParser::BasicType::INT16;
                break;
            case DataTamer::BasicType::UINT16:
                pf.type = DataTamerParser::BasicType::UINT16;
                break;
            case DataTamer::BasicType::INT32:
                pf.type = DataTamerParser::BasicType::INT32;
                break;
            case DataTamer::BasicType::UINT32:
                pf.type = DataTamerParser::BasicType::UINT32;
                break;
            case DataTamer::BasicType::INT64:
                pf.type = DataTamerParser::BasicType::INT64;
                break;
            case DataTamer::BasicType::UINT64:
                pf.type = DataTamerParser::BasicType::UINT64;
                break;
            case DataTamer::BasicType::FLOAT32:
                pf.type = DataTamerParser::BasicType::FLOAT32;
                break;
            case DataTamer::BasicType::FLOAT64:
                pf.type = DataTamerParser::BasicType::FLOAT64;
                break;
            default:
                pf.type = DataTamerParser::BasicType::OTHER;
                break;
        }
        pf.type_name = f.type_name;
        pf.is_vector = f.is_vector || (f.array_size > 0);  // treat fixed arrays as repeated too
        pf.array_size = f.array_size;                      // carry along (may be unused by helpers)
        out.fields.push_back(std::move(pf));
    }

    // If you use custom types in DT, map them here into out.custom_types.

    return out;
}

// ----------- constructors -----------

McapSink::McapSink(const std::string& filepath, const mcap::McapWriterOptions& options, Format fmt, bool append_timestamp)
  : format_(fmt), append_timestamp_(append_timestamp)
{
    base_filename_ = std::filesystem::path(filepath).filename().string();
    auto resolved = applyTimestampIfNeeded(filepath);
    openFile(resolved, options);
}

McapSink::McapSink(std::string const& filepath, Format fmt, Compression compression, std::optional<uint64_t> chunk_size, bool append_timestamp)
  : format_(fmt), append_timestamp_(append_timestamp)
{
    auto o = makeOptions(fmt, compression, chunk_size.value_or(mcap::DefaultChunkSize));
    base_filename_ = std::filesystem::path(filepath).filename().string();
    auto resolved = applyTimestampIfNeeded(filepath);
    openFile(resolved, o);
}

data_tamer_tools::McapSink::McapSink(const rclcpp::Node::SharedPtr& node, const std::string& filepath, const mcap::McapWriterOptions& options, Format fmt,
                                     bool append_timestamp)
  : format_(fmt), append_timestamp_(append_timestamp)
{
    base_filename_ = std::filesystem::path(filepath).filename().string();
    auto resolved = applyTimestampIfNeeded(filepath);
    openFile(resolved, options);
    setupRotationControl(node);
}

data_tamer_tools::McapSink::McapSink(const rclcpp::Node::SharedPtr& node, const std::string& filepath, Format fmt, Compression compression,
                                     std::optional<uint64_t> chunk_size, bool append_timestamp)
  : format_(fmt), append_timestamp_(append_timestamp)
{
    base_filename_ = std::filesystem::path(filepath).filename().string();

    auto opts = makeOptions(fmt, compression, /*chunk*/ chunk_size.value_or(1024 * 768));
    if (chunk_size.has_value() && *chunk_size == 0)
    {
        opts.noChunking = true;
    }
    auto resolved = applyTimestampIfNeeded(filepath);
    openFile(resolved, opts);
    setupRotationControl(node);
}

void data_tamer_tools::McapSink::openFile(std::string const& filepath, const mcap::McapWriterOptions& options)
{
    std::scoped_lock lk(mutex_);

    last_options_ = options;  // remember for rotations
    filepath_ = filepath;     // current full path (dir + base_filename_)

    // Tear down old dynamic-proto state before opening a new file
    scratch_msgs_.clear();
    proto_runtimes_.clear();
    parser_schemas_.clear();
    hash_to_channel_id_.clear();

    writer_ = std::make_unique<mcap::McapWriter>();
    auto status = writer_->open(filepath_, options);
    RCLCPP_INFO(rclcpp::get_logger("McapSink"), "Opening file: %s (base='%s', append_ts=%d)", filepath_.c_str(), base_filename_.c_str(), append_timestamp_);
}

std::string McapSink::applyTimestampIfNeeded(const std::string& filepath) const
{
    if (!append_timestamp_)
    {
        return filepath;
    }

    std::filesystem::path path(filepath);
    const std::string stem = path.stem().string();
    const std::string ext = path.extension().string();
    path.replace_filename(stem + "_" + makeTimestampString() + ext);
    const std::string result = path.string();
    return result;
}

std::string McapSink::makeTimestampString()
{
    const auto now = std::chrono::system_clock::now();
    const std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm local_tm;
    localtime_r(&now_time, &local_tm);
    std::ostringstream oss;
    oss << std::put_time(&local_tm, "%Y-%m-%d_%H-%M-%S");
    return oss.str();
}

McapSink::~McapSink()
{
    stopThread();
    std::scoped_lock lk(mutex_);
    if (writer_)
    {
        writer_->close();
    }
}

void data_tamer_tools::McapSink::setupRotationControl(const rclcpp::Node::SharedPtr& node)
{
    // Param name: rotate_topic (empty => disable)
    // default: "/data_tamer/rotate_mcap" (change if you prefer)
    const std::string topic = node->declare_parameter<std::string>("rotate_topic", "/data_tamer/rotate_dir");
    if (topic.empty())
        return;

    rclcpp::QoS qos(1);
    qos.transient_local().reliable();

    rotate_sub_ = node->create_subscription<data_tamer_tools::msg::LogDir>(topic, qos,
                                                                           [this](data_tamer_tools::msg::LogDir::SharedPtr msg)
                                                                           {
                                                                               if (!msg)
                                                                                   return;
                                                                               rotateToDirectory(msg->directory);
                                                                           });
}

void data_tamer_tools::McapSink::rotateToDirectory(const std::string& new_dir)
{
    if (new_dir.empty())
    {
        std::cerr << "[McapSink] rotateToDirectory: new_dir is empty" << std::endl;
        return;
    }

    std::filesystem::path dir(new_dir);
    std::filesystem::path current_dir = std::filesystem::path(filepath_).parent_path();

    // If new directory is the same as current directory, do nothing
    if (dir == current_dir)
    {
        std::cerr << "[McapSink] rotateToDirectory: new_dir is the same as current directory" << std::endl;
        return;
    }

    std::error_code ec;

    // Only create directory if it doesn't already exist
    if (!std::filesystem::exists(dir, ec))
    {
        std::filesystem::create_directories(dir, ec);  // best effort
    }

    // Compose new path as <dir>/<base_filename_> (+ timestamp if enabled)
    std::filesystem::path new_path = dir / base_filename_;

    // Use same options; restartRecording re-adds channels/schemas under mutex
    try
    {
        restartRecording(new_path.string(), last_options_);
    }
    catch (const std::exception& e)
    {
        throw std::runtime_error("Failed to restart mcap recording");
    }
}

// ----------- channel registration -----------

void McapSink::addChannel(std::string const& channel_name, DataTamer::Schema const& schema)
{
    std::scoped_lock lk(mutex_);
    schemas_[schema.hash] = schema;
    if (hash_to_channel_id_.find(schema.hash) != hash_to_channel_id_.end())
    {
        return;  // already added for this schema.hash
    }

    channel_names_to_hash_[channel_name] = schema.hash;

    // Schema & Channel records depend on format
    if (format_ == Format::Protobuf)
    {
        // Build dynamic proto for this schema
        DataTamerParser::Schema ps = toParserSchema(schema);
        ProtoRuntime rt = buildProto(ps);

        // Register Schema (protobuf) — Foxglove expects FileDescriptorSet bytes
        mcap::Schema mcap_schema(rt.full_type, "protobuf", rt.fdset_bytes);
        writer_->addSchema(mcap_schema);

        // Register Channel using messageEncoding="protobuf"
        mcap::Channel ch(channel_name, "protobuf", mcap_schema.id);
        writer_->addChannel(ch);

        hash_to_channel_id_[schema.hash] = ch.id;
        parser_schemas_[schema.hash] = std::move(ps);
        proto_runtimes_[schema.hash] = std::move(rt);

        // Pre-allocate a scratch message for encode reuse
        const auto& run = proto_runtimes_[schema.hash];
        if (const auto* proto = run.factory->GetPrototype(run.desc))
        {
            scratch_msgs_[schema.hash] = std::unique_ptr<google::protobuf::Message>(proto->New());
        }
    }
    else
    {
        // JSON path (existing behavior)
        const std::string schema_name = channel_name + "::" + std::to_string(schema.hash);
        nlohmann::json json_schema = convertToJSONSchema(schema);
        mcap::Schema mcap_schema(schema_name, "jsonschema", json_schema.dump());
        writer_->addSchema(mcap_schema);

        mcap::Channel ch(channel_name, "json", mcap_schema.id);
        writer_->addChannel(ch);
        hash_to_channel_id_[schema.hash] = ch.id;
    }
}

// ----------- writing snapshots -----------

bool McapSink::storeSnapshot(const DataTamer::Snapshot& snapshot)
{
    std::scoped_lock lk(mutex_);
    if (forced_stop_recording_ || !writer_)
    {
        if (forced_stop_recording_)
        {
            std::cerr << "[McapSink] storeSnapshot: forced stop recording is true, skipping snapshot" << std::endl;
        }
        else if (!writer_)
        {
            std::cerr << "[McapSink] storeSnapshot: mcap writer is null, skipping snapshot" << std::endl;
        }

        return false;
    }

    const auto itS = schemas_.find(snapshot.schema_hash);
    if (itS == schemas_.end())
    {
        std::cerr << "[McapSink] storeSnapshot: unknown schema hash=" << snapshot.schema_hash << "\n";
        return false;  // unknown schema
    }

    mcap::Message msg;
    msg.channelId = hash_to_channel_id_.at(snapshot.schema_hash);
    msg.sequence = sequence_++;  // optional
    msg.logTime = mcap::Timestamp(snapshot.timestamp.count());
    msg.publishTime = msg.logTime;

    std::string payload_bytes;  // reused per call

    if (format_ == Format::Protobuf)
    {
        // Build a SnapshotView that aliases DT buffers
        const auto itP = parser_schemas_.find(snapshot.schema_hash);
        const auto itR = proto_runtimes_.find(snapshot.schema_hash);
        if (itP == parser_schemas_.end() || itR == proto_runtimes_.end())
        {
            return false;
        }

        DataTamerParser::SnapshotView sv;
        sv.schema_hash = snapshot.schema_hash;
        sv.timestamp = snapshot.timestamp.count();
        sv.active_mask = { snapshot.active_mask.data(), snapshot.active_mask.size() };
        sv.payload = { snapshot.payload.data(), snapshot.payload.size() };

        google::protobuf::Message* scratch = nullptr;
        const auto itM = scratch_msgs_.find(snapshot.schema_hash);
        if (itM != scratch_msgs_.end() && itM->second)
        {
            scratch = itM->second.get();
            scratch->Clear();
        }

        if (!encodeSnapshot(itP->second, sv, itR->second, payload_bytes, scratch))
        {
            std::cerr << "[McapSink] encodeSnapshot FAILED for hash=" << snapshot.schema_hash << "\n";
            return false;
        }

        msg.data = reinterpret_cast<const std::byte*>(payload_bytes.data());
        msg.dataSize = payload_bytes.size();
    }
    else
    {
        // JSON path
        nlohmann::json jsonData = serializeSnapshotToJson(snapshot, schemas_[snapshot.schema_hash]);
        payload_bytes = jsonData.dump();
        msg.data = reinterpret_cast<const std::byte*>(payload_bytes.data());
        msg.dataSize = payload_bytes.size();
    }

    auto status = writer_->write(msg);
    if (!status.ok())
    {
        std::cerr << "[McapSink] storeSnapshot: Write failed: " << status.message << std::endl;
    }

    return status.ok();
}

// ----------- control -----------

void McapSink::stopRecording()
{
    std::scoped_lock lk(mutex_);
    forced_stop_recording_ = true;
    if (writer_)
    {
        writer_->close();
        writer_.reset();
    }
}

void McapSink::restartRecording(const std::string& filepath, const mcap::McapWriterOptions& options)
{
    std::scoped_lock lk(mutex_);
    auto resolved = applyTimestampIfNeeded(filepath);
    openFile(resolved, options);

    // rebuild the channels using the original format_
    for (auto const& [name, hash] : channel_names_to_hash_)
    {
        addChannel(name, schemas_[hash]);
    }
    forced_stop_recording_ = false;
}

}  // namespace data_tamer_tools
