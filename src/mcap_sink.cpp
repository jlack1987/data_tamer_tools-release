#include <data_tamer_tools/helpers.hpp>
#include <data_tamer_tools/sinks/mcap_sink.hpp>

#include <mcap/writer.hpp>
#include <mcap/reader.hpp>

#include <nlohmann/json.hpp>
#include <sstream>
#include <mutex>

#define MCAP_IMPLEMENTATION

namespace data_tamer_tools
{

// ----------- ctor helpers -----------

mcap::McapWriterOptions McapSink::makeOptions(Format fmt, Compression compression, uint64_t chunk_size)
{
    // IMPORTANT: profile must match the messageEncoding you will write.
    mcap::McapWriterOptions o(fmt == Format::Protobuf ? "protobuf" : "json");
    o.chunkSize = chunk_size;
    switch (compression)
    {
        case Compression::Zstd:
            o.compression = mcap::Compression::Zstd;
            break;
        case Compression::Lz4:
            o.compression = mcap::Compression::Lz4;
            break;
        default:
            o.compression = mcap::Compression::None;
            break;
    }
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

McapSink::McapSink(const std::string& filepath, const mcap::McapWriterOptions& options, Format fmt) : filepath_(filepath), format_(fmt)
{
    openFile(filepath_, options);
}

McapSink::McapSink(std::string const& filepath, Format fmt, Compression compression, uint64_t chunk_size)
{
    auto o = makeOptions(fmt, compression, chunk_size);
    filepath_ = filepath;
    format_ = fmt;
    openFile(filepath_, o);
}

void McapSink::openFile(std::string const& filepath, const mcap::McapWriterOptions& options)
{
    std::scoped_lock lk(mutex_);
    writer_ = std::make_unique<mcap::McapWriter>();
    auto status = writer_->open(filepath, options);
    if (!status.ok())
    {
        throw std::runtime_error("Failed to open MCAP file for writing");
    }
    forced_stop_recording_ = false;
    // clean up, in case this was opened a second time
    hash_to_channel_id_.clear();
    parser_schemas_.clear();
    proto_runtimes_.clear();
    scratch_msgs_.clear();
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
    filepath_ = filepath;
    openFile(filepath_, options);

    // rebuild the channels using the original format_
    for (auto const& [name, hash] : channel_names_to_hash_)
    {
        addChannel(name, schemas_[hash]);
    }
}

}  // namespace data_tamer_tools
