// mcap_converter.cpp  (DataTamer MCAP -> Foxglove-readable MCAP via Protobuf)
#include <mcap/reader.hpp>
#include <mcap/writer.hpp>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>

#include <nlohmann/json.hpp>
#include <unordered_map>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>

// --- Your DataTamer headers (adjust include paths if needed)
#include <data_tamer_tools/helpers.hpp>
#include <data_tamer_parser/data_tamer_parser.hpp>

namespace data_tamer_tools
{

// ---------- Converter main ----------

// Store the maskBytes in the channel context so we don’t recompute:
struct PerInputChannel
{
    DataTamerParser::Schema dt_schema;
    ProtoRuntime dp;
    mcap::SchemaId outSchemaId{ 0 };
    mcap::ChannelId outChannelId{ 0 };
    size_t maskBytes{ 0 };  // <— add this
};

}  // namespace data_tamer_tools

int main(int argc, char** argv)
{
    if (argc < 3)
    {
        std::cerr << "usage: " << argv[0] << " in.mcap out.mcap [--compression zstd|lz4|none] [--chunk-size <size>]\n";
        std::cerr << "  --compression: Compression type (default: zstd)\n";
        std::cerr << "  --chunk-size: Chunk size in bytes (default: writer default)\n";
        return 1;
    }
    const std::string inPath = argv[1];
    const std::string outPath = argv[2];
    
    // Parse optional arguments
    mcap::Compression compression = mcap::Compression::Zstd;
    std::optional<uint64_t> chunkSize;
    
    for (int i = 3; i < argc; i++)
    {
        std::string arg = argv[i];
        if (arg == "--compression" && i + 1 < argc)
        {
            std::string compType = argv[++i];
            if (compType == "zstd")
                compression = mcap::Compression::Zstd;
            else if (compType == "lz4")
                compression = mcap::Compression::Lz4;
            else if (compType == "none")
                compression = mcap::Compression::None;
            else
            {
                std::cerr << "Invalid compression type: " << compType << " (must be zstd, lz4, or none)\n";
                return 1;
            }
        }
        else if (arg == "--chunk-size" && i + 1 < argc)
        {
            try
            {
                chunkSize = std::stoull(argv[++i]);
                if (chunkSize.value() == 0)
                {
                    std::cerr << "Chunk size must be positive\n";
                    return 1;
                }
            }
            catch (const std::exception&)
            {
                std::cerr << "Invalid chunk size: " << argv[i] << " (must be a positive integer)\n";
                return 1;
            }
        }
        else
        {
            std::cerr << "Unknown argument: " << arg << "\n";
            return 1;
        }
    }

    if (!data_tamer_tools::isDataTamerFile(inPath))
    {
        std::cerr << "Skipping " << inPath << " (not data_tamer encoded)\n";
        return 1;
    }

    mcap::McapReader reader;
    if (auto st = reader.open(inPath); !st.ok())
    {
        std::cerr << "Failed to open input: " << st.message << "\n";
        return 1;
    }

    mcap::McapWriter writer;
    mcap::McapWriterOptions wopt("protobuf");  // profile is required in your API
    wopt.compression = compression;
    if (chunkSize.has_value())
    {
        wopt.chunkSize = chunkSize.value();
    }
    if (auto st = writer.open(outPath, wopt); !st.ok())
    {
        std::cerr << "Failed to open writer: " << st.message << "\n";
        return 1;
    }

    // Map each *input channel id* to its output schema/channel/runtime proto
    std::unordered_map<mcap::ChannelId, data_tamer_tools::PerInputChannel> chanMap;

    auto view = reader.readMessages();
    for (const auto& rec : view)
    {
        const mcap::Message& msg = rec.message;
        const mcap::Channel& inCh = *rec.channel;
        const mcap::Schema& inSch = *rec.schema;

        // --- lazy init per input channel ---
        auto it = chanMap.find(inCh.id);
        if (it == chanMap.end())
        {
            // Parse DataTamer schema text from MCAP schema record
            const auto& sbytes = inSch.data;  // ByteArray = std::vector<std::byte>
            std::string schema_text(reinterpret_cast<const char*>(sbytes.data()), sbytes.size());

            data_tamer_tools::PerInputChannel ctx;
            ctx.dt_schema = DataTamerParser::BuilSchemaFromText(schema_text);  // (spelled "Buil" in the DT headers)
            ctx.dp = data_tamer_tools::buildProto(ctx.dt_schema);

            // Register output schema (protobuf, name = full type)
            mcap::Schema outSchema(ctx.dp.full_type, "protobuf", ctx.dp.fdset_bytes);
            writer.addSchema(outSchema);
            ctx.outSchemaId = outSchema.id;

            // Register output channel mirroring the input topic
            mcap::Channel outChan(inCh.topic, "protobuf", ctx.outSchemaId, {});
            writer.addChannel(outChan);
            ctx.outChannelId = outChan.id;

            it = chanMap.emplace(inCh.id, std::move(ctx)).first;
        }

        data_tamer_tools::PerInputChannel& ctx = it->second;

        // --- slice snapshot framing: [u32 mask_len][mask][u32 payload_len][payload] ---
        DataTamerParser::SnapshotView sv;
        sv.schema_hash = ctx.dt_schema.hash;
        sv.timestamp = msg.logTime;

        if (!data_tamer_tools::parseFramedSnapshot(reinterpret_cast<const uint8_t*>(msg.data), static_cast<size_t>(msg.dataSize), sv))
        {
            std::cerr << "Failed to parse snapshot\n";
            continue;
        }

        std::string bytes;
        if (!data_tamer_tools::encodeSnapshot(ctx.dt_schema, sv, ctx.dp, bytes))
        {
            std::cerr << "Failed to encode snapshot\n";
            continue;
        }

        // Write protobuf message out
        mcap::Message outMsg;
        outMsg.channelId = ctx.outChannelId;
        outMsg.logTime = msg.logTime;
        outMsg.publishTime = msg.publishTime;
        outMsg.sequence = msg.sequence;
        outMsg.data = reinterpret_cast<const std::byte*>(bytes.data());
        outMsg.dataSize = bytes.size();

        if (auto st = writer.write(outMsg); !st.ok())
        {
            std::cerr << "writer.write failed: " << st.message << "\n";
            return 1;
        }
    }

    reader.close();
    writer.close();
    return 0;
}
