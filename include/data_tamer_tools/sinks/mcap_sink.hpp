#pragma once

#include <data_tamer/data_sink.hpp>
#include <data_tamer_parser/data_tamer_parser.hpp>
#include <data_tamer_tools/helpers.hpp>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>

#include <mutex>
#include <unordered_map>
#include <memory>
#include <string>
#include <vector>

// Forward declaration
namespace mcap
{
class McapWriter;
class McapWriterOptions;
}  // namespace mcap

namespace data_tamer_tools
{

class McapSink : public DataTamer::DataSinkBase
{
  public:
    enum Compression : size_t
    {
        None = 0,
        Lz4,
        Zstd,
    };

    enum class Format
    {
        Json,
        Protobuf,
    };

    using SharedPtr = std::shared_ptr<McapSink>;

    /**
     * @brief mcap sink with explicit writer options. Default format = Protobuf.
     */
    McapSink(std::string const& filepath, const mcap::McapWriterOptions& options, Format fmt = Format::Protobuf);

    /**
     * @brief mcap sink convenience ctor (compression/chunk). Default format = Protobuf.
     */
    McapSink(std::string const& filepath, Format fmt = Format::Protobuf, Compression compression = Compression::Zstd, uint64_t chunk_size = 1024 * 768);

    ~McapSink() override;

    void addChannel(std::string const& channel_name, DataTamer::Schema const& schema) override;
    bool storeSnapshot(const DataTamer::Snapshot& snapshot) override;

    /// Stop recording and save the file
    void stopRecording();

    /**
     * @param filepath file path of the new file (should be ".mcap" extension)
     * @param options  mcap writer options struct
     */
    void restartRecording(std::string const& filepath, const mcap::McapWriterOptions& options);

    static mcap::McapWriterOptions makeOptions(Format fmt, Compression compression, uint64_t chunk_size);

  private:
    // --- config/state ---
    std::string filepath_;
    Format format_ = Format::Json;

    std::unique_ptr<mcap::McapWriter> writer_;

    // Maps
    std::unordered_map<uint64_t, uint16_t> hash_to_channel_id_;        // schema.hash -> channelId
    std::unordered_map<uint64_t, DataTamer::Schema> schemas_;          // schema.hash -> DT schema
    std::unordered_map<std::string, uint64_t> channel_names_to_hash_;  // chan name -> schema.hash

    // Protobuf runtime per schema.hash (only used when format_ == Protobuf)
    std::unordered_map<uint64_t, DataTamerParser::Schema> parser_schemas_;                   // parser schema
    std::unordered_map<uint64_t, ProtoRuntime> proto_runtimes_;                              // dynamic proto
    std::unordered_map<uint64_t, std::unique_ptr<google::protobuf::Message>> scratch_msgs_;  // reusable scratch

    bool forced_stop_recording_ = false;
    std::recursive_mutex mutex_;
    size_t sequence_ = 0;

    void openFile(std::string const& filepath, const mcap::McapWriterOptions& options);

    // Minimal converter DT -> Parser schema so we can reuse encodeSnapshot()
    static DataTamerParser::Schema toParserSchema(const DataTamer::Schema& s);
};
}  // namespace data_tamer_tools
