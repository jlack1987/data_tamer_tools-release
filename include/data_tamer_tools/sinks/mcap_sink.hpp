#pragma once

#include <data_tamer/data_sink.hpp>
#include <data_tamer_parser/data_tamer_parser.hpp>
#include <data_tamer_tools/helpers.hpp>
#include <data_tamer_tools/msg/log_dir.hpp>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <mcap/writer.hpp>
#include <rclcpp/rclcpp.hpp>

#include <mutex>
#include <unordered_map>
#include <memory>
#include <string>
#include <vector>
#include <filesystem>

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
    McapSink(std::string const& filepath, const mcap::McapWriterOptions& options, Format fmt = Format::Protobuf, bool append_timestamp = false);
    McapSink(const rclcpp::Node::SharedPtr& n, std::string const& filepath, const mcap::McapWriterOptions& options, Format fmt = Format::Protobuf,
             bool append_timestamp = false);

    /**
     * @brief mcap sink convenience ctor (compression/chunk). Default format = Protobuf.
     * @param filepath file path of the new file (should be ".mcap" extension)
     * @param fmt format of the data to write
     * @param compression compression algorithm to use
     * @param chunk_size optional chunk size to use, default is mcap::DefaultChunkSize
     */
    McapSink(std::string const& filepath, Format fmt = Format::Protobuf, Compression compression = Compression::Zstd, std::optional<uint64_t> chunk_size = std::nullopt,
             bool append_timestamp = false);
    /**
     * @brief mcap sink convenience ctor (compression/chunk). Default format = Protobuf.
     * @param node for subscribing to log rotation topic
     * @param filepath file path of the new file (should be ".mcap" extension)
     * @param fmt format of the data to write
     * @param compression compression algorithm to use
     * @param chunk_size optional chunk size to use, default is mcap::DefaultChunkSize
     */
    McapSink(const rclcpp::Node::SharedPtr& n, std::string const& filepath, Format fmt = Format::Protobuf, Compression compression = Compression::Zstd,
             std::optional<uint64_t> chunk_size = std::nullopt, bool append_timestamp = false);
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
    // Convert directory from msg into new full path (<dir>/<base_filename_>) and rotate
    void rotateToDirectory(const std::string& new_dir);

    static mcap::McapWriterOptions makeOptions(Format fmt, Compression compression, std::optional<uint64_t> chunk_size = std::nullopt);

  private:
    mcap::McapWriterOptions last_options_{ "protobuf" };
    // Control subscription (optional)
    rclcpp::Subscription<data_tamer_tools::msg::LogDir>::SharedPtr rotate_sub_;

    // --- config/state ---
    std::string filepath_;
    std::string base_filename_;  // template filename captured from first filepath (no timestamp)
    Format format_ = Format::Json;
    bool append_timestamp_{ false };

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

    // Minimal converter DT -> Parser schema so we can reuse encodeSnapshot()
    static DataTamerParser::Schema toParserSchema(const DataTamer::Schema& s);
    // Helper to wire subscription from a node & param
    void setupRotationControl(const rclcpp::Node::SharedPtr& node);

    // openFile now remembers options + filepath (we’ll use base_filename_ set at first construction)
    void openFile(std::string const& filepath, const mcap::McapWriterOptions& options);
    std::string applyTimestampIfNeeded(const std::string& filepath) const;
    static std::string makeTimestampString();
};
}  // namespace data_tamer_tools
