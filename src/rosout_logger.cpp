#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <vector>
#include <algorithm>

#include <rclcpp/rclcpp.hpp>
#include <rcl_interfaces/msg/log.hpp>
#include <data_tamer_tools/msg/log_dir.hpp>

#include <mcap/writer.hpp>
#include <mcap/reader.hpp>

#include <foxglove/foxglove.hpp>
#include <foxglove/schemas.hpp>

using namespace std::chrono_literals;

namespace data_tamer_tools
{

class RosoutLogger : public rclcpp::Node
{
  public:
    RosoutLogger(const rclcpp::NodeOptions& options = rclcpp::NodeOptions()) : rclcpp::Node("rosout_mcap_logger", options)
    {
        // --- params ---
        rcl_interfaces::msg::ParameterDescriptor rosout_topic_desc;
        rosout_topic_desc.type = rclcpp::ParameterType::PARAMETER_STRING;
        rosout_topic_desc.name = "rosout_topic";
        rosout_topic_desc.description = "ROS topic to subscribe for logs";
        rosout_topic_desc.read_only = true;
        topic_ = declare_parameter<std::string>("rosout_topic", "/rosout", rosout_topic_desc);

        output_base_ = declare_parameter<std::string>("output_base", "rosout");
        out_dir_ = declare_parameter<std::string>("output_dir", ".");
        compression_ = declare_parameter<std::string>("compression", "zstd");  // none|zstd|lz4
        chunk_size_ = declare_parameter<int>("chunk_size", 0);
        append_timestamp_ = declare_parameter<bool>("append_timestamp", true);

        // control topic name (latched): when a String(dir) arrives, rotate to <dir>/rosout.mcap
        control_topic_ = declare_parameter<std::string>("rotate_dir_topic", "/data_tamer/rotate_dir");

        // --- subs ---
        sub_ = create_subscription<rcl_interfaces::msg::Log>(topic_, rclcpp::SystemDefaultsQoS(), std::bind(&RosoutLogger::onLog, this, std::placeholders::_1));

        // latched control subscriber
        rclcpp::QoS latched_qos(1);
        latched_qos.transient_local().reliable();
        rotate_sub_ = create_subscription<data_tamer_tools::msg::LogDir>(control_topic_, latched_qos, std::bind(&RosoutLogger::onRotateDir, this, std::placeholders::_1));

        // --- file (time-stamped initial file) ---
        openWriterAtPath(makeFilename(), /*first_open=*/true);

        RCLCPP_INFO(get_logger(), "rosout_mcap_logger started: topic='%s' dir='%s' base='%s' comp='%s' chunk=%d control='%s'", topic_.c_str(), out_dir_.c_str(),
                    output_base_.c_str(), compression_.c_str(), chunk_size_, control_topic_.c_str());
    }

    ~RosoutLogger() override
    {
        std::lock_guard<std::mutex> lk(m_);
        closeWriter();
    }

  private:
    // ---------- state ----------
    std::mutex m_;
    std::unique_ptr<mcap::McapWriter> writer_;
    mcap::Channel ch_{};
    mcap::Schema schema_{};
    std::string current_path_;
    uint64_t sequence_{ 0 };
    uint64_t bytes_written_{ 0 };

    // params
    std::string topic_;
    std::string output_base_;
    std::string out_dir_;
    std::string compression_;
    int chunk_size_{ 0 };
    bool append_timestamp_{ true };
    std::string control_topic_;

    rclcpp::Subscription<rcl_interfaces::msg::Log>::SharedPtr sub_;
    rclcpp::Subscription<data_tamer_tools::msg::LogDir>::SharedPtr rotate_sub_;

    // ---------- helpers ----------
    std::string makeFilename()
    {
        std::filesystem::create_directories(out_dir_);
        std::string filename = output_base_;
        if (append_timestamp_)
        {
            filename += "_" + timestampSuffix();
        }
        filename += ".mcap";
        return (std::filesystem::path(out_dir_) / filename).string();
    }

    std::string timestampSuffix()
    {
        auto sysnow = std::chrono::system_clock::now();
        std::time_t tt = std::chrono::system_clock::to_time_t(sysnow);
        std::tm tm{};
        localtime_r(&tt, &tm);

        char buf[64];
        if (std::strftime(buf, sizeof(buf), "%Y%m%d_%H%M%S", &tm) == 0)
        {
            std::snprintf(buf, sizeof(buf), "unknown");
        }
        return std::string(buf);
    }

    static mcap::Compression toCompression(const std::string& s)
    {
        std::string lower = s;
        std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
        if (lower == "zstd")
            return mcap::Compression::Zstd;
        if (lower == "lz4")
            return mcap::Compression::Lz4;
        return mcap::Compression::None;
    }

    // Internal helper: open writer at path (assumes lock is already held)
    bool openWriterInternal(const std::string& fullpath)
    {
        mcap::McapWriterOptions opts("protobuf");
        opts.chunkSize = chunk_size_;
        if (chunk_size_ == 0)
            opts.noChunking = true;
        opts.compression = toCompression(compression_);

        writer_ = std::make_unique<mcap::McapWriter>();
        current_path_ = fullpath;
        if (auto st = writer_->open(current_path_, opts); !st.ok())
        {
            RCLCPP_ERROR(get_logger(), "Failed to open MCAP '%s': %s", current_path_.c_str(), st.message.c_str());
            writer_.reset();
            return false;
        }

        // Register Foxglove Log schema & channel
        foxglove::Schema fs = foxglove::schemas::Log::schema();
        schema_ = mcap::Schema(fs.name, fs.encoding, std::string((const char*)fs.data, fs.data_len));
        writer_->addSchema(schema_);

        ch_ = mcap::Channel(topic_, "protobuf", schema_.id);
        writer_->addChannel(ch_);

        bytes_written_ = 0;
        return true;
    }

    // Open writer at explicit path; single lock inside (no double-locking)
    bool openWriterAtPath(const std::string& fullpath, bool first_open)
    {
        std::lock_guard<std::mutex> lk(m_);

        if (first_open)
            sequence_ = 0;

        if (!openWriterInternal(fullpath))
            return false;

        RCLCPP_INFO(get_logger(), "Opened MCAP: %s (schema=%s enc=%s)", current_path_.c_str(), schema_.name.c_str(), schema_.encoding.c_str());
        return true;
    }

    void closeWriter()
    {
        if (writer_)
        {
            writer_->close();
            writer_.reset();
            RCLCPP_INFO(get_logger(), "Closed MCAP: %s (bytes ~ %lu)", current_path_.c_str(), (unsigned long)bytes_written_);
        }
    }

    // Rotate to a fixed file name "<dir>/rosout.mcap"
    bool rotateToDirectory(const std::string& dir)
    {
        std::error_code ec;
        std::filesystem::create_directories(dir, ec);
        if (ec)
        {
            RCLCPP_ERROR(get_logger(), "Failed to create dir '%s': %s", dir.c_str(), ec.message().c_str());
            return false;
        }

        std::lock_guard<std::mutex> lk(m_);

        // Close current file
        if (writer_)
        {
            writer_->close();
            writer_.reset();
        }

        // Open new file at fixed path
        std::filesystem::path next_path = std::filesystem::path(dir) / output_base_;
        if (append_timestamp_)
        {
            next_path.replace_filename(output_base_ + "_" + timestampSuffix() + ".mcap");
        }
        else
        {
            next_path.replace_extension(".mcap");
        }

        if (!openWriterInternal(next_path.string()))
            return false;

        // Keep sequence_ monotonic across rotations (don't reset it)
        RCLCPP_INFO(get_logger(), "Rotated mcap logger to: %s", current_path_.c_str());
        return true;
    }

    // ---------- callbacks ----------
    void onRotateDir(const data_tamer_tools::msg::LogDir::SharedPtr msg)
    {
        const std::string dir = msg->directory;
        if (dir.empty())
        {
            RCLCPP_WARN(get_logger(), "RotateDir received empty directory string; ignoring");
            return;
        }
        if (!rotateToDirectory(dir))
        {
            RCLCPP_ERROR(get_logger(), "RotateDir to '%s' failed", dir.c_str());
        }
    }

    void onLog(const rcl_interfaces::msg::Log::SharedPtr msg)
    {
        foxglove::schemas::Log out;
        out.timestamp = foxglove::schemas::Timestamp{ static_cast<uint32_t>(msg->stamp.sec), static_cast<uint32_t>(msg->stamp.nanosec) };
        out.level = [&]
        {
            switch (msg->level)
            {
                case rcl_interfaces::msg::Log::DEBUG:
                    return foxglove::schemas::Log::LogLevel::DEBUG;
                case rcl_interfaces::msg::Log::INFO:
                    return foxglove::schemas::Log::LogLevel::INFO;
                case rcl_interfaces::msg::Log::WARN:
                    return foxglove::schemas::Log::LogLevel::WARNING;
                case rcl_interfaces::msg::Log::ERROR:
                    return foxglove::schemas::Log::LogLevel::ERROR;
                case rcl_interfaces::msg::Log::FATAL:
                    return foxglove::schemas::Log::LogLevel::FATAL;
                default:
                    return foxglove::schemas::Log::LogLevel::UNKNOWN;
            }
        }();
        out.message = msg->msg;
        out.name = msg->name;
        out.file = msg->file;
        out.line = msg->line;

        // encode
        thread_local std::vector<uint8_t> buf(256);
        size_t encodedLen = 0;
        for (;;)
        {
            auto err = out.encode(buf.data(), buf.size(), &encodedLen);
            if (err == foxglove::FoxgloveError::Ok)
                break;
            if (err == foxglove::FoxgloveError::BufferTooShort)
            {
                buf.resize(encodedLen);
                continue;
            }
            return;  // EncodeError: drop
        }

        const mcap::Timestamp now = this->now().nanoseconds();

        std::lock_guard<std::mutex> lk(m_);
        if (!writer_)
            return;

        mcap::Message mm;
        mm.channelId = ch_.id;
        mm.sequence = sequence_++;
        mm.publishTime = now;
        mm.logTime = now;
        mm.data = reinterpret_cast<const std::byte*>(buf.data());
        mm.dataSize = encodedLen;

        if (auto st = writer_->write(mm); !st.ok())
        {
            RCLCPP_WARN(get_logger(), "mcap write failed: %s", st.message.c_str());
            return;
        }
        bytes_written_ += encodedLen;
    }
};

}  // namespace data_tamer_tools

#include <rclcpp_components/register_node_macro.hpp>
RCLCPP_COMPONENTS_REGISTER_NODE(data_tamer_tools::RosoutLogger)
