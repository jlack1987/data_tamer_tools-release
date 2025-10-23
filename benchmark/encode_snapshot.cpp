// bench_encode_snapshot.cpp
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <memory>
#include <iostream>

#include <rosbag2_cpp/reader.hpp>
#include <rosbag2_cpp/converter_options.hpp>
#include <rosbag2_storage/serialized_bag_message.hpp>
#include <rosbag2_storage/storage_options.hpp>
#include <rclcpp/serialization.hpp>
#include <rclcpp/serialized_message.hpp>

#include <data_tamer_parser/data_tamer_parser.hpp>
#include <data_tamer_tools/helpers.hpp>  // buildProto(...) + encodeSnapshot(...)

#include <data_tamer_msgs/msg/schemas.hpp>
#include <data_tamer_msgs/msg/snapshot.hpp>

using data_tamer_msgs::msg::Schemas;
using data_tamer_msgs::msg::Snapshot;

using Clock = std::chrono::steady_clock;
using ns = std::chrono::nanoseconds;

struct SchemaBundle
{
    DataTamerParser::Schema parsed;
    data_tamer_tools::ProtoRuntime rt;
    std::unique_ptr<google::protobuf::Message> scratch;  // reused
};

struct Stats
{
    std::vector<int64_t> samples_ns;
    size_t messages = 0;
    size_t bytes_out = 0;

    void add(int64_t dur_ns, size_t out_bytes)
    {
        samples_ns.push_back(dur_ns);
        ++messages;
        bytes_out += out_bytes;
    }
    static int64_t pct(std::vector<int64_t>& v, double p)
    {
        if (v.empty())
            return 0;
        std::sort(v.begin(), v.end());
        size_t idx = std::min(v.size() - 1, static_cast<size_t>(p * (v.size() - 1)));
        return v[idx];
    }
    void print(const std::string& label, double total_sec)
    {
        if (samples_ns.empty())
        {
            std::cout << label << "  (no samples)\n";
            return;
        }
        int64_t sum = 0, mx = 0;
        for (auto t : samples_ns)
        {
            sum += t;
            mx = std::max(mx, t);
        }
        auto avg = static_cast<double>(sum) / samples_ns.size();
        auto p50 = pct(samples_ns, 0.50);
        auto p90 = pct(samples_ns, 0.90);
        double msgs_per_s = total_sec > 0 ? messages / total_sec : 0.0;
        double bytes_per_s = total_sec > 0 ? bytes_out / total_sec : 0.0;

        auto fmt_ns = [](int64_t t) { return std::to_string(t) + " ns"; };
        std::cout << "\n[" << label << "]\n"
                  << "  msgs:       " << messages << "\n"
                  << "  mean:       " << fmt_ns((int64_t)avg) << "\n"
                  << "  p50:        " << fmt_ns(p50) << "\n"
                  << "  p90:        " << fmt_ns(p90) << "\n"
                  << "  max:        " << fmt_ns(mx) << "\n"
                  << "  msgs/s:     " << msgs_per_s << "\n"
                  << "  bytes/s:    " << bytes_per_s << "\n"
                  << "  mean bytes: " << (messages ? (bytes_out / messages) : 0) << "\n";
    }
};

static void usage(const char* argv0)
{
    std::cerr << "Usage: " << argv0 << " <rosbag_path> [--schemas-topic /foo/schemas] [--data-topic /foo/data] [--warmup 100]\n";
}

int main(int argc, char** argv)
{
    if (argc < 2)
    {
        usage(argv[0]);
        return 1;
    }

    std::string bag_path = argv[1];
    std::string schemas_topic = "/data_tamer/schemas";
    std::string data_topic = "/data_tamer/data";
    int warmup = 100;

    for (int i = 2; i < argc; ++i)
    {
        std::string a = argv[i];
        if (a == "--schemas-topic" && i + 1 < argc)
        {
            schemas_topic = argv[++i];
        }
        else if (a == "--data-topic" && i + 1 < argc)
        {
            data_topic = argv[++i];
        }
        else if (a == "--warmup" && i + 1 < argc)
        {
            warmup = std::max(0, std::atoi(argv[++i]));
        }
    }

    // bag reader
    rosbag2_cpp::Reader reader;
    rosbag2_storage::StorageOptions storage_options;
    storage_options.uri = bag_path;
    storage_options.storage_id = "mcap";  // or leave empty to auto-detect

    rosbag2_cpp::ConverterOptions converter_options;
    converter_options.input_serialization_format = "cdr";
    converter_options.output_serialization_format = "cdr";

    try
    {
        reader.open(storage_options, converter_options);
    }
    catch (const std::exception& e)
    {
        std::cerr << "Failed to open bag: " << e.what() << "\n";
        return 2;
    }

    rclcpp::Serialization<Schemas> ser_schemas;
    rclcpp::Serialization<Snapshot> ser_snapshot;

    // --- caches ---
    // schema_hash -> parsed schema + runtime + scratch msg
    std::unordered_map<uint64_t, SchemaBundle> by_hash;
    // channel_name -> latest schema text (optional; useful if multiple)
    std::unordered_map<std::string, std::string> last_schema_text_by_channel;

    Stats overall;
    std::unordered_map<uint64_t, Stats> per_schema_stats;
    auto t_start = Clock::now();

    size_t snapshots_seen = 0;

    while (reader.has_next())
    {
        auto bag_msg = reader.read_next();
        if (!bag_msg)
            break;

        const std::string& topic = bag_msg->topic_name;

        if (topic == schemas_topic)
        {
            Schemas msg;
            rclcpp::SerializedMessage smsg(*bag_msg->serialized_data);
            ser_schemas.deserialize_message(&smsg, &msg);

            // Update schema cache for each entry
            for (const auto& s : msg.schemas)
            {
                last_schema_text_by_channel[s.channel_name] = s.schema_text;

                // Parse & build runtime once per hash
                if (by_hash.find(s.hash) == by_hash.end())
                {
                    DataTamerParser::Schema ps = DataTamerParser::BuilSchemaFromText(s.schema_text);
                    data_tamer_tools::ProtoRuntime rt = data_tamer_tools::buildProto(ps);

                    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
                    if (!proto)
                    {
                        std::cerr << "WARN: no prototype for schema_hash=" << s.hash << "\n";
                        continue;
                    }
                    SchemaBundle bundle;
                    bundle.parsed = std::move(ps);
                    bundle.rt = std::move(rt);
                    bundle.scratch.reset(proto->New());

                    by_hash.emplace(s.hash, std::move(bundle));
                }
            }
            continue;
        }

        if (topic == data_topic)
        {
            Snapshot msg;
            rclcpp::SerializedMessage smsg(*bag_msg->serialized_data);
            ser_snapshot.deserialize_message(&smsg, &msg);

            auto it = by_hash.find(msg.schema_hash);
            if (it == by_hash.end())
            {
                // No schema yet — skip (or buffer if you want).
                continue;
            }
            auto& bundle = it->second;

            DataTamerParser::SnapshotView sv{};
            sv.schema_hash = msg.schema_hash;
            sv.timestamp = msg.timestamp_nsec;
            sv.active_mask = { msg.active_mask.data(), msg.active_mask.size() };
            sv.payload = { msg.payload.data(), msg.payload.size() };

            // warmup: don't time first N to let caches settle
            const bool do_time = (snapshots_seen >= static_cast<size_t>(warmup));
            ++snapshots_seen;

            std::string bytes;
            int64_t dur = 0;
            if (do_time)
            {
                auto t0 = Clock::now();
                bool ok = data_tamer_tools::encodeSnapshot(bundle.parsed, sv, bundle.rt, bytes, bundle.scratch.get());
                auto t1 = Clock::now();
                if (!ok)
                {
                    std::cerr << "encodeSnapshot failed for hash=" << msg.schema_hash << "\n";
                    continue;
                }
                dur = std::chrono::duration_cast<ns>(t1 - t0).count();
            }
            else
            {
                bool ok = data_tamer_tools::encodeSnapshot(bundle.parsed, sv, bundle.rt, bytes, bundle.scratch.get());
                if (!ok)
                    continue;
            }

            if (do_time)
            {
                overall.add(dur, bytes.size());
                per_schema_stats[msg.schema_hash].add(dur, bytes.size());
            }
        }
    }

    auto t_end = Clock::now();
    double total_sec = std::chrono::duration<double>(t_end - t_start).count();

    // --- print ---
    std::cout << "\n=== EncodeSnapshot Benchmark ===\n";
    std::cout << "Bag: " << bag_path << "\n";
    std::cout << "Topics: schemas='" << schemas_topic << "' data='" << data_topic << "'\n";
    std::cout << "Warmup: " << warmup << " snapshots (not timed)\n";
    std::cout << "Distinct schemas: " << by_hash.size() << "\n";

    overall.print("OVERALL", total_sec);

    for (auto& kv : per_schema_stats)
    {
        const auto hash = kv.first;
        auto& st = kv.second;
        st.print("schema_hash=" + std::to_string(hash), total_sec);
    }

    return 0;
}
