#include <mcap/mcap.hpp>
#include <gtest/gtest.h>
#include <data_tamer/data_tamer.hpp>
#include <data_tamer_tools/helpers.hpp>
#include <data_tamer_tools/sinks/mcap_sink.hpp>
#include <filesystem>
#include <nlohmann/json.hpp>
#include <thread>

class McapSinkTest : public ::testing::Test
{
  protected:
    std::string testFilePath = "/tmp/test_sample.mcap";

    void SetUp() override
    {
        // Remove test file if it already exists
        if (std::filesystem::exists(testFilePath))
        {
            std::filesystem::remove(testFilePath);
        }
    }

    void TearDown() override
    {
        // Clean up test file after each test
        if (std::filesystem::exists(testFilePath))
        {
            std::filesystem::remove(testFilePath);
        }
    }
};

// Test that a channel can be registered and snapshot data stored in MCAP file
TEST_F(McapSinkTest, RegisterChannelsAndStoreSnapshot)
{
    // Create the MCAP sink and add it to the registry
    mcap::McapWriterOptions options("json");
    options.compression = mcap::Compression::None;
    options.noChunking = true;
    options.chunkSize = 256;
    data_tamer_tools::McapSink::SharedPtr mcap_sink = std::make_shared<data_tamer_tools::McapSink>(testFilePath, options);

    // Create a channel
    std::shared_ptr<DataTamer::LogChannel> channelA = DataTamer::LogChannel::create("channelA");
    channelA->addDataSink(mcap_sink);

    // Register a vector of doubles
    std::vector<double> v1 = { 0.5, 1.5, 2.5 };
    // double v1 = 12.;
    channelA->registerValue("vector_1", v1.data());
    channelA->registerValue("vector_2", v1.data() + 1);
    channelA->registerValue("vector_3", v1.data() + 2);

    // Take a snapshot
    for (size_t i = 0; i < 1000; ++i)
    {
        channelA->takeSnapshot();
    }

    // Ensure the file is created
    ASSERT_TRUE(std::filesystem::exists(testFilePath));

    // Stop recording
    mcap_sink->stopRecording();

    ASSERT_GT(std::filesystem::file_size(std::filesystem::path(testFilePath)), 0);
}

// Test multiple snapshots over time
TEST_F(McapSinkTest, MultipleSnapshotsOverTime)
{
    // Create the MCAP sink and add it to the registry
    mcap::McapWriterOptions opts("json");
    opts.compression = mcap::Compression::None;
    data_tamer_tools::McapSink::SharedPtr mcap_sink = std::make_shared<data_tamer_tools::McapSink>(testFilePath, opts);
    DataTamer::ChannelsRegistry::Global().addDefaultSink(mcap_sink);

    // Create a channel
    std::shared_ptr<DataTamer::LogChannel> channelA = DataTamer::LogChannel::create("channelA");

    // Register a vector of doubles
    std::vector<double> v1(10, 0.5);
    channelA->registerValue("vector_10", &v1);

    // Take multiple snapshots over time
    for (size_t i = 0; i < 10; i++)
    {
        // Modify the data between snapshots
        for (auto& val : v1)
        {
            val += 0.1;
        }
        channelA->takeSnapshot();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // Ensure the file is created
    EXPECT_TRUE(std::filesystem::exists(testFilePath));

    // Stop recording
    mcap_sink->stopRecording();
}

// Test that restarting recording works and creates a new file
TEST_F(McapSinkTest, RestartRecordingCreatesNewFile)
{
    // Create the MCAP sink and add it to the registry
    mcap::McapWriterOptions opts("json");
    opts.compression = mcap::Compression::None;
    data_tamer_tools::McapSink::SharedPtr mcap_sink = std::make_shared<data_tamer_tools::McapSink>(testFilePath, opts);
    DataTamer::ChannelsRegistry::Global().addDefaultSink(mcap_sink);

    // Create a channel
    std::shared_ptr<DataTamer::LogChannel> channelA = DataTamer::LogChannel::create("channelA");

    // Register values to channelA
    std::vector<double> v1(10, 0.5);
    channelA->registerValue("vector_10", &v1);

    // Take a snapshot
    channelA->takeSnapshot();

    // Stop and restart recording to a new file
    std::string newFilePath = "new_test_sample.mcap";
    mcap_sink->restartRecording(newFilePath, opts);

    // Take another snapshot
    channelA->takeSnapshot();

    // Ensure both files were created
    EXPECT_TRUE(std::filesystem::exists(testFilePath));
    EXPECT_TRUE(std::filesystem::exists(newFilePath));

    // Clean up the new file
    std::filesystem::remove(newFilePath);

    // Stop recording
    mcap_sink->stopRecording();
}

TEST_F(McapSinkTest, StopThenRestartAllowsWriting)
{
    mcap::McapWriterOptions opts("protobuf");
    opts.compression = mcap::Compression::None;
    opts.noChunking = true;  // still buffers; flushed on close

    auto sink = std::make_shared<data_tamer_tools::McapSink>(testFilePath, opts);
    auto chan = DataTamer::LogChannel::create("restart_channel");
    chan->addDataSink(sink);

    double val = 1.23;
    chan->registerValue("scalar", &val);

    // 1) write one snapshot (still buffered on disk)
    chan->takeSnapshot();

    // 2) stopRecording flushes and closes the file -> now size should be > 0
    sink->stopRecording();
    ASSERT_TRUE(std::filesystem::exists(testFilePath));
    const auto size_after_close = std::filesystem::file_size(testFilePath);
    ASSERT_GT(size_after_close, 0u);

    // 3) Calls after stopRecording() should be ignored (no growth)
    val = 4.56;
    chan->takeSnapshot();  // should be ignored internally
    const auto size_after_ignored = std::filesystem::file_size(testFilePath);
    EXPECT_EQ(size_after_ignored, size_after_close) << "File grew after stopRecording()";

    // 4) restartRecording -> new file should get data again
    const std::string newFilePath = "/tmp/test_sample_restart.mcap";
    if (std::filesystem::exists(newFilePath))
        std::filesystem::remove(newFilePath);

    sink->restartRecording(newFilePath, opts);
    val = 7.89;
    chan->takeSnapshot();  // now accepted

    ASSERT_TRUE(std::filesystem::exists(newFilePath));
    // Close again to ensure flush before size check
    sink->stopRecording();
    const auto new_size = std::filesystem::file_size(newFilePath);
    EXPECT_GT(new_size, 0u);

    std::filesystem::remove(newFilePath);
}

TEST_F(McapSinkTest, ProtobufRoundTrip_Simple)
{
    // --- build schema (DT) ---
    DataTamer::Schema dt_schema;
    dt_schema.channel_name = "proto_ch";
    dt_schema.fields = {
        DataTamer::TypeField{ .field_name = "rpm", .type = DataTamer::BasicType::UINT32, .type_name = "uint32", .is_vector = false, .array_size = 0 },
        DataTamer::TypeField{ .field_name = "vals", .type = DataTamer::BasicType::FLOAT32, .type_name = "float32", .is_vector = true, .array_size = 0 },  // dynamic vec
    };
    // mimic writer hash
    for (const auto& f : dt_schema.fields)
    {
        dt_schema.hash = DataTamerParser::AddFieldToHash(
            DataTamerParser::TypeField{ f.field_name, (DataTamerParser::BasicType)f.type, f.type_name, f.is_vector, f.array_size }, dt_schema.hash);
    }

    // --- open protobuf sink ---
    auto opts = data_tamer_tools::McapSink::makeOptions(data_tamer_tools::McapSink::Format::Protobuf, data_tamer_tools::McapSink::Compression::None, /*chunk*/ 256);
    auto sink = std::make_shared<data_tamer_tools::McapSink>(testFilePath, opts, data_tamer_tools::McapSink::Format::Protobuf);

    auto ch = DataTamer::LogChannel::create("proto_ch");
    ch->addDataSink(sink);

    // register values
    uint32_t rpm = 1234;
    std::vector<float> vals = { 1.5f, -2.25f, 3.75f };
    ch->registerValue("rpm", &rpm);
    ch->registerValue("vals", &vals);

    ASSERT_TRUE(ch->takeSnapshot());
    // hate this, but sleep here because calling stopRecording means any outstanding snapshots won't get written
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    sink->stopRecording();
    ASSERT_TRUE(std::filesystem::exists(testFilePath));
    ASSERT_GT(std::filesystem::file_size(testFilePath), 0u);

    // --- read back with IndexedReader ---
    mcap::McapReader reader;
    mcap::Status st = reader.open(testFilePath);
    ASSERT_TRUE(st.ok()) << st.message;

    // expect 1 channel / 1 message
    const auto& sum = reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);
    ASSERT_EQ(reader.channels().size(), 1u);
    ASSERT_EQ(reader.schemas().size(), 1u);

    // pull the single message
    size_t msg_count = 0;
    std::string payload;

    auto view = reader.readMessages();
    for (const auto& rec : view)
    {
        payload.assign(reinterpret_cast<const char*>(rec.message.data), rec.message.dataSize);
        ++msg_count;
    }
    reader.close();
    ASSERT_EQ(msg_count, 1u);

    // --- rebuild parser schema + proto runtime like the sink did ---
    // Convert DT -> Parser schema (same logic as sink::toParserSchema)
    DataTamerParser::Schema p;
    p.channel_name = dt_schema.channel_name;
    p.hash = dt_schema.hash;
    for (const auto& f : dt_schema.fields)
    {
        DataTamerParser::TypeField tf;
        tf.field_name = f.field_name;
        tf.type = static_cast<DataTamerParser::BasicType>(f.type);
        tf.type_name = f.type_name;
        tf.is_vector = f.is_vector || (f.array_size > 0);
        tf.array_size = f.array_size;
        p.fields.push_back(tf);
    }

    auto rt = data_tamer_tools::buildProto(p);
    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);

    std::unique_ptr<google::protobuf::Message> msg(proto->New());
    ASSERT_TRUE(msg->ParseFromString(payload));

    const auto* refl = msg->GetReflection();
    auto fd_rpm = rt.desc->FindFieldByName("rpm");
    auto fd_vals = rt.desc->FindFieldByName("vals");
    ASSERT_NE(fd_rpm, nullptr);
    ASSERT_NE(fd_vals, nullptr);
    ASSERT_FALSE(fd_rpm->is_repeated());
    ASSERT_TRUE(fd_vals->is_repeated());

    EXPECT_EQ(refl->GetUInt32(*msg, fd_rpm), 1234u);
    ASSERT_EQ(refl->FieldSize(*msg, fd_vals), 3);
    EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, fd_vals, 0), 1.5f);
    EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, fd_vals, 1), -2.25f);
    EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, fd_vals, 2), 3.75f);
}

TEST_F(McapSinkTest, ProtobufRoundTrip_TwoChannels)
{
    using Format = data_tamer_tools::McapSink::Format;
    using Compression = data_tamer_tools::McapSink::Compression;

    // --- build DT schemas ---
    DataTamer::Schema schA;
    schA.channel_name = "proto_ch_A";
    schA.fields = {
        { .field_name = "rpm", .type = DataTamer::BasicType::UINT32, .type_name = "uint32", .is_vector = false, .array_size = 0 },
    };
    for (const auto& f : schA.fields)
        schA.hash = DataTamerParser::AddFieldToHash({ f.field_name, (DataTamerParser::BasicType)f.type, f.type_name, f.is_vector, f.array_size }, schA.hash);

    DataTamer::Schema schB;
    schB.channel_name = "proto_ch_B";
    schB.fields = {
        { .field_name = "temp", .type = DataTamer::BasicType::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
        { .field_name = "vals", .type = DataTamer::BasicType::FLOAT32, .type_name = "float32", .is_vector = true, .array_size = 0 },  // dyn vec
    };
    for (const auto& f : schB.fields)
        schB.hash = DataTamerParser::AddFieldToHash({ f.field_name, (DataTamerParser::BasicType)f.type, f.type_name, f.is_vector, f.array_size }, schB.hash);

    // --- sink (protobuf mode) ---
    auto opts = data_tamer_tools::McapSink::makeOptions(Format::Protobuf, Compression::None, 256);
    auto sink = std::make_shared<data_tamer_tools::McapSink>(testFilePath, opts, Format::Protobuf);

    // --- channels ---
    auto chA = DataTamer::LogChannel::create("proto_ch_A");
    auto chB = DataTamer::LogChannel::create("proto_ch_B");
    chA->addDataSink(sink);
    chB->addDataSink(sink);

    // Register values
    uint32_t rpm = 4242;
    chA->registerValue("rpm", &rpm);

    double temp = 18.75;
    std::vector<float> vals = { 0.5f, -1.25f, 2.0f };
    chB->registerValue("temp", &temp);
    chB->registerValue("vals", &vals);

    // Take snapshots
    chA->takeSnapshot();
    chB->takeSnapshot();

    // Ensure background writer flushed
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Stop & close
    sink->stopRecording();
    ASSERT_TRUE(std::filesystem::exists(testFilePath));
    ASSERT_GT(std::filesystem::file_size(testFilePath), 0u);

    // --- read back ---
    mcap::McapReader reader;
    auto st = reader.open(testFilePath);
    ASSERT_TRUE(st.ok()) << st.message;

    // A) proto_ch_A
    std::string payloadA;
    {
        mcap::ReadMessageOptions ro;
        ro.topicFilter = [](std::string_view t) { return t == "proto_ch_A"; };

        size_t count = 0;

        auto view = reader.readMessages(nullptr, ro);  // <- returns a LinearMessageView
        for (const auto& rec : view)
        {  // <- range-for over the view
            payloadA.assign(reinterpret_cast<const char*>(rec.message.data), rec.message.dataSize);
            ++count;
        }
        ASSERT_EQ(count, 1u);

        // ... keep payloadA around for assertions
    }

    // B) proto_ch_B
    std::string payloadB;
    {
        mcap::ReadMessageOptions ro;
        ro.topicFilter = [](std::string_view t) { return t == "proto_ch_B"; };

        size_t count = 0;

        auto view = reader.readMessages(nullptr, ro);
        for (const auto& rec : view)
        {
            payloadB.assign(reinterpret_cast<const char*>(rec.message.data), rec.message.dataSize);
            ++count;
        }
        ASSERT_EQ(count, 1u);

        // ... keep payloadB around for assertions
    }

    reader.close();

    // --- rebuild parser schema + proto for A and B, then verify ---
    auto toParser = [](const DataTamer::Schema& s)
    {
        DataTamerParser::Schema p;
        p.channel_name = s.channel_name;
        p.hash = s.hash;
        for (const auto& f : s.fields)
        {
            DataTamerParser::TypeField tf;
            tf.field_name = f.field_name;
            tf.type = (DataTamerParser::BasicType)f.type;
            tf.type_name = f.type_name;
            tf.is_vector = f.is_vector || (f.array_size > 0);
            tf.array_size = f.array_size;
            p.fields.push_back(tf);
        }
        return p;
    };

    // A
    {
        auto pA = toParser(schA);
        auto rtA = data_tamer_tools::buildProto(pA);
        const auto* proto = rtA.factory->GetPrototype(rtA.desc);
        ASSERT_NE(proto, nullptr);
        std::unique_ptr<google::protobuf::Message> msg(proto->New());
        ASSERT_TRUE(msg->ParseFromString(payloadA));
        const auto* fd_rpm = rtA.desc->FindFieldByName("rpm");
        ASSERT_NE(fd_rpm, nullptr);
        EXPECT_EQ(msg->GetReflection()->GetUInt32(*msg, fd_rpm), 4242u);
    }

    // B
    {
        auto pB = toParser(schB);
        auto rtB = data_tamer_tools::buildProto(pB);
        const auto* proto = rtB.factory->GetPrototype(rtB.desc);
        ASSERT_NE(proto, nullptr);
        std::unique_ptr<google::protobuf::Message> msg(proto->New());
        ASSERT_TRUE(msg->ParseFromString(payloadB));

        const auto* fd_temp = rtB.desc->FindFieldByName("temp");
        const auto* fd_vals = rtB.desc->FindFieldByName("vals");
        ASSERT_NE(fd_temp, nullptr);
        ASSERT_NE(fd_vals, nullptr);
        EXPECT_DOUBLE_EQ(msg->GetReflection()->GetDouble(*msg, fd_temp), 18.75);
        ASSERT_EQ(msg->GetReflection()->FieldSize(*msg, fd_vals), 3);
        EXPECT_FLOAT_EQ(msg->GetReflection()->GetRepeatedFloat(*msg, fd_vals, 0), 0.5f);
        EXPECT_FLOAT_EQ(msg->GetReflection()->GetRepeatedFloat(*msg, fd_vals, 1), -1.25f);
        EXPECT_FLOAT_EQ(msg->GetReflection()->GetRepeatedFloat(*msg, fd_vals, 2), 2.0f);
    }
}

TEST_F(McapSinkTest, ProtobufTwoSchemas_TwoChannels_RoundTrip)
{
    // ---------- Build two DT schemas (same domain, different layouts) ----------
    DataTamer::Schema s1;
    s1.channel_name = "proto_v1";
    s1.fields = {
        DataTamer::TypeField{ .field_name = "rpm", .type = DataTamer::BasicType::UINT32, .type_name = "uint32", .is_vector = false, .array_size = 0 },
    };
    for (const auto& f : s1.fields)
    {
        s1.hash = DataTamerParser::AddFieldToHash(DataTamerParser::TypeField{ f.field_name, (DataTamerParser::BasicType)f.type, f.type_name, f.is_vector, f.array_size },
                                                  s1.hash);
    }

    DataTamer::Schema s2;
    s2.channel_name = "proto_v2";
    s2.fields = {
        DataTamer::TypeField{ .field_name = "rpm", .type = DataTamer::BasicType::UINT32, .type_name = "uint32", .is_vector = false, .array_size = 0 },
        DataTamer::TypeField{ .field_name = "temp", .type = DataTamer::BasicType::FLOAT32, .type_name = "float32", .is_vector = false, .array_size = 0 },
    };
    for (const auto& f : s2.fields)
    {
        s2.hash = DataTamerParser::AddFieldToHash(DataTamerParser::TypeField{ f.field_name, (DataTamerParser::BasicType)f.type, f.type_name, f.is_vector, f.array_size },
                                                  s2.hash);
    }

    // ---------- Open sink in Protobuf mode and add both channels ----------
    auto opts = data_tamer_tools::McapSink::makeOptions(data_tamer_tools::McapSink::Format::Protobuf, data_tamer_tools::McapSink::Compression::None,
                                                        /*chunk*/ 256);
    auto sink = std::make_shared<data_tamer_tools::McapSink>(testFilePath, opts, data_tamer_tools::McapSink::Format::Protobuf);

    auto ch1 = DataTamer::LogChannel::create(s1.channel_name);
    auto ch2 = DataTamer::LogChannel::create(s2.channel_name);
    ch1->addDataSink(sink);
    ch2->addDataSink(sink);

    // Register values for v1
    uint32_t rpm_v1 = 1111;
    ch1->registerValue("rpm", &rpm_v1);

    // Register values for v2
    uint32_t rpm_v2 = 2222;
    float temp_v2 = 36.5f;
    ch2->registerValue("rpm", &rpm_v2);
    ch2->registerValue("temp", &temp_v2);

    // ---------- Take one snapshot for each ----------
    ch1->takeSnapshot();
    ch2->takeSnapshot();

    // Let the background sink thread flush its queue before closing.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    sink->stopRecording();

    ASSERT_TRUE(std::filesystem::exists(testFilePath));
    ASSERT_GT(std::filesystem::file_size(testFilePath), 0u);

    // ---------- Read back ----------
    mcap::McapReader reader;
    auto st = reader.open(testFilePath);
    ASSERT_TRUE(st.ok()) << st.message;

    const auto& summary = reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);
    // We expect two schemas and two channels.
    ASSERT_EQ(reader.schemas().size(), 2u);
    ASSERT_EQ(reader.channels().size(), 2u);

    // Helper to convert DT schema -> Parser schema (mirror sink logic)
    auto toParser = [](const DataTamer::Schema& ds)
    {
        DataTamerParser::Schema out;
        out.channel_name = ds.channel_name;
        out.hash = ds.hash;
        for (const auto& f : ds.fields)
        {
            DataTamerParser::TypeField tf;
            tf.field_name = f.field_name;
            tf.type = static_cast<DataTamerParser::BasicType>(f.type);
            tf.type_name = f.type_name;
            tf.is_vector = f.is_vector || (f.array_size > 0);
            tf.array_size = f.array_size;
            out.fields.push_back(tf);
        }
        return out;
    };

    auto p1 = toParser(s1);
    auto p2 = toParser(s2);
    auto rt1 = data_tamer_tools::buildProto(p1);
    auto rt2 = data_tamer_tools::buildProto(p2);

    // Collect messages per topic
    std::vector<std::pair<std::string, std::string>> msgs;  // (topic, payload_str)
    mcap::ReadMessageOptions ro;
    auto view = reader.readMessages(nullptr, ro);
    for (const auto& rec : view)
    {
        mcap::ChannelPtr ch = reader.channels().at(rec.message.channelId);
        std::string payload(reinterpret_cast<const char*>(rec.message.data), rec.message.dataSize);
        msgs.emplace_back(std::string(ch->topic), std::move(payload));
    }
    reader.close();

    // We wrote exactly 2 messages, one per topic
    ASSERT_EQ(msgs.size(), 2u);

    // Decode and assert
    for (const auto& [topic, payload] : msgs)
    {
        if (topic == s1.channel_name)
        {
            const google::protobuf::Message* proto = rt1.factory->GetPrototype(rt1.desc);
            ASSERT_NE(proto, nullptr);
            std::unique_ptr<google::protobuf::Message> msg(proto->New());
            ASSERT_TRUE(msg->ParseFromString(payload));
            const auto* refl = msg->GetReflection();
            auto* fd_rpm = rt1.desc->FindFieldByName("rpm");
            ASSERT_NE(fd_rpm, nullptr);
            ASSERT_FALSE(fd_rpm->is_repeated());
            EXPECT_EQ(refl->GetUInt32(*msg, fd_rpm), rpm_v1);
        }
        else if (topic == s2.channel_name)
        {
            const google::protobuf::Message* proto = rt2.factory->GetPrototype(rt2.desc);
            ASSERT_NE(proto, nullptr);
            std::unique_ptr<google::protobuf::Message> msg(proto->New());
            ASSERT_TRUE(msg->ParseFromString(payload));
            const auto* refl = msg->GetReflection();
            auto* fd_rpm = rt2.desc->FindFieldByName("rpm");
            auto* fd_temp = rt2.desc->FindFieldByName("temp");
            ASSERT_NE(fd_rpm, nullptr);
            ASSERT_NE(fd_temp, nullptr);
            EXPECT_EQ(refl->GetUInt32(*msg, fd_rpm), rpm_v2);
            EXPECT_FLOAT_EQ(refl->GetFloat(*msg, fd_temp), temp_v2);
        }
        else
        {
            FAIL() << "Unexpected topic in file: " << topic;
        }
    }
}

TEST_F(McapSinkTest, Stress_MultiChannelsHundredsSnapshots)
{
    using Format = data_tamer_tools::McapSink::Format;
    using Compression = data_tamer_tools::McapSink::Compression;

    // Protobuf writer options (small chunk to force a few chunks even in tests)
    auto opts = data_tamer_tools::McapSink::makeOptions(Format::Protobuf, Compression::None, /*chunk*/ 8 * 1024);
    auto sink = std::make_shared<data_tamer_tools::McapSink>(testFilePath, opts, Format::Protobuf);

    // --- Channel A: rpm:uint32, vals:float32[] (dynamic)
    auto chA = DataTamer::LogChannel::create("chA");
    chA->addDataSink(sink);
    uint32_t rpmA = 0;
    std::vector<float> valsA = { 1.f, 2.f, 3.f };
    chA->registerValue("rpm", &rpmA);
    chA->registerValue("vals", &valsA);

    // --- Channel B: flag:bool, big:uint64
    auto chB = DataTamer::LogChannel::create("chB");
    chB->addDataSink(sink);
    bool flagB = false;
    uint64_t bigB = 0;
    chB->registerValue("flag", &flagB);
    chB->registerValue("big", &bigB);

    // --- Fire a bunch of snapshots on both channels
    constexpr int N = 100;
    for (int i = 0; i < N; ++i)
    {
        // mutate a bit
        rpmA = static_cast<uint32_t>(1000 + i);
        valsA[0] += 0.1f;
        valsA[1] -= 0.05f;
        valsA[2] += 0.2f;

        flagB = (i % 2) == 0;
        bigB = 123456789ULL + static_cast<uint64_t>(i);

        chA->takeSnapshot();
        chB->takeSnapshot();
    }

    // Give the background writer thread time to drain its queue
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    sink->stopRecording();

    ASSERT_TRUE(std::filesystem::exists(testFilePath));
    ASSERT_GT(std::filesystem::file_size(testFilePath), 0u);

    // --- Read back and count per topic
    mcap::McapReader reader;
    auto st = reader.open(testFilePath);
    ASSERT_TRUE(st.ok()) << st.message;

    // We’ll count messages per topic and also sanity-parse payloads with the dynamic proto
    std::unordered_map<std::string, size_t> counts;
    std::string payload;

    // Build a small map: schema.id -> (desc/factory) by reconstructing runtimes per schema name
    // Note: our sink used dynamic protos, so schemas are protobuf FileDescriptorSets
    // For the test we just confirm message counts and that payloads aren’t empty; full decode is optional.
    auto view = reader.readMessages();
    for (const auto& rec : view)
    {
        mcap::ChannelPtr ch = reader.channel(rec.channel->id);
        ASSERT_NE(ch, nullptr);
        counts[std::string(ch->topic)]++;
        // basic payload presence check
        ASSERT_GT(rec.message.dataSize, 0u);
    }
    reader.close();

    ASSERT_EQ(counts["chA"], static_cast<size_t>(N));
    ASSERT_EQ(counts["chB"], static_cast<size_t>(N));
}

TEST_F(McapSinkTest, MixedFormats_SideBySide)
{
    // ---------- build DT schema ----------
    DataTamer::Schema dt_schema;
    dt_schema.channel_name = "mixed_ch";
    dt_schema.fields = {
        // rpm: uint32, vals: float32[]
        DataTamer::TypeField{ .field_name = "rpm", .type = DataTamer::BasicType::UINT32, .type_name = "uint32", .is_vector = false, .array_size = 0 },
        DataTamer::TypeField{ .field_name = "vals", .type = DataTamer::BasicType::FLOAT32, .type_name = "float32", .is_vector = true, .array_size = 0 },  // dynamic
    };
    for (const auto& f : dt_schema.fields)
    {
        dt_schema.hash = DataTamerParser::AddFieldToHash(
            DataTamerParser::TypeField{ f.field_name, (DataTamerParser::BasicType)f.type, f.type_name, f.is_vector, f.array_size }, dt_schema.hash);
    }

    // ---------- two sinks: JSON + Protobuf ----------
    const std::string jsonPath = "/tmp/mixed_json.mcap";
    const std::string pbPath = "/tmp/mixed_pb.mcap";
    if (std::filesystem::exists(jsonPath))
        std::filesystem::remove(jsonPath);
    if (std::filesystem::exists(pbPath))
        std::filesystem::remove(pbPath);

    auto jsonOpts = data_tamer_tools::McapSink::makeOptions(data_tamer_tools::McapSink::Format::Json, data_tamer_tools::McapSink::Compression::None,
                                                            /*chunk*/ 256);

    auto pbOpts = data_tamer_tools::McapSink::makeOptions(data_tamer_tools::McapSink::Format::Protobuf, data_tamer_tools::McapSink::Compression::None,
                                                          /*chunk*/ 256);

    auto jsonSink = std::make_shared<data_tamer_tools::McapSink>(jsonPath, jsonOpts, data_tamer_tools::McapSink::Format::Json);
    auto pbSink = std::make_shared<data_tamer_tools::McapSink>(pbPath, pbOpts, data_tamer_tools::McapSink::Format::Protobuf);

    // ---------- a single channel feeds both sinks ----------
    auto ch = DataTamer::LogChannel::create(dt_schema.channel_name);
    ch->addDataSink(jsonSink);
    ch->addDataSink(pbSink);

    // register values
    uint32_t rpm = 4321;
    std::vector<float> vals = { 0.25f, -1.0f, 2.5f };
    ch->registerValue("rpm", &rpm);
    ch->registerValue("vals", &vals);

    // snapshot once
    ch->takeSnapshot();
    // give the async sink thread a moment to drain
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // stop both
    jsonSink->stopRecording();
    pbSink->stopRecording();

    ASSERT_TRUE(std::filesystem::exists(jsonPath));
    ASSERT_TRUE(std::filesystem::exists(pbPath));
    ASSERT_GT(std::filesystem::file_size(jsonPath), 0u);
    ASSERT_GT(std::filesystem::file_size(pbPath), 0u);

    // ---------- read back JSON file ----------
    {
        mcap::McapReader r;
        auto st = r.open(jsonPath);
        ASSERT_TRUE(st.ok()) << st.message;
        const auto& sum = r.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);
        ASSERT_EQ(r.channels().size(), 1u);
        ASSERT_EQ(r.schemas().size(), 1u);
        ASSERT_EQ(r.channels().begin()->second->messageEncoding, std::string("json"));

        size_t jsonMsgs = 0;
        std::string jsonPayload;
        mcap::ReadMessageOptions ro;
        ro.topicFilter = [&](std::string_view t) { return t == dt_schema.channel_name; };
        for (auto view = r.readMessages(nullptr, ro); auto const& rec : view)
        {
            jsonPayload.assign(reinterpret_cast<const char*>(rec.message.data), rec.message.dataSize);
            ++jsonMsgs;
        }
        r.close();
        ASSERT_EQ(jsonMsgs, 1u);

        // quick JSON sanity (fields exist & types look right)
        auto j = nlohmann::json::parse(jsonPayload);
        ASSERT_TRUE(j.contains("rpm"));
        ASSERT_TRUE(j.contains("vals"));
        EXPECT_EQ(j["rpm"].get<uint32_t>(), rpm);
        ASSERT_EQ(j["vals"].size(), vals.size());
        EXPECT_FLOAT_EQ(j["vals"][0].get<float>(), vals[0]);
        EXPECT_FLOAT_EQ(j["vals"][1].get<float>(), vals[1]);
        EXPECT_FLOAT_EQ(j["vals"][2].get<float>(), vals[2]);
    }

    // ---------- read back Protobuf file ----------
    {
        mcap::McapReader r;
        auto st = r.open(pbPath);
        ASSERT_TRUE(st.ok()) << st.message;
        const auto& sum = r.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);
        ASSERT_EQ(r.channels().size(), 1u);
        ASSERT_EQ(r.schemas().size(), 1u);
        ASSERT_EQ(r.channels().begin()->second->messageEncoding, std::string("protobuf"));

        size_t pbMsgs = 0;
        std::string pbPayload;
        mcap::ReadMessageOptions ro;
        ro.topicFilter = [&](std::string_view t) { return t == dt_schema.channel_name; };
        for (auto view = r.readMessages(nullptr, ro); auto const& rec : view)
        {
            pbPayload.assign(reinterpret_cast<const char*>(rec.message.data), rec.message.dataSize);
            ++pbMsgs;
        }
        r.close();
        ASSERT_EQ(pbMsgs, 1u);

        // rebuild parser schema + proto runtime like sink does
        DataTamerParser::Schema p;
        p.channel_name = dt_schema.channel_name;
        p.hash = dt_schema.hash;
        for (const auto& f : dt_schema.fields)
        {
            DataTamerParser::TypeField tf;
            tf.field_name = f.field_name;
            tf.type = static_cast<DataTamerParser::BasicType>(f.type);
            tf.type_name = f.type_name;
            tf.is_vector = f.is_vector || (f.array_size > 0);
            tf.array_size = f.array_size;
            p.fields.push_back(tf);
        }

        auto rt = data_tamer_tools::buildProto(p);
        const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
        ASSERT_NE(proto, nullptr);

        std::unique_ptr<google::protobuf::Message> msg(proto->New());
        ASSERT_TRUE(msg->ParseFromString(pbPayload));

        const auto* refl = msg->GetReflection();
        auto fd_rpm = rt.desc->FindFieldByName("rpm");
        auto fd_vals = rt.desc->FindFieldByName("vals");
        ASSERT_NE(fd_rpm, nullptr);
        ASSERT_NE(fd_vals, nullptr);
        ASSERT_FALSE(fd_rpm->is_repeated());
        ASSERT_TRUE(fd_vals->is_repeated());

        EXPECT_EQ(refl->GetUInt32(*msg, fd_rpm), rpm);
        ASSERT_EQ(refl->FieldSize(*msg, fd_vals), static_cast<int>(vals.size()));
        EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, fd_vals, 0), vals[0]);
        EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, fd_vals, 1), vals[1]);
        EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, fd_vals, 2), vals[2]);
    }
}

class McapSinkRotateNoRosTest : public ::testing::Test
{
  protected:
    void TearDown() override
    {
        for (auto& p : trash_)
        {
            std::error_code ec;
            std::filesystem::remove_all(p, ec);
        }
    }
    std::vector<std::filesystem::path> trash_;
    static void sleep_a_tick()
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(120));
    }
};

TEST_F(McapSinkRotateNoRosTest, RotateToNewDir_WritesThere)
{
    namespace fs = std::filesystem;
    fs::path dir1 = fs::temp_directory_path() / "mcap_rotate_noros_dir1";
    fs::path dir2 = fs::temp_directory_path() / "mcap_rotate_noros_dir2";
    fs::create_directories(dir1);
    trash_.push_back(dir1);
    trash_.push_back(dir2);

    const std::string basefile = "rosout.mcap";
    fs::path initial_path = dir1 / basefile;

    auto sink =
        std::make_shared<data_tamer_tools::McapSink>(initial_path.string(), data_tamer_tools::McapSink::Format::Json, data_tamer_tools::McapSink::Compression::None,
                                                     /*chunk_size*/ 0);

    auto chan = DataTamer::LogChannel::create("ch_json_rotate_noros");
    chan->addDataSink(sink);

    uint32_t rpm = 100;
    chan->registerValue("rpm", &rpm);

    // initial write -> dir1/rosout.mcap
    chan->takeSnapshot();
    sink->stopRecording();
    sleep_a_tick();
    ASSERT_TRUE(fs::exists(initial_path));
    auto size_before = fs::file_size(initial_path);
    ASSERT_GT(size_before, 0u);

    // rotate to dir2
    sink->rotateToDirectory(dir2.string());

    // next write -> dir2/rosout.mcap
    rpm = 1234;
    chan->takeSnapshot();
    sink->stopRecording();
    sleep_a_tick();

    fs::path rotated_path = dir2 / basefile;
    ASSERT_TRUE(fs::exists(rotated_path));
    auto size_after = fs::file_size(rotated_path);
    ASSERT_GT(size_after, 0u);
}

TEST_F(McapSinkRotateNoRosTest, RotateBeforeFirstWrite)
{
    namespace fs = std::filesystem;
    fs::path dirA = fs::temp_directory_path() / "mcap_rotate_noros_pre_A";
    fs::path dirB = fs::temp_directory_path() / "mcap_rotate_noros_pre_B";
    fs::create_directories(dirA);
    fs::create_directories(dirB);
    trash_.push_back(dirA);
    trash_.push_back(dirB);

    const std::string basefile = "rosout.mcap";
    fs::path initial_path = dirA / basefile;

    auto sink =
        std::make_shared<data_tamer_tools::McapSink>(initial_path.string(), data_tamer_tools::McapSink::Format::Json, data_tamer_tools::McapSink::Compression::None,
                                                     /*chunk_size*/ 0);

    auto chan = DataTamer::LogChannel::create("ch_json_pre_noros");
    chan->addDataSink(sink);

    // rotate immediately
    sink->rotateToDirectory(dirB.string());

    // first write should go to dirB
    uint32_t rpm = 42;
    chan->registerValue("rpm", &rpm);
    chan->takeSnapshot();
    sink->stopRecording();
    sleep_a_tick();

    fs::path bpath = dirB / basefile;
    ASSERT_TRUE(fs::exists(bpath));
    ASSERT_GT(fs::file_size(bpath), 0u);
}
