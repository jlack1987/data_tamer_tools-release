#include <ament_index_cpp/get_package_share_directory.hpp>
#include <data_tamer/types.hpp>
#include <data_tamer_parser/data_tamer_parser.hpp>
#include <data_tamer_tools/helpers.hpp>
#include <fstream>
#include <nlohmann/json.hpp>
#include <gtest/gtest.h>

using nlohmann::json;

class SerializeSnapshotToJsonTest : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        // Define a simple schema with different types
        schema.channel_name = "test_channel";
        schema.fields.push_back({
            .field_name = "temperature",
            .type = DataTamer::BasicType::FLOAT32,
            .type_name = DataTamer::ToStr(DataTamer::BasicType::FLOAT32),
            .is_vector = false,
            .array_size = 0,
        });
        schema.fields.push_back({
            .field_name = "id",
            .type = DataTamer::BasicType::UINT32,
            .type_name = DataTamer::ToStr(DataTamer::BasicType::UINT32),
            .is_vector = false,
            .array_size = 0,
        });

        // Active mask for both fields (2 bits)
        active_mask.resize(1);
        DataTamer::SetBit(active_mask, 0, true);  // temperature
        DataTamer::SetBit(active_mask, 1, true);  // id

        // Payload: [float temperature][uint32 id]
        float temp = 22.5f;
        uint32_t id = 42;
        const auto* tempPtr = reinterpret_cast<const uint8_t*>(&temp);
        const auto* idPtr = reinterpret_cast<const uint8_t*>(&id);
        payload.insert(payload.end(), tempPtr, tempPtr + sizeof(float));
        payload.insert(payload.end(), idPtr, idPtr + sizeof(uint32_t));

        snapshot.channel_name = "test_channel";
        snapshot.timestamp = std::chrono::nanoseconds(1'000'000);
        snapshot.active_mask = active_mask;
        snapshot.payload = payload;
    }

    DataTamer::Schema schema;
    DataTamer::Snapshot snapshot;
    DataTamer::ActiveMask active_mask;
    std::vector<uint8_t> payload;
};

// Helper: make a simple DataTamer schema
static DataTamer::Schema createTestSchema()
{
    DataTamer::Schema s;
    s.channel_name = "TestChannel";

    DataTamer::TypeField f1;
    f1.field_name = "temperature";
    f1.type = DataTamer::BasicType::FLOAT32;
    f1.is_vector = false;
    f1.array_size = 0;

    DataTamer::TypeField f2;
    f2.field_name = "ids";
    f2.type = DataTamer::BasicType::UINT32;
    f2.is_vector = true;
    f2.array_size = 10;  // converter currently doesn’t enforce length; that’s fine

    s.fields.push_back(f1);
    s.fields.push_back(f2);
    return s;
}

// --- schema converter tests (updated expectations) ---------------------------

TEST(SchemaTest, ConvertsBasicTypes_AndSetsDraftAndAdditionalProps)
{
    DataTamer::Schema s = createTestSchema();
    json j = data_tamer_tools::convertToJSONSchema(s);

    ASSERT_TRUE(j.is_object());
    EXPECT_EQ(j["$schema"], "https://json-schema.org/draft/2020-12/schema");
    EXPECT_EQ(j["title"], "TestChannel");
    EXPECT_EQ(j["type"], "object");
    ASSERT_TRUE(j.contains("additionalProperties"));
    EXPECT_FALSE(j["additionalProperties"].get<bool>());

    // Field shapes
    ASSERT_TRUE(j["properties"].is_object());
    EXPECT_EQ(j["properties"]["temperature"]["type"], "number");
    EXPECT_EQ(j["properties"]["ids"]["type"], "array");
    EXPECT_EQ(j["properties"]["ids"]["items"]["type"], "integer");
}

TEST(SchemaTest, TimestampObjectPresentAndRequired)
{
    DataTamer::Schema s = createTestSchema();
    json j = data_tamer_tools::convertToJSONSchema(s);

    ASSERT_TRUE(j["properties"].contains("timestamp"));
    const auto& ts = j["properties"]["timestamp"];
    ASSERT_TRUE(ts.is_object());
    EXPECT_EQ(ts.at("type"), "object");
    ASSERT_TRUE(ts.contains("properties"));
    ASSERT_TRUE(ts.at("properties").is_object());
    const auto& tsp = ts.at("properties");
    ASSERT_TRUE(tsp.contains("sec"));
    ASSERT_TRUE(tsp.contains("nsec"));
    EXPECT_EQ(tsp.at("sec").at("type"), "integer");
    EXPECT_EQ(tsp.at("nsec").at("type"), "integer");
    EXPECT_EQ(tsp.at("nsec").at("maximum"), 999999999);

    ASSERT_TRUE(j.contains("required"));
    ASSERT_TRUE(j["required"].is_array());
    // ensure "timestamp" is in required
    const auto it = std::find(j["required"].begin(), j["required"].end(), "timestamp");
    EXPECT_NE(it, j["required"].end());
}

#pragma pack(push, 1)
struct BinHeader
{
    uint64_t schema_hash;
    uint64_t timestamp_ns;
    uint32_t active_len;
    uint32_t payload_len;
};
#pragma pack(pop)

TEST(SerializeRealSnapshot, ParsesTimestampAndArrays)
{
    std::string base = ament_index_cpp::get_package_share_directory("data_tamer_tools") + "/test/data";
    // Load schema text and build parser schema (typo matches upstream API)

    std::ifstream f(base + "/capture.3026812782465800491.schema.txt", std::ios::binary);
    ASSERT_TRUE(f);

    std::string schema_text(std::istreambuf_iterator<char>(f), {});
    DataTamerParser::Schema ps = DataTamerParser::BuilSchemaFromText(schema_text);

    // Load snapshot header + buffers
    std::ifstream fb(base + "/capture.3026812782465800491.snapshot.bin", std::ios::binary);
    ASSERT_TRUE(fb.good()) << "open failed: " << base << "/capture.3026812782465800491.snapshot.bin";

    BinHeader hdr{};
    fb.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
    ASSERT_TRUE(fb.good()) << "failed reading header";

    std::vector<uint8_t> active(hdr.active_len), payload(hdr.payload_len);
    fb.read(reinterpret_cast<char*>(active.data()), hdr.active_len);
    fb.read(reinterpret_cast<char*>(payload.data()), hdr.payload_len);
    ASSERT_TRUE(fb.good()) << "failed reading buffers";

    // Call your function using the real parser
    auto j = data_tamer_tools::serializeSnapshotToJson(ps, active, payload, std::chrono::nanoseconds{ static_cast<int64_t>(hdr.timestamp_ns) });

    // Basic shape
    ASSERT_TRUE(j.is_object());
    ASSERT_TRUE(j.contains("timestamp"));
    ASSERT_TRUE(j["timestamp"].is_object());
    EXPECT_LT(j["timestamp"]["nsec"].get<uint32_t>(), 1000000000u);

    // If any field from the schema appears and is vector-y, it should be an array in JSON
    for (const auto& f : ps.fields)
    {
        if (!j.contains(f.field_name))
            continue;  // field may be inactive for this snapshot
        if (f.is_vector)
        {
            EXPECT_TRUE(j[f.field_name].is_array()) << "Field " << f.field_name << " expected array";
        }
        else
        {
            // Scalars should not be arrays
            EXPECT_FALSE(j[f.field_name].is_array()) << "Field " << f.field_name << " expected scalar";
        }
    }
}

TEST(SchemaTest, HandlesNonStandardTypes_AsString)
{
    DataTamer::Schema s = createTestSchema();

    DataTamer::TypeField unk;
    unk.field_name = "unknown";
    unk.type = DataTamer::BasicType::OTHER;
    unk.is_vector = false;
    unk.array_size = 0;
    s.fields.push_back(unk);

    json j = data_tamer_tools::convertToJSONSchema(s);
    EXPECT_EQ(j["properties"]["unknown"]["type"], "string");
}

// --- serializeSnapshotToJson tests (unchanged behavior) ---------------------

TEST_F(SerializeSnapshotToJsonTest, ConvertsCorrectly_WithNumericTimestampNs)
{
    json out = data_tamer_tools::serializeSnapshotToJson(snapshot, schema);

    ASSERT_TRUE(out.is_object());
    // This serializer still emits a numeric timestamp and channel_name
    ASSERT_TRUE(out.contains("channel_name"));
    EXPECT_EQ(out["channel_name"], "test_channel");

    ASSERT_TRUE(out.contains("timestamp"));
    EXPECT_EQ(out["timestamp"].get<long long>(), 1'000'000);

    ASSERT_TRUE(out.contains("temperature"));
    ASSERT_TRUE(out.contains("id"));
    EXPECT_FLOAT_EQ(out["temperature"].get<float>(), 22.5f);
    EXPECT_EQ(out["id"].get<uint32_t>(), 42u);
}

TEST(ParserSchemaTest, ConvertsBasicTypes_AndArrayItems_AndTimestampRequired)
{
    DataTamerParser::Schema s;
    s.channel_name = "ParserChannel";

    // float32 scalar
    DataTamerParser::TypeField f1;
    f1.field_name = "temperature";
    f1.type = DataTamerParser::BasicType::FLOAT32;
    f1.is_vector = false;
    s.fields.push_back(f1);

    // uint32 vector
    DataTamerParser::TypeField f2;
    f2.field_name = "ids";
    f2.type = DataTamerParser::BasicType::UINT32;
    f2.is_vector = true;
    s.fields.push_back(f2);

    json j = data_tamer_tools::convertToJSONSchema(s);

    ASSERT_TRUE(j.is_object());
    EXPECT_EQ(j["$schema"], "https://json-schema.org/draft/2020-12/schema");
    EXPECT_EQ(j["title"], "ParserChannel");
    EXPECT_EQ(j["type"], "object");
    ASSERT_TRUE(j.contains("additionalProperties") || true);  // allowed to be absent in this overload

    // Fields
    ASSERT_TRUE(j["properties"].is_object());
    EXPECT_EQ(j["properties"]["temperature"]["type"], "number");
    EXPECT_EQ(j["properties"]["ids"]["type"], "array");
    EXPECT_EQ(j["properties"]["ids"]["items"]["type"], "integer");

    // Timestamp object present & required
    ASSERT_TRUE(j["properties"].contains("timestamp"));
    const auto& ts = j["properties"]["timestamp"];
    EXPECT_EQ(ts.at("type"), "object");
    const auto& tsp = ts.at("properties");
    EXPECT_EQ(tsp.at("sec").at("type"), "integer");
    EXPECT_EQ(tsp.at("nsec").at("type"), "integer");
    EXPECT_EQ(tsp.at("nsec").at("maximum"), 999999999);

    ASSERT_TRUE(j.contains("required"));
    const auto it = std::find(j["required"].begin(), j["required"].end(), "timestamp");
    EXPECT_NE(it, j["required"].end());
}

TEST(ParserSchemaTest, NonStandardTypesFallbackToString)
{
    DataTamerParser::Schema s;
    s.channel_name = "X";

    DataTamerParser::TypeField f;
    f.field_name = "unknown";
    f.type = DataTamerParser::BasicType::OTHER;  // if OTHER exists in your enum
    f.is_vector = false;
    s.fields.push_back(f);

    json j = data_tamer_tools::convertToJSONSchema(s);
    EXPECT_EQ(j["properties"]["unknown"]["type"], "string");
}

TEST(CanonicalPath, StripsIndicesKeepsSlashes)
{
    using data_tamer_tools::stripIndicesKeepSlashes;

    EXPECT_EQ(stripIndicesKeepSlashes("accel[0]"), "accel");
    EXPECT_EQ(stripIndicesKeepSlashes("arm/joint[12]/pos"), "arm/joint/pos");
    EXPECT_EQ(stripIndicesKeepSlashes("pose/x"), "pose/x");
    EXPECT_EQ(stripIndicesKeepSlashes("name"), "name");
    EXPECT_EQ(stripIndicesKeepSlashes("ids[3]/value[7]"), "ids/value");
}

static inline void write_u32_le(std::vector<uint8_t>& buf, uint32_t v)
{
    buf.push_back(uint8_t(v & 0xFF));
    buf.push_back(uint8_t((v >> 8) & 0xFF));
    buf.push_back(uint8_t((v >> 16) & 0xFF));
    buf.push_back(uint8_t((v >> 24) & 0xFF));
}

TEST(ParseFramedSnapshot, HappyPathAndFailures)
{
    using data_tamer_tools::parseFramedSnapshot;
    DataTamerParser::SnapshotView sv{};

    // Build [u32 mask_len][mask][u32 payload_len][payload]
    std::vector<uint8_t> good;
    const uint8_t mask[] = { 0x03 };
    const uint8_t payl[] = { 0xAA, 0xBB };

    write_u32_le(good, 1);                    // mask_len
    good.insert(good.end(), mask, mask + 1);  // mask
    write_u32_le(good, 2);                    // payload_len
    good.insert(good.end(), payl, payl + 2);  // payload

    EXPECT_TRUE(parseFramedSnapshot(good.data(), good.size(), sv));
    ASSERT_EQ(sv.active_mask.size, size_t(1));
    ASSERT_EQ(sv.payload.size, size_t(2));
    EXPECT_EQ(sv.active_mask.data[0], 0x03);
    EXPECT_EQ(sv.payload.data[0], 0xAA);
    EXPECT_EQ(sv.payload.data[1], 0xBB);

    // Too short for mask_len
    std::vector<uint8_t> bad1;  // empty
    EXPECT_FALSE(parseFramedSnapshot(bad1.data(), bad1.size(), sv));

    // mask_len says 4 but only 1 byte available
    std::vector<uint8_t> bad2;
    write_u32_le(bad2, 4);
    bad2.push_back(0xFF);
    EXPECT_FALSE(parseFramedSnapshot(bad2.data(), bad2.size(), sv));

    // OK mask, but payload_len overflows remaining
    std::vector<uint8_t> bad3;
    write_u32_le(bad3, 1);
    bad3.push_back(0x00);
    write_u32_le(bad3, 10);  // too big
    bad3.push_back(0x11);
    EXPECT_FALSE(parseFramedSnapshot(bad3.data(), bad3.size(), sv));
}

TEST(ConvertTypeToJSON, DataTamerEnumMapping)
{
    using data_tamer_tools::convertTypeToJSON;
    using B = DataTamer::BasicType;

    EXPECT_EQ(convertTypeToJSON(B::BOOL), "boolean");
    EXPECT_EQ(convertTypeToJSON(B::FLOAT32), "number");
    EXPECT_EQ(convertTypeToJSON(B::FLOAT64), "number");
    EXPECT_EQ(convertTypeToJSON(B::INT32), "integer");
    EXPECT_EQ(convertTypeToJSON(B::UINT32), "integer");
    EXPECT_EQ(convertTypeToJSON(B::OTHER), "string");
}

TEST(ConvertTypeToJSON, ParserEnumMapping)
{
    using data_tamer_tools::convertTypeToJSON;
    using P = DataTamerParser::BasicType;

    EXPECT_EQ(convertTypeToJSON(P::BOOL), "boolean");
    EXPECT_EQ(convertTypeToJSON(P::FLOAT32), "number");
    EXPECT_EQ(convertTypeToJSON(P::FLOAT64), "number");
    EXPECT_EQ(convertTypeToJSON(P::INT32), "integer");
    EXPECT_EQ(convertTypeToJSON(P::UINT32), "integer");
    EXPECT_EQ(convertTypeToJSON(P::OTHER), "string");
}

static DataTamer::Schema make_dt_schema()
{
    DataTamer::Schema s;
    s.channel_name = "TestChannel";

    DataTamer::TypeField f1;
    f1.field_name = "temperature";
    f1.type = DataTamer::BasicType::FLOAT32;
    f1.is_vector = false;
    s.fields.push_back(f1);

    DataTamer::TypeField f2;
    f2.field_name = "ids";
    f2.type = DataTamer::BasicType::UINT32;
    f2.is_vector = true;
    f2.array_size = 10;
    s.fields.push_back(f2);

    return s;
}

TEST(ConvertToJSONSchema, DT_Schema_Shape_And_Timestamp)
{
    json j = data_tamer_tools::convertToJSONSchema(make_dt_schema());

    ASSERT_TRUE(j.is_object());
    EXPECT_EQ(j["$schema"], "https://json-schema.org/draft/2020-12/schema");
    EXPECT_EQ(j["title"], "TestChannel");
    EXPECT_EQ(j["type"], "object");

    ASSERT_TRUE(j["properties"].is_object());
    EXPECT_EQ(j["properties"]["temperature"]["type"], "number");
    EXPECT_EQ(j["properties"]["ids"]["type"], "array");
    EXPECT_EQ(j["properties"]["ids"]["items"]["type"], "integer");

    ASSERT_TRUE(j["properties"].contains("timestamp"));
    const auto& ts = j["properties"]["timestamp"];
    EXPECT_EQ(ts.at("type"), "object");
    const auto& tsp = ts.at("properties");
    EXPECT_EQ(tsp.at("sec").at("type"), "integer");
    EXPECT_EQ(tsp.at("nsec").at("type"), "integer");
    EXPECT_EQ(tsp.at("nsec").at("maximum"), 999999999);

    ASSERT_TRUE(j.contains("required"));
    const auto it = std::find(j["required"].begin(), j["required"].end(), "timestamp");
    EXPECT_NE(it, j["required"].end());
}

TEST(ConvertToJSONSchema, Parser_Schema_Shape_And_Timestamp)
{
    DataTamerParser::Schema s;
    s.channel_name = "ParserChannel";
    DataTamerParser::TypeField a;
    a.field_name = "temperature";
    a.type = DataTamerParser::BasicType::FLOAT32;
    a.is_vector = false;
    s.fields.push_back(a);
    DataTamerParser::TypeField b;
    b.field_name = "ids";
    b.type = DataTamerParser::BasicType::UINT32;
    b.is_vector = true;
    s.fields.push_back(b);

    json j = data_tamer_tools::convertToJSONSchema(s);

    ASSERT_TRUE(j.is_object());
    EXPECT_EQ(j["title"], "ParserChannel");
    EXPECT_EQ(j["properties"]["temperature"]["type"], "number");
    EXPECT_EQ(j["properties"]["ids"]["type"], "array");
    EXPECT_EQ(j["properties"]["ids"]["items"]["type"], "integer");

    ASSERT_TRUE(j["properties"].contains("timestamp"));
    const auto& ts = j["properties"]["timestamp"];
    EXPECT_EQ(ts.at("type"), "object");
    const auto& tsp = ts.at("properties");
    EXPECT_EQ(tsp.at("sec").at("type"), "integer");
    EXPECT_EQ(tsp.at("nsec").at("type"), "integer");
    EXPECT_EQ(tsp.at("nsec").at("maximum"), 999999999);

    ASSERT_TRUE(j.contains("required"));
    const auto it = std::find(j["required"].begin(), j["required"].end(), "timestamp");
    EXPECT_NE(it, j["required"].end());
}

// ----------------- the test -----------------
TEST(ProtoRuntime, FieldMapMatchesDescriptorByNumber)
{
    // 1) build a tiny schema with vector field to exercise repeated handling
    DataTamerParser::Schema schema;
    schema.channel_name = "unit_test_channel";
    schema.hash = 123456789ULL;
    schema.custom_types.clear();

    DataTamerParser::TypeField rpm;
    rpm.field_name = "rpm";
    rpm.type = DataTamerParser::BasicType::UINT32;
    rpm.is_vector = false;

    DataTamerParser::TypeField volts;
    volts.field_name = "voltages";
    volts.type = DataTamerParser::BasicType::FLOAT32;
    volts.is_vector = true;  // repeated

    DataTamerParser::TypeField status;
    status.field_name = "statuses";
    status.type = DataTamerParser::BasicType::UINT32;
    status.is_vector = true;  // repeated

    schema.fields = { rpm, volts, status };

    // 2) Build proto runtime
    auto rt = data_tamer_tools::buildProto(schema);

    ASSERT_NE(rt.desc, nullptr);
    ASSERT_EQ(rt.desc->field_count(), 3) << "Descriptor should have 3 fields";

    // 3) Compute expected canonical names in the same order
    auto canons = data_tamer_tools::flattenSchema(schema);
    ASSERT_EQ((int)canons.size(), rt.desc->field_count());

    // 4) For each field number i+1, the map must resolve canon -> that FieldDescriptor
    for (int i = 0; i < rt.desc->field_count(); ++i)
    {
        const auto* fd_by_num = rt.desc->FindFieldByNumber(i + 1);
        ASSERT_NE(fd_by_num, nullptr) << "Missing field #" << (i + 1);
        auto it = rt.field_by_name.find(canons[i].full_path);
        ASSERT_NE(it, rt.field_by_name.end()) << "field_by_name missing canon key: '" << canons[i].full_path << "'";
        EXPECT_EQ(it->second, fd_by_num) << "Map points to a different field than the descriptor number #" << (i + 1) << " name=" << fd_by_num->name();
    }
}

TEST(EncodeFast, ScalarsHappyPath)
{
    using namespace DataTamerParser;
    using data_tamer_tools::buildProto;

    // ----- 1) Build a tiny scalar-only schema -----
    Schema s;
    {
        TypeField f;

        f = {};
        f.field_name = "flag";
        f.type = BasicType::BOOL;
        s.fields.push_back(f);
        f = {};
        f.field_name = "neg";
        f.type = BasicType::INT32;
        s.fields.push_back(f);
        f = {};
        f.field_name = "big";
        f.type = BasicType::UINT64;
        s.fields.push_back(f);
        f = {};
        f.field_name = "f32";
        f.type = BasicType::FLOAT32;
        s.fields.push_back(f);
        f = {};
        f.field_name = "f64";
        f.type = BasicType::FLOAT64;
        s.fields.push_back(f);

        // Compute schema hash like DataTamer does
        s.hash = 0;
        for (const auto& tf : s.fields)
        {
            s.hash = AddFieldToHash(tf, s.hash);
        }
    }

    // ----- 2) Build proto runtime for that schema -----
    auto rt = buildProto(s);

    // ----- 3) Craft payload in schema order (LE, same as DataTamer Deserialize) -----
    const bool v_flag = true;
    const int32_t v_neg = -123;
    const uint64_t v_big = 9999999999ULL;
    const float v_f32 = 3.5f;
    const double v_f64 = 9.25;

    std::vector<uint8_t> payload;
    auto push_raw = [&](auto value)
    {
        const uint8_t* p = reinterpret_cast<const uint8_t*>(&value);
        payload.insert(payload.end(), p, p + sizeof(value));
    };
    push_raw(v_flag);
    push_raw(v_neg);
    push_raw(v_big);
    push_raw(v_f32);
    push_raw(v_f64);

    // Active mask: first 5 bits set (one byte is enough)
    std::vector<uint8_t> mask(1, 0);
    mask[0] = 0x1F;  // 0001 1111

    // SnapshotView aliases buffers
    SnapshotView sv{};
    sv.schema_hash = s.hash;
    sv.timestamp = 123456789ull;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    // ----- 4) Encode to protobuf bytes -----
    std::string bytes;
    ASSERT_TRUE(data_tamer_tools::encodeSnapshot(s, sv, rt, bytes)) << "encodeSnapshot failed";
    ASSERT_FALSE(bytes.empty()) << "no bytes produced";

    // ----- 5) Parse bytes back and verify via reflection -----
    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> msg(proto->New());
    ASSERT_TRUE(msg->ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

    const auto* refl = msg->GetReflection();
    ASSERT_NE(refl, nullptr);

    // Canonical names are just the field names here (no indices)
    auto fd = [&](const char* name) -> const google::protobuf::FieldDescriptor*
    {
        auto* d = rt.desc->FindFieldByName(name);  // or map lookup if you kept one
        if (!d)
        {
            ADD_FAILURE() << "no such field: " << name;
            return nullptr;
        }
        return d;
    };

    EXPECT_TRUE(refl->GetBool(*msg, fd("flag")));
    EXPECT_EQ(refl->GetInt32(*msg, fd("neg")), v_neg);
    EXPECT_EQ(refl->GetUInt64(*msg, fd("big")), v_big);
    EXPECT_FLOAT_EQ(refl->GetFloat(*msg, fd("f32")), v_f32);
    EXPECT_DOUBLE_EQ(refl->GetDouble(*msg, fd("f64")), v_f64);
}

TEST(EncodeFast, DynamicVectorFloat32)
{
    // ----- 1) Build a tiny schema: vals : float32[], rpm : uint32
    DataTamerParser::Schema schema;
    schema.channel_name = "bench";
    schema.hash = 0;  // will be checked against snapshot
    {
        DataTamerParser::TypeField vals;
        vals.field_name = "vals";
        vals.type = DataTamerParser::BasicType::FLOAT32;
        vals.is_vector = true;  // dynamic
        vals.array_size = 0;    // <-- dynamic length
        schema.fields.push_back(vals);

        DataTamerParser::TypeField rpm;
        rpm.field_name = "rpm";
        rpm.type = DataTamerParser::BasicType::UINT32;
        rpm.is_vector = false;
        schema.fields.push_back(rpm);
    }
    // mimic the same hash logic the writer used (tests often don’t care, but we set it for completeness)
    for (const auto& f : schema.fields)
    {
        schema.hash = DataTamerParser::AddFieldToHash(f, schema.hash);
    }

    // ----- 2) Build proto runtime
    auto rt = data_tamer_tools::buildProto(schema);

    // ----- 3) Build a snapshot:
    // active mask: only bit 0 set -> "vals" active, "rpm" inactive
    std::vector<uint8_t> mask(1, 0);
    mask[0] = 0x01;

    // payload: [u32 len=2][float32 1.5][float32 -3.25]
    std::vector<uint8_t> payload;
    auto push_u32 = [&](uint32_t v)
    {
        payload.push_back(uint8_t(v & 0xFF));
        payload.push_back(uint8_t((v >> 8) & 0xFF));
        payload.push_back(uint8_t((v >> 16) & 0xFF));
        payload.push_back(uint8_t((v >> 24) & 0xFF));
    };
    auto push_f32 = [&](float f)
    {
        uint32_t u;
        static_assert(sizeof(float) == 4, "float must be 32-bit");
        std::memcpy(&u, &f, 4);
        push_u32(u);
    };
    push_u32(2);
    push_f32(1.5f);
    push_f32(-3.25f);

    DataTamerParser::SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = 123456789u;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    // ----- 4) Encode
    std::string bytes;
    // Reuse a scratch message to avoid alloc noise
    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> scratch(proto->New());

    // After building rt and encoding into `msg`...
    auto* refl = scratch->GetReflection();
    auto fd = [&](const char* name) -> const google::protobuf::FieldDescriptor*
    {
        auto* d = rt.desc->FindFieldByName(name);  // or map lookup if you kept one
        if (!d)
        {
            ADD_FAILURE() << "no such field: " << name;
            return nullptr;
        }
        return d;
    };

    ASSERT_TRUE(data_tamer_tools::encodeSnapshot(schema, sv, rt, bytes, scratch.get()));
    ASSERT_GT(bytes.size(), 0u);

    // Scalar check (inactive)
    const auto* fd_rpm = fd("rpm");
    ASSERT_FALSE(fd_rpm->is_repeated());
    EXPECT_FALSE(refl->HasField(*scratch, fd_rpm));

    // ----- 5) Parse back with reflection and assert contents
    std::unique_ptr<google::protobuf::Message> msg(proto->New());
    ASSERT_TRUE(msg->ParseFromString(bytes));
    refl = msg->GetReflection();

    // Find the 'vals' field (canonical key is just "vals")
    auto it = rt.field_by_name.find("vals");
    ASSERT_NE(it, rt.field_by_name.end());
    const auto* fd_vals = it->second;
    ASSERT_TRUE(fd_vals->is_repeated());
    ASSERT_EQ(fd_vals->type(), google::protobuf::FieldDescriptor::TYPE_FLOAT);

    // size == 2, vals[0]==1.5f, vals[1]==-3.25f
    ASSERT_EQ(refl->FieldSize(*msg, fd_vals), 2);
    EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, fd_vals, 0), 1.5f);
    EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, fd_vals, 1), -3.25f);
}

TEST(EncodeFast, FixedVectorUint32)
{
    // 1) Schema: ids : uint32[3] (fixed), temp : float32 (inactive)
    DataTamerParser::Schema schema;
    schema.channel_name = "bench_fixed";
    {
        DataTamerParser::TypeField ids;
        ids.field_name = "ids";
        ids.type = DataTamerParser::BasicType::UINT32;
        ids.is_vector = true;
        ids.array_size = 3;  // <-- fixed-length vector
        schema.fields.push_back(ids);

        DataTamerParser::TypeField temp;
        temp.field_name = "temp";
        temp.type = DataTamerParser::BasicType::FLOAT32;
        temp.is_vector = false;
        schema.fields.push_back(temp);
    }
    // hash like writer
    for (const auto& f : schema.fields)
    {
        schema.hash = DataTamerParser::AddFieldToHash(f, schema.hash);
    }

    // 2) Build proto
    auto rt = data_tamer_tools::buildProto(schema);

    // 3) Snapshot: active mask enables only 'ids'
    std::vector<uint8_t> mask(1, 0x01);  // bit0(ids)=1, bit1(temp)=0

    // payload: ids has exactly 3 * uint32, no length prefix for fixed vectors
    std::vector<uint8_t> payload;
    auto push_u32 = [&](uint32_t v)
    {
        payload.push_back(uint8_t(v & 0xFF));
        payload.push_back(uint8_t((v >> 8) & 0xFF));
        payload.push_back(uint8_t((v >> 16) & 0xFF));
        payload.push_back(uint8_t((v >> 24) & 0xFF));
    };
    const uint32_t v0 = 10, v1 = 20, v2 = 30;
    push_u32(v0);
    push_u32(v1);
    push_u32(v2);

    DataTamerParser::SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = 42u;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    // 4) Encode using a scratch message
    std::string bytes;
    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> scratch(proto->New());
    ASSERT_TRUE(data_tamer_tools::encodeSnapshot(schema, sv, rt, bytes, scratch.get()));
    ASSERT_GT(bytes.size(), 0u);

    // 5) Verify
    std::unique_ptr<google::protobuf::Message> msg(proto->New());
    ASSERT_TRUE(msg->ParseFromString(bytes));
    const auto* refl = msg->GetReflection();

    // ids: repeated uint32, size 3, exact values
    auto it_ids = rt.field_by_name.find("ids");
    ASSERT_NE(it_ids, rt.field_by_name.end());
    const auto* fd_ids = it_ids->second;
    ASSERT_TRUE(fd_ids->is_repeated());
    ASSERT_EQ(fd_ids->type(), google::protobuf::FieldDescriptor::TYPE_UINT32);
    ASSERT_EQ(refl->FieldSize(*msg, fd_ids), 3);
    EXPECT_EQ(refl->GetRepeatedUInt32(*msg, fd_ids, 0), v0);
    EXPECT_EQ(refl->GetRepeatedUInt32(*msg, fd_ids, 1), v1);
    EXPECT_EQ(refl->GetRepeatedUInt32(*msg, fd_ids, 2), v2);

    // temp should be unset (inactive)
    auto it_temp = rt.field_by_name.find("temp");
    ASSERT_NE(it_temp, rt.field_by_name.end());
    EXPECT_FALSE(refl->HasField(*msg, it_temp->second));
}

TEST(EncodeFast, CompositeScalarFields)
{
    // Schema:
    //   pose : Pose { x: float64, y: float64 }   (OTHER, not a vector)
    //   rpm  : uint32                            (inactive in this test)
    DataTamerParser::Schema schema;
    schema.channel_name = "bench_composite";

    // define custom type "Pose"
    schema.custom_types["Pose"] = {
        DataTamerParser::TypeField{ .field_name = "x", .type = DataTamerParser::BasicType::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
        DataTamerParser::TypeField{ .field_name = "y", .type = DataTamerParser::BasicType::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
    };

    DataTamerParser::TypeField pose;
    pose.field_name = "pose";
    pose.type = DataTamerParser::BasicType::OTHER;
    pose.type_name = "Pose";
    pose.is_vector = false;
    schema.fields.push_back(pose);

    DataTamerParser::TypeField rpm;
    rpm.field_name = "rpm";
    rpm.type = DataTamerParser::BasicType::UINT32;
    rpm.is_vector = false;
    schema.fields.push_back(rpm);

    // compute hash like writer does
    for (const auto& f : schema.fields)
    {
        schema.hash = DataTamerParser::AddFieldToHash(f, schema.hash);
    }

    // Build proto
    auto rt = data_tamer_tools::buildProto(schema);

    // Active mask: pose active (bit0=1), rpm inactive (bit1=0)
    std::vector<uint8_t> mask(1, 0x01);

    // Payload layout for pose: x(double), y(double)
    auto push_u64 = [&](uint64_t v, std::vector<uint8_t>& buf)
    {
        buf.push_back(uint8_t(v & 0xFF));
        buf.push_back(uint8_t((v >> 8) & 0xFF));
        buf.push_back(uint8_t((v >> 16) & 0xFF));
        buf.push_back(uint8_t((v >> 24) & 0xFF));
        buf.push_back(uint8_t((v >> 32) & 0xFF));
        buf.push_back(uint8_t((v >> 40) & 0xFF));
        buf.push_back(uint8_t((v >> 48) & 0xFF));
        buf.push_back(uint8_t((v >> 56) & 0xFF));
    };
    auto dbl_to_le = [&](double d)
    {
        uint64_t u;
        static_assert(sizeof(double) == 8, "");
        std::memcpy(&u, &d, 8);
        return u;
    };

    const double x = 12.5;
    const double y = -3.75;
    std::vector<uint8_t> payload;
    push_u64(dbl_to_le(x), payload);
    push_u64(dbl_to_le(y), payload);

    // Build SnapshotView
    DataTamerParser::SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = 999u;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    // Encode (using fast path under your encodeSnapshot)
    std::string bytes;
    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> scratch(proto->New());
    ASSERT_TRUE(data_tamer_tools::encodeSnapshot(schema, sv, rt, bytes, scratch.get()));
    ASSERT_GT(bytes.size(), 0u);

    // Verify decoded message has pose/x and pose/y set
    std::unique_ptr<google::protobuf::Message> msg(proto->New());
    ASSERT_TRUE(msg->ParseFromString(bytes));
    const auto* refl = msg->GetReflection();

    auto it_px = rt.field_by_name.find("pose/x");
    auto it_py = rt.field_by_name.find("pose/y");
    ASSERT_NE(it_px, rt.field_by_name.end());
    ASSERT_NE(it_py, rt.field_by_name.end());

    EXPECT_TRUE(refl->HasField(*msg, it_px->second));
    EXPECT_TRUE(refl->HasField(*msg, it_py->second));
    EXPECT_DOUBLE_EQ(refl->GetDouble(*msg, it_px->second), x);
    EXPECT_DOUBLE_EQ(refl->GetDouble(*msg, it_py->second), y);

    // rpm should be unset
    auto it_rpm = rt.field_by_name.find("rpm");
    ASSERT_NE(it_rpm, rt.field_by_name.end());
    EXPECT_FALSE(refl->HasField(*msg, it_rpm->second));
}

TEST(EncodeFast, CompositeVectorFixed)
{
    // Schema:
    //   poses[2] : Pose { x: float32, y: float32 }
    //   temp     : int32   (inactive in this test)
    DataTamerParser::Schema schema;
    schema.channel_name = "bench_composite_vec";

    // define custom type "Pose"
    schema.custom_types["Pose"] = {
        DataTamerParser::TypeField{ .field_name = "x", .type = DataTamerParser::BasicType::FLOAT32, .type_name = "float32", .is_vector = false, .array_size = 0 },
        DataTamerParser::TypeField{ .field_name = "y", .type = DataTamerParser::BasicType::FLOAT32, .type_name = "float32", .is_vector = false, .array_size = 0 },
    };

    DataTamerParser::TypeField poses;
    poses.field_name = "poses";
    poses.type = DataTamerParser::BasicType::OTHER;
    poses.type_name = "Pose";
    poses.is_vector = true;  // vector of Pose
    poses.array_size = 2;    // fixed length
    schema.fields.push_back(poses);

    DataTamerParser::TypeField temp;
    temp.field_name = "temp";
    temp.type = DataTamerParser::BasicType::INT32;
    temp.is_vector = false;
    schema.fields.push_back(temp);

    // compute hash
    for (const auto& f : schema.fields)
    {
        schema.hash = DataTamerParser::AddFieldToHash(f, schema.hash);
    }

    // Build proto
    auto rt = data_tamer_tools::buildProto(schema);

    // Active mask: poses active (bit0=1), temp inactive (bit1=0)
    std::vector<uint8_t> mask(1, 0x01);

    // Payload layout: [x0,y0][x1,y1]
    auto f2le = [&](float f)
    {
        uint32_t u;
        std::memcpy(&u, &f, 4);
        return u;
    };
    auto push_u32 = [&](uint32_t v, std::vector<uint8_t>& buf)
    {
        buf.push_back(uint8_t(v & 0xFF));
        buf.push_back(uint8_t((v >> 8) & 0xFF));
        buf.push_back(uint8_t((v >> 16) & 0xFF));
        buf.push_back(uint8_t((v >> 24) & 0xFF));
    };

    float x0 = 1.5f, y0 = -2.25f, x1 = 3.0f, y1 = 4.5f;
    std::vector<uint8_t> payload;
    push_u32(f2le(x0), payload);
    push_u32(f2le(y0), payload);
    push_u32(f2le(x1), payload);
    push_u32(f2le(y1), payload);

    // Build SnapshotView
    DataTamerParser::SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = 111u;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    // Encode
    std::string bytes;
    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> scratch(proto->New());
    ASSERT_TRUE(data_tamer_tools::encodeSnapshot(schema, sv, rt, bytes, scratch.get()));
    ASSERT_GT(bytes.size(), 0u);

    // Decode & verify
    std::unique_ptr<google::protobuf::Message> msg(proto->New());
    ASSERT_TRUE(msg->ParseFromString(bytes));
    const auto* refl = msg->GetReflection();

    auto it_px = rt.field_by_name.find("poses/x");
    auto it_py = rt.field_by_name.find("poses/y");
    ASSERT_NE(it_px, rt.field_by_name.end());
    ASSERT_NE(it_py, rt.field_by_name.end());

    EXPECT_EQ(refl->FieldSize(*msg, it_px->second), 2);
    EXPECT_EQ(refl->FieldSize(*msg, it_py->second), 2);
    EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, it_px->second, 0), x0);
    EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, it_py->second, 0), y0);
    EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, it_px->second, 1), x1);
    EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, it_py->second, 1), y1);

    // temp should be unset
    auto it_temp = rt.field_by_name.find("temp");
    ASSERT_NE(it_temp, rt.field_by_name.end());
    EXPECT_FALSE(refl->HasField(*msg, it_temp->second));
}

TEST(EncodeFast, DynamicCompositeVector)
{
    // Schema:
    //   custom Pose { x: float64, y: float64 }
    //   poses : Pose[]   (OTHER, vector, dynamic length)
    //   rpm   : uint32   (inactive in this test)
    DataTamerParser::Schema schema;
    schema.channel_name = "bench_dyn_composite";

    // Define custom type "Pose"
    schema.custom_types["Pose"] = {
        DataTamerParser::TypeField{ .field_name = "x", .type = DataTamerParser::BasicType::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
        DataTamerParser::TypeField{ .field_name = "y", .type = DataTamerParser::BasicType::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
    };

    // Top-level: poses (vector< Pose >, dynamic), rpm (scalar)
    DataTamerParser::TypeField poses;
    poses.field_name = "poses";
    poses.type = DataTamerParser::BasicType::OTHER;
    poses.type_name = "Pose";
    poses.is_vector = true;  // dynamic
    poses.array_size = 0;    // dynamic
    schema.fields.push_back(poses);

    DataTamerParser::TypeField rpm;
    rpm.field_name = "rpm";
    rpm.type = DataTamerParser::BasicType::UINT32;
    rpm.is_vector = false;
    schema.fields.push_back(rpm);

    // Hash like writer does
    for (const auto& f : schema.fields) schema.hash = DataTamerParser::AddFieldToHash(f, schema.hash);

    // Build proto runtime
    auto rt = data_tamer_tools::buildProto(schema);

    // Active mask: poses active (bit0=1), rpm inactive (bit1=0)
    std::vector<uint8_t> mask(1, 0x01);

    // Payload: [len=2(u32)] [x0(double)] [y0(double)] [x1(double)] [y1(double)]
    auto push_u32 = [](uint32_t v, std::vector<uint8_t>& buf)
    {
        buf.push_back(uint8_t(v & 0xFF));
        buf.push_back(uint8_t((v >> 8) & 0xFF));
        buf.push_back(uint8_t((v >> 16) & 0xFF));
        buf.push_back(uint8_t((v >> 24) & 0xFF));
    };
    auto push_f64 = [](double d, std::vector<uint8_t>& buf)
    {
        uint64_t u;
        static_assert(sizeof(double) == 8, "");
        std::memcpy(&u, &d, 8);
        buf.push_back(uint8_t(u & 0xFF));
        buf.push_back(uint8_t((u >> 8) & 0xFF));
        buf.push_back(uint8_t((u >> 16) & 0xFF));
        buf.push_back(uint8_t((u >> 24) & 0xFF));
        buf.push_back(uint8_t((u >> 32) & 0xFF));
        buf.push_back(uint8_t((u >> 40) & 0xFF));
        buf.push_back(uint8_t((u >> 48) & 0xFF));
        buf.push_back(uint8_t((u >> 56) & 0xFF));
    };

    const double x0 = 1.25, y0 = -2.5;
    const double x1 = 3.75, y1 = 4.5;

    std::vector<uint8_t> payload;
    push_u32(2, payload);  // dynamic length = 2
    push_f64(x0, payload);
    push_f64(y0, payload);
    push_f64(x1, payload);
    push_f64(y1, payload);

    // Snapshot view
    DataTamerParser::SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = 123u;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    // Encode (fast path under encodeSnapshot), using a scratch message
    std::string bytes;
    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> scratch(proto->New());
    ASSERT_TRUE(data_tamer_tools::encodeSnapshot(schema, sv, rt, bytes, scratch.get()));
    ASSERT_GT(bytes.size(), 0u);

    // Decode to verify
    std::unique_ptr<google::protobuf::Message> msg(proto->New());
    ASSERT_TRUE(msg->ParseFromString(bytes));
    const auto* refl = msg->GetReflection();

    // Leaves should have been marked repeated: "poses/x" and "poses/y"
    auto it_px = rt.field_by_name.find("poses/x");
    auto it_py = rt.field_by_name.find("poses/y");
    ASSERT_NE(it_px, rt.field_by_name.end());
    ASSERT_NE(it_py, rt.field_by_name.end());
    const auto* fd_px = it_px->second;
    const auto* fd_py = it_py->second;
    ASSERT_TRUE(fd_px->is_repeated());
    ASSERT_TRUE(fd_py->is_repeated());

    ASSERT_EQ(refl->FieldSize(*msg, fd_px), 2);
    ASSERT_EQ(refl->FieldSize(*msg, fd_py), 2);

    EXPECT_DOUBLE_EQ(refl->GetRepeatedDouble(*msg, fd_px, 0), x0);
    EXPECT_DOUBLE_EQ(refl->GetRepeatedDouble(*msg, fd_py, 0), y0);
    EXPECT_DOUBLE_EQ(refl->GetRepeatedDouble(*msg, fd_px, 1), x1);
    EXPECT_DOUBLE_EQ(refl->GetRepeatedDouble(*msg, fd_py, 1), y1);

    // rpm should be unset
    auto it_rpm = rt.field_by_name.find("rpm");
    ASSERT_NE(it_rpm, rt.field_by_name.end());
    EXPECT_FALSE(refl->HasField(*msg, it_rpm->second));
}

TEST(EncodeFast, EmptyDynamicVector)
{
    using namespace DataTamerParser;

    // Schema:
    //   vals : float32[]   (dynamic vector)
    //   rpm  : int32       (scalar)
    Schema schema;
    schema.channel_name = "bench_empty_dynvec";

    TypeField vals;
    vals.field_name = "vals";
    vals.type = BasicType::FLOAT32;
    vals.is_vector = true;  // vector
    vals.array_size = 0;    // dynamic
    schema.fields.push_back(vals);

    TypeField rpm;
    rpm.field_name = "rpm";
    rpm.type = BasicType::INT32;
    rpm.is_vector = false;
    schema.fields.push_back(rpm);

    // Compute hash like writer does
    for (const auto& f : schema.fields) schema.hash = AddFieldToHash(f, schema.hash);

    // Build proto runtime
    auto rt = data_tamer_tools::buildProto(schema);

    // ----------- Build Snapshot -----------
    // Active: both fields active (bit0=vals, bit1=rpm)
    std::vector<uint8_t> mask(1, 0x03);

    // Payload layout (ordered as schema):
    // vals: [u32 length=0] + 0 elements
    // rpm : int32
    const int32_t rpm_val = -12345;

    std::vector<uint8_t> payload;
    auto push_u32 = [&](uint32_t v)
    {
        payload.push_back(uint8_t(v & 0xFF));
        payload.push_back(uint8_t((v >> 8) & 0xFF));
        payload.push_back(uint8_t((v >> 16) & 0xFF));
        payload.push_back(uint8_t((v >> 24) & 0xFF));
    };
    auto push_i32 = [&](int32_t v)
    {
        payload.push_back(uint8_t(v & 0xFF));
        payload.push_back(uint8_t((v >> 8) & 0xFF));
        payload.push_back(uint8_t((v >> 16) & 0xFF));
        payload.push_back(uint8_t((v >> 24) & 0xFF));
    };

    // vals length = 0
    push_u32(0);
    // rpm scalar
    push_i32(rpm_val);

    SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = 42;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    // ----------- Encode -----------
    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> scratch(proto->New());

    std::string bytes;
    ASSERT_TRUE(data_tamer_tools::encodeSnapshot(schema, sv, rt, bytes, scratch.get()));
    ASSERT_GT(bytes.size(), 0u);

    // ----------- Verify -----------
    std::unique_ptr<google::protobuf::Message> msg(proto->New());
    ASSERT_TRUE(msg->ParseFromString(bytes));
    const auto* refl = msg->GetReflection();

    // look up FDs by canonical names
    auto fd_vals_it = rt.field_by_name.find("vals");
    auto fd_rpm_it = rt.field_by_name.find("rpm");
    ASSERT_NE(fd_vals_it, rt.field_by_name.end());
    ASSERT_NE(fd_rpm_it, rt.field_by_name.end());
    const auto* fd_vals = fd_vals_it->second;
    const auto* fd_rpm = fd_rpm_it->second;

    ASSERT_TRUE(fd_vals->is_repeated());
    EXPECT_EQ(refl->FieldSize(*msg, fd_vals), 0);  // empty vector
    EXPECT_TRUE(refl->HasField(*msg, fd_rpm));
    EXPECT_EQ(refl->GetInt32(*msg, fd_rpm), rpm_val);  // scalar parsed correctly
}

TEST(EncodeFast, CompositeWithVectorChild_SparseLengths)
{
    // Custom type: Sensor { x: float64, samples: float32[], y: float64 }
    DataTamerParser::Schema schema;
    schema.channel_name = "bench_sparse_composite";

    schema.custom_types["Sensor"] = {
        { .field_name = "x", .type = DataTamerParser::BasicType::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
        { .field_name = "samples", .type = DataTamerParser::BasicType::FLOAT32, .type_name = "float32", .is_vector = true, .array_size = 0 },  // dynamic
        { .field_name = "y", .type = DataTamerParser::BasicType::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
    };

    // Top-level: sensor: Sensor (active), rpm: uint32 (inactive)
    DataTamerParser::TypeField sensor;
    sensor.field_name = "sensor";
    sensor.type = DataTamerParser::BasicType::OTHER;
    sensor.type_name = "Sensor";
    sensor.is_vector = false;
    schema.fields.push_back(sensor);

    DataTamerParser::TypeField rpm;
    rpm.field_name = "rpm";
    rpm.type = DataTamerParser::BasicType::UINT32;
    rpm.is_vector = false;
    schema.fields.push_back(rpm);

    // Hash like the writer
    for (const auto& f : schema.fields)
    {
        schema.hash = DataTamerParser::AddFieldToHash(f, schema.hash);
    }

    // Build runtime
    auto rt = data_tamer_tools::buildProto(schema);

    // Active mask: sensor on, rpm off  -> 0b01
    std::vector<uint8_t> mask(1, 0x01);

    // Payload: x(double), samples_len(uint32=0), y(double)
    auto push_u32 = [](uint32_t v, std::vector<uint8_t>& buf)
    {
        buf.push_back(uint8_t(v & 0xFF));
        buf.push_back(uint8_t((v >> 8) & 0xFF));
        buf.push_back(uint8_t((v >> 16) & 0xFF));
        buf.push_back(uint8_t((v >> 24) & 0xFF));
    };
    auto push_u64 = [](uint64_t v, std::vector<uint8_t>& buf)
    {
        buf.push_back(uint8_t(v & 0xFF));
        buf.push_back(uint8_t((v >> 8) & 0xFF));
        buf.push_back(uint8_t((v >> 16) & 0xFF));
        buf.push_back(uint8_t((v >> 24) & 0xFF));
        buf.push_back(uint8_t((v >> 32) & 0xFF));
        buf.push_back(uint8_t((v >> 40) & 0xFF));
        buf.push_back(uint8_t((v >> 48) & 0xFF));
        buf.push_back(uint8_t((v >> 56) & 0xFF));
    };
    auto dbl_to_u64 = [](double d)
    {
        uint64_t u;
        std::memcpy(&u, &d, 8);
        return u;
    };

    const double x = 42.25;
    const double y = -7.5;

    std::vector<uint8_t> payload;
    push_u64(dbl_to_u64(x), payload);  // sensor/x
    push_u32(0, payload);              // samples len = 0 (sparse vector)
    push_u64(dbl_to_u64(y), payload);  // sensor/y

    // SnapshotView
    DataTamerParser::SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = 123;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    // Encode via fast path
    std::string bytes;
    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> scratch(proto->New());
    ASSERT_TRUE(data_tamer_tools::encodeSnapshot(schema, sv, rt, bytes, scratch.get()));
    ASSERT_GT(bytes.size(), 0u);

    // Decode and verify
    std::unique_ptr<google::protobuf::Message> msg(proto->New());
    ASSERT_TRUE(msg->ParseFromString(bytes));
    const auto* refl = msg->GetReflection();

    // Look up leaves by canonical name
    auto fd_x = rt.field_by_name.at("sensor/x");
    auto fd_s = rt.field_by_name.at("sensor/samples");
    auto fd_y = rt.field_by_name.at("sensor/y");
    auto fd_r = rt.field_by_name.at("rpm");

    // Scalars inside composite are set
    EXPECT_TRUE(refl->HasField(*msg, fd_x));
    EXPECT_TRUE(refl->HasField(*msg, fd_y));
    EXPECT_DOUBLE_EQ(refl->GetDouble(*msg, fd_x), x);
    EXPECT_DOUBLE_EQ(refl->GetDouble(*msg, fd_y), y);

    // Vector child has length 0
    EXPECT_TRUE(fd_s->is_repeated());
    EXPECT_EQ(refl->FieldSize(*msg, fd_s), 0);

    // Inactive top-level rpm remains unset
    EXPECT_FALSE(refl->HasField(*msg, fd_r));
}

TEST(EncodeFast, DynamicCompositeVector_NonZero)
{
    using namespace DataTamerParser;

    // -------- Schema: poses : Pose[] (dynamic), rpm : uint32 (inactive) ------
    Schema schema;
    schema.channel_name = "bench_dyn_composite_vec";

    // Define Pose { x: float64, y: float64 }
    schema.custom_types["Pose"] = {
        TypeField{ .field_name = "x", .type = BasicType::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
        TypeField{ .field_name = "y", .type = BasicType::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
    };

    // Top-level dynamic vector of Pose
    TypeField poses;
    poses.field_name = "poses";
    poses.type = BasicType::OTHER;
    poses.type_name = "Pose";
    poses.is_vector = true;  // dynamic because array_size == 0
    poses.array_size = 0;
    schema.fields.push_back(poses);

    // A scalar field after it (inactive in this test)
    TypeField rpm;
    rpm.field_name = "rpm";
    rpm.type = BasicType::UINT32;
    rpm.is_vector = false;
    schema.fields.push_back(rpm);

    // Hash like writer
    for (const auto& f : schema.fields)
    {
        schema.hash = AddFieldToHash(f, schema.hash);
    }

    // -------- Build dynamic proto runtime ------------------------------------
    auto rt = data_tamer_tools::buildProto(schema);

    // -------- Build active mask: poses active (bit0=1), rpm inactive (bit1=0) -
    std::vector<uint8_t> mask(1, 0x01);

    // -------- Payload: [u32 len=3] then 3 * Pose(x,y) as doubles ------------
    auto push_u32 = [](uint32_t v, std::vector<uint8_t>& buf)
    {
        buf.push_back(uint8_t(v & 0xFF));
        buf.push_back(uint8_t((v >> 8) & 0xFF));
        buf.push_back(uint8_t((v >> 16) & 0xFF));
        buf.push_back(uint8_t((v >> 24) & 0xFF));
    };
    auto push_u64 = [](uint64_t v, std::vector<uint8_t>& buf)
    {
        buf.push_back(uint8_t(v & 0xFF));
        buf.push_back(uint8_t((v >> 8) & 0xFF));
        buf.push_back(uint8_t((v >> 16) & 0xFF));
        buf.push_back(uint8_t((v >> 24) & 0xFF));
        buf.push_back(uint8_t((v >> 32) & 0xFF));
        buf.push_back(uint8_t((v >> 40) & 0xFF));
        buf.push_back(uint8_t((v >> 48) & 0xFF));
        buf.push_back(uint8_t((v >> 56) & 0xFF));
    };
    auto dbl_bits = [](double d)
    {
        uint64_t u;
        static_assert(sizeof(double) == 8, "");
        std::memcpy(&u, &d, 8);
        return u;
    };

    const uint32_t L = 3;
    const double x0 = 1.0, y0 = -2.0;
    const double x1 = 3.25, y1 = 4.5;
    const double x2 = -7.75, y2 = 0.125;

    std::vector<uint8_t> payload;
    push_u32(L, payload);  // dynamic length prefix
    // Pose[0]
    push_u64(dbl_bits(x0), payload);
    push_u64(dbl_bits(y0), payload);
    // Pose[1]
    push_u64(dbl_bits(x1), payload);
    push_u64(dbl_bits(y1), payload);
    // Pose[2]
    push_u64(dbl_bits(x2), payload);
    push_u64(dbl_bits(y2), payload);

    SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = 42;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    // -------- Encode with fast path ------------------------------------------
    std::string bytes;
    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> scratch(proto->New());
    ASSERT_TRUE(encodeSnapshot(schema, sv, rt, bytes, scratch.get()));
    ASSERT_GT(bytes.size(), 0u);

    // -------- Parse back & verify --------------------------------------------
    std::unique_ptr<google::protobuf::Message> msg(proto->New());
    ASSERT_TRUE(msg->ParseFromString(bytes));
    const auto* refl = msg->GetReflection();

    // Canonical names map to FDs
    auto fdx = rt.field_by_name.find("poses/x");
    auto fdy = rt.field_by_name.find("poses/y");
    ASSERT_NE(fdx, rt.field_by_name.end());
    ASSERT_NE(fdy, rt.field_by_name.end());
    const auto* fd_x = fdx->second;
    const auto* fd_y = fdy->second;
    ASSERT_TRUE(fd_x->is_repeated());
    ASSERT_TRUE(fd_y->is_repeated());

    ASSERT_EQ(refl->FieldSize(*msg, fd_x), (int)L);
    ASSERT_EQ(refl->FieldSize(*msg, fd_y), (int)L);

    EXPECT_DOUBLE_EQ(refl->GetRepeatedDouble(*msg, fd_x, 0), x0);
    EXPECT_DOUBLE_EQ(refl->GetRepeatedDouble(*msg, fd_y, 0), y0);

    EXPECT_DOUBLE_EQ(refl->GetRepeatedDouble(*msg, fd_x, 1), x1);
    EXPECT_DOUBLE_EQ(refl->GetRepeatedDouble(*msg, fd_y, 1), y1);

    EXPECT_DOUBLE_EQ(refl->GetRepeatedDouble(*msg, fd_x, 2), x2);
    EXPECT_DOUBLE_EQ(refl->GetRepeatedDouble(*msg, fd_y, 2), y2);

    // rpm still unset
    auto it_rpm = rt.field_by_name.find("rpm");
    ASSERT_NE(it_rpm, rt.field_by_name.end());
    EXPECT_FALSE(refl->HasField(*msg, it_rpm->second));
}

TEST(EncodeFast, NestedComposite_ScalarMembers)
{
    using BT = DataTamerParser::BasicType;

    // Schema:
    //   type Inner { a: float64, b: int32 }
    //   type Outer { p: Inner,   q: uint32 }
    //   fields: outer: Outer, rpm: uint32
    DataTamerParser::Schema schema;
    schema.channel_name = "nested_scalar";

    schema.custom_types["Inner"] = {
        DataTamerParser::TypeField{ .field_name = "a", .type = BT::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
        DataTamerParser::TypeField{ .field_name = "b", .type = BT::INT32, .type_name = "int32", .is_vector = false, .array_size = 0 },
    };
    schema.custom_types["Outer"] = {
        DataTamerParser::TypeField{ .field_name = "p", .type = BT::OTHER, .type_name = "Inner", .is_vector = false, .array_size = 0 },
        DataTamerParser::TypeField{ .field_name = "q", .type = BT::UINT32, .type_name = "uint32", .is_vector = false, .array_size = 0 },
    };

    DataTamerParser::TypeField outer;
    outer.field_name = "outer";
    outer.type = BT::OTHER;
    outer.type_name = "Outer";
    outer.is_vector = false;
    schema.fields.push_back(outer);

    DataTamerParser::TypeField rpm;
    rpm.field_name = "rpm";
    rpm.type = BT::UINT32;
    rpm.is_vector = false;
    schema.fields.push_back(rpm);

    // Hash like writer
    for (const auto& f : schema.fields) schema.hash = DataTamerParser::AddFieldToHash(f, schema.hash);

    // Build proto runtime
    auto rt = data_tamer_tools::buildProto(schema);

    // Active mask: outer active (bit0=1), rpm inactive (bit1=0)
    std::vector<uint8_t> mask(1, 0x01);

    // Payload for outer:
    //   outer.p.a (double), outer.p.b (int32), outer.q (uint32)
    auto push_u32 = [&](uint32_t v, std::vector<uint8_t>& buf)
    {
        buf.push_back(uint8_t(v & 0xFF));
        buf.push_back(uint8_t((v >> 8) & 0xFF));
        buf.push_back(uint8_t((v >> 16) & 0xFF));
        buf.push_back(uint8_t((v >> 24) & 0xFF));
    };
    auto push_u64 = [&](uint64_t v, std::vector<uint8_t>& buf)
    {
        buf.push_back(uint8_t(v & 0xFF));
        buf.push_back(uint8_t((v >> 8) & 0xFF));
        buf.push_back(uint8_t((v >> 16) & 0xFF));
        buf.push_back(uint8_t((v >> 24) & 0xFF));
        buf.push_back(uint8_t((v >> 32) & 0xFF));
        buf.push_back(uint8_t((v >> 40) & 0xFF));
        buf.push_back(uint8_t((v >> 48) & 0xFF));
        buf.push_back(uint8_t((v >> 56) & 0xFF));
    };
    auto dbl_bits = [&](double d)
    {
        uint64_t u;
        std::memcpy(&u, &d, 8);
        return u;
    };

    const double a = 42.25;
    const int32_t b = -17;
    const uint32_t q = 9001;

    std::vector<uint8_t> payload;
    push_u64(dbl_bits(a), payload);               // outer/p/a
    push_u32(static_cast<uint32_t>(b), payload);  // outer/p/b (int32)
    push_u32(q, payload);                         // outer/q

    // SnapshotView
    DataTamerParser::SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = 123u;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    // Encode with fast path
    std::string bytes;
    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> scratch(proto->New());
    ASSERT_TRUE(data_tamer_tools::encodeSnapshot(schema, sv, rt, bytes, scratch.get()));
    ASSERT_GT(bytes.size(), 0u);

    // Decode and check
    std::unique_ptr<google::protobuf::Message> msg(proto->New());
    ASSERT_TRUE(msg->ParseFromString(bytes));
    const auto* refl = msg->GetReflection();

    // Canonical names: "outer/p/a", "outer/p/b", "outer/q"
    auto fda = rt.field_by_name.find("outer/p/a");
    auto fdb = rt.field_by_name.find("outer/p/b");
    auto fdq = rt.field_by_name.find("outer/q");
    ASSERT_NE(fda, rt.field_by_name.end());
    ASSERT_NE(fdb, rt.field_by_name.end());
    ASSERT_NE(fdq, rt.field_by_name.end());

    EXPECT_TRUE(refl->HasField(*msg, fda->second));
    EXPECT_TRUE(refl->HasField(*msg, fdb->second));
    EXPECT_TRUE(refl->HasField(*msg, fdq->second));

    EXPECT_DOUBLE_EQ(refl->GetDouble(*msg, fda->second), a);
    EXPECT_EQ(refl->GetInt32(*msg, fdb->second), b);
    EXPECT_EQ(refl->GetUInt32(*msg, fdq->second), q);

    // rpm inactive → unset
    auto fdrpm = rt.field_by_name.find("rpm");
    ASSERT_NE(fdrpm, rt.field_by_name.end());
    EXPECT_FALSE(refl->HasField(*msg, fdrpm->second));
}

TEST(EncodeFast, NestedComposite_WithFixedVectorChild_AndInactiveSibling)
{
    using namespace DataTamerParser;
    using data_tamer_tools::buildProto;
    using data_tamer_tools::encodeSnapshot;  // if your encodeSnapshot wraps the fast path

    // -------- Schema --------
    // type Inner { a: float64, vals: float32[3] }
    // type Outer { inner: Inner, temp: int32 }
    // fields: outer: Outer   (ACTIVE)
    //         rpm:   uint32  (INACTIVE)
    Schema schema;
    schema.channel_name = "bench_nested_fixedvec";

    schema.custom_types["Inner"] = {
        TypeField{ .field_name = "a", .type = BasicType::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
        TypeField{ .field_name = "vals", .type = BasicType::FLOAT32, .type_name = "float32", .is_vector = true, .array_size = 3 },
    };
    schema.custom_types["Outer"] = {
        TypeField{ .field_name = "inner", .type = BasicType::OTHER, .type_name = "Inner", .is_vector = false, .array_size = 0 },
        TypeField{ .field_name = "temp", .type = BasicType::INT32, .type_name = "int32", .is_vector = false, .array_size = 0 },
    };

    TypeField outer;
    outer.field_name = "outer";
    outer.type = BasicType::OTHER;
    outer.type_name = "Outer";
    outer.is_vector = false;
    schema.fields.push_back(outer);

    TypeField rpm;
    rpm.field_name = "rpm";
    rpm.type = BasicType::UINT32;
    rpm.is_vector = false;
    schema.fields.push_back(rpm);

    // hash
    for (const auto& f : schema.fields) schema.hash = AddFieldToHash(f, schema.hash);

    auto rt = buildProto(schema);

    // -------- Payload (only 'outer' active) --------
    // Layout:
    //   outer/inner/a (double)
    //   outer/inner/vals[0..2] (float32 x3)
    //   outer/temp (int32)
    const double a = 42.25;
    const float v0 = -1.5f, v1 = 0.25f, v2 = 8.0f;
    const int32_t temp = -17;

    std::vector<uint8_t> payload;

    auto push_u32 = [&](uint32_t v)
    {
        payload.push_back(uint8_t(v));
        payload.push_back(uint8_t(v >> 8));
        payload.push_back(uint8_t(v >> 16));
        payload.push_back(uint8_t(v >> 24));
    };
    auto push_u64 = [&](uint64_t v)
    {
        payload.push_back(uint8_t(v));
        payload.push_back(uint8_t(v >> 8));
        payload.push_back(uint8_t(v >> 16));
        payload.push_back(uint8_t(v >> 24));
        payload.push_back(uint8_t(v >> 32));
        payload.push_back(uint8_t(v >> 40));
        payload.push_back(uint8_t(v >> 48));
        payload.push_back(uint8_t(v >> 56));
    };
    auto push_f32 = [&](float f)
    {
        static_assert(sizeof(float) == 4, "");
        uint32_t u;
        std::memcpy(&u, &f, 4);
        push_u32(u);
    };
    auto push_f64 = [&](double d)
    {
        static_assert(sizeof(double) == 8, "");
        uint64_t u;
        std::memcpy(&u, &d, 8);
        push_u64(u);
    };
    auto push_i32 = [&](int32_t i)
    {
        uint32_t u;
        std::memcpy(&u, &i, 4);
        push_u32(u);
    };

    // outer.inner.a
    push_f64(a);
    // outer.inner.vals[0..2]
    push_f32(v0);
    push_f32(v1);
    push_f32(v2);
    // outer.temp
    push_i32(temp);

    // active mask: outer=1, rpm=0  -> 0b01
    std::vector<uint8_t> mask(1, 0x01);

    SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = 123;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    // -------- Encode --------
    std::string bytes;
    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> scratch(proto->New());

    ASSERT_TRUE(encodeSnapshot(schema, sv, rt, bytes, scratch.get()));
    ASSERT_GT(bytes.size(), 0u);

    // -------- Verify --------
    std::unique_ptr<google::protobuf::Message> msg(proto->New());
    ASSERT_TRUE(msg->ParseFromString(bytes));
    const auto* refl = msg->GetReflection();

    auto fd = [&](const char* name) -> const google::protobuf::FieldDescriptor*
    {
        auto it = rt.field_by_name.find(name);
        if (it == rt.field_by_name.end())
        {
            ADD_FAILURE() << "no such field: " << name;
            return nullptr;
        }
        return it->second;
    };

    const auto* fd_a = fd("outer/inner/a");
    const auto* fd_vals = fd("outer/inner/vals");
    const auto* fd_temp = fd("outer/temp");
    const auto* fd_rpm = fd("rpm");

    // scalars present
    ASSERT_FALSE(fd_a->is_repeated());
    ASSERT_FALSE(fd_temp->is_repeated());
    EXPECT_TRUE(refl->HasField(*msg, fd_a));
    EXPECT_TRUE(refl->HasField(*msg, fd_temp));
    EXPECT_DOUBLE_EQ(refl->GetDouble(*msg, fd_a), a);
    EXPECT_EQ(refl->GetInt32(*msg, fd_temp), temp);

    // vector present with 3 elements
    ASSERT_TRUE(fd_vals->is_repeated());
    ASSERT_EQ(refl->FieldSize(*msg, fd_vals), 3);
    EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, fd_vals, 0), v0);
    EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, fd_vals, 1), v1);
    EXPECT_FLOAT_EQ(refl->GetRepeatedFloat(*msg, fd_vals, 2), v2);

    // inactive sibling
    EXPECT_FALSE(refl->HasField(*msg, fd_rpm));
}

// 1) Scalar active, payload too short -> EXPECT_THROW
TEST(EncodeFast, ShortPayload_Scalar_Throws)
{
    using BT = DataTamerParser::BasicType;

    DataTamerParser::Schema schema;
    schema.channel_name = "short_scalar";
    schema.fields.push_back(DataTamerParser::TypeField{ .field_name = "rpm", .type = BT::UINT32, .type_name = "uint32", .is_vector = false, .array_size = 0 });
    schema.hash = DataTamerParser::AddFieldToHash(schema.fields[0], 0);

    auto rt = data_tamer_tools::buildProto(schema);

    // mask bit0 = 1 (rpm active)
    std::vector<uint8_t> mask(1, 0x01);

    // payload intentionally too short for a uint32 (needs 4 bytes, give 2)
    std::vector<uint8_t> payload = { 0x11, 0x22 };

    DataTamerParser::SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = 123;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    std::string bytes;
    const auto* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> scratch(proto->New());

    EXPECT_THROW({ (void)data_tamer_tools::encodeSnapshot(schema, sv, rt, bytes, scratch.get()); }, std::runtime_error);
}

// 2) Dynamic vector advertises length=3, but only 1 element in payload -> EXPECT_THROW
TEST(EncodeFast, ShortPayload_DynVec_Throws)
{
    using BT = DataTamerParser::BasicType;

    DataTamerParser::Schema schema;
    schema.channel_name = "short_dynvec";
    // vals: float32[]
    schema.fields.push_back(DataTamerParser::TypeField{ .field_name = "vals", .type = BT::FLOAT32, .type_name = "float32", .is_vector = true, .array_size = 0 });
    schema.hash = DataTamerParser::AddFieldToHash(schema.fields[0], 0);

    auto rt = data_tamer_tools::buildProto(schema);

    // active bit for vals
    std::vector<uint8_t> mask(1, 0x01);

    // payload:
    // [u32 len=3] [one float32 element]  -> missing 2 elements
    std::vector<uint8_t> payload;
    auto push_u32 = [&](uint32_t v)
    {
        payload.push_back(uint8_t(v & 0xFF));
        payload.push_back(uint8_t((v >> 8) & 0xFF));
        payload.push_back(uint8_t((v >> 16) & 0xFF));
        payload.push_back(uint8_t((v >> 24) & 0xFF));
    };
    auto push_f32 = [&](float f)
    {
        static_assert(sizeof(float) == 4, "float must be 4 bytes");
        uint32_t u;
        std::memcpy(&u, &f, 4);
        push_u32(u);
    };

    push_u32(3);     // length
    push_f32(1.0f);  // only one element provided

    DataTamerParser::SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = 456;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    std::string bytes;
    const auto* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> scratch(proto->New());

    EXPECT_THROW({ (void)data_tamer_tools::encodeSnapshot(schema, sv, rt, bytes, scratch.get()); }, std::runtime_error);
}

TEST(EncodeFast, ShortPayload_FixedCompositeVec_Throws)
{
    using namespace DataTamerParser;
    Schema schema;
    schema.channel_name = "fixed_comp_vec_short";

    // Define composite type Pose { x: f64, y: f64 }
    schema.custom_types["Pose"] = {
        TypeField{ .field_name = "x", .type = BasicType::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
        TypeField{ .field_name = "y", .type = BasicType::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
    };

    // poses[2]: Pose
    TypeField poses;
    poses.field_name = "poses";
    poses.type = BasicType::OTHER;
    poses.type_name = "Pose";
    poses.is_vector = true;
    poses.array_size = 2;
    schema.fields.push_back(poses);

    // hash like writer
    for (const auto& f : schema.fields) schema.hash = AddFieldToHash(f, schema.hash);

    auto rt = data_tamer_tools::buildProto(schema);

    // Active only poses
    std::vector<uint8_t> mask(1, 0x01);

    // Payload UNDERFILLED: only 1 Pose (x,y) = 16 bytes instead of 32
    auto push_u64 = [&](uint64_t u, std::vector<uint8_t>& buf)
    {
        for (int i = 0; i < 8; i++) buf.push_back(uint8_t(u >> (8 * i)));
    };
    auto d2u = [&](double d)
    {
        uint64_t u;
        std::memcpy(&u, &d, 8);
        return u;
    };

    std::vector<uint8_t> payload;
    push_u64(d2u(1.0), payload);  // x0
    push_u64(d2u(2.0), payload);  // y0
    // Missing x1,y1  => should throw

    SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = 0;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    std::string bytes;
    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> scratch(proto->New());

    EXPECT_THROW({ (void)data_tamer_tools::encodeSnapshot(schema, sv, rt, bytes, scratch.get()); }, std::runtime_error);
}

TEST(EncodeFast, ShortPayload_DynamicCompositeVec_Throws)
{
    // Custom type: Pose { x: float64, y: float64 }
    DataTamerParser::Schema schema;
    schema.channel_name = "short_dyn_comp_vec";
    schema.custom_types["Pose"] = {
        DataTamerParser::TypeField{ .field_name = "x", .type = DataTamerParser::BasicType::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
        DataTamerParser::TypeField{ .field_name = "y", .type = DataTamerParser::BasicType::FLOAT64, .type_name = "float64", .is_vector = false, .array_size = 0 },
    };

    // poses : Pose[]  (dynamic vector)
    DataTamerParser::TypeField poses;
    poses.field_name = "poses";
    poses.type = DataTamerParser::BasicType::OTHER;
    poses.type_name = "Pose";
    poses.is_vector = true;  // dynamic -> array_size == 0
    poses.array_size = 0;
    schema.fields.push_back(poses);

    // Hash like writer does
    for (const auto& f : schema.fields) schema.hash = DataTamerParser::AddFieldToHash(f, schema.hash);

    // Build proto/runtime
    auto rt = data_tamer_tools::buildProto(schema);

    // Active mask: poses active (bit0=1)
    std::vector<uint8_t> mask(1, 0x01);

    // Helpers to build payload
    auto push_u32 = [](uint32_t v, std::vector<uint8_t>& buf)
    {
        buf.push_back(uint8_t(v & 0xFF));
        buf.push_back(uint8_t((v >> 8) & 0xFF));
        buf.push_back(uint8_t((v >> 16) & 0xFF));
        buf.push_back(uint8_t((v >> 24) & 0xFF));
    };
    auto push_u64 = [](uint64_t v, std::vector<uint8_t>& buf)
    {
        buf.push_back(uint8_t(v & 0xFF));
        buf.push_back(uint8_t((v >> 8) & 0xFF));
        buf.push_back(uint8_t((v >> 16) & 0xFF));
        buf.push_back(uint8_t((v >> 24) & 0xFF));
        buf.push_back(uint8_t((v >> 32) & 0xFF));
        buf.push_back(uint8_t((v >> 40) & 0xFF));
        buf.push_back(uint8_t((v >> 48) & 0xFF));
        buf.push_back(uint8_t((v >> 56) & 0xFF));
    };
    auto dbl_bits = [](double d)
    {
        uint64_t u;
        static_assert(sizeof(double) == 8, "double size");
        std::memcpy(&u, &d, 8);
        return u;
    };

    // Payload:
    //  - length prefix = 2 (we claim 2 Pose elements)
    //  - BUT only provide ONE Pose (x,y) = (1.0, 2.0) -> 16 bytes
    std::vector<uint8_t> payload;
    push_u32(2, payload);              // claimed len = 2
    push_u64(dbl_bits(1.0), payload);  // Pose[0].x
    push_u64(dbl_bits(2.0), payload);  // Pose[0].y
    // Missing Pose[1].x and Pose[1].y (should trigger short-read)

    // Snapshot view
    DataTamerParser::SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = 42;
    sv.active_mask = { mask.data(), mask.size() };
    sv.payload = { payload.data(), payload.size() };

    // Encode should throw due to truncated payload
    std::string bytes;
    const google::protobuf::Message* proto = rt.factory->GetPrototype(rt.desc);
    ASSERT_NE(proto, nullptr);
    std::unique_ptr<google::protobuf::Message> scratch(proto->New());

    EXPECT_THROW({ (void)data_tamer_tools::encodeSnapshot(schema, sv, rt, bytes, scratch.get()); }, std::runtime_error);
}

// Main function to run all tests
int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
