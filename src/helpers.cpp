
#include <data_tamer_tools/helpers.hpp>
#include <google/protobuf/descriptor.pb.h>
#include <mcap/reader.hpp>

namespace data_tamer_tools
{
bool isDataTamerFile(const std::string& filepath)
{
    mcap::McapReader reader;
    auto res = reader.open(filepath);
    if (!res.ok())
    {
        throw std::runtime_error("Failed to open mcap: " + res.message);
    }

    [[maybe_unused]] mcap::Status s = reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan,
                                                         [&](const mcap::Status& s)
                                                         {
                                                             if (!s.ok())
                                                             {
                                                                 throw std::runtime_error("Reading summary from mcap file failed: " + s.message);
                                                             }
                                                         });

    // simplest check: look at any channel's messageEncoding
    for (const auto& [id, chan] : reader.channels())
    {
        if (chan->messageEncoding == "data_tamer")
        {
            return true;  // at least one channel is data_tamer
        }
    }

    // you could also check schemas if you want to be stricter
    for (const auto& [id, schema] : reader.schemas())
    {
        if (schema->encoding == "data_tamer")
        {
            return true;
        }
    }

    return false;
}

nlohmann::json convertToJSONSchema(const DataTamer::Schema& schema)
{
    nlohmann::json j;
    j["$schema"] = "https://json-schema.org/draft/2020-12/schema";
    j["title"] = schema.channel_name;
    j["type"] = "object";
    j["additionalProperties"] = false;  // catch unknown fields
    j["properties"] = nlohmann::json::object();
    j["required"] = nlohmann::json::array();

    for (const auto& field : schema.fields)
    {
        nlohmann::json f;

        if (field.is_vector)
        {
            f["type"] = "array";
            f["items"] = { { "type", convertTypeToJSON(field.type) } };

            // If your DataTamer::Field exposes a fixed length, set it here:
            // if (field.vector_len > 0) {
            //     f["minItems"] = field.vector_len;
            //     f["maxItems"] = field.vector_len;
            // }
        }
        else
        {
            f["type"] = convertTypeToJSON(field.type);  // "number" | "integer" | "boolean" | "string"
        }

        j["properties"][field.field_name] = std::move(f);
    }

    // Inline Foxglove Timestamp schema and require it
    j["properties"]["timestamp"] = {
        { "type", "object" },
        { "properties", { { "sec", { { "type", "integer" }, { "minimum", 0 } } }, { "nsec", { { "type", "integer" }, { "minimum", 0 }, { "maximum", 999999999 } } } } },
        { "required", { "sec", "nsec" } }
    };
    j["required"].push_back("timestamp");

    return j;
}

nlohmann::json convertToJSONSchema(const DataTamerParser::Schema& schema)
{
    nlohmann::json jsonSchema;
    // Use a modern draft; Foxglove examples use 2020-12, but draft-07 also works.
    jsonSchema["$schema"] = "https://json-schema.org/draft/2020-12/schema";
    jsonSchema["title"] = schema.channel_name;
    jsonSchema["type"] = "object";
    jsonSchema["properties"] = nlohmann::json::object();
    jsonSchema["required"] = nlohmann::json::array();

    for (const auto& field : schema.fields)
    {
        nlohmann::json fieldSchema;
        fieldSchema["type"] = convertTypeToJSON(field.type);

        if (field.is_vector)
        {
            fieldSchema["type"] = "array";
            fieldSchema["items"] = { { "type", convertTypeToJSON(field.type) } };
            // Optionally set minItems/maxItems here if you know fixed length.
            // fieldSchema["minItems"] = field.vector_len;
            // fieldSchema["maxItems"] = field.vector_len;
        }

        jsonSchema["properties"][field.field_name] = fieldSchema;
    }

    // Inline Foxglove Timestamp schema as "timestamp" and make it required.
    jsonSchema["properties"]["timestamp"] = {
        { "type", "object" },
        { "properties", { { "sec", { { "type", "integer" }, { "minimum", 0 } } }, { "nsec", { { "type", "integer" }, { "minimum", 0 }, { "maximum", 999999999 } } } } },
        { "required", { "sec", "nsec" } }
    };
    jsonSchema["required"].push_back("timestamp");

    return jsonSchema;
}

std::string convertTypeToJSON(DataTamer::BasicType type)
{
    switch (type)
    {
        case DataTamer::BasicType::BOOL:
            return "boolean";
        case DataTamer::BasicType::CHAR:
        case DataTamer::BasicType::INT8:
        case DataTamer::BasicType::UINT8:
        case DataTamer::BasicType::INT16:
        case DataTamer::BasicType::UINT16:
        case DataTamer::BasicType::INT32:
        case DataTamer::BasicType::UINT32:
        case DataTamer::BasicType::INT64:
        case DataTamer::BasicType::UINT64:
            return "integer";
        case DataTamer::BasicType::FLOAT32:
        case DataTamer::BasicType::FLOAT64:
            return "number";
        default:
            return "string";  // Using string for OTHER or undefined types
    }
}

std::string convertTypeToJSON(DataTamerParser::BasicType type)
{
    switch (type)
    {
        case DataTamerParser::BasicType::BOOL:
            return "boolean";
        case DataTamerParser::BasicType::CHAR:
        case DataTamerParser::BasicType::INT8:
        case DataTamerParser::BasicType::UINT8:
        case DataTamerParser::BasicType::INT16:
        case DataTamerParser::BasicType::UINT16:
        case DataTamerParser::BasicType::INT32:
        case DataTamerParser::BasicType::UINT32:
        case DataTamerParser::BasicType::INT64:
        case DataTamerParser::BasicType::UINT64:
            return "integer";
        case DataTamerParser::BasicType::FLOAT32:
        case DataTamerParser::BasicType::FLOAT64:
            return "number";
        default:
            return "string";  // Using string for OTHER or undefined types
    }
}

nlohmann::json serializeSnapshotToJson(const DataTamer::Snapshot& snapshot, const DataTamer::Schema& schema)
{
    nlohmann::json jsonData;
    jsonData["channel_name"] = std::string(snapshot.channel_name);
    jsonData["timestamp"] = snapshot.timestamp.count();

    size_t offset = 0;  // cursor into payload

    auto need = [&](size_t n)
    {
        if (offset + n > snapshot.payload.size())
            throw std::runtime_error("Payload does not contain enough data for all fields.");
    };
    auto read_primitive = [&](auto* out)
    {
        using T = std::decay_t<decltype(*out)>;
        need(sizeof(T));
        std::memcpy(out, snapshot.payload.data() + offset, sizeof(T));
        offset += sizeof(T);
    };
    auto read_u32 = [&]()
    {
        uint32_t v = 0;
        read_primitive(&v);
        return v;
    };

    auto push_scalar_to_json = [&](const DataTamer::TypeField& field, const uint8_t* bytes)
    {
        // interpret bytes → json value (no aliasing UB)
        switch (field.type)
        {
            case DataTamer::BasicType::BOOL:
            {
                bool v;
                std::memcpy(&v, bytes, sizeof(v));
                jsonData[field.field_name] = v;
                break;
            }
            case DataTamer::BasicType::CHAR:
            {
                char v;
                std::memcpy(&v, bytes, sizeof(v));
                jsonData[field.field_name] = int(v);
                break;
            }
            case DataTamer::BasicType::INT8:
            {
                int8_t v;
                std::memcpy(&v, bytes, sizeof(v));
                jsonData[field.field_name] = v;
                break;
            }
            case DataTamer::BasicType::UINT8:
            {
                uint8_t v;
                std::memcpy(&v, bytes, sizeof(v));
                jsonData[field.field_name] = v;
                break;
            }
            case DataTamer::BasicType::INT16:
            {
                int16_t v;
                std::memcpy(&v, bytes, sizeof(v));
                jsonData[field.field_name] = v;
                break;
            }
            case DataTamer::BasicType::UINT16:
            {
                uint16_t v;
                std::memcpy(&v, bytes, sizeof(v));
                jsonData[field.field_name] = v;
                break;
            }
            case DataTamer::BasicType::INT32:
            {
                int32_t v;
                std::memcpy(&v, bytes, sizeof(v));
                jsonData[field.field_name] = v;
                break;
            }
            case DataTamer::BasicType::UINT32:
            {
                uint32_t v;
                std::memcpy(&v, bytes, sizeof(v));
                jsonData[field.field_name] = v;
                break;
            }
            case DataTamer::BasicType::INT64:
            {
                int64_t v;
                std::memcpy(&v, bytes, sizeof(v));
                jsonData[field.field_name] = v;
                break;
            }
            case DataTamer::BasicType::UINT64:
            {
                uint64_t v;
                std::memcpy(&v, bytes, sizeof(v));
                jsonData[field.field_name] = v;
                break;
            }
            case DataTamer::BasicType::FLOAT32:
            {
                float v;
                std::memcpy(&v, bytes, sizeof(v));
                jsonData[field.field_name] = v;
                break;
            }
            case DataTamer::BasicType::FLOAT64:
            {
                double v;
                std::memcpy(&v, bytes, sizeof(v));
                jsonData[field.field_name] = v;
                break;
            }
            default:
                jsonData[field.field_name] = "Unsupported type";
                break;
        }
    };

    auto elem_size = [&](DataTamer::BasicType t) -> size_t
    {
        switch (t)
        {
            case DataTamer::BasicType::BOOL:
                return sizeof(bool);
            case DataTamer::BasicType::CHAR:
                return sizeof(char);
            case DataTamer::BasicType::INT8:
                return sizeof(int8_t);
            case DataTamer::BasicType::UINT8:
                return sizeof(uint8_t);
            case DataTamer::BasicType::INT16:
                return sizeof(int16_t);
            case DataTamer::BasicType::UINT16:
                return sizeof(uint16_t);
            case DataTamer::BasicType::INT32:
                return sizeof(int32_t);
            case DataTamer::BasicType::UINT32:
                return sizeof(uint32_t);
            case DataTamer::BasicType::INT64:
                return sizeof(int64_t);
            case DataTamer::BasicType::UINT64:
                return sizeof(uint64_t);
            case DataTamer::BasicType::FLOAT32:
                return sizeof(float);
            case DataTamer::BasicType::FLOAT64:
                return sizeof(double);
            default:
                return 0;
        }
    };

    for (size_t i = 0; i < schema.fields.size(); ++i)
    {
        const auto& f = schema.fields[i];
        if (!DataTamer::GetBit(snapshot.active_mask, i))
            continue;

        if (f.type == DataTamer::BasicType::OTHER)
        {
            // Not supported by this JSON path—emit a placeholder object and skip length sensibly if needed later.
            jsonData[f.field_name] = "Unsupported type";
            continue;
        }

        if (!f.is_vector)
        {
            // scalar (basic)
            const size_t sz = elem_size(f.type);
            need(sz);
            push_scalar_to_json(f, snapshot.payload.data() + offset);
            offset += sz;
        }
        else
        {
            // vector (basic)
            uint32_t len = f.array_size;
            if (len == 0)
            {
                // dynamic vector: first u32 length
                len = read_u32();
            }

            nlohmann::json arr = nlohmann::json::array();
            const size_t esz = elem_size(f.type);
            for (uint32_t k = 0; k < len; ++k)
            {
                need(esz);
                // read element as bytes → convert by type
                switch (f.type)
                {
                    case DataTamer::BasicType::BOOL:
                    {
                        bool v;
                        read_primitive(&v);
                        arr.push_back(v);
                        break;
                    }
                    case DataTamer::BasicType::CHAR:
                    {
                        char v;
                        read_primitive(&v);
                        arr.push_back(int(v));
                        break;
                    }
                    case DataTamer::BasicType::INT8:
                    {
                        int8_t v;
                        read_primitive(&v);
                        arr.push_back(v);
                        break;
                    }
                    case DataTamer::BasicType::UINT8:
                    {
                        uint8_t v;
                        read_primitive(&v);
                        arr.push_back(v);
                        break;
                    }
                    case DataTamer::BasicType::INT16:
                    {
                        int16_t v;
                        read_primitive(&v);
                        arr.push_back(v);
                        break;
                    }
                    case DataTamer::BasicType::UINT16:
                    {
                        uint16_t v;
                        read_primitive(&v);
                        arr.push_back(v);
                        break;
                    }
                    case DataTamer::BasicType::INT32:
                    {
                        int32_t v;
                        read_primitive(&v);
                        arr.push_back(v);
                        break;
                    }
                    case DataTamer::BasicType::UINT32:
                    {
                        uint32_t v;
                        read_primitive(&v);
                        arr.push_back(v);
                        break;
                    }
                    case DataTamer::BasicType::INT64:
                    {
                        int64_t v;
                        read_primitive(&v);
                        arr.push_back(v);
                        break;
                    }
                    case DataTamer::BasicType::UINT64:
                    {
                        uint64_t v;
                        read_primitive(&v);
                        arr.push_back(v);
                        break;
                    }
                    case DataTamer::BasicType::FLOAT32:
                    {
                        float v;
                        read_primitive(&v);
                        arr.push_back(v);
                        break;
                    }
                    case DataTamer::BasicType::FLOAT64:
                    {
                        double v;
                        read_primitive(&v);
                        arr.push_back(v);
                        break;
                    }
                    default:
                    { /* unreachable due to earlier guard */
                        break;
                    }
                }
            }
            jsonData[f.field_name] = std::move(arr);
        }
    }

    return jsonData;
}

// Decode payload using pre-parsed schema + active_mask into JSON using DataTamerParser.
// Returns UTF-8 JSON bytes ready for Foxglove.
nlohmann::json serializeSnapshotToJson(const DataTamerParser::Schema& schema, const std::vector<uint8_t>& active_mask, const std::vector<uint8_t>& payload,
                                       const std::chrono::nanoseconds& timestamp)
{
    // Build a SnapshotView over the buffers (for BufferSpan helpers)
    DataTamerParser::SnapshotView sv;
    sv.schema_hash = schema.hash;
    sv.timestamp = static_cast<uint64_t>(timestamp.count());
    sv.active_mask = { active_mask.data(), active_mask.size() };
    sv.payload = { payload.data(), payload.size() };

    nlohmann::json out = nlohmann::json::object();

    // Foxglove-style timestamp (sec, nsec)
    const uint64_t ns = static_cast<uint64_t>(timestamp.count());
    out["timestamp"] = { { "sec", static_cast<uint32_t>(ns / 1000000000ULL) }, { "nsec", static_cast<uint32_t>(ns % 1000000000ULL) } };

    // Local alias we’ll mutate as we consume
    DataTamerParser::BufferSpan buf = sv.payload;

    // Helper: set JSON value from VarNumber (cast char -> integer for stability)
    auto set_json_value = [](nlohmann::json& j, const DataTamerParser::VarNumber& v)
    {
        std::visit(
            [&](auto&& x)
            {
                using T = std::decay_t<decltype(x)>;
                if constexpr (std::is_same_v<T, char>)
                    j = static_cast<int>(x);
                else
                    j = x;
            },
            v);
    };

    // Ensure an array exists and has at least len elements (filled with null)
    auto ensure_array_len = [](nlohmann::json& arr, size_t len)
    {
        if (!arr.is_array())
            arr = nlohmann::json::array();
        while (arr.size() < len)
            arr.push_back(nullptr);
    };

    // Recursively process one field (scalar/vector, basic/composite).
    // `prefix` is the flattened canonical path (slashes kept, no [idx] in keys).
    std::function<void(const std::string&, const DataTamerParser::TypeField&)> process;
    process = [&](const std::string& prefix, const DataTamerParser::TypeField& tf)
    {
        using BT = DataTamerParser::BasicType;

        // Build this field's base key (flattened with '/')
        const std::string base = prefix.empty() ? tf.field_name : (prefix + "/" + tf.field_name);

        if (tf.type != BT::OTHER)
        {
            // ----- basic types -----
            if (!tf.is_vector)
            {
                // scalar basic
                const auto v = DataTamerParser::DeserializeToVarNumber(tf.type, buf);
                set_json_value(out[base], v);
            }
            else
            {
                // vector of basic (fixed or dynamic)
                uint32_t len = tf.array_size;
                if (len == 0)
                    len = DataTamerParser::Deserialize<uint32_t>(buf);

                nlohmann::json& arr = out[base];
                ensure_array_len(arr, len);

                for (uint32_t i = 0; i < len; ++i)
                {
                    const auto v = DataTamerParser::DeserializeToVarNumber(tf.type, buf);
                    set_json_value(arr[i], v);
                }
            }
            return;
        }

        // ----- composite (OTHER) -----
        const auto it = schema.custom_types.find(tf.type_name);
        if (it == schema.custom_types.end())
        {
            // Unknown composite; we can represent as opaque block size if desired,
            // but since layout is unknown we cannot safely advance. Be conservative:
            throw std::runtime_error(std::string("Unknown custom type: ") + tf.type_name);
        }
        const auto& subs = it->second;

        if (!tf.is_vector)
        {
            // single composite instance: recurse each member under the same base path
            for (const auto& sub : subs)
                process(base, sub);
        }
        else
        {
            // vector of composite: we output arrays for each leaf path (e.g., "poses/x", "poses/y")
            uint32_t len = tf.array_size;
            if (len == 0)
                len = DataTamerParser::Deserialize<uint32_t>(buf);

            // Strategy: for each subfield, we write len elements. For basic subfields,
            //           we fill arrays out[base + "/" + sub.field_name][i] = value.
            //           For nested composites, recurse accordingly per element.
            //
            // First pass: if sub is basic, pre-size its array to len.
            for (const auto& sub : subs)
            {
                if (sub.type != BT::OTHER)
                    ensure_array_len(out[base + "/" + sub.field_name], len);
            }

            for (uint32_t i = 0; i < len; ++i)
            {
                for (const auto& sub : subs)
                {
                    if (sub.type != BT::OTHER)
                    {
                        const auto v = DataTamerParser::DeserializeToVarNumber(sub.type, buf);
                        set_json_value(out[base + "/" + sub.field_name][i], v);
                    }
                    else
                    {
                        // Nested composite inside composite vector. We need to recurse into its members
                        // for this element i, so we temporarily process into a staged object and then
                        // merge arrays per-leaf at index i. Easiest approach: recurse with a temp prefix,
                        // but write directly to out[...] with index i for basic leaves.
                        //
                        // Implement a small nested walker that, for each basic leaf under `sub`,
                        // writes to out[(base + "/" + sub.field_name + "/...")][i] = value.
                        std::function<void(const std::string&, const DataTamerParser::TypeField&)> process_nested;
                        process_nested = [&](const std::string& pfx, const DataTamerParser::TypeField& t)
                        {
                            const std::string leaf_base = pfx.empty() ? t.field_name : (pfx + "/" + t.field_name);
                            if (t.type != BT::OTHER)
                            {
                                if (!t.is_vector)
                                {
                                    const auto vv = DataTamerParser::DeserializeToVarNumber(t.type, buf);
                                    ensure_array_len(out[leaf_base], i + 1);
                                    set_json_value(out[leaf_base][i], vv);
                                }
                                else
                                {
                                    // vector inside element of vector-of-composite (rare but possible):
                                    uint32_t ilen = t.array_size;
                                    if (ilen == 0)
                                        ilen = DataTamerParser::Deserialize<uint32_t>(buf);
                                    // Represent as a 2D array: out[leaf_base][i] is an array of length ilen
                                    ensure_array_len(out[leaf_base], i + 1);
                                    nlohmann::json& inner = out[leaf_base][i];
                                    if (!inner.is_array())
                                        inner = nlohmann::json::array();
                                    while (inner.size() < ilen)
                                        inner.push_back(nullptr);
                                    for (uint32_t k = 0; k < ilen; ++k)
                                    {
                                        const auto vv = DataTamerParser::DeserializeToVarNumber(t.type, buf);
                                        set_json_value(inner[k], vv);
                                    }
                                }
                            }
                            else
                            {
                                const auto jt = schema.custom_types.find(t.type_name);
                                if (jt == schema.custom_types.end())
                                    throw std::runtime_error(std::string("Unknown nested custom type: ") + t.type_name);
                                for (const auto& s2 : jt->second)
                                    process_nested(leaf_base, s2);
                            }
                        };

                        process_nested(base + "/" + sub.field_name, sub);
                    }
                }
            }
        }
    };

    // Walk top-level fields in schema order; consume payload only for active bits
    for (size_t i = 0; i < schema.fields.size(); ++i)
    {
        if (!DataTamerParser::GetBit(sv.active_mask, i))
            continue;
        process(/*prefix*/ "", schema.fields[i]);
    }

    // Optional: if any payload remains, that’s unexpected for a well-formed snapshot.
    // (We won’t fail here; leave it as a guard you can enable if useful.)
    // if (buf.size != 0) { /* log or throw */ }

    return out;
}

using google::protobuf::FieldDescriptor;
using google::protobuf::FieldDescriptorProto;

std::string stripIndicesKeepSlashes(const std::string& s)
{
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i)
    {
        char c = s[i];
        if (c == '[')
        {
            while (i < s.size() && s[i] != ']')
                ++i;
            continue;
        }
        out.push_back(c);
    }
    return out;
}
static std::string sanitizePathKeepHierarchy(std::string s)
{
    // turn '/' -> '__', lowercase, keep [a-z0-9_]
    std::string out;
    out.reserve(s.size() * 2);
    for (char c : s)
    {
        if (c == '/')
        {
            out += "__";
            continue;
        }
        if (c >= 'A' && c <= 'Z')
        {
            c = char(c - 'A' + 'a');
        }
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')))
        {
            c = '_';
        }
        out.push_back(c);
    }
    if (out.empty() || std::isdigit(unsigned(out[0])))
    {
        out.insert(out.begin(), '_');
    }
    return out;
}

static FieldDescriptorProto::Type toProtoType(DataTamerParser::BasicType t)
{
    using T = DataTamerParser::BasicType;
    switch (t)
    {
        case T::BOOL:
            return FieldDescriptorProto::TYPE_BOOL;
        case T::CHAR:
            return FieldDescriptorProto::TYPE_STRING;
        case T::INT8:
        case T::INT16:
        case T::INT32:
            return FieldDescriptorProto::TYPE_SINT32;
        case T::UINT8:
        case T::UINT16:
        case T::UINT32:
            return FieldDescriptorProto::TYPE_UINT32;
        case T::INT64:
            return FieldDescriptorProto::TYPE_SINT64;
        case T::UINT64:
            return FieldDescriptorProto::TYPE_UINT64;
        case T::FLOAT32:
            return FieldDescriptorProto::TYPE_FLOAT;
        case T::FLOAT64:
            return FieldDescriptorProto::TYPE_DOUBLE;
        case T::OTHER:
            return FieldDescriptorProto::TYPE_BYTES;
    }
    return FieldDescriptorProto::TYPE_BYTES;
}

void flattenType(const std::string& prefix, const DataTamerParser::TypeField& f, const std::map<std::string, DataTamerParser::FieldsVector>& custom_types,
                 std::vector<FlatField>& out)  // keep param if you want, but don't use it
{
    const std::string base = prefix.empty() ? f.field_name : (prefix + "/" + f.field_name);

    if (f.type != DataTamerParser::BasicType::OTHER)
    {
        // Leaf: repeat ONLY if the leaf itself is a basic-type vector
        FlatField leaf;
        leaf.full_path = base;
        leaf.tf = f;
        leaf.tf.is_vector = f.is_vector;  // <-- no propagation from parent
        leaf.fd = nullptr;
        out.push_back(std::move(leaf));
        return;
    }

    // Composite: NEVER propagate is_vector to children
    auto it = custom_types.find(f.type_name);
    if (it == custom_types.end())
        return;

    const size_t start = out.size();
    for (const auto& sub : it->second)
    {
        flattenType(base, sub, custom_types, out);
    }

    // If THIS composite is a vector, all its descendant leaves must be repeated in the proto.
    if (f.is_vector)
    {
        for (size_t i = start; i < out.size(); ++i)
        {
            out[i].tf.is_vector = true;  // mark leaf fields as repeated
        }
    }
}

std::vector<FlatField> flattenSchema(const DataTamerParser::Schema& s)
{
    std::vector<FlatField> out;
    out.reserve(s.fields.size());
    for (const auto& f : s.fields)
        flattenType("", f, s.custom_types, out);
    return out;
}

template <class AddDefaultFn>
static void ensureRepeatedSize(google::protobuf::Message* m, const FieldDescriptor* fd, int want, const AddDefaultFn& add_default)
{
    auto* refl = m->GetReflection();
    while (refl->FieldSize(*m, fd) < want)
        add_default();
}

ProtoRuntime buildProto(const DataTamerParser::Schema& schema)
{
    ProtoRuntime rt;
    rt.pool = std::make_unique<google::protobuf::DescriptorPool>();

    // 1) Flatten once, keep it for encodeSnapshot
    rt.flat = flattenSchema(schema);  // your existing helper, order matters

    google::protobuf::FileDescriptorProto file;
    file.set_name("dtfg_rt_" + std::to_string(schema.hash) + ".proto");
    file.set_package("dtfg.rt");
    auto* msg = file.add_message_type();
    msg->set_name("Snap_" + std::to_string(schema.hash));

    // Make field names readable and unique without hashing
    std::unordered_map<std::string, int> used;  // base name -> count
    auto mk_unique = [&](const std::string& base) -> std::string
    {
        auto [it, inserted] = used.emplace(base, 0);
        int n = ++it->second;
        return n == 1 ? base : (base + "__" + std::to_string(n));
    };

    // 2) Add fields in the SAME order as rt.flat
    int field_num = 1;
    for (const auto& ff : rt.flat)
    {
        const std::string canon = stripIndicesKeepSlashes(ff.full_path);
        const std::string base = sanitizePathKeepHierarchy(canon);
        const std::string name = mk_unique(base);

        auto* fd = msg->add_field();
        fd->set_name(name);
        fd->set_number(field_num++);
        fd->set_type(toProtoType(ff.tf.type));
        const bool is_rep = (ff.tf.type != DataTamerParser::BasicType::OTHER) && ff.tf.is_vector;
        fd->set_label(is_rep ? FieldDescriptorProto::LABEL_REPEATED : FieldDescriptorProto::LABEL_OPTIONAL);
    }

    // 3) Build and bind descriptors
    const auto* built = rt.pool->BuildFile(file);
    if (!built)
        throw std::runtime_error("DescriptorPool::BuildFile failed");

    rt.full_type = "dtfg.rt." + msg->name();
    rt.desc = rt.pool->FindMessageTypeByName(rt.full_type);
    if (!rt.desc)
        throw std::runtime_error("FindMessageTypeByName failed");

    // Bind each FlatField to its FieldDescriptor by number (1..N)
    {
        const int n = rt.desc->field_count();
        // Defensive check: keep flatten/proto field counts in lockstep
        if (static_cast<int>(rt.flat.size()) != n)
            throw std::runtime_error("flattened field count != proto field_count");

        for (int i = 0; i < n; ++i)
        {
            const auto* fd = rt.desc->FindFieldByNumber(i + 1);
            if (!fd)
            {
                throw std::runtime_error("FieldDescriptor null, FindFieldByNumber failed");
            }
            const bool is_rep = (fd->label() == google::protobuf::FieldDescriptor::LABEL_REPEATED);
            if (is_rep != rt.flat[i].tf.is_vector)
            {
                throw std::runtime_error("label mismatch for field #" + std::to_string(i + 1) + " name=" + fd->name());
            }
            rt.flat[i].fd = fd;  // <- hot path pointer for encodeSnapshot
            rt.field_by_name.emplace(rt.flat[i].full_path, fd);
        }
    }

    // 4) Message factory (reuse generated where possible)
    rt.factory = std::make_unique<google::protobuf::DynamicMessageFactory>(rt.pool.get());
    rt.factory->SetDelegateToGeneratedFactory(true);

    // 5) Emit FileDescriptorSet for Foxglove
    {
        google::protobuf::FileDescriptorSet set;
        auto* out_file = set.add_file();
        out_file->CopyFrom(file);
        set.SerializeToString(&rt.fdset_bytes);
    }

    return rt;
}
#ifdef DTT_DEBUG
static inline void dbgMask(const DataTamerParser::BufferSpan& m)
{
    fprintf(stderr, "[DBG] active_mask bytes (%zu):", m.size);
    for (size_t i = 0; i < m.size; ++i)
        fprintf(stderr, " %02X", m.data[i]);
    fputc('\n', stderr);
}

static inline const char* btName(DataTamerParser::BasicType t)
{
    switch (t)
    {
        case DataTamerParser::BasicType::BOOL:
            return "BOOL";
        case DataTamerParser::BasicType::CHAR:
            return "CHAR";
        case DataTamerParser::BasicType::INT8:
            return "INT8";
        case DataTamerParser::BasicType::UINT8:
            return "UINT8";
        case DataTamerParser::BasicType::INT16:
            return "INT16";
        case DataTamerParser::BasicType::UINT16:
            return "UINT16";
        case DataTamerParser::BasicType::INT32:
            return "INT32";
        case DataTamerParser::BasicType::UINT32:
            return "UINT32";
        case DataTamerParser::BasicType::INT64:
            return "INT64";
        case DataTamerParser::BasicType::UINT64:
            return "UINT64";
        case DataTamerParser::BasicType::FLOAT32:
            return "FLOAT32";
        case DataTamerParser::BasicType::FLOAT64:
            return "FLOAT64";
        case DataTamerParser::BasicType::OTHER:
            return "OTHER";
    }
    return "?";
}
#endif

bool encodeSnapshot(const DataTamerParser::Schema& schema, const DataTamerParser::SnapshotView& sv, const ProtoRuntime& rt, std::string& out_bytes,
                    google::protobuf::Message* scratch /*=nullptr*/)
{
    if (!rt.desc || !rt.factory)
        return false;

    // Prepare / reuse protobuf message
    std::unique_ptr<google::protobuf::Message> owned;
    google::protobuf::Message* m = scratch;
    if (!m)
    {
        const auto* proto = rt.factory->GetPrototype(rt.desc);
        if (!proto)
            return false;
        owned.reset(proto->New());
        m = owned.get();
    }
    else
    {
        m->Clear();
    }
    auto* refl = m->GetReflection();

#ifdef DTT_DEBUG
    auto dbg_fd = [&](const DataTamerParser::TypeField& tf, const google::protobuf::FieldDescriptor* fd, const char* ctx, int extra = -1)
    {
        fprintf(stderr, "  [DBG] %-8s name='%s' fd=%s label=%s ptype=%d is_vec=%d arr=%u elem_idx=%d\n", ctx, tf.field_name.c_str(), fd ? fd->name().c_str() : "<null>",
                (fd && fd->is_repeated()) ? "repeated" : "singular", fd ? (int)fd->type() : -1, (int)tf.is_vector, tf.array_size, extra);
        if (fd && (bool)fd->is_repeated() != (bool)tf.is_vector)
        {
            fprintf(stderr, "  [WARN] label mismatch: fd->is_repeated()=%d vs tf.is_vector=%d\n", (int)fd->is_repeated(), (int)tf.is_vector);
        }
    };
#endif

    // Aliased payload buffer
    DataTamerParser::BufferSpan buf = sv.payload;
#ifdef DTT_DEBUG
    fprintf(stderr, "\n=== encodeSnapshot ===\n");
    fprintf(stderr, "[DBG] schema.hash=%llu, desc_fields=%d, flat=%zu, payload=%zu\n", (unsigned long long)schema.hash, rt.desc ? rt.desc->field_count() : -1,
            rt.flat.size(), (size_t)sv.payload.size);

    dbgMask(sv.active_mask);
#endif

    // Cursor into the flattened leaf list (rt.flat)
    size_t leaf_i = 0;

    // Small helpers ----------------------------------------------------------

    auto add_default = [&](const google::protobuf::FieldDescriptor* fd)
    {
        using FD = google::protobuf::FieldDescriptor;
        switch (fd->type())
        {
            case FD::TYPE_SINT32:
                refl->AddInt32(m, fd, 0);
                break;
            case FD::TYPE_UINT32:
                refl->AddUInt32(m, fd, 0);
                break;
            case FD::TYPE_SINT64:
                refl->AddInt64(m, fd, 0);
                break;
            case FD::TYPE_UINT64:
                refl->AddUInt64(m, fd, 0);
                break;
            case FD::TYPE_FLOAT:
                refl->AddFloat(m, fd, 0.f);
                break;
            case FD::TYPE_DOUBLE:
                refl->AddDouble(m, fd, 0.0);
                break;
            case FD::TYPE_BOOL:
                refl->AddBool(m, fd, false);
                break;
            case FD::TYPE_STRING:
            case FD::TYPE_BYTES:
                refl->AddString(m, fd, "");
                break;
            default:
                break;
        }
    };

    auto ensureRepeatedSize = [&](const google::protobuf::FieldDescriptor* fd, int need)
    {
        int cur = refl->FieldSize(*m, fd);
        for (; cur < need; ++cur)
            add_default(fd);
    };

    auto set_scalar_from_var = [&](const google::protobuf::FieldDescriptor* fd, const DataTamerParser::VarNumber& vn)
    {
        using FD = google::protobuf::FieldDescriptor;
        std::visit(
            [&](auto&& val)
            {
                using T = std::decay_t<decltype(val)>;
                switch (fd->type())
                {
                    case FD::TYPE_BOOL:
                        if constexpr (std::is_same_v<T, bool>)
                            refl->SetBool(m, fd, val);
                        break;
                    case FD::TYPE_SINT32:
                        if constexpr (std::is_integral_v<T> && std::is_signed_v<T> && sizeof(T) <= 4)
                            refl->SetInt32(m, fd, static_cast<int32_t>(val));
                        break;
                    case FD::TYPE_UINT32:
                        if constexpr (std::is_integral_v<T> && std::is_unsigned_v<T> && sizeof(T) <= 4)
                            refl->SetUInt32(m, fd, static_cast<uint32_t>(val));
                        break;
                    case FD::TYPE_SINT64:
                        if constexpr (std::is_integral_v<T> && std::is_signed_v<T> && sizeof(T) <= 8)
                            refl->SetInt64(m, fd, static_cast<int64_t>(val));
                        break;
                    case FD::TYPE_UINT64:
                        if constexpr (std::is_integral_v<T> && std::is_unsigned_v<T> && sizeof(T) <= 8)
                            refl->SetUInt64(m, fd, static_cast<uint64_t>(val));
                        break;
                    case FD::TYPE_FLOAT:
                        if constexpr (std::is_same_v<T, float>)
                            refl->SetFloat(m, fd, val);
                        else if constexpr (std::is_same_v<T, double>)
                            refl->SetFloat(m, fd, static_cast<float>(val));
                        break;
                    case FD::TYPE_DOUBLE:
                        if constexpr (std::is_same_v<T, double>)
                            refl->SetDouble(m, fd, val);
                        else if constexpr (std::is_same_v<T, float>)
                            refl->SetDouble(m, fd, static_cast<double>(val));
                        break;
                    case FD::TYPE_STRING:
                        if constexpr (std::is_same_v<T, char>)
                            refl->SetString(m, fd, std::string(1, val));
                        break;
                    default:
                        break;
                }
            },
            vn);
    };

    auto set_repeated_from_var = [&](const google::protobuf::FieldDescriptor* fd, int idx, const DataTamerParser::VarNumber& vn)
    {
        using FD = google::protobuf::FieldDescriptor;
        std::visit(
            [&](auto&& val)
            {
                using T = std::decay_t<decltype(val)>;
                switch (fd->type())
                {
                    case FD::TYPE_BOOL:
                        if constexpr (std::is_same_v<T, bool>)
                            refl->SetRepeatedBool(m, fd, idx, val);
                        break;
                    case FD::TYPE_SINT32:
                        if constexpr (std::is_integral_v<T> && std::is_signed_v<T> && sizeof(T) <= 4)
                            refl->SetRepeatedInt32(m, fd, idx, static_cast<int32_t>(val));
                        break;
                    case FD::TYPE_UINT32:
                        if constexpr (std::is_integral_v<T> && std::is_unsigned_v<T> && sizeof(T) <= 4)
                            refl->SetRepeatedUInt32(m, fd, idx, static_cast<uint32_t>(val));
                        break;
                    case FD::TYPE_SINT64:
                        if constexpr (std::is_integral_v<T> && std::is_signed_v<T> && sizeof(T) <= 8)
                            refl->SetRepeatedInt64(m, fd, idx, static_cast<int64_t>(val));
                        break;
                    case FD::TYPE_UINT64:
                        if constexpr (std::is_integral_v<T> && std::is_unsigned_v<T> && sizeof(T) <= 8)
                            refl->SetRepeatedUInt64(m, fd, idx, static_cast<uint64_t>(val));
                        break;
                    case FD::TYPE_FLOAT:
                        if constexpr (std::is_same_v<T, float>)
                            refl->SetRepeatedFloat(m, fd, idx, val);
                        else if constexpr (std::is_same_v<T, double>)
                            refl->SetRepeatedFloat(m, fd, idx, static_cast<float>(val));
                        break;
                    case FD::TYPE_DOUBLE:
                        if constexpr (std::is_same_v<T, double>)
                            refl->SetRepeatedDouble(m, fd, idx, val);
                        else if constexpr (std::is_same_v<T, float>)
                            refl->SetRepeatedDouble(m, fd, idx, static_cast<double>(val));
                        break;
                    case FD::TYPE_STRING:
                        if constexpr (std::is_same_v<T, char>)
                            refl->SetRepeatedString(m, fd, idx, std::string(1, val));
                        break;
                    default:
                        break;
                }
            },
            vn);
    };

    // Skip/write a field (recursively), depending on type and vector-ness.
    std::function<void(const DataTamerParser::TypeField&, bool)> process_field;
    process_field = [&](const DataTamerParser::TypeField& tf, bool write)
    {
        using BT = DataTamerParser::BasicType;

        if (tf.type != BT::OTHER)
        {
            if (!tf.is_vector)
            {
                // scalar basic
                if (leaf_i >= rt.flat.size())
                    throw std::runtime_error("leaf cursor out of range (scalar)");

                const auto* fd = rt.flat[leaf_i].fd;
#ifdef DTT_DEBUG
                dbg_fd(tf, fd, "SCALAR");
#endif

                // Read one value
                const auto vn = DataTamerParser::DeserializeToVarNumber(tf.type, buf);

                if (write && fd)
                {
                    set_scalar_from_var(fd, vn);
                }

                ++leaf_i;  // advance exactly one leaf for a scalar
            }
            else
            {
                // vector of basic
                if (leaf_i >= rt.flat.size())
                    throw std::runtime_error("leaf cursor out of range (vec basic)");

                const auto* fd = rt.flat[leaf_i].fd;

                if (tf.is_vector && tf.type != DataTamerParser::BasicType::OTHER && tf.array_size == 0)
                {
                    uint32_t peek_len;
                    std::memcpy(&peek_len, buf.data, sizeof(uint32_t));
#ifdef DTT_DEBUG
                    fprintf(stderr, "  [DBG] peek dynamic-len raw=0x%08x (%u)\n", peek_len, peek_len);
#endif
                }

                uint32_t len = tf.array_size;
                if (len == 0)
                {
                    // dynamic vector length prefix
                    len = DataTamerParser::Deserialize<uint32_t>(buf);
                }

#ifdef DTT_DEBUG
                fprintf(stderr, "  [DBG] VEC-BASIC begin name='%s' fd=%s write=%d len=%u\n", tf.field_name.c_str(), (fd ? fd->name().c_str() : "<null>"), (int)write,
                        len);
                if (fd && !fd->is_repeated())
                {
                    fprintf(stderr, "  [ERR] fd is NOT repeated but tf.is_vector=1\n");
                }

                dbg_fd(tf, fd, "VEC-BASIC", (int)len);
#endif
                if (write && fd)
                {
#ifdef DTT_DEBUG
                    if (!fd->is_repeated())
                    {
                        fprintf(stderr, "  [ERR] fd not repeated for vector field\n");
                    }
#endif
                    if (len > 0)
                    {
                        ensureRepeatedSize(fd, (int)len);
#ifdef DTT_DEBUG
                        fprintf(stderr, "  [DBG] ensureRepeatedSize -> FieldSize=%d\n", refl->FieldSize(*m, fd));
#endif
                    }

                    for (uint32_t i = 0; i < len; ++i)
                    {
                        const auto vn = DataTamerParser::DeserializeToVarNumber(tf.type, buf);
                        set_repeated_from_var(fd, (int)i, vn);
#ifdef DTT_DEBUG
                        fprintf(stderr, "  [DBG] wrote idx=%u -> FieldSize=%d\n", i, refl->FieldSize(*m, fd));
#endif
                    }
                }
                else
                {
                    // skip payload entries (still must advance buf)
                    for (uint32_t i = 0; i < len; ++i)
                    {
                        (void)DataTamerParser::DeserializeToVarNumber(tf.type, buf);
                    }
                }

// IMPORTANT: advance the leaf cursor ONCE for the whole vector
#ifdef DTT_DEBUG
                fprintf(stderr, "  [DBG] VEC-BASIC end FieldSize=%d\n", refl->FieldSize(*m, fd));
#endif
                ++leaf_i;
            }
            return;
        }
        else
        {
            // Composite (OTHER)
            auto it = schema.custom_types.find(tf.type_name);
            if (it == schema.custom_types.end())
            {
#ifdef DTT_DEBUG
                fprintf(stderr, "  [DBG] COMPOSITE '%s' not found; skipping\n", tf.type_name.c_str());
#endif
                return;
            }
            const auto& subs = it->second;

            if (!tf.is_vector)
            {
#ifdef DTT_DEBUG
                fprintf(stderr, "  [DBG] COMPOSITE '%s' single\n", tf.field_name.c_str());
#endif
                for (const auto& sub : subs)
                {
                    process_field(sub, write);
                }
                return;
            }

            // Vector of composite: payload is interleaved per element: (sub0, sub1, ...), repeated len times
            uint32_t len = tf.array_size ? tf.array_size : DataTamerParser::Deserialize<uint32_t>(buf);
#ifdef DTT_DEBUG
            fprintf(stderr, "  [DBG] COMPOSITE '%s' vector len=%u\n", tf.field_name.c_str(), len);
#endif

            // 1) Anchor FDs for each subfield (advance leaf_i once per subfield)
            std::vector<const google::protobuf::FieldDescriptor*> sub_fds;
            sub_fds.reserve(subs.size());
            for ([[maybe_unused]] const auto& sub : subs)
            {
                if (leaf_i >= rt.flat.size())
                    throw std::runtime_error("leaf cursor OOR (vec composite anchor)");
                const auto* fd = rt.flat[leaf_i].fd;
#ifdef DTT_DEBUG
                dbg_fd(sub, fd, "VEC-COMP-ANCHOR", (int)len);
#endif
                if (write && fd && fd->is_repeated() && len > 0)
                {
                    ensureRepeatedSize(fd, (int)len);
                }
                sub_fds.push_back(fd);
                ++leaf_i;  // move to next subfield anchor
            }

            // 2) Consume payload element-by-element; write to index i of each subfield
            for (uint32_t i = 0; i < len; ++i)
            {
                for (size_t j = 0; j < subs.size(); ++j)
                {
                    const auto& sub = subs[j];
                    const auto* fd = sub_fds[j];

                    if (sub.type != DataTamerParser::BasicType::OTHER)
                    {
                        const auto vn = DataTamerParser::DeserializeToVarNumber(sub.type, buf);
                        if (write && fd)
                            set_repeated_from_var(fd, (int)i, vn);
                    }
                    else
                    {
                        std::ostringstream oss;
                        oss << "Unhandled composite subfield type: " << static_cast<int>(sub.type) << " (field: " << sub.field_name << ")";
                        throw std::runtime_error(oss.str());
                    }
                }
            }
            return;
        }
    };

    // Walk top-level fields by active mask (same order as payload layout)
    for (size_t i = 0; i < schema.fields.size(); ++i)
    {
        bool active = DataTamerParser::GetBit(sv.active_mask, i);
#ifdef DTT_DEBUG
        const auto& tf = schema.fields[i];
        fprintf(stderr, "[DBG] top field#%zu name='%s' type=%s is_vector=%d array_size=%u active=%d\n", i, tf.field_name.c_str(), btName(tf.type), (int)tf.is_vector,
                tf.array_size, (int)active);
#endif
        if (!active)
            continue;
        process_field(schema.fields[i], /*write=*/true);
    }

    const auto need = m->ByteSizeLong();
    out_bytes.resize(need);  // exact size up front
    return m->SerializeToArray(out_bytes.data(), need);
}

bool parseFramedSnapshot(const uint8_t* data, size_t size, DataTamerParser::SnapshotView& out)
{
    if (!data)
        return false;
    DataTamerParser::BufferSpan buf{ data, size };
    if (buf.size < sizeof(uint32_t))
        return false;
    const uint32_t mask_len = DataTamerParser::Deserialize<uint32_t>(buf);
    if (buf.size < mask_len + sizeof(uint32_t))
        return false;
    out.active_mask.data = buf.data;
    out.active_mask.size = mask_len;
    buf.trimFront(mask_len);
    const uint32_t payload_len = DataTamerParser::Deserialize<uint32_t>(buf);
    if (buf.size < payload_len)
        return false;
    out.payload.data = buf.data;
    out.payload.size = payload_len;
    return true;
}
}  // namespace data_tamer_tools
