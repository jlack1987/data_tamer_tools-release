#pragma once

#include <data_tamer/data_tamer.hpp>
#include <data_tamer_parser/data_tamer_parser.hpp>
#include <nlohmann/json.hpp>

#include <memory>
#include <string>
#include <unordered_map>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>

namespace data_tamer_tools
{
nlohmann::json serializeSnapshotToJson(const DataTamer::Snapshot& snapshot, const DataTamer::Schema& schema);
nlohmann::json serializeSnapshotToJson(const DataTamerParser::Schema& schema, const std::vector<uint8_t>& active_mask, const std::vector<uint8_t>& payload,
                                       const std::chrono::nanoseconds& timestamp);
nlohmann::json convertToJSONSchema(const DataTamer::Schema& schema);
nlohmann::json convertToJSONSchema(const DataTamerParser::Schema& schema);
std::string convertTypeToJSON(DataTamer::BasicType type);
std::string convertTypeToJSON(DataTamerParser::BasicType type);

// Future-proofing in case you want hierarchical messages later.
enum class NameStyle
{
    FlatHashed /*, Hierarchical*/
};

struct FlatField
{
    std::string full_path;          // canonical: "arm/joint/pos", "voltages", ...
    DataTamerParser::TypeField tf;  // copied & normalized for this leaf
    const google::protobuf::FieldDescriptor* fd = nullptr;
};

// Built runtime for a schema.
struct ProtoRuntime
{
    std::unique_ptr<google::protobuf::DescriptorPool> pool;
    std::unique_ptr<google::protobuf::DynamicMessageFactory> factory;
    const google::protobuf::Descriptor* desc = nullptr;
    std::string full_type;    // e.g. "dtfox.rt.Snap_<hash>"
    std::string fdset_bytes;  // serialized FileDescriptorSet
    // Keyed by CANONICAL DataTamer path (indices stripped, slashes kept), e.g. "accel", "ids", "pose/x"
    std::unordered_map<std::string, const google::protobuf::FieldDescriptor*> field_by_name;
    std::vector<FlatField> flat;
};

enum class OpCode : uint8_t
{
    SCALAR,           // payload += sizeof(elem)
    VEC_FIXED,        // read N elements (N known at compile time)
    VEC_DYNAMIC,      // read u32 length, then N elements
    ENTER_COMPOSITE,  // for OTHER type
    LEAVE_COMPOSITE,
    // vector of composite:
    VEC_FIXED_COMPOSITE,
    VEC_DYNAMIC_COMPOSITE
};

struct Op
{
    OpCode code;
    DataTamerParser::BasicType elem;  // for SCALAR/VEC_* of basic types
    uint32_t count;                   // for *_FIXED (array_size)
    uint32_t leaf_idx;                // index into FieldProgram::leaves (for SCALAR/VEC_* of basic types)
};

struct FieldProgram
{
    // Flat program for THIS top-level field
    std::vector<Op> ops;
    // Leaf writers for this field (index-compatible with ops[].leaf_idx)
    std::vector<FlatField> leaves;
};

struct CompiledSchema
{
    uint64_t schema_hash = 0;
    // One program per top-level field (same order as schema.fields)
    std::vector<FieldProgram> fields;
    // quick size table for BT
    uint8_t size_of[/*BT count*/ 12];
};

// Build a dynamic proto for a DataTamer schema.
// - FlatHashed: single-level message; field name = sanitized(canonical_path) + "_" + 8-hex hash to avoid collisions.
ProtoRuntime buildProto(const DataTamerParser::Schema& schema);

// Encode a SnapshotView into protobuf bytes using a prepared runtime.
// If 'scratch' is provided, it will be used (cleared) to avoid allocations.
bool encodeSnapshot(const DataTamerParser::Schema& schema, const DataTamerParser::SnapshotView& sv, const ProtoRuntime& rt, std::string& out_bytes,
                    google::protobuf::Message* scratch = nullptr);

// Parse DataTamer framing from a raw buffer into a SnapshotView that aliases the input memory:
// Layout: [u32 mask_len][mask][u32 payload_len][payload]
bool parseFramedSnapshot(const uint8_t* data, size_t size, DataTamerParser::SnapshotView& out);

// Utility: canonical DT path = remove [indices], keep '/' hierarchy.
std::string stripIndicesKeepSlashes(const std::string& s);

bool isDataTamerFile(const std::string& filepath);
void flattenType(const std::string& prefix, const DataTamerParser::TypeField& f, const std::map<std::string, DataTamerParser::FieldsVector>& custom_types,
                 std::vector<FlatField>& out);
std::vector<FlatField> flattenSchema(const DataTamerParser::Schema& s);
}  // namespace data_tamer_tools
