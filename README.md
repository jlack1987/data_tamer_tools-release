# Data Tamer Tools

Tools for working with DataTamer data in ROS2, providing Foxglove integration and MCAP data storage capabilities.

## Overview

This package provides three main tools:
- **Foxglove Relay**: Real-time visualization of DataTamer data through Foxglove Studio
- **MCAP Sink**: Store DataTamer data in MCAP (MessagePack) format for efficient storage and playback
- **MCAP Converter**: Convert DataTamer-encoded MCAP files to Protobuf-encoded MCAP files for Foxglove compatibility

## Tools

### 1. Foxglove Relay (`foxglove_relay`)

A ROS2 component that bridges DataTamer data to Foxglove Studio for real-time visualization.

**Features:**
- Automatically discovers DataTamer topics (`/data_tamer/schema`, `/data_tamer/schemas`, `/data_tamer/snapshot`)
- Supports both JSON and Protocol Buffers encoding for Foxglove visualization
- Converts DataTamer schemas to JSON Schema or Protocol Buffers descriptor format
- Creates Foxglove WebSocket server for real-time data streaming
- Supports soft eviction of idle channels (closes channels but keeps schemas)
- Configurable eviction policies and discovery intervals
- Optional ROS log relaying to Foxglove

**Prerequisites:**
- Your DataTamer channels must use `DataTamer::ROS2PublisherSink` to publish data to ROS2 topics
- The relay automatically discovers and subscribes to the standard DataTamer ROS2 topics

**Usage:**
```bash
# Launch as ROS2 component
ros2 run data_tamer_tools foxglove_relay

# Or with custom parameters
ros2 run data_tamer_tools foxglove_relay --ros-args \
  -p host:=127.0.0.1 \
  -p port:=8765 \
  -p use_protobuf:=true \
  -p eviction_ttl_sec:=900 \
  -p eviction_period_sec:=30 \
  -p discovery_sec:=5 \
  -p enable_rosout:=true
```

**Parameters:**

**WebSocket Server Configuration:**
- `host` (string, default: "127.0.0.1"): WebSocket server host address
- `port` (int, default: 8765): WebSocket server port number

**Data Encoding:**
- `use_protobuf` (bool, default: true): Use Protocol Buffers encoding instead of JSON
  - `true`: Data is serialized using Protocol Buffers for better performance and smaller payloads
  - `false`: Data is serialized as JSON strings for human readability

**Channel Management:**
- `eviction_ttl_sec` (int, default: 900): Time-to-live for idle channels in seconds
  - Channels that haven't received data for this duration will be soft-evicted (closed but schema kept)
- `eviction_period_sec` (int, default: 30): Interval in seconds for checking stale channels
  - How often the relay checks for channels that should be evicted
- `discovery_sec` (int, default: 5): Interval in seconds for topic discovery
  - How often the relay scans for new DataTamer topics to subscribe to

**ROS Log Integration:**
- `enable_rosout` (bool, default: true): Enable relaying of ROS logs to Foxglove
  - `true`: ROS logs from `/rosout` topic are relayed to Foxglove Log channel
  - `false`: Disable ROS log relaying
- `rosout_topic` (string, default: "/rosout"): ROS topic name for log messages
- `rosout_min_level` (int, default: 20): Minimum log level to relay (10=DEBUG, 20=INFO, 30=WARN, 40=ERROR, 50=FATAL)

**Foxglove Studio Connection:**
1. Open Foxglove Studio
2. Add a "Raw Messages" panel
3. Connect to `ws://localhost:8765` (or your configured host:port)
4. Select the desired DataTamer channel to visualize

**Encoding Notes:**
- **Protocol Buffers mode** (`use_protobuf:=true`): More efficient, smaller payloads, better performance
- **JSON mode** (`use_protobuf:=false`): Human-readable data, easier to debug, larger payloads
- Both modes provide the same data visualization capabilities in Foxglove Studio

### 2. MCAP Sink

Stores DataTamer data in MCAP (MessagePack) format for efficient storage and playback.

**Features:**
- Multiple compression options (None, LZ4, Zstd)
- Configurable chunk sizes
- Thread-safe operations
- Integration with DataTamer sink system

**Usage:**
```cpp
#include <data_tamer_tools/sinks/mcap_sink.hpp>

// Create MCAP sink
auto mcap_sink = std::make_shared<data_tamer_tools::McapSink>(
    "/path/to/file.mcap", 
    data_tamer_tools::McapSink::Compression::Zstd, 
    1024  // chunk size
);

// Add to DataTamer channel
auto channel = DataTamer::ChannelsRegistry::Global().getChannel("my_channel");
channel->addDataSink(mcap_sink);
```

**Compression Options:**
- `Compression::None`: No compression
- `Compression::LZ4`: Fast compression
- `Compression::Zstd`: High compression ratio

### 3. MCAP Converter (`mcap_converter`)

Converts DataTamer-encoded MCAP files to Protobuf-encoded MCAP files for Foxglove Studio compatibility.

**Features:**
- Converts DataTamer's custom message encoding to standard Protobuf encoding
- Preserves all message timestamps and metadata
- Uses Zstd compression for efficient output files
- Maintains channel and schema information
- Enables visualization of DataTamer data in Foxglove Studio

**Usage:**
```bash
# Convert a DataTamer MCAP file to Foxglove-compatible format
ros2 run data_tamer_tools mcap_converter input.mcap output.mcap

# With custom compression and chunk size
ros2 run data_tamer_tools mcap_converter input.mcap output.mcap \
  --compression lz4 \
  --chunk-size 1048576
```

**Command-line Options:**
- `--compression <type>`: Compression type (`zstd`, `lz4`, or `none`). Default: `zstd`
- `--chunk-size <size>`: Chunk size in bytes (positive integer). Default: writer default

**What it does:**
1. **Reads DataTamer MCAP files**: Parses MCAP files created with DataTamer's custom encoding
2. **Converts schemas**: Transforms DataTamer schemas to Protobuf descriptors
3. **Re-encodes messages**: Converts DataTamer snapshot data to Protobuf format
4. **Preserves metadata**: Maintains timestamps, channel names, and message sequences
5. **Outputs Foxglove-compatible MCAP**: Creates MCAP files that can be opened directly in Foxglove Studio

**Use Cases:**
- **Post-processing**: Convert recorded DataTamer sessions for analysis in Foxglove
- **Data sharing**: Share DataTamer recordings with team members who use Foxglove
- **Visualization**: Enable rich visualization of DataTamer data using Foxglove's plotting capabilities
- **Debugging**: Inspect DataTamer data using Foxglove's message inspection tools

**Example Workflow:**
```bash
# 1. Record data with DataTamer (creates data_tamer_encoded.mcap)
ros2 run your_package your_node

# 2. Convert to Foxglove-compatible format
ros2 run data_tamer_tools mcap_converter data_tamer_encoded.mcap foxglove_compatible.mcap

# 3. Open in Foxglove Studio
# - File -> Open Local File -> Select foxglove_compatible.mcap
# - Add panels to visualize your data
```

**Technical Details:**
- **Input**: MCAP files with DataTamer's custom message encoding (JSON Schema + binary snapshots)
- **Output**: MCAP files with Protobuf encoding (Protocol Buffers descriptors + Protobuf messages)
- **Compression**: Output files use Zstd compression for efficiency
- **Schema conversion**: DataTamer schemas are converted to Protobuf FileDescriptorSets
- **Message conversion**: DataTamer snapshots are re-encoded as Protobuf messages

## Examples

### Example: Basic MCAP Recording
```cpp
// See examples/json_mcap_sink.cpp
#include <data_tamer_tools/sinks/mcap_sink.hpp>

auto mcap_sink = std::make_shared<data_tamer_tools::McapSink>(
    "/tmp/example.mcap", 
    data_tamer_tools::Format::Protobuf,
    data_tamer_tools::McapSink::Compression::None, 
    1024
);

auto channel = DataTamer::ChannelsRegistry::Global().getChannel("sensor_data");
channel->addDataSink(mcap_sink);

// Register and log data
double temperature = 22.5;
channel->registerValue("temperature", &temperature);
channel->takeSnapshot();
```

## Building

1. Install dependencies:
```bash
rosdep install --from-paths src --ignore-src -r -y
```

2. Build with colcon:
```bash
cd /path/to/your/workspace
colcon build --packages-select data_tamer_tools
```

## License

MIT License - see LICENSE file for details.
