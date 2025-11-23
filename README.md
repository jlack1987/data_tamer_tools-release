# Data Tamer Tools

Tools for working with DataTamer data in ROS2, providing Foxglove integration and MCAP data storage capabilities.

## Overview

This package provides five main tools:
- **Foxglove Relay**: Real-time visualization of DataTamer data through Foxglove Studio
- **MCAP Sink**: Store DataTamer data in MCAP (MessagePack) format for efficient storage and playback
- **MCAP Converter**: Convert DataTamer-encoded MCAP files to Protobuf-encoded MCAP files for Foxglove compatibility
- **Rosout Logger**: Record ROS log messages (`/rosout`) to MCAP files with Foxglove Log schema
- **Log Rotation Coordinator**: Synchronize log directory rotation across multiple MCAP loggers

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
- **Log rotation support**: When initialized with a ROS node, automatically subscribes to rotation topic
- **Synchronized rotation**: Multiple MCAP sinks can rotate to new directories simultaneously

**Basic Usage:**
```cpp
#include <data_tamer_tools/sinks/mcap_sink.hpp>

// Create MCAP sink (standalone)
auto mcap_sink = std::make_shared<data_tamer_tools::McapSink>(
    "/path/to/file.mcap", 
    data_tamer_tools::Format::Protobuf,
    data_tamer_tools::McapSink::Compression::Zstd, 
    1024  // chunk size
);

// Add to DataTamer channel
auto channel = DataTamer::ChannelsRegistry::Global().getChannel("my_channel");
channel->addDataSink(mcap_sink);
```

**Log Rotation Usage:**
```cpp
#include <data_tamer_tools/sinks/mcap_sink.hpp>

// Create MCAP sink with ROS node for rotation support
auto node = std::make_shared<rclcpp::Node>("my_node");
auto mcap_sink = std::make_shared<data_tamer_tools::McapSink>(
    node,
    "/initial/path/data.mcap",
    data_tamer_tools::Format::Protobuf,
    data_tamer_tools::McapSink::Compression::Zstd
);

// Add to DataTamer channel
auto channel = DataTamer::ChannelsRegistry::Global().getChannel("my_channel");
channel->addDataSink(mcap_sink);

// The sink now listens to the rotation topic (default: /data_tamer/rotate_dir)
// When a LogDir message is received, it will:
// 1. Close the current MCAP file
// 2. Create the new directory if needed
// 3. Open a new MCAP file at <new_dir>/<base_filename>
// 4. Re-register all channels and continue recording
```

**Parameters:**
- `rotate_topic` (string, default: "/data_tamer/rotate_dir"): Topic to subscribe for rotation commands

**Compression Options:**
- `Compression::None`: No compression
- `Compression::LZ4`: Fast compression
- `Compression::Zstd`: High compression ratio

**Format Options:**
- `Format::Json`: JSON encoding with JSON schema
- `Format::Protobuf`: Protocol Buffers encoding (recommended for Foxglove compatibility)

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

### 4. Rosout Logger (`rosout_logger`)

Records ROS log messages (`/rosout`) to MCAP files using the Foxglove Log schema for seamless playback in Foxglove Studio.

**Features:**
- Subscribes to ROS `/rosout` topic and records all log messages
- Encodes logs using Foxglove Log schema (Protobuf format)
- Supports log rotation via `LogDir` topic
- Configurable compression and chunk size
- Thread-safe MCAP writing
- Creates timestamped log files on startup
- Synchronized rotation with other MCAP loggers

**Usage:**
```bash
# Launch as ROS2 component with defaults
ros2 run data_tamer_tools rosout_logger

# Or with custom parameters
ros2 run data_tamer_tools rosout_logger --ros-args \
  -p rosout_topic:=/rosout \
  -p output_base:=rosout \
  -p output_dir:=/logs \
  -p compression:=zstd \
  -p chunk_size:=0 \
  -p rotate_dir_topic:=/data_tamer/rotate_dir
```

**Parameters:**
- `rosout_topic` (string, default: "/rosout"): ROS topic to subscribe for log messages
- `output_base` (string, default: "rosout"): Base filename for MCAP files
- `output_dir` (string, default: "."): Directory for initial MCAP file
- `compression` (string, default: "zstd"): Compression type (none, zstd, or lz4)
- `chunk_size` (int, default: 0): Chunk size in bytes (0 = no chunking)
- `rotate_dir_topic` (string, default: "/data_tamer/rotate_dir"): Topic to subscribe for rotation commands

**How it works:**
1. On startup, creates a timestamped MCAP file: `<output_dir>/<output_base>_YYYYMMDD_HHMMSS.mcap`
2. Subscribes to `/rosout` and records all log messages in Foxglove Log format
3. Listens to the rotation topic (latched/transient_local)
4. When a `LogDir` message is received with a new directory:
   - Closes the current MCAP file
   - Creates the new directory if needed
   - Opens a new file at `<new_dir>/rosout.mcap` (fixed filename for rotated logs)
   - Continues recording with monotonic sequence numbers

**Foxglove Integration:**
The recorded MCAP files can be opened directly in Foxglove Studio and will display logs in the Log panel with proper severity levels, timestamps, and message details.

### 5. Log Rotation Coordinator (`log_rotation_coordinator`)

Coordinates synchronized log directory rotation across multiple MCAP loggers (data_tamer_tools::McapSink sinks and Rosout logger).

**Features:**
- Provides a ROS service to trigger rotation
- Publishes rotation commands on a latched topic
- Ensures all loggers rotate simultaneously to the same directory
- Simple service-based API for external control

**Usage:**
```bash
# Launch the coordinator
ros2 run data_tamer_tools log_rotation_coordinator

# Or with custom parameters
ros2 run data_tamer_tools log_rotation_coordinator --ros-args \
  -p rotate_topic:=/data_tamer/rotate_dir \
  -p service_name:=/data_tamer/loggers/rotate
```

**Parameters:**
- `rotate_topic` (string, default: "/data_tamer/rotate_dir"): Topic name for publishing rotation commands
- `service_name` (string, default: "/data_tamer/loggers/rotate"): Service name for rotation requests

**Triggering Rotation:**
```bash
# Rotate all loggers to a new directory
ros2 service call /data_tamer/loggers/rotate data_tamer_tools/srv/Rotate "{directory: '/logs/session_001'}"
```

**How it works:**
1. The coordinator provides a ROS service that accepts a directory path
2. When the service is called, it creates the new logdir if it doesn't xist and publishes a `LogDir` message on the rotation topic
3. The topic uses **transient_local** QoS (latched), so:
   - All current subscribers receive the message immediately
   - Late-joining subscribers (loggers started after rotation) also receive the last message
4. All MCAP sinks and Rosout loggers subscribed to this topic will:
   - Close their current files
   - Create the new directory if needed
   - Open new files in the specified directory
   - Continue recording seamlessly

**Synchronized Logging Architecture:**
```
┌─────────────────────────┐
│  Log Rotation           │
│  Coordinator            │
│  (Service Provider)     │
└───────────┬─────────────┘
            │ publishes LogDir (latched)
            ├──────────────┐──────────────┐
            ↓              ↓              ↓
    /data_tamer/rotate_dir topic         topic
            ↓              ↓              ↓
    ┌───────┴────┐   ┌────┴──────┐   ┌──────────┐
    │ McapSink 1 │   │ McapSink 2│   │  Rosout  │
    │ (DataTamer)│   │(DataTamer)│   │  Logger  │
    └────────────┘   └───────────┘   └──────────┘
         ↓                 ↓               ↓
    data1.mcap        data2.mcap      rosout.mcap
         (all in the same directory)
```

**Use Cases:**
- **Session-based logging**: Organize logs by test session or experiment
- **Time-based rotation**: Rotate to hourly/daily directories
- **Event-triggered rotation**: Switch directories on specific events
- **Multi-robot systems**: Coordinate logging across multiple robots

## Launch Files

### Bringup Launch File

The package provides a comprehensive launch file (`bringup.launch.py`) that starts all three components (Foxglove Relay, Rosout Logger, and Log Rotation Coordinator) in a single composable node container for efficient execution.

**Quick Start:**
```bash
# Launch with all default settings
ros2 launch data_tamer_tools bringup.launch.py

# Launch with custom log directory
ros2 launch data_tamer_tools bringup.launch.py logdir:=/tmp/my_logs

# Launch with custom Foxglove port
ros2 launch data_tamer_tools bringup.launch.py relay_port:=9000

# Disable specific components
ros2 launch data_tamer_tools bringup.launch.py relay:=False log_rosout:=False
```

**Architecture:**
The launch file creates a multi-threaded composable node container (`component_container_mt`) that hosts all three components, providing:
- Efficient inter-component communication
- Shared namespace configuration
- Coordinated lifecycle management
- Lower resource overhead compared to separate nodes

**Launch Arguments:**

**General Configuration:**
- `ns` (string, default: "data_tamer_tools"): Namespace for all nodes
- `logdir` (string, default: "."): Output directory for log files

**Component Enable/Disable:**
- `relay` (bool, default: True): Enable/disable Foxglove Relay component
- `log_rosout` (bool, default: True): Enable/disable Rosout Logger component
- `rotation_coordinator` (bool, default: True): Enable/disable Log Rotation Coordinator component

**Foxglove Relay Parameters:**
- `relay_host` (string, default: "127.0.0.1"): WebSocket server host address
- `relay_port` (int, default: 8765): WebSocket server port (0-65535)
- `relay_eviction_ttl_sec` (int, default: 900): Time-to-live for stale publishers in seconds (15 minutes)
- `relay_eviction_period_sec` (int, default: 30): Interval for checking stale publishers
- `relay_discover_sec` (int, default: 5): Period for rediscovering DataTamer snapshot topics
- `relay_enable_rosout` (bool, default: true): Enable relaying /rosout to Foxglove
- `relay_use_protobuf` (bool, default: true): Use Protobuf encoding instead of JSON

**Rosout Logger Parameters:**
- `logger_output_base` (string, default: "rosout"): Base filename for MCAP output files
- `logger_compression` (string, default: "zstd"): Compression type (none, zstd, or lz4)
- `logger_chunk_size` (int, default: 0): MCAP chunk size in bytes (0 = no chunking)
- `logger_rotate_dir_topic` (string, default: "/data_tamer/rotate_dir"): Topic for receiving rotation commands

**Log Rotation Coordinator Parameters:**
- `coordinator_rotate_topic` (string, default: "/data_tamer/rotate_dir"): Topic to publish rotation commands
- `coordinator_service_name` (string, default: "/data_tamer/loggers/rotate"): Service name for rotation requests

**Shared Parameters:**
- `rosout_topic` (string, default: "/rosout"): ROS topic to subscribe for logs (used by both Foxglove Relay and Rosout Logger)

**Common Usage Patterns:**

**1. Production Deployment with Custom Settings:**
```bash
ros2 launch data_tamer_tools bringup.launch.py \
  ns:=robot_logger \
  logdir:=/data/robot_logs \
  relay_host:=0.0.0.0 \
  relay_port:=8765 \
  logger_compression:=lz4 \
  relay_use_protobuf:=true
```

**2. Development/Debugging (JSON mode, disable logger):**
```bash
ros2 launch data_tamer_tools bringup.launch.py \
  relay_use_protobuf:=false \
  log_rosout:=False \
  relay_discover_sec:=2
```

**3. Logging Only (no live visualization):**
```bash
ros2 launch data_tamer_tools bringup.launch.py \
  relay:=False \
  logdir:=/logs/experiment_001
```

**4. Remote Foxglove Access:**
```bash
ros2 launch data_tamer_tools bringup.launch.py \
  relay_host:=0.0.0.0 \
  relay_port:=8765
# Now accessible from any machine: ws://<robot_ip>:8765
```

**5. High-Frequency Logging (optimized settings):**
```bash
ros2 launch data_tamer_tools bringup.launch.py \
  logger_compression:=lz4 \
  logger_chunk_size:=1048576 \
  relay_discover_sec:=10 \
  relay_eviction_period_sec:=60
```

**Typical Workflow with Launch File:**
```bash
# 1. Start the complete logging system
ros2 launch data_tamer_tools bringup.launch.py logdir:=/logs/initial

# 2. Connect Foxglove Studio to ws://localhost:8765

# 3. Run your application with DataTamer integration
ros2 run your_package your_node

# 4. Trigger log rotation for organized sessions
ros2 service call /data_tamer/loggers/rotate data_tamer_tools/srv/Rotate \
  "{directory: '/logs/test_run_001'}"

# 5. All logs now go to /logs/test_run_001/:
#    - rosout.mcap (ROS logs)
#    - <your_datatamer_files>.mcap (DataTamer data)
```

**Integration with Your Launch Files:**
```python
from launch import LaunchDescription
from launch.actions import IncludeLaunchDescription
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch_ros.substitutions import FindPackageShare
from launch.substitutions import PathJoinSubstitution

def generate_launch_description():
    return LaunchDescription([
        # Include data_tamer_tools bringup
        IncludeLaunchDescription(
            PythonLaunchDescriptionSource([
                PathJoinSubstitution([
                    FindPackageShare('data_tamer_tools'),
                    'launch',
                    'bringup.launch.py'
                ])
            ]),
            launch_arguments={
                'ns': 'my_robot/logging',
                'logdir': '/data/logs',
                'relay_port': '9000',
                'logger_compression': 'lz4',
            }.items()
        ),
        
        # Your other nodes...
    ])
```

**Benefits of Using the Launch File:**
- **Single command**: Launch all logging infrastructure at once
- **Coordinated configuration**: Shared parameters ensure consistency
- **Resource efficiency**: Composable nodes reduce overhead
- **Easy customization**: Override any parameter without modifying code
- **Production ready**: Sensible defaults for most use cases
- **Modular**: Disable components you don't need

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

### Example: Synchronized Log Rotation System
```cpp
// In your DataTamer-enabled node
#include <data_tamer_tools/sinks/mcap_sink.hpp>
#include <rclcpp/rclcpp.hpp>

class MyRobotNode : public rclcpp::Node {
public:
    MyRobotNode() : Node("my_robot") {
        // Create MCAP sink with rotation support
        auto mcap_sink = std::make_shared<data_tamer_tools::McapSink>(
            shared_from_this(),  // Pass the node for rotation
            "/initial/logs/robot_data.mcap",
            data_tamer_tools::Format::Protobuf,
            data_tamer_tools::McapSink::Compression::Zstd
        );
        
        // Set up DataTamer channel
        auto channel = DataTamer::ChannelsRegistry::Global().getChannel("robot_state");
        channel->addDataSink(mcap_sink);
        
        // Register values
        channel->registerValue("position_x", &position_x_);
        channel->registerValue("velocity", &velocity_);
        
        // Timer to take snapshots
        timer_ = create_wall_timer(
            std::chrono::milliseconds(100),
            [channel]() { channel->takeSnapshot(); }
        );
    }
    
private:
    double position_x_{0.0};
    double velocity_{0.0};
    rclcpp::TimerBase::SharedPtr timer_;
};
```

**Complete System Launch:**
```bash
# Terminal 1: Launch the rotation coordinator
ros2 run data_tamer_tools log_rotation_coordinator

# Terminal 2: Launch rosout logger
ros2 run data_tamer_tools rosout_logger --ros-args -p output_dir:=/logs/initial

# Terminal 3: Launch your robot node with DataTamer
ros2 run my_package my_robot_node

# Terminal 4: Trigger rotation to organize logs by session
ros2 service call /data_tamer/loggers/rotate data_tamer_tools/srv/Rotate "{directory: '/logs/test_session_001'}"

# Now all loggers (DataTamer MCAP sinks + Rosout logger) write to /logs/test_session_001/
# - /logs/test_session_001/robot_data.mcap
# - /logs/test_session_001/rosout.mcap

# Rotate again for a new session
ros2 service call /data_tamer/loggers/rotate data_tamer_tools/srv/Rotate "{directory: '/logs/test_session_002'}"
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

---

## Support

If you find this project useful, consider buying me a coffee! ☕

[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-support-yellow.svg?style=flat&logo=buy-me-a-coffee)](https://buymeacoffee.com/jlack)
