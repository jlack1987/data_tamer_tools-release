from launch.conditions import IfCondition
from launch.launch_description import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import ComposableNodeContainer
from launch_ros.descriptions import ComposableNode


def launch_setup(context, *args, **kwargs):
    return ([
            ComposableNodeContainer(
                name='logging_container',
                namespace=LaunchConfiguration('ns'),
                package='rclcpp_components',
                executable='component_container_mt',
                composable_node_descriptions=[
                    # Foxglove Bridge - relays DataTamer snapshots and rosout to Foxglove
                    ComposableNode(
                        package='data_tamer_tools',
                        namespace=LaunchConfiguration('ns'),
                        plugin='data_tamer_tools::DtRos2ToFoxgloveBridge',
                        name='foxglove_relay',
                        condition=IfCondition(LaunchConfiguration('relay')),
                        parameters=[{
                            'host': LaunchConfiguration('relay_host'),
                            'port': LaunchConfiguration('relay_port'),
                            'eviction_ttl_sec': LaunchConfiguration('relay_eviction_ttl_sec'),
                            'eviction_period_sec': LaunchConfiguration('relay_eviction_period_sec'),
                            'discovery_sec': LaunchConfiguration('relay_discover_sec'),
                            'enable_rosout': LaunchConfiguration('relay_enable_rosout'),
                            'rosout_topic': LaunchConfiguration('rosout_topic'),
                            'use_protobuf': LaunchConfiguration('relay_use_protobuf')
                        }]
                    ),
                    # Rosout Logger - logs rosout messages to MCAP files
                    ComposableNode(
                        package='data_tamer_tools',
                        namespace=LaunchConfiguration('ns'),
                        plugin='data_tamer_tools::RosoutLogger',
                        name='rosout_logger',
                        condition=IfCondition(LaunchConfiguration('log_rosout')),
                        parameters=[{
                            'output_dir': LaunchConfiguration('logdir'),
                            'rosout_topic': LaunchConfiguration('rosout_topic'),
                            'output_base': LaunchConfiguration('logger_output_base'),
                            'compression': LaunchConfiguration('logger_compression'),
                            'chunk_size': LaunchConfiguration('logger_chunk_size'),
                            'rotate_dir_topic': LaunchConfiguration('logger_rotate_dir_topic')
                        }]
                    ),
                    # Log Rotation Coordinator - coordinates log rotation across loggers
                    ComposableNode(
                        package='data_tamer_tools',
                        namespace=LaunchConfiguration('ns'),
                        plugin='data_tamer_tools::LogRotationCoordinator',
                        name='log_rotation_coordinator',
                        condition=IfCondition(LaunchConfiguration('rotation_coordinator')),
                        parameters=[{
                            'rotate_topic': LaunchConfiguration('coordinator_rotate_topic'),
                            'service_name': LaunchConfiguration('coordinator_service_name')
                        }]
                    )
                ],
                output='screen',
            )
            ])


def generate_launch_description():
    launch_description = LaunchDescription([
        # General arguments
        DeclareLaunchArgument('ns', default_value='data_tamer_tools',
                              description='Namespace for all nodes'),
        DeclareLaunchArgument('logdir', default_value='.',
                              description='Output directory for log files'),

        # Foxglove Bridge parameters
        DeclareLaunchArgument('relay_host', default_value='127.0.0.1',
                              description='Foxglove WebSocket server host address'),
        DeclareLaunchArgument('relay_port', default_value='8765',
                              description='Foxglove WebSocket server port (0-65535)'),
        DeclareLaunchArgument('relay_eviction_ttl_sec', default_value='900',
                              description='Time to live for stale publishers in seconds (default: 15min)'),
        DeclareLaunchArgument('relay_eviction_period_sec', default_value='30',
                              description='Check for stale publishers every N seconds'),
        DeclareLaunchArgument('relay_discover_sec', default_value='5',
                              description='Period for rediscovering DataTamer snapshot topics in seconds'),
        DeclareLaunchArgument('relay_enable_rosout', default_value='true',
                              description='Enable relaying /rosout to Foxglove (true/false)'),
        DeclareLaunchArgument('rosout_topic', default_value='/rosout',
                              description='ROS topic to subscribe for logs (Foxglove relay)'),
        DeclareLaunchArgument('relay_use_protobuf', default_value='true',
                              description='Use protobuf encoding instead of JSON (true/false)'),

        # Rosout Logger parameters
        DeclareLaunchArgument('logger_output_base', default_value='rosout',
                              description='Base filename for MCAP output files'),
        DeclareLaunchArgument('logger_compression', default_value='zstd',
                              description='MCAP compression type: none, zstd, or lz4'),
        DeclareLaunchArgument('logger_chunk_size', default_value='0',
                              description='MCAP chunk size in bytes (0 = no chunking)'),
        DeclareLaunchArgument('logger_rotate_dir_topic', default_value='/data_tamer/rotate_dir',
                              description='Topic for receiving log rotation commands'),

        # Log Rotation Coordinator parameters
        DeclareLaunchArgument('coordinator_rotate_topic', default_value='/data_tamer/rotate_dir',
                              description='Topic to publish log rotation commands'),
        DeclareLaunchArgument('coordinator_service_name', default_value='/data_tamer/loggers/rotate',
                              description='Service name for triggering log rotation'),

        # Condition arguments
        DeclareLaunchArgument('relay', default_value='True',
                              description='Enable Foxglove relay (True/False)'),
        DeclareLaunchArgument('log_rosout', default_value='True',
                              description='Enable ROSout logging (True/False)'),
        DeclareLaunchArgument('rotation_coordinator', default_value='True',
                              description='Enable log rotation coordinator (True/False)'),
        OpaqueFunction(function=launch_setup)
    ])

    return launch_description
