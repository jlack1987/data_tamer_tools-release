cmake_minimum_required(VERSION 3.16)
project(data_tamer_tools)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 20)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic -Werror)
  # Disable specific warnings for dependencies
  add_compile_options(-Wno-deprecated-declarations)
endif()

find_package(ament_cmake QUIET)
find_package(rclcpp REQUIRED)
find_package(rclcpp_components REQUIRED)
find_package(data_tamer_cpp REQUIRED)
find_package(data_tamer_msgs REQUIRED)
find_package(rcl_interfaces REQUIRED)
find_package(mcap_vendor REQUIRED)
set(mcap_LIBRARIES mcap_vendor::mcap)
set(mcap_INCLUDE_DIRS ${mcap_vendor_INCLUDE_DIRS})
find_package(Protobuf REQUIRED)
find_library(PROTOBUF_SHARED_LIB NAMES protobuf PATHS /usr/lib/x86_64-linux-gnu NO_DEFAULT_PATH)

find_package(nlohmann_json REQUIRED)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
include(FetchContent)
FetchContent_Declare(
  foxglove
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  URL https://github.com/foxglove/foxglove-sdk/releases/download/sdk%2Fv0.14.2/foxglove-v0.14.2-cpp-x86_64-unknown-linux-gnu.zip
  URL_HASH SHA256=574c3ce006d6131d6519b416f25ef37c5b2a0721ef49c894286c79a5a5e0b4fc
)
FetchContent_MakeAvailable(foxglove)
file(GLOB FOXGLOVE_SOURCES CONFIGURE_DEPENDS
  "${foxglove_SOURCE_DIR}/src/*.cpp"
  "${foxglove_SOURCE_DIR}/src/server/*.cpp"
)

add_library(${PROJECT_NAME}_tools SHARED src/helpers.cpp)
target_link_libraries(${PROJECT_NAME}_tools
  ${data_tamer_cpp_TARGETS}
  ${nlohmann_json_LIBRARIES}
  ${PROTOBUF_SHARED_LIB}
  ${mcap_LIBRARIES})
target_include_directories(${PROJECT_NAME}_tools
 PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
  ${Protobuf_INCLUDE_DIRS}
  ${nlohmann_json_INCLUDE_DIRS}
  ${mcap_INCLUDE_DIRS}
)

add_library(mcap_sink SHARED
    include/${PROJECT_NAME}/sinks/mcap_sink.hpp
    src/mcap_sink.cpp)
target_compile_definitions(mcap_sink PUBLIC USING_ROS2=1 )
target_include_directories(mcap_sink
 PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
  ${nlohmann_json_INCLUDE_DIRS}
  ${Protobuf_INCLUDE_DIRS}
)
target_link_libraries(mcap_sink
  ${data_tamer_cpp_TARGETS}
  ${nlohmann_json_LIBRARIES}
  ${mcap_LIBRARIES}
  ${PROJECT_NAME}_tools
  ${PROTOBUF_SHARED_LIB})

add_library(foxglove_relay SHARED src/foxglove_relay.cpp)
rclcpp_components_register_node(foxglove_relay PLUGIN "data_tamer_tools::DtRos2ToFoxgloveBridge" EXECUTABLE foxglove_relay_node)
target_sources(foxglove_relay PRIVATE ${FOXGLOVE_SOURCES})
target_link_libraries(foxglove_relay
  ${foxglove_SOURCE_DIR}/lib/libfoxglove.a
  ${data_tamer_cpp_TARGETS}
  ${data_tamer_msgs_TARGETS}
  ${rclcpp_components_TARGETS}
  ${nlohmann_json_LIBRARIES}
  ${rcl_interfaces_TARGETS}
  ${PROJECT_NAME}_tools
  ${PROTOBUF_SHARED_LIB})
target_include_directories(foxglove_relay
 PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<BUILD_INTERFACE:${nlohmann_json_INCLUDE_DIRS}>
  $<BUILD_INTERFACE:${foxglove_SOURCE_DIR}/include>
  $<BUILD_INTERFACE:${foxglove_SOURCE_DIR}/include/foxglove>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
  $<INSTALL_INTERFACE:include>
  ${Protobuf_INCLUDE_DIRS}
)

add_executable(snapshot_recorder src/snapshot_recorder.cpp)
target_link_libraries(snapshot_recorder
  ${rclcpp_TARGETS}
  ${data_tamer_msgs_TARGETS})
target_include_directories(snapshot_recorder
 PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

add_executable(mcap_converter src/mcap_converter.cpp)
target_link_libraries(mcap_converter
  ${PROJECT_NAME}_tools
  ${data_tamer_cpp_TARGETS}
  ${mcap_LIBRARIES}
  ${PROTOBUF_SHARED_LIB})
target_include_directories(mcap_converter
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
    ${mcap_INCLUDE_DIRS}
    ${nlohmann_json_INCLUDE_DIRS}
    ${Protobuf_INCLUDE_DIRS})

install(
  DIRECTORY include/
  DESTINATION include
)

install(
  TARGETS ${PROJECT_NAME}_tools mcap_sink foxglove_relay
  EXPORT ${PROJECT_NAME}Targets
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
  RUNTIME DESTINATION bin
  INCLUDES DESTINATION include
)

install(
  TARGETS mcap_converter
  DESTINATION lib/${PROJECT_NAME}
)

ament_export_targets(${PROJECT_NAME}Targets)
ament_export_dependencies(data_tamer_cpp rclcpp rclcpp_components nlohmann_json)
ament_export_libraries(mcap_sink foxglove_relay)
ament_package()

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    find_package(ament_index_cpp REQUIRED)
    include(GoogleTest)

    add_executable(helpers_test
        test/helpers.cpp)
    gtest_discover_tests(helpers_test DISCOVERY_MODE PRE_TEST)

    target_include_directories(helpers_test
    PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    ${nlohmann_json_INCLUDE_DIRS})

    target_link_libraries(helpers_test
                            ${PROJECT_NAME}_tools
                            ${data_tamer_cpp_TARGETS}
                            ${nlohmann_json_LIBRARIES}
                            ${ament_index_cpp_TARGETS}
                            GTest::gtest_main)

    add_executable(mcap_sink_test
        test/mcap_sink.cpp)
    gtest_discover_tests(mcap_sink_test DISCOVERY_MODE PRE_TEST)
    target_include_directories(mcap_sink_test
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    ${nlohmann_json_INCLUDE_DIRS}
    ${mcap_INCLUDE_DIRS})

    target_link_libraries(mcap_sink_test
        mcap_sink
        ${data_tamer_cpp_TARGETS}
        ${nlohmann_json_LIBRARIES}
        ${mcap_LIBRARIES}
        GTest::gtest_main)

    add_test(NAME helpers_test COMMAND $<TARGET_FILE:helpers_test>)
    add_test(NAME mcap_sink_test COMMAND $<TARGET_FILE:mcap_sink_test>)

    install(
      DIRECTORY test/data
      DESTINATION share/${PROJECT_NAME}/test
)
endif()

if(BUILD_EXAMPLES)
    add_executable(sinks_example examples/json_mcap_sink.cpp)
    target_include_directories(sinks_example
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
               $<INSTALL_INTERFACE:include>)
    target_link_libraries(sinks_example PUBLIC ${PROJECT_NAME}_tools mcap_sink ${data_tamer_cpp_TARGETS})

    add_executable(relay_sources_example examples/relay_sources_example.cpp)
    target_include_directories(relay_sources_example
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
               $<INSTALL_INTERFACE:include>)
    target_link_libraries(relay_sources_example PUBLIC mcap_sink ${data_tamer_cpp_TARGETS})

    install(
        TARGETS sinks_example relay_sources_example
        DESTINATION lib/${PROJECT_NAME}
        )
endif()

if(BUILD_BENCHMARKS)
  find_package(rosbag2_cpp REQUIRED)
  find_package(rosbag2_storage REQUIRED)
  add_executable(benchmark_encode_snapshot
  benchmark/encode_snapshot.cpp)
  target_link_libraries(benchmark_encode_snapshot
    ${rosbag2_cpp_TARGETS}
    ${rosbag2_storage_TARGETS}
    ${rclcpp_TARGETS}
    ${data_tamer_msgs_TARGETS}
    ${PROJECT_NAME}_tools
  )
  install(TARGETS benchmark_encode_snapshot DESTINATION lib/${PROJECT_NAME})
endif()
