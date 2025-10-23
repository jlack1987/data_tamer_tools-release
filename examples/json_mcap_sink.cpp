#include <atomic>
#include <csignal>
#include <thread>
#include <array>
#include <vector>
#include <iostream>

#include <data_tamer/data_tamer.hpp>
#include <data_tamer/sinks/mcap_sink.hpp>
#include <data_tamer_tools/sinks/mcap_sink.hpp>

std::atomic<bool> running(true);

int main()
{
    // Sinks
    // auto mcap_sink = std::make_shared<data_tamer_tools::McapSink>("/tmp/example.mcap", data_tamer_tools::McapSink::Compression::None, 1024);

    auto mcap_sink = std::make_shared<DataTamer::MCAPSink>("/tmp/example.mcap");

    // Channel
    auto channel = DataTamer::ChannelsRegistry::Global().getChannel("sensor_data");
    channel->addDataSink(mcap_sink);

    // Scalar fields
    double temperature = 22.5;
    int humidity = 60;
    (void)channel->registerValue("temperature", &temperature);
    (void)channel->registerValue("humidity", &humidity);

    // RAII scalar
    auto logged_pressure = channel->createLoggedValue<float>("pressure");
    logged_pressure->set(101.3f);

    // NEW: fixed-length array (3)
    auto accel = channel->createLoggedValue<std::array<float, 3>>("accel");
    accel->set(std::array<float, 3>{ 0.f, 0.f, 0.f });

    // NEW: variable-length vector
    auto ids = channel->createLoggedValue<std::vector<uint32_t>>("ids");
    ids->set({});  // start empty

    // ctrl-c
    std::signal(SIGINT,
                [](int sig)
                {
                    std::cerr << "received signal " << sig << ", shutting down\n";
                    running = false;
                });

    size_t tick = 0;
    while (running)
    {
        // update scalars
        temperature += 0.5;
        humidity += 1;
        logged_pressure->set(logged_pressure->get() + 0.1f);

        // update fixed array (stays length 3)
        std::array<float, 3> a = {
            1.0f + 0.01f * float(tick),
            2.0f + 0.01f * float(tick),
            3.0f + 0.01f * float(tick),
        };
        accel->set(a);

        // update vector: cycle through sizes
        std::vector<uint32_t> v;
        switch (tick % 6)
        {
            case 0:
                v = {};
                break;  // size 0
            case 1:
                v = { 10 };
                break;  // size 1
            case 2:
                v = { 10, 20, 30 };
                break;  // size 3
            case 3:
                v = { 10, 20, 30, 40, 50 };
                break;  // size 5
            case 4:
                v = { 1, 2 };
                break;  // size 2
            case 5:
                v = { 99, 98, 97, 96 };
                break;  // size 4
        }
        ids->set(v);

        // (optional) occasionally do NOT update ids to see if the field becomes inactive that tick
        // if ((tick % 20) == 0) { /* skip ids->set(v); */ }

        // snapshot
        channel->takeSnapshot();

        // console trace so you can correlate with Foxglove Raw Messages
        std::cout << "[tick " << tick << "] ids.size=" << v.size() << " accel=[" << a[0] << "," << a[1] << "," << a[2] << "]\n";

        ++tick;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    return 0;
}
