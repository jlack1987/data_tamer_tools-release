// file: dt_two_ros2_sources.cpp
#include <atomic>
#include <csignal>
#include <thread>
#include <array>
#include <vector>
#include <iostream>

#include <rclcpp/rclcpp.hpp>

#include <data_tamer/data_tamer.hpp>
#include <data_tamer/sinks/ros2_publisher_sink.hpp>

// Small helper to register scalar/array/vector values and tick them
struct SourceA
{
    // Scalars
    double temperature = 22.5;
    int humidity = 60;

    // Fixed array (length 3)
    std::array<float, 3> accel{ 0.f, 0.f, 0.f };

    // Dynamic vector (changes length)
    std::vector<uint32_t> ids{};

    // RAII handles into the channel
    DataTamer::RegistrationID temp_id{};
    DataTamer::RegistrationID hum_id{};
    std::shared_ptr<DataTamer::LoggedValue<std::array<float, 3>>> accel_val;
    std::shared_ptr<DataTamer::LoggedValue<std::vector<uint32_t>>> ids_val;

    void setup(std::shared_ptr<DataTamer::LogChannel> ch)
    {
        temp_id = ch->registerValue("temperature", &temperature);
        hum_id = ch->registerValue("humidity", &humidity);
        accel_val = ch->createLoggedValue<std::array<float, 3>>("accel");
        accel_val->set(accel);
        ids_val = ch->createLoggedValue<std::vector<uint32_t>>("ids");
        ids_val->set(ids);
    }

    void tick(std::shared_ptr<DataTamer::LogChannel> ch, size_t t)
    {
        temperature += 0.25;
        humidity += 1;

        accel = { 1.0f + 0.01f * float(t), 2.0f + 0.01f * float(t), 3.0f + 0.01f * float(t) };
        accel_val->set(accel);

        switch (t % 6)
        {
            case 0:
                ids = {};
                break;
            case 1:
                ids = { 10 };
                break;
            case 2:
                ids = { 10, 20, 30 };
                break;
            case 3:
                ids = { 10, 20, 30, 40, 50 };
                break;
            case 4:
                ids = { 1, 2 };
                break;
            case 5:
                ids = { 99, 98, 97, 96 };
                break;
        }
        ids_val->set(ids);

        ch->takeSnapshot();  // sends to ROS2 publisher sink
    }
};

struct SourceB
{
    uint32_t rpm = 1000;
    std::array<double, 4> voltages{ 12.0, 12.1, 12.2, 12.3 };
    std::vector<uint8_t> statuses{ 1, 0, 1 };

    DataTamer::RegistrationID rpm_id{};
    std::shared_ptr<DataTamer::LoggedValue<std::array<double, 4>>> volts_val;
    std::shared_ptr<DataTamer::LoggedValue<std::vector<uint8_t>>> statuses_val;

    void setup(std::shared_ptr<DataTamer::LogChannel> ch)
    {
        rpm_id = ch->registerValue("rpm", &rpm);
        volts_val = ch->createLoggedValue<std::array<double, 4>>("voltages");
        volts_val->set(voltages);
        statuses_val = ch->createLoggedValue<std::vector<uint8_t>>("statuses");
        statuses_val->set(statuses);
    }

    void tick(std::shared_ptr<DataTamer::LogChannel> ch, size_t t)
    {
        rpm += 25;
        for (size_t i = 0; i < voltages.size(); ++i)
            voltages[i] += 0.01;
        volts_val->set(voltages);

        // flip statuses size/pattern
        if (t % 5 == 0)
            statuses = { 1 };
        else if (t % 5 == 1)
            statuses = { 1, 1 };
        else if (t % 5 == 2)
            statuses = { 0, 1, 0, 1 };
        else if (t % 5 == 3)
            statuses = { 2, 2, 2, 2, 2 };
        else
            statuses = { 1, 0, 1 };

        statuses_val->set(statuses);
        ch->takeSnapshot();
    }
};

static std::atomic<bool> running{ true };

int main(int argc, char** argv)
{
    rclcpp::init(argc, argv);

    auto node = std::make_shared<rclcpp::Node>("dt_two_ros2_sources");
    auto exec = std::make_shared<rclcpp::executors::MultiThreadedExecutor>();
    exec->add_node(node);

    // --- Two ROS2 publisher sinks (separate namespaces so topics don't collide)
    // If your sink ctor signature differs, adjust here.
    auto sinkA = std::make_shared<DataTamer::ROS2PublisherSink>(node, "/dt/source_a");
    auto sinkB = std::make_shared<DataTamer::ROS2PublisherSink>(node, "/dt/source_b");

    // --- Two DataTamer channels
    auto chA = DataTamer::ChannelsRegistry::Global().getChannel("source_a");
    auto chB = DataTamer::ChannelsRegistry::Global().getChannel("source_b");

    chA->addDataSink(sinkA);
    chB->addDataSink(sinkB);

    // Register fields & set initial values
    SourceA a;
    SourceB b;
    a.setup(chA);
    b.setup(chB);

    // Handle Ctrl-C
    std::signal(SIGINT, [](int) { running = false; });

    // Spin executor in background (for ROS pub/sub & discovery)
    std::thread spin_thr(
        [&]
        {
            while (running)
                exec->spin_some();
        });

    // Publish at ~10 Hz
    size_t tick = 0;
    while (running)
    {
        a.tick(chA, tick);
        b.tick(chB, tick);

        // Console trace to correlate with Foxglove Raw Messages
        std::cout << "[tick " << tick << "] " << "A.ids.size=" << a.ids.size() << " A.accel=[" << a.accel[0] << "," << a.accel[1] << "," << a.accel[2] << "]   "
                  << "B.voltages=[" << b.voltages[0] << "," << b.voltages[1] << ",…]" << " B.statuses.size=" << b.statuses.size() << std::endl;

        ++tick;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    exec->cancel();
    spin_thr.join();
    rclcpp::shutdown();
    return 0;
}
