#include <memory>
#include <string>
#include <filesystem>

#include <rclcpp/rclcpp.hpp>
#include <rclcpp/qos.hpp>

#include <data_tamer_tools/msg/log_dir.hpp>
#include <data_tamer_tools/srv/rotate.hpp>

namespace data_tamer_tools
{

class LogRotationCoordinator : public rclcpp::Node
{
  public:
    LogRotationCoordinator(const rclcpp::NodeOptions& opts = rclcpp::NodeOptions()) : rclcpp::Node("rotate_coordinator", opts)
    {
        // Parameters (override if you want different names)
        rotate_topic_ = this->declare_parameter<std::string>("rotate_topic", "/data_tamer/rotate_dir");
        service_name_ = this->declare_parameter<std::string>("service_name", "/data_tamer/loggers/rotate");

        // Publisher: latched (transient local) + reliable so late joiners get the last value.
        rclcpp::QoS qos(1);
        qos.transient_local().reliable();
        pub_ = this->create_publisher<data_tamer_tools::msg::LogDir>(rotate_topic_, qos);

        // Service
        srv_ = this->create_service<data_tamer_tools::srv::Rotate>(service_name_,
                                                                   std::bind(&LogRotationCoordinator::handle_rotate, this, std::placeholders::_1, std::placeholders::_2));

        RCLCPP_INFO(get_logger(), "LogRotationCoordinator up. Service: '%s'  -> publishes LogDir to '%s'", service_name_.c_str(), rotate_topic_.c_str());
    }

  private:
    void handle_rotate(const std::shared_ptr<data_tamer_tools::srv::Rotate::Request> req, std::shared_ptr<data_tamer_tools::srv::Rotate::Response> /*resp*/)
    {
        if (!req || req->directory.empty())
        {
            RCLCPP_WARN(get_logger(), "Rotate request has empty 'directory'; ignoring.");
            return;
        }

        // Create the directory if it doesn't exist
        std::error_code ec;
        std::filesystem::create_directories(req->directory, ec);
        if (ec)
        {
            RCLCPP_ERROR(get_logger(), "Failed to create directory '%s': %s", req->directory.c_str(), ec.message().c_str());
            return;
        }

        data_tamer_tools::msg::LogDir msg;
        msg.directory = req->directory;
        pub_->publish(msg);

        RCLCPP_INFO(get_logger(), "Published rotation request -> directory='%s' on '%s'", msg.directory.c_str(), rotate_topic_.c_str());
        // If your Rotate.srv has response fields (e.g., bool ok/string msg), you could set them here.
        // We intentionally don't touch the response to stay compatible with minimal definitions.
    }

    std::string rotate_topic_;
    std::string service_name_;
    rclcpp::Publisher<data_tamer_tools::msg::LogDir>::SharedPtr pub_;
    rclcpp::Service<data_tamer_tools::srv::Rotate>::SharedPtr srv_;
};
}  // namespace data_tamer_tools

#include <rclcpp_components/register_node_macro.hpp>
RCLCPP_COMPONENTS_REGISTER_NODE(data_tamer_tools::LogRotationCoordinator)