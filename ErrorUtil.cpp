
#include "ErrorUtil.h"

#include <nlohmann/json.hpp>
#include <ctime>
#include <iostream>

// 线程安全的时间串: handler 跑在 workflow 的多个线程上，不能用 std::localtime
static std::string now_string()
{
    char buf[32];
    std::time_t t = std::time(nullptr);
    std::tm tm_buf{};
    localtime_r(&t, &tm_buf);
    std::strftime(buf, sizeof buf, "%Y-%m-%d %H:%M:%S", &tm_buf);
    return buf;
}

std::string ErrorUtil::default_message(int status)
{
    switch (status) {
        case 400: return "请求参数错误";
        case 401: return "未登录或登录已过期";
        case 404: return "资源不存在";
        default:  return "服务器内部错误";
    }
}

void ErrorUtil::send_error(wfrest::HttpResp* resp, int status)
{
    send_error(resp, status, default_message(status));
}

void ErrorUtil::send_error(wfrest::HttpResp* resp, int status, const std::string& message)
{
    std::string msg = message.empty() ? default_message(status) : message;

    // 全局错误日志: 所有接口的错误响应都经过这里
    // 4xx = 客户端问题(参数错/未登录) -> WARN; 5xx = 服务端故障 -> ERROR
    std::cout << (status >= 500 ? "[ERROR] " : "[WARN] ")
              << now_string()
              << " | " << status
              << " | " << msg << std::endl;

    nlohmann::json json;
    json["code"] = status;
    json["msg"] = msg;
    resp->set_status(status);
    resp->String(json.dump());
}
