#pragma once

#include <string>
#include <wfrest/HttpMsg.h>

// 统一的错误响应模块
// 响应体统一为 JSON: {"code":<状态码>,"msg":"<文案>"}
// 所有接口的错误返回都从这里发出，好处:
//   1. 以后改错误格式只动这一个文件
//   2. 可以在这里统一加日志、监控埋点，不用改业务代码
class ErrorUtil
{
public:
    // 按状态码取默认文案 (400/401/404/500...)
    static void send_error(wfrest::HttpResp* resp, int status);

    // 自定义文案 (如 "用户名或密码错误")
    static void send_error(wfrest::HttpResp* resp, int status, const std::string& message);

private:
    static std::string default_message(int status);
};
