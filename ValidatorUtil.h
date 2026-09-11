#pragma once

#include <string>

// 用户名/密码合法性校验模块
// 校验规则只定义在这里，注册接口强制执行;
// 登录接口刻意不做格式校验 —— 策略可能随时间变化，
// 老用户的旧密码必须始终能参与哈希比对，否则会被新策略锁在门外
class ValidatorUtil
{
public:
    // 返回空串表示合法，否则返回面向用户的错误提示
    static std::string validate_username(const std::string& username);

    static std::string validate_password(const std::string& password);
};
