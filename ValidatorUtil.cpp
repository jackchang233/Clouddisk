#include "ValidatorUtil.h"

#include <cctype>

using std::string;

// 用户名: 3-32 位, 仅字母/数字/下划线
string ValidatorUtil::validate_username(const string& username)
{
    if (username.size() < 3 || username.size() > 32)
        return "用户名长度须为 3-32 个字符";
    for (unsigned char c : username) {
        // (unsigned char) 保证多字节字符不会以负数传给 isalnum (未定义行为)
        if (!std::isalnum(c) && c != '_')
            return "用户名只能包含字母、数字和下划线";
    }
    return "";
}

// 密码: 6-64 位, 须同时包含字母和数字
string ValidatorUtil::validate_password(const string& password)
{
    if (password.size() < 6)
        return "密码长度至少 6 位";
    if (password.size() > 64)
        return "密码长度至多 64 位";

    bool has_letter = false, has_digit = false;
    for (unsigned char c : password) {
        if (std::isalpha(c)) has_letter = true;
        if (std::isdigit(c)) has_digit = true;
    }
    if (!has_letter || !has_digit)
        return "密码须同时包含字母和数字";
    return "";
}
