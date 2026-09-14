#pragma once

#include <string>
#include <workflow/MySQLUtil.h>

// 转义并包上单引号，得到安全的 SQL 字符串字面量 (防注入)
// 注意用 escape_string (反斜杠转义) 而非 escape_string_quote (引号翻倍, 仅 NO_BACKSLASH_ESCAPES 模式适用)
//
// 放在独立头文件里，而不是 CloudiskServer.h：
// wfrest 的 HTTP 服务和 srpc 的用户服务都需要它，不应让 RPC 服务依赖 HTTP 服务的头。
inline std::string sql_quote(const std::string& s)
{
    return "'" + protocol::MySQLUtil::escape_string(s) + "'";
}
