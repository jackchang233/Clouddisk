#pragma once

#include <wfrest/HttpServer.h>
#include <workflow/WFFacilities.h>


class CloudiskServer
{
public:
    CloudiskServer() {}

    void register_modules();

    // 启动回收站定时清理 (软删超期彻底删除)
    void start_recycle_sweep();

    int start(unsigned short port) { return m_server.start(port); }

    void stop() { m_server.stop(); }

    void list_routes() { m_server.list_routes(); }

    CloudiskServer& track()
    {
        m_server.track();
        return *this;
    }
private:
    void register_static_resources_module();
    void register_signup_module();
    void register_signin_module();
    void register_userinfo_module();
    void register_fileupload_module();
    void register_filelist_module();
    void register_filedownload_module();
    void register_filedelete_module();
    void register_recycle_module();
private:

    wfrest::HttpServer m_server {};    
};


// 转义并包上单引号，得到安全的 SQL 字符串字面量 (防注入)
// 注意用 escape_string (反斜杠转义) 而非 escape_string_quote (引号翻倍, 仅 NO_BACKSLASH_ESCAPES 模式适用)

static string sql_quote(const string& s){
    return "'" + MySQLUtil::escape_string(s) + "'";
}
