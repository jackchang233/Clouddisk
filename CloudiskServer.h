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

    // 启动服务发现后台刷新 (定时从 Consul 拉 UserService 实例表)
    void start_service_discovery();

    // 启动分片暂存区清理 (回收超期未完成的上传会话)
    void start_chunk_sweep();

    // 启动内容池孤儿文件回收 (GC 兜底)
    void start_blob_gc();

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
    void register_chunkupload_module();
    void register_dir_module();
private:

    wfrest::HttpServer m_server {};
};
