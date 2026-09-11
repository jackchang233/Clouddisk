#pragma once
#include <alibabacloud/oss/OssClient.h>
#include <mutex>
#include <memory>
#include <fcntl.h>
class OSSManager
{
public:
    static OSSManager* get_instance();
    static void destory_instance();

    //从磁盘上传
    bool upload_file(const std::string& bucket,const std::string& object, const std::string& file);
    bool upload_file(const std::string& bucket,const std::string& object, std::shared_ptr<std::iostream> content);
    
    ~OSSManager();

private:
    OSSManager();
    OSSManager(const OSSManager&) = delete;
    OSSManager& operator=(OSSManager &) = delete;

private:
    static OSSManager* instance;
    static std::mutex m_mutex;

    std::unique_ptr<AlibabaCloud::OSS::OssClient> m_client;
};