#include "OSSManager.h"

#include <fstream>
#include <iostream>
#include <map>

using namespace std;

// 初始化静态成员变量
OSSManager* OSSManager::instance = nullptr;
mutex OSSManager::m_mutex;

// 去掉字符串两端的空白
static string trim(const string& s)
{
    size_t b = s.find_first_not_of(" \t\r\n");
    if (b == string::npos) return "";
    size_t e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

// 解析 OSS.env: 每行形如  key = "value";
static map<string, string> load_env_config(const string& path)
{
    map<string, string> cfg;
    ifstream in(path);
    if (!in.is_open()) {
        cerr << "[OSS] 无法打开配置文件: " << path << endl;
        return cfg;
    }
    string line;
    while (getline(in, line)) {
        size_t eq = line.find('=');
        if (eq == string::npos) continue;
        string key = trim(line.substr(0, eq));
        string val = line.substr(eq + 1);
        val = trim(val);
        while (!val.empty() && (val.back() == ';' || val.back() == '"')) val.pop_back();  // 去结尾 ; 和 "
        if (!val.empty() && val.front() == '"') val.erase(val.begin());                    // 去开头 "
        if (!key.empty()) cfg[key] = val;
    }
    return cfg;
}

 OSSManager* OSSManager::get_instance(){
    if(instance == nullptr){
        lock_guard<mutex> lock(m_mutex);
        if(instance == nullptr){
            instance = new OSSManager();
        }
    }
    return instance;

}

void OSSManager::destory_instance(){
    lock_guard<mutex> lock(m_mutex);
    if(instance != nullptr){
        delete instance;
        instance = nullptr;
        AlibabaCloud::OSS::ShutdownSdk();
    }

}

OSSManager::OSSManager(){
    AlibabaCloud::OSS::InitializeSdk();

    // 固定读取当前目录下的 OSS.env
    auto cfg = load_env_config("OSS.env");
    string endpoint        = cfg["endpoint"];
    string region          = cfg["region"];
    string accessKeyId     = cfg["accessKeyId"];
    string accessKeySecret = cfg["accessKeySecret"];

    // 空值检查: 任一必要项缺失则记录错误并保持 m_client 为空
    if (endpoint.empty() || region.empty() || accessKeyId.empty() || accessKeySecret.empty()) {
        cerr << "[OSS] OSS.env 配置不完整 (endpoint/region/accessKeyId/accessKeySecret)" << endl;
        return;
    }

    AlibabaCloud::OSS::ClientConfiguration conf;
    m_client = std::make_unique<AlibabaCloud::OSS::OssClient>(endpoint, accessKeyId, accessKeySecret, conf);
    m_client->SetRegion(region);
}

OSSManager::~OSSManager() = default;

bool OSSManager::upload_file(const string& bucket, const string& object, const string& file) // 上传文件
{
    if (m_client == nullptr) {
        cerr << "[OSS] 客户端未初始化 (OSS.env 配置不完整?)" << endl;
        return false;
    }
    auto outcome = m_client->PutObject(bucket, object, file);
    if (!outcome.isSuccess()) {
        // 打印具体原因。只返回 bool 的话, 调用方只能报 "upload failed",
        // 分不清是本地文件不存在、桶名写错、还是 AK/SK 失效。
        cerr << "[OSS] PutObject 失败: code=" << outcome.error().Code()
             << ", msg=" << outcome.error().Message()
             << ", bucket=" << bucket << ", object=" << object << endl;
        return false;
    }
    return true;
}

bool OSSManager::upload_file(const string& bucket, const string& object, std::shared_ptr<std::iostream> content)
{
    if (m_client == nullptr) return false;   // 配置不完整时构造失败
    auto outcome = m_client->PutObject(bucket, object, content);
    return outcome.isSuccess();
}