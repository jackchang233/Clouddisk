#include <iostream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <nlohmann/json.hpp>
#include <SimpleAmqpClient/SimpleAmqpClient.h>

#include "OSSManager.h"

using namespace std;
using namespace AmqpClient;

// 处理单条消息。返回是否成功, 失败只记录不影响后续消息。
static bool handle_message(OSSManager* oss, const string& body)
{
    string bucket, object, file;
    try {
        // 必须包 try: 异常穿过这里会 terminate 整个 worker 进程。
        // (项目中已因未捕获异常崩过两次: Consul 查询和表单解析)
        nlohmann::json message = nlohmann::json::parse(body);
        // 用 value() 而非 operator[]: 缺字段时返回默认值而不是未定义行为
        bucket = message.value("bucket", "");
        object = message.value("object", "");
        file   = message.value("file", "");
    } catch (const std::exception& e) {
        cerr << "[WARN] 消息解析失败, 已丢弃: " << e.what()
             << " | 原文前 80 字节: [" << body.substr(0, 80) << "]" << endl;
        return false;
    }

    if (bucket.empty() || object.empty() || file.empty()) {
        cerr << "[WARN] 消息字段缺失, 已丢弃: " << body.substr(0, 80) << endl;
        return false;
    }

    // 本地文件可能已被删除 —— 典型场景: 投递后用户删除了文件,
    // 或上传会话被清理。先判断, 否则 OSS SDK 只会给出含糊的失败。
    if (access(file.c_str(), F_OK) != 0) {
        cerr << "[WARN] 本地文件已不存在, 跳过备份 (可能文件已被删除): "
             << file << endl;
        return false;
    }

    // 失败原因由 OSSManager 内部打印 (HTTP code + message)
    if (!oss->upload_file(bucket, object, file)) {
        cerr << "[WARN] OSS 备份失败: " << file << " -> " << object << endl;
        return false;
    }
    return true;
}

int main()
{
    string uri = "amqp://guest:guest@localhost:5672/%2f";
    const string& q = "taskqueue";
    Channel::ptr_t channel = Channel::CreateFromUri(uri);
    channel->BasicConsume(q);
    OSSManager* oss = OSSManager::get_instance();

    for (;;) {
        // 获取一封消息; 队列为空时一直等待
        Envelope::ptr_t envelope = channel->BasicConsumeMessage();
        if (envelope && envelope->Message())
            handle_message(oss, envelope->Message()->Body());
    }

    OSSManager::destory_instance();
}
