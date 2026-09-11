#include <iostream>
#include <string>
#include <nlohmann/json.hpp>
#include <SimpleAmqpClient/SimpleAmqpClient.h>

#include "OSSManager.h"

using namespace std;
using namespace AmqpClient;

int main(){
    string uri = "amqp://guest:guest@localhost:5672/%2f";
    const string& q = "taskqueue";
    Channel::ptr_t channel = Channel::CreateFromUri(uri);
    channel->BasicConsume(q);
    OSSManager* oss = OSSManager::get_instance();
    for(;;) {
    // 获取一封消息
    // 如果队列中没有消息，则一直等待
    Envelope::ptr_t envelope = channel->BasicConsumeMessage();

    if (envelope && envelope->Message()) {
        nlohmann::json message = nlohmann::json::parse(envelope->Message()->Body());
        string bucket = message["bucket"];   // 目标桶
        string object = message["object"];   // OSS 对象键
        string file   = message["file"];     // 本地文件路径
#ifdef DEBUG
        cout << "bucket: " << bucket << ", object: " << object
            << ", file: " << file << endl;
#endif
        if (!oss->upload_file(bucket, object, file)) {
            cerr << "Error: upload failed! bucket: " << bucket
                << ", object: " << object << ", file: " << file << endl;
        }
    }
    }

    OSSManager::destory_instance();
}