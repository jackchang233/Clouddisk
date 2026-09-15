#include <workflow/MySQLResult.h>
#include <workflow/WFTaskFactory.h>
#include <workflow/MySQLUtil.h>
#include <workflow/RedisMessage.h>
#include <wfrest/PathUtil.h>
#include <wfrest/CodeUtil.h>
#include <vector>
#include <string>
#include <functional>
#include <stdexcept>
#include <fstream>
#include <cerrno>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include <nlohmann/json.hpp>


#include "CloudiskServer.h"
#include "CryptoUtil.h"
#include "ErrorUtil.h"
#include "ValidatorUtil.h"
#include "OSSManager.h"
#include "ServiceRegistry.h"
#include "SqlUtil.h"
#include "UserService.srpc.h"
#include "UserService.pb.h"

using namespace std;
using namespace wfrest;
using namespace protocol;
using namespace std::placeholders;
using namespace AmqpClient;


static const string MYSQL_URL = "mysql://root:123456@localhost:3306/test";
static const string RABBITMQ_URL = "amqp://guest:guest@localhost:5672/%2f";
static const string OSS_BUCKET = "netdisk74147";
static const int RETRY_MAX = 3;

// 回收站清理策略: 软删超过 7 天彻底删除, 每小时扫一次
static const int RECYCLE_EXPIRE_DAYS = 7;
static const time_t RECYCLE_SWEEP_INTERVAL = 3600;

// 分片上传
static const int CHUNK_MAX_COUNT      = 10000;   // 分片数上限, 防恶意构造
static const int CHUNK_SESSION_TTL    = 86400;   // Redis 会话存活 24h
static const time_t CHUNK_SWEEP_INTERVAL = 3600; // 磁盘残留扫描周期

// RFC 5987: HTTP 头部值只允许可见 ASCII，中文文件名必须百分号编码
// 否则裸 UTF-8 字节进响应头，curl 能容忍但 Chrome 会判响应非法 (ERR_INVALID_RESPONSE)
static string percent_encode(const string& s){
    static const char* HEX = "0123456789ABCDEF";
    string out;
    for (unsigned char c : s) {
        if (c > 0x20 && c < 0x7F && c != '"' && c != '%') {
            out += c;
        } else {
            out += '%';
            out += HEX[c >> 4];
            out += HEX[c & 0xF];
        }
    }
    return out;
}

// RPC 调用失败时取出可读的错误文案:
//   - 传输层失败 (ctx->success() == false) => 服务不可达，给通用文案
//   - 业务失败 => 用服务端 err_msg 给的具体原因，没填则回落 fallback
static string rpc_err_msg(const srpc::RPCContext* ctx, const UserResponse& response,
                          const string& fallback)
{
    if (ctx == nullptr || !ctx->success()) return "用户服务暂不可用，请稍后重试";
    string msg = response.err_msg();
    return msg.empty() ? fallback : msg;
}

// 读取表单值并解码。
//
// wfrest 的 form_kv() 返回**原始未解码**的值 (与 req->query() 行为一致),
// 必须自行调用 url_decode。不解码的后果: 密码含 + % 空格 等字符时,
// 存进数据库的是编码后的值, 而不是用户真正输入的内容。
//
// ⚠️ 顺序要求: 先解码, 再交给 sql_quote 转义。
//    反序会重新引入 SQL 注入 —— 解码会把 %27 还原成引号,
//    若在转义之后才解码, 这个引号就完全绕过了转义。
static string form_get(const map<string, string>& kv, const string& key)
{
    auto it = kv.find(key);
    return it == kv.end() ? string() : CodeUtil::url_decode(it->second);
}

/*********************************************************************************
 *                     服务发现: 后台刷新 UserService 实例表                      *
 *********************************************************************************/
static const char* CONSUL_USER_SERVICE_URL =
    "http://127.0.0.1:8500/v1/health/service/UserService?passing=true";
static const time_t SERVICE_REFRESH_INTERVAL = 30;

// 从 Consul 拉一次实例表写进进程内缓存。
//
// 失败时只打日志、保留旧缓存 (stale-while-revalidate) —— 在线请求读的是缓存,
// 所以 Consul 抖动不会影响登录/注册。
static void refresh_user_service()
{
    WFHttpTask* task = WFTaskFactory::create_http_task(CONSUL_USER_SERVICE_URL, 3, 3,
        [](WFHttpTask* task)
        {
            ServiceInstance inst;
            string body;   // 放到 try 外, 便于失败时打印原始响应辅助定位
            try {
                if (task->get_state() != WFT_STATE_SUCCESS)
                    throw std::runtime_error("Consul 请求失败");

                body = HttpUtil::decode_chunked_body(task->get_resp());
                if (body.empty())
                    throw std::runtime_error("Consul 返回空响应");

                // 必须包 try: 异常穿过回调会让整个进程 terminate (曾经的实际故障)
                nlohmann::json data = nlohmann::json::parse(body);
                if (data.size() == 0)
                    throw std::runtime_error("Consul 无健康实例");

                inst.ip   = data[0]["Service"]["Address"].get<string>();
                inst.port = data[0]["Service"]["Port"].get<unsigned short>();
                if (!inst.valid())
                    throw std::runtime_error("实例地址非法");
            } catch (const std::exception& e) {
                cout << "[WARN] 刷新 UserService 实例失败: " << e.what()
                     << " | 响应前 40 字节: [" << body.substr(0, 40) << "]"
                     << " (沿用旧缓存)" << endl;
                return ;
            }
            ServiceRegistry::instance().update(inst);
            cout << "[INFO] UserService 实例: " << inst.ip << ":" << inst.port << endl;
        });
    task->start();
}

// 定时回调: 刷一次后重新调度下一次
static void service_refresh_timer_callback(WFTimerTask*)
{
    refresh_user_service();
    WFTaskFactory::create_timer_task(SERVICE_REFRESH_INTERVAL, 0,
        service_refresh_timer_callback)->start();
}

void CloudiskServer::start_service_discovery()
{
    refresh_user_service();   // 立即拉一次, 不等第一个周期
    WFTaskFactory::create_timer_task(SERVICE_REFRESH_INTERVAL, 0,
        service_refresh_timer_callback)->start();
}


void CloudiskServer::register_modules()
{
    // 设置静态资源的路由
    register_static_resources_module();
    register_signup_module();
    register_signin_module();
    register_userinfo_module();
    register_fileupload_module();
    register_filelist_module();
    register_filedownload_module();
    register_filedelete_module();
    register_recycle_module();
    register_chunkupload_module();
}

void CloudiskServer::register_static_resources_module()
{
    m_server.GET("/user/signup", [](const HttpReq *, HttpResp * resp){
        resp->Redirect("/spa/index.html", HttpStatusMovedPermanently);
    });

    // Vue SPA (Element Plus 版前端, 构建产物在 static/spa/, hash 路由)
    m_server.GET("/spa", [](const HttpReq *, HttpResp * resp){
        resp->Redirect("/spa/index.html", HttpStatusMovedPermanently);
    });
    m_server.GET("/spa/", [](const HttpReq *, HttpResp * resp){
        resp->Redirect("/spa/index.html", HttpStatusMovedPermanently);
    });
    m_server.Static("/spa", "static/spa");

    // 头像 (新 SPA 侧栏使用)
    m_server.GET("/static/img/avatar.jpeg", [](const HttpReq *, HttpResp * resp){
        resp->File("static/img/avatar.jpeg");
    });
}

/*********************************************************************************
 *                               注册                                            *
 *********************************************************************************/

void CloudiskServer::register_signup_module()
{
    m_server.POST("/user/signup", [](const HttpReq* req, HttpResp* resp, SeriesWork* series)
    {
        // TODO: 校验请求的Content-Type，不是APPLICATION_URLENCODED则返回400 Bad Request
        if(req->content_type() != APPLICATION_URLENCODED){
            ErrorUtil::send_error(resp, 400);
            return;
        }
        // 1. 解析表单数据(application/x-www-form-urlencoded)，获取用户名和密码
        //    注意: form_kv() 返回原始编码值, 必须解码后再用 (见 form_get 注释)
        map<string, string>& data = req->form_kv();
        string username = form_get(data, "username");
        string password = form_get(data, "password");
        // 2. 校验用户名和密码(用户名是否在黑名单内，密码是否符合强度要求...)
        // 这些校验可能前端也会做 (提升用户体验)
        // 但是后端永远不要相信前端传过来的数据 (因为很容易绕过前端，直接给后端发发送请求，比如用 curl)
#ifdef DEBUG
        cout << "[INFO] username: " << username << ", password: " << password << endl; /* 调试信息 */
#endif
        if (username == "" || password == "") {
            ErrorUtil::send_error(resp, 400);
            return ;
        }
        // 格式合法性校验 (仅注册强制: 理由见 ValidatorUtil.h)
        string invalid_reason = ValidatorUtil::validate_username(username);
        if (invalid_reason.empty())
            invalid_reason = ValidatorUtil::validate_password(password);
        if (!invalid_reason.empty()) {
            ErrorUtil::send_error(resp, 400, invalid_reason);
            return ;
        }
        // 3. 从进程内缓存取实例 (不查 Consul, 不阻塞线程)
        ServiceInstance inst = ServiceRegistry::instance().get();
        if (!inst.valid()) {
            ErrorUtil::send_error(resp, 503, "用户服务暂时不可用，请稍后重试");
            return ;
        }

        auto* client = new UserService::SRPCClient{ inst.ip.c_str(), inst.port };

        UserRequest request;
        request.set_username(username);
        request.set_password(password);

        // 同注册: RPC task 要挂到请求自己的 series 上, 详见 signin 处的说明
        srpc::SRPCClientTask* task = client->create_sign_up_task(
            [resp, client](UserResponse* response, srpc::RPCContext* ctx)
            {
                // 回调里绝不能抛异常出去: 穿过 workflow 回调会导致进程 terminate
                try {
                    if (ctx->success() && response->success()) {
                        resp->String("SUCCESS");
                    } else {
                        // 统一 JSON 错误体, 前端 api.js 依赖 {"code","msg"}
                        ErrorUtil::send_error(resp, 400,
                            rpc_err_msg(ctx, *response, "注册失败，请稍后重试"));
                    }
                } catch (const std::exception& e) {
                    cout << "[WARN] 注册回调异常: " << e.what() << endl;
                    ErrorUtil::send_error(resp, 500);
                }
                delete client;
            });

        task->serialize_input(&request);
        series->push_back(task);           // 入队即走, handler 不阻塞
    });
}


/*********************************************************************************
 *                               登录                                            *
 *********************************************************************************/
void CloudiskServer::register_signin_module()
{
    // 精确路由
    m_server.POST("/user/signin", [](const HttpReq* req, HttpResp* resp, SeriesWork* series)
    {
        if(req->content_type() != APPLICATION_URLENCODED){
            ErrorUtil::send_error(resp, 400);
            return;
        }
        // TODO: 1. 校验Content-Type、解析表单、校验用户名密码非空 (同signup)
        //    注意: form_kv() 返回原始编码值, 必须解码后再用 (见 form_get 注释)
        map<string, string>& data = req->form_kv();
        string username = form_get(data, "username");
        string password = form_get(data, "password");
#ifdef DEBUG
        cout << "[INFO] username: " << username << ", password: " << password << endl; /* 调试信息 */
#endif
        if (username == "" || password == "") {
            ErrorUtil::send_error(resp, 400);
            return ;
        }
        // 从进程内缓存取实例 (不查 Consul, 不阻塞线程)
        ServiceInstance inst = ServiceRegistry::instance().get();
        if (!inst.valid()) {
            ErrorUtil::send_error(resp, 503, "用户服务暂时不可用，请稍后重试");
            return ;
        }
        auto* client = new UserService::SRPCClient{ inst.ip.c_str(), inst.port };

        UserRequest request;
        request.set_username(username);
        request.set_password(password);

        // 把 RPC task 挂到**请求自己的 series** 上, 而不是 client.sign_in(req, done)。
        // 区别只在 task 归谁:
        //   client.sign_in(req, done) 内部是 task->start()  —— task 自成一个 series,
        //     handler 返回后请求 series 就空了、响应立刻发出, 回调再写 resp 已经晚了
        //     (实测: 返回 HTTP 200 但响应体为空)
        //   series->push_back(task)    —— task 属于请求的 series, series 不结束就不发响应,
        //     回调里写 resp 是安全的
        srpc::SRPCClientTask* task = client->create_sign_in_task(
            [resp, client](UserResponse* response, srpc::RPCContext* ctx)
            {
                // 回调里绝不能抛异常出去: 穿过 workflow 回调会导致进程 terminate
                try {
                    if (ctx->success() && response->success()) {
                        nlohmann::json body;
                        body["Username"] = response->username();
                        body["Token"]    = response->token();
                        body["Location"] = "/spa/";    /* 跳转到 SPA 首页 */

                        nlohmann::json json;
                        json["data"] = body;
                        resp->String(json.dump());
                    } else {
                        // 统一 JSON 错误体, 前端 api.js 依赖 {"code","msg"}
                        ErrorUtil::send_error(resp, 400,
                            rpc_err_msg(ctx, *response, "用户名或密码错误"));
                    }
                } catch (const std::exception& e) {
                    cout << "[WARN] 登录回调异常: " << e.what() << endl;
                    ErrorUtil::send_error(resp, 500);
                }
                delete client;
            });

        task->serialize_input(&request);   // 请求被序列化进 task, 局部 request 可安全销毁
        series->push_back(task);           // 入队即走, handler 不阻塞
    });
}


/*********************************************************************************
 *                               用户信息                                        *
 *********************************************************************************/
void CloudiskServer::register_userinfo_module()
{
    m_server.GET("/user/info", [](const HttpReq* req, HttpResp* resp)
    {
        string username = req->query("username");
        string token = req->query("token");
#ifdef DEBUG
        cout << "username: " << username
            << "token: " << token << endl;
#endif
        // 校验Token
        User user;
        if (!CryptoUtil::verify_token(token, user)) {
            ErrorUtil::send_error(resp, 401);
            return ;
        }

        nlohmann::json data;
        data["Username"] = user.username;
        data["SignupAt"] = user.createdAt;

        nlohmann::json json;
        json["data"] = data;
        resp->String(json.dump(2));
    });
}

/*********************************************************************************
 *                               上传文件                                        *
 *********************************************************************************/

void CloudiskServer::register_fileupload_module()
{
    m_server.POST("/file/upload", [](const HttpReq* req, HttpResp* resp)
    {
        string username = req->query("username");
        string token = req->query("token");
        // 2. 校验token
        User user{};
        if (!CryptoUtil::verify_token(token, user)) {
            ErrorUtil::send_error(resp, 401);
            return ;
        }
        // TODO: 3. 校验Content-Type: 必须是MULTIPART_FORM_DATA，否则返回400
        if(req->content_type() != MULTIPART_FORM_DATA){
            ErrorUtil::send_error(resp, 400);
            return;
        }
        // TODO: 4. 处理文件: 遍历 req->form()
        //          (Form = map<string, pair<filename, content>>，用结构化绑定取出)
        //          - CryptoUtil::generate_hashcode 计算文件哈希
        //          - 为每个用户单独创建文件夹 files/<username>/
        //            (access + F_OK 判断是否存在，不存在则 mkdir)
        //          - open(O_WRONLY|O_CREAT|O_TRUNC) / write / close 写入文件
        //          - [OSS备份]
        
        Form& form = req->form();
        for (const auto& [_,file] : form){
            const auto& [filename,content] = file;
            string hashcode = CryptoUtil::generate_hashcode(content.c_str(),content.size());
            string directory = "files/" + username +"/";
            if(access(directory.c_str(),F_OK)){
                mkdir(directory.c_str(),0777);
            }
            string filepath = directory + PathUtil::base(filename);
#ifdef DEBUG
            cout << "filepath: " << filepath << endl;
#endif
            int fd = open(filepath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
            if (fd < 0) {
                ErrorUtil::send_error(resp, 500);
                return ;
            }
            // 写入文件
            write(fd, content.c_str(), content.size());
            close(fd);

            // [OSS备份]: 异步备份，往消息队列中写入一条消息
            // 消息体对齐消费者的 OSSManager::upload_file(bucket, object, file)
            Channel::ptr_t channel = Channel::CreateFromUri(RABBITMQ_URL);
            nlohmann::json obj;
            obj["bucket"]   = OSS_BUCKET;
            obj["object"]   = std::to_string(user.id) + "/" + hashcode;
            obj["file"]     = filepath;
            obj["uid"]      = user.id;
            obj["filename"] = filename;
            obj["hashcode"] = hashcode;
            obj["size"]     = content.size();
            BasicMessage::ptr_t message = BasicMessage::Create(obj.dump());
            // 发送消息 (交换机/队列/绑定已由 RabbitMQ 侧配置完成)
            string exchange = "ossbackup";
            string routingKey = "oss";
            channel->BasicPublish(exchange, routingKey, message);
            // 写`tbl_file`表
            string sql = "REPLACE INTO tbl_file (uid, filename, hashcode, size) VALUES ("
                + std::to_string(user.id) + ", "
                + sql_quote(filename) + ", "
                + sql_quote(hashcode) + ", "
                + std::to_string(content.size()) + ")";

            std::cout << "[SQL] " << sql << endl;

            resp->MySQL(MYSQL_URL, sql, [resp](MySQLResultCursor* cursor)
            {
                if (cursor->get_cursor_status() != MYSQL_STATUS_OK) {
                    ErrorUtil::send_error(resp, 500);
                    return ;
                }
                resp->String(R"({"code":0,"msg":"SUCCESS"})");
            });
        }
    });
}


/*********************************************************************************
 *                               文件列表                                        *
 *********************************************************************************/
void filelist_callback(HttpResp* resp, MySQLResultCursor* cursor)
{
    if(cursor->get_cursor_status()!= MYSQL_STATUS_GET_RESULT){
        ErrorUtil::send_error(resp, 500);
        return;
    }
    /* MySQL任务失败
       提示: cursor->get_cursor_status() != MYSQL_STATUS_GET_RESULT 时返回500 */

    /* 任务执行成功 */
    vector <MySQLCell> record;
    nlohmann::json result = nlohmann::json::array();
    while(cursor->fetch_row(record)){
        nlohmann::json file;
        file["Id"] = record[0].as_int();
        file["FileName"] = record[1].as_string();
        file["FileHash"] = record[2].as_string();
        file["FileSize"] = record[3].as_ulonglong();
        file["UploadAt"] = record[4].as_datetime();
        file["LastUpdated"] = record[5].as_datetime();
        result.push_back(std::move(file));
    }
    resp->String(result.dump(2));
}

void CloudiskServer::register_filelist_module()
{
    m_server.POST("/file/query", [](const HttpReq* req, HttpResp* resp)
    {
        // TODO: 1. 解析请求: query中的username/token，表单中的limit (req->form_kv()["limit"])
        string username = req -> query("username");
        string token = req->query("token");
        string limit = form_get(req->form_kv(), "limit");
        // LIMIT 只接受非负整数: 白名单校验，非法或缺失则回落默认值
        if (limit.empty() || limit.find_first_not_of("0123456789") != string::npos)
            limit = "5";
        // TODO: 2. 校验Token: 失败返回401 Unauthorized
#ifdef DEBUG
        cout << "username: " << username
            << ", token: " << token
            << ", limit: " << limit << endl;
#endif
        // 3. 校验Token
        User user;
        if (!CryptoUtil::verify_token(token, user)) {
            ErrorUtil::send_error(resp, 401);
            return ;
        }
        // TODO: 3. 构建SQL: SELECT filename, hashcode, size, created_at, last_update
        //          FROM tbl_file WHERE uid=<user.id> LIMIT <limit>
        //          通过 resp->MySQL(MYSQL_URL, sql, 回调) 执行，
        //          回调用 std::bind 绑定 filelist_callback(resp, _1)
        string sql = "SELECT id, filename, hashcode, size, created_at, last_update FROM tbl_file WHERE uid="
        + std::to_string(user.id)+ " AND status = 0 LIMIT " + limit;

        std::cout << "[SQL] " << sql << endl;

        resp->MySQL(
            MYSQL_URL,
            sql,
            std::bind(filelist_callback, resp, _1)
        );
    });
}

/*********************************************************************************
 *                               下载文件                                        *
 *********************************************************************************/
// 下载回调: 查库确认文件存在且未被软删后才真正下发文件
void download_callback(HttpResp* resp, string filepath, string filename, MySQLResultCursor* cursor)
{
    if (cursor->get_cursor_status() != MYSQL_STATUS_GET_RESULT) {
        ErrorUtil::send_error(resp, 500);
        return;
    }
    vector<MySQLCell> record;
    if (!cursor->fetch_row(record)) {
        // 文件不存在或已被删除
        ErrorUtil::send_error(resp, 404, "文件不存在或已删除");
        return;
    }
    // 双写兼容: filename* (RFC 5987) 是现代浏览器优先读取的标准写法，
    // filename 作为旧客户端回退。两者都必须是纯 ASCII
    string fname = percent_encode(PathUtil::base(filename));
    resp->set_header_pair("Content-Disposition",
        "attachment; filename=\"" + fname + "\"; filename*=UTF-8''" + fname);
    resp->File(filepath);
}

void CloudiskServer::register_filedownload_module()
{
    m_server.GET("/file/download", [](const HttpReq* req, HttpResp* resp)
    {
        // TODO: 1. 获取请求参数: query中的filename/filehash/username/token
        // wfrest 不解码 query 参数: 浏览器发来的 %E8%AE%BE... 需手动还原成 UTF-8
        string filename = CodeUtil::url_decode(req->query("filename"));
        string filehash = req->query("filehash");
        string username = req -> query("username");
        string token = req->query("token");
        // TODO: 2. 校验Token: 失败返回401 Unauthorized
#ifdef DEBUG
        cout << "filename: " << filename
            <<", filehash: " <<filehash
            <<", username: " <<username
            << ", token: " << token<< endl;

#endif
        // 3. 校验Token
        User user;
        if (!CryptoUtil::verify_token(token, user)) {
            ErrorUtil::send_error(resp, 401);
            return ;
        }

        string filepath = "files/" + user.username + "/" + PathUtil::base(filename);
#ifdef DEBUG
        cout << "filepath: " << filepath << endl;
#endif
        // 下载前查库: 确认该文件属于当前用户且未被软删 (status=0)，否则 404
        // 注意: escape_string_quote 只转义不加引号，外层单引号需自己补
        string sql = "SELECT filename FROM tbl_file WHERE uid = " + std::to_string(user.id)
            + " AND filename = '" + MySQLUtil::escape_string_quote(filename, '\'') + "'"
            + " AND status = 0";

        resp->MySQL(MYSQL_URL, sql, std::bind(download_callback, resp, filepath, filename, _1));
    });
}

/*********************************************************************************
 *                               删除文件(软删)                                  *
 *********************************************************************************/
// 软删回调: 只把 status 置 1，物理文件保留 (回收站恢复需要)
void delete_callback(HttpResp* resp, string id_str, int uid, MySQLResultCursor* cursor)
{
    if (cursor->get_cursor_status() != MYSQL_STATUS_GET_RESULT) {
        ErrorUtil::send_error(resp, 500);
        return;
    }
    vector<MySQLCell> record;
    if (!cursor->fetch_row(record)) {
        ErrorUtil::send_error(resp, 404, "文件不存在或已删除");
        return;
    }

    // 软删: 只改 status 并记录删除时间，行和物理文件都保留，可进回收站、可恢复
    string sql = "UPDATE tbl_file SET status = 1, deleted_at = NOW() WHERE id = " + id_str
        + " AND uid = " + std::to_string(uid);

    resp->MySQL(MYSQL_URL, sql, [resp](MySQLResultCursor* cursor)
    {
        if (cursor->get_cursor_status() != MYSQL_STATUS_OK) {
            ErrorUtil::send_error(resp, 500);
            return;
        }
        resp->String(R"({"code":0,"msg":"SUCCESS"})");
    });
}

void CloudiskServer::register_filedelete_module()
{
    m_server.POST("/file/delete", [](const HttpReq* req, HttpResp* resp)
    {
        string token = req->query("token");
        string id_str = form_get(req->form_kv(), "id");

        // 校验Token
        User user;
        if (!CryptoUtil::verify_token(token, user)) {
            ErrorUtil::send_error(resp, 401);
            return;
        }
        // id 必须是纯数字
        if (id_str.empty() || id_str.find_first_not_of("0123456789") != string::npos) {
            ErrorUtil::send_error(resp, 400, "无效的文件ID");
            return;
        }

        // 先查文件名(校验归属 uid + 未删除 status=0)，拿不到就 404
        string sql = "SELECT filename FROM tbl_file WHERE id = " + id_str
            + " AND uid = " + std::to_string(user.id) + " AND status = 0";

        resp->MySQL(MYSQL_URL, sql, std::bind(delete_callback, resp, id_str, user.id, _1));
    });
}

/*********************************************************************************
 *                               回收站                                           *
 *********************************************************************************/
// 恢复回调: status 置回 0 (软删时物理文件未删，直接恢复即可用)
void restore_callback(HttpResp* resp, string id_str, int uid, MySQLResultCursor* cursor)
{
    if (cursor->get_cursor_status() != MYSQL_STATUS_GET_RESULT) {
        ErrorUtil::send_error(resp, 500);
        return;
    }
    vector<MySQLCell> record;
    if (!cursor->fetch_row(record)) {
        ErrorUtil::send_error(resp, 404, "文件不存在");
        return;
    }

    string sql = "UPDATE tbl_file SET status = 0, deleted_at = NULL WHERE id = " + id_str
        + " AND uid = " + std::to_string(uid);

    resp->MySQL(MYSQL_URL, sql, [resp](MySQLResultCursor* cursor)
    {
        if (cursor->get_cursor_status() != MYSQL_STATUS_OK) {
            ErrorUtil::send_error(resp, 500);
            return;
        }
        resp->String(R"({"code":0,"msg":"SUCCESS"})");
    });
}

// 彻底删除回调: 删行 + 删物理文件 (不可恢复)
void purge_callback(HttpResp* resp, string directory, string id_str, int uid, MySQLResultCursor* cursor)
{
    if (cursor->get_cursor_status() != MYSQL_STATUS_GET_RESULT) {
        ErrorUtil::send_error(resp, 500);
        return;
    }
    vector<MySQLCell> record;
    if (!cursor->fetch_row(record)) {
        ErrorUtil::send_error(resp, 404, "文件不存在");
        return;
    }
    string filename = record[0].as_string();
    string filepath = directory + PathUtil::base(filename);

    string sql = "DELETE FROM tbl_file WHERE id = " + id_str
        + " AND uid = " + std::to_string(uid);

    resp->MySQL(MYSQL_URL, sql, [resp, filepath](MySQLResultCursor* cursor)
    {
        if (cursor->get_cursor_status() != MYSQL_STATUS_OK) {
            ErrorUtil::send_error(resp, 500);
            return;
        }
        // 删行成功后尽力删物理文件: 失败只留下孤儿文件(无害)，不阻断响应
        if (unlink(filepath.c_str()) != 0) {
            std::cout << "[WARN] 物理文件删除失败: " << filepath << endl;
        }
        resp->String(R"({"code":0,"msg":"SUCCESS"})");
    });
}

// 回收站列表回调: 输出 Id/FileName/FileSize/DeletedAt
void recyclelist_callback(HttpResp* resp, MySQLResultCursor* cursor)
{
    if (cursor->get_cursor_status() != MYSQL_STATUS_GET_RESULT) {
        ErrorUtil::send_error(resp, 500);
        return;
    }
    vector<MySQLCell> record;
    nlohmann::json result = nlohmann::json::array();
    while (cursor->fetch_row(record)) {
        nlohmann::json file;
        file["Id"] = record[0].as_int();
        file["FileName"] = record[1].as_string();
        file["FileSize"] = record[2].as_ulonglong();
        file["DeletedAt"] = record[3].as_datetime();
        result.push_back(std::move(file));
    }
    resp->String(result.dump(2));
}

void CloudiskServer::register_recycle_module()
{
    // 回收站列表 (status=1)
    m_server.POST("/file/recycle", [](const HttpReq* req, HttpResp* resp)
    {
        string token = req->query("token");
        User user;
        if (!CryptoUtil::verify_token(token, user)) {
            ErrorUtil::send_error(resp, 401);
            return;
        }
        string sql = "SELECT id, filename, size, deleted_at FROM tbl_file WHERE uid = "
            + std::to_string(user.id) + " AND status = 1";

        resp->MySQL(MYSQL_URL, sql, std::bind(recyclelist_callback, resp, _1));
    });

    // 恢复 (status 置回 0)
    m_server.POST("/file/restore", [](const HttpReq* req, HttpResp* resp)
    {
        string token = req->query("token");
        string id_str = form_get(req->form_kv(), "id");

        User user;
        if (!CryptoUtil::verify_token(token, user)) {
            ErrorUtil::send_error(resp, 401);
            return;
        }
        if (id_str.empty() || id_str.find_first_not_of("0123456789") != string::npos) {
            ErrorUtil::send_error(resp, 400, "无效的文件ID");
            return;
        }

        string sql = "SELECT filename FROM tbl_file WHERE id = " + id_str
            + " AND uid = " + std::to_string(user.id) + " AND status = 1";

        resp->MySQL(MYSQL_URL, sql, std::bind(restore_callback, resp, id_str, user.id, _1));
    });

    // 彻底删除 (删行 + 删物理文件)
    m_server.POST("/file/purge", [](const HttpReq* req, HttpResp* resp)
    {
        string token = req->query("token");
        string id_str = form_get(req->form_kv(), "id");

        User user;
        if (!CryptoUtil::verify_token(token, user)) {
            ErrorUtil::send_error(resp, 401);
            return;
        }
        if (id_str.empty() || id_str.find_first_not_of("0123456789") != string::npos) {
            ErrorUtil::send_error(resp, 400, "无效的文件ID");
            return;
        }

        string directory = "files/" + user.username + "/";
        string sql = "SELECT filename FROM tbl_file WHERE id = " + id_str
            + " AND uid = " + std::to_string(user.id) + " AND status = 1";

        resp->MySQL(MYSQL_URL, sql, std::bind(purge_callback, resp, directory, id_str, user.id, _1));
    });
}

/*********************************************************************************
 *                    回收站自动清理 (软删超过 7 天彻底删除)                       *
 *********************************************************************************/
// 「先查后删」两步: SELECT 拿到 (username, filename) 用于删物理文件, 再 DELETE 删行。
// 两步之间存在微小竞态 (SELECT 后、DELETE 前恰被恢复的文件仍会被 unlink)，
// 学习项目可接受；生产环境需用事务或墓碑复核规避。
static void recycle_sweep()
{
    // status/deleted_at 仅存在于 tbl_file，JOIN 下无歧义，无需表别名
    string expired_cond = "status = 1 AND deleted_at < NOW() - INTERVAL "
        + std::to_string(RECYCLE_EXPIRE_DAYS) + " DAY";

    // 第一步: 查出过期记录的 username + filename
    WFMySQLTask* query = WFTaskFactory::create_mysql_task(MYSQL_URL, 1,
        [expired_cond](WFMySQLTask* task)
        {
            MySQLResultCursor cursor(task->get_resp());
            int status = cursor.get_cursor_status();
            if (status == MYSQL_STATUS_ERROR) {
                std::cout << "[WARN] 回收站清理查询失败 (tbl_file.deleted_at 列是否已迁移?)" << endl;
                return;
            }
            if (status != MYSQL_STATUS_GET_RESULT) return;

            vector<MySQLCell> row;
            vector<string> paths;
            while (cursor.fetch_row(row)) {
                string username = row[0].as_string();
                string filename = row[1].as_string();
                paths.push_back("files/" + username + "/" + PathUtil::base(filename));
            }
            if (paths.empty()) return;

            // 第二步: 删掉所有过期行 (单条 DELETE 即可)
            WFMySQLTask* del = WFTaskFactory::create_mysql_task(MYSQL_URL, 1,
                [paths](WFMySQLTask* dtask)
                {
                    // 第三步: 删物理文件 (尽力而为, 失败留孤儿文件, 无害)
                    for (const string& p : paths) {
                        if (unlink(p.c_str()) != 0)
                            std::cout << "[WARN] 回收站清理失败: " << p << endl;
                    }
                });
            del->get_req()->set_query("DELETE FROM tbl_file WHERE " + expired_cond);
            Workflow::create_series_work(del, nullptr)->start();
        });

    query->get_req()->set_query(
        "SELECT u.username, f.filename FROM tbl_file f JOIN tbl_user u ON f.uid = u.id WHERE "
        + expired_cond);
    Workflow::create_series_work(query, nullptr)->start();
}

// 定时回调: 扫一次后重新调度下一次
static void sweep_timer_callback(WFTimerTask* timer)
{
    recycle_sweep();
    WFTimerTask* next = WFTaskFactory::create_timer_task(RECYCLE_SWEEP_INTERVAL, 0, sweep_timer_callback);
    next->start();
}

void CloudiskServer::start_recycle_sweep()
{
    WFTimerTask* timer = WFTaskFactory::create_timer_task(RECYCLE_SWEEP_INTERVAL, 0, sweep_timer_callback);
    timer->start();
}

/*********************************************************************************
 *                          分片上传 / 断点续传                                    *
 *********************************************************************************/
// Redis 地址从 Redis.env 读取 (参照 OSS.env 的做法)。
// 容器化部署时地址会变, 做成配置可避免改代码 —— consul2 曾因 IP 重分配
// 导致整个集群丢 leader, 排查很久。
static const string& redis_url()
{
    static const string url = []() -> string
    {
        ifstream in("Redis.env");
        string line;
        while (getline(in, line)) {
            size_t eq = line.find('=');
            if (eq == string::npos) continue;

            string key = line.substr(0, eq);
            size_t kb = key.find_first_not_of(" \t");
            if (kb == string::npos) continue;
            size_t ke = key.find_last_not_of(" \t");
            if (key.substr(kb, ke - kb + 1) != "url") continue;

            string val = line.substr(eq + 1);
            size_t vb = val.find_first_not_of(" \t");
            if (vb == string::npos) continue;
            size_t ve = val.find_last_not_of(" \t;\"");
            val = val.substr(vb, ve - vb + 1);
            if (!val.empty() && val.front() == '"') val.erase(val.begin());
            if (!val.empty()) return val;
        }
        return string("redis://127.0.0.1:6379");   // 配置缺失时的兜底
    }();
    return url;
}

// upload_id 是前端算出的采样指纹 (sha256 的十六进制)。
// 必须严格白名单校验 —— 它会被直接拼进磁盘路径,
// 放行 "../" 就是路径穿越漏洞。
static bool valid_upload_id(const string& s)
{
    if (s.empty() || s.size() > 64) return false;
    return s.find_first_not_of("0123456789abcdef") == string::npos;
}

// ⚠️ upload_id 是内容指纹: 不同用户上传同一份文件会得到相同的值。
// 因此 Redis 键与暂存目录都必须按 uid 隔离 —— 否则两个用户会共享分片集合,
// 一方 complete 后清理掉, 另一方就永远传不完。
static string chunk_set_key(int uid, const string& upload_id)
{
    return "upload:" + std::to_string(uid) + ":" + upload_id + ":chunks";
}

static string chunk_dir(int uid, const string& upload_id)
{
    return "tmp/" + std::to_string(uid) + "/" + upload_id + "/";
}

// 递归创建目录。
// mkdir(2) 不创建父目录 —— 这个坑在 files/ 上踩过一次, 加 uid 一级后又踩一次,
// 所以这里统一用递归版本, 以后路径再加层级也不会出问题。
static bool ensure_dir(const string& path)
{
    if (path.empty()) return false;
    string cur;
    size_t start = 0;
    while (start < path.size()) {
        size_t slash = path.find('/', start);
        if (slash == string::npos) slash = path.size();
        cur = path.substr(0, slash);
        if (!cur.empty() && access(cur.c_str(), F_OK) != 0) {
            if (mkdir(cur.c_str(), 0777) != 0 && errno != EEXIST)
                return false;
        }
        start = slash + 1;
    }
    return true;
}

// 把「已传分片下标」查出来返回给前端 —— 这是断点续传的关键:
// 前端拿到后跳过这些下标, 剩下的照传。
static void reply_done_chunks(HttpResp* resp, int uid, const string& upload_id)
{
    resp->Redis(redis_url(), "SMEMBERS", {chunk_set_key(uid, upload_id)},
        [resp, upload_id](WFRedisTask* task)
        {
            try {
                RedisValue val;
                task->get_resp()->get_result(val);
                nlohmann::json done = nlohmann::json::array();
                if (val.is_array()) {
                    for (size_t i = 0; i < val.arr_size(); i++)
                        done.push_back(std::stoi(val.arr_at(i).string_value()));
                }
                nlohmann::json out;
                out["upload_id"] = upload_id;
                out["done"] = done;
                resp->String(out.dump());
            } catch (const std::exception& e) {
                cout << "[WARN] 查询已传分片异常: " << e.what() << endl;
                ErrorUtil::send_error(resp, 500);
            }
        });
}

void CloudiskServer::register_chunkupload_module()
{
    // ============ init: 建会话 + 返回已传分片 ============
    m_server.POST("/file/upload/init", [](const HttpReq* req, HttpResp* resp)
    {
        string token = req->query("token");
        User user;
        if (!CryptoUtil::verify_token(token, user)) {
            ErrorUtil::send_error(resp, 401);
            return ;
        }

        map<string, string>& data = req->form_kv();
        string upload_id = form_get(data, "upload_id");
        string filename  = form_get(data, "filename");
        string size_str  = form_get(data, "size");
        string chunk_str = form_get(data, "chunk_size");

        if (!valid_upload_id(upload_id)) {
            ErrorUtil::send_error(resp, 400, "无效的上传标识");
            return ;
        }
        if (filename.empty() || filename.size() > 255) {
            ErrorUtil::send_error(resp, 400, "无效的文件名");
            return ;
        }
        if (size_str.empty() || size_str.find_first_not_of("0123456789") != string::npos ||
            chunk_str.empty() || chunk_str.find_first_not_of("0123456789") != string::npos) {
            ErrorUtil::send_error(resp, 400, "无效的文件大小");
            return ;
        }
        long long total_size = 0;
        int chunk_size = 0;
        try {
            total_size = std::stoll(size_str);
            chunk_size = std::stoi(chunk_str);
        } catch (const std::exception&) {
            ErrorUtil::send_error(resp, 400, "无效的文件大小");
            return ;
        }
        if (chunk_size <= 0 || total_size < 0) {
            ErrorUtil::send_error(resp, 400, "无效的文件大小");
            return ;
        }
        int total_chunks = (int)((total_size + chunk_size - 1) / chunk_size);
        if (total_chunks <= 0 || total_chunks > CHUNK_MAX_COUNT) {
            ErrorUtil::send_error(resp, 400, "分片数超出限制");
            return ;
        }

        // 会话按 (uid, upload_id) 定位。
        // upload_id 是内容指纹, 不同用户上传同一份文件会得到相同值,
        // 所以查询必须带上 uid —— 否则会误判成"冲突"而拒绝第二个用户。
        string sel = "SELECT 1 FROM tbl_upload WHERE uid = " + std::to_string(user.id)
            + " AND upload_id = " + sql_quote(upload_id);

        resp->MySQL(MYSQL_URL, sel,
            [resp, upload_id, user, filename, size_str, chunk_str, total_chunks]
            (MySQLResultCursor* cursor)
        {
            if (cursor->get_cursor_status() != MYSQL_STATUS_GET_RESULT) {
                ErrorUtil::send_error(resp, 500);
                return ;
            }
            vector<MySQLCell> row;
            if (cursor->fetch_row(row)) {
                // 自己的会话已存在 -> 返回进度 (断点续传)
                reply_done_chunks(resp, user.id, upload_id);
                return ;
            }

            // 不存在 -> 建会话
            string ins = "INSERT INTO tbl_upload "
                "(upload_id, uid, filename, total_size, chunk_size, total_chunks) VALUES ("
                + sql_quote(upload_id) + ", " + std::to_string(user.id) + ", "
                + sql_quote(filename) + ", " + size_str + ", " + chunk_str + ", "
                + std::to_string(total_chunks) + ")";

            resp->MySQL(MYSQL_URL, ins, [resp, upload_id, user](MySQLResultCursor* c2)
            {
                if (c2->get_cursor_status() != MYSQL_STATUS_OK) {
                    ErrorUtil::send_error(resp, 500);
                    return ;
                }
                reply_done_chunks(resp, user.id, upload_id);   // 新会话, done 为空
            });
        });
    });

    // ============ chunk: 收单个分片 ============
    m_server.POST("/file/upload/chunk", [](const HttpReq* req, HttpResp* resp)
    {
        string token = req->query("token");
        User user;
        if (!CryptoUtil::verify_token(token, user)) {
            ErrorUtil::send_error(resp, 401);
            return ;
        }
        if (req->content_type() != MULTIPART_FORM_DATA) {
            ErrorUtil::send_error(resp, 400);
            return ;
        }

        Form& form = req->form();
        auto it_id    = form.find("upload_id");
        auto it_idx   = form.find("index");
        auto it_chunk = form.find("chunk");
        if (it_id == form.end() || it_idx == form.end() || it_chunk == form.end()) {
            ErrorUtil::send_error(resp, 400, "缺少分片参数");
            return ;
        }

        string upload_id = it_id->second.second;
        const string& index_str = it_idx->second.second;
        const string& content   = it_chunk->second.second;

        if (!valid_upload_id(upload_id)) {
            ErrorUtil::send_error(resp, 400, "无效的上传标识");
            return ;
        }
        if (index_str.empty() || index_str.find_first_not_of("0123456789") != string::npos) {
            ErrorUtil::send_error(resp, 400, "无效的分片序号");
            return ;
        }

        // 归属校验必须在写盘之前: 确认这个会话属于当前用户
        string sel = "SELECT total_chunks FROM tbl_upload WHERE upload_id = " + sql_quote(upload_id)
            + " AND uid = " + std::to_string(user.id) + " AND status = 0";

        resp->MySQL(MYSQL_URL, sel,
            [resp, upload_id, index_str, content, user](MySQLResultCursor* cursor)
        {
            if (cursor->get_cursor_status() != MYSQL_STATUS_GET_RESULT) {
                ErrorUtil::send_error(resp, 500);
                return ;
            }
            vector<MySQLCell> row;
            if (!cursor->fetch_row(row)) {
                ErrorUtil::send_error(resp, 404, "上传会话不存在或已失效");
                return ;
            }
            int total_chunks = row[0].as_int();
            int index = std::stoi(index_str);
            if (index < 0 || index >= total_chunks) {
                ErrorUtil::send_error(resp, 400, "分片序号超出范围");
                return ;
            }

            // 落盘。目录按 (uid, upload_id) 隔离
            string dir = chunk_dir(user.id, upload_id);
            if (!ensure_dir(dir)) {
                cout << "[WARN] 创建分片目录失败: " << dir << endl;
                ErrorUtil::send_error(resp, 500);
                return ;
            }
            string part = dir + index_str + ".part";
            int fd = open(part.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
            if (fd < 0) {
                cout << "[WARN] 写入分片失败: " << part << endl;
                ErrorUtil::send_error(resp, 500);
                return ;
            }
            ssize_t written = write(fd, content.c_str(), content.size());
            close(fd);
            if (written != (ssize_t)content.size()) {
                ErrorUtil::send_error(resp, 500);
                return ;
            }

            // 标记已传 + 续期。重复上传同一 index 即覆盖写, 天然幂等
            resp->Redis(redis_url(), "SADD",
                {chunk_set_key(user.id, upload_id), index_str},
                [resp, upload_id, user](WFRedisTask* t)
                {
                    RedisValue v;
                    t->get_resp()->get_result(v);
                    if (!v.is_ok()) {
                        ErrorUtil::send_error(resp, 500);
                        return ;
                    }
                    // 每次上传都续期, 活跃会话不会中途过期
                    resp->Redis(redis_url(), "EXPIRE",
                        {chunk_set_key(user.id, upload_id), std::to_string(CHUNK_SESSION_TTL)},
                        [resp](WFRedisTask*) {
                            resp->String(R"({"code":0,"msg":"SUCCESS"})");
                        });
                });
        });
    });

    // ============ complete: 合并 + 落库 + 投递 OSS ============
    m_server.POST("/file/upload/complete", [](const HttpReq* req, HttpResp* resp)
    {
        string token = req->query("token");
        User user;
        if (!CryptoUtil::verify_token(token, user)) {
            ErrorUtil::send_error(resp, 401);
            return ;
        }

        string upload_id = form_get(req->form_kv(), "upload_id");
        if (!valid_upload_id(upload_id)) {
            ErrorUtil::send_error(resp, 400, "无效的上传标识");
            return ;
        }

        string sel = "SELECT filename, total_size, total_chunks FROM tbl_upload WHERE upload_id = "
            + sql_quote(upload_id) + " AND uid = " + std::to_string(user.id) + " AND status = 0";

        resp->MySQL(MYSQL_URL, sel,
            [resp, upload_id, user](MySQLResultCursor* cursor)
        {
            if (cursor->get_cursor_status() != MYSQL_STATUS_GET_RESULT) {
                ErrorUtil::send_error(resp, 500);
                return ;
            }
            vector<MySQLCell> row;
            if (!cursor->fetch_row(row)) {
                ErrorUtil::send_error(resp, 404, "上传会话不存在或已完成");
                return ;
            }
            string filename   = row[0].as_string();
            long long total   = row[1].as_ulonglong();
            int total_chunks  = row[2].as_int();

            string dir = chunk_dir(user.id, upload_id);
            string directory = "files/" + user.username + "/";
            if (access(directory.c_str(), F_OK) != 0 && mkdir(directory.c_str(), 0777) != 0) {
                ErrorUtil::send_error(resp, 500);
                return ;
            }
            string filepath = directory + PathUtil::base(filename);

            // ---- 合并: 顺序读分片追加写最终文件, 同时增量算 hash ----
            // 内存恒定 64KB, 与文件大小无关 —— 这是「分片」真正换来的东西。
            // (注: 这里是同步磁盘 IO, 会占用 handler 线程; 与现有上传接口的
            //  write 一致, 学习阶段可接受, 生产可改用 WFFileIOTask)
            EVP_MD_CTX* mdctx = EVP_MD_CTX_new();
            EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL);

            int out = open(filepath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
            if (out < 0) {
                EVP_MD_CTX_free(mdctx);
                ErrorUtil::send_error(resp, 500);
                return ;
            }

            long long merged = 0;
            bool ok = true;
            vector<char> buf(64 * 1024);
            for (int i = 0; i < total_chunks && ok; i++) {
                string part = dir + std::to_string(i) + ".part";
                int in = open(part.c_str(), O_RDONLY);
                if (in < 0) { ok = false; break; }      // 有分片缺失 => 未传完
                ssize_t n;
                while ((n = read(in, buf.data(), buf.size())) > 0) {
                    if (write(out, buf.data(), n) != n) { ok = false; break; }
                    EVP_DigestUpdate(mdctx, buf.data(), n);
                    merged += n;
                }
                close(in);
                if (n < 0) ok = false;
            }
            close(out);

            if (!ok) {
                EVP_MD_CTX_free(mdctx);
                unlink(filepath.c_str());               // 清掉半成品
                ErrorUtil::send_error(resp, 400, "分片不完整，请先传完所有分片");
                return ;
            }

            unsigned char hash[EVP_MAX_MD_SIZE];
            unsigned int hash_len = 0;
            EVP_DigestFinal(mdctx, hash, &hash_len);
            EVP_MD_CTX_free(mdctx);

            char hex[EVP_MAX_MD_SIZE * 2 + 1] = { '\0' };
            for (unsigned i = 0; i < hash_len; i++)
                sprintf(hex + 2 * i, "%02x", hash[i]);
            string hashcode(hex);

            if (merged != total) {
                unlink(filepath.c_str());
                ErrorUtil::send_error(resp, 400, "文件大小与声明不符");
                return ;
            }

            // ---- 落库 ----
            string sql = "REPLACE INTO tbl_file (uid, filename, hashcode, size) VALUES ("
                + std::to_string(user.id) + ", "
                + sql_quote(PathUtil::base(filename)) + ", "
                + sql_quote(hashcode) + ", "
                + std::to_string(merged) + ")";

            resp->MySQL(MYSQL_URL, sql, [resp, upload_id, user, filepath, hashcode, merged]
                (MySQLResultCursor* c2)
            {
                if (c2->get_cursor_status() != MYSQL_STATUS_OK) {
                    ErrorUtil::send_error(resp, 500);
                    return ;
                }

                // ---- 投递 OSS 备份 (与整文件上传同一套消息格式) ----
                try {
                    Channel::ptr_t channel = Channel::CreateFromUri(RABBITMQ_URL);
                    nlohmann::json obj;
                    obj["bucket"]   = OSS_BUCKET;
                    obj["object"]   = std::to_string(user.id) + "/" + hashcode;
                    obj["file"]     = filepath;
                    obj["uid"]      = user.id;
                    BasicMessage::ptr_t message = BasicMessage::Create(obj.dump());
                    channel->BasicPublish("ossbackup", "oss", message);
                } catch (const std::exception& e) {
                    cout << "[WARN] OSS 备份消息投递失败: " << e.what() << endl;
                }

                // ---- 清理: 分片目录 + Redis 会话 + 会话行 ----
                string dir = chunk_dir(user.id, upload_id);
                string rm = "rm -rf " + dir;
                if (system(rm.c_str()) != 0)
                    cout << "[WARN] 清理分片目录失败: " << dir << endl;

                resp->Redis(redis_url(), "DEL", {chunk_set_key(user.id, upload_id)},
                    [resp, upload_id](WFRedisTask*)
                    {
                        string del = "DELETE FROM tbl_upload WHERE upload_id = " + sql_quote(upload_id);
                        resp->MySQL(MYSQL_URL, del, [resp](MySQLResultCursor*)
                        {
                            resp->String(R"({"code":0,"msg":"SUCCESS"})");
                        });
                    });
            });
        });
    });
}

/*********************************************************************************
 *                    分片暂存区清理 (超期未完成的会话)                            *
 *********************************************************************************/
// 磁盘上的 tmp/<upload_id>/ 不会随 Redis 的 TTL 自动消失, 需要单独回收。
// 口径: 会话表里 updated_at 超过 CHUNK_SESSION_TTL 的行 => 删目录 + 删行。
static void chunk_sweep()
{
    string cond = "updated_at < NOW() - INTERVAL " + std::to_string(CHUNK_SESSION_TTL) + " SECOND";

    WFMySQLTask* query = WFTaskFactory::create_mysql_task(MYSQL_URL, 1,
        [cond](WFMySQLTask* task)
        {
            MySQLResultCursor cursor(task->get_resp());
            if (cursor.get_cursor_status() != MYSQL_STATUS_GET_RESULT) return;

            vector<MySQLCell> row;
            vector<pair<int, string>> sessions;
            while (cursor.fetch_row(row)) {
                int uid = row[0].as_int();
                string id = row[1].as_string();
                sessions.emplace_back(uid, id);
            }
            if (sessions.empty()) return;

            for (const auto& [uid, id] : sessions) {
                if (!valid_upload_id(id)) continue;     // 防御: 只删合法命名的目录
                string dir = chunk_dir(uid, id);
                string rm = "rm -rf " + dir;
                if (system(rm.c_str()) != 0)
                    cout << "[WARN] 清理分片目录失败: " << dir << endl;
            }

            WFMySQLTask* del = WFTaskFactory::create_mysql_task(MYSQL_URL, 1,
                [](WFMySQLTask*) {});
            del->get_req()->set_query("DELETE FROM tbl_upload WHERE " + cond);
            Workflow::create_series_work(del, nullptr)->start();
        });

    query->get_req()->set_query("SELECT uid, upload_id FROM tbl_upload WHERE " + cond);
    Workflow::create_series_work(query, nullptr)->start();
}

static void chunk_sweep_timer_callback(WFTimerTask*)
{
    chunk_sweep();
    WFTaskFactory::create_timer_task(CHUNK_SWEEP_INTERVAL, 0, chunk_sweep_timer_callback)->start();
}

void CloudiskServer::start_chunk_sweep()
{
    WFTaskFactory::create_timer_task(CHUNK_SWEEP_INTERVAL, 0, chunk_sweep_timer_callback)->start();
}

