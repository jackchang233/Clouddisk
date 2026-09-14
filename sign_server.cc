#include <iostream>
#include <signal.h>

#include "UserService.srpc.h"
#include "workflow/WFFacilities.h"
#include "workflow/WFTaskFactory.h"
#include "workflow/MySQLResult.h"
#include "workflow/MySQLUtil.h"
#include "CryptoUtil.h"
#include "User.h"
#include "SqlUtil.h"
#include "ppconsul/agent.h"

using namespace std;
using namespace std::placeholders;
using namespace srpc;
using namespace protocol;
using namespace ppconsul::agent;
using ppconsul::Consul;

static const int RETRY_MAX = 3;
static const std::string MYSQL_URL = "mysql://root:123456@localhost:3306/test";
static WFFacilities::WaitGroup wait_group { 1 };

// MySQL 错误码 1062 = ER_DUP_ENTRY (唯一键冲突)，即用户名已存在
static const int MYSQL_ER_DUP_ENTRY = 1062;

// 用户名或密码错误的统一文案: 两种失败用同一句话，避免暴露"哪些用户名已注册"
static const char* const ERR_LOGIN = "用户名或密码错误";
static const char* const ERR_DB = "服务暂时不可用，请稍后重试";

void sig_handler(int signo)
{
	wait_group.done();
}

class UserServiceServiceImpl : public UserService::Service
{
public:

	void sign_up(UserRequest *req, UserResponse *resp, srpc::RPCContext *ctx) override
	{
		const string& username = req->username();
		const string& password = req->password();

		string salt = CryptoUtil::generate_salt();
		string hashcode = CryptoUtil::hash_password(password, salt);
		string sql = "INSERT INTO tbl_user (username, password, salt) VALUES ("
			+ sql_quote(username) + ", "
			+ sql_quote(hashcode) + ", "
			+ sql_quote(salt) + ")";
		cout << "[SQL] " << sql << endl;   /* 日志 */

		WFMySQLTask* mysqlTask = WFTaskFactory::create_mysql_task(MYSQL_URL, RETRY_MAX,
			[resp](WFMySQLTask* task)
			{
				if (task->get_state() != WFT_STATE_SUCCESS) {
					resp->set_success(false);
					resp->set_err_msg(ERR_DB);
					return;
				}
				if (task->get_resp()->get_packet_type() == MYSQL_PACKET_ERROR) {
					resp->set_success(false);
					// 唯一键冲突才是"已存在"，其他失败(磁盘满/权限等)不能误报
					resp->set_err_msg(task->get_resp()->get_error_code() == MYSQL_ER_DUP_ENTRY
						? "用户名已存在" : ERR_DB);
					return;
				}
				resp->set_success(true);
			});
		mysqlTask->get_req()->set_query(sql);

		SeriesWork* series = ctx->get_series();
		series->push_back(mysqlTask);
	}



	void sign_in(UserRequest *req, UserResponse *resp, srpc::RPCContext *ctx) override
	{
		const string& username = req->username();
		const string& password = req->password();

		// username 来自客户端，必须转义后再拼 (防 SQL 注入)
		string sql = "SELECT * FROM tbl_user WHERE username = " + sql_quote(username)
			+ " AND tomb = 0";
		cout << "[SQL] " << sql << endl;   /* 日志 */

		WFMySQLTask* mysqlTask = WFTaskFactory::create_mysql_task(
			MYSQL_URL,
			RETRY_MAX,
			std::bind(signin_callback, resp, password, _1)
		);
		mysqlTask->get_req()->set_query(sql);

		SeriesWork* series = ctx->get_series();
		series->push_back(mysqlTask);
	}

private:
	static void signin_callback(UserResponse* resp, const std::string& password, WFMySQLTask* task)
	{
		if (task->get_state() != WFT_STATE_SUCCESS ||
			task->get_resp()->get_packet_type() == MYSQL_PACKET_ERROR) {
			resp->set_success(false);
			resp->set_err_msg(ERR_DB);
			return;
		}
		// MySQL 任务执行成功
		MySQLResultCursor cursor { task->get_resp() };
		std::vector<MySQLCell> record;

		// 空结果集 => 用户不存在，与密码错误同文案
		if (!cursor.fetch_row(record)) {
			resp->set_success(false);
			resp->set_err_msg(ERR_LOGIN);
			return;
		}

		User user;
		user.id = record[0].as_int();
		user.username = record[1].as_string();
		user.hashcode = record[2].as_string();
		user.salt = record[3].as_string();
		user.createdAt = record[4].as_datetime();
#ifdef DEBUG
		cout << "[INFO] id: " << user.id
			<< ", username: " << user.username
			<< ", hashcode: " << user.hashcode
			<< ", salt: " << user.salt
			<< ", createdAt: " << user.createdAt << endl;
#endif

		if (CryptoUtil::hash_password(password, user.salt) != user.hashcode) {
			resp->set_success(false);
			resp->set_err_msg(ERR_LOGIN);
			return;
		}

		resp->set_success(true);
		resp->set_id(user.id);
		resp->set_username(user.username);
		resp->set_createdat(user.createdAt);
		resp->set_token(CryptoUtil::generate_token(user));
	}
};

// 服务注册参数集中在这里, 注册与重注册共用
static const char* const SERVICE_ID   = "UserService1";
static const char* const SERVICE_NAME = "UserService";
static const char* const CONSUL_ADDR  = "http://127.0.0.1:8500";
static const int         HEARTBEAT_SEC = 9;
// TTL 必须显著大于心跳周期, 否则调度抖动会让检查在 passing/critical 之间抖动。
// (原来是 TTL 10s / 心跳 9s, 只有 1s 余量, 实测会 flapping)
static const int         TTL_SEC       = 30;
static unsigned short    g_port = 1412;
static bool              g_registered = false;

// 注册(或重新注册)到 Consul。失败抛异常, 由调用方决定如何处理。
static void register_service(Agent* agent)
{
    agent->registerService(
        kw::id      = SERVICE_ID,
        kw::name    = SERVICE_NAME,
        kw::address = "127.0.0.1",
        // 必须与 server.start() 的端口一致, 否则客户端会连到没人监听的端口
        kw::port    = g_port,
        kw::check   = TtlCheck(std::chrono::seconds{ TTL_SEC })
    );
    g_registered = true;
    cout << "[INFO] 已注册到 Consul: " << SERVICE_ID
         << " 127.0.0.1:" << g_port << endl;
}

// 心跳回调: 已注册则续期, 未注册则重试注册 (Consul 后恢复时可自愈)
static void timer_callback(WFTimerTask* task)
{
    if (task->get_state() != WFT_STATE_SUCCESS) {
        return ;
    }
    SeriesWork* series = series_of(task);
    Agent* agent = (Agent*)series->get_context();

    // 异常必须在这里捕获: 穿过 workflow 回调会导致整个进程 terminate
    // (之前 Consul 不可达时就是这么崩的)
    if (agent != nullptr) {
        try {
            if (g_registered) {
                agent->servicePass(SERVICE_ID);   // 续期心跳
            } else {
                register_service(agent);          // 上次失败, 重试
                agent->servicePass(SERVICE_ID);   // 重注册后立即续期, 否则要等下一轮
            }
        } catch (const std::exception& e) {
            g_registered = false;
            cerr << "[WARN] Consul 交互失败 (服务仍在正常响应): " << e.what() << endl;
        }
    }

    WFTimerTask* nextTask = WFTaskFactory::create_timer_task(
        "health_check",
        HEARTBEAT_SEC,
        0,
        timer_callback
    );
    series->push_back(nextTask);
}

int main()
{
	GOOGLE_PROTOBUF_VERIFY_VERSION;

	signal(SIGINT, sig_handler);
	g_port = 1412;
	SRPCServer server;

	UserServiceServiceImpl userservice_impl;
	server.add_service(&userservice_impl);

	if (server.start(g_port) != 0) {
		cerr << "Error: start SRPCServer failed!" << endl;
		exit(1);
	}

	// 服务注册: 失败只告警、不阻断启动。
	// 之前这里没有 try, Consul 不可达时 ppconsul 抛异常直接 terminate,
	// 导致"注册中心挂 -> 用户服务起不来 -> 登录全挂"的连锁故障。
	// 现在降级为: 服务照常响应, 只是暂时不会被服务发现找到。
	Consul* consul = nullptr;
	Agent*  agent  = nullptr;
	try {
		consul = new Consul{ CONSUL_ADDR, ppconsul::kw::dc = "dc1" };
		agent  = new Agent{ *consul };
		register_service(agent);
		agent->servicePass(SERVICE_ID);
	} catch (const std::exception& e) {
		g_registered = false;
		cerr << "[WARN] 服务注册失败, 降级运行 (实例暂不可被发现): "
			 << e.what() << endl;
	}

	// 心跳序列: 每 HEARTBEAT_SEC 秒一次, 兼做注册重试
	WFTimerTask* timerTask = WFTaskFactory::create_timer_task(
		"health_check",
		HEARTBEAT_SEC,
		0,
		timer_callback
	);
	SeriesWork* series = Workflow::create_series_work(timerTask, nullptr);
	series->set_context(agent);        // 可能为 nullptr, timer_callback 里判空
	series->start();

	wait_group.wait();
	WFTaskFactory::cancel_by_name("health_check");
	server.stop();

	delete agent;
	delete consul;

	google::protobuf::ShutdownProtobufLibrary();
	return 0;
}
