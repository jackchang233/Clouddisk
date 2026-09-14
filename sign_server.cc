#include <iostream>
#include <signal.h>

#include "UserService.srpc.h"
#include "workflow/WFFacilities.h"
#include "workflow/WFTaskFactory.h"
#include "workflow/MySQLResult.h"
#include "workflow/MySQLUtil.h"
#include "CryptoUtil.h"
#include "User.h"
#include "ErrorUtil.h"
#include "CloudiskServer.h"

using namespace std;
using namespace std::placeholders;

using namespace srpc;

static const int RETRY_MAX = 3;
static WFFacilities::WaitGroup wait_group { 1 };
static const std::string MYSQL_URL = "mysql://root:123456@localhost:3306/test";
void sig_handler(int signo)
{
	wait_group.done();
}

class UserServiceServiceImpl : public UserService::Service
{
public:

	void sign_up(UserRequest *req, UserResponse *resp, srpc::RPCContext *ctx) override
	{
		// 1. 解析请求参数
        const string& username = req->username();
        const string& password = req->password();
	

        // 3. 校验通过后，创建MySQL任务，将用户名和密码插入到MySQL数据库
        string salt = CryptoUtil::generate_salt();
        string hashcode = CryptoUtil::hash_password(password, salt);
        string sql = "INSERT INTO tbl_user (username, password, salt) VALUES ("
            + sql_quote(username) + ", "
            + sql_quote(hashcode) + ", "
            + sql_quote(salt) + ")";
        cout << "[SQL] " << sql << endl;   /* 日志 */

        WFMySQLTask* mysqlTask = WFTaskFactory::create_mysql_task(MYSQL_URL, RETRY_MAX, [resp](WFMySQLTask* task){
        // 任务失败或者SQL语句执行失败
			if (task->get_state() != WFT_STATE_SUCCESS || 
				task->get_resp()->get_packet_type() == MYSQL_PACKET_ERROR) {
				resp->set_success(false); 
			} else {
				resp->set_success(true);
			}
    	});
    mysqlTask->get_req()->set_query(sql); 

    SeriesWork* series = ctx->get_series();
    series->push_back(mysqlTask);
	}

	

	void sign_in(UserRequest *req, UserResponse *resp, srpc::RPCContext *ctx) override
	{
				// 1. 解析请求参数
        const string& username = req->username();
        const string& password = req->password();
		string sql = "SELECT * FROM tbl_user WHERE username='"
		+ username + "' AND tomb=0";
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
	static void signin_callback(UserResponse* resp, const std::string& password, WFMySQLTask* task){

		using namespace protocol;
        if (task->get_state() != WFT_STATE_SUCCESS ||
            task->get_resp()->get_packet_type() == MYSQL_PACKET_ERROR) {
            resp->set_success(false);
            return ;
        }
        // MySQL 任务执行成功
        MySQLResultCursor cursor { task->get_resp() };
        std::vector<MySQLCell> record;
        
        if (!cursor.fetch_row(record)) {
            resp->set_success(false);
            return ;
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
        string hashcode1 = CryptoUtil::hash_password(password, user.salt);
#ifdef DEBUG
        std::cout << "generated hashcode: " << hashcode1 << endl;
#endif
        if (hashcode1 == user.hashcode) {
            resp->set_success(true); 
            resp->set_id(user.id);
            resp->set_username(user.username);
            resp->set_createdat(user.createdAt);
            resp->set_token(CryptoUtil::generate_token(user));
            return ;
        }
        // 密码错误
        resp->set_success(false);
	}
};

int main()
{
	GOOGLE_PROTOBUF_VERIFY_VERSION;

	signal(SIGINT,sig_handler);
	unsigned short port = 1412;
	SRPCServer server;

	UserServiceServiceImpl userservice_impl;
	server.add_service(&userservice_impl);

	if(server.start(port)==0){
		wait_group.wait();
		server.stop();
	}else{
		cerr << "Error: start SRPCServer failed!" << endl;
		exit(1);
	}

	google::protobuf::ShutdownProtobufLibrary();
	return 0;
}
