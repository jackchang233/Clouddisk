#include "UserService.srpc.h"
#include "workflow/WFFacilities.h"

using namespace srpc;

static WFFacilities::WaitGroup wait_group(1);

void sig_handler(int signo)
{
	wait_group.done();
}

static void sign_up_done(UserResponse *response, srpc::RPCContext *context)
{
}

static void sign_in_done(UserResponse *response, srpc::RPCContext *context)
{
}

int main()
{
	GOOGLE_PROTOBUF_VERIFY_VERSION;
	const char *ip = "127.0.0.1";
	unsigned short port = 1412;

	UserService::SRPCClient client(ip, port);

	// example for RPC method call
	UserRequest sign_up_req;
	//sign_up_req.set_message("Hello, srpc!");
	client.sign_up(&sign_up_req, sign_up_done);

	wait_group.wait();
	google::protobuf::ShutdownProtobufLibrary();
	return 0;
}
