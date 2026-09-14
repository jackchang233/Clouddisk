#include <iostream>
#include <signal.h>

#include "CloudiskServer.h"

WFFacilities::WaitGroup g_waitGroup { 1 };

void sig_handler(int)
{
    g_waitGroup.done();
}

int main()
{

    signal(SIGINT, sig_handler);

    CloudiskServer svr;

    svr.register_modules();
    svr.start_recycle_sweep();
    svr.start_service_discovery();

    if (svr.track().start(8888) == 0) {
        svr.list_routes();
        g_waitGroup.wait();
        svr.stop();
    } else {
        std::cerr << "Error: server start failed!" << std::endl;
    }
}
