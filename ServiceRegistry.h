#pragma once

#include <mutex>
#include <string>

// 一个服务实例的地址
struct ServiceInstance
{
    std::string ip;
    unsigned short port = 0;

    bool valid() const { return !ip.empty() && port != 0; }
};

// UserService 实例表: 进程内缓存。
//
// 设计要点 (对应之前实测出的三个问题):
//   1. 请求路径**只读缓存**, 从不发网络请求
//      => Consul 变慢/挂掉不影响在线请求 (之前是每请求同步查 Consul)
//   2. 刷新交给后台定时任务, 失败时保留旧值 (stale-while-revalidate)
//   3. 没有 WaitGroup, 不存在"漏调 done() 导致 handler 线程永久挂死"
//
// 这也是为什么不用 Redis: 服务发现要的是"关键路径上零网络调用",
// Redis 仍是远程调用, 换汤不换药。Redis 只在网关多实例、需要共享一份
// 缓存时才有价值 (届时它作为 L2, 本地缓存仍是 L1)。
class ServiceRegistry
{
public:
    // Meyers singleton: 线程安全的懒初始化, 无需双检锁
    static ServiceRegistry& instance()
    {
        static ServiceRegistry inst;
        return inst;
    }

    // 读缓存 (一次 mutex, 微秒级)
    ServiceInstance get() const
    {
        std::lock_guard<std::mutex> lock(mtx_);
        return inst_;
    }

    // 后台刷新成功后写入
    void update(const ServiceInstance& inst)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        inst_ = inst;
    }

private:
    ServiceRegistry() = default;
    ServiceRegistry(const ServiceRegistry&) = delete;
    ServiceRegistry& operator=(const ServiceRegistry&) = delete;

    mutable std::mutex mtx_;
    ServiceInstance inst_;
};
