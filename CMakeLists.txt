# 设置CMake的最小版本
cmake_minimum_required(VERSION 3.15)

# 项目名称
#   项目可以包含多个目标(target)
#   target: 可执行程序, 静态库, 动态库
project(CloudDisk)

# 指定C++的版本: -std=c++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 禁用 RTTI
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-rtti")

# 设置可执行程序生成的路径
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ../)

# 生成的可执行程序
add_executable(server
    CloudiskServer.cpp
    CryptoUtil.cpp
    ErrorUtil.cpp
    ValidatorUtil.cpp
    OSSManager.cpp
    UserService.pb.cc        # 消息序列化 (登录/注册业务已下沉到 sign_server)
    main.cpp
)

# -lcrypto -lssl -ljwt -lwfrest
# server: target的名字
# PRIVATE: 传播属性, 填PRIVATE即可，固定的用法
target_link_libraries(server PRIVATE
    crypto
    ssl
    jwt
    alibabacloud-oss-cpp-sdk
    curl
    pthread
    wfrest
    SimpleAmqpClient
    srpc                     # 作为 RPC 客户端调用 UserService
    workflow
    protobuf
    lz4
    snappy
)

# 用户服务 (srpc RPC, 监听 1412)
#
# 注意: 不要给这个 target 加 -L/usr/local/lib。系统里有两套 protobuf:
#   /usr/local/lib/libprotobuf.so.31 (3.21)   <- 缺少 pb.cc 需要的符号
#   /usr/lib/x86_64-linux-gnu/libprotobuf.so.32 (4.25) <- 正确的那套
# 默认链接顺序恰好选中系统那套；一旦显式加 -L/usr/local/lib，会被 so.31 抢先而链接失败。
#
# 另: UserService.srpc.h 无法重新生成 (protoc-gen-srpc 未安装)，必须提交进仓库。
add_executable(sign_server
    sign_server.cc
    UserService.pb.cc
    CryptoUtil.cpp
    ValidatorUtil.cpp        # 服务侧独立校验, 不依赖网关
)

target_include_directories(sign_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}    # UserService.pb.h / UserService.srpc.h
)

target_link_libraries(sign_server PRIVATE
    srpc
    workflow
    protobuf
    lz4
    snappy
    ppconsul          # 服务注册 + TTL 心跳
    jwt
    crypto
    ssl
    pthread
)

# OSS 备份消费者 (RabbitMQ worker, 从 ossbackup 交换机消费消息并上传到 OSS)
add_executable(ossbackup
    OSSbackup.cpp
    OSSManager.cpp
)

target_link_libraries(ossbackup PRIVATE
    SimpleAmqpClient
    alibabacloud-oss-cpp-sdk
    curl
    crypto
    ssl
    pthread
)
