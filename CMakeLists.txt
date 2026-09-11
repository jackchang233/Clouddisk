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
