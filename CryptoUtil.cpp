#include <openssl/evp.h>
#include <openssl/sha.h>
#include <jwt.h>
#include <stdlib.h>
#include <string.h>

#include "CryptoUtil.h"
#include "User.h"

using namespace std;

// static 全局变量: 其它编译单元引用不了这个变量
static const char* SECRET_KEY = "$Rv&O98@";

string CryptoUtil::generate_salt(int length)
{
    const char* alpha = "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    string result;
    for (int i = 0; i < length; ++i) {
        result += alpha[rand() % 62];
    }
    return result;
}

// OpenSSL 3.0 及更新版本推荐使用 EVP(Envelope) 接口
string CryptoUtil::hash_password(const string& password, const string& salt, const EVP_MD* md)
{
    EVP_MD_CTX* context = EVP_MD_CTX_new(); // 创建 EVP 上下文

    EVP_DigestInit_ex(context, md, NULL);
    // 更新上下文
    EVP_DigestUpdate(context, salt.c_str(), salt.size());
    EVP_DigestUpdate(context, password.c_str(), password.size());
    // 计算哈希值
    unsigned char hash[EVP_MAX_MD_SIZE];    // 最大哈希长度
    unsigned int len = 0;                   // 用来接收实际哈希长度
    EVP_DigestFinal(context, hash, &len);

    // 转换成十六进制字符，存储到result中
    char result[EVP_MAX_MD_SIZE * 2 + 1] = { '\0' };
    for(unsigned i = 0; i < len; i++) {
        sprintf(result + 2 * i, "%02x", hash[i]);
    }

    EVP_MD_CTX_free(context);               // 释放上下文

    return result;
}

string CryptoUtil::generate_token(const User& user, jwt_alg_t algorithm)
{
    jwt_t* jwt;
    jwt_new(&jwt);  // 创建 JWT

    jwt_set_alg(jwt, algorithm, (unsigned char*)SECRET_KEY, strlen(SECRET_KEY));

    // 设置载荷(Payload): 用户自定义数据
    // 不要放敏感数据
    jwt_add_grant(jwt, "sub", "LoginToken");
    jwt_add_grant_int(jwt, "id", user.id);
    jwt_add_grant(jwt, "username", user.username.c_str());	    // 用户ID
    jwt_add_grant(jwt, "created_at", user.createdAt.c_str());   // 注册时间
    jwt_add_grant_int(jwt, "exp", time(NULL) + 3600);   // 过期时间 (1小时)

    char* token = jwt_encode_str(jwt);		// token长度是不确定的，100-300字节
    string result = token;

    // 释放资源
    jwt_free(jwt);
    free(token);

    return result;
}

bool CryptoUtil::verify_token(const string& token, User& user)
{
    // 在必要的地方写注释
    // 代码即注释
    jwt_t* jwt;
    int err = jwt_decode(&jwt, token.c_str(), (unsigned char*)SECRET_KEY, strlen(SECRET_KEY));
    if (err) {
        return false;
    }

    // 验证主题
    const char* subject = jwt_get_grant(jwt, "sub");    
    if (subject == nullptr || strcmp(subject, "LoginToken") != 0) {
        jwt_free(jwt);
        return false;
    }

    // 验证是否超时
    long expire = jwt_get_grant_int(jwt, "exp");
    if (expire < time(NULL)) {
        jwt_free(jwt);
        return false;
    }

    user.id = jwt_get_grant_int(jwt, "id");
    user.username = jwt_get_grant(jwt, "username");
    user.createdAt = jwt_get_grant(jwt, "created_at");

    jwt_free(jwt);
    return true;
}

std::string CryptoUtil::generate_hashcode(const char* data, size_t n, const EVP_MD* md)
{
    EVP_MD_CTX* context = EVP_MD_CTX_new();     // 创建 EVP 上下文
    
    EVP_DigestInit_ex(context, md, NULL);

    EVP_DigestUpdate(context, data, n);

    // 计算哈希值
    unsigned char hash[EVP_MAX_MD_SIZE];    // 最大哈希长度
    unsigned int len = 0;                   // 用来接收实际哈希长度
    EVP_DigestFinal(context, hash, &len);   

    char result[EVP_MAX_MD_SIZE * 2 + 1] = { '\0' };
    // 转换成十六进制字符，存储到output中
    for (unsigned i = 0; i < len; i++) {
        sprintf(result + 2 * i, "%02x", hash[i]);
    }
    EVP_MD_CTX_free(context);               // 释放上下文

    return result;
}
