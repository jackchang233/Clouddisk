# CloudDisk 项目迭代概览

> 更新日期: 2026-09-03
> 定位: C++ 网络编程练手项目 —— wfrest + workflow + MySQL + Vue3 的个人云盘

## 一、当前架构

```
浏览器 (Vue3 + Element Plus, wfrest 托管 dist)
   │  axios: 统一 JSON 错误通道 / 401 全局登出 / token 拦截器
   ▼
wfrest HTTP 服务 (CloudiskServer)          workflow 异步框架
   │  六大模块: 静态资源/注册/登录/用户信息/上传/文件列表/下载
   │  工具模块: CryptoUtil(盐/SHA-256/JWT) ErrorUtil(统一错误+日志)
   │            ValidatorUtil(注册校验)
   ▼
MySQL (test库)                本地磁盘
  tbl_user (含 tomb)           files/<username>/<filename>   ← 待 blob 化
  tbl_file (含 status, uk_uid_filename 唯一键)
```

**运行方式**: 项目根目录 `./server` (端口 8888)；前端开发 `npm run dev` (Vite proxy 转发 API)。

## 二、已完成迭代

| 版本 | 内容 | 关键产出/修复 |
|---|---|---|
| v0.1 | 后端六大模块默写完成 | 路由 + workflow 异步 + MySQL 回调全链路 |
| v0.2 | 上传 500 修复 | `mkdir` 不递归导致 `open()` 失败；400 检查补 `return` |
| v0.3 | 下载中文文件名修复 | ① 响应头裸 UTF-8 → RFC 5987 `percent_encode` 双写；② wfrest 不解码 query → `CodeUtil::url_decode`。教训: **curl 与浏览器发送行为不同，测试要模拟真实客户端** |
| v0.4 | Vue3 前端重构 | Login/Register/Home 三视图，SPA，替代 jQuery+Bootstrap 多页 |
| v0.5 | 错误处理解耦 | `ErrorUtil` 模块；错误体统一 `{"code","msg"}`；无效 token 统一 401；全局错误日志 `[WARN]/[ERROR]` 分级 (`localtime_r` 防线程竞争) |
| v0.6 | 注册校验模块 | `ValidatorUtil`: 用户名 3-32 位 `[A-Za-z0-9_]`；密码 6-64 位含字母+数字。**注册强制 / 登录宽松**（防新策略锁死老密码） |
| — | 表结构治理 | `uk_uid_filename` 唯一键已建（含去重），为同名覆盖/秒传铺路 |

## 三、待修清单（短平快，随做随勾）

- [ ] 上传 SQL: `REPLACE INTO` → `ON DUPLICATE KEY UPDATE`（唯一键已就位，REPLACE 会重置 `created_at`/`status`，与表设计意图相悖）
- [ ] 上传父目录：`mkdir("files", 0777)` 兜底（现在只靠 files/admin 已存在才不炸）
- [ ] 哈希比对改常量时间（`CRYPTO_memcmp`，防时序侧信道）
- [ ] 密码哈希 SHA-256 → bcrypt/argon2（防离线穷举；SHA-256 太快）
- [ ] 上线前: HTTPS（表单明文传输是当前最大安全洞）
- [ ] `sign_error` 消息体遗留的 HTML 标签拼写（`<html>` 未闭合等）

## 四、规划迭代（按依赖顺序）

### v1.0 内容寻址改造（引用计数）★ 基石，所有后续迭代的前置

把"每用户一份实体"改为"内容池 + 引用表"，获得去重/秒传/干净的删除语义。

```sql
CREATE TABLE tbl_blob (
    hashcode  char(64) PRIMARY KEY,      -- SHA-256 hex, 与 tbl_file.hashcode 类型严格一致
    size      bigint NOT NULL DEFAULT 0,
    refcnt    int NOT NULL DEFAULT 0,    -- 口径: 全部引用行数(含墓碑) —— 与回收站兼容
    created_at datetime DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE tbl_file
    MODIFY hashcode char(64) NOT NULL,
    ADD CONSTRAINT fk_file_blob FOREIGN KEY (hashcode) REFERENCES tbl_blob(hashcode);  -- 防悬挂, 不管计数
```

- 改造点: 上传(temp+rename 原子写 + 计数)、下载(查 hashcode 读池 + status 校验)、删除(refcnt-1)、**列表不动**
- 竞态要点: 先落盘后建行；同 hash 竞写因内容相同而无害
- 原型参照: Unix inode 的 `st_nlink` 就是引用计数，`unlink` 就是"删一个名字"
- 存量迁移: 现有用户目录文件入池 + 回填引用行（或清库重来）

### v1.5 BlobStore 接口化（半天，解耦的延续）

```cpp
class BlobStore {  // put / get / exists / del
};
class LocalBlobStore : public BlobStore { /* 现有 blobs/ 目录逻辑 */ };
```

### v2.0 虚拟文件路径（独立，随需插队）

**前置: v1.0 完成**。路径纯元数据化之后，文件夹/移动/改名 = 纯 `UPDATE`，零磁盘 IO。

- 方案 a: `tbl_file` 加 `path` 列（`WHERE path LIKE '/docs/%'`）—— 练手推荐
- 方案 b: `tbl_folder(id, uid, parent_id, name)` 邻接表 —— 想"较真"再升
- 引入信号: 唯一键升格 `uk(uid, path)`、filename 参数升格 path，属接口版本变更

### v3.0 对象存储（OSS / 本地 MinIO）

**前置: v1.0 + v1.5**。OSS 是内容寻址的极致形态，blob 化后接入 = 只换 `BlobStore` 实现。

- 练手: MinIO（S3 兼容、零成本）→ 上云只换 endpoint+密钥
- 两阶段: 备份模式(本地主存+OSS 异步副本) → 主存模式(OSS 唯一存储，服务器无状态化)
- 信号: 容量超单盘 / 多实例负载均衡 / 容灾 / CDN

### v3.5 RabbitMQ（与 OSS 同期）

**锚点: 上传模块的 `[OSS备份]` TODO 就是第一个消费者。**

- 价值: 削峰(上传秒回) + 解耦(fanout: file.uploaded → OSS 推送/缩略图/审计) + 可靠交付(持久化/重试/死信)
- 与 workflow 的分界: **进程边界**。进程内异步崩了就丢；任务需要持久化、独立 worker、重试时上 MQ
- 引入成本心里账: 消息丢失(confirm+持久化)、重复消费(幂等)、顺序性(单队列单消费者)
- 选型: 任务分发选 RabbitMQ；日志流/回放才需要 Kafka

### v4.0 前端直传 + 签名 URL

**前置: v3.0**。服务器只发签名，浏览器 PUT 直达 OSS，回调确认入账；下载 302 到签名 URL。服务器彻底无状态化。

## 五、设计决策记录（ADR 摘要）

| # | 决策 | 理由 |
|---|---|---|
| D1 | 删除 = 软删优先，物理删除延迟/尽力而为 | 跨 DB+文件系统无事务；先改 DB 的最坏后果(孤儿文件)远轻于先删文件(僵尸文件) |
| D2 | refcnt 口径 = 含墓碑的全量引用行 | 保证回收站期内实体必在，"恢复"永远成立 |
| D3 | 登录不做密码策略校验 | 新策略不能锁死旧密码，格式交给哈希比对裁决 |
| D4 | 错误响应统一 JSON + 单一出口 | 前端拦截器可依赖；日志/埋点单点收口 |
| D5 | 派生数据不落库（如文件类型） | 能从 filename 推导的不存；确需服务端筛选再加到引用层 |
| D6 | 后端校验是底线，前端校验是体验 | 永不信任客户端（curl 可绕过前端） |

## 六、依赖关系总览

```
v1.0 blob化 ──┬──> v1.5 BlobStore ──> v3.0 OSS/MinIO ──┬──> v3.5 RabbitMQ
              │                                        └──> v4.0 前端直传
              └──> v2.0 虚拟路径 (独立，可插队)
```

> 一句话方针: **blob 化回答"存储怎么组织"，虚拟路径回答"用户怎么看"，OSS 回答"存放在哪"，MQ 回答"活儿怎么派"——四个问题独立演化，按依赖顺序引入。**
