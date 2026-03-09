# 飞书云文档实时 RAG 系统设计方案

## 一、系统概述

将飞书云文档作为知识库，构建支持**实时更新**的 RAG（检索增强生成）系统，并通过**飞书机器人**提供问答能力。

### 核心能力

| 能力 | 说明 |
|------|------|
| 文档同步 | 从飞书云文档拉取内容，支持 doc/docx 两种格式 |
| 实时更新 | 通过轮询 revision 或事件订阅感知文档变更并增量更新 |
| 向量检索 | 文档分块 → 向量嵌入 → 相似度检索 |
| 飞书机器人 | 在群聊/单聊中 @机器人 提问，自动检索并生成回答 |

---

## 二、架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                        飞书云文档                                 │
│  (doc/docx 文档、多维表格、知识库等)                                │
└───────────────────────┬─────────────────────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        │ 文档 API      │ 事件订阅       │
        │ (拉取内容)    │ (drive.file)   │
        ▼               ▼               │
┌───────────────────────────────────────────────────────────────────┐
│                    文档同步服务 (feishu_doc_sync)                   │
│  - 定期轮询 / 事件触发                                              │
│  - 增量更新：对比 revision，仅更新变更文档                          │
└───────────────────────┬───────────────────────────────────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────────────────────────┐
│                    RAG 引擎 (rag_engine)                            │
│  - 分块 (chunk) → 向量嵌入 (embedding) → 向量库 (ChromaDB/FAISS)   │
│  - 检索 Top-K → 拼接上下文 → LLM 生成回答                          │
└───────────────────────┬───────────────────────────────────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────────────────────────┐
│                    飞书机器人服务 (feishu_bot_server)                │
│  - 接收消息事件 (im.message.receive_v1)                             │
│  - 解析用户问题 → 调用 RAG → 回复消息                               │
└───────────────────────────────────────────────────────────────────┘
```

---

## 三、飞书 API 能力说明

### 3.1 云文档内容获取

| 文档类型 | API | 说明 |
|---------|-----|------|
| 旧版 doc | `GET /open-apis/doc/v2/{docToken}/raw_content` | 纯文本 |
| 新版 docx | `GET /open-apis/docx/v1/documents/{document_id}/raw_content` | 纯文本，推荐 |

**认证**：需 `tenant_access_token`（通过 app_id + app_secret 获取）

**权限**：应用需具备「查看、评论、编辑和管理云空间中所有文件」或「查看、评论、编辑和管理文档」

### 3.2 实时更新机制

| 方式 | 适用场景 | 说明 |
|------|----------|------|
| **轮询 revision** | 云文档 doc/docx | 定期调用文档 API 获取 revision_id，若变化则重新拉取内容 |
| **事件订阅 drive.file** | 云文档 | 订阅 `drive.file.edit_v1` 等事件，文档变更时飞书推送 |
| **多维表格事件** | 多维表格 | `record_added` / `record_deleted` / `record_edited` |

### 3.3 飞书机器人

| 类型 | 能力 | 适用 |
|------|------|------|
| **自定义机器人** | 仅 Webhook 推送，不能接收消息 | 通知类 |
| **企业自建应用** | 可接收消息、回复消息 | RAG 问答 ✓ |

需开通：
- 接收消息：`im:message:receive_v1`
- 发送消息：`im:message:send`
- 获取用户信息等

---

## 四、技术选型

| 组件 | 选型 | 说明 |
|------|------|------|
| 向量库 | ChromaDB 或 FAISS | 轻量、易部署 |
| 嵌入模型 | OpenAI API / DeepSeek / 本地模型 | 与现有 text2sql 一致 |
| LLM | DeepSeek / OpenAI | 生成回答 |
| 服务框架 | FastAPI | 接收飞书事件、提供管理接口 |

---

## 五、配置项

```env
# 飞书应用
FEISHU_APP_ID=cli_xxx
FEISHU_APP_SECRET=xxx

# 文档列表（逗号分隔的 doc_token 或 document_id）
FEISHU_DOC_IDS=docxxx,docxxx

# 嵌入模型（可选，默认与 LLM 一致）
EMBEDDING_API_KEY=
EMBEDDING_MODEL=text-embedding-3-small

# 向量库路径
VECTOR_DB_PATH=./feishu_rag_vectors

# 轮询间隔（秒）
SYNC_INTERVAL=300
```

---

## 六、部署说明

1. **飞书开放平台**：创建企业自建应用，配置权限、事件订阅 URL
2. **事件 URL**：需公网可访问（如 `https://your-domain.com/feishu/event`）
3. **验证**：飞书会发送 GET 请求做 URL 校验，需正确响应
4. **运行**：`python -m feishu_rag.feishu_bot_server` 启动服务

---

## 七、目录结构

```
feishu-rag/
├── FEISHU_RAG_设计.md      # 本文档
├── config.py               # 配置
├── feishu_api_client.py    # 飞书 API（Token、文档内容）
├── feishu_doc_sync.py      # 文档同步（轮询/事件）
├── rag_engine.py           # RAG 引擎
├── feishu_bot_server.py    # 飞书事件服务
└── requirements.txt
```
