# 飞书云文档实时 RAG 系统

基于飞书云文档构建的 RAG（检索增强生成）系统，支持**实时更新**和**飞书机器人**问答。

## 功能

- **文档同步**：从飞书云文档（doc/docx）拉取内容
- **实时更新**：轮询检测文档变更，自动更新向量库
- **RAG 问答**：向量检索 + LLM 生成回答
- **飞书机器人**：在群聊/单聊中 @机器人 提问即可获得回答

## 可视化界面（Streamlit）

```bash
cd feishu-rag
pip install streamlit
streamlit run app_streamlit.py
```

或双击 `run_streamlit.bat`。支持 **Agentic RAG** 与 **经典 RAG** 模式切换。

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

复制 `env_example.txt` 为 `.env`，填写：

- `FEISHU_APP_ID`、`FEISHU_APP_SECRET`：飞书企业自建应用凭证
- `FEISHU_DOC_IDS`：文档 ID 列表（逗号分隔）
- `DEEPSEEK_API_KEY`：用于嵌入和生成（或使用 OpenAI）

### 3. 飞书开放平台配置

1. 创建**企业自建应用**
2. 开通权限：云文档读取、接收消息、发送消息
3. 配置**事件订阅**：Request URL 填 `https://你的域名/feishu/event`

### 4. 启动服务

```bash
cd feishu-rag
python run_bot.py --port 9000
```

服务启动后会：

- 首次全量同步配置的文档到向量库
- 启动后台线程定期同步（默认 5 分钟）
- 监听飞书事件，收到消息时调用 RAG 并回复

## 文档 ID 获取

- **旧版 doc**：链接形如 `https://xxx.feishu.cn/docx/xxx`，其中 `docx` 后的部分为 document_id
- **新版 docx**：同上，或从文档「分享」-「复制链接」中获取

## Agentic RAG（可选）

启用后，Agent 会根据问题**自动选择**工具：
- **RAG 检索**：查询具体事件、某时间/机构的事件列表
- **统计分析**：机构排名、上报数量、按月份/分类统计

```bash
# .env 中设置
FEISHU_AGENTIC_RAG=1

# 或本地测试
python test_agentic.py
```

## 详细设计

参见 [FEISHU_RAG_设计.md](./FEISHU_RAG_设计.md)。
