<div align="center">
  <img src="./frontend/public/favicon.svg" alt="Aniu logo" width="72" />
  <h1>Aniu</h1>
  <p><strong>科技牛牛，带你狠狠干 A 股</strong></p>
  <p>面向 A 股研究与模拟交易的本地交易智能体工作台</p>
  <p>
    <a href="https://github.com/AnacondaKC/Aniu/actions/workflows/ci.yml"><img src="https://github.com/AnacondaKC/Aniu/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
    <a href="https://github.com/AnacondaKC/Aniu/stargazers"><img src="https://img.shields.io/github/stars/AnacondaKC/Aniu?style=flat-square&label=Stars&color=f5b942" alt="GitHub stars" /></a>
    <a href="https://github.com/AnacondaKC/Aniu/network/members"><img src="https://img.shields.io/github/forks/AnacondaKC/Aniu?style=flat-square&label=Forks&color=36cfc9" alt="GitHub forks" /></a>
    <a href="https://github.com/AnacondaKC/Aniu/issues"><img src="https://img.shields.io/github/issues/AnacondaKC/Aniu?style=flat-square&label=Issues&color=eb6f92" alt="GitHub issues" /></a>
  </p>
  <p>
    <img src="https://img.shields.io/badge/Python-3.12%2B-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python 3.12+" />
    <img src="https://img.shields.io/badge/React-19-149eca?style=flat-square&logo=react&logoColor=white" alt="React 19" />
    <img src="https://img.shields.io/badge/FastAPI-0.115%2B-009688?style=flat-square&logo=fastapi&logoColor=white" alt="FastAPI" />
    <img src="https://img.shields.io/badge/SQLite-local--first-003B57?style=flat-square&logo=sqlite&logoColor=white" alt="SQLite" />
  </p>
  <p>
    <a href="#核心能力">核心能力</a> ·
    <a href="#界面一览">界面一览</a> ·
    <a href="#快速开始">快速开始</a> ·
    <a href="#docker-部署">Docker 部署</a>
  </p>
</div>

<p align="center">
  <img src="./docs/screenshots/account-overview.png" alt="Aniu 投资总览页面" width="960" />
</p>

<p align="center"><sub>把行情、研究、工具调用、模拟组合和交易记忆放进一条可追踪的工作流。</sub></p>

---

## 项目简介

Aniu（Aniubot）是一个本地优先的股票交易智能体系统。它以 A 股行情和模拟组合为工作对象，把模型推理、市场数据工具、分阶段任务、运行轨迹、账户状态与可演化记忆连接起来，提供一个适合反复研究、观察和复盘的工作台。

项目当前面向研究与模拟交易场景，不承诺实盘下单能力，也不构成投资建议。

## 核心能力

| 模块               | 能力说明                                                                                    |
| ------------------ | ------------------------------------------------------------------------------------------- |
| **投资总览**       | 聚合模拟组合的账户资产、持仓、委托和收益表现，并提供 A 股行情总览。                         |
| **任务运行**       | 手动或按计划启动分阶段策略任务，查看研究、决策、交易、总结等阶段的运行状态。                |
| **实时轨迹**       | 通过事件流展示思考步骤、工具调用、阶段报告、耗时和 Token 统计，方便定位每次运行发生了什么。 |
| **市场数据工具**   | 统一管理实时行情、K 线、分时、排行、资金流、基本面、研报与资讯等数据工具。                  |
| **记忆梦境**       | 保存可复用的交易经验，记录读取/写入/修改活动，并按计划整理每日运行报告。                    |
| **阶段与模型配置** | 为不同运行阶段配置提示词、模型渠道、模型参数和选择策略。                                    |
| **任务调度**       | 配置交易时段内的自动运行计划，以及账户刷新和记忆整理任务。                                  |
| **本地优先**       | Token 登录、SQLite 持久化、敏感配置管理和 Docker 数据卷均以单机部署为默认路径。             |

### 一条完整工作流

```text
行情与账户数据
      ↓
分阶段智能体运行
      ↓
工具调用与实时轨迹
      ↓
模拟组合与运行报告
      ↓
记忆库与夜间整理
```

## 界面一览

以下截图来自项目工作台，数据为模拟或示例数据，仅用于展示界面和交互流程。

<table>
  <tr>
    <td width="50%">
      <img src="./docs/screenshots/market-overview.png" alt="行情总览页面" width="100%" />
      <p align="center"><strong>行情总览</strong><br /><sub>指数、涨跌分布、资金流与行业/概念排行</sub></p>
    </td>
    <td width="50%">
      <img src="./docs/screenshots/run-history.png" alt="任务运行页面" width="100%" />
      <p align="center"><strong>任务运行</strong><br /><sub>运行记录、阶段进度与策略报告</sub></p>
    </td>
  </tr>
  <tr>
    <td width="50%">
      <img src="./docs/screenshots/tool-management.png" alt="工具管理页面" width="100%" />
      <p align="center"><strong>工具管理</strong><br /><sub>数据工具、系统工具与调用日志目录</sub></p>
    </td>
    <td width="50%">
      <img src="./docs/screenshots/tool-calls.png" alt="工具调用页面" width="100%" />
      <p align="center"><strong>工具调用</strong><br /><sub>展开查看模型思考、工具参数和返回结果</sub></p>
    </td>
  </tr>
  <tr>
    <td width="50%">
      <img src="./docs/screenshots/memory-dreams.png" alt="记忆梦境页面" width="100%" />
      <p align="center"><strong>记忆梦境</strong><br /><sub>整理运行经验，维护可检索的交易记忆</sub></p>
    </td>
    <td width="50%">
      <img src="./docs/screenshots/account-overview.png" alt="投资总览页面" width="100%" />
      <p align="center"><strong>账户总览</strong><br /><sub>资产、持仓、委托与组合变化集中呈现</sub></p>
    </td>
  </tr>
</table>

## 技术栈

| 层次             | 主要技术                                                                          |
| ---------------- | --------------------------------------------------------------------------------- |
| **前端**         | React 19、TypeScript、Vite、React Router、Tailwind CSS、Radix UI、TanStack Query  |
| **后端**         | Python 3.12+、FastAPI、Uvicorn、Pydantic、SQLAlchemy 2、aiosqlite                 |
| **智能体与模型** | OpenAI SDK、Anthropic SDK、分阶段运行编排、SSE 事件流                             |
| **数据与调度**   | 股票数据适配器、SQLite、APScheduler、运行与调用审计记录                           |
| **工程工具**     | OpenAPI Typescript、Vitest、Testing Library、ESLint、Prettier、Ruff、Mypy、Docker |

## 快速开始

### 前提条件

- Python 3.12+
- Node.js 22+ 与 npm
- Docker（仅 Docker 部署需要）

### 一键源码运行

在仓库根目录执行：

```bash
# 安装后端依赖、前端依赖并构建前端
./install.sh
```

安装脚本会创建 <code>.aniu/local/.venv</code>，安装锁定依赖，构建前端，并以前台方式启动统一服务。默认访问地址：<http://127.0.0.1:8000>。

首次运行时：

1. 打开登录页，首次设置一个至少 8 个字符的访问 Token。
2. 进入「主要设置 → 妙想设置」，填写项目所需的妙想 API 密钥。
3. 在「渠道模型」配置可用模型，在「阶段设置」为不同阶段选择模型与提示词。
4. 需要自动执行时，在「交易任务」中配置计划；源码启动时设置 <code>ANIU_ENABLE_SCHEDULER=1</code>。

```bash
ANIU_ENABLE_SCHEDULER=1 ./install.sh
```

> 首次设置身份只允许来自本机回环地址。局域网部署请在启动前通过 <code>ANIU_AUTH_TOKEN</code> 预先设置 Token。

## 本地开发

后端和前端分别运行，适合边改边调试：

```bash
# 终端一：创建后端环境并安装开发依赖
python3 -m venv .aniu/local/.venv
./scripts/install_python_dev.sh .aniu/local/.venv/bin/python
.aniu/local/.venv/bin/python -m backend.serve --reload

# 终端二：安装并启动前端
npm --prefix frontend ci --include=dev
npm --prefix frontend run dev
```

默认地址：

- 前端开发服务器：<http://127.0.0.1:5173>
- 后端 API：<http://127.0.0.1:8000>
- OpenAPI 文档：<http://127.0.0.1:8000/docs>
- Readiness 检查：<http://127.0.0.1:8000/health/ready>

Vite 会将 <code>/api</code> 和 <code>/health</code> 代理到后端 8000 端口，可通过 <code>VITE_BACKEND_PROXY</code> 覆盖代理目标。

需要局域网访问时，显式开启 LAN 模式并提供精确的 Host 白名单：

```bash
.aniu/local/.venv/bin/python -m backend.serve --lan --allowed-host 192.168.1.20
```

局域网环境请在服务前使用 HTTPS 反向代理或 SSH 隧道，不要直接暴露 HTTP 登录端点。

## Docker 部署

Compose 默认只将服务绑定到 <code>127.0.0.1:8000</code>，并使用命名卷 <code>aniu-data</code> 保存数据库、密钥和运行历史。

```bash
mkdir -p .aniu/local
cp .env.example .aniu/local/.env

docker compose --env-file .aniu/local/.env up -d --build
docker compose --env-file .aniu/local/.env ps
curl http://127.0.0.1:8000/health/ready
```

访问 <http://127.0.0.1:8000> 即可使用容器内构建的前端。停止服务但保留数据：

```bash
docker compose --env-file .aniu/local/.env down
```

> 不要随意使用 <code>docker compose down -v</code>，删除卷会同时删除配置、密钥和运行历史。容器默认启用调度器；SQLite 与进程内调度器适合单实例运行。

## 配置

Compose 的基础模板位于 [<code>.env.example</code>](./.env.example)，真正的密钥建议只放在被 Git 忽略的 <code>.aniu/local/.env</code> 中。

| 变量                                         | 说明                          | 默认行为                                                 |
| -------------------------------------------- | ----------------------------- | -------------------------------------------------------- |
| <code>ANIU_AUTH_TOKEN</code>                 | 固定登录 Token，至少 8 个字符 | 未设置时可在本机首次登录页设置                           |
| <code>ANIU_PORT</code>                       | 服务端口                      | <code>8000</code>                                        |
| <code>ANIU_BIND_ADDRESS</code>               | Compose 端口绑定地址          | <code>127.0.0.1</code>                                   |
| <code>ANIU_LAN</code>                        | 是否启用局域网模式            | 源码默认关闭，Docker 模板默认关闭                        |
| <code>ANIU_ALLOWED_HOSTS</code>              | LAN 模式允许的精确 Host 列表  | 不支持 <code>*</code> 通配符                             |
| <code>ANIU_CORS_ORIGINS</code>               | 独立前端访问后端时的来源列表  | 本地服务地址                                             |
| <code>ANIU_ENABLE_SCHEDULER</code>           | 是否启用进程内调度器          | 源码默认关闭，Docker 默认开启                            |
| <code>ANIU_DATA_DIR</code>                   | 数据库、密钥和日志目录        | 源码为 <code>.aniu</code>，容器为 <code>/app/data</code> |
| <code>ANIU_DATABASE_URL</code>               | 可选的数据库连接地址          | 使用 SQLite                                              |
| <code>ANIU_MASTER_SECRET_KEY</code>          | 固定敏感配置加密密钥          | 未设置时由本地运行时管理                                 |
| <code>ANIU_MASTER_SECRET_KEY_PREVIOUS</code> | 密钥轮换时的旧密钥            | 可选                                                     |

模型渠道与妙想配置推荐在登录后的设置页面保存，不要写入 <code>VITE_*</code> 变量、前端构建产物或 URL。

## API 入口

后端 API 统一使用 <code>/api/aniu</code> 前缀，所有业务接口都要求已认证会话。

```text
GET  /health
GET  /health/live
GET  /health/ready

POST /api/aniu/auth/setup
POST /api/aniu/auth/login
POST /api/aniu/auth/logout

GET  /api/aniu/account/dashboard
GET  /api/aniu/market/overview
POST /api/aniu/runs/start
GET  /api/aniu/runs
GET  /api/aniu/settings
GET  /api/aniu/schedules
GET  /api/aniu/memories
GET  /api/aniu/memory-dreams
```

任务详情通过 SSE 推送运行事件；完整请求/响应模型可在服务启动后打开 <http://127.0.0.1:8000/docs> 查看。

## 质量检查

```bash
# 后端
.aniu/local/.venv/bin/python -m ruff check backend scripts
.aniu/local/.venv/bin/python -m mypy backend
.aniu/local/.venv/bin/python -m pytest backend/tests -q

# 前端
npm --prefix frontend test -- --run
npm --prefix frontend run lint -- --max-warnings=0
npm --prefix frontend run format:check
npm --prefix frontend run build

# OpenAPI 契约
npm --prefix frontend run api:generate
npm --prefix frontend run api:check
```

## 项目结构

```text
Aniu/
├── backend/
│   ├── api/          # HTTP、SSE 与 OpenAPI 适配
│   ├── business/     # 业务用例与领域模型
│   ├── infra/        # 数据库、仓库、调度器、Worker 与外部集成
│   ├── bootstrap/    # 应用组装、生命周期与运行时配置
│   ├── agent/        # Agent 核心
│   ├── llm/          # LLM 客户端与协议
│   └── stock_api/    # 股票数据接口适配
├── frontend/
│   ├── src/components/ # 通用布局与 UI 组件
│   ├── src/features/   # 认证、总览、运行、记忆与设置功能
│   └── src/generated/  # OpenAPI 生成的类型
├── scripts/          # 开发与契约脚本
├── docs/screenshots/ # README 页面截图
├── Dockerfile
├── compose.yaml
├── install.sh
├── pyproject.toml
└── requirements.lock
```

<code>.aniu/</code> 只用于本地运行数据、虚拟环境、缓存和日志，不是新克隆项目所需提交的源码目录。

## 安全与使用边界

- 不要把 Token、模型 API Key、妙想密钥或主密钥提交到 Git。
- 不要通过 <code>VITE_*</code> 变量传递后端秘密；这类变量会进入前端开发或构建环境。
- LAN 模式必须配置精确的 <code>ANIU_ALLOWED_HOSTS</code>，并通过 HTTPS 反向代理或 SSH 隧道保护登录流量。
- 妙想及行情服务受其自身额度、稳定性和使用条款约束。
- 项目用于研究与模拟交易，不提供投资建议；请在充分验证后决定是否采用任何分析结果。

## 致谢

感谢 FastAPI、React、Vite、Tailwind CSS、Radix UI、TanStack Query 以及其他开源项目提供的基础设施。Aniu 同时接入妙想和公开行情数据适配器，相关服务请遵循各自的使用条款。

> 许可证：当前仓库未包含 <code>LICENSE</code> 文件，使用和二次分发前请以项目实际发布说明为准。

## Star History

<p align="center">
  <a href="https://star-history.com/#AnacondaKC/Aniu&Date">
    <img src="https://api.star-history.com/svg?repos=AnacondaKC/Aniu&type=Date" alt="Star History Chart" width="760" />
  </a>
</p>

<!-- LINK GROUP -->
