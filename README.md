# Aniubot

Aniubot 是一个面向股票交易工作流的本地交易智能体系统，前端提供工作台，后端负责认证、设置、运行编排、持久化、调度和 Agent/LLM 集成。

## 项目结构

```text
backend/
├── api/          # HTTP、SSE 和 OpenAPI 适配
├── business/     # 按功能组织的业务用例与模型
├── infra/        # 数据库、仓库、调度器、Worker 和外部集成
├── bootstrap/    # 应用组装、生命周期和运行时配置
├── agent/        # 独立 Agent 核心
├── llm/          # 独立 LLM 客户端与协议
└── stock_api/    # 股票数据接口适配
frontend/         # React、TypeScript、Vite 前端
scripts/          # 仓库开发脚本
install.sh         # 一键源码安装并启动入口
.aniu/            # 本机运行数据、依赖、缓存和参考资料，不提交到 Git
├── docs/          # 本地参考源码、接口资料和品牌图片
│   └── references/ # 外部参考项目和测试资料
└── local/         # 虚拟环境、依赖、缓存和构建产物
```

## 一键安装

在 GitHub 源码目录中执行：

```bash
./install.sh
```

脚本会检查 Python 3.12+ 和 npm，创建本地虚拟环境，按 `requirements.lock` 安装后端依赖，安装并构建前端，然后以前台方式启动应用。默认地址为 <http://127.0.0.1:8000>；按 `Ctrl+C` 停止服务。

## 本地开发

```bash
python -m venv .aniu/local/.venv
.aniu/local/.venv/bin/python -m pip install --require-hashes -r requirements.lock
.aniu/local/.venv/bin/python -m pip install -e ".[dev]"
.aniu/local/.venv/bin/python .aniu/dev.py
```

`requirements.lock` 锁定 Docker 和 CI 使用的 Python 运行时依赖及其 hash；开发依赖仍通过项目的 `dev` extra 安装。

默认仅监听本机回环地址：

- 前端：<http://127.0.0.1:5173>
- 后端：<http://127.0.0.1:8000>
- Readiness：<http://127.0.0.1:8000/health/ready>

需要局域网开发时显式启用，并提供精确的 Host 白名单：

```bash
.aniu/local/.venv/bin/python .aniu/dev.py --lan --allowed-host 192.168.1.20
```

独立后端同样使用 `--lan` 显式启用。Host 白名单只接受精确 IP 或 DNS 名称，不接受 `*`。LAN 上的 Token 登录需要通过 HTTPS 反向代理或 SSH 隧道保护；不要直接在不受信任的 HTTP 网络上传输 Token 或会话。

登录只需要至少 8 个字符的访问 Token。部署时可在 `.aniu/local/.env` 中设置 `ANIU_AUTH_TOKEN`；未设置时，首次从本机登录页输入的 Token 会以哈希形式保存，之后继续使用该 Token 登录。首次设置 Token 始终限本机回环地址，局域网部署请预先设置 `ANIU_AUTH_TOKEN`。不要把 Token 放入 `VITE_*` 变量、前端构建产物或 URL。

启用调度器：

```bash
.aniu/local/.venv/bin/python .aniu/dev.py --enable-scheduler
```

单独启动后端：

```bash
.aniu/local/.venv/bin/python -m backend.serve
```

## 本地运行数据

默认情况下，应用数据库、加密密钥和轮转日志都写入仓库根目录的 `.aniu/`。开发依赖、构建产物和缓存集中放在 `.aniu/local/`；参考源码、接口资料、参考项目和品牌图片统一放在 `.aniu/docs/`（外部参考项目位于 `.aniu/docs/references/`）。这些内容都不会上传到 GitHub。

Python 开发环境固定放在 `.aniu/local/.venv`，MyPy、Pytest、Ruff、Python 字节码、Vite、TypeScript、npm 和 pip 的 Aniu 专属缓存也固定放在 `.aniu/local/`，不会再在源码树生成这些缓存；本地参考源码、参考项目和图片位于 `.aniu/docs/`。`.aniu/dev.py` 是仅供本机开发使用的启动器，不进入 GitHub；新克隆的公共代码可直接使用 `backend.serve` 和前端 npm 命令，生产环境使用 `backend.serve` 或 Docker。前端依赖仍放在被 Git 忽略的 `frontend/node_modules/`，不搬入 `.aniu`，以避免 Node 工具解析异常；根目录不需要 `node_modules`。`.git` 是 Git 的版本库元数据，必须留在项目根目录，但也不会上传为项目文件。

本地 Docker Compose 配置保存在 `.aniu/local/.env`。Compose 通过 `--env-file .aniu/local/.env` 读取它；systemd 安装生成的 unit 也以权限为 `0600` 的 `EnvironmentFile` 读取它。直接运行 Python 时，`.env` 不会自动注入进程环境。

需要把运行数据完全放到仓库外时，设置 `ANIU_DATA_DIR` 或传入 `--data-dir`：

```bash
ANIU_DATA_DIR=$HOME/.local/share/aniu .aniu/local/.venv/bin/python .aniu/dev.py
.aniu/local/.venv/bin/python -m backend.serve --data-dir $HOME/.local/share/aniu
```

从旧版 `data/` 布局升级时，先停止当前工作区启动的服务，再迁移已有状态：

```bash
mv data .aniu
```

Docker 继续使用命名 volume 中的 `/app/data`；源码一键运行默认使用仓库根目录的 `.aniu/`。

## Docker 部署

镜像使用多阶段构建：先在 Node 阶段构建前端，再由 Python 镜像运行后端并托管 `frontend/dist`。Node/Python 基础镜像按 digest 固定，Python 运行时依赖通过带 hash 的 `requirements.lock` 安装。数据库、加密密钥和日志保存在 Docker volume `aniu-data` 中。

```bash
docker compose --env-file .aniu/local/.env build
docker compose --env-file .aniu/local/.env up -d
docker compose --env-file .aniu/local/.env ps
curl http://127.0.0.1:8000/health/ready
```

Compose 默认只将端口发布到 `127.0.0.1`。需要局域网或反向代理访问时，必须显式修改绑定地址、启用 LAN 模式并配置实际访问的精确 Host；独立前端还需配置对应的 `ANIU_CORS_ORIGINS`。不支持 `*` 通配符：

```bash
ANIU_BIND_ADDRESS=0.0.0.0 ANIU_LAN=1 \
ANIU_ALLOWED_HOSTS=localhost,127.0.0.1,192.168.1.20,aniu.example.internal \
  docker compose --env-file .aniu/local/.env up -d
```

将端口暴露到 LAN 时，请在服务前使用 HTTPS 反向代理或通过 SSH 隧道访问；不要直接暴露 HTTP 登录端点。

容器默认启用调度器，并以单实例运行。当前 SQLite 和进程内调度器不适合直接水平扩容；需要多副本时应先迁移到共享数据库并保证只有一个调度器实例。

Docker volume 中保存了数据库和加密密钥。删除 volume 会同时清空配置、密钥和运行历史：

```bash
docker compose --env-file .aniu/local/.env down -v
docker compose --env-file .aniu/local/.env up -d
```

仅更新镜像而保留数据时，使用 `docker compose --env-file .aniu/local/.env build` 后再次执行 `docker compose --env-file .aniu/local/.env up -d`，不要加 `-v`。

## 质量检查

```bash
.aniu/local/.venv/bin/python -m ruff check backend scripts
.aniu/local/.venv/bin/python -m mypy backend
.aniu/local/.venv/bin/python -m pytest backend/tests -q
npm --prefix frontend test -- --run
npm --prefix frontend run lint -- --max-warnings=0
npm --prefix frontend run build
```

## OpenAPI

后端路由变更后重新生成前端契约：

```bash
npm --prefix frontend run api:generate
npm --prefix frontend run api:check
```

生成文件是仓库契约的一部分，应与代码一起提交：

- `frontend/openapi.json`
- `frontend/src/generated/api-schema.ts`

## 重建本地数据库

项目使用可重建的 SQLite 开发数据库，不维护旧 Schema 迁移。重建前先按 `Ctrl+C` 停止一键启动的服务：

```bash
rm -f .aniu/aniu.sqlite3 .aniu/aniu.sqlite3-wal .aniu/aniu.sqlite3-shm
```

然后重新执行 `./install.sh`。

重建后确认已删除的 Skill 表没有被遗留：

```bash
sqlite3 .aniu/aniu.sqlite3 \
  "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('skills','skill_stage_bindings');"
```

上述命令应无输出。

不要删除 `.aniu/.aniu-secret-key`，否则现有加密 secret 将无法解密。需要彻底清空 secret 时，应同时删除数据库和密钥，再重新启动服务。
