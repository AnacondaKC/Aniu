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
install.sh         # 公共源码/Docker 安装入口
.aniu/            # 本机运行数据、依赖、缓存和参考资料，不提交到 Git
├── docs/          # 本地参考源码、接口资料和品牌图片
│   └── references/ # 外部参考项目和测试资料
└── local/         # 虚拟环境、依赖、缓存和构建产物
```

## 一键安装

在 GitHub 源码目录中执行，默认使用 Docker 构建并启动：

```bash
./install.sh
```

使用 GitHub Actions 生成的预构建镜像时：

```bash
./install.sh docker --image ghcr.io/OWNER/IMAGE:latest
```

不使用 Docker、直接安装源码和前端依赖时：

```bash
./install.sh source
./install.sh source --start
./install.sh source --systemd
```

清理可重建的缓存、日志、前端依赖和参考项目构建产物：

```bash
./install.sh clean
```

Docker 模式会将本地环境文件创建为 `.aniu/local/.env`，不会提交密钥。源码模式使用 `requirements.lock` 安装 Python 依赖，并执行前端生产构建；需要长期后台运行时使用 `./install.sh source --systemd`。`clean` 不会删除数据库、密钥、`.env`、虚拟环境、参考源码或私有资料，但会删除 `frontend/node_modules`；再次进行前端开发时执行 `npm --prefix frontend ci --include=dev`。Docker 模式不需要 systemd。

## 本地开发

```bash
python -m venv .aniu/local/.venv
.aniu/local/.venv/bin/python -m pip install --require-hashes -r requirements.lock
.aniu/local/.venv/bin/python -m pip install -e ".[dev]"
.aniu/local/.venv/bin/python .aniu/dev.py
```

`requirements.lock` 锁定 Docker 和 CI 使用的 Python 运行时依赖及其 hash；开发依赖仍通过项目的 `dev` extra 安装。

默认地址：

- 前端：<http://127.0.0.1:5173>
- 后端：<http://127.0.0.1:8000>
- Readiness：<http://127.0.0.1:8000/health/ready>

启用局域网访问和调度器：

```bash
.aniu/local/.venv/bin/python .aniu/dev.py --lan --allowed-host your-lan-host --enable-scheduler
```

单独启动后端：

```bash
.aniu/local/.venv/bin/python -m backend.serve
```

## 本地运行数据

默认情况下，应用数据库、加密密钥和轮转日志都写入仓库根目录的 `.aniu/`。开发依赖、构建产物和缓存集中放在 `.aniu/local/`；参考源码、接口资料、参考项目和品牌图片统一放在 `.aniu/docs/`（外部参考项目位于 `.aniu/docs/references/`）。这些内容都不会上传到 GitHub。

Python 开发环境固定放在 `.aniu/local/.venv`，MyPy、Pytest、Ruff、Python 字节码、Vite、TypeScript、npm 和 pip 的 Aniu 专属缓存也固定放在 `.aniu/local/`，不会再在源码树生成这些缓存；本地参考源码、参考项目和图片位于 `.aniu/docs/`。`.aniu/dev.py` 是仅供本机开发使用的启动器，不进入 GitHub；新克隆的公共代码可直接使用 `backend.serve` 和前端 npm 命令，生产环境使用 `backend.serve` 或 Docker。前端依赖仍放在被 Git 忽略的 `frontend/node_modules/`，不搬入 `.aniu`，以避免 Node 工具解析异常；根目录不需要 `node_modules`。`.git` 是 Git 的版本库元数据，必须留在项目根目录，但也不会上传为项目文件。

本地 Docker Compose 配置保存在 `.aniu/local/.env`。使用 Compose 时通过 `--env-file .aniu/local/.env` 读取它；直接运行 Python 时，`.env` 不会自动注入进程环境。

需要把运行数据完全放到仓库外时，设置 `ANIU_DATA_DIR` 或传入 `--data-dir`：

```bash
ANIU_DATA_DIR=$HOME/.local/share/aniu .aniu/local/.venv/bin/python .aniu/dev.py
.aniu/local/.venv/bin/python -m backend.serve --data-dir $HOME/.local/share/aniu
```

从旧版 `data/` 布局升级时，先停止当前工作区启动的服务，再迁移已有状态：

```bash
mv data .aniu
```

Docker 继续使用命名 volume 中的 `/app/data`；源码 systemd 模式使用 `/var/lib/aniu`，两者都不会把运行数据写入源码目录。

## Docker 部署

镜像使用多阶段构建：先在 Node 阶段构建前端，再由 Python 镜像运行后端并托管 `frontend/dist`。Node/Python 基础镜像按 digest 固定，Python 运行时依赖通过带 hash 的 `requirements.lock` 安装。数据库、加密密钥和日志保存在 Docker volume `aniu-data` 中。

```bash
docker compose --env-file .aniu/local/.env build
docker compose --env-file .aniu/local/.env up -d
docker compose --env-file .aniu/local/.env ps
curl http://127.0.0.1:8000/health/ready
```

通过局域网或域名访问时，必须把请求使用的 Host 加入允许列表：

```bash
ANIU_ALLOWED_HOSTS=localhost,127.0.0.1,your-lan-host docker compose --env-file .aniu/local/.env up -d
```

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

项目使用可重建的 SQLite 开发数据库，不维护旧 Schema 迁移。重建前先停止服务：

```bash
sudo systemctl stop aniu.service
rm -f .aniu/aniu.sqlite3 .aniu/aniu.sqlite3-wal .aniu/aniu.sqlite3-shm
sudo systemctl start aniu.service
```

重建后确认已删除的 Skill 表没有被遗留：

```bash
sqlite3 .aniu/aniu.sqlite3 \
  "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('skills','skill_stage_bindings');"
```

上述命令应无输出。

不要删除 `.aniu/.aniu-secret-key`，否则现有加密 secret 将无法解密。需要彻底清空 secret 时，应同时删除数据库和密钥，再重新启动服务。

## systemd

源码安装并作为 Linux 后台服务长期运行时，直接让安装脚本生成并启用服务单元：

```bash
ANIU_SYSTEMD_ALLOWED_HOST=example.com ./install.sh source --systemd
```

脚本会生成 `/etc/systemd/system/aniu.service`，使用 `.aniu/local/.venv` 启动后端，将数据写入 `/var/lib/aniu`，并执行 `daemon-reload` 和 `enable --now`。请以实际部署用户运行脚本，脚本会在需要时调用 `sudo`。可通过 `ANIU_SYSTEMD_USER`、`ANIU_SYSTEMD_PORT` 和 `ANIU_SYSTEMD_CORS_ORIGINS` 调整服务配置。脚本需要 `systemd` 和 `sudo`；Docker 安装不使用这条路径。
