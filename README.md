<div align="center">

# Aniu

<img width="120" alt="Image" src="./icon.png" />

</div>

Aniu 是一个面向 A 股 / 港股场景的智能分析与模拟交易系统，提供以下能力：

- AI 分析任务执行与结果展示
- AI 聊天问答
- 账户总览、持仓、委托、交易信息展示
- 定时任务调度
- Docker 一键部署

当前项目采用前后端分离开发、单容器发布的方式：

- 前端：Vue 3 + Vite + Pinia
- 后端：FastAPI + SQLAlchemy + SQLite
- 发布：Docker 多阶段构建，单容器同时提供前端静态资源和后端 API

## 项目结构

```text
Aniu/
├── backend/
│   ├── app/
│   │   ├── api/
│   │   ├── core/
│   │   ├── data/
│   │   ├── db/
│   │   ├── schemas/
│   │   ├── services/
│   │   └── main.py
│   ├── tests/
│   ├── .env.example
│   └── requirements.txt
├── frontend/
│   ├── public/
│   ├── src/
│   ├── tests/
│   ├── package.json
│   └── vite.config.ts
├── .env.docker.example
├── .dockerignore
├── .gitignore
├── Dockerfile
├── docker-compose.yml
└── LICENSE
```

## 功能概览

- 总览页：展示账户、持仓、委托、交易信息和运行统计
- AI 分析页：查看分析任务列表、详情、接口调用、交易动作和输出结果
- AI 聊天页：与系统进行对话
- 定时设置页：维护自动任务
- 功能设置页：配置登录、LLM、MX 等相关参数

## 环境要求

### 本地开发

- Node.js 20+
- Python 3.12+ 或 3.13+

### Docker 部署

- Docker 26+

如果服务器支持 Compose v2，也可以直接使用 `docker compose`。

## 本地开发

### 1. 后端启动

进入 `backend/`：

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
cp .env.example .env
./.venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

默认后端地址：

```text
http://127.0.0.1:8000
```

健康检查：

```text
GET /health
```

### 2. 前端启动

进入 `frontend/`：

```bash
npm install
npm run dev
```

默认前端地址：

```text
http://127.0.0.1:3003
```

本地开发时，Vite 会把 `/api` 和 `/health` 代理到后端 `8000` 端口。

## 关键环境变量

### 后端本地环境变量

参考文件：`backend/.env.example`

关键项：

- `APP_LOGIN_USERNAME`：系统登录用户名（必须）
- `APP_LOGIN_PASSWORD`：系统登录密码（必须）

### Docker 部署环境变量

参考文件：`.env.docker.example`


## Docker 部署

### 方式一：直接拉取 GHCR 镜像并运行

1. 复制部署环境模板：

```bash
cp .env.docker.example .env.docker
```

2. 修改 `.env.docker` 中的实际配置。

3. 拉取镜像：

```bash
docker pull ghcr.io/anacondakc/aniu:latest
```

4. 启动容器：

```bash
docker run -d \
  --name aniu \
  -p 8000:8000 \
  --env-file .env.docker \
  -v aniu-data:/app/data \
  ghcr.io/anacondakc/aniu:latest
```

### 方式二：使用 `docker compose`

1. 复制部署环境模板：

```bash
cp .env.docker.example .env.docker
```

2. 修改 `.env.docker`。

如果你要固定到某个镜像版本，可以把 `ANIU_IMAGE_TAG` 改成例如 `sha-95cd1a4` 或后续发布的 `v1.0.0`。

3. 启动：

```bash
docker compose pull && docker compose up -d
```

4. 停止：

```bash
docker compose down
```

## Docker 运行说明

当前发布镜像具备以下特点：

- 前端在构建阶段打包后复制到 `/app/static`
- 后端通过 FastAPI 同时提供 API 和静态前端页面
- SQLite 数据库存放在 `/app/data/aniu.sqlite3`
- 容器内置健康检查：

```text
GET /health
```

- 容器暴露端口：`8000`

## 接口前缀

当前 API 前缀为：

```text
/api/aniu
```

例如：

- `POST /api/aniu/login`
- `GET /api/aniu/settings`
- `GET /api/aniu/runs`
- `GET /api/aniu/runtime-overview`

## 常用验证命令

### 前端构建

```bash
cd frontend
npm run build
```

### 后端测试

```bash
cd backend
./.venv/bin/pytest
```

### Docker 健康检查

```bash
curl http://127.0.0.1:8000/health
```

### 登录接口验证

```bash
curl -X POST http://127.0.0.1:8000/api/aniu/login \
  -H "Content-Type: application/json" \
  -d '{"username":"your-username","password":"your-password"}'
```

## 数据与持久化

- 默认数据库：`/app/data/aniu.sqlite3`
- `docker-compose.yml` 中已挂载命名卷：`aniu-data`

如果使用 `docker run`，请务必挂载数据卷或宿主机目录，否则容器重建后 SQLite 数据会丢失。

## 注意事项

### 1. CORS 配置

当前 `.env.docker.example` 中默认：

```text
CORS_ALLOW_ORIGINS=*
```

正式环境建议改成明确域名，例如：

```text
CORS_ALLOW_ORIGINS=https://your-domain.com
```

### 2. 交易日历缓存

镜像中已包含 `backend/app/data/trading_calendar.json`，可以降低首次启动时因为交易日历远程接口异常导致的风险。

## 镜像发布

仓库已包含 GitHub Actions 工作流 `.github/workflows/publish-image.yml`。

- 当你向 `main` 分支推送代码时，会自动构建并发布 `ghcr.io/anacondakc/aniu:latest`
- 同时会附带一个基于提交 SHA 的镜像标签，便于回滚和定位
- 当你推送形如 `v1.0.0` 的 Git tag 时，也会自动发布同名版本标签
- `docker-compose.yml` 默认会拉取 `ghcr.io/anacondakc/aniu:${ANIU_IMAGE_TAG:-latest}`

拉取示例：

```bash
docker pull ghcr.io/anacondakc/aniu:latest
```
