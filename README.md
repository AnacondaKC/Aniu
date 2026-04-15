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

### 前提条件
需要大家下载东方财富APP，首页搜索妙想Skills，立即领取。点击APP下方交易，点击上方模拟，领取20万元模拟资金。回到妙想Skills界面，下滑找到妙想模拟组合管理skill，绑定你的模拟组合。然后将API Key保存到程序设置界面填入即可。
注意妙想的相关技能使用有限额。

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

- `APP_LOGIN_PASSWORD`：系统登录密码（必须）

### Docker 部署环境变量

参考文件：`.env.docker.example`

Docker Compose 最小部署时，默认只需要关心：

- `APP_LOGIN_PASSWORD`：系统登录密码
- `ANIU_IMAGE_TAG`：镜像标签，默认 `latest`

其它模型与妙想相关配置可以在容器启动后，通过页面中的“功能设置”进行填写和保存，无需强制写入 Compose 环境变量。


## Docker 部署

### 方式一：直接拉取 GHCR 镜像并运行

1. 复制部署环境模板：

```bash
cp .env.docker.example .env.docker
```

2. 最少修改 `.env.docker` 中的登录配置：

```text
APP_LOGIN_PASSWORD=your-password
```

如果你希望固定镜像版本，可以同时修改 `ANIU_IMAGE_TAG`。

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
  -v "$(pwd)/data:/app/data" \
  ghcr.io/anacondakc/aniu:latest
```

5. 启动后访问 `http://<你的主机IP>:8000`，输入上一步设置的密码登录。

6. 首次进入后，到“功能设置”页面中填写：

- `OpenAI API Key`
- `OpenAI Base URL`
- `OpenAI Model`
- `妙想密钥`

保存后，AI 分析与妙想工具即可正常使用。

### 方式二：使用 `docker compose`

1. 复制部署环境模板：

```bash
cp .env.docker.example .env.docker
```

2. 最少修改 `.env.docker` 中的登录配置：

```text
APP_LOGIN_PASSWORD=your-password
```

如果你要固定到某个镜像版本，可以把 `ANIU_IMAGE_TAG` 改成例如 `sha-95cd1a4` 或后续发布的 `v1.0.0`。

3. 启动：

```bash
docker compose pull && docker compose up -d
```

4. 打开 `http://<你的主机IP>:8000`，输入 `.env.docker` 中设置的密码登录。

5. 首次进入后，在“功能设置”页面中补充：

- `OpenAI API Key`
- `OpenAI Base URL`
- `OpenAI Model`
- `妙想密钥`

保存后即可开始使用 AI 分析、定时任务与妙想工具。

6. 停止：

```bash
docker compose down
```

## Docker 运行说明

当前发布镜像具备以下特点：

- 前端在构建阶段打包后复制到 `/app/static`
- 后端通过 FastAPI 同时提供 API 和静态前端页面
- 默认 SQLite 数据库存放在 `/app/data/aniu.db`
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
  -d '{"password":"your-password"}'
```

## 数据与持久化

- 默认数据库：`/app/data/aniu.db`
- `docker-compose.yml` 默认挂载宿主机目录：`./data:/app/data`

如果使用 `docker run`，请务必挂载数据卷或宿主机目录，否则容器重建后 SQLite 数据会丢失。

## 注意事项

### 1. 首次配置说明

为了简化 Compose 部署流程，OpenAI 与妙想相关配置默认不要求写在 Compose 文件中。推荐流程是：

1. 先用最小配置启动容器
2. 登录页面
3. 在“功能设置”中填写并保存以下内容：

- `OpenAI API Key`
- `OpenAI Base URL`
- `OpenAI Model`
- `妙想密钥`

这样可以减少部署时需要维护的环境变量数量。

### 2. JWT Secret

`JWT_SECRET` 当前支持自动生成，因此不是最小部署的必填项。

- 如果你只关注快速启动，可以不显式设置
- 如果你希望容器重启或重建后仍保持登录态稳定，建议在 `.env.docker` 中手动设置固定值

### 3. CORS 配置

当前后端默认允许所有来源：

```text
CORS_ALLOW_ORIGINS=*
```

正式环境建议改成明确域名，例如：

```text
CORS_ALLOW_ORIGINS=https://your-domain.com
```

### 4. 交易日历缓存

镜像中已包含 `backend/app/data/trading_calendar.json`，可以降低首次启动时因为交易日历远程接口异常导致的风险。

## 镜像发布

仓库已包含 GitHub Actions 工作流 `.github/workflows/publish-image.yml`。

- 当你向 `main` 分支推送代码时，会自动构建并发布 `ghcr.io/anacondakc/aniu:latest`
- 同时会附带一个基于提交 SHA 的镜像标签，便于回滚和定位
- 当你推送形如 `v1.0.0` 的 Git tag 时，会自动发布 `v1.0.0` 和 `1.0.0` 两个版本镜像标签
- 推送版本 tag 后，会自动创建对应的 GitHub Release
- `docker-compose.yml` 默认会拉取 `ghcr.io/anacondakc/aniu:${ANIU_IMAGE_TAG:-latest}`

拉取示例：

```bash
docker pull ghcr.io/anacondakc/aniu:latest
```

### 版本发布

如果你要正式发布一个版本，推荐按下面步骤执行：

1. 确认要发布的提交已经推送到 `main`
2. 创建版本 tag，例如：

```bash
git tag v0.1.0
git push origin v0.1.0
```

3. GitHub Actions 会自动完成：

- 发布 `ghcr.io/anacondakc/aniu:v0.1.0`
- 发布 `ghcr.io/anacondakc/aniu:0.1.0`
- 创建对应的 GitHub Release

如果你要部署指定版本，可以把 `.env.docker` 中的 `ANIU_IMAGE_TAG` 改成 `v0.1.0`。
