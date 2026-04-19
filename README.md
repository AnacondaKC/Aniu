<div align="center">

# Aniu——科技牛牛带你猛猛干A股

<img width="120" alt="Aniu icon" src="./frontend/public/aniu.ico" />

**面向 A 股 / 港股的智能分析与模拟交易系统**

[![Stars][stars-shield]][repo-link]
[![Forks][forks-shield]][repo-link]
[![Issues][issues-shield]][issues-link]
[![License][license-shield]][license-link]

</div>

<div align="center">
  <img src="./docs/banner.png" alt="Aniu Screenshot" width="800" />
</div>

---

### 核心特性

- **AI 分析** — 任务执行与结果可视化展示
- **AI 聊天** — 与系统进行自然语言对话
- **账户总览** — 持仓 / 委托 / 交易实时展示
- **定时调度** — 自动任务配置与执行
- **一键部署** — Docker 单容器发布，开箱即用

### 技术栈

- **前端** — Vue 3 + Vite + Pinia
- **后端** — FastAPI + SQLAlchemy + SQLite
- **发布** — Docker 多阶段构建，单容器同时提供前端资源与后端 API

---

### 前提条件

下载东方财富 APP，首页搜索「妙想 Skills」立即领取。点击 APP 下方交易 → 上方模拟，领取 20 万元模拟资金。回到妙想 Skills 界面，下滑找到「妙想模拟组合管理」skill，绑定模拟组合，将 API Key 保存到程序设置界面。

> 妙想相关技能使用有限额。

---

### 快速部署（Docker）

#### 1. 准备环境模板

```bash
cp .env.docker.example .env.docker
```

#### 2. 设置登录密码

编辑 `.env.docker`：

```text
APP_LOGIN_PASSWORD=your-password
```

#### 3. 启动服务

**方式一：docker compose**

```bash
docker compose pull && docker compose up -d
```

**方式二：docker run**

```bash
docker pull ghcr.io/anacondakc/aniu:latest

docker run -d \
  --name aniu \
  -p 8000:8000 \
  --env-file .env.docker \
  -v "$(pwd)/data:/app/data" \
  ghcr.io/anacondakc/aniu:latest
```

#### 4. 登录并配置

访问 `http://<主机IP>:8000`，使用密码登录后，在「功能设置」中填写：

- `OpenAI API Key`
- `OpenAI Base URL`
- `OpenAI Model`
- `妙想密钥`

保存后即可使用 AI 分析与妙想工具。

---

### 本地开发

#### 环境要求

- Node.js 20+
- Python 3.12 / 3.13+

#### 后端启动

```bash
cd backend
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
cp .env.example .env
./.venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

默认地址：`http://127.0.0.1:8000`

#### 前端启动

```bash
cd frontend
npm install
npm run dev
```

默认地址：`http://127.0.0.1:3003`

> Vite 开发时会自动将 `/api` 和 `/health` 代理到后端 `8000` 端口。

---

### 项目结构

```text
Aniu/
├── backend/              # FastAPI 后端
│   ├── app/
│   ├── tests/
│   └── requirements.txt
├── frontend/             # Vue 3 前端
│   ├── public/
│   └── src/
├── docs/                 # 文档与展示素材
├── Dockerfile
├── docker-compose.yml
└── .env.docker.example
```

---

### 接口说明

- API 前缀：`/api/aniu`
- 健康检查：`GET /health`

常用端点：

```text
POST /api/aniu/login
GET  /api/aniu/settings
GET  /api/aniu/runs
GET  /api/aniu/runtime-overview
```

---

### 配置说明

#### 关键环境变量

| 变量 | 说明 |
|------|------|
| `APP_LOGIN_PASSWORD` | 登录密码（必填） |
| `ANIU_IMAGE_TAG` | 镜像标签，默认 `latest` |
| `JWT_SECRET` | 未设置时自动生成，建议固定以保持登录态稳定 |
| `CORS_ALLOW_ORIGINS` | 默认 `*`，正式环境建议设为具体域名 |

> OpenAI 与妙想相关配置无需写入环境变量，推荐首次登录后在「功能设置」页面中保存，减少部署维护成本。

#### 数据持久化

- 默认数据库：`/app/data/aniu.sqlite3`
- 宿主机挂载：`./data:/app/data`
- 兼容旧版本 `aniu.db` 文件，自动识别并继续使用
- 镜像内置交易日历缓存 `backend/app/data/trading_calendar.json`，降低首次启动因远程接口异常导致的失败风险

> 使用 `docker run` 时请务必挂载数据卷，否则容器重建后数据丢失。

---

### 验证命令

```bash
# 前端构建
cd frontend && npm run build

# 后端测试
cd backend && ./.venv/bin/pytest

# 健康检查
curl http://127.0.0.1:8000/health

# 登录接口
curl -X POST http://127.0.0.1:8000/api/aniu/login \
  -H "Content-Type: application/json" \
  -d '{"password":"your-password"}'
```

---

### 镜像发布

仓库包含 GitHub Actions 工作流 `.github/workflows/publish-image.yml`：

- 推送 `main` 分支 → 发布 `ghcr.io/anacondakc/aniu:latest` 及 SHA 标签
- 推送 `v1.0.0` 格式 tag → 发布对应版本镜像并自动创建 Release
- `docker-compose.yml` 默认拉取 `ghcr.io/anacondakc/aniu:${ANIU_IMAGE_TAG:-latest}`

---

### License

[MIT](./LICENSE)

---

### 致谢

本项目开发使用了公益站，感谢 [LINUX DO](https://linux.do/t/topic/1987329) 社区的支持。

---

### Star History

[![Star History Chart](https://api.star-history.com/svg?repos=AnacondaKC/Aniu&type=Date)](https://www.star-history.com/#AnacondaKC/Aniu&Date)

<!-- LINK GROUP -->

[repo-link]: https://github.com/AnacondaKC/Aniu
[issues-link]: https://github.com/AnacondaKC/Aniu/issues
[license-link]: ./LICENSE
[stars-shield]: https://img.shields.io/github/stars/AnacondaKC/Aniu?color=ffcb47&labelColor=black&style=flat-square
[forks-shield]: https://img.shields.io/github/forks/AnacondaKC/Aniu?color=8ae8ff&labelColor=black&style=flat-square
[issues-shield]: https://img.shields.io/github/issues/AnacondaKC/Aniu?color=ff80eb&labelColor=black&style=flat-square
[license-shield]: https://img.shields.io/github/license/AnacondaKC/Aniu?color=c4f042&labelColor=black&style=flat-square
