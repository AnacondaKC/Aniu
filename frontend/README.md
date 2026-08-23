# Aniu 前端

Aniu 交易智能体工作台的 React、TypeScript 与 Vite 前端。

## 开发

```bash
npm install --include=dev
npm run dev
```

本地开发服务器会将 `/api` 与 `/health` 代理至后端；可通过 `VITE_BACKEND_PROXY` 覆盖目标地址。

## 质量检查

```bash
npm run lint          # ESLint + TypeScript 类型规则
npm run format:check  # Prettier 与 Tailwind 类名排序
npm run knip          # 未引用文件、导出和依赖检查
npm test              # Vitest
npm run build         # TypeScript 编译和生产构建
```

API 类型由后端 OpenAPI 文档生成：

```bash
npm run api:generate
npm run api:check
```
