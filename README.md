# 象棋弈台 —— Koyeb 版(xqs.696919.xyz serv00 版移植)

https://xq-koyeb-fan-1eab00ce.koyeb.app/ — Koyeb 免费实例上运行的象棋弈台完整服务端。
代码 = serv00 部署版(零依赖 Node, node:sqlite)**原样移植**,仅平台适配差异,功能 1:1(40+ REST API、大厅/房间 WS、快速匹配、Elo、VIP、支付、公告、管理后台、Pikafish 前端)。

## 目录结构

```
/                      ← 仓库根 = 应用根(Dockerfile COPY . .)
├─ Dockerfile          node:22-alpine, ENV XQ_PORT=3000, CMD node server.js
├─ package.json        零依赖(node:sqlite 内置), engines node>=22.5
├─ server.js           入口: 路由/静态/WS升级/301/竞速上报
├─ ws.js db.js rooms.js match.js lobby.js api.js util.js
├─ test/smoke-test.js  74 项端到端冒烟测试
└─ dist/               前端(index.html + Pikafish wasm/NNUE 分卷 + 管理后台)
```

## Koyeb 平台适配点(与 serv00 版的差异)

| 项 | serv00 | Koyeb(本仓库) |
|---|---|---|
| 端口 | 10949(devil 保留端口) | **3000**(服务定义 ports/routes,TCP 健康检查) |
| 读取顺序 | `XQ_PORT` | `XQ_PORT` → `PORT`(PaaS 注入)→ `10949` |
| 数据库 | `~/domains/<域名>/data/chess.db` **持久** | 容器层 `/app/data/chess.db` **非持久**:重新部署/缩容唤醒后清空 |
| 实例 | 常驻 + cron 保活 | free 实例 min 0:空闲睡眠,首请求冷启动 |
| HTTPS | Let's Encrypt(命令行签发) | Koyeb 域名自带证书 |

> **数据说明**:Koyeb 免费计划无持久卷,数据库随部署重置——本版本适合作演示/热备;
> 主站(持久数据)在 serv00:https://xqs.696919.xyz 。账号密码哈希算法与 CF Worker/serv00 完全一致,数据表结构相同,理论上可互导。

## 本地运行 / 测试

```bash
node server.js                    # 默认 10949
npm test                          # 74 项冒烟(需要 node>=22.5)
```

## 部署

推送到本仓库 main 分支即可——Koyeb 服务已绑定 GitHub(auto deploy on push),Dockerfile 构建后自动滚动发布。
