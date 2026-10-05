# 象棋弈台 xq-koyeb —— 零依赖 Node 服务端(node:sqlite, Node >= 22.5)
FROM node:22-alpine

WORKDIR /app

# 整体复制(代码 + dist 前端 + test);无 npm 依赖,无需 npm install
COPY . .

# Koyeb 容器必须监听定义端口(3000)
ENV XQ_PORT=3000
ENV NODE_ENV=production

EXPOSE 3000

CMD ["node", "server.js"]
