/* ============================================================
 * 象棋弈台 serv00 服务端 - server.js（入口）
 * 1:1 对齐 Cloudflare Worker cf-chess2 的入口行为:
 *   - http→https 301（x-forwarded-proto 判定, 与 Worker 一致）
 *   - POST /api/race-report（页面竞速匿名上报）
 *   - /api/*  → api.js（40+ 端点）
 *   - /admin  → admin.html（no-store）
 *   - /ws     → 大厅 WS；/ws?roomId=x → 房间 WS
 *   - /       → dist/index.html（no-store）；.html 全部 no-store
 *   - 其余    → dist 静态文件（ETag/304，与 CF Assets 行为对齐）
 * 运行: node server.js   端口默认 10949（可用 XQ_PORT 覆盖）
 * ============================================================ */
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const { createD1 } = require('./db');
const { MatchQueue } = require('./match');
const { attachRoomWebSocket, startRoomSweep } = require('./rooms');
const { handleLobbyWebSocket } = require('./lobby');
const { handleApiRequest } = require('./api');

/* 进程级容错（对齐 Worker 事件隔离语义: 单个事件/Promise 异常只记日志, 不拖垮整个进程） */
process.on('uncaughtException', (err) => {
  console.error('[uncaughtException]', (err && (err.stack || err.message)) || err);
});
process.on('unhandledRejection', (err) => {
  console.error('[unhandledRejection]', (err && (err.stack || err.message)) || err);
});

/* ===== 配置 ===== */
const PORT = parseInt(process.env.XQ_PORT || process.env.PORT || '10949', 10);
const HOST = process.env.XQ_HOST || '0.0.0.0';
const ROOT = __dirname;
const DIST = process.env.XQ_DIST || path.join(ROOT, 'dist');
const DB_FILE = process.env.XQ_DB || path.join(ROOT, 'data', 'chess.db');

/* 数据目录不存在则创建(Koyeb/Docker 等全新容器环境) */
try { fs.mkdirSync(path.dirname(DB_FILE), { recursive: true }); } catch (e) {}

/* ===== MIME（与 CF Assets 行为对齐） ===== */
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.htm': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.mjs': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.txt': 'text/plain; charset=utf-8',
  '.wasm': 'application/wasm',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.mp3': 'audio/mpeg',
  '.part0': 'application/octet-stream',
  '.part1': 'application/octet-stream',
  '.part2': 'application/octet-stream',
  '.nnue': 'application/octet-stream'
};
const NO_STORE = 'no-store, no-cache, must-revalidate';

/* ===== 初始化 ===== */
const db = createD1(DB_FILE);
console.log('[xq] SQLite 就绪:', DB_FILE, '后端:', db.backend);

/* 竞速上报表（与 Worker D1 结构一致，启动时确保存在） */
try {
  db.exec("CREATE TABLE IF NOT EXISTS race_report (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, asn INTEGER, city TEXT, entry TEXT, t_self INTEGER, t_alt INTEGER, t_p1 INTEGER, t_p2 INTEGER, t_p3 INTEGER, t_p4 INTEGER, t_p5 INTEGER, t_p6 INTEGER, t_p7 INTEGER, t_p8 INTEGER, t_p9 INTEGER)");
} catch (e) {}


const env = {
  CHESS_DB: db,
  MATCH_QUEUE: new MatchQueue(db),
  onlineCount: 0,
  ip: ''
};

startRoomSweep();

/* ===== 请求包装（对齐 Worker Request 形状） ===== */
function makeRequest(req) {
  const host = req.headers.host || 'localhost';
  const fullUrl = 'http://' + host + req.url;
  let _raw = null;
  let _json;
  const wrapper = {
    url: fullUrl,
    method: req.method,
    headers: {
      get(name) {
        const v = req.headers[String(name).toLowerCase()];
        return v === undefined ? null : (Array.isArray(v) ? v[0] : v);
      }
    },
    async text() {
      if (_raw === null) {
        _raw = await readBody(req);
      }
      return _raw;
    },
    async json() {
      if (_json !== undefined) return _json;
      const t = await wrapper.text();
      try {
        _json = JSON.parse(t);
      } catch (e) {
        _json = null;
      }
      return _json;
    }
  };
  return wrapper;
}

function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let done = false;
    req.on('data', (c) => {
      if (done) return;
      size += c.length;
      if (size > (limit || 4 * 1024 * 1024)) {
        done = true;
        resolve('');
        req.removeAllListeners('data');
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => { if (!done) { done = true; resolve(Buffer.concat(chunks).toString('utf8')); } });
    req.on('error', (e) => { if (!done) { done = true; resolve(''); } });
  });
}

/* ===== 静态文件 ===== */
function etagOf(stat, p) {
  return '"' + stat.size.toString(16) + '-' + Number(stat.mtimeMs).toString(16) + '-' + crypto.createHash('md5').update(p).digest('hex').slice(0, 8) + '"';
}

function serveStatic(req, res, pathname) {
  let p;
  try {
    p = decodeURIComponent(pathname);
  } catch (e) {
    p = pathname;
  }
  if (p === '/') p = '/index.html';
  const file = path.normalize(path.join(DIST, p));
  if (!file.startsWith(DIST)) {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not found');
    return;
  }
  // 对齐 .assetsignore: index_bak.html 不对外
  if (path.basename(file) === 'index_bak.html' && path.dirname(file) === DIST) {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not found');
    return;
  }
  let stat;
  try {
    stat = fs.statSync(file);
  } catch (e) {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not found');
    return;
  }
  if (!stat.isFile()) {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not found');
    return;
  }
  const ext = path.extname(file).toLowerCase();
  const isHtml = ext === '.html' || ext === '.htm';
  const etag = etagOf(stat, file);
  const inm = req.headers['if-none-match'];
  const baseHeaders = {
    'Content-Type': MIME[ext] || 'application/octet-stream',
    'ETag': etag,
    'Cache-Control': isHtml ? NO_STORE : 'public, max-age=300'
  };
  if (isHtml) {
    baseHeaders['Pragma'] = 'no-cache';
    baseHeaders['Expires'] = '0';
  }
  if (inm && inm === etag) {
    res.writeHead(304, { 'ETag': etag, 'Cache-Control': baseHeaders['Cache-Control'] });
    res.end();
    return;
  }
  res.writeHead(200, baseHeaders);
  if (req.method === 'HEAD') {
    res.end();
    return;
  }
  const stream = fs.createReadStream(file);
  stream.on('error', () => {
    try { res.destroy(); } catch (e) {}
  });
  stream.pipe(res);
}

/* ===== 响应 Worker 形状的结果 ===== */
function sendResult(res, r) {
  if (!r) {
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    res.end('Internal error');
    return;
  }
  const headers = r.headers || {};
  res.writeHead(r.status || 200, headers);
  res.end(r.body == null ? '' : r.body);
}

/* ===== HTTP 服务 ===== */
const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://' + (req.headers.host || 'localhost'));
    const pathname = url.pathname;

    // http→https 301（与 Worker 一致: 仅在明确 x-forwarded-proto=http 时重定向）
    if ((req.headers['x-forwarded-proto'] || 'https') === 'http') {
      const httpsUrl = 'https://' + (req.headers.host || '') + req.url;
      res.writeHead(301, { Location: httpsUrl });
      res.end();
      return;
    }

    // 页面竞速匿名上报（Worker 在 /api 路由之前处理）
    if (pathname === '/api/race-report' && req.method === 'POST') {
      try {
        const b = JSON.parse(await readBody(req) || '{}');
        await db.prepare(
          "INSERT INTO race_report (ts,asn,city,entry,t_self,t_alt,t_p1,t_p2,t_p3,t_p4,t_p5,t_p6,t_p7,t_p8,t_p9) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ).bind(Date.now(), 0, "", String(b.entry || "").slice(0, 64), b.t_self | 0, b.t_alt | 0, b.t_p1 | 0, b.t_p2 | 0, b.t_p3 | 0, b.t_p4 | 0, b.t_p5 | 0, b.t_p6 | 0, b.t_p7 | 0, b.t_p8 | 0, b.t_p9 | 0).run();
      } catch (e) {
      }
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('ok');
      return;
    }

    if (pathname.startsWith('/api/')) {
      env.ip = (req.headers['cf-connecting-ip'] || (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || 'x');
      const wrapped = makeRequest(req);
      const result = await handleApiRequest(wrapped, env);
      sendResult(res, result);
      return;
    }

    if (pathname === '/admin' || pathname === '/admin/') {
      const adminFile = path.join(DIST, 'admin.html');
      let body;
      try {
        body = fs.readFileSync(adminFile);
      } catch (e) {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('Not found');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': NO_STORE });
      res.end(body);
      return;
    }

    // 静态资源（含 / 与 .html 的 no-store 行为）
    serveStatic(req, res, pathname);
  } catch (e) {
    console.error('[xq] HTTP error:', e && e.message || e);
    try {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Internal error');
    } catch (e2) {}
  }
});

/* ===== WebSocket 升级 ===== */
const wsLib = require('./ws');
server.on('upgrade', async (req, socket, head) => {
  try {
    const url = new URL(req.url, 'http://' + (req.headers.host || 'localhost'));
    if (url.pathname !== '/ws') {
      socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
      socket.destroy();
      return;
    }
    const conn = wsLib.accept(req, socket);
    if (!conn) return;
    if (head && head.length) {
      conn._onData(head);
    }
    const roomId = url.searchParams.get('roomId');
    if (roomId) {
      await attachRoomWebSocket(roomId, conn, db);
    } else {
      await handleLobbyWebSocket(conn, env);
    }
  } catch (e) {
    console.error('[xq] WS upgrade error:', e && e.message || e);
    try { socket.destroy(); } catch (e2) {}
  }
});

server.listen(PORT, HOST, () => {
  console.log('[xq] 象棋弈台 serv00 服务端已启动: http://' + HOST + ':' + PORT + '  (dist=' + DIST + ')');
});

/* ===== 优雅退出 ===== */
function shutdown() {
  console.log('[xq] 收到退出信号，正在关闭...');
  try { server.close(); } catch (e) {}
  try { db.close(); } catch (e) {}
  setTimeout(() => process.exit(0), 500);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
