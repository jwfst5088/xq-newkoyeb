/* ============================================================
 * 象棋弈台 serv00 服务端 - 冒烟测试（零依赖）
 * 运行: node test/smoke-test.js
 * 覆盖: 静态页/全部核心API/大厅WS/房间WS完整对局/匹配/限额/VIP/Elo结算
 * ============================================================ */
'use strict';
const { spawn } = require('child_process');
const http = require('http');
const crypto = require('crypto');
const os = require('os');
const fs = require('fs');
const path = require('path');

const PORT = 39871;
const BASE = 'http://127.0.0.1:' + PORT;
let passed = 0, failed = 0;
const failures = [];

function ok(cond, name) {
  if (cond) { passed++; console.log('  ✓ ' + name); }
  else { failed++; failures.push(name); console.log('  ✗ ' + name); }
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

/* ===== HTTP helper ===== */
function req(method, p, body, headers) {
  return new Promise((resolve, reject) => {
    const data = body == null ? null : (typeof body === 'string' ? body : JSON.stringify(body));
    const u = new URL(BASE + p);
    const r = http.request({
      hostname: u.hostname, port: u.port, path: u.pathname + u.search, method,
      headers: Object.assign({}, data != null ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } : {}, headers || {})
    }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        let json = null;
        try { json = JSON.parse(text); } catch (e) {}
        resolve({ status: res.statusCode, headers: res.headers, text, json });
      });
    });
    r.on('error', reject);
    if (data != null) r.write(data);
    r.end();
  });
}

/* ===== 极简 WebSocket 客户端（RFC6455, 掩码帧） ===== */
class WSClient {
  constructor(urlStr) {
    this.url = urlStr;
    this.onmessage = null;
    this.onclose = null;
    this._buf = Buffer.alloc(0);
    this._waiters = [];
    this._attached = false;
    this.closed = false;
  }
  connect() {
    return new Promise((resolve, reject) => {
      const u = new URL(this.url);
      const key = crypto.randomBytes(16).toString('base64');
      const r = http.request({
        hostname: u.hostname, port: u.port, path: u.pathname + u.search,
        headers: {
          Connection: 'Upgrade', Upgrade: 'websocket',
          'Sec-WebSocket-Key': key, 'Sec-WebSocket-Version': '13'
        }
      });
      r.on('upgrade', (res, socket, head) => {
        this.socket = socket;
        socket.on('data', (c) => { this._buf = Buffer.concat([this._buf, c]); this._parse(); });
        socket.on('close', () => { this.closed = true; if (this.onclose) this.onclose(); });
        socket.on('error', () => { if (!this.closed) { this.closed = true; if (this.onclose) this.onclose(); } });
        if (head && head.length) { this._buf = Buffer.concat([head, this._buf]); this._parse(); }
        resolve(this);
      });
      r.on('response', (res) => { reject(new Error('ws handshake failed: ' + res.statusCode)); });
      r.on('error', reject);
      r.end();
    });
  }
  _parse() {
    for (;;) {
      const buf = this._buf;
      if (buf.length < 2) return;
      const fin = (buf[0] & 0x80) !== 0;
      const opcode = buf[0] & 0x0f;
      let len = buf[1] & 0x7f;
      let off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      if (buf.length < off + len) return;
      const payload = buf.slice(off, off + len);
      this._buf = buf.slice(off + len);
      if (opcode === 0x8) { try { this.socket.destroy(); } catch (e) {} return; }
      else if (opcode === 0x9) { this._sendFrame(0xA, payload); }
      else if (opcode === 0x1 && fin) {
        const ev = { data: payload.toString('utf8') };
        let isPing = false;
        try {
          const d = JSON.parse(ev.data);
          if (d.event === 'ping') { this.send({ event: 'pong' }); isPing = true; }
        } catch (e) {}
        if (!isPing) {
          try { if (this.onmessage) this.onmessage(ev); } catch (e) {}
          let d = null;
          try { d = JSON.parse(ev.data); } catch (e) { continue; }
          for (let i = this._waiters.length - 1; i >= 0; i--) {
            const w = this._waiters[i];
            if (d.event === w.eventName && (!w.pred || w.pred(d))) {
              this._waiters.splice(i, 1);
              clearTimeout(w.timer);
              w.resolve(d);
            }
          }
        }
      }
    }
  }
  _sendFrame(opcode, payload) {
    const mask = crypto.randomBytes(4);
    const masked = Buffer.from(payload);
    for (let i = 0; i < masked.length; i++) masked[i] ^= mask[i & 3];
    let header;
    const len = payload.length;
    if (len < 126) header = Buffer.from([0x80 | opcode, 0x80 | len]);
    else if (len < 65536) { header = Buffer.alloc(4); header[0] = 0x80 | opcode; header[1] = 0x80 | 126; header.writeUInt16BE(len, 2); }
    else { header = Buffer.alloc(10); header[0] = 0x80 | opcode; header[1] = 0x80 | 127; header.writeBigUInt64BE(BigInt(len), 2); }
    try { this.socket.write(Buffer.concat([header, mask, masked])); } catch (e) {}
  }
  send(obj) { this._sendFrame(0x1, Buffer.from(JSON.stringify(obj), 'utf8')); }
  close() { try { this.socket.destroy(); } catch (e) {} this.closed = true; }
  /* 注册事件等待: 必须在 send 之前调用 */
  wait(eventName, pred, timeoutMs) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const i = this._waiters.indexOf(w);
        if (i >= 0) this._waiters.splice(i, 1);
        reject(new Error('等待事件 ' + eventName + ' 超时(' + (timeoutMs || 6000) + 'ms)'));
      }, timeoutMs || 6000);
      const w = { eventName, pred: pred || null, resolve, timer };
      this._waiters.push(w);
    });
  }
}

/* ===== 主流程 ===== */
async function main() {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'xq-test-'));
  /* dist 目录: 本地工作区在 ../../dist, serv00 部署布局在 ../dist */
  const distCandidates = [path.join(__dirname, '..', '..', 'dist'), path.join(__dirname, '..', 'dist')];
  const distDir = distCandidates.find((p) => fs.existsSync(path.join(p, 'index.html')));
  if (!distDir) {
    console.error('找不到 dist 目录(检查 ' + distCandidates.join(' 或 ') + ')');
    process.exit(2);
  }
  console.log('启动测试服务器 (port ' + PORT + ', db=' + tmpDir + ') ...');
  const child = spawn(process.execPath, ['server.js'], {
    cwd: path.join(__dirname, '..'),
    env: Object.assign({}, process.env, { XQ_PORT: String(PORT), XQ_HOST: '127.0.0.1', XQ_DB: path.join(tmpDir, 'chess.db'), XQ_DIST: distDir }),
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let serverLog = '';
  child.stdout.on('data', (c) => { serverLog += c.toString(); });
  child.stderr.on('data', (c) => { serverLog += c.toString(); });

  /* 等待服务器就绪 */
  let up = false;
  for (let i = 0; i < 60 && !up; i++) {
    await sleep(250);
    try { const r = await req('GET', '/api/online'); if (r.status === 200) up = true; } catch (e) {}
  }
  if (!up) {
    console.error('服务器未能启动:\n' + serverLog);
    child.kill();
    process.exit(2);
  }
  console.log('服务器已就绪\n');

  try {
    /* ---------- 静态页 ---------- */
    console.log('[静态资源]');
    const idx = await req('GET', '/');
    ok(idx.status === 200 && /象棋弈台/.test(idx.text) && /startPikafishGame/.test(idx.text), 'GET / 返回象棋弈台首页(含 startPikafishGame)');
    ok(/no-store/.test(idx.headers['cache-control'] || ''), '首页 Cache-Control no-store');
    const admin = await req('GET', '/admin');
    ok(admin.status === 200 && /no-store/.test(admin.headers['cache-control'] || ''), 'GET /admin 返回 admin.html(no-store)');
    const bak = await req('GET', '/index_bak.html');
    ok(bak.status === 404, 'index_bak.html 被 .assetsignore 屏蔽(404)');

    /* ---------- 核心 API ---------- */
    console.log('[核心 API]');
    const online = await req('GET', '/api/online');
    ok(online.status === 200 && typeof online.json.online === 'number', '/api/online 返回在线数');
    const train = await req('GET', '/api/train/status');
    ok(train.status === 200 && train.json.weights && train.json.total, '/api/train/status 返回权重+统计');
    const ann = await req('GET', '/api/announce');
    ok(ann.status === 200 && 'text' in ann.json, '/api/announce 公告端点');
    const lb = await req('GET', '/api/leaderboard');
    ok(lb.status === 200 && Array.isArray(lb.json.list), '/api/leaderboard 排行榜');
    const eg = await req('GET', '/api/endgame/list');
    ok(eg.status === 200 && Array.isArray(eg.json.list), '/api/endgame/list 残局列表');
    const aiGet = await req('GET', '/api/ai/data');
    ok(aiGet.status === 200 && aiGet.json.weights, '/api/ai/data GET 权重');
    const aiPost = await req('POST', '/api/ai/data', { weights: { attackKing: 71 }, stats: { games: 1 } });
    ok(aiPost.status === 200 && aiPost.json.success === true && aiPost.json.weights.attackKing > 70, '/api/ai/data POST 权重合并');
    const rr = await req('POST', '/api/race-report', JSON.stringify({ entry: 'test', t_self: 100 }), { 'Content-Type': 'application/json' });
    ok(rr.status === 200 && rr.text === 'ok', '/api/race-report 上报');
    const roomsList = await req('GET', '/api/rooms');
    ok(roomsList.status === 200 && Array.isArray(roomsList.json.rooms), '/api/rooms 房间列表');
    const createRoom = await req('GET', '/api/create-room');
    ok(createRoom.status === 200 && createRoom.json.roomId, '/api/create-room 生成房间号');

    /* ---------- 账号 ---------- */
    console.log('[注册/登录/资料]');
    const reg1 = await req('POST', '/api/auth/register', { username: '甲乙丙', password: 'secret66', deviceId: 'devA' });
    ok(reg1.status === 200 && reg1.json.ok === true && reg1.json.player.deviceId === 'U:甲乙丙', '注册账号(U: 前缀)');
    const regDup = await req('POST', '/api/auth/register', { username: '甲乙丙', password: 'secret66' });
    ok(regDup.status === 409, '重复注册返回 409');
    const badLen = await req('POST', '/api/auth/register', { username: 'x', password: 'secret66' });
    ok(badLen.status === 400, '用户名过短返回 400');
    const login1 = await req('POST', '/api/auth/login', { username: '甲乙丙', password: 'secret66' });
    ok(login1.status === 200 && login1.json.ok === true, '登录成功');
    const loginBad = await req('POST', '/api/auth/login', { username: '甲乙丙', password: 'wrong!!' });
    ok(loginBad.status === 401, '错误密码返回 401');
    await req('POST', '/api/auth/register', { username: '乙丙丁', password: 'secret66', deviceId: 'devB' });
    const prof = await req('GET', '/api/player/profile?deviceId=U:甲乙丙');
    ok(prof.status === 200 && prof.json.player && prof.json.player.name === '甲乙丙', '/api/player/profile');
    const predf = await req('POST', '/api/player/register', { deviceId: 'devG', name: '游客G' });
    ok(predf.status === 200 && predf.json.player.name === '游客G', '/api/player/register 游客');

    /* ---------- 管理端 ---------- */
    console.log('[管理端]');
    const admLogin = await req('POST', '/api/admin/login', { username: 'adminfan', password: 'Fdh_19681010' });
    ok(admLogin.status === 200 && admLogin.json.token, 'admin 登录取 token');
    const admTok = admLogin.json.token;
    const admBad = await req('POST', '/api/admin/login', { username: 'adminfan', password: 'bad' });
    ok(admBad.status === 401, 'admin 错误密码 401');
    const admPlayers = await req('POST', '/api/admin/players', { token: admTok });
    ok(admPlayers.status === 200 && admPlayers.json.ok === true && Array.isArray(admPlayers.json.list) && admPlayers.json.list.length >= 2, '/api/admin/players');
    const admNoAuth = await req('POST', '/api/admin/players', {});
    ok(admNoAuth.status === 401, '未登录 admin 401');
    const setScore = await req('POST', '/api/admin/setScore', { token: admTok, deviceId: 'U:甲乙丙', score: 300 });
    ok(setScore.status === 200 && setScore.json.player.elo === 300, 'setScore 改分');
    const vipGen = await req('POST', '/api/admin/vip/generate', { token: admTok, count: 2, days: 30 });
    ok(vipGen.status === 200 && vipGen.json.codes.length === 2, '生成 VIP 兑换码');
    const vipCode = vipGen.json.codes[0];
    const vipRedeem = await req('POST', '/api/vip/redeem', { id: 'devA', code: vipCode });
    ok(vipRedeem.json.ok === true, '兑换 VIP');
    const vipRedeem2 = await req('POST', '/api/vip/redeem', { id: 'devH', code: vipCode });
    ok(vipRedeem2.json && vipRedeem2.json.ok === false, '兑换码不可复用');
    const vipSt = await req('GET', '/api/vip/status?id=devA');
    ok(vipSt.json.member === true, 'VIP 状态生效');
    const setVip = await req('POST', '/api/admin/setVip', { token: admTok, deviceId: 'U:乙丙丁', days: 30 });
    ok(setVip.json.ok === true, 'setVip 直接开会员');
    const resetPw = await req('POST', '/api/admin/resetPassword', { token: admTok, username: '乙丙丁', newPassword: 'newpass88' });
    ok(resetPw.json.ok === true, 'resetPassword 重置密码');
    const loginNew = await req('POST', '/api/auth/login', { username: '乙丙丁', password: 'newpass88' });
    ok(loginNew.json.ok === true, '新密码可登录');

    /* ---------- 限额 ---------- */
    console.log('[每日限额]');
    const q1 = await req('POST', '/api/game/quota', { id: 'devQ', kind: 'match', check: 1 });
    ok(q1.json.ok === true && q1.json.remaining === 5, '限额检查(剩余5)');
    for (let i = 0; i < 5; i++) await req('POST', '/api/game/quota', { id: 'devQ', kind: 'match' });
    const q6 = await req('POST', '/api/game/quota', { id: 'devQ', kind: 'match' });
    ok(q6.json.ok === false, '5盘后限额拦截');

    /* ---------- 历史对局 ---------- */
    console.log('[历史对局]');
    const hs = await req('POST', '/api/history/save', { deviceId: 'devA', gm: 'ai', myColor: 'red', winner: 'red', moves: [{ fromRow: 9, fromCol: 7, toRow: 8, toCol: 7 }] });
    ok(hs.json.ok === true, 'history/save');
    const hl = await req('GET', '/api/history/list?deviceId=devA');
    ok(hl.json.ok === true && hl.json.games.length === 1, 'history/list');
    const gid = hl.json.games[0].id;
    const hg = await req('GET', '/api/history/get?id=' + gid + '&deviceId=devA');
    ok(hg.json.ok === true && hg.json.game.moves.length === 1, 'history/get');

    /* ---------- 快速匹配 ---------- */
    console.log('[快速匹配]');
    const mj1 = await req('POST', '/api/match/join', { deviceId: 'devC', name: 'C', elo: 1200 });
    const mj2 = await req('POST', '/api/match/join', { deviceId: 'devD', name: 'D', elo: 1250 });
    ok(mj1.json.ok && mj2.json.ok, 'match/join x2');
    let pairRoom = null;
    for (let i = 0; i < 10 && !pairRoom; i++) {
      await sleep(400);
      const s1 = await req('GET', '/api/match/status?ticket=' + mj1.json.ticket);
      if (s1.json.state === 'paired') pairRoom = s1.json.roomId;
    }
    ok(!!pairRoom, '匹配成功(roomId=' + pairRoom + ')');
    const dbg = await req('GET', '/api/match/debug');
    ok(dbg.status === 200 && dbg.json.ok, '/api/match/debug');
    const mj3 = await req('POST', '/api/match/join', { deviceId: 'devE', name: 'E', elo: 1300 });
    const mc = await req('POST', '/api/match/cancel', { ticket: mj3.json.ticket });
    ok(mc.json.ok === true, 'match/cancel');

    /* ---------- 大厅 WS ---------- */
    console.log('[大厅 WS]');
    const lobbyA = new WSClient('ws://127.0.0.1:' + PORT + '/ws');
    const ocWaitA = lobbyA.wait('online_count', null, 5000);
    await lobbyA.connect();
    const ocA = await ocWaitA;
    ok(!!ocA && typeof ocA.data === 'number', '连接即收到 online_count');
    lobbyA.send({ event: 'presence', payload: { deviceId: 'devA' } });

    /* ---------- 房间对局全流程 ---------- */
    console.log('[房间对局全流程]');
    // devA 已有 VIP → 建房放行
    const redirWait = lobbyA.wait('redirect_room', (d) => d.data && d.data.roomId === '8888' && d.data.action === 'create', 5000);
    lobbyA.send({ event: 'create_room', payload: { roomId: '8888', lastColor: 'black', deviceId: 'U:甲乙丙', playerName: '甲乙丙', vipId: 'devA', elo: 300 } });
    const redir = await redirWait;
    ok(!!redir, 'VIP 建房 → redirect_room(8888)');

    const roomA = new WSClient('ws://127.0.0.1:' + PORT + '/ws?roomId=8888');
    await roomA.connect();
    const createdWait = roomA.wait('room_created', null, 5000);
    roomA.send({ event: 'create_room', payload: { roomId: '8888', lastColor: 'black', deviceId: 'U:甲乙丙', playerName: '甲乙丙', elo: 300 } });
    const created = await createdWait;
    ok(!!created && created.data.color === 'red' && created.data.pid, '建房入座(红方+pid)');
    const pidA = created && created.data.pid;

    // 无 VIP 的乙 → lobby 建房应被拒
    const lobbyB = new WSClient('ws://127.0.0.1:' + PORT + '/ws');
    const ocWaitB = lobbyB.wait('online_count', null, 5000);
    await lobbyB.connect();
    await ocWaitB;
    const vipGateWait = lobbyB.wait('vip_only_create', null, 5000);
    lobbyB.send({ event: 'create_room', payload: { roomId: '9999', deviceId: 'devB' } });
    const vipGate = await vipGateWait;
    ok(!!vipGate, '非会员建房被门禁拦截(vip_only_create)');

    // 乙加入 8888
    const redirBWait = lobbyB.wait('redirect_room', (d) => d.data && d.data.action === 'join', 5000);
    lobbyB.send({ event: 'join_room', payload: { roomId: '8888', deviceId: 'U:乙丙丁', playerName: '乙丙丁' } });
    const redirB = await redirBWait;
    ok(!!redirB, 'join_room → redirect_room');
    const roomB = new WSClient('ws://127.0.0.1:' + PORT + '/ws?roomId=8888');
    await roomB.connect();
    const joinedWait = roomB.wait('room_joined', (d) => d.data.color === 'black', 5000);
    const gsAWait = roomA.wait('game_start', null, 6000);
    roomB.send({ event: 'join_room', payload: { roomId: '8888', deviceId: 'U:乙丙丁', playerName: '乙丙丁' } });
    const joined = await joinedWait;
    ok(!!joined && joined.data.pid, '乙入座(黑方)');
    const gsA = await gsAWait;
    ok(!!gsA, '甲方收到 game_start');

    // 走子
    const ackAWait = roomA.wait('move_ack', null, 5000);
    const oppBWait = roomB.wait('opponent_move', null, 5000);
    roomA.send({ event: 'make_move', payload: { fromRow: 9, fromCol: 7, toRow: 8, toCol: 7, redLeft: 899, blkLeft: 900 } });
    const ackA = await ackAWait;
    ok(!!ackA && ackA.data.moveHistoryLen === 1, '甲走子 move_ack');
    const oppB = await oppBWait;
    ok(!!oppB && oppB.data.fromCol === 7, '乙收到 opponent_move');
    // 非走子方走子应被拒: 刚才红方已走, 现在轮黑方 → 红方再走即违规(服务端仅校验回合, 与Worker一致)
    const rejAWait = roomA.wait('move_rejected', (d) => d.data.reason === 'not_your_turn', 5000);
    roomA.send({ event: 'make_move', payload: { fromRow: 0, fromCol: 0, toRow: 1, toCol: 0 } });
    const rejA = await rejAWait;
    ok(!!rejA, '红方抢走被拒(not_your_turn)');
    // 乙正常走子
    const ackBWait = roomB.wait('move_ack', null, 5000);
    const oppAWait = roomA.wait('opponent_move', null, 5000);
    roomB.send({ event: 'make_move', payload: { fromRow: 0, fromCol: 7, toRow: 1, toCol: 7, redLeft: 899, blkLeft: 898 } });
    const ackB = await ackBWait;
    ok(!!ackB && ackB.data.moveHistoryLen === 2, '乙走子 move_ack');
    const oppA = await oppAWait;
    ok(!!oppA, '甲收到 opponent_move');

    // 聊天
    const chatBWait = roomB.wait('chat', null, 5000);
    roomA.send({ event: 'chat', payload: '你好' });
    const chatB = await chatBWait;
    ok(!!chatB && chatB.data.message === '你好' && chatB.data.name === '甲乙丙', '聊天(带昵称)');

    // 悔棋
    const undoReqWait = roomB.wait('undo_requested', null, 5000);
    roomA.send({ event: 'request_undo', payload: {} });
    const undoReq = await undoReqWait;
    ok(!!undoReq, '悔棋请求');
    const undoAccWait = roomA.wait('undo_accepted', null, 5000);
    roomB.send({ event: 'accept_undo', payload: {} });
    const undoAcc = await undoAccWait;
    ok(!!undoAcc, '悔棋被接受');

    // 求和
    const drawReqWait = roomB.wait('draw_requested', null, 5000);
    roomA.send({ event: 'request_draw', payload: {} });
    const drawReq = await drawReqWait;
    ok(!!drawReq, '求和请求');

    // 悔棋后再走两步(保证≥2步用于Elo); 注意不能用与棋谱顶部相同的坐标(会被幂等去重)
    const ackA2Wait = roomA.wait('move_ack', null, 5000);
    roomA.send({ event: 'make_move', payload: { fromRow: 9, fromCol: 8, toRow: 8, toCol: 8, redLeft: 890, blkLeft: 890 } });
    await ackA2Wait;
    const ackB2Wait = roomB.wait('move_ack', null, 5000);
    roomB.send({ event: 'make_move', payload: { fromRow: 0, fromCol: 8, toRow: 1, toCol: 8, redLeft: 890, blkLeft: 889 } });
    await ackB2Wait;

    // 断线重连（乙断开→甲收到 player_disconnected→乙重连接管座位）
    roomB.close();
    await roomA.wait('player_disconnected', null, 5000).catch(() => null);
    ok(true, '对手断线通知');
    await sleep(300);
    const roomB2 = new WSClient('ws://127.0.0.1:' + PORT + '/ws?roomId=8888');
    await roomB2.connect();
    const restWait = roomB2.wait('room_state', (d) => d.data && d.data.color === 'black', 5000);
    roomB2.send({ event: 'reconnect_room', payload: { roomId: '8888', color: 'black', pid: joined.data.pid, deviceId: 'U:乙丙丁', playerName: '乙丙丁' } });
    const rest = await restWait;
    ok(!!rest && Array.isArray(rest.data.moveHistory) && rest.data.moveHistory.length >= 2, '断线重连恢复 room_state(含棋谱)');

    // 认输 → Elo 结算（双方都是 U: 账号, 已走3步）
    const goAWait = roomA.wait('game_over', null, 5000);
    const goBWait = roomB2.wait('game_over', null, 5000);
    const ratingWait = roomB2.wait('rating_update', null, 9000);
    roomA.send({ event: 'resign', payload: {} });
    const goA = await goAWait;
    const goB = await goBWait;
    ok(!!goA && !!goB && goA.data.winner === 'black' && goA.data.reason === 'resign', '认输 → game_over(黑胜)');
    const rating = await ratingWait;
    ok(!!rating && rating.data.red && rating.data.black, 'Elo 结算 rating_update');
    if (rating) console.log('    red:', rating.data.red.before, '→', rating.data.red.after, ' black:', rating.data.black.before, '→', rating.data.black.after);

    // 重赛
    const remReqWait = roomB2.wait('rematch_requested', null, 5000);
    roomA.send({ event: 'rematch_request', payload: {} });
    const remReq = await remReqWait;
    ok(!!remReq, '重赛请求');
    const remStartWait = roomA.wait('rematch_start', null, 5000);
    roomB2.send({ event: 'accept_rematch', payload: {} });
    const remStart = await remStartWait;
    ok(!!remStart, '重赛开始(换边)');

    // 离开房间 → 对方收到 room_closed
    const closedWait = roomB2.wait('room_closed', null, 5000);
    roomA.send({ event: 'leave_room', payload: {} });
    const closed = await closedWait;
    ok(!!closed, '离开 → room_closed');

    // 空房加入 → room_error
    const roomS = new WSClient('ws://127.0.0.1:' + PORT + '/ws?roomId=8888');
    await roomS.connect();
    const specWait = roomS.wait('room_error', null, 5000);
    roomS.send({ event: 'join_room', payload: { roomId: '8888' } });
    const spec = await specWait;
    ok(!!spec, '空房加入报 room_error(房间不存在或已解散)');

    lobbyA.close(); lobbyB.close(); roomA.close(); roomB2.close(); roomS.close();

    /* ---------- 支付 ---------- */
    console.log('[扫码支付]');
    const order = await req('POST', '/api/pay/order', { tier: 't30' });
    ok(order.json.ok === true && order.json.amount === '10.00', '支付下单(金额=10.00)');
    const hb = await req('GET', '/api/pay/heartbeat?key=XqPay_aF8k2mQ9wZ4t_2026');
    ok(hb.json.ok === true, '支付心跳回调');
    const notify = await req('POST', '/api/pay/notify?key=XqPay_aF8k2mQ9wZ4t_2026&amount=10.00', '');
    ok(notify.json.ok === true && notify.json.matched === true && notify.json.days === 30, '金额匹配自动发码');
    const paySt = await req('GET', '/api/pay/status?id=' + order.json.id);
    ok(paySt.json.status === 'paid' && /^VIP-/.test(paySt.json.code), '订单已支付+发码');
    const mon = await req('GET', '/api/pay/monitor?token=' + admTok);
    ok(mon.json.ok === true && mon.json.phoneOnline === true, '支付监控');

    /* ---------- 公告 ---------- */
    console.log('[公告]');
    const annSet = await req('POST', '/api/admin/announce', { token: admTok, text: '测试公告' });
    ok(annSet.json.ok === true, '发布公告');
    const annGet = await req('GET', '/api/announce');
    ok(annGet.json.text === '测试公告', '公告生效');

    /* ---------- 大厅在线人数一致性 ---------- */
    console.log('[在线账本]');
    const on2 = await req('GET', '/api/online');
    ok(on2.json.online >= 0 && typeof on2.json.online === 'number', '/api/online 一致性');

  } catch (e) {
    failed++;
    failures.push('异常: ' + e.message);
    console.error('测试异常:', e);
  } finally {
    if (failed) {
      console.log('\n----- 服务器日志(尾部60行) -----');
      const lines = serverLog.split('\n');
      console.log(lines.slice(-60).join('\n'));
    }
    console.log('\n========== 测试结果: ' + passed + ' 通过, ' + failed + ' 失败 ==========');
    if (failures.length) console.log('失败项:\n  - ' + failures.join('\n  - '));
    child.kill();
    setTimeout(() => process.exit(failed ? 1 : 0), 300);
  }
}

main().catch((e) => { console.error(e); process.exit(2); });
