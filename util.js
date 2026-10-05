/* ============================================================
 * 象棋弈台 serv00 服务端 - util.js
 * 从 Cloudflare Worker index.js 1:1 移植的共享工具/数据层
 * ============================================================ */
'use strict';
const crypto = require('crypto');

/* WebCrypto 兼容垫片（Worker 代码使用 crypto.getRandomValues 填充字节数组）
 * 注: Node>=19 的 crypto.getRandomValues 是只读 getter, 不能直接赋值, 用本地实现替代 */
function randomValues(arr) {
  const b = crypto.randomBytes(arr.length);
  for (let i = 0; i < arr.length; i++) arr[i] = b[i];
  return arr;
}

/* ===== 等级分系统 (Elo + 段位) — 与 index.js 完全一致 ===== */
const TIER_TABLE = [
  [-160, "学1-1"], [-100, "学1-2"], [-40, "学1-3"],
  [20, "学2-1"], [80, "学2-2"], [140, "学2-3"],
  [200, "学3-1"], [260, "学3-2"], [320, "学3-3"],
  [400, "业1-1"], [480, "业1-2"], [560, "业1-3"],
  [640, "业2-1"], [720, "业2-2"], [800, "业2-3"],
  [880, "业3-1"], [960, "业3-2"], [1040, "业3-3"],
  [1120, "业4-1"], [1200, "业4-2"], [1280, "业4-3"],
  [1360, "业5-1"], [1440, "业5-2"], [1520, "业5-3"],
  [1600, "业6-1"], [1700, "业6-2"], [1800, "业6-3"],
  [1900, "业7-1"], [2000, "业7-2"], [2100, "业7-3"],
  [2200, "业8-1"], [2350, "业8-2"], [2500, "业8-3"],
  [2700, "业9-1"], [3000, "业9-2"], [3300, "业9-3"],
  [3700, "神1-1"], [4100, "神1-2"], [4500, "神1-3"],
  [5000, "神2-1"], [5500, "神2-2"], [6000, "神2-3"],
  [6400, "神3-1"], [6700, "神3-2"], [7000, "神3-3"]
];
const TIER_SCORE_MIN = -250;
const TIER_SCORE_MAX = 7000;
const TIER_SCORE_DEFAULT = -160;

function rankTitle(score) {
  let label = TIER_TABLE[0][1];
  for (const t of TIER_TABLE) {
    if (score >= t[0]) label = t[1];
  }
  return label;
}

function tierIndex(score) {
  let i = 0;
  for (let k = 0; k < TIER_TABLE.length; k++) {
    if (score >= TIER_TABLE[k][0]) i = k;
  }
  return i;
}

/* 天天象棋棋力评测结算: 同小段 ±10; 对手低1小段 胜+15/负-5; 对手高1小段 胜+5/负-15; 平0 */
function tierDelta(myScore, oppScore, outcome) {
  if (outcome === 0.5) return 0;
  let d = tierIndex(myScore) - tierIndex(oppScore);
  if (d > 1) d = 1;
  else if (d < -1) d = -1;
  const win = outcome === 1;
  if (d === 0) return win ? 10 : -10;
  if (d > 0) return win ? 5 : -15;
  return win ? 15 : -5;
}

/* ===== 密码/盐/摘要 ===== */
function randomSalt() {
  const b = randomValues(new Uint8Array(16));
  return [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
}

/* 与 Worker WebCrypto PBKDF2 输出完全一致: SHA-256, 100000 次, 256 位, hex */
function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(String(password), Buffer.from(String(salt), "utf8"), 100000, 32, "sha256").toString("hex");
}

function sha256Hex(text) {
  return crypto.createHash("sha256").update(Buffer.from(String(text), "utf8")).digest("hex");
}

function newPid() {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

/* ===== 残局棋子校验（与 index.js 一致） ===== */
function validatePieces(arr) {
  if (!Array.isArray(arr) || arr.length < 4 || arr.length > 32) return null;
  const okT = { rook: 1, horse: 1, elephant: 1, advisor: 1, king: 1, cannon: 1, pawn: 1 };
  const out = [];
  let rk = 0, bk = 0;
  const seen = new Set();
  for (const p of arr) {
    if (!p || !okT[p.type] || (p.color !== "red" && p.color !== "black")) return null;
    const row = parseInt(p.row), col = parseInt(p.col);
    if (!(row >= 0 && row <= 9 && col >= 0 && col <= 8)) return null;
    const key = row + "," + col;
    if (seen.has(key)) return null;
    seen.add(key);
    if (p.type === "king") { if (p.color === "red") rk++; else bk++; }
    out.push({ type: p.type, color: p.color, row: row, col: col });
  }
  if (rk !== 1 || bk !== 1) return null;
  return out;
}

/* ===== D1 表结构（与 index.js 的 ensure* 完全一致） ===== */
async function ensureRatingTables(db) {
  if (!db) return;
  try {
    await db.exec("CREATE TABLE IF NOT EXISTS players (device_id TEXT PRIMARY KEY, name TEXT, elo INTEGER DEFAULT 1200, games INTEGER DEFAULT 0, wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, draws INTEGER DEFAULT 0, created_at INTEGER, last_seen INTEGER)");
    await db.exec("CREATE TABLE IF NOT EXISTS games (id INTEGER PRIMARY KEY AUTOINCREMENT, room_id TEXT, red_device TEXT, black_device TEXT, red_name TEXT, black_name TEXT, result TEXT, reason TEXT, moves INTEGER, red_elo_before INTEGER, black_elo_before INTEGER, red_elo_after INTEGER, black_elo_after INTEGER, end_ts INTEGER)");
  } catch (e) {
  }
}

let _authSchemaReady = false;
async function ensureAuthSchema(db) {
  if (!db || _authSchemaReady) return;
  try {
    await db.prepare("CREATE TABLE IF NOT EXISTS user_auth (username TEXT PRIMARY KEY, pass_hash TEXT NOT NULL, pass_salt TEXT NOT NULL, created_at INTEGER)").run();
    _authSchemaReady = true;
  } catch (e) {
  }
}

let _egSchemaReady = false;
async function ensureEndgameSchema(db) {
  if (!db || _egSchemaReady) return;
  try {
    await db.exec("CREATE TABLE IF NOT EXISTS endgame_puzzles (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, difficulty INTEGER DEFAULT 1, side TEXT DEFAULT 'red', pieces TEXT NOT NULL, goal TEXT DEFAULT '', enabled INTEGER DEFAULT 1, created_at INTEGER)");
    await db.exec("CREATE TABLE IF NOT EXISTS endgame_records (id INTEGER PRIMARY KEY AUTOINCREMENT, puzzle_id INTEGER, uid TEXT, moves INTEGER, win INTEGER, created_at INTEGER)");
    try { await db.exec("ALTER TABLE endgame_puzzles ADD COLUMN solution TEXT DEFAULT '[]'"); } catch (eS1) {}
    _egSchemaReady = true;
  } catch (e) {
  }
}

async function ensureGameHistoryTable(db) {
  if (!db) return;
  try {
    await db.exec("CREATE TABLE IF NOT EXISTS game_history (id INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT, ts INTEGER, gm TEXT, my_color TEXT, winner TEXT, moves TEXT, move_count INTEGER)");
    await db.exec("CREATE INDEX IF NOT EXISTS idx_game_history_dev ON game_history (device_id, ts)");
  } catch (e) {
  }
}

async function ensureRoomStateTable(db) {
  if (!db) return;
  try {
    await db.exec("CREATE TABLE IF NOT EXISTS room_state (room_id TEXT PRIMARY KEY, state TEXT, updated_at INTEGER)");
  } catch (e) {
  }
}

async function ensureRoomsRegistryTable(db) {
  if (!db) return;
  try {
    await db.exec("CREATE TABLE IF NOT EXISTS rooms_registry (id TEXT PRIMARY KEY, red TEXT, black TEXT, status TEXT, watchers INTEGER, updated_at INTEGER)");
  } catch (e) {
  }
}

async function ensureAiDb(db) {
  try {
    await db.exec(`CREATE TABLE IF NOT EXISTS ai_weights (id INTEGER PRIMARY KEY, weights TEXT, stats TEXT, updated_at INTEGER)`);
  } catch (e) {
  }
}

async function ensureKvTable(db) {
  try {
    await db.exec("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)");
  } catch (e) {
  }
}

async function kvGet(db, key) {
  try {
    const r = await db.prepare("SELECT v FROM kv WHERE k = ?").bind(key).first();
    if (!r || r.v == null) return null;
    return JSON.parse(r.v);
  } catch (e) {
    return null;
  }
}

async function kvPut(db, key, val) {
  try {
    await db.prepare("INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)").bind(key, JSON.stringify(val)).run();
  } catch (e) {
  }
}

async function upsertPlayer(db, deviceId, name) {
  if (!db || !deviceId) return null;
  const now = Date.now();
  try {
    await db.prepare("INSERT INTO players (device_id, name, elo, games, wins, losses, draws, created_at, last_seen) VALUES (?, ?, -160, 0, 0, 0, 0, ?, ?) ON CONFLICT(device_id) DO UPDATE SET last_seen = excluded.last_seen").bind(deviceId, name || null, now, now).run();
    if (name) await db.prepare("UPDATE players SET name = ? WHERE device_id = ?").bind(name, deviceId).run();
    return await db.prepare("SELECT * FROM players WHERE device_id = ?").bind(deviceId).first();
  } catch (e) {
    return null;
  }
}

/* ===== Elo 结算（与 index.js applyEloResult 完全一致） ===== */
async function applyEloResult(db, roomId, red, black, result, reason, moveCount) {
  if (!db || !red || !black || !red.dev || !black.dev || red.dev === black.dev) return null;
  if (!["red", "black", "draw"].includes(result)) return null;
  // 游客不记录: 任一方为非注册账号(设备身份)则整局不入战绩/排行
  if (!/^U:/.test(String(red.dev)) || !/^U:/.test(String(black.dev))) return null;
  try {
    const now = Date.now();
    const insP = "INSERT INTO players (device_id, name, elo, games, wins, losses, draws, created_at, last_seen) VALUES (?, ?, -160, 0, 0, 0, 0, ?, ?)";
    const r0 = await db.prepare("SELECT elo, games FROM players WHERE device_id = ?").bind(red.dev).first();
    if (!r0) await db.prepare(insP).bind(red.dev, red.name || null, now, now).run();
    const b0 = await db.prepare("SELECT elo, games FROM players WHERE device_id = ?").bind(black.dev).first();
    if (!b0) await db.prepare(insP).bind(black.dev, black.name || null, now, now).run();
    const before = { red: r0 ? r0.elo : TIER_SCORE_DEFAULT, black: b0 ? b0.elo : TIER_SCORE_DEFAULT };
    const outcomeR = result === "red" ? 1 : result === "draw" ? 0.5 : 0;
    const outcomeB = result === "black" ? 1 : result === "draw" ? 0.5 : 0;
    const dR = tierDelta(before.red, before.black, outcomeR);
    const dB = tierDelta(before.black, before.red, outcomeB);
    const after = { red: Math.max(TIER_SCORE_MIN, Math.min(TIER_SCORE_MAX, before.red + dR)), black: Math.max(TIER_SCORE_MIN, Math.min(TIER_SCORE_MAX, before.black + dB)) };
    const wr = result === "red" ? 1 : 0;
    const br = result === "black" ? 1 : 0;
    const dr = result === "draw" ? 1 : 0;
    await db.batch([
      db.prepare("UPDATE players SET elo = ?, games = games + 1, wins = wins + ?, losses = losses + ?, draws = draws + ?, last_seen = ? WHERE device_id = ?").bind(after.red, wr, br, dr, now, red.dev),
      db.prepare("UPDATE players SET elo = ?, games = games + 1, wins = wins + ?, losses = losses + ?, draws = draws + ?, last_seen = ? WHERE device_id = ?").bind(after.black, br, wr, dr, now, black.dev),
      db.prepare("INSERT INTO games (room_id, red_device, black_device, red_name, black_name, result, reason, moves, red_elo_before, black_elo_before, red_elo_after, black_elo_after, end_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").bind(roomId || null, red.dev, black.dev, red.name || null, black.name || null, result, reason || null, moveCount || 0, before.red, before.black, after.red, after.black, now)
    ]);
    return { red: { before: before.red, after: after.red }, black: { before: before.black, after: after.black } };
  } catch (e) {
    return null;
  }
}

/* 生成未被占用(在线对局中)的4位房间号; 查room_state存档表, 对局中房间必有行, 结束即删 */
async function generateFreeRoomId(db) {
  for (let i = 0; i < 24; i++) {
    const id = String(Math.floor(1000 + Math.random() * 9000));
    try {
      if (!db) return id;
      const row = await db.prepare("SELECT 1 FROM room_state WHERE room_id = ?").bind(id).first();
      if (!row) return id;
    } catch (e) {
      return id;
    }
  }
  return String(Date.now()).slice(-6);
}

/* ===== HTTP 响应助手（对齐 Worker Response 行为） ===== */
function jsonResponse(obj, status, extraHeaders) {
  return {
    status: status || 200,
    headers: Object.assign({ "Content-Type": "application/json" }, extraHeaders || {}),
    body: JSON.stringify(obj)
  };
}

module.exports = {
  TIER_TABLE, TIER_SCORE_MIN, TIER_SCORE_MAX, TIER_SCORE_DEFAULT,
  rankTitle, tierIndex, tierDelta,
  randomValues, randomSalt, hashPassword, sha256Hex, newPid, validatePieces,
  ensureRatingTables, ensureAuthSchema, ensureEndgameSchema,
  ensureGameHistoryTable, ensureRoomStateTable, ensureRoomsRegistryTable,
  ensureAiDb, ensureKvTable, kvGet, kvPut,
  upsertPlayer, applyEloResult, generateFreeRoomId,
  jsonResponse
};
