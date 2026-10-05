/* ============================================================
 * 象棋弈台 serv00 服务端 - api.js
 * Cloudflare Worker handleApiRequest 的 1:1 Node 移植
 * 端点清单（与线上 Worker 完全一致）:
 *   /api/auth/register|login
 *   /api/player/register|profile
 *   /api/endgame/list|clear
 *   /api/leaderboard
 *   /api/match/join|status|cancel|debug
 *   /api/online
 *   /api/history/save|list|get
 *   /api/admin/login|players|setScore|deletePlayer|resetPassword
 *   /api/admin/vip/generate|batch_delete|codes|members|setVip
 *   /api/admin/endgame/list|save|op
 *   /api/admin/announce
 *   /api/admin/pay/list|confirm
 *   /api/ai/db-check|data   /api/train/start|stop|status
 *   /api/pay/order|notify|status|heartbeat|monitor
 *   /api/game/quota
 *   /api/announce
 *   /api/vip/status|redeem|consume
 *   /api/rooms   /api/create-room   /api/race-report
 * ============================================================ */
'use strict';
const crypto = require('crypto');
const U = require('./util');

/* ===== AI 权重全局状态（与 index.js 一致） ===== */
const aiWeights = {
  attackKing: 70, limitKingMob: 35, approach: 30, mobility: 5,
  rookNotMoved: 5, rookCrossed: 110, rookDeveloped: 90, horseDeveloped: 25,
  cannonDeveloped: 15, pieceSafety: 80, hangingPenalty: 80, tradeAccuracy: 120,
  pawnPromotion: 50, checkBonus: 130, centerControl: 20, rookCoordination: 60,
  kingSafety: 40
};
const aiTotalStats = { games: 0, redWins: 0, blkWins: 0, draws: 0 };

async function loadAiFromDb(db) {
  try {
    const result = await db.prepare("SELECT weights, stats FROM ai_weights WHERE id = 1").first();
    if (result) {
      if (result.weights) {
        try {
          const w = JSON.parse(result.weights);
          for (const k in w) {
            if (aiWeights[k] !== void 0 && typeof w[k] === "number") aiWeights[k] = w[k];
          }
        } catch (e) {
        }
      }
      if (result.stats) {
        try {
          const s = JSON.parse(result.stats);
          if (s && typeof s === "object") {
            if (typeof s.games === "number") aiTotalStats.games = s.games;
            if (typeof s.redWins === "number") aiTotalStats.redWins = s.redWins;
            if (typeof s.blkWins === "number") aiTotalStats.blkWins = s.blkWins;
            if (typeof s.draws === "number") aiTotalStats.draws = s.draws;
          }
        } catch (e) {
        }
      }
    }
  } catch (e) {
  }
}

async function saveAiToDb(db) {
  try {
    await db.exec(`CREATE TABLE IF NOT EXISTS ai_weights (id INTEGER PRIMARY KEY, weights TEXT, stats TEXT, updated_at INTEGER)`);
    await db.prepare(`INSERT OR REPLACE INTO ai_weights (id, weights, stats, updated_at) VALUES (1, ?, ?, ?)`).bind(JSON.stringify(aiWeights), JSON.stringify(aiTotalStats), Date.now()).run();
    return true;
  } catch (e) {
    return false;
  }
}

/* ===== 管理端登录限速（内存版） ===== */
const _admFail = new Map();
function adminLoginLimited(ip) {
  const now = Date.now();
  const arr = (_admFail.get(ip) || []).filter((t) => now - t < 60000);
  const limited = arr.length >= 5;
  if (!limited) arr.push(now);
  _admFail.set(ip, arr);
  return limited;
}

async function adminToken(dayKey) {
  return U.sha256Hex("xq-admin-Fdh_19681010-" + dayKey);
}

/* ===== 支付档位与回调密钥（与 index.js 一致） ===== */
const PAY_TIERS = {
  t30: { label: "30天会员", days: 30, yuan: 10 },
  t190: { label: "半年190天", days: 190, yuan: 30 },
  t365: { label: "一年365天", days: 365, yuan: 50 },
  t99: { label: "永久99年", days: 36135, yuan: 99 }
};
const PAY_NOTIFY_KEY = "XqPay_aF8k2mQ9wZ4t_2026";

/* ===== 主入口 ===== */
async function handleApiRequest(request, env) {
  const url = new URL(request.url);
  const path = url.pathname;
  const db = env.CHESS_DB;
  const matchQueue = env.MATCH_QUEUE;
  const corsHeaders = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type"
  };
  if (request.method === "OPTIONS") {
    return { status: 200, headers: corsHeaders, body: "" };
  }
  const json = (obj, status) => ({ status: status || 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify(obj) });

  if (path === "/api/auth/register" && request.method === "POST") {
    if (!db) return json({ error: "no db" }, 500);
    await U.ensureAuthSchema(db);
    await U.ensureRatingTables(db);
    try {
      const body = await request.json();
      const username = body.username ? String(body.username).trim() : "";
      const password = body.password ? String(body.password) : "";
      if (!/^[A-Za-z0-9_\u4e00-\u9fa5]{2,12}$/.test(username)) return json({ ok: false, error: "用户名需2-12位（中文/字母/数字/下划线）" }, 400);
      if (password.length < 6 || password.length > 64) return json({ ok: false, error: "密码需6-64位" }, 400);
      const exists = await db.prepare("SELECT username FROM user_auth WHERE username = ?").bind(username).first();
      if (exists) return json({ ok: false, error: "用户名已被注册" }, 409);
      const salt = U.randomSalt();
      const hash = U.hashPassword(password, salt);
      await db.prepare("INSERT INTO user_auth (username, pass_hash, pass_salt, created_at) VALUES (?, ?, ?, ?)").bind(username, hash, salt, Date.now()).run();
      // 账号身份键：U:用户名 —— Elo/战绩跟随账号而非设备
      const accId = "U:" + username;
      // 若当前设备已有战绩（游客试玩过），继承到账号
      let seeded = null;
      if (body.deviceId && body.deviceId !== accId) {
        try {
          seeded = await db.prepare("SELECT elo, games, wins, losses, draws FROM players WHERE device_id = ?").bind(String(body.deviceId).slice(0, 64)).first();
        } catch (e2) {
          seeded = null;
        }
      }
      const now = Date.now();
      if (seeded && seeded.games > 0) {
        await db.prepare("INSERT INTO players (device_id, name, elo, games, wins, losses, draws, created_at, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(device_id) DO UPDATE SET name = excluded.name, last_seen = excluded.last_seen").bind(accId, username, seeded.elo, seeded.games, seeded.wins, seeded.losses, seeded.draws || 0, now, now).run();
      } else {
        await U.upsertPlayer(db, accId, username);
      }
      const p = await db.prepare("SELECT * FROM players WHERE device_id = ?").bind(accId).first();
      return json({ ok: true, username, player: p ? { deviceId: p.device_id, name: p.name, elo: p.elo, games: p.games, wins: p.wins, losses: p.losses, draws: p.draws, title: U.rankTitle(p.elo) } : null });
    } catch (e) {
      return json({ ok: false, error: "注册失败，请重试" }, 500);
    }
  }

  if (path === "/api/auth/login" && request.method === "POST") {
    if (!db) return json({ error: "no db" }, 500);
    await U.ensureAuthSchema(db);
    await U.ensureRatingTables(db);
    try {
      const body = await request.json();
      const username = body.username ? String(body.username).trim() : "";
      const password = body.password ? String(body.password) : "";
      const row = await db.prepare("SELECT pass_hash, pass_salt FROM user_auth WHERE username = ?").bind(username).first();
      if (!row) return json({ ok: false, error: "用户名不存在" }, 401);
      const hash = U.hashPassword(password, row.pass_salt);
      if (hash !== row.pass_hash) return json({ ok: false, error: "密码错误" }, 401);
      const accId = "U:" + username;
      const p = await U.upsertPlayer(db, accId, username);
      return json({ ok: true, username, player: p ? { deviceId: p.device_id, name: p.name, elo: p.elo, games: p.games, wins: p.wins, losses: p.losses, draws: p.draws, title: U.rankTitle(p.elo) } : null });
    } catch (e) {
      return json({ ok: false, error: "登录失败，请重试" }, 500);
    }
  }

  if (path === "/api/player/register" && request.method === "POST") {
    if (!db) return json({ error: "no db" }, 500);
    await U.ensureRatingTables(db);
    try {
      const body = await request.json();
      if (!body.deviceId) return json({ error: "deviceId required" }, 400);
      const p = await U.upsertPlayer(db, String(body.deviceId).slice(0, 64), body.name ? String(body.name).slice(0, 24) : null);
      return json({ ok: true, player: p ? { deviceId: p.device_id, name: p.name, elo: p.elo, games: p.games, wins: p.wins, losses: p.losses, draws: p.draws, title: U.rankTitle(p.elo) } : null });
    } catch (e) {
      return json({ error: "bad request" }, 400);
    }
  }

  if (path === "/api/player/profile") {
    if (!db) return json({ error: "no db" }, 500);
    await U.ensureRatingTables(db);
    const devId = url.searchParams.get("deviceId");
    if (!devId) return json({ error: "deviceId required" }, 400);
    const p = await db.prepare("SELECT * FROM players WHERE device_id = ?").bind(String(devId).slice(0, 64)).first();
    return json({ ok: true, player: p ? { deviceId: p.device_id, name: p.name, elo: p.elo, games: p.games, wins: p.wins, losses: p.losses, draws: p.draws, title: U.rankTitle(p.elo) } : null });
  }

  if (path === "/api/endgame/list") {
    if (!db) return json({ error: "no db" }, 500);
    await U.ensureEndgameSchema(db);
    try {
      const rows = await db.prepare("SELECT id, name, difficulty, side, pieces, goal, solution FROM endgame_puzzles WHERE enabled = 1 ORDER BY difficulty ASC, id ASC LIMIT 100").all();
      const list = (rows.results || []).map(function (r) { let pc = []; try { pc = JSON.parse(r.pieces); } catch (e0) {} let sol = r.solution;
        if (sol == null || sol === "") sol = [];
        else if (typeof sol === "string") { try { const _p = JSON.parse(sol); if (Array.isArray(_p)) sol = _p; } catch (eS) {} }
        else if (!Array.isArray(sol)) sol = [];
        return { id: r.id, name: r.name, difficulty: r.difficulty, side: r.side, pieces: pc, goal: r.goal, solution: sol }; }).filter(function (x) { return x.pieces && x.pieces.length > 3; });
      return json({ ok: true, list: list }, 200);
    } catch (e) {
      return json({ error: "list failed" }, 500);
    }
  }

  if (path === "/api/endgame/clear" && request.method === "POST") {
    if (!db) return json({ error: "no db" }, 500);
    await U.ensureEndgameSchema(db);
    try {
      const b = await request.json();
      const pid = parseInt(b && b.id);
      const uid = b && b.deviceId ? String(b.deviceId).slice(0, 64) : null;
      const mv = Math.max(0, Math.min(500, parseInt(b && b.moves) || 0));
      if (!pid || !uid) return json({ ok: false, error: "参数错误" }, 400);
      await db.prepare("INSERT INTO endgame_records (puzzle_id, uid, moves, win, created_at) VALUES (?1, ?2, ?3, 1, ?4)").bind(pid, uid, mv, Date.now()).run();
      return json({ ok: true }, 200);
    } catch (e) {
      return json({ error: "clear failed" }, 500);
    }
  }

  if (path === "/api/leaderboard") {
    if (!db) return json({ error: "no db" }, 500);
    await U.ensureRatingTables(db);
    const rows = await db.prepare("SELECT device_id, name, elo, games, wins, losses, draws FROM players WHERE games > 0 AND device_id LIKE 'U:%' ORDER BY elo DESC LIMIT 500").all();
    return json({ ok: true, list: (rows.results || []).map((r, i) => ({ rank: i + 1, name: r.name || "游客", games: r.games, wins: r.wins, losses: r.losses, draws: r.draws })) });
  }

  if (path === "/api/match/join" && request.method === "POST" && matchQueue) {
    try {
      const body = await request.json();
      if (!body.deviceId) return json({ error: "deviceId required" }, 400);
      return json(await matchQueue.join({ deviceId: body.deviceId, name: body.name, elo: body.elo }));
    } catch (e) {
      return json({ error: "match unavailable" }, 503);
    }
  }

  if (path === "/api/match/status" && matchQueue) {
    try {
      const ticket = url.searchParams.get("ticket");
      if (!ticket) return json({ error: "ticket required" }, 400);
      return json(await matchQueue.status(ticket));
    } catch (e) {
      return json({ error: "match unavailable" }, 503);
    }
  }

  if (path === "/api/online") {
    const pT3 = await matchQueue ? matchQueue.presence({ get: true }) : null;
    if (pT3 && typeof pT3.online === "number") env.onlineCount = pT3.online;
    return json({ online: env.onlineCount, count: env.onlineCount });
  }

  if (path === "/api/history/save" && db) {
    try {
      await U.ensureGameHistoryTable(db);
      const body = await request.json();
      const dev = body && body.deviceId ? String(body.deviceId).slice(0, 64) : "";
      const moves = body && Array.isArray(body.moves) ? body.moves : null;
      if (!dev || !moves || !moves.length) return json({ ok: false, error: "bad request" }, 400);
      const compact = moves.map((m) => [m.fromRow, m.fromCol, m.toRow, m.toCol]);
      await db.prepare(
        "INSERT INTO game_history (device_id, ts, gm, my_color, winner, moves, move_count) VALUES (?, ?, ?, ?, ?, ?, ?)"
      ).bind(dev, Date.now(), String(body.gm || "").slice(0, 16), String(body.myColor || "").slice(0, 8), String(body.winner || "").slice(0, 8), JSON.stringify(compact), compact.length).run();
      await db.prepare(
        "DELETE FROM game_history WHERE device_id = ? AND id NOT IN (SELECT id FROM game_history WHERE device_id = ? ORDER BY ts DESC, id DESC LIMIT 10)"
      ).bind(dev, dev).run();
      return json({ ok: true });
    } catch (e) {
      return json({ ok: false }, 500);
    }
  }

  if (path === "/api/history/list" && db) {
    try {
      await U.ensureGameHistoryTable(db);
      const dev = url.searchParams.get("deviceId");
      if (!dev) return json({ ok: false, error: "deviceId required" }, 400);
      const r = await db.prepare(
        "SELECT id, ts, gm, my_color, winner, move_count FROM game_history WHERE device_id = ? ORDER BY ts DESC, id DESC LIMIT 10"
      ).bind(dev.slice(0, 64)).all();
      return json({ ok: true, games: (r.results || []).slice().reverse() });
    } catch (e) {
      return json({ ok: false }, 500);
    }
  }

  if (path === "/api/history/get" && db) {
    try {
      await U.ensureGameHistoryTable(db);
      const gid = parseInt(url.searchParams.get("id"), 10);
      const dev = url.searchParams.get("deviceId");
      if (!gid || !dev) return json({ ok: false, error: "bad request" }, 400);
      const r = await db.prepare(
        "SELECT id, ts, gm, my_color, winner, moves FROM game_history WHERE id = ? AND device_id = ?"
      ).bind(gid, dev.slice(0, 64)).first();
      if (!r) return json({ ok: false, error: "not found" }, 404);
      let mv = [];
      try { mv = JSON.parse(r.moves).map((a) => ({ fromRow: a[0], fromCol: a[1], toRow: a[2], toCol: a[3] })); } catch (e2) {}
      return json({ ok: true, game: { id: r.id, ts: r.ts, gm: r.gm, myCl: r.my_color, winner: r.winner, moves: mv } });
    } catch (e) {
      return json({ ok: false }, 500);
    }
  }

  /* ---------- 管理端 ---------- */
  const adminDayKey = new Date().toISOString().slice(0, 10);
  const urlToken = url.searchParams.get("token") || null;
  let bodyToken = null;
  if (request.method === "POST") {
    try {
      const b = await request.json();
      if (b && typeof b.token === "string") bodyToken = b.token;
    } catch (e) {
    }
  }
  const token = bodyToken || urlToken;
  const isAdmin = !!token && token === (await adminToken(adminDayKey));

  if (path === "/api/admin/login" && request.method === "POST") {
    try {
      const b = await request.json();
      const ip = env.ip || "x";
      if (adminLoginLimited(ip)) return json({ ok: false, error: "尝试过于频繁，请稍后再试" }, 429);
      if (b && b.username === "adminfan" && b.password === "Fdh_19681010") return json({ ok: true, token: await adminToken(adminDayKey) });
      return json({ ok: false, error: "账号或密码错误" }, 401);
    } catch (e) {
      return json({ ok: false, error: "bad request" }, 400);
    }
  }

  if (path === "/api/admin/players" && isAdmin) {
    if (!db) return json({ error: "no db" }, 500);
    await U.ensureRatingTables(db);
    try {
      await db.exec("CREATE TABLE IF NOT EXISTS vip_members (id TEXT PRIMARY KEY, tier TEXT, expires_at INTEGER, created_at INTEGER, source TEXT)");
      const rows = await db.prepare("SELECT p.device_id, p.name, p.elo, p.games, p.wins, p.losses, p.draws, p.created_at, p.last_seen, v.tier AS vip_tier, v.expires_at AS vip_exp FROM players p LEFT JOIN vip_members v ON v.id = 'acc:' || substr(p.device_id, 3) WHERE p.device_id LIKE 'U:%' ORDER BY p.last_seen DESC LIMIT 1000").all();
      const _nowV = Date.now();
      return json({ ok: true, list: (rows.results || []).map((r) => ({ deviceId: r.device_id, name: r.name, elo: r.elo, games: r.games, wins: r.wins, losses: r.losses, draws: r.draws, title: U.rankTitle(r.elo), created_at: r.created_at, last_seen: r.last_seen, vip: (r.vip_exp && r.vip_exp > _nowV) ? { tier: r.vip_tier || "vip", expiresAt: r.vip_exp } : null })) });
    } catch (e) {
      return json({ ok: false, error: "查询失败" }, 500);
    }
  }

  if (path === "/api/admin/setScore" && request.method === "POST" && isAdmin) {
    if (!db) return json({ error: "no db" }, 500);
    try {
      const b = await request.json();
      const dev = b && b.deviceId ? String(b.deviceId).slice(0, 64) : null;
      const sc = b && Number.isFinite(Number(b.score)) ? Math.round(Number(b.score)) : null;
      if (!dev || sc === null) return json({ ok: false, error: "参数错误" }, 400);
      const clamped = Math.max(-250, Math.min(7000, sc));
      await db.prepare("UPDATE players SET elo = ? WHERE device_id = ?").bind(clamped, dev).run();
      const p = await db.prepare("SELECT device_id, name, elo, games, wins, losses, draws FROM players WHERE device_id = ?").bind(dev).first();
      return json({ ok: true, player: p ? { deviceId: p.device_id, name: p.name, elo: p.elo, games: p.games, wins: p.wins, losses: p.losses, draws: p.draws, title: U.rankTitle(p.elo) } : null });
    } catch (e) {
      return json({ ok: false, error: "修改失败" }, 500);
    }
  }

  if (path === "/api/admin/deletePlayer" && request.method === "POST" && isAdmin) {
    if (!db) return json({ error: "no db" }, 500);
    try {
      const b = await request.json();
      const dev = b && b.deviceId ? String(b.deviceId).slice(0, 64) : null;
      if (!dev) return json({ ok: false, error: "参数错误" }, 400);
      await db.prepare("DELETE FROM players WHERE device_id = ?").bind(dev).run();
      if (dev.startsWith("U:")) {
        try {
          await db.prepare("DELETE FROM user_auth WHERE username = ?").bind(dev.slice(2)).run();
        } catch (e2) {
        }
      }
      return json({ ok: true, deleted: dev });
    } catch (e) {
      return json({ ok: false, error: "删除失败" }, 500);
    }
  }

  if (path === "/api/admin/resetPassword" && request.method === "POST" && isAdmin) {
    if (!db) return json({ error: "no db" }, 500);
    await U.ensureAuthSchema(db);
    try {
      const b = await request.json();
      const username = b && b.username ? String(b.username).slice(0, 12) : null;
      const np = b && typeof b.newPassword === "string" ? b.newPassword : null;
      if (!username || !np || np.length < 6 || np.length > 64) return json({ ok: false, error: "新密码需6~64位" }, 400);
      const salt = U.randomSalt();
      const hash = U.hashPassword(np, salt);
      const r = await db.prepare("UPDATE user_auth SET pass_hash = ?, pass_salt = ? WHERE username = ?").bind(hash, salt, username).run();
      if (!r.success || r.meta.changes === 0) return json({ ok: false, error: "该玩家不是账号（游客无密码）" }, 404);
      return json({ ok: true, username });
    } catch (e) {
      return json({ ok: false, error: "重置失败" }, 500);
    }
  }

  if (path === "/api/admin/players") return json({ ok: false, error: "未登录" }, 401);

  if (path === "/api/match/debug" && matchQueue) {
    try {
      return json(await matchQueue.debug());
    } catch (e) {
      return json({ error: "match unavailable" }, 503);
    }
  }

  if (path === "/api/match/cancel" && request.method === "POST" && matchQueue) {
    try {
      const body = await request.json();
      return json(await matchQueue.cancel(body && body.ticket));
    } catch (e) {
      return json({ error: "match unavailable" }, 503);
    }
  }

  /* ---------- AI 权重/训练（保留原有加载点位） ---------- */
  if (db) {
    await U.ensureAiDb(db);
    await loadAiFromDb(db);
  }

  if (path === "/api/ai/db-check") {
    if (db) {
      const raw = await db.prepare("SELECT * FROM ai_weights WHERE id = 1").first();
      return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ raw, aiTotalStats }) };
    }
    return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ error: "no db" }) };
  }

  if (path === "/api/train/start") {
    return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ success: true }) };
  }

  if (path === "/api/train/stop") {
    return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ success: true }) };
  }

  if (path === "/api/train/status") {
    if (db) {
      await loadAiFromDb(db);
    }
    return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({
      training: false,
      session: { games: 0, redWins: 0, blkWins: 0, draws: 0 },
      total: aiTotalStats,
      weights: aiWeights
    }) };
  }

  if (path === "/api/ai/data") {
    if (request.method === "GET") {
      if (db) {
        await loadAiFromDb(db);
      }
      return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ weights: aiWeights, trainStats: aiTotalStats }) };
    }
    if (request.method === "POST") {
      try {
        if (db) {
          await loadAiFromDb(db);
        }
        const data = await request.json();
        if (data.weights) {
          const mergeFactor = data.isDelta ? 0.3 : 0.5;
          for (const k in data.weights) {
            if (aiWeights[k] !== void 0) {
              const newValue = data.weights[k];
              aiWeights[k] = aiWeights[k] * (1 - mergeFactor) + newValue * mergeFactor;
            }
          }
        }
        if (data.stats) {
          for (const k in data.stats) {
            if (aiTotalStats[k] !== void 0) {
              aiTotalStats[k] += data.stats[k] || 0;
            }
          }
        }
        let saved = false;
        if (db) {
          saved = await saveAiToDb(db);
        }
        return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ success: true, weights: aiWeights, total: aiTotalStats, saved }) };
      } catch (e) {
        return { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ success: false, error: e.message }) };
      }
    }
  }

  if (path === "/api/admin/vip/generate" && request.method === "POST" && isAdmin) {
    try {
      await db.exec("CREATE TABLE IF NOT EXISTS vip_codes (code TEXT PRIMARY KEY, tier TEXT, days INTEGER, used_by TEXT, used_at INTEGER, created_at INTEGER)");
      const b = await request.json();
      const count = Math.max(1, Math.min(50, parseInt(b && b.count) || 1));
      const days = Math.max(1, Math.min(36500, parseInt(b && b.days) || 30));
      const tier = String((b && b.tier) || "vip").slice(0, 20);
      const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
      const codes = [];
      for (let i = 0; i < count; i++) {
        const bytes = new Uint8Array(10);
        U.randomValues(bytes);
        let code = "VIP-";
        for (let j = 0; j < 10; j++) code += alphabet[bytes[j] % alphabet.length];
        try {
          const ins = await db.prepare("INSERT OR IGNORE INTO vip_codes (code, tier, days, created_at) VALUES (?1, ?2, ?3, ?4)").bind(code, tier, days, Date.now()).run();
          if (ins && ins.meta && ins.meta.changes === 1) codes.push(code);
        } catch (e) {}
      }
      return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: true, codes }) };
    } catch (e) {
      return { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ error: "generate failed" }) };
    }
  }

  if (path === "/api/admin/endgame/list" && isAdmin) {
    if (!db) return json({ error: "no db" }, 500);
    await U.ensureEndgameSchema(db);
    try {
      const rows = await db.prepare("SELECT id, name, difficulty, side, pieces, goal, enabled, created_at, solution FROM endgame_puzzles ORDER BY id ASC LIMIT 500").all();
      const list = (rows.results || []).map(function (r) { let pc = []; try { pc = JSON.parse(r.pieces); } catch (e0) {} let sol = r.solution;
        if (sol == null || sol === "") sol = [];
        else if (typeof sol === "string") { try { const _p = JSON.parse(sol); if (Array.isArray(_p)) sol = _p; } catch (eS) {} }
        else if (!Array.isArray(sol)) sol = [];
      return { id: r.id, name: r.name, difficulty: r.difficulty, side: r.side, pieces: pc, goal: r.goal, enabled: r.enabled, created_at: r.created_at, count: (pc || []).length, solution: sol }; });
      return json({ ok: true, list: list }, 200);
    } catch (e) {
      return json({ error: "list failed" }, 500);
    }
  }

  if (path === "/api/admin/endgame/save" && request.method === "POST" && isAdmin) {
    if (!db) return json({ error: "no db" }, 500);
    await U.ensureEndgameSchema(db);
    try {
      const b = await request.json();
      const name = b && b.name ? String(b.name).trim().slice(0, 30) : "";
      const difficulty = Math.max(1, Math.min(5, parseInt(b && b.difficulty) || 1));
      const side = b && b.side === 'black' ? 'black' : 'red';
      const goal = b && b.goal ? String(b.goal).trim().slice(0, 120) : "";
      const pieces = U.validatePieces(b && b.pieces);
      if (!name || !pieces) return json({ ok: false, error: "名称或棋子布局不合法（双方各需一将）" }, 400);
      const pj = JSON.stringify(pieces);
      let solJson = "[]";
      if (b && typeof b.solutionText === "string" && b.solutionText.trim()) {
        solJson = String(b.solutionText).slice(0, 4000);
      }
      if (b && Array.isArray(b.solution)) {
        const sol = [];
        for (const sm of b.solution) { if (sm && typeof sm === "object") { const fr = parseInt(sm.fromRow), fc = parseInt(sm.fromCol), tr = parseInt(sm.toRow), tc = parseInt(sm.toCol); if ([fr, fc, tr, tc].every(function (v) { return v >= 0 && v <= 9; })) sol.push({ fromRow: fr, fromCol: fc, toRow: tr, toCol: tc }); } }
        solJson = JSON.stringify(sol.slice(0, 30));
      }
      if (b && b.id) {
        await db.prepare("UPDATE endgame_puzzles SET name=?1, difficulty=?2, side=?3, pieces=?4, goal=?5, enabled=?6, solution=?7 WHERE id=?8").bind(name, difficulty, side, pj, goal, b.enabled === 0 ? 0 : 1, solJson, parseInt(b.id)).run();
        return json({ ok: true, id: parseInt(b.id) }, 200);
      }
      const mxRow = await db.prepare("SELECT COALESCE(MAX(id), 0) + 1 AS nid FROM endgame_puzzles").first();
      const nid = (mxRow && mxRow.nid) || 1;
      const ins = await db.prepare("INSERT INTO endgame_puzzles (id, name, difficulty, side, pieces, goal, enabled, created_at, solution) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?8)").bind(nid, name, difficulty, side, pj, goal, Date.now(), solJson).run();
      return json({ ok: true, id: nid }, 200);
    } catch (e) {
      return json({ error: "save failed" }, 500);
    }
  }

  if (path === "/api/admin/endgame/op" && request.method === "POST" && isAdmin) {
    if (!db) return json({ error: "no db" }, 500);
    try {
      const b = await request.json();
      const id = parseInt(b && b.id);
      if (!id) return json({ ok: false, error: "参数错误" }, 400);
      if (b && b.op === "delete") { await db.prepare("DELETE FROM endgame_puzzles WHERE id = ?1").bind(id).run(); return json({ ok: true }, 200); }
      if (b && b.op === "toggle") { await db.prepare("UPDATE endgame_puzzles SET enabled = CASE enabled WHEN 1 THEN 0 ELSE 1 END WHERE id = ?1").bind(id).run(); return json({ ok: true }, 200); }
      return json({ ok: false, error: "未知操作" }, 400);
    } catch (e) {
      return json({ error: "op failed" }, 500);
    }
  }

  if (path === "/api/admin/vip/batch_delete" && request.method === "POST" && isAdmin) {
    try {
      const b = await request.json();
      const type = b && b.type === "members" ? "members" : "codes";
      const ids = Array.isArray(b && b.ids) ? b.ids.map(function (x) { return String(x).slice(0, 64); }).filter(Boolean).slice(0, 200) : [];
      if (!ids.length) return { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: false, error: "未选择任何项" }) };
      const stmts = ids.map(function (k) { return type === "members" ? db.prepare("DELETE FROM vip_members WHERE id = ?1").bind(k) : db.prepare("DELETE FROM vip_codes WHERE code = ?1").bind(k); });
      const rs = await db.batch(stmts);
      const deleted = rs.reduce(function (acc, r) { return acc + (r && r.meta && r.meta.changes ? r.meta.changes : 0); }, 0);
      return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: true, deleted }) };
    } catch (e) {
      return { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ error: "batch delete failed" }) };
    }
  }

  if (path === "/api/admin/vip/codes" && isAdmin) {
    try {
      const rs = await db.prepare("SELECT code, tier, days, used_by, used_at, created_at FROM vip_codes ORDER BY created_at DESC LIMIT 200").all();
      return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: true, codes: rs.results || [] }) };
    } catch (e) {
      return { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ error: "list failed" }) };
    }
  }

  if (path === "/api/admin/vip/members" && isAdmin) {
    try {
      const rs = await db.prepare("SELECT id, tier, expires_at, created_at, source FROM vip_members ORDER BY expires_at DESC LIMIT 200").all();
      return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: true, members: rs.results || [] }) };
    } catch (e) {
      return { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ error: "list failed" }) };
    }
  }

  if (path === "/api/admin/setVip" && request.method === "POST" && isAdmin) {
    if (!db) return json({ error: "no db" }, 500);
    try {
      await db.exec("CREATE TABLE IF NOT EXISTS vip_members (id TEXT PRIMARY KEY, tier TEXT, expires_at INTEGER, created_at INTEGER, source TEXT)");
      const b = await request.json();
      const dev = b && b.deviceId ? String(b.deviceId).slice(0, 64) : null;
      const days = b && Number.isFinite(Number(b.days)) ? Math.round(Number(b.days)) : null;
      if (!dev || days === null) return json({ ok: false, error: "参数错误" }, 400);
      const vipId2 = dev.startsWith("U:") ? "acc:" + dev.slice(2) : dev;
      const nowV = Date.now();
      if (days <= 0) {
        await db.prepare("DELETE FROM vip_members WHERE id = ?").bind(vipId2).run();
        return json({ ok: true, vip: null });
      }
      const cur = await db.prepare("SELECT expires_at FROM vip_members WHERE id = ?").bind(vipId2).first();
      const base = cur && cur.expires_at > nowV ? cur.expires_at : nowV;
      const expiresAt = base + days * 86400e3;
      await db.prepare("INSERT INTO vip_members (id, tier, expires_at, created_at, source) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT (id) DO UPDATE SET tier = excluded.tier, expires_at = excluded.expires_at").bind(vipId2, "vip", expiresAt, nowV, "admin").run();
      return json({ ok: true, vip: { tier: "vip", expiresAt } });
    } catch (e) {
      return json({ ok: false, error: "操作失败" }, 500);
    }
  }

  /* ---------- 扫码付款自动发码（V免签式：手机监听回调） ---------- */
  if (path === "/api/pay/order" && request.method === "POST") {
    if (!db) return json({ ok: false, error: "no db" }, 500);
    try {
      await db.exec("CREATE TABLE IF NOT EXISTS pay_orders (id TEXT PRIMARY KEY, tier TEXT, days INTEGER, amount_cents INTEGER, status TEXT DEFAULT 'pending', code TEXT, created_at INTEGER, paid_at INTEGER)");
      await db.exec("CREATE UNIQUE INDEX IF NOT EXISTS pay_orders_pending_amt ON pay_orders(amount_cents) WHERE status = 'pending'");
      const b = await request.json().catch(() => ({}));
      const tier = PAY_TIERS[b && b.tier];
      if (!tier) return json({ ok: false, error: "无效档位" }, 400);
      const now = Date.now();
      await db.prepare("DELETE FROM pay_orders WHERE status='pending' AND created_at < ?1").bind(now - 35 * 60e3).run();
      const cnt = await db.prepare("SELECT COUNT(*) AS c FROM pay_orders WHERE status='pending'").first();
      if (cnt && cnt.c >= 20) return json({ ok: false, error: "当前订单较多，请稍后再试" }, 429);
      let id = null, cents = null, made = false;
      const start = Math.floor(Math.random() * 97) + 1;
      for (let i = 0; i < 100 && !made; i++) {
        const off = i === 0 ? 0 : ((start + i - 2) % 99) + 1;
        const c2 = tier.yuan * 100 + off;
        const id2 = crypto.randomUUID();
        try {
          await db.prepare("INSERT INTO pay_orders (id, tier, days, amount_cents, status, created_at) VALUES (?1, ?2, ?3, ?4, 'pending', ?5)").bind(id2, b.tier, tier.days, c2, now).run();
          id = id2; cents = c2; made = true;
        } catch (e) {}
      }
      if (!made) return json({ ok: false, error: "订单创建失败，请稍后再试" }, 503);
      return json({ ok: true, id, amount: (cents / 100).toFixed(2), label: tier.label, minutes: 30 });
    } catch (e) {
      return json({ ok: false, error: "order failed" }, 500);
    }
  }

  if (path === "/api/pay/notify" && (request.method === "POST" || request.method === "GET")) {
    if (!db) return json({ ok: false, error: "no db" }, 500);
    try {
      const u2 = new URL(request.url);
      let key = u2.searchParams.get("key"), amount = u2.searchParams.get("amount"), text = u2.searchParams.get("text") || "";
      if (request.method === "POST") {
        const raw = await request.text();
        let pb = null;
        try { pb = JSON.parse(raw); } catch (e) {}
        if (pb) { key = key || pb.key; amount = amount || pb.amount; text = text || pb.text || ""; }
        else if (!amount) text = text || raw;
      }
      if (!key || key !== PAY_NOTIFY_KEY) return json({ ok: false, error: "bad key" }, 403);
      let cents = 0;
      if (amount) { const m2 = String(amount).match(/(\d{1,4}(?:\.\d{1,2})?)/); if (m2) cents = Math.round(parseFloat(m2[1]) * 100); }
      if (!cents && text) { const m3 = text.match(/(\d{1,4}\.\d{2})/); if (m3) cents = Math.round(parseFloat(m3[1]) * 100); }
      if (!cents) return json({ ok: false, error: "no amount" }, 400);
      const now = Date.now();
      const ord = await db.prepare("SELECT id, tier FROM pay_orders WHERE amount_cents = ?1 AND status='pending' AND created_at > ?2 ORDER BY created_at DESC LIMIT 1").bind(cents, now - 35 * 60e3).first();
      if (!ord) return json({ ok: true, matched: false, note: "no pending order for this amount" });
      const tier = PAY_TIERS[ord.tier] || { days: 30 };
      const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
      await db.exec("CREATE TABLE IF NOT EXISTS vip_codes (code TEXT PRIMARY KEY, tier TEXT, days INTEGER, used_by TEXT, used_at INTEGER, created_at INTEGER)");
      let code = "", codeOk = false;
      for (let att = 0; att < 3 && !codeOk; att++) {
        const bytes = new Uint8Array(10);
        U.randomValues(bytes);
        code = "VIP-";
        for (let j = 0; j < 10; j++) code += alphabet[bytes[j] % alphabet.length];
        const ins = await db.prepare("INSERT OR IGNORE INTO vip_codes (code, tier, days, created_at) VALUES (?1, ?2, ?3, ?4)").bind(code, "vip", tier.days, now).run();
        codeOk = !!(ins && ins.meta && ins.meta.changes === 1);
      }
      if (!codeOk) return json({ ok: false, error: "发码失败，请重试" }, 503);
      await db.prepare("UPDATE pay_orders SET status='paid', code=?1, paid_at=?2 WHERE id=?3").bind(code, now, ord.id).run();
      return json({ ok: true, matched: true, orderId: ord.id, days: tier.days });
    } catch (e) {
      return json({ ok: false, error: "notify failed" }, 500);
    }
  }

  if (path === "/api/pay/status" && request.method === "GET") {
    if (!db) return json({ ok: false, error: "no db" }, 500);
    try {
      const id = new URL(request.url).searchParams.get("id") || "";
      if (!id) return json({ ok: false, error: "bad id" }, 400);
      const r = await db.prepare("SELECT status, code FROM pay_orders WHERE id = ?1").bind(id).first();
      if (!r) return json({ ok: true, status: "gone" });
      if (r.status === "paid" && r.code) return json({ ok: true, status: "paid", code: r.code });
      return json({ ok: true, status: r.status });
    } catch (e) {
      return json({ ok: false, error: "status failed" }, 500);
    }
  }

  if (path === "/api/pay/heartbeat" && (request.method === "GET" || request.method === "POST")) {
    if (!db) return json({ ok: false }, 500);
    try {
      const u3 = new URL(request.url);
      const key = u3.searchParams.get("key") || "";
      if (key !== PAY_NOTIFY_KEY) return json({ ok: false, error: "bad key" }, 403);
      await db.exec("CREATE TABLE IF NOT EXISTS pay_hb (id INTEGER PRIMARY KEY, ts INTEGER)");
      await db.prepare("INSERT INTO pay_hb (id, ts) VALUES (1, ?1) ON CONFLICT (id) DO UPDATE SET ts = ?1").bind(Date.now()).run();
      return json({ ok: true });
    } catch (e) {
      return json({ ok: false }, 500);
    }
  }

  if (path === "/api/pay/monitor" && request.method === "GET") {
    if (!db) return json({ ok: false, error: "no db" }, 500);
    try {
      const u4 = new URL(request.url);
      if (u4.searchParams.get("token") !== (await adminToken(adminDayKey))) return json({ ok: false, error: "unauthorized" }, 403);
      await db.exec("CREATE TABLE IF NOT EXISTS pay_hb (id INTEGER PRIMARY KEY, ts INTEGER)");
      const hb = await db.prepare("SELECT ts FROM pay_hb WHERE id = 1").first();
      const pc = await db.prepare("SELECT COUNT(*) AS c FROM pay_orders WHERE status='pending'").first();
      const last = hb && hb.ts ? Math.round((Date.now() - hb.ts) / 1000) : -1;
      return json({ ok: true, hbSecondsAgo: last, phoneOnline: last >= 0 && last < 300, pendingOrders: pc ? pc.c : 0 });
    } catch (e) {
      return json({ ok: false, error: "monitor failed" }, 500);
    }
  }

  if (path === "/api/admin/pay/list" && request.method === "GET" && isAdmin) {
    if (!db) return json({ ok: false, error: "no db" }, 500);
    try {
      const rs = await db.prepare("SELECT id, tier, days, amount_cents, status, code, created_at, paid_at FROM pay_orders ORDER BY created_at DESC LIMIT 50").all();
      return json({ ok: true, orders: rs.results || [] });
    } catch (e) {
      return json({ ok: false, error: "list failed" }, 500);
    }
  }

  if (path === "/api/admin/pay/confirm" && request.method === "POST" && isAdmin) {
    if (!db) return json({ ok: false, error: "no db" }, 500);
    try {
      const b = await request.json();
      const oid = b && b.orderId ? String(b.orderId).slice(0, 64) : "";
      if (!oid) return json({ ok: false, error: "参数错误" }, 400);
      const ord = await db.prepare("SELECT id, tier, status, code FROM pay_orders WHERE id = ?1").bind(oid).first();
      if (!ord) return json({ ok: false, error: "订单不存在" }, 404);
      if (ord.status === "paid" && ord.code) return json({ ok: true, already: true, code: ord.code });
      const tier = PAY_TIERS[ord.tier] || { days: 30 };
      const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
      const now = Date.now();
      await db.exec("CREATE TABLE IF NOT EXISTS vip_codes (code TEXT PRIMARY KEY, tier TEXT, days INTEGER, used_by TEXT, used_at INTEGER, created_at INTEGER)");
      let code = "", codeOk = false;
      for (let att = 0; att < 3 && !codeOk; att++) {
        const bytes = new Uint8Array(10);
        U.randomValues(bytes);
        code = "VIP-";
        for (let j = 0; j < 10; j++) code += alphabet[bytes[j] % alphabet.length];
        const ins = await db.prepare("INSERT OR IGNORE INTO vip_codes (code, tier, days, created_at) VALUES (?1, ?2, ?3, ?4)").bind(code, "vip", tier.days, now).run();
        codeOk = !!(ins && ins.meta && ins.meta.changes === 1);
      }
      if (!codeOk) return json({ ok: false, error: "发码失败，请重试" }, 503);
      await db.prepare("UPDATE pay_orders SET status='paid', code=?1, paid_at=?2 WHERE id=?3").bind(code, now, oid).run();
      return json({ ok: true, code, days: tier.days });
    } catch (e) {
      return json({ ok: false, error: "confirm failed" }, 500);
    }
  }

  /* ---------- 非会员每日对局限额（棋力评测/人机高级/残局挑战 各5盘，会员无限） ---------- */
  const GAME_KINDS = { match: "棋力评测", aiadv: "人机对弈高级", endgame: "残局挑战" };
  const GAME_DAILY_LIMIT = 5;
  if (path === "/api/game/quota" && (request.method === "GET" || request.method === "POST")) {
    if (!db) return json({ ok: false, error: "no db" }, 500);
    try {
      await db.exec("CREATE TABLE IF NOT EXISTS game_quota (id TEXT, day TEXT, kind TEXT, count INTEGER, PRIMARY KEY (id, day, kind))");
      await db.exec("CREATE TABLE IF NOT EXISTS vip_members (id TEXT PRIMARY KEY, tier TEXT, expires_at INTEGER, created_at INTEGER, source TEXT)");
      const u5 = new URL(request.url);
      let id = u5.searchParams.get("id") || "", kind = u5.searchParams.get("kind") || "", check = u5.searchParams.get("check") === "1";
      if (request.method === "POST") {
        const b = await request.json().catch(() => ({}));
        id = (b && b.id) || id; kind = (b && b.kind) || kind;
        check = !!(b && b.check);
      }
      id = String(id).slice(0, 80); kind = String(kind).slice(0, 20);
      if (!id || !GAME_KINDS[kind]) return json({ ok: false, error: "参数错误" }, 400);
      const day = new Date(Date.now() + 28800e3).toISOString().slice(0, 10);
      const m = await db.prepare("SELECT expires_at FROM vip_members WHERE id = ?1").bind(id).first();
      const member = !!(m && m.expires_at > Date.now());
      const row = await db.prepare("SELECT count FROM game_quota WHERE id = ?1 AND day = ?2 AND kind = ?3").bind(id, day, kind).first();
      const used = row ? row.count : 0;
      if (request.method === "GET") {
        return json({ ok: true, kind, member, used, limit: member ? 0 : GAME_DAILY_LIMIT, remaining: member ? -1 : Math.max(0, GAME_DAILY_LIMIT - used) });
      }
      if (member) return json({ ok: true, member: true, kind, remaining: -1 });
      if (used >= GAME_DAILY_LIMIT) return json({ ok: false, member: false, kind, used, limit: GAME_DAILY_LIMIT, error: "今日" + GAME_KINDS[kind] + "免费5盘已用完，开通会员无限畅玩" });
      if (check) return json({ ok: true, member: false, kind, used, limit: GAME_DAILY_LIMIT, remaining: GAME_DAILY_LIMIT - used, check: true });
      await db.prepare("INSERT INTO game_quota (id, day, kind, count) VALUES (?1, ?2, ?3, 1) ON CONFLICT (id, day, kind) DO UPDATE SET count = count + 1").bind(id, day, kind).run();
      return json({ ok: true, member: false, kind, used: used + 1, limit: GAME_DAILY_LIMIT, remaining: GAME_DAILY_LIMIT - used - 1 });
    } catch (e) {
      return json({ ok: false, error: "quota failed" }, 500);
    }
  }

  if (path === "/api/announce" && request.method === "GET") {
    try {
      const adb = db;
      await adb.exec("CREATE TABLE IF NOT EXISTS announcements (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT, url TEXT, active INTEGER, created_at INTEGER)");
      const row = await adb.prepare("SELECT id, text, url, created_at FROM announcements WHERE active = 1 ORDER BY id DESC LIMIT 1").first();
      return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ text: row ? row.text : null, url: row ? row.url : null }) };
    } catch (e) {
      return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ text: null, url: null }) };
    }
  }

  if (path === "/api/admin/announce" && request.method === "POST" && isAdmin) {
    try {
      const adb = db;
      const b = await request.json();
      await adb.exec("CREATE TABLE IF NOT EXISTS announcements (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT, url TEXT, active INTEGER, created_at INTEGER)");
      if (b && b.clear) {
        await adb.prepare("UPDATE announcements SET active = 0 WHERE active = 1").run();
        return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: true, cleared: true }) };
      }
      const text = b && b.text ? String(b.text).slice(0, 500) : "";
      const aurl = b && b.url ? String(b.url).slice(0, 300) : "";
      if (!text) return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: false, error: "内容为空" }) };
      await adb.prepare("UPDATE announcements SET active = 0 WHERE active = 1").run();
      await adb.prepare("INSERT INTO announcements (text, url, active, created_at) VALUES (?1, ?2, 1, ?3)").bind(text, aurl, Date.now()).run();
      return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: true }) };
    } catch (e) {
      return { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: false, error: "发布失败" }) };
    }
  }

  /* ---------- VIP ---------- */
  if (path.startsWith("/api/vip/")) {
    if (!db) return { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ error: "no db" }) };
    try {
      await db.exec("CREATE TABLE IF NOT EXISTS vip_members (id TEXT PRIMARY KEY, tier TEXT, expires_at INTEGER, created_at INTEGER, source TEXT)");
      await db.exec("CREATE TABLE IF NOT EXISTS vip_codes (code TEXT PRIMARY KEY, tier TEXT, days INTEGER, used_by TEXT, used_at INTEGER, created_at INTEGER)");
      await db.exec("CREATE TABLE IF NOT EXISTS vip_usage (id TEXT, day TEXT, count INTEGER, PRIMARY KEY (id, day))");
    } catch (e) {}
    const vipNow = Date.now();
    const vipDay = new Date(vipNow + 28800e3).toISOString().slice(0, 10);
    const vipQuota = 3;
    const vipGetMember = async (id) => {
      try { const r = await db.prepare("SELECT tier, expires_at FROM vip_members WHERE id = ?").bind(id).first(); if (r && r.expires_at > vipNow) return { member: true, tier: r.tier, expiresAt: r.expires_at }; } catch (e) {}
      return { member: false };
    };
    const vipUsedToday = async (id) => {
      try { const r = await db.prepare("SELECT count FROM vip_usage WHERE id = ? AND day = ?").bind(id, vipDay).first(); return r ? r.count : 0; } catch (e) { return 0; }
    };
    if (path === "/api/vip/status" && request.method === "GET") {
      const id = (url.searchParams.get("id") || "").slice(0, 80);
      if (!id) return { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ error: "no id" }) };
      const m = await vipGetMember(id);
      const used = m.member ? 0 : await vipUsedToday(id);
      let gq = null;
      if (!m.member) {
        try {
          await db.exec("CREATE TABLE IF NOT EXISTS game_quota (id TEXT, day TEXT, kind TEXT, count INTEGER, PRIMARY KEY (id, day, kind))");
          const rows = await db.prepare("SELECT kind, count FROM game_quota WHERE id = ?1 AND day = ?2").bind(id, vipDay).all();
          gq = {};
          for (const K of ["match", "aiadv", "endgame"]) {
            const r2 = (rows.results || []).find(function (x) { return x.kind === K; });
            const c = r2 ? r2.count : 0;
            gq[K] = { used: c, remaining: Math.max(0, 5 - c) };
          }
        } catch (e) { gq = null; }
      }
      return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ member: m.member, tier: m.tier || null, expiresAt: m.expiresAt || null, used, remaining: m.member ? null : Math.max(0, vipQuota - used), freeQuota: vipQuota, gq }) };
    }
    if (path === "/api/vip/redeem" && request.method === "POST") {
      try {
        const b = await request.json();
        const id = String((b && b.id) || "").slice(0, 80);
        const code = String((b && b.code) || "").trim().toUpperCase();
        if (!id || !code) return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: false, error: "参数缺失" }) };
        const cr = await db.prepare("SELECT code, tier, days, used_by FROM vip_codes WHERE code = ?").bind(code).first();
        if (!cr) return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: false, error: "兑换码无效" }) };
        if (cr.used_by) return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: false, error: "兑换码已被使用" }) };
        const cur = await db.prepare("SELECT expires_at FROM vip_members WHERE id = ?").bind(id).first();
        const base = cur && cur.expires_at > vipNow ? cur.expires_at : vipNow;
        const expiresAt = base + (cr.days || 30) * 86400e3;
        await db.prepare("INSERT INTO vip_members (id, tier, expires_at, created_at, source) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT (id) DO UPDATE SET tier = excluded.tier, expires_at = excluded.expires_at").bind(id, cr.tier || "vip", expiresAt, vipNow, code).run();
        await db.prepare("UPDATE vip_codes SET used_by = ?1, used_at = ?2 WHERE code = ?3").bind(id, vipNow, code).run();
        return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: true, tier: cr.tier || "vip", expiresAt }) };
      } catch (e) {
        return { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: false, error: "兑换失败,请稍后再试" }) };
      }
    }
    if (path === "/api/vip/consume" && request.method === "POST") {
      try {
        const b = await request.json();
        const id = String((b && b.id) || "").slice(0, 80);
        if (!id) return { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: false, error: "no id" }) };
        const m = await vipGetMember(id);
        if (m.member) return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: true, member: true, remaining: null }) };
        const used = await vipUsedToday(id);
        if (used >= vipQuota) return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: false, error: "quota", used, remaining: 0, freeQuota: vipQuota }) };
        const nUsed = used + 1;
        await db.prepare("INSERT INTO vip_usage (id, day, count) VALUES (?1, ?2, ?3) ON CONFLICT (id, day) DO UPDATE SET count = vip_usage.count + 1").bind(id, vipDay, nUsed).run();
        return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: true, member: false, used: nUsed, remaining: vipQuota - nUsed, freeQuota: vipQuota }) };
      } catch (e) {
        return { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ ok: false, error: "consume failed" }) };
      }
    }
    return { status: 404, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ error: "Not found" }) };
  }

  if (path === "/api/rooms") {
    try {
      const rdb = db;
      await rdb.exec("CREATE TABLE IF NOT EXISTS rooms_registry (id TEXT PRIMARY KEY, red TEXT, black TEXT, status TEXT, watchers INTEGER, updated_at INTEGER)");
      const rows = await rdb.prepare("SELECT id, red, black, status, watchers, updated_at FROM rooms_registry WHERE updated_at > ? ORDER BY updated_at DESC LIMIT 50").bind(Date.now() - 12e4).all();
      return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ rooms: rows.results || [] }) };
    } catch (e) {
      return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ rooms: [] }) };
    }
  }

  if (path === "/api/create-room") {
    const customId = url.searchParams.get("id");
    const roomId = customId || (await U.generateFreeRoomId(db));
    return { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ roomId }) };
  }

  return { status: 404, headers: { ...corsHeaders, "Content-Type": "application/json" }, body: JSON.stringify({ error: "Not found" }) };
}

module.exports = { handleApiRequest, aiWeights, aiTotalStats };
