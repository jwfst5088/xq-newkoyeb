/* ============================================================
 * 象棋弈台 serv00 服务端 - match.js
 * Cloudflare Worker MatchQueue DO 的 1:1 Node 移植
 * - 快速匹配队列（elo 容差配对: 60 + 等待秒数*12, 上限600）
 * - 幽灵票据清理（40秒无 /status 轮询过期）
 * - 在线唯一账本 presence（连接注册表/设备去重/心跳保活/70秒僵尸清理）
 * - 队列与配对结果持久化（kv 表，替代 DO storage）
 * ============================================================ */
'use strict';
const { generateFreeRoomId, kvGet, kvPut, ensureKvTable } = require('./util');

class MatchQueue {
  constructor(db) {
    this.db = db;
    this.queue = [];
    this.pairings = new Map();
    this._loaded = false;
    this.events = [];
    this.online = 0;
    this._pConn = new Map();
    this._pTotal = 0;
    this._sweepTimer = setInterval(() => {
      try {
        if (this.queue.length > 0) {
          this._tryPair();
          this._persist();
        }
      } catch (e) {
      }
    }, 2000);
    if (this._sweepTimer.unref) this._sweepTimer.unref();
  }

  _log(ev, detail) {
    try {
      this.events.push({ t: Date.now(), ev: ev, d: detail || "" });
      if (this.events.length > 40) this.events = this.events.slice(-40);
    } catch (e) {
    }
  }

  async _ensureLoaded() {
    if (this._loaded) return;
    this._loaded = true;
    try {
      await ensureKvTable(this.db);
      const q = await kvGet(this.db, "mq_queue");
      if (Array.isArray(q)) this.queue = q;
      const p = await kvGet(this.db, "mq_pairings");
      if (Array.isArray(p)) this.pairings = new Map(p);
    } catch (e) {
    }
  }

  async _persist() {
    try {
      await ensureKvTable(this.db);
      await kvPut(this.db, "mq_queue", this.queue);
      await kvPut(this.db, "mq_pairings", [...this.pairings]);
    } catch (e) {
    }
  }

  async _tryPair() {
    const now = Date.now();
    // 幽灵票据清理: act=最后活跃时间(/status轮询刷新), 40秒无轮询即过期(关页未cancel的票据快速失效); ts=原始入队时间(仅用于配对容差,不变)
    this.queue = this.queue.filter((e) => now - (e.act || 0) < 40e3);
    if (this.queue.length < 2) return;
    const sorted = [...this.queue].sort((a, b) => a.elo - b.elo);
    const pairedTickets = new Set();
    for (let i = 0; i < sorted.length; i++) {
      const a = sorted[i];
      if (pairedTickets.has(a.ticket)) continue;
      for (let j = i + 1; j < sorted.length; j++) {
        const b = sorted[j];
        if (pairedTickets.has(b.ticket)) continue;
        const waitSec = Math.floor((now - Math.min(a.ts, b.ts)) / 1e3);
        // 棋力分制: 初始±小段约60-80分, 起始容差60, 等待越久越宽(上限600)
        const tol = Math.min(60 + waitSec * 12, 600);
        if (Math.abs(a.elo - b.elo) <= tol) {
          pairedTickets.add(a.ticket);
          pairedTickets.add(b.ticket);
          const roomId = await generateFreeRoomId(this.db);
          const redIsA = Math.random() < 0.5;
          this._log("pair", (a.name || "?") + " vs " + (b.name || "?") + " room=" + roomId);
          this.pairings.set(a.ticket, { data: { roomId, color: redIsA ? "red" : "black", oppName: b.name || null, oppElo: b.elo || null, rated: true }, ts: now });
          this.pairings.set(b.ticket, { data: { roomId, color: redIsA ? "black" : "red", oppName: a.name || null, oppElo: a.elo || null, rated: true }, ts: now });
          break;
        }
      }
    }
    if (pairedTickets.size) {
      this.queue = this.queue.filter((e) => !pairedTickets.has(e.ticket));
      if (this.pairings.size > 500) {
        const entries = [...this.pairings.entries()].sort((x, y) => x[1].ts - y[1].ts);
        for (let k = 0; k < 250; k++) this.pairings.delete(entries[k][0]);
      }
    }
  }

  /* 对齐 DO fetch /join */
  async join(body) {
    await this._ensureLoaded();
    try {
      if (!body || !body.deviceId) return { ok: false, error: "deviceId required" };
      const ticket = Math.random().toString(36).slice(2) + Date.now().toString(36);
      const elo = Number.isFinite(body.elo) ? Math.round(body.elo) : 1200;
      // 同一设备最多保留3条排队（允许同机多窗口互相匹配，同时防刷）
      const devCount = this.queue.filter((e) => e.deviceId === body.deviceId).length;
      if (devCount >= 3) this.queue = this.queue.filter((e) => e.deviceId !== body.deviceId);
      this.queue.push({ ticket, deviceId: String(body.deviceId).slice(0, 64), name: body.name ? String(body.name).slice(0, 24) : null, elo, ts: Date.now(), act: Date.now() });
      this._log("join", (body.name || body.deviceId) + " elo=" + elo + " queue=" + this.queue.length);
      await this._tryPair();
      await this._persist();
      return { ok: true, ticket };
    } catch (e) {
      return { ok: false, error: "bad request" };
    }
  }

  /* 对齐 DO fetch /status */
  async status(ticket) {
    await this._ensureLoaded();
    const p = ticket && this.pairings.get(ticket);
    if (p) {
      this.pairings.delete(ticket);
      await this._persist();
      return { ok: true, state: "paired", roomId: p.data.roomId, color: p.data.color, oppName: p.data.oppName, oppElo: p.data.oppElo };
    }
    const still2 = ticket && this.queue.find((e) => e.ticket === ticket);
    if (still2) still2.act = Date.now(); // 轮询即活跃: 刷新票据存活时间(防幽灵票据占位)
    const still = !!still2;
    return { ok: true, state: still ? "waiting" : "none", queueSize: this.queue.length };
  }

  /* 对齐 DO fetch /cancel */
  async cancel(ticket) {
    await this._ensureLoaded();
    try {
      this.queue = this.queue.filter((e) => e.ticket !== ticket);
      this._log("cancel", "left=" + this.queue.length);
      await this._persist();
      return { ok: true };
    } catch (e) {
      return { ok: false };
    }
  }

  /* 对齐 DO fetch /online */
  async online() {
    await this._ensureLoaded();
    return { ok: true, online: this.online || 0 };
  }

  /* 对齐 DO fetch /presence — 在线唯一账本 */
  async presence(body) {
    await this._ensureLoaded();
    try {
      const now = Date.now();
      if (!this._pConn) this._pConn = new Map();
      if (body && typeof body.join === "string") {
        const pex = this._pConn.get(body.join);
        this._pConn.set(body.join, { dev: pex ? pex.dev : null, seen: now });
      } else if (body && typeof body.leave === "string") {
        this._pConn.delete(body.leave);
      } else if (body && typeof body.set === "string" && body.deviceId) {
        const pex2 = this._pConn.get(body.set);
        if (pex2) pex2.dev = String(body.deviceId).slice(0, 64); else this._pConn.set(body.set, { dev: String(body.deviceId).slice(0, 64), seen: now });
      } else if (body && Array.isArray(body.heartbeat)) {
        for (const hit of body.heartbeat) {
          if (hit && typeof hit.id === "string") {
            const pc2 = this._pConn.get(hit.id);
            if (pc2) { pc2.seen = now; if (hit.dev && !pc2.dev) pc2.dev = String(hit.dev).slice(0, 64); } else this._pConn.set(hit.id, { dev: hit.dev ? String(hit.dev).slice(0, 64) : null, seen: now });
          }
        }
      }
      for (const [pid2, pc5] of [...this._pConn]) { if (now - pc5.seen > 70000) this._pConn.delete(pid2); }
      const devs = new Set();
      let anon = 0;
      for (const [, pc4] of this._pConn) { if (pc4.dev) devs.add(pc4.dev); else anon++; }
      this._pTotal = devs.size + anon;
      return { ok: true, online: this._pTotal };
    } catch (e) {
      return { ok: false, online: this._pTotal || 0 };
    }
  }

  /* 对齐 DO fetch /debug */
  async debug() {
    await this._ensureLoaded();
    const now = Date.now();
    const _pList = [];
    if (this._pConn) { for (const [k2, v2] of this._pConn) { _pList.push({ id: k2.slice(0, 6), dev: v2.dev ? v2.dev.slice(0, 8) : null, ageMs: Date.now() - v2.seen }); } }
    return {
      ok: true, queueSize: this.queue.length, pairings: this.pairings.size,
      presence: { total: this._pTotal || 0, conns: _pList },
      queue: this.queue.map((e) => ({ dev: String(e.deviceId).slice(0, 6) + "***", name: e.name || null, elo: e.elo, ageSec: Math.floor((now - e.ts) / 1e3), ticket: String(e.ticket).slice(0, 6) })),
      events: this.events
    };
  }
}

module.exports = { MatchQueue };
