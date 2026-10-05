/* ============================================================
 * 象棋弈台 serv00 服务端 - rooms.js
 * Cloudflare Worker ChessRoom DO 的 1:1 Node 移植
 * 事件: create_room / join_room / make_move / resign / request_draw /
 *       accept_draw / reject_draw / chat / rematch_request / accept_rematch /
 *       request_undo / accept_undo / reject_undo / reconnect_room / leave_room
 * 附加: 心跳(30s ping / 60s 超时) / 房间计时器(1s) / 断线23s判负 /
 *       room_state 存档恢复(1小时) / rooms_registry / Elo 结算
 * ============================================================ */
'use strict';
const {
  rankTitle, applyEloResult, ensureRoomStateTable, ensureRoomsRegistryTable
} = require('./util');
const { WebSocket } = require('./ws');

function newPid() {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

class ChessRoom {
  constructor(roomId, db) {
    this.roomId = roomId;
    this.db = db;
    this.env = { CHESS_DB: db };
    this.ctx = {
      waitUntil: (p) => { Promise.resolve(p).catch(() => {}); }
    };
    this.room = null;
    this.disconnected = {};
    this._cleanupTimer = null;
    this._conns = 0;
  }

  _connectionCount() { return this._conns; }

  async initRoom(roomId) {
    if (!this.room) {
      this.room = {
        id: roomId,
        players: new Map(),
        spectators: new Set(),
        currentTurn: "red",
        gameOver: false,
        winner: null,
        redTime: 900,
        blkTime: 900,
        moveHistory: [],
        capturedRed: [],
        capturedBlack: [],
        playerTokens: null,
        gameStarted: false,
        seatIdentity: {},
        _ratedRecorded: false,
        createdAt: Date.now()
      };
      try {
        if (this.env.CHESS_DB) {
          const saved = await this.env.CHESS_DB.prepare(
            "SELECT state FROM room_state WHERE room_id = ?"
          ).bind(roomId).first();
          if (saved && saved.state) {
            const s = JSON.parse(saved.state);
            if (s && s.createdAt && Date.now() - s.createdAt < 36e5) {
              this.room.currentTurn = s.currentTurn || "red";
              this.room.gameOver = s.gameOver || false;
              this.room.winner = s.winner || null;
              this.room.redTime = s.redTime != null ? s.redTime : 900;
              this.room.blkTime = s.blkTime != null ? s.blkTime : 900;
              this.room.moveHistory = s.moveHistory || [];
              this.room.capturedRed = s.capturedRed || [];
              this.room.capturedBlack = s.capturedBlack || [];
              this.room.createdAt = s.createdAt;
              this.room.playerTokens = s.playerTokens || null;
              this.room.gameStarted = !!s.gameStarted;
              this.room.seatIdentity = s.seatIdentity || {};
              this.room._ratedRecorded = !!s._ratedRecorded;
              this.room._restoredFromDb = true;
              this.ctx.waitUntil(this._enrichSeatNames().then((ch) => {
                if (ch) {
                  try {
                    this.broadcastRoomState();
                  } catch (e2) {
                  }
                }
              }));
            }
          }
        }
      } catch (e) {
      }
    }
  }

  async _saveRoomState() {
    if (!this.room || !this.env.CHESS_DB) return;
    try {
      await ensureRoomStateTable(this.env.CHESS_DB);
      const state = JSON.stringify({
        currentTurn: this.room.currentTurn,
        gameOver: this.room.gameOver,
        winner: this.room.winner,
        redTime: this.room.redTime,
        blkTime: this.room.blkTime,
        moveHistory: this.room.moveHistory.slice(-200),
        capturedRed: this.room.capturedRed || [],
        capturedBlack: this.room.capturedBlack || [],
        playerTokens: this.room.playerTokens || null,
        gameStarted: !!this.room.gameStarted,
        seatIdentity: this.room.seatIdentity || {},
        _ratedRecorded: !!this.room._ratedRecorded,
        createdAt: this.room.createdAt
      });
      await this.env.CHESS_DB.prepare(
        "INSERT OR REPLACE INTO room_state (room_id, state, updated_at) VALUES (?, ?, ?)"
      ).bind(this.room.id, state, Date.now()).run();
    } catch (e) {
    }
  }

  handleRoomWebSocket(ws) {
    this._conns++;
    const socketData = { color: null, spectator: false, replaced: false };
    ws._socketData = socketData;
    ws._lastSeen = Date.now();
    let heartbeatTimer = null;
    let heartbeatTimeout = null;
    const startHeartbeat = () => {
      stopHeartbeat();
      heartbeatTimer = setInterval(() => {
        if (heartbeatTimeout) {
          clearTimeout(heartbeatTimeout);
          heartbeatTimeout = null;
        }
        try {
          ws.send(JSON.stringify({ event: "ping" }));
        } catch (e) {
        }
        heartbeatTimeout = setTimeout(() => {
          try {
            ws.close();
          } catch (e) {
          }
        }, 6e4);
      }, 3e4);
    };
    const stopHeartbeat = () => {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
      }
      if (heartbeatTimeout) {
        clearTimeout(heartbeatTimeout);
        heartbeatTimeout = null;
      }
    };
    startHeartbeat();

    ws.onmessage = async (event) => {
      try {
        ws._lastSeen = Date.now();
        const data = JSON.parse(event.data);
        const eventName = data.event || data[0];
        const payload = data.payload || data[1];
        if (eventName === "pong") {
          if (heartbeatTimeout) {
            clearTimeout(heartbeatTimeout);
            heartbeatTimeout = null;
          }
        } else if (eventName === "ping") {
          try {
            ws.send(JSON.stringify({ event: "pong" }));
          } catch (e) {
          }
        } else if (eventName === "create_room") {
          if (this._cleanupTimer) {
            clearTimeout(this._cleanupTimer);
            this._cleanupTimer = null;
          }
          // 清扫僵尸座位：已关闭/半关闭/已被替换的旧连接不再占用颜色，
          // 防止"离开后同房间号重进"产生同色双座位（卡死根源）
          for (const [pws, p] of [...this.room.players]) {
            const st = pws.readyState;
            const rep = pws._socketData && pws._socketData.replaced;
            if (st === 2 || st === 3 || rep) this.room.players.delete(pws);
          }
          let lastColor = null;
          if (payload && typeof payload === "object" && payload.lastColor) {
            lastColor = payload.lastColor;
          }
          if (this.room.players.size > 0) {
            let existingColor = [...this.room.players.values()][0].color;
            let myColor = existingColor === "red" ? "black" : "red";
            // 最终保险：若目标颜色仍被其他存活连接占用（半开僵尸未被清扫），则取反色；双方都被占则拒绝
            const takenByLive = [...this.room.players.entries()].some(([pws2, p2]) => p2.color === myColor && pws2 !== ws && !(pws2._socketData && pws2._socketData.replaced));
            if (takenByLive) myColor = myColor === "red" ? "black" : "red";
            const stillTaken = [...this.room.players.values()].some((p2) => p2.color === myColor);
            if (stillTaken) {
              ws.send(JSON.stringify({ event: "error", data: "房间已满" }));
              return;
            }
            const myPid = newPid();
            if (!this.room.playerTokens) this.room.playerTokens = {};
            this.room.playerTokens[myColor] = myPid;
            this.room.players.set(ws, { id: Math.random().toString(36).slice(2), color: myColor, assignedAt: Date.now() });
            socketData.color = myColor;
            await this._setSeatIdentity(myColor, payload);
            ws.send(JSON.stringify({ event: "room_created", data: { roomId: this.room.id, color: myColor, pid: myPid, seatInfo: this.getRoomState().seatInfo } }));
            this.ctx.waitUntil(this._enrichSeatNames().then(() => this._regUpdate(this.room.players.size >= 2 ? "playing" : "waiting")).catch(() => {}));
            this._regKeepAlive();
          } else {
            let myColor;
            if (lastColor === "red") {
              myColor = "black";
            } else if (lastColor === "black") {
              myColor = "red";
            } else {
              myColor = Math.random() < 0.5 ? "red" : "black";
            }
            const myPid = newPid();
            if (!this.room.playerTokens) this.room.playerTokens = {};
            this.room.playerTokens[myColor] = myPid;
            this.room.players.set(ws, { id: Math.random().toString(36).slice(2), color: myColor, assignedAt: Date.now() });
            socketData.color = myColor;
            await this._setSeatIdentity(myColor, payload);
            ws.send(JSON.stringify({ event: "room_created", data: { roomId: this.room.id, color: myColor, pid: myPid, seatInfo: this.getRoomState().seatInfo } }));
            this.ctx.waitUntil(this._enrichSeatNames().then(() => this._regUpdate(this.room.players.size >= 2 ? "playing" : "waiting")).catch(() => {}));
            this._regKeepAlive();
          }
        } else if (eventName === "join_room") {
          if (this._cleanupTimer) {
            clearTimeout(this._cleanupTimer);
            this._cleanupTimer = null;
          }
          // 同上：先清扫僵尸座位再判断房间是否满员
          for (const [pws, p] of [...this.room.players]) {
            const st = pws.readyState;
            const rep = pws._socketData && pws._socketData.replaced;
            if (st === 2 || st === 3 || rep) this.room.players.delete(pws);
          }
          if (this.room.players.size === 0) {
            if (!(payload && payload.via === "match")) {
              try { ws.send(JSON.stringify({ event: "room_error", data: { msg: "房间不存在或已解散" } })); } catch (eRoomErr) {}
              return;
            }
            // 匹配出来的房间没有"创建者"：第一个进入者直接入座（快速匹配场景）
            const firstPid = newPid();
            const firstColor = "red";
            if (!this.room.playerTokens) this.room.playerTokens = {};
            this.room.playerTokens[firstColor] = firstPid;
            this.room.players.set(ws, { id: Math.random().toString(36).slice(2), color: firstColor, assignedAt: Date.now() });
            socketData.color = firstColor;
            await this._setSeatIdentity(firstColor, payload);
            try {
              ws.send(JSON.stringify({ event: "room_created", data: { roomId: this.room.id, color: firstColor, pid: firstPid, seatInfo: this.getRoomState().seatInfo } }));
            } catch (e) {
            }
            this.broadcastRoomState();
            this.ctx.waitUntil(this._enrichSeatNames().then(() => this._regUpdate("waiting")).catch(() => {}));
            this._regKeepAlive();
            return;
          }
          if (this.room.players.size >= 2) {
            this.room.spectators.add(ws);
            socketData.spectator = true;
            ws.send(JSON.stringify({ event: "spectator_joined", data: { roomId: this.room.id, moveHistory: this.room.moveHistory } }));
            this.ctx.waitUntil(this._regTouch());
            return;
          }
          let color = [...this.room.players.values()][0].color === "red" ? "black" : "red";
          const joinPid = newPid();
          if (!this.room.playerTokens) this.room.playerTokens = {};
          this.room.playerTokens[color] = joinPid;
          this.room.players.set(ws, { id: Math.random().toString(36).slice(2), color, assignedAt: Date.now() });
          socketData.color = color;
          await this._setSeatIdentity(color, payload);
          if (this.room.players.size >= 2) this.room.gameStarted = true;
          this.ctx.waitUntil(this._enrichSeatNames().then((ch) => {
            if (ch) {
              try {
                this.broadcastRoomState();
              } catch (e2) {
              }
            }
          }));
          ws.send(JSON.stringify({ event: "room_joined", data: { roomId: this.room.id, color, pid: joinPid, seatInfo: this.getRoomState().seatInfo } }));
          this.broadcastRoomState();
          this.broadcastToPlayers(JSON.stringify({ event: "game_start", data: { currentTurn: this.room.currentTurn, seatInfo: this.getRoomState().seatInfo } }));
          this.startRoomTimer();
          this.ctx.waitUntil(this._enrichSeatNames().then(() => this._regUpdate("playing")).catch(() => {}));
        } else if (eventName === "make_move") {
          if (!this.room || this.room.gameOver) {
            ws.send(JSON.stringify({ event: "move_rejected", data: { reason: "invalid_state" } }));
            return;
          }
          if (this.room.players.size < 2 && this.room.moveHistory.length === 0 && !this.room.gameStarted) {
            ws.send(JSON.stringify({ event: "move_rejected", data: { reason: "invalid_state" } }));
            return;
          }
          if (!socketData.color) {
            for (const [pws, player] of this.room.players) {
              if (pws === ws) {
                socketData.color = player.color;
                break;
              }
            }
          }
          if (!socketData.color) {
            ws.send(JSON.stringify({ event: "move_rejected", data: { reason: "no_color" } }));
            return;
          }
          const histLen = this.room.moveHistory.length;
          const lastMv = this.room.moveHistory[histLen - 1];
          if (lastMv && payload && lastMv.fromRow === payload.fromRow && lastMv.fromCol === payload.fromCol && lastMv.toRow === payload.toRow && lastMv.toCol === payload.toCol) {
            try {
              ws.send(JSON.stringify({ event: "move_ack", data: { moveHistoryLen: this.room.moveHistory.length, lastMove: { fromRow: lastMv.fromRow, fromCol: lastMv.fromCol, toRow: lastMv.toRow, toCol: lastMv.toCol }, currentTurn: this.room.currentTurn } }));
            } catch (e) {
            }
            return;
          }
          if (this.room.currentTurn !== socketData.color) {
            ws.send(JSON.stringify({ event: "move_rejected", data: { reason: "not_your_turn" } }));
            return;
          }
          const move = { ...payload, timestamp: Date.now() };
          this.room.moveHistory.push(move);
          if (move.captured) {
            if (!this.room.capturedRed) this.room.capturedRed = [];
            if (!this.room.capturedBlack) this.room.capturedBlack = [];
            if (move.captured.color === "red") this.room.capturedRed.push(move.captured);
            else this.room.capturedBlack.push(move.captured);
          }
          this.room.currentTurn = socketData.color === "red" ? "black" : "red";
          if (move.redLeft !== void 0) this.room.redTime = move.redLeft;
          if (move.blkLeft !== void 0) this.room.blkTime = move.blkLeft;
          if (move.gameOver) {
            this.room.gameOver = true;
            this.room._gameEndedAt = Date.now();
            // 胜者以服务端验证过的走子方座位为准，不信任客户端上报的winner（防止换边后客户端颜色状态错乱导致归属反转）
            this.room.winner = socketData.color;
            if (this.room._timer) {
              clearInterval(this.room._timer);
              this.room._timer = null;
            }
          }
          const opponentMove = { ...move, redLeft: this.room.redTime, blkLeft: this.room.blkTime };
          if (move.gameOver) this.ctx.waitUntil(this._recordGameEnd("checkmate"));
          const ackData = { moveHistoryLen: this.room.moveHistory.length, lastMove: { fromRow: move.fromRow, fromCol: move.fromCol, toRow: move.toRow, toCol: move.toCol }, currentTurn: this.room.currentTurn };
          try {
            ws.send(JSON.stringify({ event: "move_ack", data: ackData }));
          } catch (e) {
          }
          this.broadcastToOpponent(ws, JSON.stringify({ event: "opponent_move", data: opponentMove }));
          this.broadcastRoomStateToOpponent(ws);
          this.broadcastToSpectators(JSON.stringify({ event: "opponent_move", data: opponentMove }));
          this.ctx.waitUntil(this._saveRoomState());
          this.ctx.waitUntil(this._regTouch());
        } else if (eventName === "resign") {
          if (!this.room) return;
          this.room.gameOver = true;
          this.room._gameEndedAt = Date.now();
          this.room.winner = socketData.color === "red" ? "black" : "red";
          if (this.room._timer) {
            clearInterval(this.room._timer);
            this.room._timer = null;
          }
          this.broadcastToRoom(JSON.stringify({ event: "game_over", data: { winner: this.room.winner, reason: "resign" } }));
          this.ctx.waitUntil(this._recordGameEnd("resign"));
          await this._saveRoomState();
        } else if (eventName === "request_draw") {
          if (!this.room) return;
          this.broadcastToOpponent(ws, JSON.stringify({ event: "draw_requested", data: { from: socketData.color } }));
        } else if (eventName === "accept_draw") {
          if (!this.room) return;
          this.room.gameOver = true;
          this.room._gameEndedAt = Date.now();
          this.room.winner = "draw";
          if (this.room._timer) {
            clearInterval(this.room._timer);
            this.room._timer = null;
          }
          this.broadcastToRoom(JSON.stringify({ event: "game_over", data: { winner: "draw", reason: "draw" } }));
          this.ctx.waitUntil(this._recordGameEnd("draw"));
          await this._saveRoomState();
        } else if (eventName === "reject_draw") {
          if (!this.room) return;
          this.broadcastToOpponent(ws, JSON.stringify({ event: "draw_rejected", data: {} }));
        } else if (eventName === "chat") {
          if (!this.room) return;
          let _cn = null;
          if (socketData.color === "red" || socketData.color === "black") {
            const _si = this.room.seatIdentity && this.room.seatIdentity[socketData.color];
            _cn = (_si && _si.name) || null;
          }
          const msg = { from: socketData.color || "spectator", color: socketData.color, name: _cn, message: payload, timestamp: Date.now() };
          this.broadcastToRoom(JSON.stringify({ event: "chat", data: msg }));
        } else if (eventName === "rematch_request") {
          if (!this.room || this.room.players.size < 2) return;
          this.broadcastToOpponent(ws, JSON.stringify({ event: "rematch_requested", data: {} }));
        } else if (eventName === "accept_rematch") {
          if (!this.room || this.room.players.size < 2) return;
          const playerEntries = [...this.room.players.entries()];
          if (playerEntries.length === 2) {
            const [wsA, dataA] = playerEntries[0];
            const [wsB, dataB] = playerEntries[1];
            const tmpColor = dataA.color;
            dataA.color = dataB.color;
            dataB.color = tmpColor;
            if (wsA._socketData) wsA._socketData.color = dataA.color;
            if (wsB._socketData) wsB._socketData.color = dataB.color;
          }
          if (this.room.playerTokens && this.room.playerTokens.red && this.room.playerTokens.black) {
            const tmpTok = this.room.playerTokens.red;
            this.room.playerTokens.red = this.room.playerTokens.black;
            this.room.playerTokens.black = tmpTok;
          }
          if (this.room.seatIdentity) {
            const tmpId = this.room.seatIdentity.red;
            this.room.seatIdentity.red = this.room.seatIdentity.black;
            this.room.seatIdentity.black = tmpId;
          }
          this.room._ratedRecorded = false;
          this.room.gameOver = false;
          this.room._gameEndedAt = null;
          this.room.winner = null;
          this.room.currentTurn = "red";
          this.room.redTime = 900;
          this.room.blkTime = 900;
          this.room.moveHistory = [];
          this.room.capturedRed = [];
          this.room.capturedBlack = [];
          this.room.createdAt = Date.now();
          this.startRoomTimer();
          this.broadcastToRoom(JSON.stringify({ event: "rematch_start", data: {} }));
          this.broadcastRoomState();
          await this._saveRoomState();
        } else if (eventName === "request_undo") {
          if (!this.room || this.room.moveHistory.length === 0 || this.room.gameOver) return;
          this.broadcastToOpponent(ws, JSON.stringify({ event: "undo_requested", data: {} }));
        } else if (eventName === "accept_undo") {
          if (!this.room || this.room.moveHistory.length === 0) return;
          const lastMove = this.room.moveHistory.pop();
          this.room.gameOver = false;
          this.room.winner = null;
          this.room.currentTurn = lastMove.currentTurn === "red" ? "black" : "red";
          if (lastMove.captured) {
            if (lastMove.captured.color === "red" && this.room.capturedRed && this.room.capturedRed.length > 0) {
              this.room.capturedRed.pop();
            } else if (lastMove.captured.color === "black" && this.room.capturedBlack && this.room.capturedBlack.length > 0) {
              this.room.capturedBlack.pop();
            }
          }
          this.broadcastToOpponent(ws, JSON.stringify({ event: "undo_accepted", data: {} }));
          this.broadcastRoomState();
          await this._saveRoomState();
        } else if (eventName === "reject_undo") {
          if (!this.room) return;
          this.broadcastToOpponent(ws, JSON.stringify({ event: "undo_rejected", data: {} }));
        } else if (eventName === "reconnect_room") {
          // 自愈：房间对象已被销毁（如对手离开重建）时，从存档或全新状态恢复，避免静默失败
          if (!this.room) {
            try { await this.initRoom(this.room && this.room.id ? this.room.id : this.roomId); } catch (e2) {}
          }
          if (!this.room) return;
          try {
            const enriched = await this._enrichSeatNames();
            if (enriched) this.broadcastRoomState();
          } catch (e2b) {
          }
          const otherEntries = [...this.room.players.entries()].filter(([pws]) => pws !== ws);
          const isReplaceable = (c) => otherEntries.some(([pws, p]) => p.color === c && (pws.readyState === WebSocket.CLOSED || pws.readyState === WebSocket.CLOSING || pws.readyState === WebSocket.CONNECTING || this.disconnected && this.disconnected[c]));
          const isFree = (c) => !otherEntries.some(([, p]) => p.color === c);
          if (payload && payload.pid && this.room.playerTokens) {
            const tokColor = this.room.playerTokens.red === payload.pid ? "red" : this.room.playerTokens.black === payload.pid ? "black" : null;
            if (tokColor) {
              payload.color = tokColor;
              // 本人重连：座位身份迁移到当前设备/账号（登录账号后dev与昵称都会变）
              try {
                await this._setSeatIdentity(tokColor, payload);
              } catch (e4) {
              }
            }
          } else if (payload && payload.deviceId && this.room.seatIdentity) {
            // 无pid时按设备钉座位：换边/重开后客户端可能报旧颜色，设备永远对应自己的座位
            const dv = String(payload.deviceId);
            const devColor = this.room.seatIdentity.red && this.room.seatIdentity.red.dev === dv ? "red" : this.room.seatIdentity.black && this.room.seatIdentity.black.dev === dv ? "black" : null;
            if (devColor) {
              payload.color = devColor;
              try {
                await this._setSeatIdentity(devColor, payload);
              } catch (e4) {
              }
            }
          }
          let color = payload.color;
          if (color) {
            if (isFree(color)) {
            } else {
              const sameColorEntry = otherEntries.find(([, p]) => p.color === color);
              if (sameColorEntry) {
                // 同色座位占用裁决：
                //  允许接管 = pid匹配(本人) | 持有者已死/被替换 | 持有者闲置>25s(僵尸) | 座位早于该色离开标记(旧主人回归)
                //  其余（离开重建后新客人合法占座）→ 不抢，改反色或拒绝
                const [hws, seat] = sameColorEntry;
                const pidMatches = !!(payload.pid && this.room.playerTokens && this.room.playerTokens[color] === payload.pid);
                const holderDead = hws.readyState !== 1 || (hws._socketData && hws._socketData.replaced);
                const holderIdle = !hws._lastSeen || Date.now() - hws._lastSeen > 25e3;
                const preDiscSeat = !!(this.disconnected && this.disconnected[color] && seat.assignedAt && this.disconnected[color] > seat.assignedAt);
                const devPinned = !!(payload.deviceId && this.room.seatIdentity && this.room.seatIdentity[color] && this.room.seatIdentity[color].dev != null && String(this.room.seatIdentity[color].dev) === String(payload.deviceId));
                if (!pidMatches && !devPinned && !holderDead && !holderIdle && !preDiscSeat) {
                  const alt2 = color === "red" ? "black" : "red";
                  if (isFree(alt2) || isReplaceable(alt2)) {
                    color = alt2;
                  } else {
                    ws.send(JSON.stringify({ event: "error", data: "房间已满，无法重连" }));
                    try { ws.close(); } catch (e2) {}
                    return;
                  }
                }
              } else {
                const alt = color === "red" ? "black" : "red";
                if (isFree(alt) || isReplaceable(alt)) {
                  color = alt;
                } else {
                  ws.send(JSON.stringify({ event: "error", data: "房间已满，无法重连" }));
                  try {
                    ws.close();
                  } catch (e) {
                  }
                  return;
                }
              }
            }
          }
          if (!color) {
            const existingColors = [...this.room.players.values()].map((p) => p.color);
            if (existingColors.includes("red")) color = "black";
            else if (existingColors.includes("black")) color = "red";
            else if (this.disconnected.red) color = "red";
            else if (this.disconnected.black) color = "black";
          }
          if (!color) {
            ws.send(JSON.stringify({ event: "error", data: "无法重连" }));
            return;
          }
          if (this.disconnected[color]) delete this.disconnected[color];
          if (this.room._disconnectTimer) {
            clearTimeout(this.room._disconnectTimer);
            this.room._disconnectTimer = null;
          }
          // 身份自愈：若来者 pid 与该座位令牌不符（换先后本地过期），接管座位时轮换令牌，
          // 并在 room_state 中下发新 pid（附加字段，旧前端忽略，无兼容性影响）
          let rotatedPid = null;
          const pidMatches2 = !!(payload.pid && this.room.playerTokens && this.room.playerTokens[color] === payload.pid);
          if (!pidMatches2) {
            rotatedPid = newPid();
            if (!this.room.playerTokens) this.room.playerTokens = {};
            this.room.playerTokens[color] = rotatedPid;
          }
          for (const [pws, player] of this.room.players) {
            if (player.color === color && pws !== ws) {
              if (pws._socketData) pws._socketData.replaced = true;
              this.room.players.delete(pws);
              break;
            }
          }
          this.room.players.set(ws, { id: Math.random().toString(36).slice(2), color, assignedAt: Date.now() });
          socketData.color = color;
          await this._setSeatIdentity(color, payload);
          if (this.room.players.size >= 2) this.room.gameStarted = true;
          this.ctx.waitUntil(this._enrichSeatNames().then((ch) => {
            if (ch) {
              try {
                this.broadcastRoomState();
              } catch (e2) {
              }
            }
          }));
          const gameInProgress = !this.room.gameOver && this.room.moveHistory.length > 0;
          try {
            ws.send(JSON.stringify({ event: "room_state", data: {
              roomId: this.room.id,
              color,
              moveHistory: this.room.moveHistory,
              currentTurn: this.room.currentTurn,
              gameOver: this.room.gameOver,
              winner: this.room.winner,
              redTime: this.room.redTime,
              blkTime: this.room.blkTime,
              capturedRed: this.room.capturedRed,
              capturedBlack: this.room.capturedBlack,
              gameStarted: true,
              pid: rotatedPid || payload.pid || undefined,
              seatInfo: this.getRoomState().seatInfo
            } }));
          } catch (e) {
          }
          this.broadcastRoomState();
          if (!this.room.gameOver && this.room.players.size >= 2) {
            this.startRoomTimer();
          }
          this.broadcastToOpponent(ws, JSON.stringify({ event: "player_reconnected", data: { color } }));
        } else if (eventName === "leave_room") {
          if (!this.room) return;
          if (socketData.spectator) {
            this.room.spectators.delete(ws);
            return;
          }
          // 按返回按钮退出: 房间立即销毁
          // 计分规则: 棋已开下(非初始盘面)且未分胜负 → 离开者直接判负扣分; 初始盘面或已有输赢 → 不扣分
          const roomLeaving = this.room;
          const initPosition = !roomLeaving.moveHistory || roomLeaving.moveHistory.length === 0;
          let judgedWinner = null;
          if (roomLeaving.gameStarted && !roomLeaving.gameOver && socketData.color && !roomLeaving._ratedRecorded && !initPosition) {
            roomLeaving.gameOver = true;
            roomLeaving._gameEndedAt = Date.now();
            roomLeaving.winner = socketData.color === "red" ? "black" : "red";
            judgedWinner = roomLeaving.winner;
            if (roomLeaving._timer) {
              clearInterval(roomLeaving._timer);
              roomLeaving._timer = null;
            }
            if (roomLeaving._disconnectTimer) {
              clearTimeout(roomLeaving._disconnectTimer);
              roomLeaving._disconnectTimer = null;
            }
            this.ctx.waitUntil(this._recordGameEnd("opponent_left"));
          }
          // 立即销毁: 通知留下的一方(先发结算再发room_closed) → 关闭全部连接 → 删除存档行
          const room = roomLeaving;
          this.ctx.waitUntil(this._regUpdate(null));
          this.room = null;
          if (this._regKA) {
            clearInterval(this._regKA);
            this._regKA = null;
          }
          if (room._timer) {
            clearInterval(room._timer);
            room._timer = null;
          }
          if (room._disconnectTimer) {
            clearTimeout(room._disconnectTimer);
            room._disconnectTimer = null;
          }
          if (this._cleanupTimer) {
            clearTimeout(this._cleanupTimer);
            this._cleanupTimer = null;
          }
          this.disconnected = {};
          room.players.delete(ws);
          for (const [pws] of room.players) {
            try {
              if (judgedWinner) {
                pws.send(JSON.stringify({ event: "game_over", data: { winner: judgedWinner, reason: "opponent_left" } }));
              }
              pws.send(JSON.stringify({ event: "room_closed", data: { reason: "opponent_left" } }));
            } catch (e) {
            }
          }
          room.players.forEach((p, pws) => {
            try {
              pws.close();
            } catch (e) {
            }
          });
          room.spectators.forEach((s) => {
            try {
              s.send(JSON.stringify({ event: "room_closed", data: { reason: "opponent_left" } }));
            } catch (e) {
            }
          });
          room.spectators.forEach((s) => {
            try {
              s.close();
            } catch (e) {
            }
          });
          try {
            ws.close();
          } catch (e) {
          }
          try {
            if (this.env.CHESS_DB) this.env.CHESS_DB.prepare("DELETE FROM room_state WHERE room_id = ?").bind(room.id).run();
          } catch (e) {
          }
        }
      } catch (e) {
        console.error("Room WebSocket message error:", e && e.message || e);
      }
    };

    ws.onclose = () => {
      this._conns = Math.max(0, this._conns - 1);
      stopHeartbeat();
      if (socketData.spectator) {
        if (this.room) this.room.spectators.delete(ws);
        return;
      }
      if (!this.room || !socketData.color) return;
      if (socketData.replaced) return;
      this.room.players.delete(ws);
      if (this.room._timer) {
        clearInterval(this.room._timer);
        this.room._timer = null;
      }
      this.disconnected[socketData.color] = Date.now();
      this.broadcastToOpponent(ws, JSON.stringify({ event: "player_disconnected", data: { color: socketData.color } }));
      this._saveRoomState();
      if (!this.room._disconnectTimer) {
        this.room._disconnectTimer = setTimeout(() => {
          if (!this.room || this.room.gameOver) return;
          const now = Date.now();
          for (const color of ["red", "black"]) {
            if (this.disconnected[color] && now - this.disconnected[color] > 20e3) {
              this.room.gameOver = true;
              this.room._gameEndedAt = now;
              this.room.winner = color === "red" ? "black" : "red";
              this.broadcastToRoom(JSON.stringify({ event: "game_over", data: { winner: this.room.winner, reason: "disconnect_timeout" } }));
              this.broadcastToRoom(JSON.stringify({ event: "room_timeout", data: {} }));
              this.ctx.waitUntil(this._recordGameEnd("disconnect_timeout"));
              if (this.room._timer) {
                clearInterval(this.room._timer);
                this.room._timer = null;
              }
              this._saveRoomState();
              break;
            }
          }
          if (this.room) this.room._disconnectTimer = null;
        }, 23e3);
      }
    };

    ws.onerror = () => {
      stopHeartbeat();
    };
  }

  broadcastToRoom(msg) {
    if (!this.room) return;
    for (const ws of this.room.players.keys()) {
      try {
        ws.send(msg);
      } catch (e) {
      }
    }
    for (const ws of this.room.spectators) {
      try {
        ws.send(msg);
      } catch (e) {
      }
    }
  }

  broadcastRoomState() {
    if (!this.room) return;
    const baseState = this.getRoomState();
    for (const [ws, player] of this.room.players) {
      try {
        ws.send(JSON.stringify({ event: "room_state", data: { ...baseState, color: player.color } }));
      } catch (e) {
      }
    }
    for (const ws of this.room.spectators) {
      try {
        ws.send(JSON.stringify({ event: "room_state", data: baseState }));
      } catch (e) {
      }
    }
  }

  broadcastToPlayers(msg) {
    if (!this.room) return;
    for (const ws of this.room.players.keys()) {
      try {
        ws.send(msg);
      } catch (e) {
      }
    }
  }

  broadcastToSpectators(msg) {
    if (!this.room) return;
    for (const ws of this.room.spectators) {
      try {
        ws.send(msg);
      } catch (e) {
      }
    }
  }

  broadcastToOpponent(ws, msg) {
    if (!this.room) return;
    for (const [pws, player] of this.room.players) {
      if (pws !== ws) {
        try {
          pws.send(msg);
        } catch (e) {
        }
      }
    }
  }

  broadcastRoomStateToOpponent(ws) {
    if (!this.room) return;
    const baseState = this.getRoomState();
    for (const [pws, player] of this.room.players) {
      if (pws !== ws) {
        try {
          pws.send(JSON.stringify({ event: "room_state", data: { ...baseState, color: player.color } }));
        } catch (e) {
        }
      }
    }
  }

  getRoomState() {
    const si = this.room.seatIdentity || {};
    return {
      roomId: this.room.id,
      playerCount: this.room.players.size,
      currentTurn: this.room.currentTurn,
      gameOver: this.room.gameOver,
      winner: this.room.winner,
      moveHistory: this.room.moveHistory,
      redTime: this.room.redTime,
      blkTime: this.room.blkTime,
      capturedRed: this.room.capturedRed,
      capturedBlack: this.room.capturedBlack,
      gameStarted: this.room.gameStarted || this.room.players.size >= 2,
      seatInfo: {
        red: si.red ? { name: si.red.name || (si.red.dev ? "游客" : null), elo: si.red.elo || null, title: si.red.elo ? rankTitle(si.red.elo) : null } : null,
        black: si.black ? { name: si.black.name || (si.black.dev ? "游客" : null), elo: si.black.elo || null, title: si.black.elo ? rankTitle(si.black.elo) : null } : null
      }
    };
  }

  async _regUpdate(status) {
    if (!this.env || !this.env.CHESS_DB) return;
    const rid = this.room ? this.room.id : null;
    if (!rid) return;
    // 快照必须在首个 await 之前同步取好：leave 路径会在 await 期间把 this.room 置空
    const si = this.room.seatIdentity || {};
    const fmt = function (s) { if (!s) return null; var n = (s.name || "游客"); var t = (typeof s.elo === "number") ? rankTitle(s.elo) : null; return t ? (n + " " + t) : n; };
    const rn = fmt(si.red), bn = fmt(si.black);
    const watchers = this.room.spectators ? this.room.spectators.size : 0;
    try {
      await ensureRoomsRegistryTable(this.env.CHESS_DB);
      if (status === null) {
        await this.env.CHESS_DB.prepare("DELETE FROM rooms_registry WHERE id = ?").bind(rid).run();
      } else {
        await this.env.CHESS_DB.prepare("INSERT OR REPLACE INTO rooms_registry (id, red, black, status, watchers, updated_at) VALUES (?, ?, ?, ?, ?, ?)").bind(rid, rn, bn, status, watchers, Date.now()).run();
      }
    } catch (e) {
    }
  }

  async _regTouch() {
    if (!this.env || !this.env.CHESS_DB || !this.room) return;
    try {
      await this.env.CHESS_DB.prepare("UPDATE rooms_registry SET watchers = ?, updated_at = ? WHERE id = ?").bind(this.room.spectators ? this.room.spectators.size : 0, Date.now(), this.room.id).run();
    } catch (e) {
    }
  }

  // 房间保活: 只要房间存在(哪怕还没人加入), 每分钟刷新 registry 的 updated_at, 避免被 /api/rooms 的120秒窗口过滤掉
  _regKeepAlive() {
    if (!this.env || !this.env.CHESS_DB) return;
    if (this._regKA) return;
    this._regKA = setInterval(() => {
      try {
        if (!this.room) {
          clearInterval(this._regKA);
          this._regKA = null;
          return;
        }
        this.ctx.waitUntil(this._regTouch());
      } catch (e) {
      }
    }, 60000);
  }

  async _enrichSeatNames() {
    const si = this.room && this.room.seatIdentity;
    if (!si || !this.env || !this.env.CHESS_DB) return false;
    let changed = false;
    for (const color of ["red", "black"]) {
      const s = si[color];
      if (s && s.dev && (!s.name || !(typeof s.elo === "number" && s.elo > 0))) {
        try {
          const row = await this.env.CHESS_DB.prepare("SELECT name, elo FROM players WHERE device_id = ?").bind(s.dev).first();
          if (row) {
            if (!s.name && row.name) {
              s.name = String(row.name).slice(0, 24);
              changed = true;
            }
            if (typeof row.elo === "number" && row.elo > 0 && s.elo !== row.elo) {
              s.elo = row.elo;
              changed = true;
            }
          }
        } catch (e) {
        }
      }
    }
    return changed;
  }

  async _setSeatIdentity(color, payload) {
    if (!color || !payload || typeof payload !== "object") return;
    if (!this.room.seatIdentity) this.room.seatIdentity = {};
    const prev = this.room.seatIdentity[color] || {};
    const dev = payload.deviceId ? String(payload.deviceId).slice(0, 64) : prev.dev || null;
    let name = payload.playerName ? String(payload.playerName).slice(0, 24) : prev.name || null;
    // 昵称为空时回查玩家表的真实昵称（该设备/账号历史注册名），避免显示设备尾号
    if (!name && dev && this.env && this.env.CHESS_DB) {
      try {
        const row = await this.env.CHESS_DB.prepare("SELECT name FROM players WHERE device_id = ?").bind(dev).first();
        if (row && row.name) name = String(row.name).slice(0, 24);
      } catch (e) {
      }
    }
    this.room.seatIdentity[color] = {
      dev: dev,
      name: name || null,
      elo: Number.isFinite(payload.elo) ? Math.round(payload.elo) : prev.elo || null
    };
  }

  async _recordGameEnd(reason) {
    if (!this.room || !this.env.CHESS_DB || this.room._ratedRecorded) return;
    const si = this.room.seatIdentity || {};
    const red = si.red;
    const black = si.black;
    if (!red || !black || !red.dev || !black.dev || red.dev === black.dev) return;
    if (!this.room.moveHistory || this.room.moveHistory.length < 2) return;
    const result = this.room.winner === "red" ? "red" : this.room.winner === "black" ? "black" : "draw";
    this.room._ratedRecorded = true;
    const res = await applyEloResult(this.env.CHESS_DB, this.room.id, red, black, result, reason, this.room.moveHistory.length);
    this.ctx.waitUntil(this._regTouch());
    if (res) {
      try {
        this.broadcastToRoom(JSON.stringify({ event: "rating_update", data: {
          red: { name: red.name || null, before: res.red.before, after: res.red.after, title: rankTitle(res.red.after) },
          black: { name: black.name || null, before: res.black.before, after: res.black.after, title: rankTitle(res.black.after) }
        } }));
      } catch (e) {
      }
      this.ctx.waitUntil(this._saveRoomState());
    } else {
      this.room._ratedRecorded = false;
    }
  }

  startRoomTimer() {
    if (!this.room) return;
    if (this.room._timer) clearInterval(this.room._timer);
    this.room._timerLastTick = Date.now();
    this.room._timer = setInterval(() => {
      if (!this.room) return;
      if (this.room.gameOver) {
        clearInterval(this.room._timer);
        this.room._timer = null;
        return;
      }
      // 每秒推送极小的 time_update：持续冲刷发送缓冲，走子消息不再等客户端ping才送达（前端安全忽略未知事件）
      this.broadcastToRoom(JSON.stringify({ event: "time_update", data: { redTime: this.room.redTime, blkTime: this.room.blkTime, currentTurn: this.room.currentTurn } }));
      const now = Date.now();
      const elapsed = Math.max(1, Math.round((now - this.room._timerLastTick) / 1e3));
      this.room._timerLastTick = now;
      if (this.room.currentTurn === "red") {
        this.room.redTime = Math.max(0, this.room.redTime - elapsed);
        if (this.room.redTime <= 0) {
          this.room.gameOver = true;
          this.room.winner = "black";
          this.room._gameEndedAt = Date.now();
          clearInterval(this.room._timer);
          this.room._timer = null;
          this.broadcastToRoom(JSON.stringify({ event: "timeout", data: { winner: "black" } }));
          this.broadcastToRoom(JSON.stringify({ event: "game_over", data: { winner: "black", reason: "timeout" } }));
          this.ctx.waitUntil(this._recordGameEnd("timeout"));
          this._saveRoomState();
        }
      } else {
        this.room.blkTime = Math.max(0, this.room.blkTime - elapsed);
        if (this.room.blkTime <= 0) {
          this.room.gameOver = true;
          this.room.winner = "red";
          this.room._gameEndedAt = Date.now();
          clearInterval(this.room._timer);
          this.room._timer = null;
          this.broadcastToRoom(JSON.stringify({ event: "timeout", data: { winner: "red" } }));
          this.broadcastToRoom(JSON.stringify({ event: "game_over", data: { winner: "red", reason: "timeout" } }));
          this.ctx.waitUntil(this._recordGameEnd("timeout"));
          this._saveRoomState();
        }
      }
    }, 1e3);
  }
}

/* ===== 房间注册表（对齐 DO 语义: 无人且已销毁的实例可被新实例替换） ===== */
const rooms = new Map();

function getRoom(roomId, db) {
  let r = rooms.get(roomId);
  if (r && !r.room && r._connectionCount() === 0) {
    // 已销毁(leave_room)且无人连接: 丢弃旧实例, 新连接按 DO 行为从 room_state 恢复
    if (r._regKA) { clearInterval(r._regKA); r._regKA = null; }
    rooms.delete(roomId);
    r = null;
  }
  if (!r) {
    r = new ChessRoom(roomId, db);
    rooms.set(roomId, r);
  }
  return r;
}

/* 周期清扫: 防止极端情况下注册表泄漏 */
let _sweep = null;
function startRoomSweep() {
  if (_sweep) return;
  _sweep = setInterval(() => {
    try {
      for (const [id, r] of rooms) {
        if (!r.room && r._connectionCount() === 0) {
          if (r._regKA) { clearInterval(r._regKA); r._regKA = null; }
          rooms.delete(id);
        }
      }
    } catch (e) {
    }
  }, 10 * 60 * 1000);
  if (_sweep.unref) _sweep.unref();
}

/* 房间 WS 入口（对齐 Worker: /ws?roomId=xxx → DO fetch /ws） */
async function attachRoomWebSocket(roomId, ws, db) {
  const room = getRoom(roomId, db);
  await room.initRoom(roomId);
  room.handleRoomWebSocket(ws);
}

module.exports = { ChessRoom, getRoom, attachRoomWebSocket, startRoomSweep, rooms };
