/* ============================================================
 * 象棋弈台 serv00 服务端 - lobby.js
 * Cloudflare Worker 大厅 WebSocket（handleWebSocket）1:1 移植
 * 事件: presence / create_room(会员门禁+redirect_room) / ping(pong+30s节流心跳)
 *       join_room / reconnect_room → redirect_room
 * 连接即注册 presence(join)，断开 presence(leave)，online_count 广播
 * ============================================================ */
'use strict';
const { generateFreeRoomId, newPid } = require('./util');

const activeConnections = new Set();
let lastHbAt = 0;

function broadcastOnlineCount(env) {
  const msg = JSON.stringify({ event: "online_count", data: env.onlineCount });
  for (const ws of activeConnections) {
    try {
      ws.send(msg);
    } catch (e) {
    }
  }
}

async function handleLobbyWebSocket(ws, env) {
  activeConnections.add(ws);
  ws._connId = newPid();
  ws._lastSeen = Date.now();
  const pT0 = await env.MATCH_QUEUE.presence({ join: ws._connId });
  if (pT0 && typeof pT0.online === "number") env.onlineCount = pT0.online;
  try {
    ws.send(JSON.stringify({ event: "online_count", data: env.onlineCount }));
  } catch (e) {
  }
  broadcastOnlineCount(env);

  ws.onmessage = async (event) => {
    try {
      const data = JSON.parse(event.data);
      const eventName = data.event || data[0];
      const payload = data.payload || data[1];
      if (eventName === "presence") {
        const dev = payload && typeof payload.deviceId === "string" ? payload.deviceId : null;
        if (dev && ws._connId && !ws._deviceId) {
          ws._deviceId = dev;
          const pT1 = await env.MATCH_QUEUE.presence({ set: ws._connId, deviceId: dev });
          if (pT1 && typeof pT1.online === "number") env.onlineCount = pT1.online;
          broadcastOnlineCount(env);
        }
      } else if (eventName === "create_room") {
        let lastColor = null;
        let requestedRoomId = null;
        if (payload && typeof payload === "object") {
          requestedRoomId = payload.roomId;
          lastColor = payload.lastColor;
        } else {
          requestedRoomId = payload;
        }
        let _vipOk = false;
        try {
          const cands = [];
          if (payload && typeof payload.vipId === "string" && payload.vipId) cands.push(String(payload.vipId).slice(0, 64));
          const _did = (payload && typeof payload.deviceId === "string" && payload.deviceId) ? String(payload.deviceId).slice(0, 64) : (ws._deviceId || null);
          if (_did) cands.push(_did);
          const uniq = [];
          for (var ci = 0; ci < cands.length; ci++) if (uniq.indexOf(cands[ci]) < 0) uniq.push(cands[ci]);
          let _vipRow = null;
          if (uniq.length) {
            const _q = "SELECT id FROM vip_members WHERE id IN (" + uniq.map(function (_, i2) { return "?"; }).join(",") + ") AND expires_at > ?";
            _vipRow = await env.CHESS_DB.prepare(_q).bind(...uniq, Date.now()).first();
          }
          _vipOk = !!_vipRow;
        } catch (eVip2) {
          _vipOk = true;
          console.error("[gate] ERR", eVip2 && eVip2.message);
        }
        if (_vipOk) {
          const roomId = requestedRoomId || (await generateFreeRoomId(env.CHESS_DB));
          ws.send(JSON.stringify({ event: "redirect_room", data: { roomId, action: "create", lastColor } }));
        } else {
          try { ws.send(JSON.stringify({ event: "vip_only_create", data: { msg: "只有会员才能创建房间，可使用快速匹配" } })); } catch (eVip3) {}
        }
      } else if (eventName === "ping") {
        ws._lastSeen = Date.now();
        try {
          ws.send(JSON.stringify({ event: "pong" }));
        } catch (e) {
        }
        if (Date.now() - (lastHbAt || 0) > 30000) {
          lastHbAt = Date.now();
          const hbIds = [];
          for (const w of activeConnections) { if (w._connId) hbIds.push({ id: w._connId, dev: w._deviceId || null }); }
          if (hbIds.length) {
            env.MATCH_QUEUE.presence({ heartbeat: hbIds }).then(function (pT4) {
              if (pT4 && typeof pT4.online === "number") {
                env.onlineCount = pT4.online;
                broadcastOnlineCount(env);
              }
            }).catch(function () {});
          }
        }
      } else if (eventName === "join_room") {
        const roomId = payload && typeof payload === "object" ? payload.roomId : payload;
        ws.send(JSON.stringify({ event: "redirect_room", data: { roomId, action: "join" } }));
      } else if (eventName === "reconnect_room") {
        const roomId = payload.roomId;
        if (!roomId) return;
        ws.send(JSON.stringify({ event: "redirect_room", data: { roomId, action: "reconnect", color: payload.color } }));
      }
    } catch (e) {
      console.error("WebSocket message error:", e && e.message || e);
    }
  };

  const lobbyDetach = () => {
    if (activeConnections.delete(ws)) {
      env.MATCH_QUEUE.presence({ leave: ws._connId }).then(function (pT2) {
        if (pT2 && typeof pT2.online === "number") {
          env.onlineCount = pT2.online;
          broadcastOnlineCount(env);
        }
      }).catch(function () {});
    }
  };
  ws.onclose = lobbyDetach;
  ws.onerror = lobbyDetach;
}

module.exports = { handleLobbyWebSocket, activeConnections };
