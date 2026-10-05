/* ============================================================
 * 象棋弈台 serv00 服务端 - ws.js
 * RFC6455 WebSocket 服务端实现（零依赖）
 * 暴露与 Worker WebSocketPair 一致的对象形状:
 *   ws.send(str) / ws.close() / ws.readyState(1/2/3)
 *   ws.onmessage({data}) / ws.onclose({}) / ws.onerror({})
 * ============================================================ */
'use strict';
const crypto = require('crypto');

const CONNECTING = 0, OPEN = 1, CLOSING = 2, CLOSED = 3;
const MAX_MSG = 2 * 1024 * 1024; // 单条消息上限 2MB
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

const WebSocket = { CONNECTING, OPEN, CLOSING, CLOSED };

function acceptKey(key) {
  return crypto.createHash('sha1').update(key + GUID).digest('base64');
}

/* 完成 HTTP Upgrade 握手，返回 WSConnection；失败返回 null */
function accept(req, socket) {
  const key = req.headers['sec-websocket-key'];
  const upgrade = String(req.headers.upgrade || '').toLowerCase();
  if (!key || upgrade !== 'websocket') {
    try { socket.destroy(); } catch (e) {}
    return null;
  }
  try {
    socket.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      'Sec-WebSocket-Accept: ' + acceptKey(key) + '\r\n' +
      '\r\n'
    );
  } catch (e) {
    try { socket.destroy(); } catch (e2) {}
    return null;
  }
  return new WSConnection(socket);
}

class WSConnection {
  constructor(socket) {
    this._socket = socket;
    this.readyState = OPEN;
    this._onmessage = null;
    this._pendingMsgs = []; // 处理函数就绪前到达的消息缓冲（对齐 Worker WebSocketPair 排队行为）
    this.onclose = null;
    this.onerror = null;
    this._buf = Buffer.alloc(0);
    this._fragments = [];
    this._fragOpcode = 0;
    this._closedSent = false;
    this._closeEmitted = false;
    this._destroyTimer = null;
    try { socket.setNoDelay(true); } catch (e) {}
    socket.on('data', (chunk) => {
      try {
        this._buf = this._buf.length ? Buffer.concat([this._buf, chunk]) : chunk;
        this._parse();
      } catch (e) {
        this._fail();
      }
    });
    socket.on('error', () => { try { if (this.onerror) this.onerror({}); } catch (e) {} this._fail(); });
    socket.on('close', () => this._emitClose());
    socket.on('end', () => { try { socket.end(); } catch (e) {} });
  }

  get onmessage() { return this._onmessage; }
  set onmessage(fn) {
    this._onmessage = fn;
    if (fn && this._pendingMsgs.length) {
      const pend = this._pendingMsgs;
      this._pendingMsgs = [];
      for (const ev of pend) {
        try { fn(ev); } catch (e) {}
      }
    }
  }

  _onData(chunk) {
    this._buf = this._buf.length ? Buffer.concat([this._buf, chunk]) : chunk;
    this._parse();
  }

  _parse() {
    for (;;) {
      const buf = this._buf;
      if (buf.length < 2) return;
      const b0 = buf[0], b1 = buf[1];
      const fin = (b0 & 0x80) !== 0;
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let off = 2;
      if (len === 126) {
        if (buf.length < 4) return;
        len = buf.readUInt16BE(2); off = 4;
      } else if (len === 127) {
        if (buf.length < 10) return;
        const big = buf.readBigUInt64BE(2);
        if (big > BigInt(MAX_MSG)) { this._fail(); return; }
        len = Number(big); off = 10;
      }
      if (len > MAX_MSG) { this._fail(); return; }
      const maskLen = masked ? 4 : 0;
      if (buf.length < off + maskLen + len) return;
      let payload = buf.slice(off + maskLen, off + maskLen + len);
      if (masked && maskLen) {
        const mask = buf.slice(off, off + 4);
        for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      }
      this._buf = buf.slice(off + maskLen + len);

      if (opcode === 0x8) { // close
        if (this.readyState === OPEN) this.readyState = CLOSING;
        if (!this._closedSent) {
          this._closedSent = true;
          this._sendFrame(0x8, payload.slice(0, 2));
        }
        try { this._socket.destroy(); } catch (e) {}
        this._emitClose();
        return;
      } else if (opcode === 0x9) { // ping -> pong
        this._sendFrame(0xA, payload);
      } else if (opcode === 0xA) { // pong: 应用层自行处理，忽略
      } else if (opcode === 0x1 || opcode === 0x2 || opcode === 0x0) {
        if (opcode !== 0x0) {
          this._fragOpcode = opcode;
          this._fragments = [payload];
        } else {
          this._fragments.push(payload);
        }
        if (fin) {
          const data = Buffer.concat(this._fragments);
          const op = this._fragOpcode;
          this._fragments = [];
          this._fragOpcode = 0;
          if (op === 0x1) {
            const ev = { data: data.toString('utf8') };
            if (this._onmessage) {
              try { this._onmessage(ev); } catch (e) {}
            } else if (this._pendingMsgs.length < 64) {
              // 处理函数尚未就绪: 排队, onmessage 赋值时冲刷
              this._pendingMsgs.push(ev);
            }
          }
          // 二进制消息：本应用未使用，忽略
        }
      } else {
        // 未知 opcode: 忽略
      }
    }
  }

  _sendFrame(opcode, payload) {
    const len = payload.length;
    let header;
    if (len < 126) {
      header = Buffer.from([0x80 | opcode, len]);
    } else if (len < 65536) {
      header = Buffer.alloc(4);
      header[0] = 0x80 | opcode; header[1] = 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.alloc(10);
      header[0] = 0x80 | opcode; header[1] = 127;
      header.writeBigUInt64BE(BigInt(len), 2);
    }
    try {
      this._socket.write(Buffer.concat([header, payload]));
      return true;
    } catch (e) {
      return false;
    }
  }

  send(str) {
    if (this.readyState !== OPEN) return false;
    return this._sendFrame(0x1, Buffer.from(String(str), 'utf8'));
  }

  close() {
    if (this.readyState === CLOSING || this.readyState === CLOSED) return;
    this.readyState = CLOSING;
    if (!this._closedSent) {
      this._closedSent = true;
      this._sendFrame(0x8, Buffer.from([0x03, 0xe8])); // 1000
    }
    try { this._socket.end(); } catch (e) {}
    this._destroyTimer = setTimeout(() => {
      try { this._socket.destroy(); } catch (e) {}
    }, 5000);
    if (this._destroyTimer.unref) this._destroyTimer.unref();
  }

  _fail() {
    try { this._socket.destroy(); } catch (e) {}
    this._emitClose();
  }

  _emitClose() {
    if (this._closeEmitted) return;
    this._closeEmitted = true;
    this.readyState = CLOSED;
    if (this._destroyTimer) { clearTimeout(this._destroyTimer); this._destroyTimer = null; }
    try { if (this.onclose) this.onclose({}); } catch (e) {}
  }
}

module.exports = { accept, WebSocket, WSConnection, acceptKey };
