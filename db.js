/* ============================================================
 * 象棋弈台 serv00 服务端 - db.js
 * D1 形状适配器: prepare().bind().first()/all()/run() / exec / batch
 * 后端优先 node:sqlite (Node>=22.5), 兜底 better-sqlite3
 * ============================================================ */
'use strict';
const fs = require('fs');
const path = require('path');

function createD1(dbFile) {
  fs.mkdirSync(path.dirname(dbFile), { recursive: true });
  let backend = null;
  let e1 = null, e2 = null;

  /* ---- 尝试 node:sqlite ---- */
  try {
    const { DatabaseSync } = require('node:sqlite');
    const raw = new DatabaseSync(dbFile);
    try { raw.exec('PRAGMA journal_mode = WAL;'); } catch (eP) {}
    try { raw.exec('PRAGMA busy_timeout = 5000;'); } catch (eP2) {}
    backend = {
      kind: 'node:sqlite',
      prepare(sql) {
        const st = raw.prepare(sql);
        return {
          run(...a) { const r = st.run(...a); return { success: true, meta: { changes: Number(r.changes || 0), last_row_id: Number(r.lastInsertRowid || 0) } }; },
          get(...a) { const r = st.get(...a); return r === undefined ? null : r; },
          all(...a) { return st.all(...a); }
        };
      },
      exec(sql) { return raw.exec(sql); },
      close() { try { raw.close(); } catch (e) {} }
    };
  } catch (err1) {
    e1 = err1;
  }

  /* ---- 兜底 better-sqlite3 ---- */
  if (!backend) {
    try {
      const Database = require('better-sqlite3');
      const raw = new Database(dbFile);
      try { raw.pragma('journal_mode = WAL'); } catch (eP) {}
      backend = {
        kind: 'better-sqlite3',
        prepare(sql) {
          const st = raw.prepare(sql);
          return {
            run(...a) { const r = st.run(...a); return { success: true, meta: { changes: Number(r.changes || 0), last_row_id: Number(r.lastInsertRowid || 0) } }; },
            get(...a) { const r = st.get(...a); return r === undefined ? null : r; },
            all(...a) { return st.all(...a); }
          };
        },
        exec(sql) { return raw.exec(sql); },
        close() { try { raw.close(); } catch (e) {} }
      };
    } catch (err2) {
      e2 = err2;
    }
  }

  if (!backend) {
    throw new Error(
      '无可用 SQLite 后端。需要 Node>=22.5 (node:sqlite) 或安装 better-sqlite3。\n' +
      '  node:sqlite 错误: ' + (e1 && e1.message) + '\n' +
      '  better-sqlite3 错误: ' + (e2 && e2.message)
    );
  }

  /* ---- D1 形状封装 ---- */
  const d1 = {
    backend: backend.kind,
    _raw: backend,
    prepare(sql) {
      const s = backend.prepare(sql);
      const stmt = {
        _sql: sql,
        _args: [],
        bind(...args) { stmt._args = args; return stmt; },
        async first(...args) {
          const a = args.length ? args : stmt._args;
          try { return s.get(...a); } catch (e) { throw e; }
        },
        async all(...args) {
          const a = args.length ? args : stmt._args;
          const rows = s.all(...a);
          return { results: rows || [] };
        },
        async run(...args) {
          const a = args.length ? args : stmt._args;
          return s.run(...a);
        }
      };
      return stmt;
    },
    async exec(sql) { return backend.exec(sql); },
    async batch(stmts) {
      const out = [];
      for (const st of stmts) out.push(await st.run());
      return out;
    },
    close() { backend.close(); }
  };
  return d1;
}

module.exports = { createD1 };
