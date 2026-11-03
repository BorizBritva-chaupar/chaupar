// Чаупар — онлайн-сервер.
// Без сторонних библиотек: только встроенные модули Node.js (18+).
// Раздаёт игру из папки public и держит комнаты для игры по сети через WebSocket (/ws).
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, 'public');
const HAS_PUBLIC = fs.existsSync(path.join(PUBLIC, 'index.html'));
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.json': 'application/json', '.webmanifest': 'application/manifest+json', '.png': 'image/png',
  '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.txt': 'text/plain; charset=utf-8'
};

// ---------------- static files ----------------
const server = http.createServer((req, res) => {
  let url = decodeURIComponent((req.url || '/').split('?')[0]);
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (url === '/') url = '/index.html';
  let file;
  if (HAS_PUBLIC) { file = path.normalize(path.join(PUBLIC, url)); if (!file.startsWith(PUBLIC)) { res.writeHead(403); return res.end(); } }
  else { // all files uploaded side by side (no folders): serve only the game's own files
    const name = path.basename(url), ext = path.extname(name).toLowerCase();
    if (!(['.html', '.png', '.webmanifest', '.ico', '.svg'].includes(ext) || name === 'sw.js')) { res.writeHead(404); return res.end('Not found'); }
    file = path.join(__dirname, name);
  }
  fs.readFile(file, (err, data) => {
    if (err) { // unknown paths fall back to the game page
      return fs.readFile(path.join(HAS_PUBLIC ? PUBLIC : __dirname, 'index.html'), (e2, html) => {
        if (e2) { res.writeHead(404); return res.end('Not found'); }
        res.writeHead(200, { 'Content-Type': MIME['.html'], 'Cache-Control': 'no-cache' }); res.end(html);
      });
    }
    const ext = path.extname(file).toLowerCase();
    const cache = (ext === '.html' || url === '/sw.js' || ext === '.webmanifest') ? 'no-cache' : 'public, max-age=86400';
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream', 'Cache-Control': cache });
    res.end(data);
  });
});

// ---------------- minimal WebSocket (RFC 6455) ----------------
const clients = new Set();
server.on('upgrade', (req, socket) => {
  if ((req.url || '').split('?')[0] !== '/ws' || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { socket, buf: Buffer.alloc(0), frags: [], alive: true, id: null, room: null };
  clients.add(c);
  socket.on('data', d => { c.buf = Buffer.concat([c.buf, d]); if (c.buf.length > 2e6) return kill(c); parse(c); });
  socket.on('close', () => gone(c));
  socket.on('error', () => gone(c));
});

function parse(c) {
  while (c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1], fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
    if (len > 1e6) return kill(c);
    const need = off + (masked ? 4 : 0) + len;
    if (c.buf.length < need) return;
    let data = c.buf.subarray(off + (masked ? 4 : 0), need);
    if (masked) { const m = c.buf.subarray(off, off + 4); data = Buffer.from(data); for (let i = 0; i < data.length; i++) data[i] ^= m[i & 3]; }
    c.buf = c.buf.subarray(need);
    if (op === 0x8) { send(c, null, 0x8); return kill(c); }          // close
    if (op === 0x9) { send(c, data, 0xA); continue; }                // ping -> pong
    if (op === 0xA) { c.alive = true; continue; }                    // pong
    if (op === 0x1 || op === 0x0) {
      c.frags.push(data);
      if (fin) { const text = Buffer.concat(c.frags).toString('utf8'); c.frags = []; onText(c, text); }
    }
  }
}
function send(c, payload, op = 0x1) {
  if (!c || c.socket.destroyed) return;
  const data = payload == null ? Buffer.alloc(0) : Buffer.isBuffer(payload) ? payload : Buffer.from(payload, 'utf8');
  let head;
  if (data.length < 126) { head = Buffer.from([0x80 | op, data.length]); }
  else if (data.length < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(data.length, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(data.length), 2); }
  try { c.socket.write(Buffer.concat([head, data])); } catch (e) { }
}
const msg = (c, obj) => send(c, JSON.stringify(obj));
function kill(c) { try { c.socket.destroy(); } catch (e) { } gone(c); }

// keep connections alive through proxies and drop dead ones
setInterval(() => {
  for (const c of clients) { if (!c.alive) { kill(c); continue; } c.alive = false; send(c, null, 0x9); }
}, 25000);

// ---------------- rooms ----------------
// A room: { code, host, members: Map(id -> {name, conn|null}), state, emptyAt }
const rooms = new Map();
const LETTERS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function newCode() {
  for (let n = 0; n < 1000; n++) {
    let s = ''; for (let i = 0; i < 4; i++) s += LETTERS[crypto.randomInt(LETTERS.length)];
    if (!rooms.has(s)) return s;
  }
  return crypto.randomBytes(3).toString('hex').toUpperCase();
}
const cleanName = n => String(n || 'Игрок').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 18) || 'Игрок';
const cleanId = id => String(id || '').replace(/[^a-z0-9]/gi, '').slice(0, 32);
function members(room) { return [...room.members].map(([id, m]) => ({ id, name: m.name, online: !!m.conn })); }
function toRoom(room, obj, except) { const s = JSON.stringify(obj); for (const [, m] of room.members) if (m.conn && m.conn !== except) send(m.conn, s); }
function shareMembers(room) { toRoom(room, { type: 'members', list: members(room) }); }
function pickHost(room) {
  const h = room.members.get(room.host);
  if (h && h.conn) return;
  for (const [id, m] of room.members) if (m.conn) { room.host = id; toRoom(room, { type: 'host', host: id }); return; }
}

function onText(c, text) {
  let d; try { d = JSON.parse(text); } catch (e) { return; }
  if (!d || typeof d.type !== 'string') return;
  const reply = obj => msg(c, Object.assign({ type: 'ack', rid: d.rid }, obj));
  const room = c.room && rooms.get(c.room);

  if (d.type === 'create' || d.type === 'join') {
    const id = cleanId(d.id); if (!id) return reply({ ok: false, error: 'Нет идентификатора игрока' });
    if (c.room) leave(c);
    let r;
    if (d.type === 'create') {
      if (rooms.size > 5000) return reply({ ok: false, error: 'Сервер переполнен, попробуйте позже' });
      r = { code: newCode(), host: id, members: new Map(), state: null, emptyAt: 0, chat: [] };
      rooms.set(r.code, r);
    } else {
      r = rooms.get(String(d.code || '').toUpperCase().trim());
      if (!r) return reply({ ok: false, error: 'Комната не найдена. Проверьте код.' });
      if (!r.members.has(id) && r.members.size >= 12) return reply({ ok: false, error: 'В комнате уже слишком много людей' });
    }
    const old = r.members.get(id);
    if (old && old.conn && old.conn !== c) { old.conn.room = null; kill(old.conn); }
    r.members.set(id, { name: cleanName(d.name), conn: c });
    c.id = id; c.room = r.code; r.emptyAt = 0;
    pickHost(r);
    reply({ ok: true, code: r.code, host: r.host, members: members(r), state: r.host === id ? null : r.state, hostState: r.host === id ? r.state : null, chat: r.chat || [] });
    shareMembers(r);
    return;
  }
  if (!room) return;
  const me = room.members.get(c.id);
  if (!me || me.conn !== c) return;

  if (d.type === 'event') {                       // only the host decides what happens in the game
    if (room.host !== c.id || !d.ev || typeof d.ev !== 'object') return;
    if (d.ev.t === 'state' && d.ev.S) room.state = d.ev.S;
    else if (d.ev.pre) room.state = d.ev.pre;
    toRoom(room, { type: 'event', ev: d.ev }, c);
    return;
  }
  if (d.type === 'intent') {                      // a guest asks the host to roll or move
    const h = room.members.get(room.host);
    if (h && h.conn && d.it && typeof d.it === 'object') msg(h.conn, { type: 'intent', it: Object.assign({}, d.it, { from: c.id }) });
    return;
  }
  if (d.type === 'name') { me.name = cleanName(d.name); shareMembers(room); return; }
  // room chat and playful throws (snowball, tomato, egg, grenade), lightly rate-limited
  const now = Date.now();
  if (d.type === 'chat') {
    const text = String(d.text || '').replace(/[\u0000-\u001f]/g, ' ').trim().slice(0, 200);
    if (!text) return;
    me.chatT = (me.chatT || []).filter(t => now - t < 10000);
    if (me.chatT.length >= 8) return;
    me.chatT.push(now);
    const m = { id: c.id, name: me.name, text, ts: now };
    room.chat = (room.chat || []).concat(m).slice(-40);
    toRoom(room, Object.assign({ type: 'chat' }, m));
    return;
  }
  if (d.type === 'fx') {
    const kind = ['snow', 'tomato', 'egg', 'grenade'].includes(d.kind) ? d.kind : null;
    const to = Number.isInteger(d.to) && d.to >= 0 && d.to < 4 ? d.to : null;
    if (!kind || to === null) return;
    if (now - (me.fxT || 0) < 1200) return;
    me.fxT = now;
    toRoom(room, { type: 'fx', id: c.id, name: me.name, kind, to });
    return;
  }
  if (d.type === 'leave') { leave(c); return; }
}

function leave(c) {
  const room = c.room && rooms.get(c.room);
  c.room = null;
  if (!room) return;
  room.members.delete(c.id);
  if (room.host === c.id) { room.host = null; pickHost(room); }
  shareMembers(room);
  if (![...room.members.values()].some(m => m.conn)) room.emptyAt = Date.now();
}
function gone(c) {
  if (!clients.has(c)) return;
  clients.delete(c);
  const room = c.room && rooms.get(c.room);
  if (!room) return;
  const m = room.members.get(c.id);
  if (m && m.conn === c) m.conn = null;          // keep the seat: the player may come back
  pickHost(room);
  shareMembers(room);
  if (![...room.members.values()].some(x => x.conn)) room.emptyAt = Date.now();
}
// rooms nobody is in are removed after 2 hours
setInterval(() => { const now = Date.now(); for (const [code, r] of rooms) if (r.emptyAt && now - r.emptyAt > 2 * 3600e3) rooms.delete(code); }, 600e3);

server.listen(PORT, () => console.log('Чаупар запущен: http://localhost:' + PORT));
