'use strict';
/* CRICHUB realtime server: static site + WebSocket database + media storage.
   One process, no external database. Data is kept in memory and saved to DATA_DIR/db.json. */
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), zlib = require('zlib');
const EventEmitter = require('events');

const PORT = +process.env.PORT || 3000;
const DATA = process.env.DATA_DIR || path.join(__dirname, 'data');
const PUB = path.join(__dirname, 'public');
const MEDIA = path.join(DATA, 'media');
const DBF = path.join(DATA, 'db.json');
const MAXMEDIA = 5.5e6;
fs.mkdirSync(MEDIA, { recursive: true });

/* ---------------- in-memory store ---------------- */
const docs = new Map();   // "rooms/1234" -> object
const cols = new Map();   // "rooms" -> Map(id -> object)
const parentOf = p => p.slice(0, p.lastIndexOf('/'));
const idOf = p => p.slice(p.lastIndexOf('/') + 1);
const PATH_RE = /^[A-Za-z0-9_.\-]+(\/[A-Za-z0-9_.\-]+)*$/;
const okPath = p => typeof p === 'string' && p.length < 200 && PATH_RE.test(p) && !p.includes('..');
const clone = o => { const c = JSON.parse(JSON.stringify(o)); delete c.__proto__; return c; };
const isObj = o => o && typeof o === 'object' && !Array.isArray(o);
const newId = () => Date.now().toString(36) + crypto.randomBytes(3).toString('hex');
let dirty = 0;
const EPHEMERAL = /\/(bids|chat)\//;
const markDirty = (p, d) => { if (EPHEMERAL.test(p + '/')) return; if (d) { const k = Object.keys(d); if (k.length === 1 && k[0] === 'seen') return; } dirty = 1; };

/* ---------------- subscriptions ---------------- */
const docSubs = new Map(), colSubs = new Map(), pendingSubs = new Set();
const addTo = (m, k, s) => { let x = m.get(k); if (!x) m.set(k, x = new Set()); x.add(s); };
const delFrom = (m, k, s) => { const x = m.get(k); if (x) { x.delete(s); if (!x.size) m.delete(k); } };
let subT = 0;
function changed(p) {
  const a = docSubs.get(p), b = colSubs.get(parentOf(p));
  if (a) for (const s of a) pendingSubs.add(s);
  if (b) for (const s of b) pendingSubs.add(s);
  if (pendingSubs.size && !subT) subT = setImmediate(flushSubs);
}
function put(p, obj, quiet) {
  docs.set(p, obj);
  const par = parentOf(p);
  let c = cols.get(par); if (!c) cols.set(par, c = new Map());
  c.set(idOf(p), obj);
  changed(p);
}
function rem(p) {
  if (!docs.delete(p)) return false;
  const c = cols.get(parentOf(p)); if (c) { c.delete(idOf(p)); if (!c.size) cols.delete(parentOf(p)); }
  changed(p); return true;
}
function runQuery(col, f, o, l) {
  const c = cols.get(col); let a = [];
  if (c) for (const [id, d] of c) { let ok = true; for (const [k, v] of f) if (d[k] !== v) { ok = false; break; } if (ok) a.push([id, d]); }
  if (o) { const k = o[0], s = o[1] === 'desc' ? -1 : 1; a.sort((x, y) => (x[1][k] > y[1][k] ? 1 : x[1][k] < y[1][k] ? -1 : 0) * s); }
  if (l > 0) a = a.slice(0, l);
  return a;
}
/* outbound batching: everything produced in one tick goes out as one frame per socket */
const outq = new Map();
let outT = 0;
function out(ws, m) { let a = outq.get(ws); if (!a) outq.set(ws, a = []); a.push(m); if (!outT) outT = setImmediate(flushOut); }
function flushOut() {
  outT = 0;
  for (const [ws, a] of outq) if (ws.readyState === 1) ws.send(a.length === 1 ? JSON.stringify(a[0]) : JSON.stringify(a));
  outq.clear();
}
function syncSub(s, first) {
  if (s.kind === 'd') {
    const d = docs.get(s.path);
    if (!d) { if (s.ex || first) { s.ex = false; s.last = null; out(s.ws, { t: 'd', i: s.i, x: 0 }); } return; }
    if (!s.ex || first) { s.ex = true; s.last = { ...d }; out(s.ws, { t: 'd', i: s.i, x: 1, f: 1, s: d }); return; }
    const set = {}, del = []; let any = 0;
    for (const k in d) if (s.last[k] !== d[k]) { set[k] = d[k]; any = 1; }
    for (const k in s.last) if (!(k in d)) { del.push(k); any = 1; }
    if (any) { s.last = { ...d }; out(s.ws, { t: 'd', i: s.i, x: 1, s: set, d: del }); }
  } else {
    const a = runQuery(s.col, s.f, s.o, s.l), now = new Map(), add = [], rm = [];
    for (const [id, d] of a) { now.set(id, d); if (first || s.last.get(id) !== d) add.push([id, d]); }
    if (!first) for (const id of s.last.keys()) if (!now.has(id)) rm.push(id);
    s.last = now;
    if (first || add.length || rm.length) out(s.ws, { t: 'q', i: s.i, f: first ? 1 : 0, a: add, r: rm });
  }
}
function flushSubs() {
  subT = 0;
  const list = [...pendingSubs]; pendingSubs.clear();
  for (const s of list) if (s.ws.readyState === 1 && s.ws.subs.get(s.i) === s) syncSub(s, false);
  flushOut();
}
function addSub(ws, i, r) {
  if (ws.subs.size >= 60 || !isObj(r)) return;
  let s;
  if (r.op === 'get' && okPath(r.path)) s = { ws, i, kind: 'd', path: r.path, ex: false, last: null };
  else if (r.op === 'query' && okPath(r.col)) {
    const f = Array.isArray(r.f) ? r.f.filter(x => Array.isArray(x) && typeof x[0] === 'string') : [];
    s = { ws, i, kind: 'q', col: r.col, f, o: Array.isArray(r.o) && typeof r.o[0] === 'string' ? r.o : null, l: Math.max(0, Math.min(500, +r.l || 0)), last: new Map() };
  } else return;
  const old = ws.subs.get(i); if (old) dropSub(old);
  ws.subs.set(i, s);
  addTo(s.kind === 'd' ? docSubs : colSubs, s.kind === 'd' ? s.path : s.col, s);
  syncSub(s, true);
}
function dropSub(s) { delFrom(s.kind === 'd' ? docSubs : colSubs, s.kind === 'd' ? s.path : s.col, s); pendingSubs.delete(s); }

/* ---------------- operations ---------------- */
const CAPS = { bids: 400, chat: 150 };
function prune(col) {
  const cap = CAPS[col.slice(col.lastIndexOf('/') + 1)]; if (!cap) return;
  const c = cols.get(col); if (!c || c.size <= cap + 50) return;
  let n = c.size - cap; for (const id of [...c.keys()]) { if (n-- <= 0) break; rem(col + '/' + id); }
}
function exec(r) {
  if (!isObj(r)) throw 'Bad request';
  switch (r.op) {
    case 'get': { if (!okPath(r.path)) throw 'Bad path'; const d = docs.get(r.path); return d ? { exists: true, data: d } : { exists: false }; }
    case 'set': { if (!okPath(r.path) || !isObj(r.data)) throw 'Bad request'; if (/^rooms\/\d+$/.test(r.path) && !docs.has(r.path)) { today().rooms++; ST.rooms++; stDirty = 1; } put(r.path, clone(r.data)); markDirty(r.path); return null; }
    case 'update': {
      if (!okPath(r.path) || !isObj(r.data)) throw 'Bad request';
      const d = docs.get(r.path); if (!d) throw 'Not found';
      const n = { ...d }, add = clone(r.data); Object.assign(n, add);
      for (const k in add) if (add[k] === null && false) delete n[k];
      put(r.path, n); markDirty(r.path, add); return null;
    }
    case 'delete': { if (!okPath(r.path)) throw 'Bad path'; if (rem(r.path)) markDirty(r.path); return null; }
    case 'acquire': {
      if (!okPath(r.path) || typeof r.holder !== 'string') throw 'Bad request';
      const cur = docs.get(r.path), now = Date.now();
      if (cur && cur.__x > now && cur.__h !== r.holder) return { acquired: false, holder: cur.__h };
      put(r.path, { ...(isObj(r.data) ? clone(r.data) : {}), __h: r.holder, __x: now + (Math.min(+r.ttlMs, 2592000000) || 60000) }); markDirty(r.path);
      return { acquired: true, holder: r.holder };
    }
    case 'add': {
      if (!okPath(r.col) || !isObj(r.data)) throw 'Bad request';
      const id = newId(); put(r.col + '/' + id, clone(r.data)); markDirty(r.col + '/' + id); prune(r.col); return { id };
    }
    case 'bid': {   /* atomic bid: one network trip, no waiting for the host's browser */
      if (!okPath(r.path) || typeof r.uid !== 'string' || typeof r.team !== 'string') throw 'Bad request';
      const room = docs.get(r.path), c = room && room.cur, amt = +r.amt, no = why => ({ ok: false, why });
      if (!room || room.status !== 'live' || !c || c.phase !== 'bid') return no('closed');
      if (c.k !== r.k) return no('stale');
      if ((c.bid || 0) !== (+r.prev || 0)) return no('outbid');
      if (!(amt > (c.bid || 0)) || amt > 1e6 || (c.by ? amt !== c.bid + room.inc : amt > 100)) return no('amount');
      if (c.end < Date.now() - 400) return no('late');
      const m = docs.get(r.path + '/members/' + r.uid);
      if (!m || !r.team || m.team !== r.team) return no('team');
      if (c.by === r.team) return no('lead');
      put(r.path, { ...room, cur: { ...c, bid: amt, by: r.team, end: Date.now() + Math.max(3, Math.min(60, +room.bt || 10)) * 1000 } });
      put(r.path + '/bids/' + newId(), { uid: r.uid, team: r.team, amt, k: c.k, t: Date.now(), srv: 1 });
      prune(r.path + '/bids'); markDirty(r.path);
      return { ok: true };
    }
    case 'query': {
      if (!okPath(r.col)) throw 'Bad path';
      const f = Array.isArray(r.f) ? r.f.filter(x => Array.isArray(x)) : [];
      return { docs: runQuery(r.col, f, Array.isArray(r.o) ? r.o : null, Math.max(0, Math.min(500, +r.l || 0))).map(([id, data]) => ({ id, data })) };
    }
  }
  throw 'Unknown operation';
}

/* ---------------- usage statistics (admin only) ---------------- */
const STF = path.join(DATA, 'stats.json'), TZ = (+process.env.TZ_OFFSET_MIN || 330) * 60000;   // day boundaries in IST by default
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const dayKey = (t = Date.now()) => new Date(t + TZ).toISOString().slice(0, 10);
const DAYS = new Map(), ST = { peak: { n: 0, t: 0 }, since: Date.now(), rooms: 0, logins: 0, visits: 0, out: 0, inn: 0 };
let stDirty = 0;
function today() { const k = dayKey(); let d = DAYS.get(k); if (!d) { d = { users: new Set(), logins: 0, visits: 0, rooms: 0, out: 0, inn: 0, peak: 0 }; DAYS.set(k, d); } return d; }
const addOut = n => { today().out += n; ST.out += n; stDirty = 1; };
const addIn = n => { today().inn += n; ST.inn += n; stDirty = 1; };
function loadStats() {
  try {
    const j = JSON.parse(fs.readFileSync(STF, 'utf8'));
    Object.assign(ST, j.total || {});
    for (const k in j.days || {}) { const d = j.days[k]; DAYS.set(k, { ...d, users: new Set(d.users || []) }); }
  } catch (e) { if (e.code !== 'ENOENT') console.error('stats read failed', e.message); }
}
function saveStats(sync) {
  if (!stDirty) return; stDirty = 0;
  const keys = [...DAYS.keys()].sort().slice(-400), days = {};
  for (const k of keys) { const d = DAYS.get(k); days[k] = { ...d, users: [...d.users] }; }
  for (const k of [...DAYS.keys()]) if (!(k in days)) DAYS.delete(k);
  const str = JSON.stringify({ total: ST, days }), tmp = STF + '.tmp';
  if (sync) { fs.writeFileSync(tmp, str); fs.renameSync(tmp, STF); } else fs.writeFile(tmp, str, e => { if (e) stDirty = 1; else fs.rename(tmp, STF, () => {}); });
}
setInterval(() => saveStats(false), 30000);
function unionUsers(keys) { const u = new Set(); for (const k of keys) { const d = DAYS.get(k); if (d) for (const x of d.users) u.add(x); } return u; }
function statsReport() {
  const now = Date.now(), tk = dayKey(), keys = [...DAYS.keys()].sort(), td = today();
  const back = n => keys.filter(k => k > dayKey(now - n * 864e5));
  const month = keys.filter(k => k.slice(0, 7) === tk.slice(0, 7)), sum = (ks, f) => ks.reduce((a, k) => a + (DAYS.get(k)[f] || 0), 0);
  const online = [...new Set([...clients].map(w => w.user).filter(Boolean))];
  const accounts = (cols.get('accounts') || new Map()).size, rooms = cols.get('rooms') || new Map();
  let live = 0; for (const r of rooms.values()) if (r.status === 'live' || r.status === 'paused') live++;
  const pack = ks => ({ activeUsers: unionUsers(ks).size, logins: sum(ks, 'logins'), visits: sum(ks, 'visits'), auctionsCreated: sum(ks, 'rooms'), mbOut: +(sum(ks, 'out') / 1048576).toFixed(2), mbIn: +(sum(ks, 'inn') / 1048576).toFixed(2) });
  return {
    now: { connections: clients.size, usersOnline: online.length, online: online.slice(0, 100), liveAuctions: live, openRooms: rooms.size },
    today: pack([tk]), last7days: pack(back(7)), last30days: pack(back(30)), thisMonth: pack(month),
    allTime: { registeredAccounts: accounts, uniqueActiveUsers: unionUsers(keys).size, logins: ST.logins, visits: ST.visits, auctionsCreated: ST.rooms, mbOut: +(ST.out / 1048576).toFixed(2), mbIn: +(ST.inn / 1048576).toFixed(2), peakConcurrentConnections: ST.peak.n, peakAt: ST.peak.t, trackingSince: ST.since },
    daily: keys.slice(-30).reverse().map(k => { const d = DAYS.get(k); return { date: k, activeUsers: d.users.size, logins: d.logins, visits: d.visits, auctions: d.rooms, peak: d.peak, mbOut: +(d.out / 1048576).toFixed(2) }; })
  };
}
function statsHttp(u, res) {
  const key = u.searchParams.get('key') || '';
  const a = Buffer.from(crypto.createHash('sha256').update(key).digest('hex')), b = Buffer.from(crypto.createHash('sha256').update(ADMIN_KEY).digest('hex'));
  if (!ADMIN_KEY) return json(res, 503, { error: 'Stats are off. Set the ADMIN_KEY environment variable on your host.' });
  if (!crypto.timingSafeEqual(a, b)) return json(res, 401, { error: 'Wrong stats key' });
  json(res, 200, statsReport());
}

/* ---------------- persistence ---------------- */
function load() {
  try {
    const j = JSON.parse(fs.readFileSync(DBF, 'utf8'));
    for (const p in j) { const par = parentOf(p); docs.set(p, j[p]); let c = cols.get(par); if (!c) cols.set(par, c = new Map()); c.set(idOf(p), j[p]); }
    console.log('loaded', docs.size, 'documents');
  } catch (e) { if (e.code !== 'ENOENT') console.error('could not read db.json:', e.message); }
}
function save(sync) {
  if (!dirty) return; dirty = 0;
  const o = {}; for (const [p, d] of docs) if (!EPHEMERAL.test(p)) o[p] = d;
  const s = JSON.stringify(o), tmp = DBF + '.tmp';
  if (sync) { fs.writeFileSync(tmp, s); fs.renameSync(tmp, DBF); }
  else fs.writeFile(tmp, s, e => { if (e) { dirty = 1; return console.error('save failed', e.message); } fs.rename(tmp, DBF, e2 => { if (e2) dirty = 1; }); });
}
setInterval(() => save(false), 4000);
/* delete old finished rooms so the database never grows forever */
function cleanup() {
  const now = Date.now(), dead = [];
  const rooms = cols.get('rooms'); if (!rooms) return;
  for (const [id, r] of rooms) {
    const age = now - (r.t || 0), st = r.status;
    if ((st === 'done' || st === 'cancelled') && age > 2 * 864e5 || st === 'lobby' && age > 2 * 864e5 || age > 10 * 864e5) dead.push(id);
  }
  for (const id of dead) { for (const p of [...docs.keys()]) if (p === 'rooms/' + id || p.startsWith('rooms/' + id + '/')) rem(p); }
  if (dead.length) { dirty = 1; console.log('cleaned', dead.length, 'old rooms'); }
}
setInterval(cleanup, 3600e3);

/* ---------------- media ---------------- */
const mtype = new Map(), msize = new Map();
for (const f of fs.readdirSync(MEDIA)) if (f.endsWith('.t')) { const id = f.slice(0, -2); try { mtype.set(id, fs.readFileSync(path.join(MEDIA, f), 'utf8')); msize.set(id, fs.statSync(path.join(MEDIA, id)).size); } catch (e) {} }
const json = (res, code, o) => { const b = JSON.stringify(o); res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(b); addOut(b.length); };
function upload(req, res) {
  const ty = (req.headers['content-type'] || '').split(';')[0].trim().toLowerCase();
  if (!/^(image|audio)\/[a-z0-9.+\-]+$/.test(ty)) { req.resume(); return json(res, 400, { error: 'Only image or audio files are allowed' }); }
  const ch = []; let n = 0, over = 0;
  req.on('data', c => { n += c.length; addIn(c.length); if (n > MAXMEDIA) over = 1; else ch.push(c); });
  req.on('error', () => {});
  req.on('end', () => {
    if (over) return json(res, 413, { error: 'File too big (max 5 MB)' });
    const id = crypto.randomBytes(9).toString('hex'), buf = Buffer.concat(ch);
    fs.writeFile(path.join(MEDIA, id), buf, e => {
      if (e) return json(res, 500, { error: 'Could not save file' });
      fs.writeFile(path.join(MEDIA, id + '.t'), ty, () => { mtype.set(id, ty); msize.set(id, buf.length); json(res, 200, { id }); });
    });
  });
}
function media(id, req, res) {
  const ty = mtype.get(id), size = msize.get(id);
  if (!/^[a-f0-9]{18}$/.test(id) || !ty) { res.writeHead(404); return res.end('not found'); }
  const h = { 'content-type': ty, 'cache-control': 'public, max-age=31536000, immutable', 'x-content-type-options': 'nosniff', 'content-security-policy': "default-src 'none'; sandbox", 'accept-ranges': 'bytes' };
  let start = 0, end = size - 1, code = 200;
  const m = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || '');
  if (m && (m[1] || m[2])) {
    if (m[1] === '') { start = Math.max(0, size - +m[2]); } else { start = +m[1]; if (m[2]) end = Math.min(end, +m[2]); }
    if (start > end || start >= size) { res.writeHead(416, { 'content-range': 'bytes */' + size }); return res.end(); }
    code = 206; h['content-range'] = `bytes ${start}-${end}/${size}`;
  }
  h['content-length'] = end - start + 1;
  res.writeHead(code, h); addOut(end - start + 1);
  if (req.method === 'HEAD') return res.end();
  fs.createReadStream(path.join(MEDIA, id), { start, end }).on('error', () => res.destroy()).pipe(res);
}

/* ---------------- static site ---------------- */
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.json': 'application/json', '.mp3': 'audio/mpeg', '.webp': 'image/webp' };
const fcache = new Map();
function serve(req, res, pathname) {
  let f = path.normalize(path.join(PUB, pathname === '/' ? 'index.html' : pathname));
  if (!f.startsWith(PUB + path.sep) && f !== PUB) { res.writeHead(403); return res.end(); }
  let st; try { st = fs.statSync(f); if (st.isDirectory()) { f = path.join(f, 'index.html'); st = fs.statSync(f); } } catch (e) { f = path.join(PUB, 'index.html'); try { st = fs.statSync(f); } catch (e2) { res.writeHead(404); return res.end('not found'); } }
  let c = fcache.get(f);
  if (!c || c.m !== st.mtimeMs) {
    const buf = fs.readFileSync(f), ext = path.extname(f).toLowerCase();
    c = { m: st.mtimeMs, buf, gz: /^(\.html|\.js|\.css|\.svg|\.json)$/.test(ext) ? zlib.gzipSync(buf, { level: 9 }) : null, etag: '"' + crypto.createHash('sha1').update(buf).digest('hex').slice(0, 16) + '"', type: TYPES[ext] || 'application/octet-stream' };
    fcache.set(f, c);
  }
  const h = { 'content-type': c.type, etag: c.etag, 'cache-control': 'no-cache', vary: 'accept-encoding', 'x-content-type-options': 'nosniff' };
  if (pathname === '/' || pathname === '/index.html') { today().visits++; ST.visits++; stDirty = 1; }
  if (req.headers['if-none-match'] === c.etag) { res.writeHead(304, h); return res.end(); }
  const gz = c.gz && /\bgzip\b/.test(req.headers['accept-encoding'] || '');
  const body = gz ? c.gz : c.buf; if (gz) h['content-encoding'] = 'gzip'; h['content-length'] = body.length;
  res.writeHead(200, h); res.end(req.method === 'HEAD' ? undefined : body); addOut(body.length);
}
const server = http.createServer((req, res) => {
  let u; try { u = new URL(req.url, 'http://x'); } catch (e) { res.writeHead(400); return res.end(); }
  const p = decodeURIComponent(u.pathname.replace(/%(?![0-9a-f]{2})/gi, '%25'));
  if (req.method === 'POST' && p === '/api/media') return upload(req, res);
  if ((req.method === 'GET' || req.method === 'HEAD') && p.startsWith('/api/media/')) return media(p.slice(11), req, res);
  if (p === '/api/stats') return statsHttp(u, res);
  if (p === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405); return res.end(); }
  serve(req, res, p);
});

/* ---------------- websocket (RFC 6455, no dependencies) ---------------- */
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11', MAXFRAME = 8e6;
class WS extends EventEmitter {
  constructor(sock) {
    super(); this.sock = sock; this.readyState = 1; this.buf = Buffer.alloc(0); this.frag = []; this.fragLen = 0; this.fragOp = 0;
    sock.on('data', d => this.onData(d));
    sock.on('close', () => { if (this.readyState !== 3) { this.readyState = 3; this.emit('close'); } });
    sock.on('error', () => sock.destroy());
  }
  frame(op, payload) {
    const n = payload.length; let h;
    if (n < 126) h = Buffer.from([0x80 | op, n]);
    else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
    return Buffer.concat([h, payload]);
  }
  raw(b) {
    if (this.readyState !== 1 || !this.sock.writable) return;
    if (this.sock.writableLength > 4e6) return this.terminate();   // client can't keep up
    this.sock.write(b); addOut(b.length);
  }
  send(str) { this.raw(this.frame(1, Buffer.from(str))); }
  ping() { this.raw(this.frame(9, Buffer.alloc(0))); }
  close(code = 1000) { if (this.readyState !== 1) return; const p = Buffer.alloc(2); p.writeUInt16BE(code); this.raw(this.frame(8, p)); this.readyState = 2; this.sock.end(); }
  terminate() { this.readyState = 3; this.sock.destroy(); this.emit('close'); }
  onData(d) {
    addIn(d.length);
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = b[0] & 0x80, op = b[0] & 15, masked = b[1] & 0x80; let len = b[1] & 127, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; const big = b.readBigUInt64BE(2); if (big > BigInt(MAXFRAME)) return this.close(1009); len = Number(big); off = 10; }
      if (len > MAXFRAME || !masked) return this.close(masked ? 1009 : 1002);
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4), p = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) p[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      if (op === 8) return this.close(1000);
      if (op === 9) { this.raw(this.frame(10, p)); continue; }
      if (op === 10) { this.emit('pong'); continue; }
      if (op === 1 || op === 2) { this.fragOp = op; this.frag = []; this.fragLen = 0; }
      if (op === 0 || op === 1 || op === 2) {
        this.frag.push(p); this.fragLen += len;
        if (this.fragLen > MAXFRAME) return this.close(1009);
        if (fin) { const m = Buffer.concat(this.frag); this.frag = []; this.fragLen = 0; if (this.fragOp === 1) this.emit('message', m.toString('utf8')); }
      }
    }
  }
}
const clients = new Set();
server.on('upgrade', (req, sock) => {
  sock.on('error', () => sock.destroy());
  let pn = ''; try { pn = new URL(req.url, 'http://x').pathname; } catch (e) {}
  const key = req.headers['sec-websocket-key'];
  if (pn !== '/ws' || !key || (req.headers.upgrade || '').toLowerCase() !== 'websocket' || clients.size > 3000) return sock.destroy();
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + crypto.createHash('sha1').update(key + GUID).digest('base64') + '\r\n\r\n');
  sock.setNoDelay(true); sock.setTimeout(0);
  onConnection(new WS(sock));
});
function onConnection(ws) {
  clients.add(ws); { const d = today(); if (clients.size > d.peak) d.peak = clients.size; if (clients.size > ST.peak.n) ST.peak = { n: clients.size, t: Date.now() }; stDirty = 1; }
  ws.subs = new Map(); ws.alive = true; ws.tok = 200; ws.tt = Date.now();
  ws.on('pong', () => ws.alive = true);
  ws.on('message', raw => {
    ws.alive = true;
    const now = Date.now(); ws.tok = Math.min(200, ws.tok + (now - ws.tt) * 0.15); ws.tt = now;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!isObj(m)) return;
    if (m.t === 'p') return out(ws, { t: 'o', c: m.c, s: Date.now() });
    if (m.t === 'h') { if (typeof m.u === 'string' && /^[a-z0-9_]{3,16}$/.test(m.u) && ws.user !== m.u) { ws.user = m.u; const d = today(); d.users.add(m.u); d.logins++; ST.logins++; stDirty = 1; } return; }
    if (m.t === 's') return addSub(ws, m.i, m.r);
    if (m.t === 'u') { const s = ws.subs.get(m.i); if (s) { dropSub(s); ws.subs.delete(m.i); } return; }
    if (m.t === 'r') {
      if (--ws.tok < 0) return out(ws, { t: 'a', n: m.n, err: 'Too many requests, slow down' });
      let v; try { v = exec(m.r); } catch (e) { return out(ws, { t: 'a', n: m.n, err: typeof e === 'string' ? e : 'Server error' }); }
      out(ws, { t: 'a', n: m.n, ok: v });
    }
  });
  ws.on('close', () => { clients.delete(ws); for (const s of ws.subs.values()) dropSub(s); ws.subs.clear(); outq.delete(ws); });
}
setInterval(() => { const d = today(); for (const ws of clients) if (ws.user) d.users.add(ws.user); stDirty = 1; for (const ws of clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; ws.ping(); } }, 25000);

load(); loadStats(); cleanup();
server.listen(PORT, () => console.log('CRICHUB running on port ' + PORT + ' · data in ' + DATA));
const bye = () => { try { save(true); stDirty = 1; saveStats(true); } catch (e) {} process.exit(0); };
process.on('SIGTERM', bye); process.on('SIGINT', bye);
