// Delta Client — full WebSocket backend
// Handles:
//   - WebSocket auth (subprotocol = "<session_token>-minecraft", validated against site /api/user/sync)
//   - IRC chat relay between all online users (persists last 100 messages in memory)
//   - Friend marker routing (broadcasts "friend mark" packets to all online users)
//   - Cosmetics push via HTTP POST /cosmetics/wear
//   - HTTP health: GET /
//
// Free hosting: any platform that supports long-lived Node processes with WS (Render, Railway,
// fly.io, Oracle Free Tier, etc.). For TLS/wss, use a reverse proxy (Caddy/Nginx) or the
// platform's TLS termination (Render/Railway both provide wss:// out of the box).
//
// Required env:
//   PORT                  — port to listen on (default 8080)
//   SITE_BASE             — base URL of the auth site (default https://site-5a0.pages.dev)
//   ADMIN_TOKEN           — shared secret required for /cosmetics/wear (set anything)

const http = require('http');
const { URL } = require('url');
const { WebSocketServer } = require('ws');

const PORT       = parseInt(process.env.PORT || '8080', 10);
const SITE_BASE  = (process.env.SITE_BASE  || 'https://site-5a0.pages.dev').replace(/\/+$/, '');
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  if (req.method === 'GET' && url.pathname === '/') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Delta WS backend OK');
    return;
  }

  if (req.method === 'GET' && url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, online: clients.size }));
    return;
  }

  // Cosmetics push: POST /cosmetics/wear  { token, uuid, name, category, scale, x, y, z, geometry, animation, texture(base64) }
  if (req.method === 'POST' && url.pathname === '/cosmetics/wear') {
    if (!ADMIN_TOKEN) { return json(res, 500, { error: 'ADMIN_TOKEN not set' }); }
    const auth = req.headers['x-admin-token'];
    if (auth !== ADMIN_TOKEN) { return json(res, 401, { error: 'unauthorized' }); }

    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const { uuid, cosmetic } = data;
        if (!uuid || !cosmetic) return json(res, 400, { error: 'missing uuid/cosmetic' });
        const packet = wrapPacket('cosmetics', { action: 'WEAR', type: 'COSMETIC', uuid, cosmetic });
        let delivered = 0;
        for (const [_, c] of clients) {
          if (c.uid && c.ws.readyState === 1) {
            c.ws.send(packet);
            delivered++;
          }
        }
        return json(res, 200, { ok: true, delivered });
      } catch (e) {
        return json(res, 400, { error: 'bad json' });
      }
    });
    return;
  }

  // Cosmetics unwear: POST /cosmetics/unwear  { token, uuid, category }
  if (req.method === 'POST' && url.pathname === '/cosmetics/unwear') {
    if (!ADMIN_TOKEN) { return json(res, 500, { error: 'ADMIN_TOKEN not set' }); }
    const auth = req.headers['x-admin-token'];
    if (auth !== ADMIN_TOKEN) { return json(res, 401, { error: 'unauthorized' }); }

    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const { uuid, category } = data;
        if (!uuid || !category) return json(res, 400, { error: 'missing uuid/category' });
        const packet = wrapPacket('cosmetics', {
          action: 'UNWEAR',
          type: 'COSMETIC',
          uuid,
          cosmetic: JSON.stringify({ category })
        });
        let delivered = 0;
        for (const [_, c] of clients) {
          if (c.ws.readyState === 1) { c.ws.send(packet); delivered++; }
        }
        return json(res, 200, { ok: true, delivered });
      } catch (e) {
        return json(res, 400, { error: 'bad json' });
      }
    });
    return;
  }

  // Site can POST an IRC "broadcast" so the WS fans it out to all online users.
  // POST /irc/broadcast  { token, message, priority }
  if (req.method === 'POST' && url.pathname === '/irc/broadcast') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const token = data.token;
        const user = await validateSession(token);
        if (!user) return json(res, 401, { error: 'invalid session' });
        const message = String(data.message || '').trim().slice(0, 500);
        if (!message) return json(res, 400, { error: 'empty message' });
        const priority = data.priority || user.role || 'USER';
        const packet = wrapPacket('irc', { type: 'message', user: user.username, message, priority });
        broadcast(packet);
        pushHistory({ type: 'irc', payload: { type: 'message', user: user.username, message, priority }, t: Date.now() });
        return json(res, 200, { ok: true });
      } catch (e) {
        return json(res, 400, { error: 'bad json' });
      }
    });
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
});

function json(res, status, data) {
  res.writeHead(status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
  res.end(JSON.stringify(data));
}

const wss = new WebSocketServer({ server, handleProtocols: (protocols, request) => {
  // Client uses subprotocol "<token>-minecraft"
  if (protocols && protocols.size > 0) {
    for (const p of protocols) {
      if (p.endsWith('-minecraft')) return p;
    }
    return protocols.values().next().value;
  }
  return false;
} });

const clients = new Map(); // ws -> { uid, username, role, hwid, ws }
const history = [];        // last 100 server-issued packets
const MAX_HISTORY = 100;

function pushHistory(msg) {
  history.push(msg);
  if (history.length > MAX_HISTORY) history.shift();
}

function wrapPacket(id, payloadObj) {
  // Client unpackPacket does .get("payload").getAsString(), so payload must be a string.
  return JSON.stringify({ id, payload: JSON.stringify(payloadObj) });
}

function broadcast(packet) {
  for (const [ws] of clients) {
    if (ws.readyState === 1) ws.send(packet);
  }
}

async function validateSession(token) {
  if (!token || !token.startsWith('sess_')) return null;
  try {
    const r = await fetch(`${SITE_BASE}/api/user/sync`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_token: token })
    });
    if (!r.ok) return null;
    const data = await r.json();
    if (data.status !== 'success') return null;
    if (data.banned) return null;
    return {
      username: data.username,
      uid: data.uid,
      role: data.role || 'user',
      hwid: data.hwid || '',
      access: !!data.access
    };
  } catch (e) {
    return null;
  }
}

wss.on('connection', async (ws, request) => {
  // Extract token from the chosen subprotocol: "<token>-minecraft"
  let token = null;
  const proto = ws.protocol || request.headers['sec-websocket-protocol'] || '';
  if (proto.endsWith('-minecraft')) token = proto.slice(0, -'-minecraft'.length);

  if (!token) { ws.close(4001, 'no auth'); return; }
  const user = await validateSession(token);
  if (!user) { ws.close(4003, 'invalid session'); return; }
  if (user.banned) { ws.close(4004, 'banned'); return; }
  if (!user.access && user.role !== 'admin' && user.role !== 'ADMIN') {
    ws.close(4005, 'no access'); return;
  }

  clients.set(ws, { ws, uid: user.uid, username: user.username, role: user.role, hwid: user.hwid });

  // Send IRC history
  for (const m of history) {
    if (m.type === 'irc') {
      ws.send(wrapPacket('irc', m.payload));
    }
  }

  // Announce join
  const joinPacket = wrapPacket('irc', { type: 'system', user: 'system', message: user.username + ' подключился', priority: 'SYSTEM' });
  broadcast(joinPacket);
  pushHistory({ type: 'irc', payload: JSON.parse(joinPacket.match(/"payload":"(.+?)"}/)?.[1] || '{"type":"system"}') });

  ws.on('message', (raw) => {
    try {
      const obj = JSON.parse(raw.toString());
      const id = obj.id;
      // payload may be a JSON object OR a string; normalize to string then parse
      let payload = obj.payload;
      if (typeof payload !== 'string') {
        try { payload = JSON.stringify(payload); } catch (e) { return; }
      }

      if (id === 'irc') {
        let data;
        try { data = JSON.parse(payload); } catch (e) { return; }
        const message = String(data.message || '').trim().slice(0, 500);
        if (!message) return;
        const me = clients.get(ws);
        const out = wrapPacket('irc', { type: 'message', user: me.username, message, priority: me.role || 'USER' });
        broadcast(out);
        // Persist as history (best-effort)
        try {
          const inner = { type: 'message', user: me.username, message, priority: me.role || 'USER' };
          pushHistory({ type: 'irc', payload: inner, t: Date.now() });
        } catch (e) {}
      }

      if (id === 'friend') {
        let data;
        try { data = JSON.parse(payload); } catch (e) { return; }
        if (data.type === 'mark') {
          const me = clients.get(ws);
          const out = wrapPacket('friend', {
            type: 'mark',
            minecraft: me.username,
            pos: data.pos
          });
          broadcast(out);
        }
      }

      if (id === 'ping' || id === 'heartbeat') {
        ws.send(wrapPacket('pong', {}));
      }
    } catch (e) {
      // ignore
    }
  });

  ws.on('close', () => {
    const me = clients.get(ws);
    clients.delete(ws);
    if (me) {
      const leavePacket = wrapPacket('irc', { type: 'system', user: 'system', message: me.username + ' отключился', priority: 'SYSTEM' });
      broadcast(leavePacket);
    }
  });

  ws.on('error', () => {});
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Delta WS backend listening on :${PORT}`);
  console.log(`Site auth: ${SITE_BASE}`);
});
