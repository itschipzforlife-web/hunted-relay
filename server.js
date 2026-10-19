// Hunted by Stupidity - room relay (WebSocket). Host client is authoritative; this just routes messages.
const http = require('http');
const { WebSocketServer } = require('ws');

const MAX_PLAYERS = 4;
const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
    res.end('ok');
  } else { res.writeHead(404); res.end(); }
});
const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });
const rooms = new Map();
let nid = 1;

const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const newCode = () => {
  for (let i = 0; i < 50; i++) {
    const c = String(Math.floor(100000 + Math.random() * 900000));
    if (!rooms.has(c)) return c;
  }
  return null;
};

wss.on('connection', ws => {
  ws.id = 'p' + (nid++);
  ws.room = null;
  ws.isHost = false;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    let d; try { d = JSON.parse(raw); } catch (e) { return; }
    if (!d || typeof d.t !== 'string') return;
    const room = ws.room;

    if (d.t === 'host') {
      if (room) return;
      const code = newCode();
      if (!code) return send(ws, { t: 'err', why: 'busy' });
      const r = { code, host: ws, clients: new Map(), started: false };
      rooms.set(code, r); ws.room = r; ws.isHost = true;
      send(ws, { t: 'hosted', code, id: ws.id });
    } else if (d.t === 'join') {
      if (room) return;
      const r = rooms.get(String(d.code || ''));
      if (!r) return send(ws, { t: 'err', why: 'nogame' });
      if (r.started) return send(ws, { t: 'err', why: 'started' });
      if (r.clients.size + 1 >= MAX_PLAYERS) return send(ws, { t: 'err', why: 'full' });
      r.clients.set(ws.id, ws); ws.room = r;
      send(ws, { t: 'joined', id: ws.id, code: r.code });
    } else if (d.t === 'to' && room) {
      if (ws.isHost) {
        const c = room.clients.get(d.id);
        if (c) send(c, { t: 'msg', m: d.m });
      } else {
        send(room.host, { t: 'fwd', from: ws.id, m: d.m });
      }
    } else if (d.t === 'all' && room && ws.isHost) {
      const out = JSON.stringify({ t: 'msg', m: d.m });
      for (const [id, c] of room.clients) if (id !== d.except && c.readyState === 1) c.send(out);
    } else if (d.t === 'lock' && room && ws.isHost) {
      room.started = true;
    }
  });

  ws.on('close', () => {
    const r = ws.room;
    if (!r) return;
    if (ws.isHost) {
      for (const c of r.clients.values()) { send(c, { t: 'host_left' }); try { c.close(); } catch (e) {} }
      rooms.delete(r.code);
    } else {
      r.clients.delete(ws.id);
      send(r.host, { t: 'peer_left', id: ws.id });
    }
  });
  ws.on('error', () => {});
});

// drop dead sockets and keep free hosts from idling the connection out
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; try { ws.ping(); } catch (e) {}
  }
}, 30000);

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log('relay listening on ' + PORT));
