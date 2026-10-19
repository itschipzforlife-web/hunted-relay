// Hunted by Stupidity - room relay on Cloudflare Workers + Durable Objects.
// One Durable Object per 6-digit room code. The host client is authoritative; this only routes messages.
const MAX_PLAYERS = 4;

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/' || url.pathname === '/health') {
      return new Response('ok', { headers: { 'Access-Control-Allow-Origin': '*' } });
    }
    if (url.pathname === '/ws') {
      if (req.headers.get('Upgrade') !== 'websocket') return new Response('expected websocket', { status: 426 });
      const room = url.searchParams.get('room') || '';
      if (!/^\d{6}$/.test(room)) return new Response('bad room', { status: 400 });
      return env.ROOM.get(env.ROOM.idFromName(room)).fetch(req);
    }
    return new Response('not found', { status: 404 });
  },
};

export class Room {
  constructor() {
    this.host = null;
    this.clients = new Map();
    this.started = false;
    this.n = 0;
    this.code = '';
  }

  async fetch(req) {
    const url = new URL(req.url);
    const wantHost = url.searchParams.get('host') === '1';
    this.code = url.searchParams.get('room');
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    const send = (ws, o) => { try { ws.send(JSON.stringify(o)); } catch (e) {} };
    const fail = why => { send(server, { t: 'err', why }); setTimeout(() => { try { server.close(1000, why); } catch (e) {} }, 200); };

    if (wantHost) {
      if (this.host) fail('taken');
      else this.attachHost(server, send);
    } else if (!this.host) fail('nogame');
    else if (this.started) fail('started');
    else if (this.clients.size + 1 >= MAX_PLAYERS) fail('full');
    else this.attachClient(server, send);

    return new Response(null, { status: 101, webSocket: client });
  }

  attachHost(ws, send) {
    const id = 'p' + (++this.n);
    this.host = ws;
    send(ws, { t: 'hosted', code: this.code, id });
    ws.addEventListener('message', ev => {
      let d; try { d = JSON.parse(ev.data); } catch (e) { return; }
      if (!d) return;
      if (d.t === 'to') {
        const c = this.clients.get(d.id);
        if (c) send(c, { t: 'msg', m: d.m });
      } else if (d.t === 'all') {
        const out = JSON.stringify({ t: 'msg', m: d.m });
        for (const [cid, c] of this.clients) if (cid !== d.except) { try { c.send(out); } catch (e) {} }
      } else if (d.t === 'lock') {
        this.started = true;
      }
    });
    const gone = () => {
      if (this.host !== ws) return;
      this.host = null;
      for (const c of this.clients.values()) { send(c, { t: 'host_left' }); try { c.close(1000, 'host_left'); } catch (e) {} }
      this.clients.clear();
    };
    ws.addEventListener('close', gone);
    ws.addEventListener('error', gone);
  }

  attachClient(ws, send) {
    const id = 'p' + (++this.n);
    this.clients.set(id, ws);
    send(ws, { t: 'joined', id, code: this.code });
    ws.addEventListener('message', ev => {
      let d; try { d = JSON.parse(ev.data); } catch (e) { return; }
      if (d && d.t === 'to' && this.host) send(this.host, { t: 'fwd', from: id, m: d.m });
    });
    const gone = () => {
      if (!this.clients.delete(id)) return;
      if (this.host) send(this.host, { t: 'peer_left', id });
    };
    ws.addEventListener('close', gone);
    ws.addEventListener('error', gone);
  }
}
