// Load test against a running server on localhost:3000 (Node 22+).
// Connects ROOMS x PER_ROOM clients, then every client sends one message.
//
//   node bench/load.mjs [rooms=20] [per_room=100]
const [ROOMS = 20, PER_ROOM = 100] = process.argv.slice(2).map(Number);
const BASE = 'ws://127.0.0.1:3000/chat';

function connect(room, username) {
  const ws = new WebSocket(`${BASE}/${room}?username=${username}`);
  Object.assign(ws, { joined: false, messages: 0, closed: false });
  ws.addEventListener('message', (e) => {
    const event = JSON.parse(e.data);
    if (event.type === 'message') ws.messages++;
    if (event.type === 'join' && event.username === username) ws.joined = true;
  });
  ws.addEventListener('close', () => (ws.closed = true));
  return new Promise((resolve, reject) => {
    ws.addEventListener('open', () => resolve(ws), { once: true });
    ws.addEventListener('error', () => reject(new Error(`cannot connect ${room}/${username}`)), {
      once: true,
    });
  });
}

function until(done, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve, reject) => {
    const timer = setInterval(() => {
      const ok = done();
      if (ok || Date.now() > deadline) {
        clearInterval(timer);
        ok ? resolve() : reject(new Error('timed out'));
      }
    }, 2);
  });
}

const total = ROOMS * PER_ROOM;
let start = performance.now();
const clients = await Promise.all(
  Array.from({ length: total }, (_, i) => connect(`load${i % ROOMS}`, `u${Math.floor(i / ROOMS)}`)),
);
await until(() => clients.every((c) => c.joined));
console.log(`${total} clients in ${ROOMS} rooms joined in ${(performance.now() - start).toFixed(0)} ms`);

start = performance.now();
for (const c of clients) c.send('hello');
await until(() => clients.every((c) => c.closed || c.messages === PER_ROOM));
const elapsed = (performance.now() - start).toFixed(0);
const delivered = clients.reduce((sum, c) => sum + c.messages, 0);
const dropped = clients.filter((c) => c.closed).length;
console.log(`${total} messages sent, ${delivered} delivered in ${elapsed} ms, ${dropped} connections dropped`);

for (const c of clients) c.close();
await until(() => clients.every((c) => c.closed));
