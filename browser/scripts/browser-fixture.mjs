// Local HTTP/2 fixture and demo host. This is not an S3 adapter or a storage benchmark.
import { createSecureServer } from 'node:http2';
import { createServer as createHttpsServer } from 'node:https';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const temporary = mkdtempSync(join(tmpdir(), 's3-streams-browser-'));
execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
  '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1',
  '-keyout', join(temporary, 'key.pem'), '-out', join(temporary, 'cert.pem')], { stdio: 'ignore' });
const tls = { key: readFileSync(join(temporary, 'key.pem')), cert: readFileSync(join(temporary, 'cert.pem')) };
const objects = new Map();
const stats = new Map();
let http1Origin;
function json(res, status, value) {
  const data = JSON.stringify(value);
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) });
  res.end(data);
}
async function handle(req, res) {
  req.on('error', () => {}); res.on('error', () => {});
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, PUT, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Upload-Length, Range');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Content-Type, ETag, Accept-Ranges, Content-Encoding');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
  const location = new URL(req.url, 'https://localhost');
  if (location.pathname === '/fixture-info') { json(res, 200, { http1Origin }); return; }
  if (location.pathname === '/stats') { json(res, 200, stats.get(location.searchParams.get('key')) ?? { bytes: 0 }); return; }
  if (location.pathname.startsWith('/objects/')) {
    const key = decodeURIComponent(location.pathname.slice('/objects/'.length));
    if (req.method === 'PUT') {
      const state = { bytes: 0, ended: false, aborted: false, protocol: req.httpVersion };
      stats.set(key, state);
      req.on('aborted', () => { state.aborted = true; });
      const parts = [];
      try {
        for await (const part of req) {
          state.bytes += part.length;
          if (key.startsWith('network-drop')) {
            if (req.stream) req.stream.close(2); else req.socket.destroy();
            return;
          }
          if (state.bytes > 8 * 1024 * 1024) {
            json(res, 413, { committed: false, code: 'TOO_LARGE', message: 'Fixture is limited to 8 MiB' }); return;
          }
          parts.push(part);
        }
      } catch { state.aborted = true; return; }
      state.ended = true;
      const known = req.headers['x-upload-length'];
      if (known !== undefined && Number(known) !== state.bytes) {
        json(res, 400, { committed: false, code: 'LENGTH_MISMATCH', message: 'Declared size mismatch' }); return;
      }
      const data = Buffer.concat(parts);
      const etag = createHash('sha256').update(data).digest('hex');
      objects.set(key, { data, etag });
      json(res, 201, { committed: true, key: key.startsWith('invalid-commit') ? 'wrong-key' : key, bytes: data.length, etag });
      return;
    }
    if (req.method === 'GET') {
      const value = objects.get(key);
      if (!value) { json(res, 404, { code: 'NOT_FOUND', message: 'Unknown fixture key' }); return; }
      let start = 0, end = value.data.length - 1, status = 200;
      const range = req.headers.range;
      if (range && !key.startsWith('ignore-range')) {
        const match = /^bytes=(\d*)-(\d*)$/.exec(range);
        if (!match || (!match[1] && !match[2])) { res.writeHead(416); res.end(); return; }
        if (!match[1]) start = Math.max(0, value.data.length - Number(match[2]));
        else { start = Number(match[1]); if (match[2]) end = Math.min(end, Number(match[2])); }
        if (start > end || start >= value.data.length) { res.writeHead(416); res.end(); return; }
        status = 206;
        res.setHeader('Content-Range', `bytes ${start}-${end}/${value.data.length}`);
      }
      const body = value.data.subarray(start, end + 1);
      res.writeHead(status, { 'Content-Type': 'application/octet-stream', 'Content-Length': body.length,
        ETag: value.etag, 'Accept-Ranges': 'bytes' });
      res.end(body); return;
    }
  }
  const files = location.pathname === '/' ? 'demo/index.html'
    : /^\/(dist\/[a-z-]+\.js|demo\/[a-z-]+\.(js|html))$/.test(location.pathname) ? location.pathname.slice(1) : undefined;
  if (files) {
    try {
      const data = await readFile(join(root, files));
      res.writeHead(200, { 'Content-Type': files.endsWith('.html') ? 'text/html; charset=utf-8' : 'text/javascript', 'Content-Length': data.length });
      res.end(data); return;
    } catch { /* normal 404 */ }
  }
  json(res, 404, { code: 'NOT_FOUND', message: 'Unknown route' });
}
const listener = (req, res) => { void handle(req, res).catch(error => { if (!res.headersSent) json(res, 500, { message: error.message }); else res.destroy(); }); };
const h2 = createSecureServer({ ...tls, allowHTTP1: true }, listener);
const h1 = createHttpsServer(tls, listener);
await new Promise(resolve => h1.listen(0, '127.0.0.1', resolve));
http1Origin = `https://127.0.0.1:${h1.address().port}`;
await new Promise(resolve => h2.listen(Number(process.env.PORT ?? 9443), '127.0.0.1', resolve));
console.log(JSON.stringify({ fixture: `https://127.0.0.1:${h2.address().port}`, http1Origin,
  storage: 'bounded in-memory protocol fixture, not S3', certificates: 'temporary; removed on shutdown' }));
let shuttingDown = false;
function stop() {
  if (shuttingDown) return;
  shuttingDown = true;
  h2.close(); h1.close();
  rmSync(temporary, { recursive: true, force: true });
  setTimeout(() => process.exit(0), 50).unref();
}
process.on('SIGINT', stop); process.on('SIGTERM', stop);
process.on('exit', () => rmSync(temporary, { recursive: true, force: true }));
