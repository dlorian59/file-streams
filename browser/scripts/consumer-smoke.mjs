import { execFileSync } from 'node:child_process';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const temp = mkdtempSync(join(tmpdir(), 's3-streams-consumer-'));
try {
  const packed = JSON.parse(execFileSync('npm', ['pack', '--json', '--pack-destination', temp], { cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'] }));
  const files = packed[0].files.map(file => file.path);
  if (files.some(name => /^(src|test|demo|scripts|node_modules|output)\//.test(name))) throw new Error('Development files leaked into npm pack');
  writeFileSync(join(temp, 'package.json'), JSON.stringify({ private: true, type: 'module' }));
  execFileSync('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund', join(temp, packed[0].filename)], { cwd: temp, stdio: 'inherit' });
  writeFileSync(join(temp, 'types.ts'), `import { uploadStream, downloadTo, streamCapabilities, objectUrl, TransferError, type UploadCommit, type ByteRange } from '@59alex/file-streams';
const range: ByteRange = { start: 0, end: 1 };
const commit: Promise<UploadCommit> = uploadStream({ url: objectUrl('https://example.test/objects', 'key'), key: 'key', maxBytes: 0, source: new ReadableStream<Uint8Array>() });
void [range, commit, downloadTo, streamCapabilities, new TransferError('ABORTED', 'test')];
`);
  execFileSync(join(root, 'node_modules/.bin/tsc'), ['--noEmit', '--strict', '--module', 'NodeNext', '--moduleResolution', 'NodeNext', '--target', 'ES2022', '--lib', 'ES2022,DOM', 'types.ts'], { cwd: temp, stdio: 'inherit' });
  writeFileSync(join(temp, 'runtime.mjs'), `import assert from 'node:assert/strict';
import { uploadStream, downloadTo } from '@59alex/file-streams';
const commit = await uploadStream({ url: 'https://example.test/key', key: 'key', maxBytes: 0,
source: new ReadableStream({ start(c) { c.close(); } }),
fetch: async (_url, init) => { const reader=init.body.getReader(); assert.equal((await reader.read()).done,true); reader.releaseLock(); return Response.json({committed:true,key:'key',bytes:0,etag:'opaque'},{status:201}); } });
assert.equal(commit.bytes,0);
let closed=false;
const result = await downloadTo({url:'https://example.test/key',maxBytes:0,destination:new WritableStream({close(){closed=true;}}),
fetch:async()=>new Response(new Uint8Array(),{headers:{'Content-Length':'0',ETag:'opaque','Accept-Ranges':'bytes'}})});
assert.equal(result.bytesWritten,0);assert.equal(closed,true);
console.log('Clean packed consumer: exports, declarations, zero-byte upload/download passed');
`);
  execFileSync(process.execPath, ['runtime.mjs'], { cwd: temp, stdio: 'inherit' });
  console.log(`Packed ${files.length} files; no development fixtures or runtime dependencies`);
} finally { rmSync(temp, { recursive: true, force: true }); }
