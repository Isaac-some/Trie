import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { access, mkdtemp, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

const root = path.resolve(import.meta.dirname, '..');
const sessionId = 'export-flow-test';

async function startServer(extraEnv = {}) {
  const dataDir = await mkdtemp(path.join(os.tmpdir(), 'trie-export-test-'));
  const port = 39000 + Math.floor(Math.random() * 1000);
  const child = spawn(process.execPath, ['server/index.mjs'], {
    cwd: root,
    env: { ...process.env, ...extraEnv, DATA_DIR: dataDir, HOST: '127.0.0.1', PORT: String(port) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('测试服务未能启动')), 5000);
    child.once('error', reject);
    child.stdout.on('data', (chunk) => {
      if (String(chunk).includes('Transfer service listening')) {
        clearTimeout(timer);
        resolve();
      }
    });
  });
  return {
    baseUrl: `http://127.0.0.1:${port}`,
    dataDir,
    stop: async () => {
      child.kill();
      await rm(dataDir, { recursive: true, force: true });
    },
  };
}

async function request(baseUrl, pathname, init = {}) {
  const headers = new Headers(init.headers);
  headers.set('X-Session-Id', sessionId);
  return fetch(`${baseUrl}${pathname}`, { ...init, headers });
}

async function sha256(value) {
  return (await import('node:crypto')).createHash('sha256').update(value).digest('hex');
}

async function json(baseUrl, pathname, body) {
  const response = await request(baseUrl, pathname, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  assert.equal(response.status, 202);
  return response.json();
}

async function waitForJob(baseUrl, jobId) {
  for (let attempt = 0; attempt < 80; attempt++) {
    const response = await request(baseUrl, `/api/jobs/${jobId}`);
    const { job } = await response.json();
    if (job.status === 'completed') return job;
    if (job.status === 'failed') throw new Error(job.error || '任务失败');
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error('任务超时');
}

test('exports a source/destination CSV and preserves the session for download', async (t) => {
  const server = await startServer();
  t.after(() => server.stop());
  const csv = Buffer.from('rowkey,path\noriginal-row-key-001,tos://1/2/3/4/5/file.jpg\n');
  const initResponse = await request(server.baseUrl, '/api/uploads/init', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: 'paths.csv', size: csv.length, fingerprint: 'paths.csv:test' }),
  });
  assert.equal(initResponse.status, 201);
  const { upload } = await initResponse.json();
  const invalidChunkResponse = await request(server.baseUrl, `/api/uploads/${upload.id}/chunks/0`, { method: 'PUT', headers: { 'X-Chunk-SHA256': '0'.repeat(64) }, body: csv });
  assert.equal(invalidChunkResponse.status, 400);
  const chunkResponse = await request(server.baseUrl, `/api/uploads/${upload.id}/chunks/0`, { method: 'PUT', headers: { 'X-Chunk-SHA256': await sha256(csv) }, body: csv });
  assert.equal(chunkResponse.status, 200);
  const completeResponse = await request(server.baseUrl, `/api/uploads/${upload.id}/complete`, { method: 'POST' });
  assert.equal(completeResponse.status, 201);
  const { datasetId } = await completeResponse.json();
  const processJob = await json(server.baseUrl, `/api/datasets/${datasetId}/process`, { selectedColumns: [1] });
  await waitForJob(server.baseUrl, processJob.jobId);
  const treeResponse = await request(server.baseUrl, `/api/datasets/${datasetId}/tree?limit=5`);
  assert.equal(treeResponse.status, 200);
  const tree = await treeResponse.json();
  assert.equal(tree.children[0].autoBranch[0].name, '2');
  assert.equal(tree.children[0].autoBranch[0].autoBranch[0].name, '3');

  const previewResponse = await request(server.baseUrl, `/api/datasets/${datasetId}/previews`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ mappings: [{ source: 'tos://1/2/3/4/', destination: 'tos://6/7/8/' }] }),
  });
  assert.equal(previewResponse.status, 200);
  assert.deepEqual((await previewResponse.json()).previews, [{
    source: 'tos://1/2/3/4/5/file.jpg',
    destination: 'tos://6/7/8/4/5/file.jpg',
  }]);

  const duplicateResponse = await request(server.baseUrl, '/api/exports', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ entries: [{ datasetId, mappings: [
      { source: 'tos://1/2/3/4/', destination: 'cos://5/' },
      { source: 'tos://1/2/3/4/', destination: 'cos://6/' },
    ] }] }),
  });
  assert.equal(duplicateResponse.status, 400);

  const exportJob = await json(server.baseUrl, '/api/exports', {
    entries: [{ datasetId, mappings: [{ source: 'tos://1/2/3/4/', destination: 'tos://6/7/8/' }] }],
  });
  await waitForJob(server.baseUrl, exportJob.jobId);
  const download = await request(server.baseUrl, `/api/exports/${exportJob.exportId}/download`);
  assert.equal(download.status, 200);
  assert.equal(await download.text(), 'src_path,dst_path,rowkey\ntos://1/2/3/4/5/file.jpg,tos://6/7/8/4/5/file.jpg,original-row-key-001\n');

  const deleteResponse = await request(server.baseUrl, `/api/datasets/${datasetId}`, { method: 'DELETE' });
  assert.equal(deleteResponse.status, 200);
  const datasetsResponse = await request(server.baseUrl, '/api/datasets');
  assert.equal(datasetsResponse.status, 200);
  const remaining = await datasetsResponse.json();
  assert.deepEqual(remaining.datasets, []);
  assert.equal(remaining.storage.usedBytes, 0);
  assert.equal((await request(server.baseUrl, `/api/datasets/${datasetId}`)).status, 404);
  assert.equal((await request(server.baseUrl, `/api/uploads/${upload.id}`)).status, 404);
  await assert.rejects(access(path.join(server.dataDir, 'uploads', `${upload.id}.csv`)));
  await assert.rejects(access(path.join(server.dataDir, 'datasets', `${datasetId}.tree.json`)));
  await assert.rejects(access(path.join(server.dataDir, 'exports', `${exportJob.exportId}.csv`)));
});

test('accepts a 500MB CSV upload plan as 8MB verified chunks', async (t) => {
  const server = await startServer();
  t.after(() => server.stop());
  const size = 500 * 1024 * 1024;
  const response = await fetch(`${server.baseUrl}/api/uploads/init`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Session-Id': 'large-upload-test' },
    body: JSON.stringify({ name: 'large.csv', size, fingerprint: 'large.csv:test' }),
  });
  assert.equal(response.status, 201);
  const { upload } = await response.json();
  assert.equal(upload.chunkSize, 8 * 1024 * 1024);
  assert.equal(upload.totalChunks, 63);
});

test('merges verified chunks byte-for-byte before deleting the source parts', async (t) => {
  const server = await startServer();
  t.after(() => server.stop());
  const payload = Buffer.concat([
    Buffer.from('rowkey,path\nrow-001,tos://bucket/'),
    Buffer.alloc(8 * 1024 * 1024, 'x'),
    Buffer.from('/file.csv\n'),
  ]);
  const initResponse = await request(server.baseUrl, '/api/uploads/init', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: 'two-parts.csv', size: payload.length, fingerprint: 'two-parts.csv:test' }),
  });
  const { upload } = await initResponse.json();
  assert.equal(upload.totalChunks, 2);
  for (let index = 0; index < upload.totalChunks; index++) {
    const part = payload.subarray(index * upload.chunkSize, Math.min(payload.length, (index + 1) * upload.chunkSize));
    const response = await request(server.baseUrl, `/api/uploads/${upload.id}/chunks/${index}`, {
      method: 'PUT',
      headers: { 'X-Chunk-SHA256': await sha256(part) },
      body: part,
    });
    assert.equal(response.status, 200);
  }
  const completeResponse = await request(server.baseUrl, `/api/uploads/${upload.id}/complete`, { method: 'POST' });
  assert.equal(completeResponse.status, 201);
  assert.deepEqual(await readFile(path.join(server.dataDir, 'uploads', `${upload.id}.csv`)), payload);
});

test('warns near the local storage limit and blocks another upload beyond it', async (t) => {
  const server = await startServer({ MAX_SESSION_STORAGE_MB: '1' });
  t.after(() => server.stop());
  const csv = Buffer.concat([Buffer.from('rowkey,path\n'), Buffer.alloc(850 * 1024, 'x')]);
  const initResponse = await request(server.baseUrl, '/api/uploads/init', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: 'near-limit.csv', size: csv.length, fingerprint: 'near-limit.csv:test' }),
  });
  const { upload } = await initResponse.json();
  const chunkResponse = await request(server.baseUrl, `/api/uploads/${upload.id}/chunks/0`, {
    method: 'PUT', headers: { 'X-Chunk-SHA256': await sha256(csv) }, body: csv,
  });
  assert.equal(chunkResponse.status, 200);
  assert.equal((await request(server.baseUrl, `/api/uploads/${upload.id}/complete`, { method: 'POST' })).status, 201);
  const summary = await (await request(server.baseUrl, '/api/datasets')).json();
  assert.equal(summary.storage.warning, true);

  const blocked = await request(server.baseUrl, '/api/uploads/init', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: 'another.csv', size: 250 * 1024, fingerprint: 'another.csv:test' }),
  });
  assert.equal(blocked.status, 400);
  assert.match((await blocked.json()).error, /删除/);
});
