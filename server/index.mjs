import { createHash, randomUUID } from 'node:crypto';
import { createReadStream, createWriteStream, existsSync } from 'node:fs';
import { access, rm, stat, writeFile } from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { pipeline } from 'node:stream/promises';
import { csvCell, csvRecords, extractTosPaths, inspectCsv } from './csv.mjs';
import { addPath, createTree, treePage } from './trie.mjs';
import { ensureStore, entityPath, exportFilePath, listEntities, paths, readEntity, treePath, uploadFilePath, uploadPartPath, writeEntity } from './store.mjs';

const port = Number(process.env.PORT || 8787);
const chunkSize = 5 * 1024 * 1024;
const maxUploadSize = 512 * 1024 * 1024;
const activeJobs = new Set();
const appRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const distRoot = path.join(appRoot, 'dist');

function now() { return new Date().toISOString(); }
function id() { return randomUUID(); }
function normalizeTarget(value) { const trimmed = String(value || '').trim(); return trimmed && !trimmed.endsWith('/') ? `${trimmed}/` : trimmed; }

function json(response, status, data) {
  response.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  response.end(JSON.stringify(data));
}

function notFound(response) { json(response, 404, { error: '未找到请求的资源' }); }

async function readJson(request) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > 1024 * 1024) throw new Error('请求内容过大');
    chunks.push(chunk);
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}');
}

async function writeChunk(request, destination) {
  const temporary = `${destination}.${id()}.uploading`;
  await pipeline(request, createWriteStream(temporary, { flags: 'w' }));
  await rm(destination, { force: true });
  await writeFile(destination, await (await import('node:fs/promises')).readFile(temporary));
  await rm(temporary, { force: true });
}

async function hasFile(filePath) {
  try { await access(filePath); return true; } catch { return false; }
}

async function uploadResponse(upload) {
  const totalChunks = Math.ceil(upload.size / upload.chunkSize);
  const received = [];
  for (let index = 0; index < totalChunks; index++) {
    if (await hasFile(uploadPartPath(upload.id, index))) received.push(index);
  }
  return { upload: { ...upload, received, totalChunks } };
}

async function listDatasets() {
  return (await listEntities('datasets'))
    .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
    .map(({ filePath, ...dataset }) => dataset);
}

async function joinParts(upload) {
  const finalPath = uploadFilePath(upload.id);
  await rm(finalPath, { force: true });
  const totalChunks = Math.ceil(upload.size / upload.chunkSize);
  for (let index = 0; index < totalChunks; index++) {
    await pipeline(createReadStream(uploadPartPath(upload.id, index)), createWriteStream(finalPath, { flags: 'a' }));
  }
  return finalPath;
}

async function updateJob(job, patch) {
  Object.assign(job, patch, { updatedAt: now() });
  await writeEntity('jobs', job.id, job);
}

async function processDataset(datasetId, jobId) {
  if (activeJobs.has(jobId)) return;
  activeJobs.add(jobId);
  try {
    const dataset = await readEntity('datasets', datasetId);
    const job = await readEntity('jobs', jobId);
    if (!dataset || !job) return;
    await updateJob(job, { status: 'running', startedAt: now() });
    await writeEntity('datasets', datasetId, { ...dataset, status: 'processing', updatedAt: now() });

    const tree = createTree();
    let processedRows = 0;
    let pathCount = 0;
    let firstRow = true;
    let lastCheckpoint = Date.now();
    for await (const row of csvRecords(dataset.filePath)) {
      if (firstRow) { firstRow = false; continue; }
      processedRows++;
      for (const index of dataset.selectedColumns) {
        for (const sourcePath of extractTosPaths(row[index] || '')) {
          addPath(tree, sourcePath);
          pathCount++;
        }
      }
      if (processedRows % 5000 === 0 && Date.now() - lastCheckpoint > 800) {
        lastCheckpoint = Date.now();
        await updateJob(job, { progress: { processedRows, pathCount } });
      }
    }
    await writeFile(treePath(datasetId), JSON.stringify(tree));
    await updateJob(job, { status: 'completed', progress: { processedRows, pathCount }, completedAt: now() });
    await writeEntity('datasets', datasetId, {
      ...dataset,
      status: 'ready',
      stats: { processedRows, pathCount, topLevelFolders: Object.keys(tree.children).length },
      updatedAt: now(),
    });
  } catch (error) {
    const job = await readEntity('jobs', jobId);
    const dataset = await readEntity('datasets', datasetId);
    if (job) await updateJob(job, { status: 'failed', error: error instanceof Error ? error.message : '处理失败' });
    if (dataset) await writeEntity('datasets', datasetId, { ...dataset, status: 'failed', error: '处理失败，请重试', updatedAt: now() });
  } finally {
    activeJobs.delete(jobId);
  }
}

async function exportDataset(datasetId, exportId, jobId, mappings) {
  if (activeJobs.has(jobId)) return;
  activeJobs.add(jobId);
  try {
    const dataset = await readEntity('datasets', datasetId);
    const exportJob = await readEntity('exports', exportId);
    const job = await readEntity('jobs', jobId);
    if (!dataset || !exportJob || !job) return;
    await updateJob(job, { status: 'running', startedAt: now() });
    const output = createWriteStream(exportFilePath(exportId));
    output.write('src_path,dst_path,rowkey\n');
    let rows = 0;
    let processedRows = 0;
    let firstRow = true;
    for await (const record of csvRecords(dataset.filePath)) {
      if (firstRow) { firstRow = false; continue; }
      processedRows++;
      for (const column of dataset.selectedColumns) {
        for (const sourcePath of extractTosPaths(record[column] || '')) {
          for (const mapping of mappings) {
            if (!sourcePath.startsWith(mapping.source)) continue;
            const destination = `${mapping.destination}${sourcePath.slice(mapping.source.length)}`;
            const rowKey = createHash('sha256').update(`${sourcePath}\n${destination}`).digest('hex');
            if (!output.write(`${csvCell(sourcePath)},${csvCell(destination)},${rowKey}\n`)) await new Promise((resolve) => output.once('drain', resolve));
            rows++;
          }
        }
      }
      if (processedRows % 5000 === 0) await updateJob(job, { progress: { processedRows, rows } });
    }
    await new Promise((resolve, reject) => output.end((error) => error ? reject(error) : resolve()));
    await updateJob(job, { status: 'completed', progress: { processedRows, rows }, completedAt: now() });
    await writeEntity('exports', exportId, { ...exportJob, status: 'ready', rows, completedAt: now() });
  } catch (error) {
    const job = await readEntity('jobs', jobId);
    const exportJob = await readEntity('exports', exportId);
    if (job) await updateJob(job, { status: 'failed', error: error instanceof Error ? error.message : '导出失败' });
    if (exportJob) await writeEntity('exports', exportId, { ...exportJob, status: 'failed', error: '导出失败' });
  } finally {
    activeJobs.delete(jobId);
  }
}

async function handleApi(request, response, url) {
  const parts = url.pathname.split('/').filter(Boolean);
  if (request.method === 'GET' && url.pathname === '/api/health') return json(response, 200, { ok: true });
  if (request.method === 'GET' && url.pathname === '/api/datasets') return json(response, 200, { datasets: await listDatasets() });

  if (request.method === 'POST' && url.pathname === '/api/uploads/init') {
    const body = await readJson(request);
    const name = String(body.name || '').trim();
    const size = Number(body.size || 0);
    const fingerprint = String(body.fingerprint || '');
    if (!name.toLowerCase().endsWith('.csv') || !size || size > maxUploadSize) return json(response, 400, { error: '请上传小于 512MB 的 CSV 文件' });
    const existing = (await listEntities('uploads')).find((item) => item.fingerprint === fingerprint && item.size === size && item.status !== 'completed');
    if (existing) return json(response, 200, await uploadResponse(existing));
    const upload = { id: id(), name, size, fingerprint, chunkSize, status: 'uploading', createdAt: now(), updatedAt: now() };
    await writeEntity('uploads', upload.id, upload);
    return json(response, 201, await uploadResponse(upload));
  }

  if (parts[0] === 'api' && parts[1] === 'uploads' && parts[2]) {
    const upload = await readEntity('uploads', parts[2]);
    if (!upload) return notFound(response);
    if (request.method === 'GET' && parts.length === 3) return json(response, 200, await uploadResponse(upload));
    if (request.method === 'PUT' && parts[3] === 'chunks' && parts[4] !== undefined) {
      const index = Number(parts[4]);
      const totalChunks = Math.ceil(upload.size / upload.chunkSize);
      if (!Number.isInteger(index) || index < 0 || index >= totalChunks) return json(response, 400, { error: '分片编号无效' });
      const expected = index === totalChunks - 1 ? upload.size - index * upload.chunkSize : upload.chunkSize;
      const contentLength = Number(request.headers['content-length'] || 0);
      if (contentLength && contentLength !== expected) return json(response, 400, { error: '分片大小不正确' });
      await writeChunk(request, uploadPartPath(upload.id, index));
      await writeEntity('uploads', upload.id, { ...upload, updatedAt: now() });
      return json(response, 200, { ok: true, index });
    }
    if (request.method === 'POST' && parts[3] === 'complete') {
      const current = await uploadResponse(upload);
      if (current.upload.received.length !== current.upload.totalChunks) return json(response, 409, { error: '仍有分片未上传完成', ...current });
      if (upload.datasetId) return json(response, 200, { datasetId: upload.datasetId });
      const filePath = await joinParts(upload);
      const columns = await inspectCsv(filePath);
      const dataset = { id: id(), uploadId: upload.id, name: upload.name, filePath, size: upload.size, columns, selectedColumns: [], status: 'needs_columns', createdAt: now(), updatedAt: now() };
      await writeEntity('datasets', dataset.id, dataset);
      await writeEntity('uploads', upload.id, { ...upload, status: 'completed', datasetId: dataset.id, updatedAt: now() });
      return json(response, 201, { datasetId: dataset.id });
    }
  }

  if (parts[0] === 'api' && parts[1] === 'datasets' && parts[2]) {
    const dataset = await readEntity('datasets', parts[2]);
    if (!dataset) return notFound(response);
    if (request.method === 'GET' && parts.length === 3) return json(response, 200, { dataset: { ...dataset, filePath: undefined } });
    if (request.method === 'POST' && parts[3] === 'process') {
      const body = await readJson(request);
      const selectedColumns = [...new Set((body.selectedColumns || []).map(Number).filter((index) => Number.isInteger(index) && index >= 0 && index < dataset.columns.length))];
      if (!selectedColumns.length) return json(response, 400, { error: '至少选择一个路径列' });
      const job = { id: id(), type: 'build_tree', datasetId: dataset.id, status: 'queued', progress: { processedRows: 0, pathCount: 0 }, createdAt: now(), updatedAt: now() };
      await writeEntity('jobs', job.id, job);
      await writeEntity('datasets', dataset.id, { ...dataset, selectedColumns, status: 'queued', jobId: job.id, updatedAt: now() });
      void processDataset(dataset.id, job.id);
      return json(response, 202, { jobId: job.id });
    }
    if (request.method === 'GET' && parts[3] === 'tree') {
      if (dataset.status !== 'ready') return json(response, 409, { error: '路径树尚未准备完成' });
      const root = JSON.parse(await (await import('node:fs/promises')).readFile(treePath(dataset.id), 'utf8'));
      const result = treePage(root, url.searchParams.get('path') || '', Number(url.searchParams.get('offset') || 0), Math.min(20, Math.max(1, Number(url.searchParams.get('limit') || 5))));
      return result ? json(response, 200, result) : notFound(response);
    }
    if (request.method === 'POST' && parts[3] === 'exports') {
      const body = await readJson(request);
      const mappings = (body.mappings || []).map((mapping) => ({ source: String(mapping.source || '').trim(), destination: normalizeTarget(mapping.destination) }))
        .filter((mapping) => mapping.source && mapping.destination);
      if (!mappings.length) return json(response, 400, { error: '请先选择来源前缀并填写目的地' });
      const exportItem = { id: id(), datasetId: dataset.id, name: `${dataset.name.replace(/\.csv$/i, '')}-transfer.csv`, status: 'queued', createdAt: now() };
      const job = { id: id(), type: 'export_csv', datasetId: dataset.id, exportId: exportItem.id, status: 'queued', progress: { processedRows: 0, rows: 0 }, createdAt: now(), updatedAt: now() };
      await writeEntity('exports', exportItem.id, exportItem);
      await writeEntity('jobs', job.id, job);
      void exportDataset(dataset.id, exportItem.id, job.id, mappings);
      return json(response, 202, { exportId: exportItem.id, jobId: job.id });
    }
  }

  if (parts[0] === 'api' && parts[1] === 'jobs' && parts[2] && request.method === 'GET') {
    const job = await readEntity('jobs', parts[2]);
    return job ? json(response, 200, { job }) : notFound(response);
  }
  if (parts[0] === 'api' && parts[1] === 'exports' && parts[2]) {
    const exportItem = await readEntity('exports', parts[2]);
    if (!exportItem) return notFound(response);
    if (request.method === 'GET' && parts[3] === 'download') {
      if (exportItem.status !== 'ready') return json(response, 409, { error: '导出文件尚未准备完成' });
      response.writeHead(200, { 'Content-Type': 'text/csv; charset=utf-8', 'Content-Disposition': `attachment; filename="${encodeURIComponent(exportItem.name)}"` });
      return createReadStream(exportFilePath(exportItem.id)).pipe(response);
    }
    if (request.method === 'GET') return json(response, 200, { export: exportItem });
  }
  return notFound(response);
}

async function staticFile(request, response, url) {
  if (request.method !== 'GET' || !existsSync(distRoot)) return notFound(response);
  const relative = decodeURIComponent(url.pathname === '/' ? '/index.html' : url.pathname).replace(/^\/+/, '');
  const destination = path.resolve(distRoot, relative);
  if (!destination.startsWith(distRoot)) return notFound(response);
  const filePath = existsSync(destination) ? destination : path.join(distRoot, 'index.html');
  const extension = path.extname(filePath);
  const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json' };
  response.writeHead(200, { 'Content-Type': types[extension] || 'application/octet-stream' });
  createReadStream(filePath).pipe(response);
}

await ensureStore();
for (const dataset of await listEntities('datasets')) {
  if (dataset.status === 'processing' || dataset.status === 'queued') {
    const job = await readEntity('jobs', dataset.jobId);
    if (job?.type === 'build_tree') void processDataset(dataset.id, job.id);
  }
}

http.createServer(async (request, response) => {
  try {
    const url = new URL(request.url || '/', `http://${request.headers.host || 'localhost'}`);
    if (url.pathname.startsWith('/api/')) await handleApi(request, response, url);
    else await staticFile(request, response, url);
  } catch (error) {
    console.error(error);
    if (!response.headersSent) json(response, 500, { error: error instanceof Error ? error.message : '服务器处理失败' });
    else response.end();
  }
}).listen(port, '0.0.0.0', () => console.log(`Transfer service listening on ${port}`));
