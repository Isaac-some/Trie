import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

type Column = { index: number; name: string; pathSampleCount: number; samples: string[] };
type Dataset = {
  id: string; name: string; size: number; columns: Column[]; selectedColumns: number[]; status: 'needs_columns' | 'queued' | 'processing' | 'ready' | 'failed';
  stats?: { processedRows: number; pathCount: number; topLevelFolders: number }; jobId?: string; error?: string; createdAt: string;
};
type Job = { id: string; status: 'queued' | 'running' | 'completed' | 'failed'; progress: Record<string, number>; error?: string };
type TreeItem = { name: string; fullPath: string; leafCount: number; fileCount: number; childCount: number; samples: string[] };
type TreePage = { node: TreeItem & { split?: { branches: number; ratio: number } | null }; children: TreeItem[]; nextOffset: number | null };
type UploadState = { name: string; progress: number; message: string; error?: string };
type Mapping = { source: string; destination: string };

const formatNumber = (value: number) => new Intl.NumberFormat('zh-CN').format(value);
const formatSize = (value: number) => value > 1024 * 1024 ? `${(value / 1024 / 1024).toFixed(1)} MB` : `${Math.ceil(value / 1024)} KB`;
const resumableKey = (file: File) => `trie-upload:${file.name}:${file.size}:${file.lastModified}`;
const pageSessionId = crypto.randomUUID();

window.addEventListener('pagehide', () => {
  void navigator.sendBeacon(`/api/session/close?sessionId=${encodeURIComponent(pageSessionId)}`, new Blob([], { type: 'application/octet-stream' }));
});

async function api<T>(url: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  headers.set('X-Session-Id', pageSessionId);
  const response = await fetch(url, { ...init, headers });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || '请求失败');
  return body as T;
}

async function uploadCsv(file: File, update: (state: Partial<UploadState>) => void) {
  const fingerprint = `${file.name}:${file.size}:${file.lastModified}`;
  const storedId = localStorage.getItem(resumableKey(file));
  let upload: { id: string; chunkSize: number; received: number[]; totalChunks: number };
  if (storedId) {
    try { upload = (await api<{ upload: typeof upload }>('/api/uploads/' + storedId)).upload; }
    catch { localStorage.removeItem(resumableKey(file)); upload = (await api<{ upload: typeof upload }>('/api/uploads/init', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: file.name, size: file.size, fingerprint }) })).upload; }
  } else {
    upload = (await api<{ upload: typeof upload }>('/api/uploads/init', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: file.name, size: file.size, fingerprint }) })).upload;
  }
  localStorage.setItem(resumableKey(file), upload.id);
  let uploadedBytes = upload.received.length * upload.chunkSize;
  for (let index = 0; index < upload.totalChunks; index++) {
    if (upload.received.includes(index)) continue;
    const slice = file.slice(index * upload.chunkSize, Math.min(file.size, (index + 1) * upload.chunkSize));
    update({ message: `上传第 ${index + 1}/${upload.totalChunks} 个分片`, progress: Math.min(98, Math.round(uploadedBytes / file.size * 100)) });
    let lastError: unknown;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const response = await fetch(`/api/uploads/${upload.id}/chunks/${index}`, { method: 'PUT', headers: { 'Content-Type': 'application/octet-stream', 'Content-Length': String(slice.size), 'X-Session-Id': pageSessionId }, body: slice });
        if (!response.ok) throw new Error((await response.json().catch(() => ({}))).error || '分片上传失败');
        lastError = null;
        break;
      } catch (error) { lastError = error; }
    }
    if (lastError) throw lastError;
    uploadedBytes += slice.size;
    update({ progress: Math.round(uploadedBytes / file.size * 100) });
  }
  update({ message: '校验文件并读取表头', progress: 99 });
  const done = await api<{ datasetId: string }>(`/api/uploads/${upload.id}/complete`, { method: 'POST' });
  localStorage.removeItem(resumableKey(file));
  return done.datasetId;
}

export default function App() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [uploads, setUploads] = useState<UploadState[]>([]);
  const fileInput = useRef<HTMLInputElement>(null);

  const refresh = useCallback(async () => {
    try { setDatasets((await api<{ datasets: Dataset[] }>('/api/datasets')).datasets); } catch { /* Server may be starting. */ }
  }, []);

  useEffect(() => { void refresh(); const timer = window.setInterval(() => void refresh(), 2500); return () => window.clearInterval(timer); }, [refresh]);

  const addFiles = useCallback(async (files: FileList | File[]) => {
    const csvFiles = Array.from(files).filter((file) => file.name.toLowerCase().endsWith('.csv'));
    for (const file of csvFiles) {
      setUploads((current) => [...current, { name: file.name, progress: 0, message: '准备上传' }]);
      const update = (patch: Partial<UploadState>) => setUploads((current) => current.map((item) => item.name === file.name ? { ...item, ...patch } : item));
      try {
        await uploadCsv(file, update);
        update({ progress: 100, message: '已上传，等待选择表头' });
      } catch (error) {
        update({ error: error instanceof Error ? error.message : '上传失败', message: '上传中断' });
      }
      await refresh();
    }
  }, [refresh]);

  return <div className="app">
    <header className="app-header"><div><p className="eyebrow">TRANSFER ROUTING</p><h1>传输路径分组</h1><p>可恢复上传、后台处理、按文件导出传输清单</p></div><button className="upload-button" onClick={() => fileInput.current?.click()}>上传 CSV</button></header>
    <input ref={fileInput} className="visually-hidden" type="file" multiple accept=".csv,text/csv" onChange={(event) => { if (event.target.files) void addFiles(event.target.files); event.target.value = ''; }} />
    <main className="layout">
      <aside className="side-panel">
        <section className="drop-zone" onDragOver={(event) => event.preventDefault()} onDrop={(event) => { event.preventDefault(); void addFiles(event.dataTransfer.files); }}>
          <strong>上传多个 CSV</strong><span>单个文件最大 512MB。网络中断后重新选择同一文件，会从已完成分片继续。</span><button onClick={() => fileInput.current?.click()}>选择文件</button>
        </section>
        <section className="side-section"><div className="section-heading"><span>上传队列</span><span>{uploads.length}</span></div>{uploads.length ? uploads.map((item) => <UploadRow key={item.name} upload={item} />) : <p className="quiet">暂无上传任务</p>}</section>
        <section className="side-section"><div className="section-heading"><span>文件输入</span><span>{datasets.length}</span></div>{datasets.length ? datasets.map((dataset, index) => <div className="dataset-summary" key={dataset.id}><span className="input-number">{String(index + 1).padStart(2, '0')}</span><div><strong>{dataset.name}</strong><small>{formatSize(dataset.size)} · {statusLabel(dataset.status)}</small></div></div>) : <p className="quiet">上传完成后按文件名建立独立输入</p>}</section>
      </aside>
      <section className="content"><div className="content-heading"><div><h2>输入与路径树</h2><p>每个 CSV 单独选择表头、生成路径树和导出结果。</p></div><button className="icon-button" aria-label="刷新任务" onClick={() => void refresh()}>↻</button></div>
        {datasets.length ? datasets.map((dataset, index) => <DatasetPanel key={dataset.id} dataset={dataset} number={index + 1} onChange={refresh} />) : <div className="empty">上传 CSV 后，每个文件会在这里出现一棵独立路径树。</div>}
      </section>
    </main>
  </div>;
}

function UploadRow({ upload }: { upload: UploadState }) {
  return <div className="upload-row"><div><strong>{upload.name}</strong><span>{upload.error || upload.message}</span></div><b>{upload.progress}%</b><div className="progress"><i style={{ width: `${upload.progress}%` }} /></div></div>;
}

function DatasetPanel({ dataset, number, onChange }: { dataset: Dataset; number: number; onChange: () => Promise<void> }) {
  const [job, setJob] = useState<Job | null>(null);
  const [expanded, setExpanded] = useState(true);
  useEffect(() => {
    if (!dataset.jobId || !['queued', 'processing'].includes(dataset.status)) { setJob(null); return; }
    let active = true;
    const poll = async () => { try { const result = await api<{ job: Job }>(`/api/jobs/${dataset.jobId}`); if (active) setJob(result.job); } catch { } };
    void poll(); const timer = window.setInterval(() => void poll(), 1200); return () => { active = false; window.clearInterval(timer); };
  }, [dataset.jobId, dataset.status]);
  return <article className="dataset-panel"><header><button className="collapse" onClick={() => setExpanded((value) => !value)} aria-label={expanded ? '收起文件' : '展开文件'}>{expanded ? '−' : '+'}</button><span className="input-number">{String(number).padStart(2, '0')}</span><div className="dataset-title"><h3>{dataset.name}</h3><p>{formatSize(dataset.size)} · {statusLabel(dataset.status)}</p></div>{dataset.stats && <span className="stats">{formatNumber(dataset.stats.pathCount)} 条路径</span>}</header>
    {expanded && <div className="dataset-body">
      {dataset.status === 'needs_columns' && <ColumnSelection dataset={dataset} onChange={onChange} />}
      {['queued', 'processing'].includes(dataset.status) && <JobProgress job={job} />}
      {dataset.status === 'failed' && <div className="error-state">{dataset.error || '处理失败。请重新上传或稍后重试。'}</div>}
      {dataset.status === 'ready' && <DatasetWorkspace dataset={dataset} />}
    </div>}
  </article>;
}

function ColumnSelection({ dataset, onChange }: { dataset: Dataset; onChange: () => Promise<void> }) {
  const [selected, setSelected] = useState<Set<number>>(() => new Set(dataset.columns.filter((column) => column.pathSampleCount > 0).map((column) => column.index)));
  const [saving, setSaving] = useState(false);
  const start = async () => { setSaving(true); try { await api(`/api/datasets/${dataset.id}/process`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ selectedColumns: [...selected] }) }); await onChange(); } finally { setSaving(false); } };
  return <section className="column-selection"><div className="subheading"><div><h4>选择路径表头</h4><p>未勾选的列和表头不会进入路径树。</p></div><button className="primary" disabled={!selected.size || saving} onClick={() => void start()}>{saving ? '已提交' : '开始后台处理'}</button></div><div className="column-list">{dataset.columns.map((column) => <label key={column.index} className={selected.has(column.index) ? 'checked' : ''}><input type="checkbox" checked={selected.has(column.index)} onChange={() => setSelected((previous) => { const next = new Set(previous); next.has(column.index) ? next.delete(column.index) : next.add(column.index); return next; })} /><b>{column.name}</b><span>{column.pathSampleCount ? `${column.pathSampleCount}/30 条样本含路径` : '样本未发现路径'}</span><small title={column.samples.join('\n')}>{column.samples.join(' · ') || '空列'}</small></label>)}</div></section>;
}

function JobProgress({ job }: { job: Job | null }) { const count = job?.progress.pathCount ?? job?.progress.rows ?? 0; return <div className="job-progress"><div className="spinner" /><div><strong>{job?.status === 'queued' ? '正在排队' : '后台处理中'}</strong><p>已读取 {formatNumber(job?.progress.processedRows ?? 0)} 行，已生成 {formatNumber(count)} 条结果。关闭页面不会中断服务器任务。</p></div></div>; }

function DatasetWorkspace({ dataset }: { dataset: Dataset }) {
  const [selected, setSelected] = useState<string[]>([]);
  const [defaultDestination, setDefaultDestination] = useState('');
  const [destinations, setDestinations] = useState<Record<string, string>>({});
  const [exportId, setExportId] = useState<string | null>(null);
  const [exportJob, setExportJob] = useState<Job | null>(null);
  const toggle = (source: string) => setSelected((previous) => previous.includes(source) ? previous.filter((item) => item !== source) : [...previous.filter((item) => !item.startsWith(source) && !source.startsWith(item)), source]);
  const mapping = useMemo(() => selected.map((source) => ({ source, destination: destinations[source] ?? defaultDestination })), [defaultDestination, destinations, selected]);
  const createExport = async () => { const result = await api<{ exportId: string; jobId: string }>(`/api/datasets/${dataset.id}/exports`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ mappings: mapping.filter((item) => item.destination.trim()) }) }); setExportId(result.exportId); setExportJob({ id: result.jobId, status: 'queued', progress: {} }); };
  useEffect(() => { if (!exportJob || ['completed', 'failed'].includes(exportJob.status)) return; const timer = window.setInterval(() => void api<{ job: Job }>(`/api/jobs/${exportJob.id}`).then((result) => setExportJob(result.job)).catch(() => {}), 1200); return () => window.clearInterval(timer); }, [exportJob]);
  return <div className="workspace-grid"><section className="tree-area"><div className="subheading"><div><h4>路径树</h4><p>每层初始显示 5 个目录，文件名最多显示 20 个样本。</p></div><span>{formatNumber(dataset.stats?.pathCount || 0)} 条路径</span></div><TreeBrowser datasetId={dataset.id} selected={selected} onToggle={toggle} /></section><section className="routing-area"><div className="subheading"><div><h4>交付项与目的地</h4><p>编号与树上选择一致。</p></div><span>{selected.length} 条</span></div><label className="destination-default">默认目的地<input value={defaultDestination} placeholder="tos://target-bucket/delivery/" onChange={(event) => setDefaultDestination(event.target.value)} /></label>{selected.length ? <div className="mapping-list">{mapping.map((item, index) => <div className="mapping" key={item.source}><b>{String(index + 1).padStart(2, '0')}</b><code title={item.source}>{shortPath(item.source)}</code><span>→</span><input value={item.destination} placeholder="填写目的地" onChange={(event) => setDestinations((old) => ({ ...old, [item.source]: event.target.value }))} /></div>)}</div> : <div className="quiet large">从左侧路径树选择目录。</div>}<button className="primary export" disabled={!mapping.some((item) => item.destination.trim()) || Boolean(exportJob && !['completed', 'failed'].includes(exportJob.status))} onClick={() => void createExport()}>生成传输 CSV</button>{exportJob && <div className="export-status">{exportJob.status === 'completed' && exportId ? <a href={`/api/exports/${exportId}/download`}>下载已生成的 CSV</a> : exportJob.status === 'failed' ? '导出失败，请重试。' : `正在生成：${formatNumber(exportJob.progress.rows ?? 0)} 行`}</div>}</section></div>;
}

function TreeBrowser({ datasetId, selected, onToggle }: { datasetId: string; selected: string[]; onToggle: (path: string) => void }) {
  const [page, setPage] = useState<TreePage | null>(null);
  const [loading, setLoading] = useState(true);
  const load = useCallback(async () => { setLoading(true); try { setPage(await api<TreePage>(`/api/datasets/${datasetId}/tree?limit=5`)); } finally { setLoading(false); } }, [datasetId]);
  useEffect(() => { void load(); }, [load]);
  if (loading || !page) return <div className="tree-loading">读取路径树...</div>;
  return <div className="tree"><TreeChildren datasetId={datasetId} page={page} selected={selected} onToggle={onToggle} depth={0} /></div>;
}

function TreeChildren({ datasetId, page, selected, onToggle, depth }: { datasetId: string; page: TreePage; selected: string[]; onToggle: (path: string) => void; depth: number }) {
  const [current, setCurrent] = useState(page);
  const more = async () => { const result = await api<TreePage>(`/api/datasets/${datasetId}/tree?path=${encodeURIComponent(current.node.fullPath)}&offset=${current.children.length}&limit=5`); setCurrent((old) => ({ ...old, children: [...old.children, ...result.children], nextOffset: result.nextOffset })); };
  return <>{current.node.split && <div className="split-hint">关键分叉：{current.node.split.branches} 支，最大/最小约 {current.node.split.ratio.toFixed(1)}:1</div>}{current.children.map((node) => <TreeNode key={node.fullPath} datasetId={datasetId} node={node} selected={selected} onToggle={onToggle} depth={depth} />)}{current.nextOffset !== null && <button className="more" onClick={() => void more()}>再看 5 个目录</button>}</>;
}

function TreeNode({ datasetId, node, selected, onToggle, depth }: { datasetId: string; node: TreeItem; selected: string[]; onToggle: (path: string) => void; depth: number }) {
  const [expanded, setExpanded] = useState(false);
  const [page, setPage] = useState<TreePage | null>(null);
  const number = selected.indexOf(node.fullPath);
  const expand = async () => { if (!page && node.childCount) setPage(await api<TreePage>(`/api/datasets/${datasetId}/tree?path=${encodeURIComponent(node.fullPath)}&limit=5`)); setExpanded((value) => !value); };
  return <div className="tree-node" style={{ marginLeft: depth * 16 }}><div className={number >= 0 ? 'tree-row selected' : 'tree-row'}><button className="expand" disabled={!node.childCount} onClick={() => void expand()}>{node.childCount ? (expanded ? '−' : '+') : '·'}</button><button className="tree-name" title={node.fullPath} onClick={() => void expand()}>{node.name}</button><span className="count">{formatNumber(node.leafCount)}</span><button className="select-node" onClick={() => onToggle(node.fullPath)}>{number >= 0 ? String(number + 1).padStart(2, '0') : '选择'}</button></div>{expanded && <div className="tree-children">{page && <TreeChildren datasetId={datasetId} page={page} selected={selected} onToggle={onToggle} depth={depth + 1} />}{node.samples.length > 0 && <div className="samples"><span>文件名样本</span>{node.samples.slice(0, 5).map((sample) => <code key={sample} title={sample}>{sample}</code>)}</div>}</div>}</div>;
}

function statusLabel(status: Dataset['status']) { return ({ needs_columns: '等待选择表头', queued: '等待处理', processing: '处理中', ready: '路径树已完成', failed: '处理失败' })[status]; }
function shortPath(value: string) { const protocol = value.match(/^.*?:\/\//)?.[0] ?? ''; const parts = value.slice(protocol.length).split('/').filter(Boolean); return parts.length > 4 ? `${protocol}${parts.slice(0, 2).join('/')}/.../${parts.slice(-2).join('/')}/` : value; }
