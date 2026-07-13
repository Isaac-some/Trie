import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

type Column = { index: number; name: string; pathSampleCount: number; samples: string[] };
type Dataset = {
  id: string; name: string; size: number; columns: Column[]; selectedColumns: number[]; status: 'needs_columns' | 'queued' | 'processing' | 'ready' | 'failed';
  stats?: { processedRows: number; pathCount: number; topLevelFolders: number }; jobId?: string; error?: string; createdAt: string;
};
type Job = { id: string; status: 'queued' | 'running' | 'completed' | 'failed'; progress: Record<string, number>; error?: string };
type TreeItem = { name: string; fullPath: string; leafCount: number; fileCount: number; childCount: number; samples: string[]; autoBranch?: TreeItem[] };
type TreePage = { node: TreeItem & { split?: { branches: number; ratio: number } | null }; children: TreeItem[]; nextOffset: number | null };
type UploadState = { name: string; progress: number; message: string; error?: string };
type Destination = { id: string; path: string };
type Mapping = { source: string; destination: string };
type StorageSummary = { usedBytes: number; limitBytes: number; warning: boolean };

const formatNumber = (value: number) => new Intl.NumberFormat('zh-CN').format(value);
const formatSize = (value: number) => value > 1024 * 1024 ? `${(value / 1024 / 1024).toFixed(1)} MB` : `${Math.ceil(value / 1024)} KB`;
const resumableKey = (file: File) => `trie-upload:${file.name}:${file.size}:${file.lastModified}`;
const sessionKey = 'trie-page-session-id';
const pageSessionId = sessionStorage.getItem(sessionKey) || crypto.randomUUID();
sessionStorage.setItem(sessionKey, pageSessionId);
const sameMappings = (left: Mapping[] = [], right: Mapping[] = []) => left.length === right.length && left.every((mapping, index) => mapping.source === right[index]?.source && mapping.destination === right[index]?.destination);

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
  const chunkSizeAt = (index: number) => Math.min(upload.chunkSize, file.size - index * upload.chunkSize);
  let uploadedBytes = upload.received.reduce((total, index) => total + chunkSizeAt(index), 0);
  const pending = [...Array(upload.totalChunks).keys()].filter((index) => !upload.received.includes(index));
  const sha256 = async (slice: Blob) => Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', await slice.arrayBuffer())), (value) => value.toString(16).padStart(2, '0')).join('');
  const uploadOne = async (index: number) => {
    const slice = file.slice(index * upload.chunkSize, Math.min(file.size, (index + 1) * upload.chunkSize));
    const digest = await sha256(slice);
    let lastError: unknown;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        update({ message: `上传第 ${index + 1}/${upload.totalChunks} 个分片`, progress: Math.min(98, Math.round(uploadedBytes / file.size * 100)) });
        const response = await fetch(`/api/uploads/${upload.id}/chunks/${index}`, { method: 'PUT', headers: { 'Content-Type': 'application/octet-stream', 'Content-Length': String(slice.size), 'X-Chunk-SHA256': digest, 'X-Session-Id': pageSessionId }, body: slice });
        if (!response.ok) throw new Error((await response.json().catch(() => ({}))).error || '分片上传失败');
        uploadedBytes += slice.size;
        update({ progress: Math.min(98, Math.round(uploadedBytes / file.size * 100)) });
        return;
      } catch (error) { lastError = error; }
    }
    throw lastError;
  };
  let next = 0;
  await Promise.all([...Array(Math.min(3, pending.length)).keys()].map(async () => {
    while (next < pending.length) {
      const index = pending[next++];
      await uploadOne(index);
    }
  }));
  update({ message: '校验文件并读取表头', progress: 99 });
  const done = await api<{ datasetId: string }>(`/api/uploads/${upload.id}/complete`, { method: 'POST' });
  localStorage.removeItem(resumableKey(file));
  return done.datasetId;
}

export default function App() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [storage, setStorage] = useState<StorageSummary>({ usedBytes: 0, limitBytes: 1024 * 1024 * 1024, warning: false });
  const [uploads, setUploads] = useState<UploadState[]>([]);
  const [destinations, setDestinations] = useState<Destination[]>([]);
  const [mappingsByDataset, setMappingsByDataset] = useState<Record<string, Mapping[]>>({});
  const [globalExportId, setGlobalExportId] = useState<string | null>(null);
  const [globalExportJob, setGlobalExportJob] = useState<Job | null>(null);
  const [globalExportError, setGlobalExportError] = useState('');
  const [globalDownloading, setGlobalDownloading] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);

  const refresh = useCallback(async () => {
    try {
      const result = await api<{ datasets: Dataset[]; storage: StorageSummary }>('/api/datasets');
      setDatasets(result.datasets);
      setStorage(result.storage);
    } catch { /* Server may be starting. */ }
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

  const addDestination = () => setDestinations((current) => [...current, { id: crypto.randomUUID(), path: '' }]);
  const updateDestination = (id: string, path: string) => setDestinations((current) => current.map((destination) => destination.id === id ? { ...destination, path } : destination));
  const removeDestination = (id: string) => setDestinations((current) => current.filter((destination) => destination.id !== id));
  const deleteDataset = useCallback(async (datasetId: string) => {
    await api(`/api/datasets/${datasetId}`, { method: 'DELETE' });
    setMappingsByDataset((current) => {
      const { [datasetId]: _removed, ...remaining } = current;
      return remaining;
    });
    await refresh();
  }, [refresh]);
  const updateDatasetMappings = useCallback((datasetId: string, mappings: Mapping[]) => setMappingsByDataset((current) => sameMappings(current[datasetId], mappings) ? current : { ...current, [datasetId]: mappings }), []);
  const exportEntries = useMemo(() => datasets.map((dataset) => ({ datasetId: dataset.id, mappings: mappingsByDataset[dataset.id] || [] })).filter((entry) => entry.mappings.length), [datasets, mappingsByDataset]);
  const totalPairs = exportEntries.reduce((total, entry) => total + entry.mappings.filter((mapping) => mapping.destination).length, 0);
  const hasIncompletePairs = exportEntries.some((entry) => entry.mappings.some((mapping) => !mapping.destination));
  const createGlobalExport = async () => {
    setGlobalExportError('');
    try {
      const result = await api<{ exportId: string; jobId: string }>('/api/exports', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ entries: exportEntries }) });
      setGlobalExportId(result.exportId);
      setGlobalExportJob({ id: result.jobId, status: 'queued', progress: {} });
    } catch (error) { setGlobalExportError(error instanceof Error ? error.message : '无法生成汇总 CSV。'); }
  };
  const downloadGlobalExport = async () => {
    if (!globalExportId) return;
    setGlobalDownloading(true);
    setGlobalExportError('');
    try {
      const response = await fetch(`/api/exports/${globalExportId}/download`, { headers: { 'X-Session-Id': pageSessionId } });
      if (!response.ok) { const body = await response.json().catch(() => ({})); throw new Error(body.error || '下载失败，请重新生成 CSV。'); }
      const fileUrl = URL.createObjectURL(await response.blob());
      const link = document.createElement('a');
      link.href = fileUrl;
      link.download = 'transfer-all.csv';
      document.body.append(link);
      link.click();
      link.remove();
      window.setTimeout(() => URL.revokeObjectURL(fileUrl), 0);
    } catch (error) { setGlobalExportError(error instanceof Error ? error.message : '下载失败，请重新生成 CSV。'); }
    finally { setGlobalDownloading(false); }
  };
  useEffect(() => { if (!globalExportJob || ['completed', 'failed'].includes(globalExportJob.status)) return; const timer = window.setInterval(() => void api<{ job: Job }>(`/api/jobs/${globalExportJob.id}`).then((result) => setGlobalExportJob(result.job)).catch(() => {}), 1200); return () => window.clearInterval(timer); }, [globalExportJob]);

  return <div className="app">
    <header className="app-header"><div><p className="eyebrow">TRANSFER ROUTING</p><h1>传输路径分组</h1><p>可恢复上传、后台处理、汇总导出传输清单</p></div><div className="header-actions"><div className="global-export-status" aria-live="polite">{globalExportError ? <span className="header-error">{globalExportError}</span> : globalExportJob?.status === 'completed' ? <span>汇总 CSV 已生成</span> : hasIncompletePairs ? <span>{totalPairs} 个传送对已完成，仍有待匹配项</span> : totalPairs ? <span>{totalPairs} 个传送对待导出</span> : null}</div><button className="upload-button" onClick={() => fileInput.current?.click()}>上传 CSV</button><button className="primary global-export" disabled={!totalPairs || hasIncompletePairs || Boolean(globalExportJob && !['completed', 'failed'].includes(globalExportJob.status))} onClick={() => void createGlobalExport()}>{globalExportJob && !['completed', 'failed'].includes(globalExportJob.status) ? `正在汇总 ${formatNumber(globalExportJob.progress.rows ?? 0)} 行` : `导出全部传送对${totalPairs ? ` (${totalPairs})` : ''}`}</button>{globalExportJob?.status === 'completed' && globalExportId && <button className="secondary-button download-button" disabled={globalDownloading} onClick={() => void downloadGlobalExport()}>{globalDownloading ? '正在下载' : '下载 CSV'}</button>}</div></header>
    <input ref={fileInput} className="visually-hidden" type="file" multiple accept=".csv,text/csv" onChange={(event) => { if (event.target.files) void addFiles(event.target.files); event.target.value = ''; }} />
    <main className="layout">
      <aside className="side-panel">
        <section className="drop-zone" onDragOver={(event) => event.preventDefault()} onDrop={(event) => { event.preventDefault(); void addFiles(event.dataTransfer.files); }}>
          <strong>上传多个 CSV</strong><span>单个文件最大 500MB，本次页面会话合计最大 512MB。每个分片经校验后才合并；中断后重新选择同一文件会继续上传。</span><button onClick={() => fileInput.current?.click()}>选择文件</button>
        </section>
        <section className="side-section"><div className="section-heading"><span>上传队列</span><span>{uploads.length}</span></div>{uploads.length ? uploads.map((item) => <UploadRow key={item.name} upload={item} />) : <p className="quiet">暂无上传任务</p>}</section>
        <section className="side-section"><div className="section-heading"><span>文件输入</span><span>{datasets.length}</span></div>{datasets.length ? datasets.map((dataset, index) => <div className="dataset-summary" key={dataset.id}><span className="input-number">{String(index + 1).padStart(2, '0')}</span><div><strong>{dataset.name}</strong><small>{formatSize(dataset.size)} · {statusLabel(dataset.status)}</small></div></div>) : <p className="quiet">上传完成后按文件名建立独立输入</p>}</section>
        <section className={storage.warning ? 'side-section storage-warning' : 'side-section storage-status'}><div className="section-heading"><span>本机临时数据</span><span>{formatSize(storage.usedBytes)} / {formatSize(storage.limitBytes)}</span></div><p className="quiet">{storage.warning ? '已接近上限，请删除不再使用的文件树。' : '删除文件会一并清理其路径树、后台任务和临时文件。'}</p></section>
      </aside>
      <section className="content"><div className="content-heading"><div><h2>输入与路径树</h2><p>每个 CSV 独立选择起点；传送终点可跨文件复用。</p></div><button className="icon-button" aria-label="刷新任务" onClick={() => void refresh()}>↻</button></div>
        <DestinationLibrary destinations={destinations} onAdd={addDestination} onUpdate={updateDestination} onRemove={removeDestination} />
        {datasets.length ? datasets.map((dataset, index) => <DatasetPanel key={dataset.id} dataset={dataset} number={index + 1} onChange={refresh} onDelete={deleteDataset} destinations={destinations} onMappingsChange={updateDatasetMappings} />) : <div className="empty">上传 CSV 后，每个文件会在这里出现一棵独立路径树。</div>}
      </section>
    </main>
  </div>;
}

function UploadRow({ upload }: { upload: UploadState }) {
  return <div className="upload-row"><div><strong>{upload.name}</strong><span>{upload.error || upload.message}</span></div><b>{upload.progress}%</b><div className="progress"><i style={{ width: `${upload.progress}%` }} /></div></div>;
}

function DestinationLibrary({ destinations, onAdd, onUpdate, onRemove }: { destinations: Destination[]; onAdd: () => void; onUpdate: (id: string, path: string) => void; onRemove: (id: string) => void }) {
  return <section className="destination-library" aria-labelledby="destination-library-title"><header><div><h3 id="destination-library-title">传送终点库</h3><p>所有路径树共用；在各自的起点分支中选择即可。</p></div><button className="secondary-button" onClick={onAdd}>+ 添加终点</button></header>{destinations.length ? <div className="destination-library-list">{destinations.map((destination, index) => <label className="destination-row" key={destination.id}><span>终点 {String(index + 1).padStart(2, '0')}</span><input value={destination.path} placeholder="cos://bucket/prefix/" onChange={(event) => onUpdate(destination.id, event.target.value)} /><button className="remove-button" type="button" onClick={() => onRemove(destination.id)}>移除</button></label>)}</div> : <p className="quiet">先添加一个传送终点，再在每个 CSV 的起点分支中选择它。</p>}</section>;
}

function DatasetPanel({ dataset, number, onChange, onDelete, destinations, onMappingsChange }: { dataset: Dataset; number: number; onChange: () => Promise<void>; onDelete: (datasetId: string) => Promise<void>; destinations: Destination[]; onMappingsChange: (datasetId: string, mappings: Mapping[]) => void }) {
  const [job, setJob] = useState<Job | null>(null);
  const [expanded, setExpanded] = useState(true);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState('');
  useEffect(() => {
    if (!dataset.jobId || !['queued', 'processing'].includes(dataset.status)) { setJob(null); return; }
    let active = true;
    const poll = async () => { try { const result = await api<{ job: Job }>(`/api/jobs/${dataset.jobId}`); if (active) setJob(result.job); } catch { } };
    void poll(); const timer = window.setInterval(() => void poll(), 1200); return () => { active = false; window.clearInterval(timer); };
  }, [dataset.jobId, dataset.status]);
  const remove = async () => {
    if (!window.confirm(`确定删除“${dataset.name}”吗？该文件的路径树和未下载导出结果会一并删除。`)) return;
    setDeleting(true);
    setDeleteError('');
    try { await onDelete(dataset.id); }
    catch (error) { setDeleteError(error instanceof Error ? error.message : '删除失败，请重试。'); }
    finally { setDeleting(false); }
  };
  return <article className="dataset-panel"><header><button className="collapse" disabled={deleting} onClick={() => setExpanded((value) => !value)} aria-label={expanded ? '收起文件' : '展开文件'}>{expanded ? '−' : '+'}</button><span className="input-number">{String(number).padStart(2, '0')}</span><div className="dataset-title"><h3>{dataset.name}</h3><p>{formatSize(dataset.size)} · {statusLabel(dataset.status)}</p></div>{dataset.stats && <span className="stats">{formatNumber(dataset.stats.pathCount)} 条路径</span>}<div className="dataset-actions"><button className="remove-button" disabled={deleting} onClick={() => void remove()}>{deleting ? '删除中' : '删除文件'}</button></div></header>
    {expanded && <div className="dataset-body">
      {deleteError && <div className="error-state">{deleteError}</div>}
      {dataset.status === 'needs_columns' && <ColumnSelection dataset={dataset} onChange={onChange} />}
      {['queued', 'processing'].includes(dataset.status) && <JobProgress job={job} />}
      {dataset.status === 'failed' && <div className="error-state">{dataset.error || '处理失败。请重新上传或稍后重试。'}</div>}
      {dataset.status === 'ready' && <DatasetWorkspace dataset={dataset} destinations={destinations} onMappingsChange={onMappingsChange} />}
    </div>}
  </article>;
}

function ColumnSelection({ dataset, onChange }: { dataset: Dataset; onChange: () => Promise<void> }) {
  const [selected, setSelected] = useState<Set<number>>(() => new Set(dataset.columns.filter((column) => column.pathSampleCount > 0).map((column) => column.index)));
  const [saving, setSaving] = useState(false);
  const start = async () => { setSaving(true); try { await api(`/api/datasets/${dataset.id}/process`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ selectedColumns: [...selected] }) }); await onChange(); } finally { setSaving(false); } };
  return <section className="column-selection"><div className="subheading"><div><h4>选择路径表头</h4><p>未勾选的列和表头不会进入路径树。</p></div><button className="primary" disabled={!selected.size || saving} onClick={() => void start()}>{saving ? '已提交' : '开始后台处理'}</button></div><div className="column-list">{dataset.columns.map((column) => <label key={column.index} className={selected.has(column.index) ? 'checked' : ''}><input type="checkbox" checked={selected.has(column.index)} onChange={() => setSelected((previous) => { const next = new Set(previous); next.has(column.index) ? next.delete(column.index) : next.add(column.index); return next; })} /><b>{column.name}</b><span>{column.pathSampleCount ? `${column.pathSampleCount}/30 条样本含路径` : '样本未发现路径'}</span><small title={column.samples.join('\n')}>{column.samples.join(' · ') || '空列'}</small></label>)}</div></section>;
}

function JobProgress({ job }: { job: Job | null }) { const count = job?.progress.pathCount ?? job?.progress.rows ?? 0; return <div className="job-progress"><div className="spinner" /><div><strong>{job?.status === 'queued' ? '正在排队' : '后台处理中'}</strong><p>已读取 {formatNumber(job?.progress.processedRows ?? 0)} 行，已生成 {formatNumber(count)} 条结果。处理仅在本机临时目录进行，关闭页面后会清理该会话文件。</p></div></div>; }

function DatasetWorkspace({ dataset, destinations, onMappingsChange }: { dataset: Dataset; destinations: Destination[]; onMappingsChange: (datasetId: string, mappings: Mapping[]) => void }) {
  const [sources, setSources] = useState<string[]>([]);
  const [assignments, setAssignments] = useState<Record<string, string>>({});
  const [pickerOpen, setPickerOpen] = useState(false);
  const destinationById = useMemo(() => new Map(destinations.map((destination) => [destination.id, destination.path.trim()])), [destinations]);
  const mappings = useMemo<Mapping[]>(() => sources.map((source) => ({ source, destination: destinationById.get(assignments[source]) || '' })), [assignments, destinationById, sources]);
  const pairedGroups = useMemo(() => destinations.map((destination) => ({ destination, sources: sources.filter((source) => assignments[source] === destination.id) })).filter((group) => group.destination.path.trim() && group.sources.length), [assignments, destinations, sources]);
  const allMatched = sources.length > 0 && mappings.every((mapping) => mapping.destination);
  const updateSources = (nextSources: string[]) => {
    setSources(nextSources);
    setAssignments((current) => Object.fromEntries(nextSources.map((source) => [source, current[source] || ''])));
  };
  useEffect(() => { onMappingsChange(dataset.id, mappings); }, [dataset.id, mappings, onMappingsChange]);
  return <section className="mapping-workspace"><header className="mapping-header"><div><h4>起止点生成器</h4><p>从这棵路径树选择起点，再从上方终点库完成匹配。</p></div><span>{sources.length} 个起点 / {pairedGroups.length} 组传送对</span></header><div className="mapping-columns"><section className="source-area"><div className="mapping-section-title"><div><h5>起点分支</h5><p>从路径树中挑选要传送的目录。</p></div><button className="secondary-button" onClick={() => setPickerOpen(true)}>选择分支</button></div>{sources.length ? <div className="source-list">{sources.map((source, index) => <div className={destinationById.get(assignments[source]) ? 'source-row is-matched' : 'source-row'} key={source}><b>{String(index + 1).padStart(2, '0')}</b><div><code title={source}>{shortPath(source)}</code><label>传送到<select value={assignments[source] || ''} onChange={(event) => setAssignments((current) => ({ ...current, [source]: event.target.value }))}><option value="">选择终点</option>{destinations.map((destination) => <option key={destination.id} value={destination.id} disabled={!destination.path.trim()}>{destination.path.trim() || '未填写的终点'}</option>)}</select></label></div><button className="remove-button" onClick={() => updateSources(sources.filter((item) => item !== source))}>移除</button></div>)}</div> : <div className="quiet large">还没有起点分支。点击“选择分支”开始。</div>}</section><section className="route-preview-area" aria-label="已匹配传送对"><div className="mapping-section-title"><div><h5>已匹配传送对</h5><p>完成选择后会在这里生成起点到终点的关系。</p></div>{pairedGroups.length > 0 && <span className="pair-count">{pairedGroups.length} 组</span>}</div>{pairedGroups.length ? <PairGroups groups={pairedGroups} /> : <div className="quiet large">选择起点并指定终点后，这里会显示传送对。</div>}</section></div><footer className="mapping-footer"><div><strong>{allMatched ? '本树的传送对已汇入顶部的统一导出。' : '每个起点都需要选择一个已填写的终点。'}</strong></div></footer><BranchPickerDialog open={pickerOpen} datasetId={dataset.id} selected={sources} onCancel={() => setPickerOpen(false)} onConfirm={(nextSources) => { updateSources(nextSources); setPickerOpen(false); }} /></section>;
}

function PairGroups({ groups }: { groups: Array<{ destination: Destination; sources: string[] }> }) {
  return <div className="pair-list">{groups.map((group, index) => <article className="pair-group" key={group.destination.id}><header><strong>传送对 {String(index + 1).padStart(2, '0')}</strong><span>{group.sources.length} 个起点</span></header><div className="pair-route"><div className="pair-endpoint"><span>起点</span><div className="pair-source-chips">{group.sources.map((source) => <code key={source} title={source}>{shortPath(source)}</code>)}</div></div><div className="pair-arrow" aria-hidden="true">→</div><div className="pair-endpoint pair-destination"><span>终点</span><code title={group.destination.path.trim()}>{group.destination.path.trim()}</code></div></div></article>)}</div>;
}

function BranchPickerDialog({ open, datasetId, selected, onCancel, onConfirm }: { open: boolean; datasetId: string; selected: string[]; onCancel: () => void; onConfirm: (sources: string[]) => void }) {
  const [pending, setPending] = useState(selected);
  useEffect(() => { if (open) setPending(selected); }, [open, selected]);
  useEffect(() => { if (!open) return; const close = (event: KeyboardEvent) => { if (event.key === 'Escape') onCancel(); }; window.addEventListener('keydown', close); return () => window.removeEventListener('keydown', close); }, [onCancel, open]);
  if (!open) return null;
  const toggle = (source: string) => setPending((current) => current.includes(source) ? current.filter((item) => item !== source) : [...current.filter((item) => !item.startsWith(source) && !source.startsWith(item)), source]);
  return <div className="branch-dialog-backdrop" role="presentation"><section className="branch-dialog" role="dialog" aria-modal="true" aria-labelledby="branch-dialog-title"><header><div><p className="eyebrow">选择起点</p><h4 id="branch-dialog-title">从完整路径树选择分支</h4><p>已选择 {pending.length} 个分支；上层与下层不能同时选择，避免重复输出。</p></div><button className="remove-button" onClick={onCancel}>取消</button></header><div className="branch-dialog-tree"><TreeBrowser datasetId={datasetId} selected={pending} onToggle={toggle} /></div><footer><span>{pending.length ? `将建立 ${pending.length} 个待匹配起点` : '请选择至少一个分支'}</span><div><button className="secondary-button" onClick={onCancel}>返回</button><button className="primary" disabled={!pending.length} onClick={() => onConfirm(pending)}>确认起点</button></div></footer></section></div>;
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
  const automaticChildren = node.autoBranch || [];
  const [expanded, setExpanded] = useState(automaticChildren.length > 0);
  const [page, setPage] = useState<TreePage | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState('');
  const number = selected.indexOf(node.fullPath);
  const expand = async () => {
    if (!node.childCount) return;
    if (expanded) { setExpanded(false); return; }
    if (automaticChildren.length || page) { setExpanded(true); return; }
    setLoading(true);
    setLoadError('');
    try {
      setPage(await api<TreePage>(`/api/datasets/${datasetId}/tree?path=${encodeURIComponent(node.fullPath)}&limit=5`));
      setExpanded(true);
    } catch (error) { setLoadError(error instanceof Error ? error.message : '无法读取下级目录'); }
    finally { setLoading(false); }
  };
  return <div className="tree-node" style={{ marginLeft: depth * 16 }}><div className={number >= 0 ? 'tree-row selected' : 'tree-row'}><button className="expand" disabled={!node.childCount || loading} aria-label={expanded ? `收起 ${node.name}` : `展开 ${node.name}`} aria-expanded={node.childCount ? expanded : undefined} onClick={() => void expand()}>{loading ? '…' : node.childCount ? (expanded ? '−' : '+') : '·'}</button><button className="tree-name" title={node.fullPath} onClick={() => void expand()}>{node.name}</button><span className="count">{formatNumber(node.leafCount)}</span><button className="select-node" onClick={() => onToggle(node.fullPath)}>{number >= 0 ? `已选 ${String(number + 1).padStart(2, '0')}` : '选择分支'}</button></div>{loadError && <div className="tree-error" role="alert">{loadError}<button onClick={() => void expand()}>重试</button></div>}{expanded && <div className="tree-children">{automaticChildren.length ? automaticChildren.map((child) => <TreeNode key={child.fullPath} datasetId={datasetId} node={child} selected={selected} onToggle={onToggle} depth={depth + 1} />) : page && <TreeChildren datasetId={datasetId} page={page} selected={selected} onToggle={onToggle} depth={depth + 1} />}{node.samples.length > 0 && <div className="samples"><span>文件名样本</span>{node.samples.slice(0, 5).map((sample) => <code key={sample} title={sample}>{sample}</code>)}</div>}</div>}</div>;
}

function statusLabel(status: Dataset['status']) { return ({ needs_columns: '等待选择表头', queued: '等待处理', processing: '处理中', ready: '路径树已完成', failed: '处理失败' })[status]; }
function shortPath(value: string) { const protocol = value.match(/^.*?:\/\//)?.[0] ?? ''; const parts = value.slice(protocol.length).split('/').filter(Boolean); return parts.length > 4 ? `${protocol}${parts.slice(0, 2).join('/')}/.../${parts.slice(-2).join('/')}/` : value; }
