import { mkdir, readFile, readdir, rename, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = process.env.DATA_DIR || path.join(os.tmpdir(), 'trie-transfer-data');
const folders = ['uploads', 'datasets', 'jobs', 'exports'];

export const paths = Object.fromEntries(folders.map((folder) => [folder, path.join(root, folder)]));

export async function ensureStore() {
  await Promise.all([mkdir(root, { recursive: true }), ...folders.map((folder) => mkdir(paths[folder], { recursive: true }))]);
}

export function entityPath(folder, id) {
  return path.join(paths[folder], `${id}.json`);
}

export async function readEntity(folder, id) {
  try {
    return JSON.parse(await readFile(entityPath(folder, id), 'utf8'));
  } catch (error) {
    if (error?.code === 'ENOENT') return null;
    throw error;
  }
}

export async function writeEntity(folder, id, value) {
  const destination = entityPath(folder, id);
  const temporary = `${destination}.${process.pid}.tmp`;
  await writeFile(temporary, JSON.stringify(value, null, 2));
  await rename(temporary, destination);
  return value;
}

export async function listEntities(folder) {
  const entries = await readdir(paths[folder], { withFileTypes: true });
  const values = await Promise.all(entries
    // Tree snapshots are stored beside dataset records as `<id>.tree.json`.
    // Only `<id>.json` is an entity record.
    .filter((entry) => entry.isFile() && /^[^.]+\.json$/.test(entry.name))
    .map((entry) => readEntity(folder, entry.name.slice(0, -5))));
  return values.filter(Boolean);
}

export function uploadPartPath(uploadId, index) {
  return path.join(paths.uploads, `${uploadId}.part.${index}`);
}

export function uploadFilePath(uploadId) {
  return path.join(paths.uploads, `${uploadId}.csv`);
}

export function treePath(datasetId) {
  return path.join(paths.datasets, `${datasetId}.tree.json`);
}

export function exportFilePath(exportId) {
  return path.join(paths.exports, `${exportId}.csv`);
}
