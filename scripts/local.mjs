import { access } from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { spawn } from 'node:child_process';
import net from 'node:net';

const root = path.resolve(import.meta.dirname, '..');
const npmCommand = process.platform === 'win32' ? 'npm.cmd' : 'npm';

async function exists(relativePath) {
  try {
    await access(path.join(root, relativePath));
    return true;
  } catch {
    return false;
  }
}

function run(args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(npmCommand, args, { cwd: root, stdio: 'inherit', ...options });
    child.on('error', reject);
    child.on('exit', (code) => code === 0 ? resolve() : reject(new Error(`npm ${args.join(' ')} 退出，状态码 ${code}`)));
  });
}

function availablePort(port) {
  return new Promise((resolve) => {
    const probe = net.createServer();
    probe.once('error', () => resolve(false));
    probe.once('listening', () => probe.close(() => resolve(true)));
    probe.listen({ host: '127.0.0.1', port });
  });
}

async function selectPort() {
  const requested = Number(process.env.PORT || 8787);
  if (Number.isInteger(requested) && requested > 0 && await availablePort(requested)) return requested;
  if (process.env.PORT) throw new Error(`端口 ${process.env.PORT} 已被占用。请换一个端口后重试。`);
  for (let port = 8788; port <= 8797; port++) if (await availablePort(port)) return port;
  throw new Error('127.0.0.1:8787 到 8797 均被占用。请关闭不再使用的本地服务后重试。');
}

const nodeMajor = Number(process.versions.node.split('.')[0]);
if (nodeMajor < 22) {
  console.error(`需要 Node.js 22 或更高版本，当前版本为 ${process.versions.node}。`);
  process.exit(1);
}

if (!await exists('node_modules/react/package.json')) {
  console.log('首次运行：正在安装项目依赖...');
  await run([await exists('package-lock.json') ? 'ci' : 'install']);
}

await run(['run', 'doctor']);
await run(['run', 'build']);
const port = await selectPort();
console.log(`\n服务已准备就绪。请在浏览器打开 http://127.0.0.1:${port}\n`);
await run(['run', 'start'], { env: { ...process.env, PORT: String(port) } });
