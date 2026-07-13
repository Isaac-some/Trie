import { access } from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { spawn } from 'node:child_process';

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

function run(args) {
  return new Promise((resolve, reject) => {
    const child = spawn(npmCommand, args, { cwd: root, stdio: 'inherit' });
    child.on('error', reject);
    child.on('exit', (code) => code === 0 ? resolve() : reject(new Error(`npm ${args.join(' ')} 退出，状态码 ${code}`)));
  });
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
console.log('\n服务已准备就绪。请在浏览器打开 http://127.0.0.1:8787\n');
await run(['run', 'start']);
