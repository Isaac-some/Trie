import { access } from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';

const root = path.resolve(import.meta.dirname, '..');
const requiredNodeMajor = 22;

async function exists(relativePath) {
  try {
    await access(path.join(root, relativePath));
    return true;
  } catch {
    return false;
  }
}

function status(label, ok, detail) {
  console.log(`${ok ? 'OK' : '需要处理'}  ${label}: ${detail}`);
}

const nodeMajor = Number(process.versions.node.split('.')[0]);
const dependenciesInstalled = await exists('node_modules/react/package.json');
const buildExists = await exists('dist/index.html');

console.log('本地运行环境检查');
status('Node.js', nodeMajor >= requiredNodeMajor, `${process.versions.node}（需要 ${requiredNodeMajor} 或更高版本）`);
status('npm', Boolean(process.env.npm_config_user_agent), process.env.npm_config_user_agent?.match(/npm\/[^ ]+/)?.[0] || '请通过 npm 运行此检查');
status('项目依赖', dependenciesInstalled, dependenciesInstalled ? '已安装' : '尚未安装');
status('前端构建', buildExists, buildExists ? '已生成' : '首次启动会自动生成');
status('本地访问', true, `仅监听本机 127.0.0.1；默认从 http://127.0.0.1:${process.env.PORT || 8787} 开始查找可用端口，不向局域网或互联网传输 CSV`);

if (nodeMajor < requiredNodeMajor) process.exitCode = 1;
