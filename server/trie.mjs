const PROTOCOL = /^([a-zA-Z][a-zA-Z0-9+.-]*:\/\/)/;

function node(name, fullPath) {
  return { name, fullPath, leafCount: 0, fileCount: 0, samples: [], children: {} };
}

export function createTree() {
  return node('root', '');
}

export function addPath(root, rawPath) {
  const path = rawPath.trim();
  const protocolMatch = path.match(PROTOCOL);
  const protocol = protocolMatch?.[1] ?? '';
  let remaining = protocol ? path.slice(protocol.length) : path;
  const trailingSlash = remaining.endsWith('/');
  remaining = remaining.replace(/\/+$/, '');
  const parts = remaining.split('/').filter(Boolean);
  if (!parts.length) return;

  root.leafCount++;
  let current = root;
  let fullPath = protocol;
  const folderParts = trailingSlash ? parts : parts.slice(0, -1);
  for (const part of folderParts) {
    fullPath += `${part}/`;
    if (!current.children[part]) current.children[part] = node(part, fullPath);
    current = current.children[part];
    current.leafCount++;
  }

  if (!trailingSlash) {
    const fileName = parts.at(-1);
    current.fileCount++;
    if (current.samples.length < 20 && !current.samples.includes(fileName)) current.samples.push(fileName);
  }
}

export function readNode(root, selectedPath = '') {
  if (!selectedPath) return root;
  const protocolMatch = selectedPath.match(PROTOCOL);
  const protocol = protocolMatch?.[1] ?? '';
  const parts = (protocol ? selectedPath.slice(protocol.length) : selectedPath).split('/').filter(Boolean);
  let current = root;
  for (const part of parts) {
    current = current.children[part];
    if (!current) return null;
  }
  return current;
}

export function treePage(root, selectedPath, offset = 0, limit = 5) {
  const current = readNode(root, selectedPath);
  if (!current) return null;
  const children = Object.values(current.children).sort((left, right) => right.leafCount - left.leafCount || left.name.localeCompare(right.name));
  const counts = children.map((child) => child.leafCount).filter(Boolean);
  const largest = counts.length ? Math.max(...counts) : 0;
  const smallest = counts.length ? Math.min(...counts) : 0;
  return {
    node: {
      name: current.name,
      fullPath: current.fullPath,
      leafCount: current.leafCount,
      fileCount: current.fileCount,
      samples: current.samples.slice(0, 20),
      childCount: children.length,
      split: counts.length > 1 && largest / smallest >= 3 ? { branches: counts.length, ratio: largest / smallest } : null,
    },
    children: children.slice(offset, offset + limit).map((child) => ({
      name: child.name,
      fullPath: child.fullPath,
      leafCount: child.leafCount,
      fileCount: child.fileCount,
      childCount: Object.keys(child.children).length,
      samples: child.samples.slice(0, 5),
    })),
    nextOffset: offset + limit < children.length ? offset + limit : null,
  };
}
