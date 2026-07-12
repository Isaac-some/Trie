import { createReadStream } from 'node:fs';

const TOS_PATH = /tos:\/\/[^\s,;"']+/gi;

export async function* csvRecords(filePath) {
  const stream = createReadStream(filePath, { encoding: 'utf8' });
  let cell = '';
  let row = [];
  let quoted = false;
  let skipNextNewline = false;

  for await (const chunk of stream) {
    for (let index = 0; index < chunk.length; index++) {
      const character = chunk[index];
      if (skipNextNewline && character === '\n') {
        skipNextNewline = false;
        continue;
      }
      skipNextNewline = false;

      if (character === '"') {
        if (quoted && chunk[index + 1] === '"') {
          cell += '"';
          index++;
        } else {
          quoted = !quoted;
        }
      } else if (character === ',' && !quoted) {
        row.push(cell);
        cell = '';
      } else if ((character === '\n' || character === '\r') && !quoted) {
        row.push(cell);
        yield row;
        row = [];
        cell = '';
        skipNextNewline = character === '\r';
      } else {
        cell += character;
      }
    }
  }

  if (cell.length || row.length) {
    row.push(cell);
    yield row;
  }
}

export async function inspectCsv(filePath) {
  let headers = [];
  const samples = [];
  let records = 0;
  for await (const row of csvRecords(filePath)) {
    if (!headers.length) {
      headers = row.map((value, index) => value.trim() || `列 ${index + 1}`);
      continue;
    }
    samples.push(row);
    records++;
    if (records >= 30) break;
  }

  return headers.map((name, index) => {
    const values = samples.map((row) => row[index]?.trim() ?? '').filter(Boolean);
    return {
      index,
      name,
      pathSampleCount: values.filter((value) => /^tos:\/\//i.test(value)).length,
      samples: values.slice(0, 3),
    };
  });
}

export function extractTosPaths(value) {
  return [...value.matchAll(TOS_PATH)].map((match) => match[0].replace(/["';]+$/, '').trim());
}

export function csvCell(value) {
  const text = String(value ?? '');
  return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}
