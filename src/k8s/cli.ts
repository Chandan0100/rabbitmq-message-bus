#!/usr/bin/env node
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { extname, join, resolve } from 'node:path';
import { parseArgs } from 'node:util';
import { parse } from 'yaml';
import { generateWorkerManifests, WorkerManifestsConfig } from './manifests';

const USAGE = `Usage:
  rabbitmq-bus-k8s generate --config <file> [--out <dir>]

Options:
  --config <file>   Worker definitions: a .json, .yml/.yaml, or .js/.cjs file
  --out <dir>       Where the manifests are written. Printed to stdout when omitted`;

function loadConfig(path: string): WorkerManifestsConfig {
  const absolute = resolve(path);

  switch (extname(absolute)) {
    case '.json':
      return JSON.parse(readFileSync(absolute, 'utf8'));
    case '.yml':
    case '.yaml':
      return parse(readFileSync(absolute, 'utf8'));
    default: {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const loaded = require(absolute);
      return loaded.default ?? loaded;
    }
  }
}

function main(argv: string[]): void {
  const { values, positionals } = parseArgs({
    args: argv,
    options: {
      config: { type: 'string' },
      out: { type: 'string' },
      help: { type: 'boolean', short: 'h' },
    },
    allowPositionals: true,
  });

  if (values.help) {
    console.log(USAGE);
    return;
  }
  if (positionals[0] !== 'generate' || !values.config) {
    throw new Error(`Expected "generate --config <file>".\n\n${USAGE}`);
  }

  const manifests = generateWorkerManifests(loadConfig(values.config));

  if (!values.out) {
    console.log(manifests.map((manifest) => manifest.yaml).join('---\n'));
    return;
  }

  mkdirSync(values.out, { recursive: true });
  for (const manifest of manifests) {
    writeFileSync(join(values.out, manifest.fileName), manifest.yaml);
    console.log(`${manifest.kind.padEnd(10)} ${join(values.out, manifest.fileName)}`);
  }
}

try {
  main(process.argv.slice(2));
} catch (error) {
  console.error(`ERROR: ${(error as Error).message}`);
  process.exit(1);
}
