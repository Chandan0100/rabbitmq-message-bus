// Turns the guides in ../docs into the pages of this site.
//
// The Markdown in ../docs stays the single source: it also ships inside the npm
// package and reads well on its own. This script copies it to ./guide and
// changes only what a website needs: page names, titles and links that point
// outside the guides.
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const source = join(root, '..', 'docs');
const target = join(root, 'guide');

/** When set, links to source files point there, e.g. https://github.com/acme/rabbitmq-message-bus/blob/main */
const repository = (process.env.DOCS_REPOSITORY_URL ?? '').replace(/\/$/, '');

if (!existsSync(source)) {
  if (existsSync(target)) {
    console.log('sync-docs: ../docs not found, keeping the pages already in ./guide');
    process.exit(0);
  }
  console.error('sync-docs: ../docs not found and ./guide is empty. Build from a checkout of the whole repository.');
  process.exit(1);
}

const pageName = (file) => (file === 'README.md' ? 'index.md' : file.replace(/^\d+-/, ''));

function convert(markdown) {
  return (
    markdown
      // "# 3. Consuming" becomes "# Consuming": the sidebar gives the order.
      .replace(/^# \d+\. /m, '# ')
      // The list of sections under the title: the site shows it beside the page.
      .replace(/^(# [^\n]*\n\n)(?:- \[[^\n]*\]\(#[^\n]*\)\n)+\n?/, '$1')
      // The site has its own previous / next links.
      .replace(/^Next: \[[^\n]*\n?/m, '')
      .replace(/\[([^\]]*)\]\(([^)\s]+)\)/g, (whole, text, href) => {
        if (/^(https?:|#|\/)/.test(href)) return whole;
        const label = text.replace(/\s+/g, ' ').trim();

        const guide = href.match(/^(?:\.\/)?(\d+-[\w-]+\.md|README\.md)(#.*)?$/);
        if (guide) return `[${label}](./${pageName(guide[1])}${guide[2] ?? ''})`;

        if (href.startsWith('../')) {
          const path = href.slice(3);
          if (path === 'examples/demo/README.md') return `[${label}](/playground)`;
          return repository ? `[${label}](${repository}/${path})` : `\`${path}\``;
        }
        return whole;
      })
  );
}

rmSync(target, { recursive: true, force: true });
mkdirSync(target, { recursive: true });

const files = readdirSync(source).filter((file) => file.endsWith('.md'));
for (const file of files) {
  writeFileSync(join(target, pageName(file)), convert(readFileSync(join(source, file), 'utf8')));
}
console.log(`sync-docs: wrote ${files.length} pages to ./guide`);
