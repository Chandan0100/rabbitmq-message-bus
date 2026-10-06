# Documentation website

The public documentation for `rabbitmq-message-bus`, built with
[VitePress](https://vitepress.dev). It is a static site: the build produces a
folder of HTML, CSS and JavaScript that any web server or static host can
serve. It does not need the library, the playground or a broker to run.

## Work on it

```bash
cd website
npm install
npm run dev        # http://localhost:5173, reloads as you edit
```

## Where the content lives

| Content | File | |
| --- | --- | --- |
| The guides | `../docs/*.md` | The single source. They also ship in the npm package |
| Home page | `index.md` | |
| Playground page | `playground.md` | |
| Navigation, sidebar, search | `.vitepress/config.mts` | |
| Colors | `.vitepress/theme/custom.css` | |
| Logo and screenshots | `public/` | |

`npm run dev` and `npm run build` first copy the guides from `../docs` into
`guide/` (`scripts/sync-docs.mjs`). `guide/` is generated: edit the files in
`../docs`, never the copies. Run `npm run sync` to pick up an edit while the
dev server is running.

A new guide needs two things: the file in `../docs`, and a line in the
`sidebar` of `.vitepress/config.mts`.

## Build

```bash
npm run build      # writes .vitepress/dist
npm run preview    # serves that folder on http://localhost:4200
```

The build fails on a link that leads nowhere, so a broken link never ships.

Three environment variables adapt the build to where it is hosted:

| Variable | Default | |
| --- | --- | --- |
| `DOCS_BASE` | `/` | The path the site is served under. `/rabbitmq-message-bus/` for `https://you.github.io/rabbitmq-message-bus/`. Must start and end with a slash |
| `DOCS_REPOSITORY_URL` | none | For example `https://github.com/you/rabbitmq-message-bus/blob/main`. Links to source files point there and a GitHub icon appears in the top bar. Without it, source files are shown as plain paths |
| `PLAYGROUND_URL` | `http://localhost:4100` | Where "Open the playground" leads. Set it if you host the playground somewhere |

## Deploy

Everything to publish is in `.vitepress/dist` after a build.

**Any static host or web server.** Upload the contents of `.vitepress/dist`.
Pages are written as `guide/consuming.html` and linked as `/guide/consuming`,
so the server must try `.html` when a path has no extension. Most static hosts
do; for nginx see `nginx.conf` here.

**Netlify, Vercel, Cloudflare Pages.** Point the project at the repository and
set:

| Setting | Value |
| --- | --- |
| Base (root) directory | `website` |
| Build command | `npm run build` |
| Output directory | `.vitepress/dist` |

The build reads `../docs`, so the host must check out the whole repository,
not only `website/`. All three do by default.

**GitHub Pages.** Build with `DOCS_BASE=/<repository-name>/` and publish
`.vitepress/dist` with the `actions/upload-pages-artifact` and
`actions/deploy-pages` actions.

**Docker.** Run from the repository root:

```bash
docker build -f website/Dockerfile -t rabbitmq-message-bus-docs .
docker run -p 8080:80 rabbitmq-message-bus-docs      # http://localhost:8080
```

Pass the variables above with `--build-arg`, for example
`--build-arg DOCS_BASE=/docs/`.
