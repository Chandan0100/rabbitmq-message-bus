import { defineConfig } from 'vitepress';

// Everything that differs between deployments comes from the environment, so
// the same checkout builds for any host.
const base = process.env.DOCS_BASE ?? '/';
const repository = process.env.DOCS_REPOSITORY_URL;
const playgroundUrl = process.env.PLAYGROUND_URL ?? 'http://localhost:4100';

export default defineConfig({
  base,
  lang: 'en',
  title: 'rabbitmq-message-bus',
  description:
    'Reliable RabbitMQ messaging for Node.js and NestJS: consumers with retries and an error queue, an outbox publisher, and worker commands.',
  cleanUrls: true,
  lastUpdated: false,
  // Addresses on the reader's own machine are examples, not pages to check.
  ignoreDeadLinks: 'localhostLinks',
  srcExclude: ['README.md'],
  head: [['link', { rel: 'icon', type: 'image/svg+xml', href: `${base}logo.svg` }]],

  themeConfig: {
    logo: '/logo.svg',
    playgroundUrl,

    nav: [
      { text: 'Guide', link: '/guide/getting-started', activeMatch: '^/guide/(?!configuration|troubleshooting)' },
      { text: 'Reference', link: '/guide/configuration-reference', activeMatch: '^/guide/configuration' },
      { text: 'Troubleshooting', link: '/guide/troubleshooting' },
      { text: 'Playground', link: '/playground' },
    ],

    sidebar: [
      {
        text: 'Introduction',
        items: [
          { text: 'Overview', link: '/guide/' },
          { text: 'Concepts', link: '/guide/concepts' },
          { text: 'Getting started', link: '/guide/getting-started' },
          { text: 'Playground', link: '/playground' },
        ],
      },
      {
        text: 'Guides',
        items: [
          { text: 'Consuming', link: '/guide/consuming' },
          { text: 'Publishing', link: '/guide/publishing' },
          { text: 'Exchange types', link: '/guide/exchange-types' },
          { text: 'NestJS', link: '/guide/nestjs' },
          { text: 'Workers and deployment', link: '/guide/workers-and-deployment' },
        ],
      },
      {
        text: 'Reference',
        items: [
          { text: 'Configuration', link: '/guide/configuration-reference' },
          { text: 'Troubleshooting', link: '/guide/troubleshooting' },
        ],
      },
      {
        text: 'Migration',
        items: [{ text: 'From padmasana-service', link: '/guide/migrating-padmasana-service' }],
      },
    ],

    outline: { level: [2, 3], label: 'On this page' },
    search: { provider: 'local' },
    socialLinks: repository ? [{ icon: 'github', link: repository.replace(/\/(blob|tree)\/.*$/, '') }] : [],
    footer: { message: 'rabbitmq-message-bus documentation' },
  },
});
