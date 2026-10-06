/* The dashboard: polls /api/state once a second and streams the log over SSE. */

const $ = (selector) => document.querySelector(selector);

const HANDLERS = {
  email: 'SendTaskAssignedEmailProcessor',
  push: 'SendTaskAssignedPushProcessor',
  completed: 'SendTaskCompletedEmailProcessor',
  audit: 'RecordAuditEntryProcessor',
};

const EXCHANGE_NOTES = {
  topic: 'Routes by pattern. Each queue asks for the routing keys it wants.',
  direct: 'Routes by exact key. The key is the event type, one binding per handled type.',
  fanout: 'Copies every event to every queue. A queue ignores what it has no processor for.',
};

const HANDLER_STATUS = {
  handled: { icon: '✓', tone: 'good', label: 'handled' },
  retrying: { icon: '↻', tone: 'warning', label: 'retrying' },
  failed: { icon: '✕', tone: 'critical', label: 'in error queue' },
  waiting: { icon: '…', tone: 'idle', label: 'waiting' },
  pending: { icon: '○', tone: 'idle', label: 'not published yet' },
};

// Stroke icons on a 24px grid, drawn with the current text color.
const ICONS = {
  flow: '<circle cx="6" cy="19" r="3"/><path d="M9 19h8.5a3.5 3.5 0 0 0 0-7h-11a3.5 3.5 0 0 1 0-7H15"/><circle cx="18" cy="5" r="3"/>',
  code: '<path d="m16 18 6-6-6-6M8 6l-6 6 6 6"/>',
  database: '<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5"/><path d="M3 12a9 3 0 0 0 18 0"/>',
  exchange: '<path d="M8 3 4 7l4 4"/><path d="M4 7h16"/><path d="m16 21 4-4-4-4"/><path d="M20 17H4"/>',
  queue: '<path d="M22 12h-6l-2 3h-4l-2-3H2"/><path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z"/>',
  retry: '<path d="M21 12a9 9 0 1 1-9-9c2.52 0 4.93 1 6.74 2.74L21 8"/><path d="M21 3v5h-5"/>',
  processor: '<rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/><path d="M15 2v2M15 20v2M2 15h2M2 9h2M20 15h2M20 9h2M9 2v2M9 20v2"/>',
  alert: '<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><path d="M12 9v4M12 17h.01"/>',
  play: '<polygon points="6 3 20 12 6 21 6 3"/>',
  stop: '<rect x="5" y="5" width="14" height="14" rx="2"/>',
  send: '<path d="m22 2-7 20-4-9-9-4Z"/><path d="M22 2 11 13"/>',
  plus: '<path d="M5 12h14M12 5v14"/>',
  user: '<path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/>',
  check: '<path d="M20 6 9 17l-5-5"/>',
  zap: '<path d="M13 2 3 14h9l-1 8 10-12h-9l1-8z"/>',
  copy: '<rect x="8" y="8" width="14" height="14" rx="2"/><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/>',
  help: '<circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3M12 17h.01"/>',
  trash: '<path d="M3 6h18M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>',
  sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/>',
  moon: '<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/>',
  monitor: '<rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/>',
  external: '<path d="M15 3h6v6M10 14 21 3M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>',
  flask: '<path d="M10 2v7.31L4.2 19a2 2 0 0 0 1.72 3h12.16a2 2 0 0 0 1.72-3L14 9.3V2M8.5 2h7M7 16h10"/>',
  sliders: '<path d="M4 21v-7M4 10V3M12 21v-9M12 8V3M20 21v-5M20 12V3M2 14h4M10 8h4M18 16h4"/>',
  mail: '<rect x="2" y="4" width="20" height="16" rx="2"/><path d="m22 7-10 6L2 7"/>',
  terminal: '<path d="m4 17 6-6-6-6M12 19h8"/>',
  file: '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6"/>',
};

const icon = (name) => `<svg class="ico" viewBox="0 0 24 24" aria-hidden="true">${ICONS[name]}</svg>`;

let state = null;
let count = 1;
let selectedMessage = null;
let logSource = 'all';
let followLog = true;
let busy = false;

// --- helpers ---------------------------------------------------------------

const escapeHtml = (value) =>
  String(value ?? '').replace(/[&<>"']/g, (char) => `&#${char.charCodeAt(0)};`);

const shortId = (id) => (id ? id.slice(0, 8) : '');
const shortName = (name) => name.replace(/Processor$/, '');
const time = (iso) => new Date(iso).toLocaleTimeString([], { hour12: false });
const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;
const number = (value) => (value === null || value === undefined ? '–' : value);

async function api(path, body) {
  const response = await fetch(`/api/${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.message || `Request failed (${response.status})`);
  return payload;
}

function toast(text) {
  const element = document.createElement('div');
  element.className = 'toast';
  element.textContent = text;
  $('#toasts').append(element);
  setTimeout(() => element.remove(), 4500);
}

/** Runs an action, reports a failure, and refreshes straight after. */
async function act(work, done) {
  if (busy) return;
  busy = true;
  try {
    const result = await work();
    if (done) toast(typeof done === 'function' ? done(result) : done);
  } catch (error) {
    toast(error.message);
  } finally {
    busy = false;
    refresh();
  }
}

/** Replaces the content only when it changed, and never under an open dropdown. */
function paint(element, html) {
  if (element.dataset.html === html) return;
  if (element.contains(document.activeElement) && document.activeElement.tagName === 'SELECT') return;
  element.dataset.html = html;
  element.innerHTML = html;
}

function setChecked(container, attribute, value) {
  for (const button of container.querySelectorAll('button')) {
    button.setAttribute('aria-checked', String(button.dataset[attribute] === String(value)));
  }
}

// --- rendering -------------------------------------------------------------

function renderTopbar() {
  paint(
    $('#mode'),
    state.modes
      .map(
        (mode) =>
          `<button role="radio" data-mode="${mode}" aria-checked="${mode === state.mode}">${mode}</button>`,
      )
      .join(''),
  );

  const broker = $('#broker-status');
  broker.className = `status ${state.broker.reachable ? 'good' : 'critical'}`;
  broker.textContent = state.broker.reachable ? 'Connected' : 'Unreachable';
  $('#management-link').href = state.broker.managementUrl;

  const banner = $('#banner');
  banner.hidden = state.broker.reachable;
  banner.innerHTML =
    'Cannot reach RabbitMQ. Start the demo broker with <code>npm run broker</code> in <code>examples/demo</code>; this page picks it up by itself.';

  $('#auto-dispatch').checked = state.autoDispatch;
}

const link = (id, label) =>
  `<div class="link ${label ? 'labelled' : ''}">${
    label ? `<div class="link-label">${label}</div>` : ''
  }<div class="link-line" data-link="${escapeHtml(id)}"></div></div>`;

const nodeHead = (iconName, kind, name, tone = '') =>
  `<div class="node-head">
    <span class="node-icon ${tone}">${icon(iconName)}</span>
    <div><div class="node-kind">${kind}</div>${name ? `<div class="node-name ${name.mono ? 'mono' : ''}">${escapeHtml(name.text)}</div>` : ''}</div>
  </div>`;

function consumerStatus(consumer) {
  if (consumer.state === 'consuming') return '<span class="status good">Consuming</span>';
  if (consumer.state === 'stopped') {
    return consumer.queue.consumers
      ? `<span class="status good">${plural(consumer.queue.consumers, 'external worker')}</span>`
      : '<span class="status">Stopped</span>';
  }
  return `<span class="status warning">${consumer.state === 'starting' ? 'Starting' : 'Reconnecting'}</span>`;
}

function renderFlow() {
  const { eventsExchange, outbox } = state;

  const lanes = state.consumers
    .map((consumer) => {
      const keys = consumer.bindingKeys
        .map((key) => `<code>${escapeHtml(key || '(any)')}</code>`)
        .join('');
      const retrying = consumer.retryQueue.messages > 0;

      const processors = consumer.handlers
        .map(
          (handler) => `
            <div class="processor" data-node="proc:${escapeHtml(handler.name)}">
              <span class="processor-icon">${icon('processor')}</span>
              <div class="processor-text">
                <div class="processor-name" title="${escapeHtml(handler.name)}">${escapeHtml(shortName(handler.name))}</div>
                <div class="processor-meta">${handler.types
                  .map((type) => `<code>${escapeHtml(type.replace('demo.', ''))}</code>`)
                  .join(' ')} · ${handler.handled} handled</div>
              </div>
              <select data-handler="${escapeHtml(handler.name)}" aria-label="Behaviour of ${escapeHtml(handler.name)}">
                ${Object.entries(state.behaviours)
                  .map(
                    ([value, label]) =>
                      `<option value="${value}" ${value === handler.behaviour ? 'selected' : ''}>${escapeHtml(label)}</option>`,
                  )
                  .join('')}
              </select>
            </div>`,
        )
        .join('');

      return `
        <div class="lane">
          ${link(`in:${consumer.context}`, `<b>${state.mode === 'fanout' ? 'No key needed' : 'Binding keys'}</b>${keys}`)}
          <div class="node" data-node="queue:${escapeHtml(consumer.context)}">
            ${nodeHead('queue', `Queue · ${escapeHtml(consumer.context)}`, { text: consumer.queue.name, mono: true })}
            <div class="stats">
              <div><div class="stat-value">${number(consumer.queue.messages)}</div><div class="stat-label">Ready</div></div>
              <div><div class="stat-value">${number(consumer.queue.consumers)}</div><div class="stat-label">Consumers</div></div>
            </div>
            <div class="subnode" data-node="retry:${escapeHtml(consumer.context)}">
              ${nodeHead('retry', 'Retry queue', null, retrying ? 'warning spin' : '')}
              <div class="stats">
                <div><div class="stat-value">${number(consumer.retryQueue.messages)}</div><div class="stat-label">Waiting to retry</div></div>
              </div>
              ${retrying ? '<div class="node-note"><span class="status warning">Returning after its delay</span></div>' : ''}
            </div>
            <div class="node-foot">${consumerStatus(consumer)}</div>
          </div>
          ${link(`out:${consumer.context}`)}
          <div class="node wide">
            <div class="node-kind">Processors · ${consumer.retry.immediateRetries} immediate and ${consumer.retry.delayedRetries} delayed retries</div>
            ${processors}
          </div>
        </div>`;
    })
    .join('');

  paint(
    $('#flow'),
    `<div class="flow-source">
      <div class="node">
        ${nodeHead('code', 'Your code', { text: 'Tasks API' })}
        <div class="node-note">Saves the task and its event in one transaction.</div>
        <div class="subnode" data-node="outbox">
          ${nodeHead('database', 'Outbox table', null)}
          <div class="stats">
            <div><div class="stat-value">${outbox.pending}</div><div class="stat-label">Pending</div></div>
            <div><div class="stat-value">${outbox.sent}</div><div class="stat-label">Published</div></div>
          </div>
          <div class="node-note">${state.autoDispatch ? 'Dispatched every 3 seconds.' : 'Timer off: dispatch by hand.'}</div>
        </div>
      </div>
      ${link('dispatch', '<b>Dispatcher</b>publishes and waits for the broker to confirm')}
      <div class="node exchange" data-node="exchange">
        ${nodeHead('exchange', `Exchange <span class="tag">${escapeHtml(eventsExchange.type)}</span>`, { text: eventsExchange.name, mono: true })}
        <div class="node-note">${EXCHANGE_NOTES[eventsExchange.type]}</div>
      </div>
      <div class="link stub"><div class="link-line"></div></div>
    </div>
    <div class="flow-lanes">${lanes}</div>`,
  );

  const failed = state.errorQueue.messages;
  paint(
    $('#recovery'),
    `<div class="node">
      ${nodeHead('exchange', `Exchange <span class="tag">${escapeHtml(state.recoveryExchange.type)}</span>`, { text: state.recoveryExchange.name, mono: true })}
      <div class="node-note">Carries retries and failed messages.</div>
    </div>
    ${link('error', `<b>Routing key</b><code>${escapeHtml(state.errorQueue.routingKey)}</code>`)}
    <div class="node" data-node="error">
      ${nodeHead('alert', 'Error queue', { text: state.errorQueue.name, mono: true }, failed ? 'critical' : '')}
      <div class="stats">
        <div><div class="stat-value">${number(failed)}</div><div class="stat-label">Failed messages</div></div>
      </div>
      ${failed ? '<div class="node-note"><span class="status critical">Needs attention</span></div>' : ''}
    </div>
    <p class="recovery-note">
      When a processor keeps failing, the message waits in its consumer's retry queue and comes back, up to
      ${state.consumers[0].retry.delayedRetries} times with a longer delay each time. After the last retry it lands in the error
      queue. A retry returns to its own queue only, so one consumer's failures never reach the other.
    </p>`,
  );
}

function renderWorkers() {
  if (!state.inlineWorkers) {
    paint(
      $('#workers'),
      `<p class="hint">Workers are not running in this process. Start them yourself:<br />
        <code>npm run handle-messages -- --module notifications</code><br />
        <code>npm run handle-messages -- --module audit</code><br />
        <code>npm run dispatch-messages -- --module tasks</code></p>`,
    );
    return;
  }

  paint(
    $('#workers'),
    state.consumers
      .map((consumer) => {
        const stopped = consumer.state === 'stopped';
        return `<button class="button" data-worker="${consumer.context}" data-worker-action="${stopped ? 'start' : 'stop'}">
          ${icon(stopped ? 'play' : 'stop')}${stopped ? 'Start' : 'Stop'} the ${escapeHtml(consumer.context)} consumer
        </button>`;
      })
      .join(''),
  );
}

function handlerChip(handler) {
  const status = HANDLER_STATUS[handler.status];
  const attempts = handler.attempts > 1 ? ` <small>· ${handler.attempts} attempts</small>` : '';
  const title = `${handler.name}: ${status.label}${handler.lastError ? `. Last error: ${handler.lastError}` : ''}`;

  return `<span class="chip ${status.tone}" title="${escapeHtml(title)}">
    <span class="icon" aria-hidden="true">${status.icon}</span>${escapeHtml(shortName(handler.name))}
    <small>${status.label}</small>${attempts}
  </span>`;
}

function renderMessages() {
  $('#messages-empty').hidden = state.messages.length > 0;
  $('#messages').hidden = state.messages.length === 0;

  paint(
    $('#messages tbody'),
    state.messages
      .map(
        (message) => `
          <tr data-message="${message.messageId}" class="${message.messageId === selectedMessage ? 'selected' : ''}">
            <td class="time">${time(message.createdAt)}</td>
            <td><code>${escapeHtml(message.type.replace('demo.', ''))}</code><br /><small class="mono">${shortId(message.messageId)}</small></td>
            <td>${
              message.outbox === 'SENT'
                ? '<span class="chip good"><span class="icon" aria-hidden="true">✓</span>published</span>'
                : '<span class="chip idle"><span class="icon" aria-hidden="true">○</span>pending</span>'
            }</td>
            <td><div class="chips">${message.handlers.map(handlerChip).join('')}</div></td>
          </tr>`,
      )
      .join(''),
  );
}

function renderFailed() {
  const failed = state.failed;
  $('#failed-empty').hidden = failed.length > 0;
  $('#failed').hidden = failed.length === 0;
  $('#replay-all').hidden = failed.length < 2;

  paint(
    $('#failed tbody'),
    failed
      .map((message) => {
        const errors = [...new Set(message.failure?.errors.map((error) => error.message) ?? [])];
        const last = message.failure?.errors.at(-1)?.failedAt;
        return `
          <tr>
            <td><code>${escapeHtml((message.type ?? '').replace('demo.', ''))}</code><br /><small class="mono">${shortId(message.messageId)}</small></td>
            <td class="mono">${escapeHtml(message.failure?.endpoint)}</td>
            <td>${errors.map(escapeHtml).join('<br />')}</td>
            <td class="time">${last ? time(last) : ''}</td>
            <td class="mono">${escapeHtml(message.failure?.replayExchange)} → ${escapeHtml(message.failure?.replayRoutingKey)}</td>
            <td><button class="button small" data-replay="${message.messageId}">Replay</button></td>
          </tr>`;
      })
      .join(''),
  );
}

function render() {
  renderTopbar();
  renderFlow();
  renderWorkers();
  renderMessages();
  renderFailed();
}

// --- showing movement --------------------------------------------------------

const reducedMotion = matchMedia('(prefers-reduced-motion: reduce)');

/** Sends dots along a connector. They live in an overlay, so a repaint does not cut them short. */
function sendPackets(linkId, { count: howMany = 1, delay = 0, tone = '' } = {}) {
  const line = document.querySelector(`[data-link="${CSS.escape(linkId)}"]`);
  if (!line || reducedMotion.matches || document.hidden) return;

  const host = $('#packets');
  const origin = host.getBoundingClientRect();
  const rect = line.getBoundingClientRect();
  if (rect.width < 8) return;

  for (let index = 0; index < Math.min(howMany, 4); index += 1) {
    const packet = document.createElement('span');
    packet.className = `packet ${tone}`;
    packet.style.left = `${rect.left - origin.left}px`;
    packet.style.top = `${rect.top - origin.top + rect.height / 2}px`;
    host.append(packet);
    packet
      .animate(
        [
          { transform: 'translateX(0)', opacity: 0 },
          { opacity: 1, offset: 0.15 },
          { opacity: 1, offset: 0.85 },
          { transform: `translateX(${rect.width}px)`, opacity: 0 },
        ],
        { duration: 700, delay: delay + index * 150, easing: 'ease-in-out', fill: 'both' },
      )
      .finished.finally(() => packet.remove());
  }
}

/** Rings a step of the diagram whose numbers just changed. */
function pulse(nodeId, { delay = 0, tone = '' } = {}) {
  setTimeout(() => {
    const node = document.querySelector(`[data-node="${CSS.escape(nodeId)}"]`);
    if (!node) return;
    node.classList.remove('pulse', 'warning', 'critical');
    void node.offsetWidth;
    node.classList.add('pulse', ...(tone ? [tone] : []));
  }, delay);
}

/** Whether a topic binding pattern matches a routing key. */
function topicMatches(pattern, key) {
  const source = pattern
    .split('.')
    .map((word) => (word === '#' ? '.*' : word === '*' ? '[^.]+' : word.replace(/[^\w-]/g, '\\$&')))
    .join('\\.');
  return new RegExp(`^${source}$`).test(key);
}

/** Compares two snapshots and plays what happened in between, in the order it happened. */
function animateChanges(previous, next) {
  if (!previous || previous.mode !== next.mode) return;

  const published = next.outbox.sent - previous.outbox.sent;
  if (next.outbox.pending + next.outbox.sent > previous.outbox.pending + previous.outbox.sent) pulse('outbox');

  let newTypes = [];
  if (published > 0) {
    const before = new Map(previous.messages.map((message) => [message.messageId, message.outbox]));
    newTypes = next.messages
      .filter((message) => message.outbox === 'SENT' && before.get(message.messageId) !== 'SENT')
      .map((message) => message.type);
    sendPackets('dispatch', { count: published });
    pulse('exchange', { delay: 600 });
  }

  next.consumers.forEach((consumer, index) => {
    const old = previous.consumers[index];
    if (!old) return;

    const wanted =
      next.mode === 'fanout' ||
      newTypes.length === 0 ||
      newTypes.some(
        (type) =>
          consumer.handlers.some((handler) => handler.types.includes(type)) ||
          consumer.bindingKeys.some((key) => topicMatches(key, type)),
      );
    const queued = (consumer.queue.messages ?? 0) - (old.queue.messages ?? 0);
    if ((published > 0 && wanted) || queued > 0) {
      sendPackets(`in:${consumer.context}`, { count: Math.max(published, queued), delay: published > 0 ? 650 : 0 });
      pulse(`queue:${consumer.context}`, { delay: published > 0 ? 1300 : 650 });
    }

    let handled = 0;
    consumer.handlers.forEach((handler) => {
      const done = handler.handled - (old.handlers.find((other) => other.name === handler.name)?.handled ?? 0);
      if (done <= 0) return;
      handled += done;
      pulse(`proc:${handler.name}`, { delay: published > 0 ? 2000 : 700 });
    });
    if (handled > 0) sendPackets(`out:${consumer.context}`, { count: handled, delay: published > 0 ? 1350 : 0 });

    const retrying = (consumer.retryQueue.messages ?? 0) - (old.retryQueue.messages ?? 0);
    if (retrying > 0) pulse(`retry:${consumer.context}`, { tone: 'warning' });
    if (retrying < 0) pulse(`queue:${consumer.context}`);
  });

  const failed = (next.errorQueue.messages ?? 0) - (previous.errorQueue.messages ?? 0);
  if (failed > 0) {
    sendPackets('error', { count: failed, tone: 'critical' });
    pulse('error', { delay: 650, tone: 'critical' });
  }
}

async function refresh() {
  try {
    const previous = state;
    state = await api('state');
    render();
    animateChanges(previous, state);
  } catch {
    $('#banner').hidden = false;
    $('#banner').textContent = 'The demo API is not answering. Is `npm start` still running?';
  }
}

// --- live log --------------------------------------------------------------

const logGroup = (source) => (source === 'handler' || source === 'demo' ? source : 'library');

function logLineVisible(line) {
  const sourceOk = logSource === 'all' || line.dataset.group === logSource;
  const messageOk = !selectedMessage || line.dataset.message === selectedMessage;
  return sourceOk && messageOk;
}

function applyLogFilters() {
  for (const line of $('#log').children) line.hidden = !logLineVisible(line);

  $('#log-caption').innerHTML = selectedMessage
    ? `Only message <code>${shortId(selectedMessage)}</code>. <a href="#" id="clear-message">Show everything</a>`
    : 'What the library and the processors report, from every process.';
  $('#log').scrollTop = $('#log').scrollHeight;
  updateLogCount();
}

function updateLogCount() {
  const lines = [...$('#log').children];
  const visible = lines.filter((line) => !line.hidden).length;
  $('#log-count').textContent =
    visible === lines.length ? plural(lines.length, 'line') : `${visible} of ${plural(lines.length, 'line')}`;
}

function setFollow(follow) {
  followLog = follow;
  $('#log-follow').setAttribute('aria-pressed', String(follow));
  $('#log-prompt').textContent = follow ? 'following' : 'paused: scroll down or press Follow to resume';
  if (follow) $('#log').scrollTop = $('#log').scrollHeight;
}

const LEVEL_LABELS = { debug: 'DEBUG', info: 'INFO', warn: 'WARN', error: 'ERROR' };

/** Escapes a log text and marks the message ids in it. */
const logText = (text) =>
  escapeHtml(text).replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g, '<span class="id">$&</span>');

function appendLog(event) {
  const log = $('#log');
  const line = document.createElement('div');
  line.className = `log-line fresh ${event.level}`;
  line.dataset.group = logGroup(event.source);
  line.dataset.message = event.message_id ?? '';
  line.innerHTML = `<span class="at">${time(event.at)}</span><span class="level">${LEVEL_LABELS[event.level] ?? escapeHtml(String(event.level).toUpperCase())}</span><span class="source">${escapeHtml(event.source)}</span><span class="text">${logText(event.text)}</span>`;
  line.hidden = !logLineVisible(line);
  log.append(line);

  // The latest line also shows under the diagram, so you can watch both at once.
  $('#ticker').textContent = `${time(event.at)}  ${event.source}  ${event.text}`;
  $('#ticker').className = `ticker ${event.level}`;

  while (log.children.length > 600) log.firstChild.remove();
  if (followLog) log.scrollTop = log.scrollHeight;
  updateLogCount();
}

function connectLog() {
  const source = new EventSource('/api/events');
  source.onmessage = (message) => appendLog(JSON.parse(message.data));
}

// --- guided experiments ----------------------------------------------------

async function behave(overrides = {}) {
  for (const [key, handler] of Object.entries(HANDLERS)) {
    await api('behaviours', { handler, behaviour: overrides[key] ?? 'succeed' });
  }
}

async function publish(action) {
  await api('tasks', { action, count: 1 });
  await api('dispatch', {});
}

const EXPERIMENTS = [
  {
    title: 'The happy path',
    watch: 'Events cross the outbox and each processor handles them once.',
    run: async () => {
      await behave();
      await publish('complete');
    },
  },
  {
    title: 'A glitch, retried at once',
    watch: 'The email fails, then succeeds in the same delivery. The retry queue stays empty.',
    run: async () => {
      await behave({ email: 'fail-once' });
      await publish('assign');
    },
  },
  {
    title: 'An outage, retried later',
    watch: 'Audit fails twice. Its retry queue holds the message for 2s, then 4s.',
    run: async () => {
      await behave({ audit: 'flaky' });
      await publish('create');
    },
  },
  {
    title: 'One of two processors fails',
    watch: 'The push is retried. The email already succeeded and is not sent again.',
    run: async () => {
      await behave({ push: 'flaky' });
      await publish('assign');
    },
  },
  {
    title: 'A message that can never work',
    watch: 'About 12 seconds later it is in the error queue. Set the processor to "Succeeds", then replay it.',
    run: async () => {
      await behave({ completed: 'always-fail' });
      await publish('complete');
    },
  },
  {
    title: 'The same message twice',
    watch: 'The log says "Duplicate message ignored" for each processor.',
    run: async () => {
      await behave();
      await api('chaos/duplicate', {});
    },
  },
  {
    title: 'A lost connection',
    watch: 'Both consumers reconnect and resume within a few seconds.',
    run: () => api('chaos/kill-connections', {}),
  },
  {
    title: 'Another exchange type',
    watch: 'The binding keys change; the processors and the retry flow do not.',
    run: () => {
      const next = state.modes[(state.modes.indexOf(state.mode) + 1) % state.modes.length];
      return api('settings', { mode: next });
    },
  },
];

function renderExperiments() {
  $('#experiments').innerHTML = EXPERIMENTS.map(
    (experiment, index) => `
      <li>
        <b>${escapeHtml(experiment.title)}</b>
        <span>${escapeHtml(experiment.watch)}</span>
        <button class="button small" data-experiment="${index}">${icon('play')}Run</button>
      </li>`,
  ).join('');
}

// --- events ----------------------------------------------------------------

document.addEventListener('click', (event) => {
  const target = event.target.closest('button, a, tr[data-message]');
  if (!target) return;
  const data = target.dataset;

  if (data.mode) {
    act(() => api('settings', { mode: data.mode }), `Now routing through a ${data.mode} exchange.`);
  } else if (data.themeChoice) {
    setTheme(data.themeChoice);
  } else if (data.count) {
    count = Number(data.count);
    setChecked($('#count'), 'count', count);
  } else if (data.action) {
    act(() => api('tasks', { action: data.action, count }));
  } else if (data.chaos) {
    act(() => api(`chaos/${data.chaos}`, {}), 'Done. Watch the log.');
  } else if (data.worker) {
    act(() => api(`workers/${data.worker}/${data.workerAction}`, {}));
  } else if (data.experiment) {
    const experiment = EXPERIMENTS[Number(data.experiment)];
    act(experiment.run, experiment.watch);
  } else if (data.replay) {
    act(() => api('error-queue/replay', { messageId: data.replay }), 'Replayed to the queue it failed in.');
  } else if (data.source) {
    logSource = data.source;
    setChecked($('#log-filter'), 'source', logSource);
    applyLogFilters();
  } else if (data.message) {
    selectedMessage = selectedMessage === data.message ? null : data.message;
    $('#messages tbody').dataset.html = '';
    renderMessages();
    applyLogFilters();
  } else if (target.id === 'clear-message') {
    event.preventDefault();
    selectedMessage = null;
    $('#messages tbody').dataset.html = '';
    renderMessages();
    applyLogFilters();
  } else if (target.id === 'log-follow') {
    setFollow(!followLog);
  } else if (target.id === 'log-clear') {
    $('#log').innerHTML = '';
    updateLogCount();
  } else if (target.id === 'log-copy') {
    const text = [...$('#log').children]
      .filter((line) => !line.hidden)
      .map((line) => [...line.children].map((cell) => cell.textContent).join('  '))
      .join('\n');
    navigator.clipboard.writeText(text).then(
      () => toast('Copied the visible log lines.'),
      () => toast('The browser refused to copy.'),
    );
  } else if (target.id === 'dispatch') {
    act(
      () => api('dispatch', {}),
      (result) => (result.fetched ? `Published ${result.published} of ${result.fetched}.` : 'The outbox is empty.'),
    );
  } else if (target.id === 'replay-all') {
    act(() => api('error-queue/replay', {}), (result) => `Replayed ${plural(result.replayed, 'message')}.`);
  } else if (target.id === 'reset') {
    if (!confirm('Purge the demo queues and clear its tables?')) return;
    act(() => api('reset', {}), 'Queues purged and tables cleared.').then(() => {
      $('#log').innerHTML = '';
      updateLogCount();
    });
  }
});

document.addEventListener('change', (event) => {
  const { target } = event;

  if (target.dataset.handler) {
    target.blur();
    act(() => api('behaviours', { handler: target.dataset.handler, behaviour: target.value }));
  } else if (target.id === 'auto-dispatch') {
    act(() => api('settings', { autoDispatch: target.checked }));
  }
});

// --- theme -------------------------------------------------------------------

/** "auto" follows the system; a choice is remembered in this browser only. */
function setTheme(choice) {
  if (choice === 'auto') delete document.documentElement.dataset.theme;
  else document.documentElement.dataset.theme = choice;
  try {
    if (choice === 'auto') localStorage.removeItem('demo-theme');
    else localStorage.setItem('demo-theme', choice);
  } catch {
    // Private windows may refuse storage; the choice then lasts until reload.
  }
  setChecked($('#theme'), 'themeChoice', choice);
}

for (const element of document.querySelectorAll('[data-icon]')) {
  element.insertAdjacentHTML('afterbegin', icon(element.dataset.icon));
}
setChecked($('#theme'), 'themeChoice', document.documentElement.dataset.theme ?? 'auto');

// Scrolling up pauses following; returning to the bottom resumes it.
$('#log').addEventListener('scroll', () => {
  const log = $('#log');
  const atBottom = log.scrollHeight - log.scrollTop - log.clientHeight < 40;
  if (atBottom !== followLog) setFollow(atBottom);
});
setFollow(true);

renderExperiments();
connectLog();
refresh();
setInterval(refresh, 1000);
