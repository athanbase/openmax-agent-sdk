import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import WebSocket from 'ws';
import { CwsAgentBridge } from './orchestrator.js';
import { CwsHttpClient } from './transport/http.js';
import { createDeduper } from './transport/ws.js';
import { memoryStorage } from './providers.js';

// Keep tests hermetic regardless of ambient CWS env config.
for (const k of ['COCO_API_URL', 'COCO_API_KEY', 'COCO_ORG_ID', 'COCO_USER_TOKEN',
  'COCO_AUTH_TOKEN', 'COCO_API_PREFIX', 'COCO_DEVICE_ID', 'COCO_CLIENT_VERSION', 'COCO_RPC_LOG']) {
  delete process.env[k];
}
process.env.COCO_RPC_LOG = '0';   // silence the http client's stdout RPC log

const quietLogger = { info() {}, warn() {}, error() {}, debug() {} };

// Fetch that routes by URL/method and records every call. Responses are
// D8-wrapped ({data, request_id}) so the http client's envelope unwrap fires.
function routingFetch(routes = []) {
  const calls = [];
  const fn = async (url, opts = {}) => {
    const method = opts.method || 'GET';
    calls.push({ url, method, opts });
    for (const r of routes) {
      if (r.match(url, method)) {
        const status = r.status ?? 200;
        const body = JSON.stringify(r.body ?? { data: r.data ?? {}, request_id: 'r' });
        return mkRes(status, body);
      }
    }
    // Default: empty D8 single envelope.
    return mkRes(200, JSON.stringify({ data: {}, request_id: 'r' }));
  };
  fn.calls = calls;
  return fn;
}
function mkRes(status, body) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: `HTTP ${status}`,
    async text() { return body; },
    async arrayBuffer() { return Buffer.from(body); },
  };
}

// Minimal fake of the `ws` socket the WsClient drives (mirrors ws.test.js).
class FakeWebSocket extends EventEmitter {
  constructor() {
    super();
    this.readyState = WebSocket.OPEN;
    this.closed = false;
    this.terminateCount = 0;
  }
  ping() {}
  terminate() { this.terminateCount += 1; }
  send() {}
  close() { this.closed = true; }
}

function flush(n = 4) {
  // Drain the microtask + immediate queues so the async inbound chain settles.
  return new Promise((resolve) => {
    let i = 0;
    const tick = () => (++i >= n ? resolve() : setImmediate(tick));
    setImmediate(tick);
  });
}

function baseOrg(overrides = {}) {
  return {
    // No `slug` field — orgs are keyed by org_id end to end. The whole suite
    // therefore runs org_id-only; see the dedicated legacy-slug-ignored test.
    org_id: 'org1',
    org_name: 'Org One',
    self: { member_id: 'self1', display_name: 'Bot' },
    owner: { member_id: 'owner1', name: 'Owner' },
    access: { dmPolicy: 'open' },
    ...overrides,
  };
}

function makeHttp(routes) {
  const fetch = routingFetch(routes);
  const http = new CwsHttpClient({ baseUrl: 'http://api.test', fetch, logger: quietLogger });
  http.setApiKey('test-key'); // short-circuit the token manager
  return { http, fetch };
}

function makeBridge({ http, orgConfigs, inbound, callbacks = {}, reporters = {}, ws = {}, storage }) {
  const sockets = [];
  const bridge = new CwsAgentBridge({
    http,
    ws: {
      baseUrl: 'wss://test/ws',
      urlProvider: async () => 'wss://test/ws?ticket=t',
      wsFactory: () => { const s = new FakeWebSocket(); sockets.push(s); return s; },
      ...ws,
    },
    orgConfigs,
    providers: { inbound, logger: quietLogger, ...(storage ? { storage } : {}) },
    // syncSelf reports ready immediately so the self-name hydration barrier
    // resolves on the first attempt (no retry backoff) in tests.
    callbacks: { syncSelf: async () => ({ nameReady: true }), ...callbacks },
    reporters: { metrics: false, frameMetrics: false, markReadOnDeliver: false, ...reporters },
  });
  return { bridge, sockets };
}

const msgFrame = (over = {}) => ({
  type: 'message',
  payload: { id: 'm1', conversation_id: 'c1', sender_id: 'u1', ...over },
});

test('inbound message frame flows dedupe → normalize → access-policy → deliver', async () => {
  const { http, fetch } = makeHttp([
    { match: (u, m) => m === 'GET' && /\/conversations\/c1\/messages\/m1$/.test(u),
      data: { id: 'm1', sender_id: 'u1', seq: 7, type: 'text', inbox_seq: 7,
        content: { content_type: 'text', body: { text: 'hello' }, attachments: [] } } },
    { match: (u, m) => m === 'GET' && /\/conversations\/c1$/.test(u),
      data: { id: 'c1', type: 'dm', name: 'DM' } },
  ]);
  const delivered = [];
  const inbound = { deliver: async (msg, endpoint, priority) => { delivered.push({ msg, endpoint, priority }); return { ok: true }; } };

  const { bridge } = makeBridge({ http, orgConfigs: [baseOrg()], inbound });
  await bridge.start();

  bridge.injectFrame('org1', msgFrame());
  await flush();

  assert.equal(delivered.length, 1, 'deliver called once');
  const d = delivered[0];
  assert.equal(d.msg.conversationId, 'c1');
  assert.equal(d.msg.conversationType, 'dm');
  assert.equal(d.msg.messageId, 'm1');
  assert.equal(d.msg.senderId, 'u1');
  assert.equal(d.msg.text, 'hello', 'normalized text extracted from detail');
  assert.equal(d.msg.decision.handle, true);
  assert.match(d.msg.decision.reason, /dm:open/);
  assert.equal(d.endpoint, 'c1', 'endpoint formatted from the conversation');
  assert.ok(d.msg.message, 'raw merged frame passed through for the adapter');
  assert.equal(d.msg.orgId, 'org1', 'inbound object carries orgId = org_id');
  assert.equal('orgSlug' in d.msg, false, 'no deprecated orgSlug alias — org_id only');

  // Dedupe: re-injecting the same message id delivers nothing more.
  bridge.injectFrame('org1', msgFrame());
  await flush();
  assert.equal(delivered.length, 1, 'duplicate message_id deduped');

  await bridge.stop();
});

test('orgs are keyed by org_id end to end; a legacy `slug` field is ignored', async () => {
  const { http } = makeHttp([
    { match: (u, m) => m === 'GET' && /\/conversations\/c1\/messages\/m1$/.test(u),
      data: { id: 'm1', sender_id: 'u1', seq: 7, type: 'text', inbox_seq: 7,
        content: { content_type: 'text', body: { text: 'hello' }, attachments: [] } } },
    { match: (u, m) => m === 'GET' && /\/conversations\/c1$/.test(u),
      data: { id: 'c1', type: 'dm', name: 'DM' } },
  ]);
  const delivered = [];
  const inbound = { deliver: async (msg, endpoint) => { delivered.push({ msg, endpoint }); return { ok: true }; } };

  // An org carrying a DIFFERENT, vestigial `slug` alongside its org_id. The SDK
  // must key everything by org_id ('orgX') and completely ignore the slug.
  const org = baseOrg({ org_id: 'orgX', slug: 'legacy-slug' });
  const { bridge } = makeBridge({ http, orgConfigs: [org], inbound });
  await bridge.start();

  // Driving by org_id works…
  bridge.injectFrame('orgX', msgFrame());
  await flush();
  assert.equal(delivered.length, 1, 'frame delivered when keyed by org_id');
  assert.equal(delivered[0].msg.orgId, 'orgX');

  // …and the legacy slug is NOT a valid key.
  assert.throws(() => bridge.injectFrame('legacy-slug', msgFrame()),
    /injectFrame: org "legacy-slug" not started/,
    'the vestigial slug is not usable as an org key');

  await bridge.stop();
});

test('access-policy rejection does NOT call deliver (and posts a reject notice)', async () => {
  const { http, fetch } = makeHttp([
    { match: (u, m) => m === 'GET' && /\/conversations\/c1\/messages\/m1$/.test(u),
      data: { id: 'm1', sender_id: 'stranger', seq: 3, type: 'text',
        content: { content_type: 'text', body: { text: 'hi' }, attachments: [] } } },
    { match: (u, m) => m === 'GET' && /\/conversations\/c1$/.test(u),
      data: { id: 'c1', type: 'dm' } },
    // reject-notice POST (owner-policy drop with userNotice) — accept it.
    { match: (u, m) => m === 'POST' && /\/conversations\/c1\/messages$/.test(u),
      data: { id: 'reject1' } },
  ]);
  const delivered = [];
  const inbound = { deliver: async (msg) => { delivered.push(msg); return { ok: true }; } };

  // dmPolicy 'owner' with a bound owner → a stranger's DM is rejected.
  const org = baseOrg({ access: { dmPolicy: 'owner' } });
  const { bridge } = makeBridge({ http, orgConfigs: [org], inbound });
  await bridge.start();

  bridge.injectFrame('org1', msgFrame({ sender_id: 'stranger' }));
  await flush();

  assert.equal(delivered.length, 0, 'rejected message never delivered');
  const posted = fetch.calls.some(c => c.method === 'POST' && /\/conversations\/c1\/messages$/.test(c.url));
  assert.ok(posted, 'a reject notice was posted to cws-core');

  await bridge.stop();
});

test('reconnect (onOpen with a warm sync_seq) triggers a /sync catch-up', async () => {
  const { http, fetch } = makeHttp([
    // /sync sweep returns one event then stops.
    { match: (u, m) => m === 'POST' && /\/sync$/.test(u),
      data: { events: [{ message_id: 'm9', conversation_id: 'c1', seq: 6 }], has_more: false, next_cursor: 6 } },
    { match: (u, m) => m === 'GET' && /\/conversations\/c1\/messages\/m9$/.test(u),
      data: { id: 'm9', sender_id: 'u1', seq: 6, type: 'text',
        content: { content_type: 'text', body: { text: 'missed' }, attachments: [] } } },
    { match: (u, m) => m === 'GET' && /\/conversations\/c1$/.test(u),
      data: { id: 'c1', type: 'dm' } },
    { match: (u, m) => m === 'POST' && /\/sync\/ack$/.test(u), data: {} },
  ]);
  const delivered = [];
  const inbound = { deliver: async (msg) => { delivered.push(msg); return { ok: true }; } };

  // loadSession seeds a warm cursor so onOpen takes the reconnect (catch-up) path.
  const { bridge, sockets } = makeBridge({
    http, orgConfigs: [baseOrg()], inbound,
    callbacks: { loadSession: async () => ({ sync_seq: 5 }) },
  });
  await bridge.start();
  await flush();

  // Drive the WS open on the fake socket → onOpen → syncMissedEvents.
  assert.ok(sockets[0], 'a socket was created');
  sockets[0].emit('open');
  await flush(8);

  const syncCalled = fetch.calls.some(c => c.method === 'POST' && /\/sync$/.test(c.url));
  assert.ok(syncCalled, 'reconnect ran a /sync catch-up sweep');
  assert.equal(delivered.length, 1, 'the caught-up event flowed through the pipeline to deliver');
  assert.equal(delivered[0].messageId, 'm9');
  assert.equal(delivered[0].via, 'sync');

  await bridge.stop();
});

test('start() arms periodic timers and stop() clears them + all orgs', async () => {
  const { http } = makeHttp([]);
  const inbound = { deliver: async () => ({ ok: true }) };
  const runtimeState = { async getMetrics() { return {}; } };

  const bridge = new CwsAgentBridge({
    http,
    ws: {
      baseUrl: 'wss://test/ws',
      urlProvider: async () => 'wss://test/ws?ticket=t',
      wsFactory: () => new FakeWebSocket(),
    },
    orgConfigs: [baseOrg()],
    providers: { inbound, runtimeState, logger: quietLogger },
    callbacks: { syncSelf: async () => ({ nameReady: true }) },
    // metrics + frame-metrics timers both armed.
    reporters: { metrics: true, frameMetrics: true, metricsIntervalMs: 60000 },
  });

  await bridge.start();
  assert.ok(bridge._timers.length >= 2, 'metrics + frame-metrics timers armed');
  assert.equal(bridge._orgs.size, 1, 'one org runtime record');

  await bridge.stop();
  assert.equal(bridge._timers.length, 0, 'all periodic timers cleared');
  assert.equal(bridge._orgs.size, 0, 'all org records torn down');
  assert.equal(bridge._liveOrgCount, 0);
});

test('send() posts an AGENT_TEXT to the endpoint conversation and returns messageId', async () => {
  const { http, fetch } = makeHttp([
    { match: (u, m) => m === 'POST' && /\/conversations\/c5\/messages$/.test(u),
      data: { id: 'out1' } },
  ]);
  const inbound = { deliver: async () => ({ ok: true }) };
  const { bridge } = makeBridge({ http, orgConfigs: [baseOrg()], inbound });

  const res = await bridge.send('c5|reply:p9', 'hi there', { orgId: 'org1' });
  assert.equal(res.messageId, 'out1');
  const call = fetch.calls.find(c => c.method === 'POST' && /\/conversations\/c5\/messages$/.test(c.url));
  assert.ok(call, 'posted to the right conversation');
  const body = JSON.parse(call.opts.body);
  assert.equal(body.type, 'AGENT_TEXT');
  assert.equal(body.content.body.text, 'hi there');
  assert.equal(body.parent_id, 'p9', 'reply parent parsed from endpoint');
});

test('system recall frame is policy-gated and handed to onSystemNotice (not deliver)', async () => {
  const { http } = makeHttp([
    { match: (u, m) => m === 'GET' && /\/conversations\/c1$/.test(u),
      data: { id: 'c1', type: 'dm' } },
  ]);
  const delivered = [];
  const notices = [];
  const inbound = { deliver: async (msg) => { delivered.push(msg); return { ok: true }; } };
  const org = baseOrg({ access: { dmPolicy: 'open' } });
  const { bridge } = makeBridge({
    http, orgConfigs: [org], inbound,
    callbacks: { onSystemNotice: (o, ev) => notices.push(ev) },
  });
  await bridge.start();

  bridge.injectFrame('org1', {
    type: 'system',
    payload: { event: 'message.recalled', conversation_id: 'c1', data: { message_id: 'm1', recalled_by: 'u1' } },
  });
  await flush();

  assert.equal(delivered.length, 0, 'system notices do not go through InboundDelivery');
  assert.equal(notices.length, 1, 'onSystemNotice fired');
  assert.equal(notices[0].kind, 'recall');
  assert.equal(notices[0].conversationId, 'c1');
  assert.equal(notices[0].messageId, 'm1');

  await bridge.stop();
});

// ── P1-1: delivery failure must NOT prematurely commit dedupe/ledger/cursor ──
// The dedupe + inbox-ledger + sync cursor are "consumed" markers. They must
// commit ONLY on deliver {ok:true} or a terminal policy reject — never on an
// {ok:false} or a thrown deliver(), or the message is lost (redelivery blocked).

// Full detail + conversation routes for a c1/m1 message that carries inbox_seq.
function m1Routes(text = 'hi') {
  return [
    { match: (u, m) => m === 'GET' && /\/conversations\/c1\/messages\/m1$/.test(u),
      data: { id: 'm1', sender_id: 'u1', seq: 7, type: 'text', inbox_seq: 7,
        content: { content_type: 'text', body: { text }, attachments: [] } } },
    { match: (u, m) => m === 'GET' && /\/conversations\/c1$/.test(u),
      data: { id: 'c1', type: 'dm' } },
  ];
}

test('P1-1(a,c,d): deliver {ok:false} is not deduped; redelivered on replay, then committed once', async () => {
  const { http } = makeHttp(m1Routes());
  const results = [{ ok: false, failureClass: 'wake_failed', retryAfterMs: 500 }, { ok: true }];
  let attempt = 0;
  const delivered = [];
  const inbound = { deliver: async (msg) => { delivered.push(msg); return results[attempt++] ?? { ok: true }; } };
  const { bridge } = makeBridge({ http, orgConfigs: [baseOrg()], inbound });
  await bridge.start();

  bridge.injectFrame('org1', msgFrame());
  await flush();
  assert.equal(delivered.length, 1, 'first delivery attempted (returned ok:false)');

  // Replay same message-id: NOT deduped because the failed attempt never committed.
  bridge.injectFrame('org1', msgFrame());
  await flush();
  assert.equal(delivered.length, 2, 'redelivered after the failed attempt (dedupe/ledger not committed)');

  // Third replay lands after the eventual success → deduped, no double-commit.
  bridge.injectFrame('org1', msgFrame());
  await flush();
  assert.equal(delivered.length, 2, 'deduped after a successful delivery (single commit)');

  await bridge.stop();
});

test('P1-1(b): a thrown deliver() is not swallowed — not deduped, redelivered', async () => {
  const { http } = makeHttp(m1Routes());
  let attempt = 0;
  const delivered = [];
  const inbound = { deliver: async (msg) => {
    delivered.push(msg);
    if (attempt++ === 0) throw new Error('wake boom');
    return { ok: true };
  } };
  const { bridge } = makeBridge({ http, orgConfigs: [baseOrg()], inbound });
  await bridge.start();

  bridge.injectFrame('org1', msgFrame());
  await flush();
  assert.equal(delivered.length, 1, 'first attempt threw');

  bridge.injectFrame('org1', msgFrame());
  await flush();
  assert.equal(delivered.length, 2, 'redelivered after a thrown deliver() (no swallow, no premature dedupe)');

  await bridge.stop();
});

test('P1-1: a failed delivery during /sync does NOT advance the sync cursor or ack', async () => {
  const { http, fetch } = makeHttp([
    { match: (u, m) => m === 'POST' && /\/sync$/.test(u),
      data: { events: [{ message_id: 'm9', conversation_id: 'c1', seq: 6 }], has_more: false, next_cursor: 6 } },
    { match: (u, m) => m === 'GET' && /\/conversations\/c1\/messages\/m9$/.test(u),
      data: { id: 'm9', sender_id: 'u1', seq: 6, type: 'text', inbox_seq: 6,
        content: { content_type: 'text', body: { text: 'missed' }, attachments: [] } } },
    { match: (u, m) => m === 'GET' && /\/conversations\/c1$/.test(u), data: { id: 'c1', type: 'dm' } },
    { match: (u, m) => m === 'POST' && /\/sync\/ack$/.test(u), data: {} },
  ]);
  const saved = [];
  const inbound = { deliver: async () => ({ ok: false, failureClass: 'wake_failed' }) };
  const { bridge, sockets } = makeBridge({
    http, orgConfigs: [baseOrg()], inbound,
    callbacks: {
      loadSession: async () => ({ sync_seq: 5 }),
      saveSession: (orgId, partial) => saved.push(partial),
    },
  });
  await bridge.start();
  await flush();
  sockets[0].emit('open');   // reconnect → syncMissedEvents from seq 5
  await flush(8);

  const advanced = saved.some(p => typeof p.sync_seq === 'number' && p.sync_seq >= 6);
  assert.equal(advanced, false, 'sync cursor did NOT advance past the failed event');
  const acked = fetch.calls.some(c => c.method === 'POST' && /\/sync\/ack$/.test(c.url));
  assert.equal(acked, false, 'no /sync/ack posted for a message that failed delivery');

  await bridge.stop();
});

test('P1-1: a terminal policy reject DOES commit dedupe (consumed, not redelivered)', async () => {
  const { http } = makeHttp([
    { match: (u, m) => m === 'GET' && /\/conversations\/c1\/messages\/m1$/.test(u),
      data: { id: 'm1', sender_id: 'stranger', seq: 3, type: 'text', inbox_seq: 3,
        content: { content_type: 'text', body: { text: 'hi' }, attachments: [] } } },
    { match: (u, m) => m === 'GET' && /\/conversations\/c1$/.test(u), data: { id: 'c1', type: 'dm' } },
    { match: (u, m) => m === 'POST' && /\/conversations\/c1\/messages$/.test(u), data: { id: 'reject1' } },
  ]);
  let rejectPosts = 0;
  const origFetch = http._fetch;
  http._fetch = async (url, opts) => {
    if ((opts?.method || 'GET') === 'POST' && /\/conversations\/c1\/messages$/.test(url)) rejectPosts++;
    return origFetch(url, opts);
  };
  const delivered = [];
  const inbound = { deliver: async (msg) => { delivered.push(msg); return { ok: true }; } };
  const org = baseOrg({ access: { dmPolicy: 'owner' } });
  const { bridge } = makeBridge({ http, orgConfigs: [org], inbound });
  await bridge.start();

  bridge.injectFrame('org1', msgFrame({ sender_id: 'stranger' }));
  await flush();
  assert.equal(delivered.length, 0, 'rejected message never delivered');
  assert.equal(rejectPosts, 1, 'one reject notice posted');

  // Replay: the reject was terminal/consumed → deduped → no second reject notice.
  bridge.injectFrame('org1', msgFrame({ sender_id: 'stranger' }));
  await flush();
  assert.equal(rejectPosts, 1, 'policy reject committed dedupe — no re-processing on replay');

  await bridge.stop();
});

// ── P1-3: async adapter callbacks must be awaited + caught ────────────────────

test('P1-3: rejecting onSystemNotice is caught, not deduped (retried), no unhandledRejection', async () => {
  const rejections = [];
  const onUnhandled = (e) => rejections.push(e);
  process.on('unhandledRejection', onUnhandled);
  try {
    const { http } = makeHttp([
      { match: (u, m) => m === 'GET' && /\/conversations\/c1$/.test(u), data: { id: 'c1', type: 'dm' } },
    ]);
    let calls = 0;
    const inbound = { deliver: async () => ({ ok: true }) };
    const { bridge } = makeBridge({
      http, orgConfigs: [baseOrg({ access: { dmPolicy: 'open' } })], inbound,
      callbacks: { onSystemNotice: async () => { calls += 1; if (calls === 1) throw new Error('notice boom'); } },
    });
    await bridge.start();

    const recall = {
      type: 'system',
      payload: { event: 'message.recalled', conversation_id: 'c1', data: { message_id: 'm1', recalled_by: 'u1' } },
    };
    bridge.injectFrame('org1', recall);
    await flush();
    assert.equal(calls, 1, 'onSystemNotice invoked and awaited');

    bridge.injectFrame('org1', recall);
    await flush();
    assert.equal(calls, 2, 'event NOT deduped after the rejecting callback — retried on replay');

    bridge.injectFrame('org1', recall);
    await flush();
    assert.equal(calls, 2, 'deduped after the notice eventually succeeded (single commit)');

    await flush(6);
    assert.equal(rejections.length, 0, 'no unhandledRejection escaped from onSystemNotice');

    await bridge.stop();
  } finally {
    process.removeListener('unhandledRejection', onUnhandled);
  }
});

test('P1-3: rejecting onConfigEvent is awaited + caught (no unhandledRejection), retried on replay', async () => {
  const rejections = [];
  const onUnhandled = (e) => rejections.push(e);
  process.on('unhandledRejection', onUnhandled);
  try {
    const { http } = makeHttp([]);
    let calls = 0;
    const inbound = { deliver: async () => ({ ok: true }) };
    const { bridge } = makeBridge({
      http, orgConfigs: [baseOrg()], inbound,
      callbacks: { onConfigEvent: async () => { calls += 1; throw new Error('config boom'); } },
    });
    await bridge.start();

    const frame = {
      type: 'system',
      payload: { event: 'agent.config.dm_policy_changed', data: { agent_member_id: 'self1', policy: 'allowlist' } },
    };
    bridge.injectFrame('org1', frame);
    await flush();
    assert.equal(calls, 1, 'onConfigEvent invoked and awaited');

    // Config events are not consumed by a failed callback → retried on replay.
    bridge.injectFrame('org1', frame);
    await flush();
    assert.equal(calls, 2, 'config event retried after the rejecting callback');

    await flush(6);
    assert.equal(rejections.length, 0, 'no unhandledRejection escaped from onConfigEvent');

    await bridge.stop();
  } finally {
    process.removeListener('unhandledRejection', onUnhandled);
  }
});

// ── P1-A: atomic reserve — concurrent same-id delivery is EXACTLY once ────────
// The reservation is a SYNCHRONOUS check-and-claim taken before the first await
// (message-detail fetch / conversation fetch), so two handlers racing on the
// same id can never both reach deliver()/onSystemNotice(). With the old peek-only
// has() both would read "not seen" (commit happens post-deliver) and double-fire.

test('P1-A: two concurrent handlers for the SAME message id deliver exactly once', async () => {
  const { http } = makeHttp(m1Routes());
  let release;
  const gate = new Promise((r) => { release = r; });
  const delivered = [];
  // deliver hangs on a gate so BOTH frames are in-flight simultaneously if the
  // claim is not atomic.
  const inbound = { deliver: async (msg) => { delivered.push(msg); await gate; return { ok: true }; } };
  const { bridge } = makeBridge({ http, orgConfigs: [baseOrg()], inbound });
  await bridge.start();

  // Fire the same message id twice back-to-back with NO await in between.
  bridge.injectFrame('org1', msgFrame());
  bridge.injectFrame('org1', msgFrame());
  await flush();

  assert.equal(delivered.length, 1, 'concurrent same-id claimed atomically → delivered exactly once');
  release();
  await flush();
  assert.equal(delivered.length, 1, 'still exactly once after the in-flight delivery completes');

  await bridge.stop();
});

test('P1-A: two concurrent system frames for the SAME event fire onSystemNotice once', async () => {
  const { http } = makeHttp([
    { match: (u, m) => m === 'GET' && /\/conversations\/c1$/.test(u), data: { id: 'c1', type: 'dm' } },
  ]);
  const notices = [];
  const inbound = { deliver: async () => ({ ok: true }) };
  const { bridge } = makeBridge({
    http, orgConfigs: [baseOrg({ access: { dmPolicy: 'open' } })], inbound,
    callbacks: { onSystemNotice: async (o, ev) => { notices.push(ev); } },
  });
  await bridge.start();

  const recall = {
    type: 'system',
    payload: { event: 'message.recalled', conversation_id: 'c1', data: { message_id: 'm1', recalled_by: 'u1' } },
  };
  bridge.injectFrame('org1', recall);
  bridge.injectFrame('org1', recall);
  await flush();

  assert.equal(notices.length, 1, 'concurrent same system event → onSystemNotice fired exactly once');

  await bridge.stop();
});

test('P1-A: a failed deliver releases the reservation → a later replay redelivers', async () => {
  const { http } = makeHttp(m1Routes());
  const results = [{ ok: false, failureClass: 'wake_failed' }, { ok: true }];
  let attempt = 0;
  const delivered = [];
  const inbound = { deliver: async (msg) => { delivered.push(msg); return results[attempt++] ?? { ok: true }; } };
  const { bridge } = makeBridge({ http, orgConfigs: [baseOrg()], inbound });
  await bridge.start();

  bridge.injectFrame('org1', msgFrame());
  await flush();
  assert.equal(delivered.length, 1, 'first attempt failed (reservation released, not committed)');

  bridge.injectFrame('org1', msgFrame());
  await flush();
  assert.equal(delivered.length, 2, 'reservation was released → replay re-reserved and redelivered');

  await bridge.stop();
});

// ── P1-B: only a LITERAL {ok:true} is success ────────────────────────────────
// {}, undefined, {ok:"yes"}, {ok:1} etc. are malformed/absent acks → retryable
// failures (no commit, redelivered). The old `res?.ok !== false` treated all of
// these as success and silently committed.

test('P1-B: {}, undefined, {ok:"yes"}, {ok:1} are failures; only {ok:true} commits', async () => {
  const { http } = makeHttp(m1Routes());
  const results = [{}, undefined, { ok: 'yes' }, { ok: 1 }, { ok: true }];
  let attempt = 0;
  const delivered = [];
  const inbound = { deliver: async (msg) => { delivered.push(msg); return results[attempt++]; } };
  const { bridge } = makeBridge({ http, orgConfigs: [baseOrg()], inbound });
  await bridge.start();

  // Each malformed ack must NOT commit → the message is redelivered on replay,
  // right up until the literal {ok:true} (attempt 5) finally commits it.
  for (let i = 1; i <= 5; i++) {
    bridge.injectFrame('org1', msgFrame());
    await flush();
    assert.equal(delivered.length, i, `attempt ${i}: non-true ack (${JSON.stringify(results[i - 1])}) redelivered`);
  }

  // A 6th replay after the {ok:true} commit is deduped — committed exactly once.
  bridge.injectFrame('org1', msgFrame());
  await flush();
  assert.equal(delivered.length, 5, 'committed exactly once on {ok:true}; no redelivery afterwards');

  await bridge.stop();
});

test('P1-B: the /sync catch-up path also treats a malformed ack as failure (no cursor advance)', async () => {
  const { http, fetch } = makeHttp([
    { match: (u, m) => m === 'POST' && /\/sync$/.test(u),
      data: { events: [{ message_id: 'm9', conversation_id: 'c1', seq: 6 }], has_more: false, next_cursor: 6 } },
    { match: (u, m) => m === 'GET' && /\/conversations\/c1\/messages\/m9$/.test(u),
      data: { id: 'm9', sender_id: 'u1', seq: 6, type: 'text', inbox_seq: 6,
        content: { content_type: 'text', body: { text: 'missed' }, attachments: [] } } },
    { match: (u, m) => m === 'GET' && /\/conversations\/c1$/.test(u), data: { id: 'c1', type: 'dm' } },
    { match: (u, m) => m === 'POST' && /\/sync\/ack$/.test(u), data: {} },
  ]);
  const saved = [];
  // deliver returns {} — malformed ack; under strict ok:true this is a failure.
  const inbound = { deliver: async () => ({}) };
  const { bridge, sockets } = makeBridge({
    http, orgConfigs: [baseOrg()], inbound,
    callbacks: {
      loadSession: async () => ({ sync_seq: 5 }),
      saveSession: (orgId, partial) => saved.push(partial),
    },
  });
  await bridge.start();
  await flush();
  sockets[0].emit('open');
  await flush(8);

  const advanced = saved.some(p => typeof p.sync_seq === 'number' && p.sync_seq >= 6);
  assert.equal(advanced, false, 'malformed ack ({}) did NOT advance the sync cursor');
  const acked = fetch.calls.some(c => c.method === 'POST' && /\/sync\/ack$/.test(c.url));
  assert.equal(acked, false, 'no /sync/ack for a message whose ack was malformed');

  await bridge.stop();
});

// ── P1: a non-atomic (legacy plain-function) callbacks.dedupe is REJECTED ──────
// A plain check-and-record function records the id at reserve and cannot release
// it, so a {ok:false} deliver followed by a replay is permanently suppressed
// (the message is lost after one attempt). A warning is not a fix — the SDK now
// throws at construction so the mistake surfaces immediately.
function newBridge({ http, orgConfigs, inbound, callbacks = {}, logger = quietLogger }) {
  return new CwsAgentBridge({
    http,
    ws: { baseUrl: 'wss://test/ws', urlProvider: async () => 'wss://test/ws?ticket=t', wsFactory: () => new FakeWebSocket() },
    orgConfigs,
    providers: { inbound, logger },
    callbacks: { syncSelf: async () => ({ nameReady: true }), ...callbacks },
    reporters: { metrics: false, frameMetrics: false, markReadOnDeliver: false },
  });
}

test('P1: constructing with a legacy plain-function dedupe THROWS (non-atomic rejected)', () => {
  const { http } = makeHttp(m1Routes());
  const seen = new Set();
  // Legacy shape: a bare callable with NO reserve()/commit()/release().
  const legacyDedupe = (id) => { if (seen.has(id)) return true; seen.add(id); return false; };
  const inbound = { deliver: async () => ({ ok: true }) };

  assert.throws(
    () => newBridge({ http, orgConfigs: [baseOrg()], inbound, callbacks: { dedupe: legacyDedupe } }),
    /custom dedupe must implement reserve\/commit\/release; pass none to use the built-in atomic deduper/,
    'a non-atomic deduper is rejected at construction',
  );
});

test('P1: a partial (reserve/commit but no release) dedupe also THROWS', () => {
  const { http } = makeHttp(m1Routes());
  const partial = () => false;
  partial.reserve = () => true;
  partial.commit = () => {};
  // no release() → not the full atomic interface
  const inbound = { deliver: async () => ({ ok: true }) };

  assert.throws(
    () => newBridge({ http, orgConfigs: [baseOrg()], inbound, callbacks: { dedupe: partial } }),
    /custom dedupe must implement reserve\/commit\/release/,
    'a dedupe missing release() is rejected',
  );
});

test('P1: the default (no custom dedupe) and a proper atomic dedupe both construct + deliver', async () => {
  // (a) default deduper — no callbacks.dedupe supplied.
  {
    const { http } = makeHttp(m1Routes());
    const delivered = [];
    const inbound = { deliver: async (msg) => { delivered.push(msg); return { ok: true }; } };
    const bridge = newBridge({ http, orgConfigs: [baseOrg()], inbound });
    await bridge.start();
    bridge.injectFrame('org1', msgFrame());
    await flush();
    assert.equal(delivered.length, 1, 'default deduper delivers');
    await bridge.stop();
  }
  // (b) a proper atomic deduper (createDeduper-shaped) supplied by the adapter.
  {
    const { http } = makeHttp(m1Routes());
    const delivered = [];
    const inbound = { deliver: async (msg) => { delivered.push(msg); return { ok: true }; } };
    const atomic = createDeduper();
    const bridge = newBridge({ http, orgConfigs: [baseOrg()], inbound, callbacks: { dedupe: atomic } });
    await bridge.start();
    bridge.injectFrame('org1', msgFrame());
    await flush();
    assert.equal(delivered.length, 1, 'a supplied atomic deduper delivers');
    bridge.injectFrame('org1', msgFrame());
    await flush();
    assert.equal(delivered.length, 1, 'and still dedupes a committed duplicate');
    await bridge.stop();
  }
});

test('P1 (regression): default deduper {ok:false}-then-replay REDELIVERS (not permanently suppressed)', async () => {
  const { http } = makeHttp(m1Routes());
  const results = [{ ok: false, failureClass: 'wake_failed' }, { ok: true }];
  let attempt = 0;
  const delivered = [];
  const inbound = { deliver: async (msg) => { delivered.push(msg); return results[attempt++] ?? { ok: true }; } };
  const bridge = newBridge({ http, orgConfigs: [baseOrg()], inbound });
  await bridge.start();

  bridge.injectFrame('org1', msgFrame());
  await flush();
  assert.equal(delivered.length, 1, 'first attempt returned {ok:false} (claim released, not committed)');

  // The whole point of the atomic-deduper requirement: a failed deliver must not
  // permanently suppress the id — the replay redelivers.
  bridge.injectFrame('org1', msgFrame());
  await flush();
  assert.equal(delivered.length, 2, 'replay after {ok:false} REDELIVERED (retryable, not suppressed)');

  await bridge.stop();
});

test('config-update frame is handed to onConfigEvent (SDK does not persist)', async () => {
  const { http } = makeHttp([]);
  const inbound = { deliver: async () => ({ ok: true }) };
  const events = [];
  const { bridge } = makeBridge({
    http, orgConfigs: [baseOrg()], inbound,
    callbacks: { onConfigEvent: (o, ev) => events.push(ev) },
  });
  await bridge.start();

  bridge.injectFrame('org1', {
    type: 'system',
    payload: { event: 'agent.config.dm_policy_changed', data: { agent_member_id: 'self1', policy: 'allowlist' } },
  });
  await flush();

  assert.equal(events.length, 1, 'onConfigEvent fired');
  assert.equal(events[0].event, 'agent.config.dm_policy_changed');
  assert.equal(events[0].data.policy, 'allowlist');

  await bridge.stop();
});

// ── #4: restart with a stale session cursor must NOT lose messages ────────────
// The ledger persists acked_seq on every record (durable), but the session
// cursor only advances on the 5s onAck tick. A process that stops before that
// tick fires leaves a stale/0 session sync_seq while the ledger's acked_seq is
// higher. On restart the SDK must seed sync_seq from the ledger's durable
// acked_seq so onOpen takes the /sync catch-up path — NOT initSyncSeq (seek to
// inbox end), which would silently discard everything that arrived while down.

test('#4: restart with a stale session cursor seeds sync_seq from the ledger acked_seq (no message loss)', async () => {
  const { http } = makeHttp([
    { match: (u, m) => m === 'POST' && /\/sync$/.test(u),
      data: { events: [{ message_id: 'm9', conversation_id: 'c1', seq: 6 }], has_more: false, next_cursor: 6 } },
    { match: (u, m) => m === 'GET' && /\/conversations\/c1\/messages\/m9$/.test(u),
      data: { id: 'm9', sender_id: 'u1', seq: 6, type: 'text', inbox_seq: 6,
        content: { content_type: 'text', body: { text: 'missed during restart' }, attachments: [] } } },
    { match: (u, m) => m === 'GET' && /\/conversations\/c1$/.test(u), data: { id: 'c1', type: 'dm' } },
    { match: (u, m) => m === 'POST' && /\/sync\/ack$/.test(u), data: {} },
  ]);
  // Durable ledger recorded acked_seq=5, but the 5s onAck tick never wrote the
  // session file before the restart → loadSession returns a stale sync_seq of 0.
  const storage = memoryStorage();
  await storage.set('inbox-org1.json', JSON.stringify({ acked_seq: 5, received: [] }));

  const delivered = [];
  const inbound = { deliver: async (msg) => { delivered.push(msg); return { ok: true }; } };
  const { bridge, sockets } = makeBridge({
    http, orgConfigs: [baseOrg()], inbound, storage,
    callbacks: { loadSession: async () => ({ sync_seq: 0 }) },
  });
  await bridge.start();
  await flush();
  sockets[0].emit('open');   // WS open → onOpen path selection
  await flush(8);

  // Fix: sync_seq seeded from ledger acked_seq(5) → reconnect catch-up re-pulls
  // and delivers m9. Bug: sync_seq stays 0 → initSyncSeq seeks to inbox end and
  // m9 is discarded (delivered.length === 0).
  assert.equal(delivered.length, 1, 'the message that arrived during the restart window was recovered');
  assert.equal(delivered[0].messageId, 'm9');
  assert.equal(delivered[0].via, 'sync');

  await bridge.stop();
});

test('send() puts structured mentions at the top level of the posted body', async () => {
  const { http, fetch } = makeHttp([
    { match: (u, m) => m === 'POST' && /\/conversations\/c6\/messages$/.test(u),
      data: { id: 'out2' } },
  ]);
  const inbound = { deliver: async () => ({ ok: true }) };
  const { bridge } = makeBridge({ http, orgConfigs: [baseOrg()], inbound });

  const mentions = [{ type: 'member', member_id: 'm-alice' }];
  await bridge.send('c6', 'hi @Alice', { orgId: 'org1', mentions });
  const call = fetch.calls.find(c => c.method === 'POST' && /\/conversations\/c6\/messages$/.test(c.url));
  const body = JSON.parse(call.opts.body);
  assert.deepEqual(body.mentions, mentions, 'indexed by cws-core only at the top level');
  assert.equal(body.content.body.mentions, undefined);
});

test('send() omits mentions when none are given', async () => {
  const { http, fetch } = makeHttp([
    { match: (u, m) => m === 'POST' && /\/conversations\/c7\/messages$/.test(u),
      data: { id: 'out3' } },
  ]);
  const inbound = { deliver: async () => ({ ok: true }) };
  const { bridge } = makeBridge({ http, orgConfigs: [baseOrg()], inbound });

  await bridge.send('c7', 'plain', { orgId: 'org1' });
  const call = fetch.calls.find(c => c.method === 'POST' && /\/conversations\/c7\/messages$/.test(c.url));
  assert.ok(!('mentions' in JSON.parse(call.opts.body)));
});
