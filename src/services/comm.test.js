import { test } from 'node:test';
import assert from 'node:assert/strict';
import { CommService, createCommService } from './comm.js';
import { makeClient, ok, clearCwsEnv } from './_fixtures.js';

clearCwsEnv();

test('send auto-detects plain text and builds the v5 message body', async () => {
  const { http, fetch } = makeClient([ok({ seq: 1 })]);
  await new CommService(http).send({ conversationId: 'cv1', content: 'hello there', clientMsgId: 'cm1' });
  assert.equal(fetch.requests[0].method, 'POST');
  assert.equal(fetch.requests[0].path, '/api/v1/conversations/cv1/messages');
  assert.deepEqual(fetch.requests[0].body, {
    client_msg_id: 'cm1',
    type: 'AGENT_TEXT',
    content: { content_type: 'text', body: { text: 'hello there' }, attachments: [] },
  });
});

test('send detects markdown content_type and threads replyTo → parent_id', async () => {
  const { http, fetch } = makeClient([ok()]);
  await new CommService(http).send({ conversationId: 'cv1', content: '# Title\n- a\n- b', clientMsgId: 'cm2', replyTo: 'm0' });
  assert.equal(fetch.requests[0].body.content.content_type, 'markdown');
  assert.equal(fetch.requests[0].body.parent_id, 'm0');
});

test('createDm derives peer from any alias; org/member come from JWT', async () => {
  const { http, fetch } = makeClient([ok()]);
  await new CommService(http).createDm({ participantId: 'peer-9' });
  assert.equal(fetch.requests[0].path, '/api/v1/conversations/dm');
  assert.deepEqual(fetch.requests[0].body, { peer_member_id: 'peer-9' });
});

test('getMessages maps seq window params', async () => {
  const { http, fetch } = makeClient([ok([])]);
  await new CommService(http).getMessages({ conversationId: 'cv1', afterSeq: 10, limit: 50 });
  assert.equal(fetch.requests[0].path, '/api/v1/conversations/cv1/messages');
  assert.equal(fetch.requests[0].query.after_seq, '10');
  assert.equal(fetch.requests[0].query.limit, '50');
});

test('markRead posts read_until_seq; sync posts since_seq/device_id', async () => {
  const c1 = makeClient([ok()]);
  await new CommService(c1.http).markRead({ conversationId: 'cv1', seq: 42 });
  assert.deepEqual(c1.fetch.requests[0].body, { read_until_seq: 42 });

  const c2 = makeClient([ok()]);
  await new CommService(c2.http).sync({ sinceSeq: 7, deviceId: 'dev1', limit: 100 });
  assert.equal(c2.fetch.requests[0].path, '/api/v1/sync');
  assert.deepEqual(c2.fetch.requests[0].body, { since_seq: 7, device_id: 'dev1', limit: 100 });
});

test('config-coupled methods throw without an injected config provider', () => {
  const { http } = makeClient([ok()]);
  assert.throws(() => new CommService(http).dmList({}), /requires an injected config provider/);
});

test('dmPolicy sets policy through the config provider (updateConfig)', async () => {
  const { http } = makeClient([ok()]);
  const cfg = {
    orgs: { o1: { org_id: 'o1', org_name: 'Acme', access: { dmPolicy: 'owner' } } },
  };
  const config = {
    enabledOrgs: () => [cfg.orgs.o1],
    getOrgByOrgId: (id) => Object.values(cfg.orgs).find((o) => o.org_id === id),
    updateConfig: (fn) => { fn(cfg); return cfg; },
    setOwner: () => {},
  };
  const out = new CommService(http, config).dmPolicy({ policy: 'allowlist' });
  assert.deepEqual(out, { org: 'Acme', dmPolicy: 'allowlist', applied: true });
  assert.equal(cfg.orgs.o1.access.dmPolicy, 'allowlist');
});

test('dmPolicy rejects an invalid policy', () => {
  const { http } = makeClient([ok()]);
  const config = { enabledOrgs: () => [{ org_id: 'o1', access: {} }], updateConfig: (f) => f, setOwner: () => {} };
  assert.throws(() => new CommService(http, config).dmPolicy({ policy: 'nope' }), /Invalid policy/);
});

test('syncOwner reads self member then pulls + sets the authoritative owner', async () => {
  const { http, fetch } = makeClient([
    ok({ owner_member_id: 'owner-new' }),                 // GET /members/self (getForOrg)
    ok({ display_name: 'Alice' }),                        // GET /members/owner-new
  ]);
  const setOwnerCalls = [];
  const org = { org_id: 'o1', org_name: 'Acme', self: { member_id: 'self-1' }, owner: { member_id: 'owner-old' } };
  const config = {
    enabledOrgs: () => [org],
    getOrgByOrgId: () => undefined,
    updateConfig: (f) => f,
    setOwner: (orgId, id, name) => setOwnerCalls.push({ orgId, id, name }),
  };
  const out = await createCommService(http, config).syncOwner({});
  assert.equal(fetch.requests[0].path, '/api/v1/members/self-1');
  assert.equal(fetch.requests[1].path, '/api/v1/members/owner-new');
  assert.deepEqual(setOwnerCalls, [{ orgId: 'o1', id: 'owner-new', name: 'Alice' }]);
  assert.equal(out.synced, true);
  assert.deepEqual(out.owner, { member_id: 'owner-new', name: 'Alice' });
});

test('send puts mentions at the request TOP LEVEL, never inside content.body', async () => {
  const { http, fetch } = makeClient([ok({ seq: 1 })]);
  const mentions = [{ type: 'member', member_id: 'm-alice' }];
  await new CommService(http).send({ conversationId: 'cv1', content: 'hi @Alice', clientMsgId: 'cm3', mentions });
  const body = fetch.requests[0].body;
  // cws-core indexes the top-level array; one nested under content.body is
  // stored as ordinary body data and notifies nobody.
  assert.deepEqual(body.mentions, mentions);
  assert.equal(body.content.body.mentions, undefined);
});

test('send omits the mentions key entirely when there are none', async () => {
  const { http, fetch } = makeClient([ok(), ok()]);
  const svc = new CommService(http);
  await svc.send({ conversationId: 'cv1', content: 'plain', clientMsgId: 'cm4' });
  await svc.send({ conversationId: 'cv1', content: 'plain', clientMsgId: 'cm5', mentions: [] });
  assert.ok(!('mentions' in fetch.requests[0].body));
  assert.ok(!('mentions' in fetch.requests[1].body));
});

test('conversationMembers returns an array whatever envelope shape comes back', async () => {
  const roster = [{ member_id: 'm1', display_name: 'Alice' }];
  for (const payload of [roster, { data: roster }, { members: roster }]) {
    const { http } = makeClient([ok(payload)]);
    assert.deepEqual(await new CommService(http).conversationMembers({ conversationId: 'cv1' }), roster);
  }
});

test('conversationMembers returns [] rather than undefined on an unrecognized payload', async () => {
  // Reading `.data` off a bare array yields undefined and iterates zero times —
  // silently indistinguishable from never having run. Always hand back an array.
  const { http } = makeClient([ok({ unexpected: true })]);
  assert.deepEqual(await new CommService(http).conversationMembers({ conversationId: 'cv1' }), []);
});

test('send keeps top-level mentions when the caller also uses the advanced body override', async () => {
  // Dropping them here would post a valid-looking message that notifies nobody —
  // the exact silent failure this parameter exists to prevent.
  const { http, fetch } = makeClient([ok()]);
  const mentions = [{ type: 'member', member_id: 'm-alice' }];
  await new CommService(http).send({
    conversationId: 'cv1',
    clientMsgId: 'cm6',
    body: { type: 'AGENT_TEXT', content: { content_type: 'markdown', body: { text: 'hi @Alice' } } },
    mentions,
  });
  const body = fetch.requests[0].body;
  assert.deepEqual(body.mentions, mentions);
  assert.equal(body.content.content_type, 'markdown', 'the override still controls the content');
});

test('a mentions array inside the advanced body override wins over params.mentions', async () => {
  const { http, fetch } = makeClient([ok()]);
  await new CommService(http).send({
    conversationId: 'cv1',
    clientMsgId: 'cm7',
    body: {
      type: 'AGENT_TEXT',
      content: { content_type: 'text', body: { text: 'hi' } },
      mentions: [{ type: 'member', member_id: 'm-from-body' }],
    },
    mentions: [{ type: 'member', member_id: 'm-from-params' }],
  });
  assert.deepEqual(fetch.requests[0].body.mentions, [{ type: 'member', member_id: 'm-from-body' }]);
});
