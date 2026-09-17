import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createMentionRegistry } from './mention.js';
import { memoryStorage } from '../providers.js';

// ── recordParticipants ────────────────────────────────────────────────────────

test('resolveMentions canonicalizes a case-insensitive @name to the exact display name', async () => {
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordParticipants('c1', 'Alice Wong');
  assert.equal(await reg.resolveMentions('hey @alice wong, look', 'c1'), 'hey @Alice Wong, look');
  assert.equal(await reg.resolveMentions('hey @ALICE WONG!', 'c1'), 'hey @Alice Wong!');
});

test('resolveMentions matches the longest name first (longer wins over a shorter prefix)', async () => {
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordParticipants('c1', ['Alice', 'Alice Wong']);
  // "@Alice Wong" must canonicalize as the full name, not "@Alice" + " Wong".
  assert.equal(await reg.resolveMentions('ping @alice wong', 'c1'), 'ping @Alice Wong');
  // A bare "@Alice" still resolves to the short participant.
  assert.equal(await reg.resolveMentions('ping @alice', 'c1'), 'ping @Alice');
});

test('resolveMentions leaves unknown @handles and mention-free text untouched', async () => {
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordParticipants('c1', 'Bob');
  assert.equal(await reg.resolveMentions('hi @charlie', 'c1'), 'hi @charlie');
  assert.equal(await reg.resolveMentions('no mention here', 'c1'), 'no mention here');
  // Unknown conversation → passthrough.
  assert.equal(await reg.resolveMentions('@bob hello', 'other'), '@bob hello');
});

test('resolveMentions short-circuits empty / mention-free / missing-conversation input', async () => {
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordParticipants('c1', 'Bob');
  assert.equal(await reg.resolveMentions('', 'c1'), '');
  assert.equal(await reg.resolveMentions('plain', 'c1'), 'plain');
  assert.equal(await reg.resolveMentions('@bob', ''), '@bob');
});

test('recordParticipants dedupes by normalized name and keeps the latest casing', async () => {
  const storage = memoryStorage();
  const reg = createMentionRegistry({ storage });
  await reg.recordParticipants('c1', ['Alice', 'ALICE', '  alice  ']);
  const persisted = JSON.parse(await storage.get('mention-registry.json'));
  // One normalized key "alice"; value is the last-seen trimmed form.
  assert.deepEqual(Object.keys(persisted.c1.names), ['alice']);
  assert.equal(persisted.c1.names.alice, 'alice');
});

test('recordParticipants ignores empty conversationId and blank names', async () => {
  const storage = memoryStorage();
  const reg = createMentionRegistry({ storage });
  await reg.recordParticipants('', 'Alice');
  await reg.recordParticipants('c1', ['', '   ', null, undefined]);
  assert.equal(await storage.get('mention-registry.json'), null); // nothing persisted
});

test('registry persists across instances backed by the same storage', async () => {
  const storage = memoryStorage();
  const a = createMentionRegistry({ storage });
  await a.recordParticipants('c1', 'Dana');
  // A fresh instance (simulated restart) reads the persisted registry.
  const b = createMentionRegistry({ storage });
  assert.equal(await b.resolveMentions('yo @DANA', 'c1'), 'yo @Dana');
});

test('per-conversation name set is capped (oldest evicted)', async () => {
  const storage = memoryStorage();
  const reg = createMentionRegistry({ storage, maxNamesPerConv: 2 });
  await reg.recordParticipants('c1', ['One', 'Two', 'Three']);
  const persisted = JSON.parse(await storage.get('mention-registry.json'));
  // Cap=2 keeps the two most-recently inserted; "one" was evicted.
  assert.equal(Object.keys(persisted.c1.names).length, 2);
  assert.deepEqual(Object.keys(persisted.c1.names), ['two', 'three']);
});

test('a storage write failure never throws out of recordParticipants', async () => {
  const storage = {
    async get() { return null; },
    async set() { throw new Error('disk full'); },
  };
  const reg = createMentionRegistry({ storage });
  await assert.doesNotReject(reg.recordParticipants('c1', 'Alice'));
});

test('a name with regex metacharacters is matched literally', async () => {
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordParticipants('c1', 'A.B (dev)');
  assert.equal(await reg.resolveMentions('cc @a.b (DEV)', 'c1'), 'cc @A.B (dev)');
});

// ── recordMembers / resolveOutbound ───────────────────────────────────────────

test('resolveOutbound canonicalizes the text AND emits a structured row for a known member', async () => {
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordMembers('c1', [{ displayName: 'Alice Wong', memberId: 'm-alice' }]);
  const out = await reg.resolveOutbound('hey @alice wong, look', 'c1');
  assert.equal(out.text, 'hey @Alice Wong, look');
  assert.deepEqual(out.mentions, [{ type: 'member', member_id: 'm-alice' }]);
});

test('resolveOutbound canonicalizes but emits NO row for a name with no member id', async () => {
  // The notify half needs an id; a name learned from an inbound sender has none.
  // Highlight still works, which is exactly why this case is invisible in the UI.
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordParticipants('c1', 'Bob');
  const out = await reg.resolveOutbound('ping @bob', 'c1');
  assert.equal(out.text, 'ping @Bob');
  assert.deepEqual(out.mentions, []);
});

test('resolveOutbound consumes the longest name first, so a shorter participant inside it is not also emitted', async () => {
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordMembers('c1', [
    { displayName: 'Alice', memberId: 'm-short' },
    { displayName: 'Alice Wong', memberId: 'm-long' },
  ]);
  const out = await reg.resolveOutbound('ping @alice wong', 'c1');
  assert.deepEqual(out.mentions, [{ type: 'member', member_id: 'm-long' }]);
});

test('resolveOutbound emits one row per member however many times they are mentioned', async () => {
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordMembers('c1', [{ displayName: 'Alice', memberId: 'm-alice' }]);
  const out = await reg.resolveOutbound('@alice ping @Alice again @ALICE', 'c1');
  assert.deepEqual(out.mentions, [{ type: 'member', member_id: 'm-alice' }]);
});

test('resolveOutbound leaves unknown handles, mention-free text and unknown conversations alone', async () => {
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordMembers('c1', [{ displayName: 'Alice', memberId: 'm-alice' }]);
  assert.deepEqual(await reg.resolveOutbound('hi @charlie', 'c1'), { text: 'hi @charlie', mentions: [] });
  assert.deepEqual(await reg.resolveOutbound('no mention here', 'c1'), { text: 'no mention here', mentions: [] });
  assert.deepEqual(await reg.resolveOutbound('@alice hi', 'other'), { text: '@alice hi', mentions: [] });
});

test('resolveOutbound matches a display name containing regex metacharacters literally', async () => {
  // Names are matched by substring scan, never compiled into a pattern.
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordMembers('c1', [{ displayName: 'a.b(c)', memberId: 'm-meta' }]);
  assert.deepEqual((await reg.resolveOutbound('ping @a.b(c)', 'c1')).mentions,
    [{ type: 'member', member_id: 'm-meta' }]);
  // The metacharacters must not match arbitrary text the way a pattern would.
  assert.deepEqual((await reg.resolveOutbound('ping @axbxcx', 'c1')).mentions, []);
});

test('a registry persisted in the legacy flat shape still resolves, and takes member ids on top', async () => {
  const storage = memoryStorage();
  // Pre-structured-mentions on-disk shape: { conv: { normName: exactName } }.
  await storage.set('mention-registry.json', JSON.stringify({ c1: { alice: 'Alice' } }));
  const reg = createMentionRegistry({ storage });
  assert.equal(await reg.resolveMentions('hi @ALICE', 'c1'), 'hi @Alice');
  assert.deepEqual((await reg.resolveOutbound('hi @alice', 'c1')).mentions, []);

  await reg.recordMembers('c1', [{ displayName: 'Alice', memberId: 'm-alice' }]);
  assert.deepEqual((await reg.resolveOutbound('hi @alice', 'c1')).mentions,
    [{ type: 'member', member_id: 'm-alice' }]);
  const persisted = JSON.parse(await storage.get('mention-registry.json'));
  assert.deepEqual(persisted.c1, { names: { alice: 'Alice' }, ids: { alice: 'm-alice' } });
});

test('a legacy bucket whose participant is literally called "names" still migrates', async () => {
  const storage = memoryStorage();
  await storage.set('mention-registry.json', JSON.stringify({ c1: { names: 'Names' } }));
  const reg = createMentionRegistry({ storage });
  assert.equal(await reg.resolveMentions('hi @NAMES', 'c1'), 'hi @Names');
});

test('eviction drops a name together with its member id', async () => {
  const storage = memoryStorage();
  const reg = createMentionRegistry({ storage, maxNamesPerConv: 2 });
  await reg.recordMembers('c1', [
    { displayName: 'One', memberId: 'm1' },
    { displayName: 'Two', memberId: 'm2' },
    { displayName: 'Three', memberId: 'm3' },
  ]);
  const persisted = JSON.parse(await storage.get('mention-registry.json'));
  assert.deepEqual(Object.keys(persisted.c1.names), ['two', 'three']);
  // An id left behind for an evicted name would be unreachable state.
  assert.deepEqual(Object.keys(persisted.c1.ids), ['two', 'three']);
});

test('recordMembers ignores entries missing a name or an id', async () => {
  const storage = memoryStorage();
  const reg = createMentionRegistry({ storage });
  await reg.recordMembers('c1', [
    { displayName: 'Alice', memberId: '' },
    { displayName: '', memberId: 'm-x' },
    { displayName: '  ', memberId: '  ' },
  ]);
  assert.equal(await storage.get('mention-registry.json'), null);
});

// ── handle boundary: a known short name must not eat an unknown longer one ────

test('a known short name does NOT match the prefix of an unknown longer handle', async () => {
  // The failure this guards is not cosmetic: before the boundary check, a
  // registry knowing only `Ann` rewrote `@anna` to `@Anna` and emitted Ann's
  // member id, notifying someone who was never mentioned.
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordMembers('c1', [{ displayName: 'Ann', memberId: 'm-ann' }]);
  assert.deepEqual(await reg.resolveOutbound('ping @anna', 'c1'), { text: 'ping @anna', mentions: [] });
  assert.deepEqual(await reg.resolveOutbound('ping @annabelle', 'c1'), { text: 'ping @annabelle', mentions: [] });
  // The same text must not be canonicalized either — the old API shares the rule.
  assert.equal(await reg.resolveMentions('ping @anna', 'c1'), 'ping @anna');
});

test('a separator only continues a handle when something follows it', async () => {
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordMembers('c1', [{ displayName: 'Ann', memberId: 'm-ann' }]);
  // Sentence punctuation after a complete handle is not part of it.
  assert.deepEqual((await reg.resolveOutbound('hi @Ann.', 'c1')).mentions,
    [{ type: 'member', member_id: 'm-ann' }]);
  // ...but a separator joined to more name characters is.
  assert.deepEqual((await reg.resolveOutbound('hi @ann-lee', 'c1')).mentions, []);
  assert.deepEqual((await reg.resolveOutbound('hi @ann_lee', 'c1')).mentions, []);
});

test('a dotted handle does not resolve to a registry that only knows its first segment', async () => {
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordMembers('c1', [{ displayName: 'athan', memberId: 'm-athan' }]);
  assert.deepEqual((await reg.resolveOutbound('ping @athan.chen', 'c1')).mentions, []);
  assert.deepEqual((await reg.resolveOutbound('ping @athan', 'c1')).mentions,
    [{ type: 'member', member_id: 'm-athan' }]);
});

test('the boundary rule is unicode-aware, not ascii-only', async () => {
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordMembers('c1', [{ displayName: '张三', memberId: 'm-zhang' }]);
  assert.deepEqual((await reg.resolveOutbound('ping @张三丰', 'c1')).mentions, []);
  assert.deepEqual((await reg.resolveOutbound('ping @张三', 'c1')).mentions,
    [{ type: 'member', member_id: 'm-zhang' }]);
});

test('a rejected prefix match does not consume the handle for a later, longer name', async () => {
  // `Ann` is tried first only if it sorts first; either way the rejected span
  // must stay available, or the real participant silently loses their mention.
  const reg = createMentionRegistry({ storage: memoryStorage() });
  await reg.recordMembers('c1', [
    { displayName: 'Ann', memberId: 'm-ann' },
    { displayName: 'Annabelle', memberId: 'm-belle' },
  ]);
  assert.deepEqual((await reg.resolveOutbound('ping @annabelle', 'c1')).mentions,
    [{ type: 'member', member_id: 'm-belle' }]);
  assert.deepEqual((await reg.resolveOutbound('ping @annabelle and @ann', 'c1')).mentions.map((m) => m.member_id).sort(),
    ['m-ann', 'm-belle']);
});
