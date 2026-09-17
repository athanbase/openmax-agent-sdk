/**
 * Outbound @-mention canonicalization registry.
 *
 * A mention has TWO independent halves, and only one of them is visible:
 *
 *   1. HIGHLIGHT is client-side. cws-fe scans a message's text for
 *      `@<participant display_name>` and wraps matches in a highlight chip
 *      (`renderTextWithMentions`). Its candidate name list includes the
 *      conversation's participants, so plain text alone is enough to render the
 *      chip.
 *   2. NOTIFICATION is server-side, and is driven by a structured `mentions`
 *      array at the TOP LEVEL of the send request — next to `type`/`content`,
 *      NOT inside `content.body`. cws-core indexes that array; it is what wakes a
 *      mentioned agent and what lights the `unread_mention` badge.
 *
 * The two are independent, which makes this area hostile to eyeballing: text with
 * no structured mention renders exactly as blue as a real one, so "it looks
 * mentioned" is not evidence that anyone was notified. Read the top-level
 * `mentions` array back from get-message instead.
 *
 * So the registry does two things:
 *   1. record the display names AND member ids seen in each conversation, and
 *   2. on send, canonicalize any `@name` token to the exact recorded
 *      display_name (`resolveMentions`, half 1), and resolve the same tokens to
 *      `{type:"member", member_id}` rows for the request's top-level array
 *      (`resolveOutbound`, half 2).
 *
 * Note the request/response field asymmetry: the request element key is
 * `member_id`, while get-message returns `mentioned_id`. Sending `mentioned_id`
 * is rejected with `validation failed`, and the error does not say which field.
 *
 * This is a PLATFORM-level contract (it encodes how cws-fe renders mentions), not
 * a runtime-specific concern — all four *-openmax adapters need identical logic,
 * so it lives in the SDK (issue #8). One implementation + golden fixtures beats
 * four copies drifting apart.
 *
 * Extraction notes (ported from zylos-openmax src/lib/mention.js):
 *   - The hard-coded `fs` + `~/zylos/.../mention-registry.json` path is replaced
 *     by the injected StorageProvider (`get(key)` / `set(key, value)`, string
 *     values). The adapter maps `key` to a concrete path. Because the provider is
 *     async, `recordParticipants()` and `resolveMentions()` are now async.
 *   - The rewrite algorithm is unchanged: longest-name-first, case-insensitive
 *     `@name` → canonical `@<exact display_name>`; unknown `@handles` untouched.
 *   - The per-conversation name set keeps the same MAX_NAMES_PER_CONV cap
 *     (oldest-insertion-order eviction) so a busy group can't grow unbounded.
 */

import { memoryStorage } from '../providers.js';

// Bound the per-conversation name set so a busy group can't grow the state
// unbounded. LRU-ish: cap the number of distinct names retained (oldest dropped).
const MAX_NAMES_PER_CONV = 200;

const norm = (s) => String(s ?? '').trim().toLowerCase();

/**
 * Create a per-conversation @mention canonicalization registry backed by a
 * StorageProvider.
 *
 * State schema (persisted under `key`, default `mention-registry.json`):
 *   { [conversationId]: { names: { [normalizedName]: exactDisplayName },
 *                         ids:   { [normalizedName]: memberId } } }
 *
 * Registries written before structured mentions stored the name map flat
 * (`{ [conversationId]: { [normalizedName]: exactDisplayName } }`). Those are
 * migrated on read, so an adapter upgrading in place keeps the names it learned.
 *
 * @param {object} [opts]
 * @param {import('../providers.js').StorageProvider} [opts.storage] StorageProvider (default in-memory).
 * @param {string} [opts.key] storage key (default `mention-registry.json`).
 * @param {number} [opts.maxNamesPerConv] per-conversation name cap (default 200).
 * @param {(...args:any[])=>void} [opts.log] best-effort log sink.
 * @returns {{recordParticipants:(conversationId:string, names:string|string[])=>Promise<void>, recordMembers:(conversationId:string, members:Array<{displayName:string, memberId:string}>)=>Promise<void>, resolveMentions:(text:string, conversationId:string)=>Promise<string>, resolveOutbound:(text:string, conversationId:string)=>Promise<{text:string, mentions:Array<{type:string, member_id:string}>}>}}
 */
export function createMentionRegistry({
  storage = memoryStorage(),
  key = 'mention-registry.json',
  maxNamesPerConv = MAX_NAMES_PER_CONV,
  log = () => {},
} = {}) {
  // Lazy in-memory cache of the whole registry (single-process orchestrator).
  // Mirrors the original's load-on-use but avoids a storage read per call;
  // writes are write-through so a restart resumes from the persisted state.
  let cache = null;

  async function ensureLoaded() {
    if (cache) return cache;
    try {
      const raw = await storage.get(key);
      cache = raw ? JSON.parse(raw) : {};
    } catch {
      // Missing or corrupt — start fresh; a read failure must never break
      // message handling.
      cache = {};
    }
    return cache;
  }

  async function persist(reg) {
    try {
      await storage.set(key, JSON.stringify(reg));
    } catch (err) {
      // Best-effort: a write failure must never break message handling.
      log(`mention-registry persist failed: ${err?.message || err}`);
    }
  }

  /**
   * Per-conversation bucket in the current `{names, ids}` shape, migrating a
   * legacy flat bucket in place. Detection is by SHAPE, not by key presence — a
   * participant could legitimately be called "names", and a legacy bucket would
   * then carry a string under that key.
   */
  function bucketOf(reg, conversationId, { create = false } = {}) {
    const existing = reg[conversationId];
    if (!existing) return create ? (reg[conversationId] = { names: {}, ids: {} }) : null;
    if (typeof existing.names !== 'object' || existing.names === null) {
      return (reg[conversationId] = { names: { ...existing }, ids: {} });
    }
    if (typeof existing.ids !== 'object' || existing.ids === null) existing.ids = {};
    return existing;
  }

  // Cap retained names (drop oldest insertion order) and keep `ids` aligned so a
  // dropped name cannot leave an id behind that nothing can address any more.
  function evict(conv) {
    const keys = Object.keys(conv.names);
    if (keys.length <= maxNamesPerConv) return;
    for (const k of keys.slice(0, keys.length - maxNamesPerConv)) {
      delete conv.names[k];
      delete conv.ids[k];
    }
  }

  /**
   * Record one or more participant display names seen in a conversation.
   * @param {string} conversationId
   * @param {string|string[]} names
   */
  async function recordParticipants(conversationId, names) {
    if (!conversationId) return;
    const list = (Array.isArray(names) ? names : [names])
      .map((n) => String(n ?? '').trim())
      .filter(Boolean);
    if (!list.length) return;

    const reg = await ensureLoaded();
    const conv = bucketOf(reg, conversationId, { create: true });
    let changed = false;
    for (const name of list) {
      const nkey = norm(name);
      if (conv.names[nkey] !== name) {
        conv.names[nkey] = name;
        changed = true;
      }
    }
    if (!changed) return;
    evict(conv);
    await persist(reg);
  }

  /**
   * Record participants together with their member ids — the only way a name can
   * later resolve to a structured mention. Names learned from inbound senders
   * alone carry no id, so a conversation roster read is what makes a participant
   * who has never spoken mentionable at all (see `CommService.conversationMembers`).
   *
   * @param {string} conversationId
   * @param {Array<{displayName:string, memberId:string}>} members
   */
  async function recordMembers(conversationId, members) {
    if (!conversationId) return;
    const list = (Array.isArray(members) ? members : [members])
      .map((m) => ({ name: String(m?.displayName ?? '').trim(), id: String(m?.memberId ?? '').trim() }))
      .filter((m) => m.name && m.id);
    if (!list.length) return;

    const reg = await ensureLoaded();
    const conv = bucketOf(reg, conversationId, { create: true });
    let changed = false;
    for (const { name, id } of list) {
      const nkey = norm(name);
      if (conv.names[nkey] !== name) { conv.names[nkey] = name; changed = true; }
      if (conv.ids[nkey] !== id) { conv.ids[nkey] = id; changed = true; }
    }
    if (!changed) return;
    evict(conv);
    await persist(reg);
  }

  /**
   * Canonicalize `@name` tokens in outbound text to the exact recorded display
   * name for the conversation, so cws-fe's participant-name matcher highlights
   * them. Only rewrites mentions that match a known participant; leaves all other
   * text (including unknown `@handles`) untouched.
   *
   * @param {string} text
   * @param {string} conversationId
   * @returns {Promise<string>}
   */
  async function resolveMentions(text, conversationId) {
    if (!text || !conversationId || !String(text).includes('@')) return text;
    const reg = await ensureLoaded();
    const conv = bucketOf(reg, conversationId);
    if (!conv) return text;

    // Match cws-fe's strategy: try known names longest-first so a longer name
    // (e.g. "Alice Wong") wins over a shorter prefix ("Alice"). Names may contain
    // spaces, so we match the full display_name case-insensitively after an `@`.
    const namesList = Object.values(conv.names).sort((a, b) => b.length - a.length);
    let out = String(text);
    for (const name of namesList) {
      const esc = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      // `@` + the name (case-insensitive); rewrite to the canonical `@<exact>`.
      // False positive: `esc` is the regex-metachar-escaped name (line above), so the
      // pattern is a literal `@name` — linear, no ReDoS. Lead-approved.
      // nosemgrep: javascript.lang.security.audit.detect-non-literal-regexp.detect-non-literal-regexp
      out = out.replace(new RegExp('@' + esc, 'gi'), '@' + name);
    }
    return out;
  }

  /**
   * Resolve outbound text into the two halves of a mention: the canonicalized
   * text (what cws-fe highlights) and the structured rows for the request's
   * TOP-LEVEL `mentions` array (what actually notifies anyone).
   *
   * A name is only emitted as a row if its member id is known — a name learned
   * from an inbound sender but never from a roster read still canonicalizes and
   * still highlights, it just cannot notify. Callers that need every participant
   * mentionable must seed ids via `recordMembers`.
   *
   * @param {string} text
   * @param {string} conversationId
   * @returns {Promise<{text:string, mentions:Array<{type:string, member_id:string}>}>}
   */
  async function resolveOutbound(text, conversationId) {
    const canonical = await resolveMentions(text, conversationId);
    if (typeof canonical !== 'string' || !conversationId || !canonical.includes('@')) {
      return { text: canonical, mentions: [] };
    }
    const reg = await ensureLoaded();
    const conv = bucketOf(reg, conversationId);
    if (!conv) return { text: canonical, mentions: [] };

    // Longest-first, mirroring resolveMentions, so "@Alice Wong" is consumed as
    // the full name and does not also register the participant "Alice". Matched
    // spans are blanked (length-preserving) in the lowercased scan buffer, so a
    // shorter name cannot match inside a span a longer one already claimed. The
    // returned text is `canonical` — the buffer only drives the scan.
    //
    // Plain substring scanning, deliberately not a RegExp: display names are
    // attacker-influenced input, and building a pattern from them is how this
    // turns into a ReDoS.
    const names = Object.values(conv.names).sort((a, b) => b.length - a.length);
    let restLower = canonical.toLowerCase();
    const mentions = [];
    const seen = new Set();

    for (const name of names) {
      const token = ('@' + name).toLowerCase();
      let matched = false;
      for (let from = 0; ; ) {
        const at = restLower.indexOf(token, from);
        if (at < 0) break;
        matched = true;
        const blank = ' '.repeat(token.length);
        restLower = restLower.slice(0, at) + blank + restLower.slice(at + token.length);
        from = at + token.length;
      }
      if (!matched) continue;
      const memberId = conv.ids[norm(name)];
      if (!memberId || seen.has(memberId)) continue;
      seen.add(memberId);
      mentions.push({ type: 'member', member_id: memberId });
    }

    return { text: canonical, mentions };
  }

  return { recordParticipants, recordMembers, resolveMentions, resolveOutbound };
}
