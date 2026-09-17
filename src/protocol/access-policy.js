/**
 * DM / group access-policy engine (open / allowlist / owner + group modes).
 *
 * This module merges two things from zylos-openmax:
 *   - `src/lib/dm-access.js` → `isSiblingAgentSender` (pure).
 *   - `src/comm-bridge.js` `shouldHandleMessage` (+ its pure helpers
 *     `extractMentions`, `isSelfNameMentionedInText`, and the reject-notice
 *     copy) → exported here as `decideInbound`.
 *
 * The engine is pure policy: it takes an inbound message, its conversation, and
 * the org config, and returns a decision. It performs NO side effects and
 * touches NO filesystem — the two runtime couplings from the original are
 * abstracted:
 *
 *   1. cws-core member-owner lookup (`fetchMemberOwner`) — needed for the
 *      sibling-agent exemption — is injected via `deps.fetchMemberOwner`
 *      (defaults to a resolver that returns null → exemption simply never
 *      fires, a safe degrade).
 *   2. the owner-display-name auto-fill (originally an in-place mutation +
 *      `updateOwnerName` config persist) is surfaced as `decision.ownerNameHint`
 *      instead of being written; the adapter persists it if it wishes.
 *
 * Owner auto-bind on first DM is likewise surfaced as `decision.bindOwnerHint`
 * (unchanged from the original contract).
 */

import { isSystemSender } from './system-message.js';

// Policy schema (pure). Exported so config-update parsing / validation in the
// orchestrator or adapter can share the authoritative sets.
export const VALID_DM_POLICIES  = new Set(['open', 'allowlist', 'owner']);
export const VALID_GROUP_SCOPES = new Set(['open', 'allowlist', 'disabled']);
export const VALID_GROUP_MODES  = new Set(['smart', 'mention', 'silent']);

/**
 * Sibling-agent DM exemption: two agents that share the same owner may DM each
 * other by default, regardless of the target agent's dmPolicy (open / allowlist
 * / owner). This mirrors the owner-exempt branch — an owner's own agents form a
 * trusted circle, so they don't need to be added to each other's allowlist.
 *
 * Only AGENT senders qualify; humans always go through the normal dmPolicy
 * gates. Both owner ids must be known and equal — a missing owner (e.g. the
 * target agent has no bound owner yet, or the sender's owner couldn't be read
 * from cws-core) never grants access.
 *
 * @param {object} [p]
 * @param {string} [p.senderType]    frame sender_type ("HUMAN" | "AGENT" | "SYSTEM")
 * @param {string} [p.senderOwnerId] sender agent's owner_member_id (from cws-core)
 * @param {string} [p.selfOwnerId]   this agent's own owner_member_id
 * @returns {boolean}
 */
export function isSiblingAgentSender({ senderType, senderOwnerId, selfOwnerId } = {}) {
  if (String(senderType || '').toUpperCase() !== 'AGENT') return false;
  if (!selfOwnerId || !senderOwnerId) return false;
  return String(senderOwnerId) === String(selfOwnerId);
}

/**
 * Extract mentioned member ids from an inbound message. cws-comm shape is
 * {entity_id, ...}; raw string ids and {id} variants are supported as fallbacks.
 */
export function extractMentions(msg) {
  const raw =
       msg.mentions
    || msg.mention_user_ids
    || msg.content?.mention_user_ids
    || msg.message?.mentions
    || [];
  return raw.map(m =>
    typeof m === 'string'
      ? m
      : String(m?.entity_id || m?.mentioned_id || m?.id || '')
  ).filter(Boolean);
}

/**
 * Detect @<selfName> in the message text body. This is a FALLBACK for senders
 * that emit a plain-text "@Name" with no structured mentions[] — cws-core does
 * return a structured array on get-message when the sender supplied one (see
 * `extractMentions` above, and protocol/mention.js for the send side), but a
 * sender is free to omit it, and historically most did. Without this fallback
 * the mode=mention gate and the owner-mention bypass would miss those.
 */
export function isSelfNameMentionedInText(msg, selfName) {
  if (!selfName) return false;
  const text =
       msg.content?.body?.text
    || (typeof msg.content === 'string' ? msg.content : '')
    || (typeof msg.message?.content === 'string' ? msg.message.content : '')
    || msg.content_text
    || '';
  if (!text) return false;
  const escaped = selfName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  // `(?![\w-])` keeps "@Zylos" from matching "@Zylos-GavinBox" or "@ZylosX".
  // False positive: `escaped` is the regex-metachar-escaped selfName (line above),
  // so the pattern is a literal `@name` + fixed lookahead — linear, no ReDoS. Lead-approved.
  // nosemgrep: javascript.lang.security.audit.detect-non-literal-regexp.detect-non-literal-regexp
  return new RegExp('@' + escaped + '(?![\\w-])', 'i').test(text);
}

// User-facing notice strings shown when a message is rejected. Group rejections
// only fire when the sender actually @-mentioned us, so a non-@-ed group message
// that hits a policy gate stays silent (no userNotice set). DM rejections always
// notify.
export function noticeDmNotAllowed(ownerName) {
  if (ownerName) return `Sorry, I'm not available for private messages. Please contact ${ownerName} to grant you access.`;
  return "Sorry, I'm not available for private messages. Please ask my owner to grant you access.";
}
export const NOTICE_GROUP_DISABLED =
  "Sorry, group chat is currently disabled.";
export const NOTICE_GROUP_NOT_ALLOWED =
  "Sorry, I'm not available in this group.";
export const NOTICE_GROUP_SENDER_NOT_ALLOWED =
  "Sorry, you don't have permission to interact with me in this group.";

/**
 * Apply DM / group access policy for a specific org. Returns:
 *   { handle: true, reason }   — message passes, agent should respond
 *   { handle: false, reason }  — message dropped (logged with reason)
 *   { handle: true, bindOwnerHint: {memberId, displayName} }
 *                              — pass + caller should auto-bind owner
 *   { handle: true, ownerNameHint: string }
 *                              — pass + caller may persist the owner display name
 *
 * When a drop should be surfaced back to the sender as a polite refusal, the
 * decision additionally carries a `userNotice` string. Caller posts it via the
 * cws-core messages API; sync-replay frames are expected to skip the notice to
 * avoid spamming old conversations after a bug fix.
 *
 * @param {object} msg        inbound message frame / detail
 * @param {object} conv       conversation record ({type, name, ...})
 * @param {object} orgConfig  per-org config ({self, owner, access, org_id})
 * @param {object} [deps]
 * @param {(orgId:string, memberId:string) => Promise<string|null>} [deps.fetchMemberOwner]
 *        cws-core owner lookup used by the sibling-agent exemption. Defaults to
 *        a resolver returning null (exemption never fires — safe degrade).
 * @returns {Promise<object>} decision
 */
export async function decideInbound(msg, conv, orgConfig, deps = {}) {
  const fetchMemberOwner = deps.fetchMemberOwner || (async () => null);
  const selfMemberId = orgConfig.self?.member_id;

  // Skip self-echo: agent's own messages within this org.
  if (msg.sender_id && selfMemberId && msg.sender_id === selfMemberId) {
    return { handle: false, reason: 'self-echo' };
  }

  // System Member (调度中心 等平台播报源) is a trusted, write-only identity —
  // let it through unconditionally, bypassing dmPolicy / owner-binding /
  // groupPolicy, which only exist to filter human/agent senders. handle:true
  // with no userNotice also guarantees we never post a reject notice back to a
  // system DM. See v0.7-event-delivery-design.md §6.3.
  if (isSystemSender(msg)) {
    return { handle: true, reason: 'system-sender' };
  }

  const convType = (conv?.type || '').toLowerCase() || (msg.thread_id ? 'thread' : 'dm');
  const access = orgConfig.access || {};
  const senderId = msg.sender_id;
  const senderName = msg.sender_display_name || msg.sender?.display_name || '';

  if (convType === 'dm') {
    const policy = access.dmPolicy || 'owner';
    // Owner is always allowed in DM, regardless of policy (mirrors the group
    // branch's owner @-mention exemption below). Without this, dmPolicy=allowlist
    // silently drops the bound owner's DMs unless their member_id was also
    // manually added to dmAllowFrom.
    const dmOwnerMemberId = orgConfig.owner?.member_id;
    if (dmOwnerMemberId && String(senderId) === String(dmOwnerMemberId)) {
      const decision = { handle: true, reason: 'dm:owner-exempt' };
      // Original mutated orgConfig.owner.name + persisted via updateOwnerName;
      // here we surface it as a hint and leave persistence to the adapter.
      if (!orgConfig.owner?.name && senderName) decision.ownerNameHint = senderName;
      return decision;
    }
    if (policy === 'open') return { handle: true, reason: 'dm:open' };
    // Sibling-agent exemption: agents under the same owner may DM each other by
    // default, regardless of dmPolicy. Checked here (before allowlist/owner
    // gates, after the cheap open short-circuit) so the cws-core owner lookup
    // only fires for AGENT senders that would otherwise be filtered. sender_type
    // and senderId on the frame are trustworthy (cws-comm sets them from the
    // authenticated principal); the frame just doesn't carry the sender's owner,
    // so we resolve it from cws-core via the injected fetchMemberOwner.
    const selfOwnerId = orgConfig.owner?.member_id;
    const senderType = String(msg.sender_type || msg.message?.sender_type || '').toUpperCase();
    if (selfOwnerId && senderType === 'AGENT') {
      const senderOwnerId = await fetchMemberOwner(orgConfig.org_id, senderId);
      if (isSiblingAgentSender({ senderType, senderOwnerId, selfOwnerId })) {
        return { handle: true, reason: 'dm:sibling-agent' };
      }
    }
    if (policy === 'allowlist') {
      const list = (access.dmAllowFrom || []).map(String);
      if (list.includes(String(senderId))) return { handle: true, reason: 'dm:allowlist' };
      const ownerName = orgConfig.owner?.name;
      return {
        handle: false,
        reason: `dm:allowlist (sender ${senderId} not listed)`,
        userNotice: noticeDmNotAllowed(ownerName),
      };
    }
    // policy === 'owner' — bound state is derived from owner.member_id
    const owner = orgConfig.owner || {};
    if (!owner.member_id) {
      // First DM ever for this org → auto-bind sender as owner and accept.
      return {
        handle: true,
        reason: 'dm:owner (auto-bind)',
        bindOwnerHint: { memberId: senderId, displayName: senderName },
      };
    }
    // A bound owner is already accepted by the owner-exempt check above; any
    // other sender under owner-policy is rejected.
    return {
      handle: false,
      reason: `dm:owner (sender ${senderId} != bound owner ${owner.member_id})`,
      userNotice: noticeDmNotAllowed(owner.name),
    };
  }

  // group / thread — compute mentioned/owner once up front so all gates and
  // their userNotice decisions share the same view.
  const policy = access.groupPolicy || 'allowlist';
  const convId = msg.conversation_id;
  const groupCfg = (access.groups || {})[convId];

  // Mention detection has two paths: structured mentions[] from cws-comm, and
  // a text-based "@<selfName>" fallback for messages where the server returns
  // the raw text without a structured mentions array.
  const mentions = extractMentions(msg).map(String);
  const mentionedById = !!selfMemberId && mentions.includes(String(selfMemberId));
  // Match against BOTH the authoritative cws-core display_name (self.display_name)
  // and any hand-configured self.name (kept as an alias). This is the only viable
  // path for cws-comm-native messages, whose @ is plain text with no structured
  // mentions[] — so a mismatched/empty self.name must not silently drop a real @.
  const selfNames = [orgConfig.self?.display_name, orgConfig.self?.name].filter(Boolean);
  const mentionedByText = selfNames.some((n) => isSelfNameMentionedInText(msg, n));
  const mentioned = mentionedById || mentionedByText;
  const ownerMemberId = orgConfig.owner?.member_id;
  const senderIsOwner = !!ownerMemberId && String(senderId) === String(ownerMemberId);

  if (policy === 'disabled') {
    return {
      handle: false,
      reason: 'group:disabled',
      userNotice: mentioned ? NOTICE_GROUP_DISABLED : undefined,
    };
  }

  // Owner @-mention bypasses the allowlist gate.
  if (policy === 'allowlist' && !groupCfg && !(senderIsOwner && mentioned)) {
    return {
      handle: false,
      reason: `group:allowlist (${convId} not in groups{})`,
      userNotice: mentioned ? NOTICE_GROUP_NOT_ALLOWED : undefined,
    };
  }

  // mode: per-group `mode` if present, else default to 'mention'
  const mode = groupCfg?.mode || 'mention';
  if (mode === 'mention' && !mentioned) {
    // No userNotice — this is normal background traffic, replying would spam.
    return { handle: false, reason: 'group:mention (not @-ed)' };
  }
  // mode === 'smart' bypasses the mention requirement

  // allowFrom: ['*'] / [] = all members allowed; otherwise restrict.
  // Owner is exempt from per-group allowFrom.
  const allowFrom = groupCfg?.allowFrom;
  if (allowFrom && allowFrom.length > 0 && !allowFrom.includes('*') && !senderIsOwner) {
    if (!allowFrom.map(String).includes(String(senderId))) {
      return {
        handle: false,
        reason: `group:allowFrom (sender ${senderId} not allowed in ${convId})`,
        userNotice: mentioned ? NOTICE_GROUP_SENDER_NOT_ALLOWED : undefined,
      };
    }
  }

  const ownerTag = (!groupCfg && senderIsOwner && mentioned) ? ' [owner-mention-bypass]' : '';
  return {
    handle: true,
    reason: `group:${policy}/${mode}${ownerTag}`,
    mode,
    mentioned,
    groupCfg,
  };
}
