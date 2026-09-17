/**
 * Communication service client — proactive IM operations against cws-core
 * (contract-v5). Reactive IM (replying to a live WebSocket frame) is handled by
 * the transport/orchestrator layers; this client is REST-only proactive IM:
 * starting a DM, sending into a non-current conversation, pulling history, etc.
 *
 * Ported from zylos-openmax `src/cli/comm.js`. The argv/stdout CLI shell stays
 * in the adapter.
 *
 * Two tiers of methods:
 *   1. Pure REST (need only the CwsHttpClient): listConversations, createDm,
 *      createGroup, getConversation, getMessages, send, getMessage, unread,
 *      markRead, search, sync.
 *   2. Config-coupled (design §3.3 — need an injected config provider):
 *      syncOwner, dmPolicy, dmList, dmAllow, dmRevoke. The old direct
 *      loadConfig/updateConfig/enabledOrgs/setOwner coupling is replaced by an
 *      injected `config` provider. When no provider is supplied these methods
 *      throw a clear error rather than reaching into ~/zylos.
 *
 * Config provider interface (all optional; only what a given method needs):
 *   {
 *     enabledOrgs(): OrgConfig[],
 *     getOrgByOrgId(id): OrgConfig | undefined,
 *     updateConfig(fn): Config,        // mutate-in-place, returns updated config
 *     setOwner(orgId, memberId, name): void,
 *   }
 * where OrgConfig has at least { org_id, org_name?, self?, owner?, access? }.
 */

import { randomUUID } from 'node:crypto';
import { looksLikeMarkdown } from '../protocol/message-codec.js';

function ensureClientMsgId(id) {
  return id || `cmsg_${randomUUID()}`;
}

/**
 * Build the cws-core v5 send-message body from caller input. Caller can pass:
 *   - string                              → text/markdown auto-detect
 *   - {text} | {body}                     → text/markdown auto-detect
 *   - {content_type, body, attachments?}  → pass-through (advanced)
 *   - already-built {body:{type,content}} → returned as-is (with client_msg_id)
 *
 * `params.mentions` (optional) is emitted as the request's top-level `mentions`
 * array — build it with `createMentionRegistry().resolveOutbound()`.
 */
function buildSendBody(params) {
  // `mentions` belongs at the TOP LEVEL, next to `type`/`content`: cws-core
  // indexes it from there to notify the mentioned members. An array nested under
  // `content.body` is accepted and stored verbatim as body data and notifies
  // nobody, while reading the message back still shows it — so the wrong
  // placement looks indistinguishable from the right one. See protocol/mention.js.
  const mentions = Array.isArray(params.mentions) && params.mentions.length ? params.mentions : null;

  // Allow advanced caller to override completely. `params.mentions` is still
  // honoured here rather than silently dropped — dropping it would send a
  // perfectly valid-looking message that notifies nobody, which is the exact
  // failure this parameter exists to prevent. A `mentions` inside the override
  // wins, since that branch is the caller taking control of the wire body.
  if (params.body && params.body.content && params.body.type) {
    return {
      client_msg_id: ensureClientMsgId(params.clientMsgId || params.clientMessageId),
      ...(mentions ? { mentions } : {}),
      ...params.body,
      ...(params.replyTo ? { parent_id: params.replyTo } : {}),
    };
  }
  const c = params.content;
  let msgType = params.type;
  let contentType, body, attachments;
  if (c && typeof c === 'object' && c.content_type) {
    contentType = c.content_type;
    body        = c.body ?? {};
    attachments = c.attachments ?? [];
    if (!msgType) msgType = contentType === 'image' ? 'IMAGE'
                       : contentType === 'file' ? 'FILE'
                       : 'AGENT_TEXT';
  } else {
    const text = (typeof c === 'string') ? c
              : (c && typeof c === 'object') ? (c.text ?? c.body ?? '')
              : '';
    contentType = looksLikeMarkdown(text) ? 'markdown' : 'text';
    body        = { text: String(text) };
    attachments = [];
    if (!msgType) msgType = 'AGENT_TEXT';
  }
  return {
    client_msg_id: ensureClientMsgId(params.clientMsgId || params.clientMessageId),
    type:          msgType,
    content:       { content_type: contentType, body, attachments },
    ...(mentions ? { mentions } : {}),
    ...(params.replyTo ? { parent_id: params.replyTo } : {}),
  };
}

export class CommService {
  /**
   * @param {import('../transport/http.js').CwsHttpClient} http
   * @param {object} [config]  ConfigMutator/provider for the config-coupled
   *        commands (syncOwner + dm_* access control). Omit for pure-REST use.
   */
  constructor(http, config = null) {
    if (!http) throw new Error('CommService requires a CwsHttpClient');
    this.http = http;
    this.config = config;
  }

  _p(path) { return this.http.apiPath(path); }

  _requireConfig(method) {
    if (!this.config) {
      throw new Error(`${method} requires an injected config provider`);
    }
    return this.config;
  }

  // ---- Conversation collection ---------------------------------------------

  listConversations(params = {}) {
    return this.http.get(this._p('/conversations'), {
      cursor:           params.cursor ?? params.pageToken,
      limit:            params.limit  ?? params.pageSize,
      include_archived: params.includeArchived,
    });
  }

  // cws-core derives org_id and caller member_id from the JWT — do NOT send them.
  createDm(params = {}) {
    return this.http.post(this._p('/conversations/dm'), {
      peer_member_id: params.peerMemberId || params.participantId || params.peerId,
    });
  }

  createGroup(params = {}) {
    return this.http.post(this._p('/conversations/groups'), {
      name:            params.name || params.title,
      member_ids:      params.memberIds || params.participantIds,
      description:     params.description,
      avatar_media_id: params.avatarMediaId,
      metadata:        params.metadata,
    });
  }

  getConversation(params = {}) {
    return this.http.get(this._p(`/conversations/${params.conversationId}`));
  }

  /**
   * Members of a conversation, ALWAYS as an array.
   *
   * Unlike the other reads here this one normalizes the response instead of
   * handing it back raw. The http client unwraps the D8 envelope, so an
   * unpaginated response arrives as a BARE ARRAY while a paginated one arrives as
   * `{data, pagination}` — a caller writing the usual `res.data` gets undefined
   * for the first shape, iterates zero times and throws nothing, which on disk
   * and in logs is indistinguishable from the call never having run. All the
   * shapes are accepted here so no adapter has to learn that the hard way.
   *
   * Feeds `createMentionRegistry().recordMembers()`: names learned from inbound
   * senders carry no member id, so without a roster read a participant who has
   * never spoken cannot be mentioned.
   */
  async conversationMembers(params = {}) {
    const res = await this.http.get(this._p(`/conversations/${params.conversationId}/members`));
    if (Array.isArray(res)) return res;
    if (Array.isArray(res?.data)) return res.data;
    if (Array.isArray(res?.members)) return res.members;
    return [];
  }

  // ---- Messages ------------------------------------------------------------

  getMessages(params = {}) {
    return this.http.get(this._p(`/conversations/${params.conversationId}/messages`), {
      after_seq:  params.afterSeq,
      before_seq: params.beforeSeq,
      limit:      params.limit,
    });
  }

  send(params = {}) {
    return this.http.post(
      this._p(`/conversations/${params.conversationId}/messages`),
      buildSendBody(params),
    );
  }

  getMessage(params = {}) {
    return this.http.get(
      this._p(`/conversations/${params.conversationId}/messages/${params.messageId}`),
    );
  }

  unread(params = {}) {
    return this.http.get(this._p(`/conversations/${params.conversationId}/unread`));
  }

  markRead(params = {}) {
    return this.http.post(this._p(`/conversations/${params.conversationId}/read`), {
      read_until_seq: params.seq,
    });
  }

  // KB page search (only search surface in v5).
  search(params = {}) {
    return this.http.get(this._p('/search/pages'), {
      query:  params.query || params.q,
      kb_id:  params.kbId,
      limit:  params.limit  ?? params.pageSize,
      offset: params.offset,
      sort:   params.sort,
    });
  }

  // Pull missed events after WS reconnect.
  sync(params = {}) {
    return this.http.post(this._p('/sync'), {
      since_seq: params.sinceSeq,
      device_id: params.deviceId,
      limit:     params.limit,
    });
  }

  // ---- Owner (local cache ↔ cws-core authoritative) ------------------------

  /**
   * Resolve the target org block from an injected config provider. Accepts
   * `org`/`orgId`/`org_id` as an org_id, or an org_name (case-insensitive);
   * with none, defaults to the single enabled org.
   */
  _resolveOrgConfig(p) {
    const config = this._requireConfig('resolveOrgConfig');
    const key = p.org || p.orgId || p.org_id;
    const enabled = config.enabledOrgs();
    if (key) {
      const byKey = enabled.find((o) => o.org_id === key);
      if (byKey) return byKey;
      const byId = config.getOrgByOrgId ? config.getOrgByOrgId(key) : undefined;
      if (byId) return byId;
      const norm = (s) => s?.toLowerCase().replace(/[-_ ]/g, '');
      const keyNorm = norm(key);
      const byName = enabled.find((o) => norm(o.org_name) === keyNorm);
      if (byName) return byName;
      const names = enabled.map((o) => o.org_name || o.org_id).join(', ');
      throw new Error(`org not found in config: "${key}" (known: ${names || 'none'})`);
    }
    if (enabled.length === 1) return enabled[0];
    if (enabled.length === 0) throw new Error('no enabled orgs in config.orgs');
    const names = enabled.map((o) => o.org_name || o.org_id).join(', ');
    throw new Error(`multiple enabled orgs — pass {"org":"<name>"} (one of: ${names})`);
  }

  // Read this agent's own member record; the authoritative owner_member_id lives here.
  _fetchSelfMember(org) {
    const selfId = org.self?.member_id;
    if (!selfId) throw new Error(`org "${org.org_id}" has no self.member_id yet (token exchange not completed)`);
    return this.http.getForOrg(org.org_id, this._p(`/members/${selfId}`));
  }

  async syncOwner(params = {}) {
    const config = this._requireConfig('syncOwner');
    const org = this._resolveOrgConfig(params);
    const member = await this._fetchSelfMember(org);
    const coreOwnerId = member?.owner_member_id || '';
    const localOwnerId = org.owner?.member_id || '';
    if (!coreOwnerId) {
      return { org_id: org.org_id, synced: false, reason: 'core has no owner recorded; local binding left as-is', local_owner_id: localOwnerId };
    }
    if (coreOwnerId === localOwnerId) {
      return { org_id: org.org_id, synced: false, reason: 'already in sync', owner_id: coreOwnerId };
    }
    let name = '';
    try {
      const ownerMember = await this.http.getForOrg(org.org_id, this._p(`/members/${coreOwnerId}`));
      name = ownerMember?.display_name || ownerMember?.username || '';
    } catch { /* name is cosmetic */ }
    config.setOwner(org.org_id, coreOwnerId, name);
    return { org_id: org.org_id, synced: true, previous_owner_id: localOwnerId, owner: { member_id: coreOwnerId, name } };
  }

  // ---- DM access control (local config, hot-reloaded) ----------------------

  dmPolicy(params = {}) {
    const config = this._requireConfig('dmPolicy');
    const org = this._resolveOrgConfig(params);
    const access = org.access || {};
    if (params.policy) {
      const valid = ['open', 'allowlist', 'owner'];
      if (!valid.includes(params.policy)) {
        throw new Error(`Invalid policy: ${params.policy}. Must be one of: ${valid.join(', ')}`);
      }
      config.updateConfig((cfg) => {
        cfg.orgs[org.org_id].access = { ...cfg.orgs[org.org_id].access, dmPolicy: params.policy };
      });
      return { org: org.org_name || org.org_id, dmPolicy: params.policy, applied: true };
    }
    return { org: org.org_name || org.org_id, dmPolicy: access.dmPolicy || 'owner', dmAllowFrom: access.dmAllowFrom || [] };
  }

  dmList(params = {}) {
    this._requireConfig('dmList');
    const org = this._resolveOrgConfig(params);
    const access = org.access || {};
    return { org: org.org_name || org.org_id, dmPolicy: access.dmPolicy || 'owner', dmAllowFrom: access.dmAllowFrom || [] };
  }

  dmAllow(params = {}) {
    const config = this._requireConfig('dmAllow');
    const ids = params.memberIds || params.memberId
      ? [].concat(params.memberIds || params.memberId)
      : [];
    if (!ids.length) throw new Error('memberIds (or memberId) required');
    const org = this._resolveOrgConfig(params);
    const result = config.updateConfig((cfg) => {
      const access = cfg.orgs[org.org_id].access = cfg.orgs[org.org_id].access || {};
      const list = new Set(access.dmAllowFrom || []);
      for (const id of ids) list.add(id);
      access.dmAllowFrom = [...list];
    });
    return { org: org.org_name || org.org_id, dmAllowFrom: result.orgs[org.org_id].access.dmAllowFrom, added: ids };
  }

  dmRevoke(params = {}) {
    const config = this._requireConfig('dmRevoke');
    const ids = params.memberIds || params.memberId
      ? [].concat(params.memberIds || params.memberId)
      : [];
    if (!ids.length) throw new Error('memberIds (or memberId) required');
    const org = this._resolveOrgConfig(params);
    const result = config.updateConfig((cfg) => {
      const access = cfg.orgs[org.org_id].access = cfg.orgs[org.org_id].access || {};
      const remove = new Set(ids.map(String));
      access.dmAllowFrom = (access.dmAllowFrom || []).filter((id) => !remove.has(String(id)));
    });
    return { org: org.org_name || org.org_id, dmAllowFrom: result.orgs[org.org_id].access.dmAllowFrom, removed: ids };
  }
}

export function createCommService(http, config) {
  return new CommService(http, config);
}
