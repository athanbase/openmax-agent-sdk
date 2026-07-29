/**
 * JWT token manager for cws-core auth flow.
 *
 * Three operations (cws-core auth.go):
 *
 *   exchange:   POST /auth/agent/token
 *               Authorization: Bearer <api_key>  (cwsk_xxx)
 *               Body: { org_id? }                 (empty body == identity-only JWT)
 *               → { access_token, access_token_expires_at,
 *                   refresh_token, refresh_token_expires_at }
 *
 *   refresh:    POST /auth/refresh
 *               Authorization: Bearer <access_token>
 *               Body: { refresh_token, org_id? }
 *               → rotated token pair
 *
 *   wsTicket:   POST /auth/ws-ticket
 *               Authorization: Bearer <access_token>
 *               Body: { org_id }   (server requires org-scoped JWT)
 *               → { ticket, expires_at }  (30s TTL, one-time)
 *
 * Per-org caching: access_tokens are bound to a specific org_id, so we cache
 * state in a Map keyed by org_id (or '' for identity-only). Disk persistence
 * likewise lives behind the injected StorageProvider under keys
 * `tokens/<org_id|_identity>.json`.
 *
 * api_key binding: the cache key is org-only, but a JWT is minted from a
 * specific api_key. If the api_key changes while the org stays the same (common
 * in testing: swapping identity within one org), a purely org-keyed cache would
 * hand back a STALE JWT minted from the OLD api_key — the agent would then
 * authenticate as the wrong identity. To prevent that, every persisted record
 * carries `apiKeyFp = sha256(api_key).slice(0,8)`; on load we compare it against
 * the current api_key's fingerprint and treat a mismatch (or a legacy record
 * with no fingerprint) as a cache miss, forcing a fresh exchange. This keeps a
 * single self-healing file per org (no orphaned `<org>-<fp>.json` files). The
 * fingerprint is a hash, never the api_key itself, so it is safe to log.
 *
 * Inflight Promise dedup: concurrent callers asking for the same org's JWT
 * share a single in-flight HTTP request — important on boot when N orgs spin
 * up at once and again when a CLI fan-outs several calls before the cache
 * is warm.
 *
 * Side-effect on first exchange: when an org-scoped JWT comes back, we decode
 * the `member_id` claim and surface it via the injected `onMemberId(orgId,
 * memberId)` callback. In zylos-openmax this wrote back into
 * `config.orgs[org_id].self.member_id`; that config mutation is now the adapter's
 * responsibility (SDK just reports the decoded id).
 *
 * Extraction notes (Zylos coupling removed):
 *   - The hard-coded `~/zylos/components/openmax/runtime/tokens` directory is
 *     gone; disk persistence goes through the injected `storage` provider.
 *   - `loadConfig()` (api_key / bff_url / default org) is replaced by
 *     constructor options + env (COCO_API_KEY / COCO_API_URL / COCO_ORG_ID).
 *   - `updateConfig()` member_id write-back is replaced by the `onMemberId`
 *     callback.
 *   - Logging routes through an injected `logger` (defaults to console).
 *   - `fetch` is injectable for testing (defaults to global fetch).
 * No dependency on the HTTP client — uses raw fetch to avoid circular imports
 * (matching the source).
 */

import { createHash } from 'node:crypto';
import { cfAccessHeaders } from './cf-access.js';
import { redactSecrets } from './redact.js';
import { memoryStorage } from '../providers.js';

const REFRESH_MARGIN_MS = 60_000;   // refresh when <60 s remain on access_token
const LOG = '[token]';

// ── redirect handling (credential-leak guard, P1-C) ──────────────────────────
// Every token request carries a Bearer credential (api_key on exchange, the
// access_token on refresh/ws-ticket). Native fetch auto-follows 3xx and re-sends
// that Authorization header to the target; a cross-origin redirect would leak
// the credential. We use redirect:'manual' and fail closed on any redirect that
// leaves the cws-core origin (see _corePostNoAutoFollow).
const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);
function isRedirectStatus(s) { return REDIRECT_STATUSES.has(s); }
function readLocation(res) {
  const h = res?.headers;
  if (!h || typeof h.get !== 'function') return null;
  return h.get('location') ?? h.get('Location') ?? null;
}
function safeOrigin(u) {
  try { return new URL(u).origin; } catch { return String(u); }
}
const MAX_REDIRECT_HOPS = 5;

// The full request-body header set, dropped when a redirect downgrades a
// body-carrying request to a bodyless GET (matches the Fetch spec).
const REQUEST_BODY_HEADERS = new Set([
  'content-encoding', 'content-language', 'content-location',
  'content-type', 'content-length',
]);
function stripBodyHeaders(headers) {
  if (!headers || typeof headers !== 'object') return headers;
  const out = {};
  for (const [k, v] of Object.entries(headers)) {
    if (REQUEST_BODY_HEADERS.has(k.toLowerCase())) continue;
    out[k] = v;
  }
  return out;
}

// Rewrite the fetch init for a FOLLOWED redirect to match native fetch's
// method/body semantics (per WHATWG Fetch): 301/302 downgrade to GET ONLY for
// POST; 303 → GET for any non-GET/HEAD; 307/308 preserve method+body; GET/HEAD
// unchanged. Token requests are POSTs, so a same-origin 301/302 downgrades to
// GET (a re-POST with the stale JSON body would break a core auth endpoint that
// legitimately redirects).
function rewriteInitForRedirect(init, status) {
  const method = String(init.method || 'GET').toUpperCase();
  const isGetOrHead = method === 'GET' || method === 'HEAD';
  const downgradeToGet = (status === 303 && !isGetOrHead)
    || ((status === 301 || status === 302) && method === 'POST');
  if (!downgradeToGet) return init;   // 307/308 preserve; non-POST 301/302 preserve; GET/HEAD unchanged
  return { ...init, method: 'GET', body: undefined, headers: stripBodyHeaders(init.headers) };
}

// ── RPC logging (env-gated, mirrors the HTTP client) ─────────────────────────
function rpcLogStdoutEnabled() {
  return process.env.COCO_RPC_LOG !== '0';
}

// ── helpers ──────────────────────────────────────────────────────────────────
function toMs(val) {
  if (!val) return 0;
  if (typeof val === 'number') return val;
  return new Date(val).getTime() || 0;
}

/**
 * Short, non-reversible fingerprint of an api_key, used to bind a cached token
 * to the key that minted it. Returns '' when there is no api_key to fingerprint.
 * Never derived from (nor derives) the token; safe to log at debug level.
 */
function apiKeyFingerprint(apiKey) {
  if (!apiKey) return '';
  return createHash('sha256').update(String(apiKey)).digest('hex').slice(0, 8);
}

/** Decode JWT claims (no signature check — for member_id extraction only). */
function decodeJwtClaims(jwt) {
  try {
    const parts = jwt.split('.');
    if (parts.length !== 3) return null;
    const payload = Buffer.from(parts[1].replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
    return JSON.parse(payload);
  } catch {
    return null;
  }
}

export class TokenManager {
  /**
   * @param {Object} [opts]
   * @param {string} [opts.apiKey]              cwsk_ api key (else COCO_API_KEY env)
   * @param {string} [opts.coreUrl]             base URL for /auth/* (else COCO_API_URL env, else localhost)
   * @param {Object} [opts.cfAccess]            cf_access config for CF-Access headers
   * @param {import('../providers.js').StorageProvider} [opts.storage]  token persistence (default in-memory)
   * @param {() => string} [opts.resolveDefaultOrgId]  default org for ws-ticket (else COCO_ORG_ID env)
   * @param {(orgId: string, memberId: string) => (void|Promise<void>)} [opts.onMemberId]  member_id write-back hook
   * @param {number} [opts.refreshMarginMs]     early-refresh margin (default 60s)
   * @param {{log:Function,warn:Function,error:Function}} [opts.logger]  default console
   * @param {typeof fetch} [opts.fetch]         fetch impl (default global fetch)
   */
  constructor({
    apiKey,
    coreUrl,
    cfAccess,
    storage,
    resolveDefaultOrgId,
    onMemberId,
    refreshMarginMs = REFRESH_MARGIN_MS,
    logger,
    fetch: fetchImpl,
  } = {}) {
    this._apiKey = apiKey || null;
    this._coreUrl = coreUrl || null;
    this._cfAccess = cfAccess;
    this._storage = storage || memoryStorage();
    this._resolveDefaultOrgId = resolveDefaultOrgId || (() => process.env.COCO_ORG_ID || '');
    this._onMemberId = onMemberId || null;
    this._refreshMarginMs = refreshMarginMs;
    this._logger = logger || console;
    this._fetch = fetchImpl || ((...a) => fetch(...a));

    this._stateByOrg = new Map();  // orgId('' == identity) → token state
    this._inflight = new Map();    // cache key → Promise
  }

  // ── config resolution ──────────────────────────────────────────────────────
  _resolveApiKey() {
    return process.env.COCO_API_KEY || this._apiKey || '';
  }

  _resolveCoreUrl() {
    const base = process.env.COCO_API_URL || this._coreUrl || 'http://127.0.0.1:8080';
    return base.replace(/\/$/, '');
  }

  _resolveOrgId(orgId) {
    if (orgId) return orgId;
    return this._resolveDefaultOrgId();
  }

  // ── api_key binding (stale-JWT / wrong-identity guard) ───────────────────────
  /** Fingerprint of the currently configured api_key (''' if none). */
  _currentFp() {
    return apiKeyFingerprint(this._resolveApiKey());
  }

  /**
   * Is a cached token record still usable for the current api_key? A record is
   * valid only when its stored fingerprint matches the current api_key's. A
   * mismatch means the api_key changed (the JWT belongs to a different identity);
   * a legacy record with no `apiKeyFp` predates this binding and is treated as a
   * miss so it is safely re-exchanged. When no api_key is configured we cannot
   * compare, so we leave the record as-is (a downstream exchange fails loudly).
   */
  _cacheValidForApiKey(state) {
    if (!state) return false;
    const fp = this._currentFp();
    if (!fp) return true;                 // no api_key to compare against
    if (state.apiKeyFp === fp) return true;
    this._logger.error(
      `${LOG} api_key changed (record fp=${state.apiKeyFp ?? '(none)'} != current fp=${fp}); discarding cached token`,
    );
    return false;
  }

  // ── inflight dedup ──────────────────────────────────────────────────────────
  _withInflight(key, factory) {
    if (this._inflight.has(key)) return this._inflight.get(key);
    const p = factory().finally(() => this._inflight.delete(key));
    this._inflight.set(key, p);
    return p;
  }

  // ── redirect-safe fetch (P1-C) ──────────────────────────────────────────────
  // Token requests always carry a Bearer credential and always target cws-core.
  // We disable auto-follow and refuse any redirect that leaves the core origin,
  // so the credential can never be re-sent to a third party. A same-origin
  // redirect is followed; a 3xx without a Location is handed back unchanged.
  async _fetchNoAutoFollow(url, init) {
    const coreOrigin = safeOrigin(this._resolveCoreUrl());
    let curUrl = String(url);
    let curInit = { ...init, redirect: 'manual' };
    for (let hop = 0; ; hop++) {
      const res = await this._fetch(curUrl, curInit);
      if (!isRedirectStatus(res.status)) return res;
      const loc = readLocation(res);
      if (!loc) return res;
      if (hop >= MAX_REDIRECT_HOPS) {
        throw new Error(`token: too many redirects (>${MAX_REDIRECT_HOPS}) from ${url}`);
      }
      let nextUrl;
      try { nextUrl = new URL(loc, curUrl).toString(); }
      catch { throw new Error(`token: invalid redirect Location "${loc}" from ${curUrl}`); }
      if (safeOrigin(nextUrl) !== coreOrigin) {
        const err = new Error(
          `token: refusing cross-origin redirect to ${safeOrigin(nextUrl)} that would carry the auth credential (from ${coreOrigin})`,
        );
        err.status = res.status;
        err.code = 'CROSS_ORIGIN_REDIRECT';
        throw err;
      }
      curUrl = nextUrl;
      // Match native fetch method/body semantics on the followed hop (P2-b).
      curInit = rewriteInitForRedirect(curInit, res.status);
    }
  }

  // ── raw HTTP helper (no auth dependency) ────────────────────────────────────
  async _corePost(endpoint, body, bearerToken) {
    const url = `${this._resolveCoreUrl()}${endpoint}`;
    const headers = {
      'Content-Type': 'application/json',
      ...cfAccessHeaders(this._cfAccess),
    };
    if (bearerToken) headers.Authorization = `Bearer ${bearerToken}`;

    if (rpcLogStdoutEnabled()) {
      this._logger.log(`[rpc] → POST ${url} req: ${JSON.stringify(redactSecrets(body))}`);
    }

    // Fail-closed on cross-origin redirects so the Bearer credential is never
    // re-sent off the cws-core origin (P1-C).
    const res = await this._fetchNoAutoFollow(url, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    });
    const text = await res.text();
    let data;
    try { data = JSON.parse(text); } catch { data = text; }

    if (rpcLogStdoutEnabled()) {
      const redacted = typeof data === 'string' ? data : redactSecrets(data);
      const bodyStr = typeof redacted === 'string' ? redacted : JSON.stringify(redacted);
      const level = res.status >= 400 ? 'warn' : 'log';
      this._logger[level](`[rpc] ← POST ${url} resp ${res.status}: ${bodyStr}`);
    }

    if (!res.ok) {
      const msg = (data && typeof data === 'object'
        ? (data.detail || data.error || data.message)
        : null) || text || `HTTP ${res.status}`;
      const err = new Error(`${endpoint}: ${msg}`);
      err.status = res.status;
      throw err;
    }
    return data;
  }

  // ── member_id write-back hook ──────────────────────────────────────────────
  async _writeBackMemberId(orgId, jwt) {
    if (!orgId || !jwt || !this._onMemberId) return;
    const claims = decodeJwtClaims(jwt);
    const memberId = claims?.member_id || claims?.mid;
    if (!memberId) return;
    try {
      await this._onMemberId(orgId, memberId);
    } catch (e) {
      this._logger.warn(`${LOG} onMemberId(${orgId}) failed:`, e.message);
    }
  }

  // ── disk persistence (per-org, via StorageProvider) ─────────────────────────
  _tokenKey(orgIdOrEmpty) {
    const safe = orgIdOrEmpty ? orgIdOrEmpty : '_identity';
    return `tokens/${safe}.json`;
  }

  async _readDisk(orgIdOrEmpty) {
    try {
      const raw = await this._storage.get(this._tokenKey(orgIdOrEmpty));
      return raw ? JSON.parse(raw) : null;
    } catch { return null; }
  }

  async _writeDisk(orgIdOrEmpty, state) {
    try {
      await this._storage.set(this._tokenKey(orgIdOrEmpty), JSON.stringify(state, null, 2));
    } catch (e) {
      this._logger.warn(`${LOG} writeDisk(${orgIdOrEmpty || '_identity'}) failed:`, e.message);
    }
  }

  // ── public API ──────────────────────────────────────────────────────────────

  /**
   * Exchange api_key for a fresh JWT pair. Pass orgId='' (or omit) for an
   * identity-only JWT (no org context). Pass a real orgId for an org-scoped
   * JWT (server validates active membership; 401 if missing).
   */
  async exchange(orgIdArg) {
    const oid = orgIdArg || '';                  // '' == identity-only
    return this._withInflight(`exchange:${oid}`, async () => {
      const apiKey = this._resolveApiKey();
      if (!apiKey) throw new Error('token.exchange: api_key not set');
      const body = oid ? { org_id: oid } : {};
      this._logger.error(`${LOG} exchange org=${oid || '(identity-only)'}`);
      const raw = await this._corePost('/auth/agent/token', body, apiKey);
      const d = (raw && typeof raw === 'object' && raw.data) ? raw.data : raw;
      const state = {
        access_token:             d.access_token,
        access_token_expires_at:  toMs(d.access_token_expires_at),
        refresh_token:            d.refresh_token,
        refresh_token_expires_at: toMs(d.refresh_token_expires_at),
        apiKeyFp:                 apiKeyFingerprint(apiKey),
      };
      this._stateByOrg.set(oid, state);
      await this._writeDisk(oid, state);
      if (oid) await this._writeBackMemberId(oid, state.access_token);
      this._logger.error(`${LOG} exchange ok org=${oid || '(identity-only)'} exp=${new Date(state.access_token_expires_at).toISOString()}`);
      return state.access_token;
    });
  }

  async refresh(orgIdArg) {
    const oid = orgIdArg || '';
    return this._withInflight(`refresh:${oid}`, async () => {
      let s = this._stateByOrg.get(oid) || await this._readDisk(oid);
      // A token minted from a different api_key (or a pre-binding legacy record)
      // must not be refreshed — the refresh_token itself belongs to the old
      // identity. Fall through to a fresh exchange with the current api_key.
      if (s && !this._cacheValidForApiKey(s)) s = null;
      if (!s?.refresh_token) return this.exchange(oid);
      try {
        const body = oid ? { refresh_token: s.refresh_token, org_id: oid }
                         : { refresh_token: s.refresh_token };
        this._logger.error(`${LOG} refresh org=${oid || '(identity-only)'}`);
        const raw = await this._corePost('/auth/refresh', body, s.access_token);
        const d = (raw && typeof raw === 'object' && raw.data) ? raw.data : raw;
        const state = {
          access_token:             d.access_token,
          access_token_expires_at:  toMs(d.access_token_expires_at),
          refresh_token:            d.refresh_token ?? s.refresh_token,
          refresh_token_expires_at: toMs(d.refresh_token_expires_at) || s.refresh_token_expires_at,
          apiKeyFp:                 this._currentFp(),
        };
        this._stateByOrg.set(oid, state);
        await this._writeDisk(oid, state);
        if (oid) await this._writeBackMemberId(oid, state.access_token);
        this._logger.error(`${LOG} refresh ok org=${oid || '(identity-only)'} exp=${new Date(state.access_token_expires_at).toISOString()}`);
        return state.access_token;
      } catch (err) {
        this._logger.warn(`${LOG} refresh(${oid || '_identity'}) failed, re-exchanging with api_key:`, err.message);
        return this.exchange(oid);
      }
    });
  }

  /**
   * Return a valid access token for the given org (or identity-only when orgId
   * is empty). Uses the cache when possible; falls through to refresh/exchange.
   *
   * IMPORTANT: callers that need an org-scoped JWT (e.g. before ws-ticket) must
   * pass a non-empty orgId. Callers that explicitly want identity-only (e.g.
   * org-create flow) should pass '' or omit.
   */
  async getAccessToken(orgIdArg) {
    const oid = orgIdArg || '';
    let s = this._stateByOrg.get(oid);
    // Drop an in-memory record minted from a now-changed api_key.
    if (s && !this._cacheValidForApiKey(s)) {
      this._stateByOrg.delete(oid);
      s = null;
    }
    if (!s) {
      s = await this._readDisk(oid);
      // A persisted record from a different api_key (or a legacy record with no
      // fingerprint) is a cache miss → fall through to refresh/exchange.
      if (s && !this._cacheValidForApiKey(s)) s = null;
      if (s) this._stateByOrg.set(oid, s);
    }
    const now = Date.now();
    if (s?.access_token && s.access_token_expires_at - now > this._refreshMarginMs) {
      return s.access_token;
    }
    if (s?.refresh_token) return this.refresh(oid);
    return this.exchange(oid);
  }

  async getWsTicket(orgIdArg) {
    const oid = this._resolveOrgId(orgIdArg);
    if (!oid) throw new Error('token.getWsTicket: org_id required (no default org configured)');
    const accessToken = await this.getAccessToken(oid);
    this._logger.error(`${LOG} ws-ticket org=${oid}`);
    const raw = await this._corePost('/auth/ws-ticket', { org_id: oid }, accessToken);
    const d = (raw && typeof raw === 'object' && raw.data) ? raw.data : raw;
    if (!d.ticket) throw new Error('token.getWsTicket: server returned no ticket');
    this._logger.error(`${LOG} ws-ticket ok org=${oid}`);
    return d.ticket;
  }

  /**
   * Invalidate the cached token for a specific org (e.g. after WS 4003).
   * If no orgId is passed, clears the entire cache.
   */
  invalidate(orgIdArg) {
    if (orgIdArg === undefined) {
      this._stateByOrg.clear();
      return;
    }
    this._stateByOrg.delete(orgIdArg || '');
  }
}
