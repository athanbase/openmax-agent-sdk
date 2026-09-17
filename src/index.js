/**
 * @openmaxai/openmax-agent-sdk — single root export.
 *
 * Scaffold. Modules are re-exported here as they are extracted from
 * zylos-openmax (Phase A). Current tranche: providers.
 */
export * from './providers.js';

// ── transport layer (Phase A · milestone 1) ─────────────────────────────────
// WS client (keepalive-ping + frame-watchdog + backoff reconnect), HTTP client,
// token manager, and Cloudflare-Access headers.
export * from './transport/ws.js';          // WsClient, createDeduper
export * from './transport/http.js';         // CwsHttpClient
export * from './transport/token.js';        // TokenManager
export * from './transport/cf-access.js';    // cfAccessHeaders

// ── protocol layer (Phase A · milestone 2) ──────────────────────────────────
// Runtime-agnostic CWS protocol: message codec (endpoint parse/format, media
// prefix, markdown detect, split), frame classification/dispatch, DM/group
// access-policy engine, and system-message helpers. Runtime-specific formatting
// (e.g. the C4 `formatInboundForC4` XML envelope) is intentionally NOT here —
// it stays in the adapter.
export * from './protocol/message-codec.js';   // newClientMsgId, parseEndpoint, formatEndpoint, looksLikeMarkdown, parseMediaPrefix, splitMessage
export * from './protocol/frame-dispatch.js';  // classifyFrame, classifySystemEvent, createFrameDispatcher, FRAME_KIND
export * from './protocol/access-policy.js';   // decideInbound, isSiblingAgentSender, extractMentions, isSelfNameMentionedInText, notices, VALID_* sets
export * from './protocol/system-message.js';  // isSystemSender, systemEventPriority
export * from './protocol/mention.js';         // createMentionRegistry (outbound @mention canonicalization, StorageProvider-backed)

// ── service clients (Phase A · milestone 3) ─────────────────────────────────
// Programmatic REST clients for the cws-core surface (tm/kb/as/comm/core/conn),
// each taking the shared CwsHttpClient. Extracted from zylos-openmax
// src/cli/*.js; the argv/stdout CLI shell stays in the runtime adapter.
export * from './services/index.js';  // {Tm,Kb,As,Comm,Core,Conn}Service + create* factories

// ── sync layer (Phase A · milestone 4) ──────────────────────────────────────
// Inbox-seq ledger (dedup + contiguous-ack watermark + gap detection) and the
// `/sync` gap catch-up engine (cursor/has_more paging + ack). Protocol-only:
// cursor persistence goes through the injected StorageProvider (ledger) / a
// saveSession callback (engine); recovered events are handed to a supplied
// onMessage handler — message assembly stays in the orchestrator/adapter.
export * from './sync/inbox-ledger.js';   // createInboxLedger
export * from './sync/sync-engine.js';    // SyncEngine, SYNC_PAGE_SIZE, SYNC_MAX_EVENTS

// ── reporters (Phase A · milestone 4) — agent-level only ─────────────────────
// Online self-report (onboarding trigger), runtime-metrics PUT, cgroup CPU/mem
// gauges, and the LLM billing/credit gate. All DI: HTTP via CwsHttpClient,
// runtime state via RuntimeStateProvider. The channel-liveness reporter is
// intentionally NOT here — it is a channel (IM) concern and stays in the
// adapter (owner decision 2026-07-17).
export * from './reporters/online.js';    // createOnlineReporter
export * from './reporters/metrics.js';   // createMetricsReporter, buildPayload, selectPrimaryOrg
export * from './reporters/cgroup.js';    // createCgroupCollector (Linux /sys/fs/cgroup; DI-swappable)
export * from './reporters/billing.js';   // isOrgLLMSuspended, resolveAgentOrigin, shouldSendOverdueNotice, OVERDUE_NOTICE, ...

// ── identity (Phase A · milestone 4) ─────────────────────────────────────────
// Agent public base-URL / domain resolution (consumed by CoreService.agentDomain)
// and the startup self display_name hydration barrier.
export * from './identity/agent-domain.js';          // resolveAgentBaseUrl, resolveAgentIdentityId, normalizeBaseUrl
export * from './identity/self-name-hydration.js';   // createSelfNameHydrator

// ── orchestrator (Phase A · milestone 5) — the integration surface ───────────
// CwsAgentBridge composes every extracted piece (transport/protocol/sync/
// reporters/identity) into one instantiable class: per-org WS lifecycle,
// dedupe→normalize→access-policy→InboundDelivery inbound pipeline, frame
// dispatch, ledger↔sync-engine wiring, and protocol-generic system-frame
// handling. Runtime-specific work (C4 forwarding, formatInboundForC4, media,
// history, config.json persistence, pm2/channel install/liveness, auto-upgrade,
// CLI shells) stays in the adapter behind the injected providers/callbacks.
export * from './orchestrator.js';   // CwsAgentBridge

// Hardcoded literal, NOT a runtime read of package.json. A `createRequire(...)
// ('../package.json')` here does not survive bundling: when a consumer inlines
// the SDK into a self-contained artifact, that call is preserved and resolves
// `../package.json` relative to the CONSUMER's bundle at runtime — which fails
// when the bundle is loaded in isolation. The `SDK_VERSION` test asserts this
// literal stays in sync with package.json, so it can never silently drift.
export const SDK_VERSION = '1.1.0';
