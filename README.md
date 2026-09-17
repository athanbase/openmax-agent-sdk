# @openmaxai/openmax-agent-sdk

CWS agent runtime SDK — the **cws-comm protocol layer** extracted from `zylos-openmax`, so a **Node.js** agent runtime (Claude Code, Codex, OpenClaw) can connect to COCO Workspace without depending on Zylos internals.

> **Scope of this package.** This is a **Node.js / ESM** SDK. It serves Node runtimes and their thin Node adapters — it is **not** the cross-language protocol contract. A non-Node runtime (e.g. a Python/Hermes SDK) does **not** consume this package; it re-implements the same wire protocol against the *canonical, language-neutral contract* in [`schemas/v1/`](./schemas/v1) + [`fixtures/v1/`](./fixtures/v1) (see [`CONTRACT.md`](./CONTRACT.md)). Treat the JS types/shapes here as one conformant implementation, not as the source of truth for other languages.

> Design: `协议归协议、runtime 归 runtime`. This package is **Layer 1** (shared protocol) *for Node*. Runtime-specific bridging lives in thin **Layer 2** adapter repos (`*-openmax`) that import this SDK. Mirrors the `@coco-xyz/hxa-connect-sdk` + adapter precedent.

## Installation

```bash
npm i @openmaxai/openmax-agent-sdk
```

> **`1.0.0` is the first stable release.** A bare `npm i @openmaxai/openmax-agent-sdk`
> resolves it via the `latest` dist-tag. Pre-1.0 alphas remain available under the
> `alpha` dist-tag for history.

> Cutting a new version? See **[`RELEASING.md`](./RELEASING.md)** for the exact
> files to bump and the release flow.

**In the SDK** (generic CWS + agent-level concerns):

- **transport/** — `WsClient` (auth, heartbeat, client keepalive-ping + frame-watchdog, exponential-backoff reconnect, 4001–4006 close-code handling), HTTP client (native `fetch` + auth), CF-Access headers, token/identity management.
- **protocol/** — frame dispatch, message codec (CWS message ↔ neutral shape), access policy, system-message handling.
- **sync/** — `SyncEngine` (`/sync` gap catch-up) + inbox-ledger (dedup + contiguous ack).
- **services/** — `tm` / `kb` / `as` / `comm` / `core` / `conn` service clients over the cws-core REST API.
- **reporters/** — agent-level online-report + runtime metrics (+ cgroup resources, billing status). *(No channel-liveness — that is Zylos-specific, see below.)*
- **identity/** — agent-domain resolution, self-name hydration.
- **orchestrator.js** — a single instantiable class that wires the above (the protocol orchestration currently inlined in `comm-bridge.js`).
- **providers.js** — injection interfaces so the SDK never touches a runtime's filesystem/process manager directly.

**NOT in the SDK** (Zylos/openmax adapter-only, owner decision 2026-07-17):

- **channel-liveness** — enumerating the 13 IM-channel components' pm2 status. Not done here at all.
- **the 13 IM channels** — install / management / connection (`channel-connector`), which are coupled to pm2 + the `zylos` CLI.

These stay entirely inside the `zylos-openmax` adapter.

## Providers (dependency injection)

The SDK is environment-agnostic. A runtime adapter supplies:

- `StorageProvider` — read/write config + cached credentials (no hard-coded `~/zylos` paths).
- `RuntimeStateProvider` — runtime metrics/state for the online + metrics reporters.
- `InboundDelivery` — the core translation point: deliver an inbound CWS message into the runtime's context (Cat.A native channel, or Cat.B `/wake`).
- `Logger` — structured logging sink.

Each provider has a safe no-op/degraded default.

## Orchestrator — the integration surface

`CwsAgentBridge` (`src/orchestrator.js`) is the one class an adapter instantiates. It composes every module above into a working agent: per-org WS lifecycle (open → first-connect `initSyncSeq` or reconnect catch-up → seed inbox-ledger → arm reporters → self-name hydration barrier), the protocol-generic inbound pipeline (**dedupe → fetch detail → normalize → access-policy `decideInbound` → `InboundDelivery.deliver`**), frame dispatch, ledger↔sync-engine wiring, and protocol-generic system-frame handling.

```js
const bridge = new CwsAgentBridge({
  http,                       // CwsHttpClient
  tokenManager,               // TokenManager (mints ws-tickets; optional in tests)
  ws: { baseUrl, reconnectMaxMs, heartbeatIntervalMs, pingIntervalMs, deviceId, clientVersion },
  orgConfigs,                 // [{ org_id, self, owner, access }] — orgs are keyed by org_id
  providers: { storage, runtimeState, inbound, logger },   // inbound is the required seam
  callbacks: { /* adapter seams, all optional — see below */ },
  reporters: { metrics, metricsIntervalMs, version },
});
await bridge.start();         // bootstrap tokens + self-name, open WS pool, arm reporters
await bridge.send(endpoint, content, { orgId, replyTo, mentions });   // outbound reply → cws-core
await bridge.stop();          // disarm all timers, close every WS + ledger, no leaks
```

**Required:** `providers.inbound.deliver(msg, endpoint, priority)` — the SDK hands it a normalized `InboundMessage` after access policy; the adapter does the actual runtime bridging (Cat.A native channel / Cat.B `POST /wake`). INVARIANT: resolve `{ok:true}` only once the message genuinely entered the runtime context (a false ack loses the message — the ledger/`/sync` retry stops).

**Adapter callbacks (seams the SDK never implements):** `loadSession`/`saveSession` (sync cursor), `loadConfig`/`syncSelf` (self-name hydration inputs), `fetchMemberOwner` (sibling-agent DM exemption), `onOwnerBind`/`onOwnerNameHint` (owner hints → config), `onConfigEvent` (`agent.config.*` → **adapter persists**, SDK does not), `onSystemNotice` (policy-gated recall/edit → **adapter formats + delivers**), `onConnectionEvent`/`onChannelEvent` (connection/channel frames → adapter), `onOrgTerminated`/`onAllOrgsTerminated`.

**Still adapter-owned** (behind `InboundDelivery` / callbacks): C4 forwarding + `formatInboundForC4` + work-reference formatting, media download, group-history/context assembly, quoted-message expansion, receive-reactions/typing, `config.json` persistence, pm2 channel install/liveness, auto-upgrade, the argv CLI shells, and dashboard/api-key provisioning. The `orchestrator.js` header block enumerates the full contract.

## Outbound @-mentions

A mention has **two independent halves**, and only one of them is visible:

- **Highlight** is client-side — cws-fe wraps `@<participant display_name>` found in the text. Its candidate list includes the conversation's participants, so plain text alone renders the chip.
- **Notification** is server-side — driven by a structured `mentions` array at the **top level** of the send request, next to `type`/`content`, **never inside `content.body`**. cws-core indexes that array; it is what wakes a mentioned agent and lights the `unread_mention` badge.

Because the two are independent, **"it looks mentioned" is not evidence that anyone was notified** — text with no structured mention renders exactly as blue as a real one. Verify by reading the top-level `mentions` array back from get-message.

`createMentionRegistry` (`src/protocol/mention.js`) covers both halves:

```js
// Names learned from inbound senders carry no member id, so a participant who has
// never spoken cannot be mentioned. Seed ids from the roster — when and how often
// is the adapter's call.
const roster = await comm.conversationMembers({ conversationId });   // always an array
await registry.recordMembers(conversationId,
  roster.map(m => ({ displayName: m.display_name, memberId: m.member_id })));

const { text, mentions } = await registry.resolveOutbound(raw, conversationId);
await comm.send({ conversationId, content: text, mentions });        // or bridge.send(ep, text, { mentions })
```

`resolveMentions(text, conversationId) → string` (canonicalization only) stays as it was. Note the request/response field asymmetry: the request element key is `member_id`, while get-message returns `mentioned_id` — sending `mentioned_id` is rejected with `validation failed`, and the error does not say which field.

## Protocol contract (canonical, language-neutral)

The **source of truth** for the wire protocol is not this JS code — it is the
versioned, language-neutral contract that ships alongside it:

- **[`schemas/v1/`](./schemas/v1)** — JSON Schema (draft 2020-12) for each protocol surface: `frame`, `inbound-message`, `wake-request`, `wake-result`, `failure-class`, plus the `auth-lifecycle.md` state machine. Each schema is `$id`- and `version`-tagged.
- **[`fixtures/v1/`](./fixtures/v1)** — shared golden `{input, expected}` fixtures (frame/system-event classification, normalized inbound messages, wake req/result). **Passing this corpus is the definition of "protocol-conformant" in any language.**

This JS SDK is *one conformant implementation*. `test/contract.test.js` feeds the
fixtures through the real SDK (`classifyFrame` / `classifySystemEvent` / the full
`CwsAgentBridge` inbound pipeline) and validates the output against the schemas —
so any drift between the code and the contract fails the build. A future
Python/Hermes SDK runs the identical fixtures against the identical schemas. See
[`CONTRACT.md`](./CONTRACT.md) for the full contract and its flagged looseness.

## Language

Plain **JavaScript / ESM**, no build step (matches `zylos-openmax`; owner decision 2026-07-17 — TS and JS are runtime-identical). Optional hand-written `types/*.d.ts` may be added later for consumer type hints without adopting a TS build.

This package targets **Node.js only** (see `engines.node >= 20`). It is not published for, nor consumable by, other language runtimes — those bind to the wire protocol directly. The plan for a shared, language-neutral contract is in [`CONTRACT.md`](./CONTRACT.md).

## Status

`1.0.0` — **first stable release. Phase A extraction complete**: transport, protocol, sync, services (tm/kb/as/comm/core/conn), reporters (agent-level), identity, and the `CwsAgentBridge` orchestrator, plus the **canonical `schemas/v1/` + `fixtures/v1/` protocol contract** with a self-verifying conformance test — **full suite green via `node --test`, 0 coupling to Zylos internals**. Next: Phase B — refactor `zylos-openmax` to consume this SDK, then int→prod parity verification (WS keepalive/watchdog focus). See the design doc for the full module map, cut line, wake contract, and migration phasing.
