# Downstream Changes

This index records changes still maintained on `dev` relative to the consumed
upstream baseline, including features, fixes, and compatibility adjustments.
It tracks current semantic differences, not historical changes. Keep each
change's implementation, tests, and documentation together.

## Baseline

- Consumed upstream: `ding113/claude-code-hub`, `main` at
  `743643847cdf538a72d984663ee504d925e9579f` (v0.9.7).
- Upstream PR target: `ding113/claude-code-hub`, `dev`.
- Fork: `ROOOO/claude-code-hub`, local development branch `dev`.

## Current Changes

| ID | Change | Upstream PR / Status |
| --- | --- | --- |
| [FIX-001](#fix-001-exclusive-responses-websocket-upgrades) | Exclusive Responses WebSocket upgrades | — / Local |
| [FIX-002](#fix-002-connection-local-responses-continuation) | Connection-local Responses continuation | — / Local |
| [FIX-003](#fix-003-consistent-codex-websocket-session-headers) | Consistent Codex WebSocket session headers | — / Local |

## Maintenance

- Keep independently upstreamable behaviors in separate entries. Corrective changes
  and regression tests for the same behavior belong to the existing entry.
- Keep entry IDs stable. Do not renumber or reuse IDs after deleting an entry. Git
  and PR history retain removed entries; no historical archive is maintained here.
- Use `Local` before submitting a PR. Add its link and set `PR open` when submitted;
  if it closes without merging, return the entry to `Local` and retain the link.
- After upstream merges the change, set `Merged upstream, pending sync`. A merged PR
  or refreshed remote reference alone does not remove the downstream difference.
- Update the consumed baseline only after integrating that upstream revision.
  Compare final behavior and regression coverage, including equivalent changes
  implemented independently upstream; commit identity is not proof of equivalence,
  especially after squash or rebase.
- Once the consumed upstream baseline includes equivalent semantics, remove both
  the index row and the detailed entry. If upstream absorbs only part of a change,
  keep its ID and describe only the remaining difference, updating PR links and
  status to match that remaining work.
- Removing an entry does not mean reverting its original commits or deleting
  equivalent upstream code and regression tests.
- Update this index with related implementation, PR-state, or upstream-sync changes.
  Do not record local commit hashes: the document can be part of those commits.

## FIX-001: Exclusive Responses WebSocket upgrades

Code: [server.js](../server.js).
Tests: [server-helpers.test.ts](../src/app/v1/_lib/responses-ws/__tests__/server-helpers.test.ts),
[server-ws-upgrade-routing.test.ts](../tests/unit/server-ws-upgrade-routing.test.ts).

### Symptoms

Responses WebSocket connections can close immediately after a successful `101`
handshake. Clients report a reset without a closing handshake and reconnect.
Different worker initialization orders can produce alternating successful and
failed connections.

### Cause

CCH and Next.js can both handle the same HTTP server `upgrade` event. CCH accepts
the socket, then a Next.js listener closes it. Next.js can register that listener
after the worker's first ordinary HTTP request.

### Solution

Route `/v1/responses` upgrades exclusively to CCH before invoking ordinary
upgrade listeners. Keep normal listener dispatch for other paths, and reject
upgrades when no ordinary listener is registered. Preserve each worker's private
HTTP loopback target, payload limits, and shutdown cleanup.

## FIX-002: Connection-local Responses continuation

Code: [continuation.ts](../src/app/v1/_lib/responses-ws/continuation.ts),
[upstream-adapter.ts](../src/app/v1/_lib/responses-ws/upstream-adapter.ts),
[forwarder.ts](../src/app/v1/_lib/proxy/forwarder.ts),
[provider-selector.ts](../src/app/v1/_lib/proxy/provider-selector.ts),
[response-handler.ts](../src/app/v1/_lib/proxy/response-handler.ts),
[server.js](../server.js).
Tests: [upstream-adapter.test.ts](../src/app/v1/_lib/responses-ws/__tests__/upstream-adapter.test.ts),
[server-ws-continuation.test.ts](../tests/unit/server-ws-continuation.test.ts),
[response-handler-stream-terminal.test.ts](../tests/unit/proxy/response-handler-stream-terminal.test.ts).

### Symptoms

A client can receive `previous_response_not_found` while its WebSocket is still
open. An incremental turn can reach a new upstream connection without the prior
response's connection-local context. Remote Compaction v2 can also appear as a
successful request when its WebSocket stream ends with this error.

### Cause

Logical session affinity does not guarantee reuse of the same physical upstream
WebSocket. Route or credential changes, an upstream disconnect, a busy or evicted
retained connection, or HTTP fallback can lose connection-local state. Replaying
an incremental body on another connection cannot recover that state. The raw
response policy used by compaction also skips general response-body diagnostics.

### Solution

Bind connection-local continuations to a live upstream connection and its
Provider, endpoint, generation, and complete header fingerprint. Recheck the
binding immediately before dispatch, respect current access and availability
rules, and reject changes instead of creating a replacement connection or
retrying the incremental input over HTTP or another Provider.

Keep only the prior response's storage mode on each downstream connection and
pass it through authenticated internal metadata. A later `store:true` request
cannot make an earlier unstored response durable. Exclude connection-local chains
from Replay and fake streaming; complete initial input can still use normal
routing and HTTP fallback.

When context is unavailable, return the structured `previous_response_not_found`
error and close normally after the terminal frame is acknowledged. The client
must resend the full context without the old ID; CCH never drops the ID and
retries an incomplete conversation itself. Retain a downstream connection only
when its last connection-local result came from the matching reusable upstream
WebSocket, and discard queued frames when resetting it.

Carry the exact structured WebSocket continuation failure into attempt-scoped
finalization so compaction cannot record it as a successful HTTP 200. Preserve
ordinary opaque HTTP handling and release request resources once, without
penalizing Provider health for a lost continuation context.

## FIX-003: Consistent Codex WebSocket session headers

Code: [forwarder.ts](../src/app/v1/_lib/proxy/forwarder.ts).
Tests: [proxy-forwarder.test.ts](../tests/unit/proxy/proxy-forwarder.test.ts),
[responses-ws-session-header-continuation.test.ts](../tests/unit/proxy/responses-ws-session-header-continuation.test.ts).

### Symptoms

A normal Codex WebSocket turn and a later raw compaction turn can produce
different outbound headers from the same handshake. Connection-local continuation
protection then rejects compaction because its complete header fingerprint has
changed.

### Cause

Normal session completion supplies both `session_id` and `x-session-id`, while
raw compaction skips that preprocessing. A handshake carrying only one valid
alias therefore produces inconsistent outbound headers. When both handshake
aliases are absent, normal completion can also derive both headers from a valid
body `prompt_cache_key`, while raw compaction omits them. The mismatch also exists
in the consumed upstream baseline, where it can replace the upstream connection.

### Solution

For authenticated internal Codex WebSocket requests, copy a valid outbound
session identifier into its missing header alias. Only when both outbound aliases
are absent, use a valid body `prompt_cache_key` to supply both headers. Preserve
explicitly supplied values, the request body, and the original request headers.
Do not generate identifiers or substitute logical session or connection IDs when
these sources are unavailable. Keep complete fingerprint checks for route,
credential, and other header changes. This fix can be submitted independently of
FIX-002 and FIX-004.

