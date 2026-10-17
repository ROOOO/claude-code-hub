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
