-- Lease semantics from the PAS orchestrator spike (2026-09-06).
--
-- `lease_secs`: the lease length a worker asked for when it claimed, honoured
-- by every renewal until the item settles. NULL means the 30 s default.
--
-- `last_worker_id` / `last_lease_fence`: who held an expired lease and with
-- which fence, recorded when the claim path re-queues the item. A heartbeat
-- from that worker presenting that fence, arriving while the item is still
-- pending and unclaimed, re-attaches the lease instead of losing the work.
ALTER TABLE work_items ADD COLUMN lease_secs INTEGER;
ALTER TABLE work_items ADD COLUMN last_worker_id TEXT;
ALTER TABLE work_items ADD COLUMN last_lease_fence INTEGER;
