-- 000001_init_event_hub.down.sql
-- Rollback migration for event hub initialization

DROP INDEX IF EXISTS idx_outbox_unprocessed;
DROP TABLE IF EXISTS outbox_events;
DROP INDEX IF EXISTS idx_entries_transfer_id;
DROP INDEX IF EXISTS idx_entries_account_id;
DROP TABLE IF EXISTS entries;
DROP INDEX IF EXISTS idx_transfers_to_account;
DROP INDEX IF EXISTS idx_transfers_from_account;
DROP TABLE IF EXISTS transfers;
DROP TABLE IF EXISTS accounts;

