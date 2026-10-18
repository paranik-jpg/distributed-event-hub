-- 000001_init_event_hub.up.sql
-- Enterprise Database Schema: Double-Entry Financial Ledger & Transactional Outbox Engine

-- 1. Enable UUID Extension (pgcrypto / uuid-ossp for random UUID generation)
-- In PostgreSQL, extensions are like plugins or libraries.
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================================
-- TABLE 1: ACCOUNTS
-- Represents client balances. Enforces strict non-negative balance constraint.
-- ============================================================================
CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner VARCHAR(255) NOT NULL,
    -- A 3-letter currency code
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0),
    -- DEFAULT NOW() => Record the exact time the row was created.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- TABLE 2: TRANSFERS
-- Tracks intent of money moving between two distinct accounts.
-- ============================================================================
CREATE TABLE IF NOT EXISTS transfers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- REFERENCES accounts(id) => This is a Foreign Key
    -- ON DELETE RESTRICT => If someone tries to delete an account from the accounts table that has transfers attached to it, Postgres blocks the deletion.
    from_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    to_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    amount BIGINT NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- An account is forbidden from transferring money to itself.
    CONSTRAINT chk_different_accounts CHECK (from_account_id <> to_account_id)
);

-- An index creates a B-Tree lookup structure so searching for an account's transfers is nearly instant: O(log N).
CREATE INDEX IF NOT EXISTS idx_transfers_from_account ON transfers(from_account_id);
CREATE INDEX IF NOT EXISTS idx_transfers_to_account ON transfers(to_account_id);

-- ============================================================================
-- TABLE 3: ENTRIES (Double-Entry Bookkeeping Ledger)
-- Immutable audit log recording all debits (negative) and credits (positive).
-- Never updated or deleted; only appended.
-- ============================================================================
CREATE TABLE IF NOT EXISTS entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    transfer_id UUID REFERENCES transfers(id) ON DELETE RESTRICT,
    amount BIGINT NOT NULL, -- negative for debit, positive for credit
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entries_account_id ON entries(account_id);
CREATE INDEX IF NOT EXISTS idx_entries_transfer_id ON entries(transfer_id);

-- ============================================================================
-- TABLE 4: OUTBOX_EVENTS (Transactional Outbox Pattern)
-- Solves the Dual-Write Problem. Enables at-least-once message delivery to Redis.
-- ============================================================================
CREATE TABLE IF NOT EXISTS outbox_events (
    -- BIGSERIAL => An auto-incrementing integer
    sequence_id BIGSERIAL PRIMARY KEY,
    id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(64) NOT NULL,      -- e.g., 'TRANSFER', 'ACCOUNT'
    aggregate_id VARCHAR(128) NOT NULL,       -- ID of the related domain entity
    event_type VARCHAR(64) NOT NULL,          -- e.g., 'TRANSFER_CREATED'
    payload JSONB NOT NULL,                   -- Structured domain event data
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING'
    -- PENDING: Saved in Postgres, waiting to be sent to Redis.
    -- PROCESSING: A background worker picked it up.
    -- PUBLISHED: Successfully sent to Redis Pub/Sub!
    -- DEAD: Failed after 5 retries (Dead Letter Queue) so engineers can investigate without breaking the queue.
        CHECK (status IN ('PENDING', 'PROCESSING', 'PUBLISHED', 'DEAD')),
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 5,
    locked_until TIMESTAMPTZ,                 -- Lease timestamp for worker concurrency lock
    last_error TEXT,                          -- Error message if dispatch failed
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- PARTIAL INDEX: HIGH-PERFORMANCE WORKER QUEUE SCANNING
-- Keeps index footprint microscopic by only indexing unhandled events.
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed
ON outbox_events (sequence_id ASC)
WHERE status IN ('PENDING', 'PROCESSING');
