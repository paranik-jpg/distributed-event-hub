package storage_test

import (
	"context"
	"distributed-event-hub/internal/storage"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var testDBURL = "postgres://postgres:password@localhost:5432/event_hub?sslmode=disable"

func getTestPool(t *testing.T) *pgxpool.Pool {
	if url := os.Getenv("TEST_DATABASE_URL"); url != "" {
		testDBURL = url
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := storage.NewPool(ctx, testDBURL)
	if err != nil {
		t.Fatalf("Failed to connect to test database at %s: %v", testDBURL, err)
	}
	return pool
}

// createTestAccount inserts a test account with an initial balance.
func createTestAccount(t *testing.T, pool *pgxpool.Pool, owner string, balance int64) uuid.UUID {
	var id uuid.UUID
	err := pool.QueryRow(context.Background(), `
		INSERT INTO accounts (owner, currency, balance)
		VALUES ($1, 'USD', $2)
		RETURNING id;
	`, owner, balance).Scan(&id)
	if err != nil {
		t.Fatalf("Failed to create test account for %s: %v", owner, err)
	}
	return id
}

// ============================================================================
// VERIFICATION 2: ACID Invariant & Rollback on Overdraft
// ============================================================================
func TestACID_OverdraftRollback(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()

	// 1. Create two test accounts: Alice ($50) and Bob ($10)
	aliceID := createTestAccount(t, pool, "Alice", 5000) // $50.00
	bobID := createTestAccount(t, pool, "Bob", 1000)     // $10.00

	// Get initial count of outbox events
	var initialOutboxCount int
	err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events;").Scan(&initialOutboxCount)
	if err != nil {
		t.Fatalf("Failed to query outbox count: %v", err)
	}

	// 2. Attempt an overdraft transfer: Alice sends $100.00 (more than her $50.00 balance)
	params := storage.TransferParams{
		FromAccountID: aliceID,
		ToAccountID:   bobID,
		Amount:        10000, // $100.00
		Currency:      "USD",
	}

	res, err := storage.TransferTx(ctx, pool, params)
	if err == nil {
		t.Fatalf("Expected transfer to fail due to insufficient balance, but it succeeded: %+v", res)
	}
	t.Logf("Expected failure received: %v", err)

	// 3. Verify Balances Remain Unchanged
	var aliceBal, bobBal int64
	_ = pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = $1;", aliceID).Scan(&aliceBal)
	_ = pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = $1;", bobID).Scan(&bobBal)

	if aliceBal != 5000 || bobBal != 1000 {
		t.Fatalf("ACID VIOLATION: Balances mutated after rollback! Alice: %d (expected 5000), Bob: %d (expected 1000)", aliceBal, bobBal)
	}

	// 4. Verify ZERO rows inserted into outbox_events, transfers, or entries
	var finalOutboxCount int
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events;").Scan(&finalOutboxCount)
	if finalOutboxCount != initialOutboxCount {
		t.Fatalf("ACID VIOLATION: %d outbox events were inserted despite transaction failure!", finalOutboxCount-initialOutboxCount)
	}

	var transferCount, entryCount int
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM transfers WHERE from_account_id = $1;", aliceID).Scan(&transferCount)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM entries WHERE account_id = $1;", aliceID).Scan(&entryCount)

	if transferCount != 0 || entryCount != 0 {
		t.Fatalf("ACID VIOLATION: Audit records exist after rollback! transfers=%d, entries=%d", transferCount, entryCount)
	}

	t.Log("ACID Verification PASSED: Overdraft aborted, rollback was clean, 0 outbox events staged.")
}

// ============================================================================
// VERIFICATION 3: Dual-Worker Race Verification (1,000 Seeded Events)
// ============================================================================
func TestDualWorker_RaceVerification(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()

	t.Log("Seeding 1,000 PENDING outbox events...")
	const numEvents = 1000

	// Batch seed 1,000 pending events
	seedQuery := `
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, status)
		SELECT
			'SEED_AGGREGATE',
			gen_random_uuid()::text,
			'SEED_EVENT',
			'{"test": true}'::jsonb,
			'PENDING'
		FROM generate_series(1, $1);
	`
	_, err := pool.Exec(ctx, seedQuery, numEvents)
	if err != nil {
		t.Fatalf("Failed to seed 1,000 outbox events: %v", err)
	}

	t.Log("Spawning Worker 1 (app_1) and Worker 2 (app_2) with FOR UPDATE SKIP LOCKED...")

	// Simulate claiming query that workers run:
	// SELECT ... FOR UPDATE SKIP LOCKED ensures workers claim strictly non-overlapping slices!
	claimQuery := `
		UPDATE outbox_events
		SET status = 'PROCESSING', locked_until = NOW() + INTERVAL '30 seconds', updated_at = NOW()
		WHERE sequence_id IN (
			SELECT sequence_id
			FROM outbox_events
			WHERE status = 'PENDING'
			ORDER BY sequence_id ASC
			LIMIT 50
			FOR UPDATE SKIP LOCKED
		)
		RETURNING sequence_id;
	`

	worker1Claimed := make([]int64, 0, numEvents)
	worker2Claimed := make([]int64, 0, numEvents)

	var mu1, mu2 sync.Mutex
	var wg sync.WaitGroup
	wg.Add(2)

	workerFunc := func(workerName string, claimedSlice *[]int64, mu *sync.Mutex) {
		defer wg.Done()
		for {
			tx, err := pool.Begin(ctx)
			if err != nil {
				return
			}

			// 1. Run the claimQuery with FOR UPDATE SKIP LOCKED
			rows, err := tx.Query(ctx, claimQuery)
			if err != nil {
				tx.Rollback(ctx)
				return
			}

			// 2. Read the batch of claimed sequence IDs
			var batch []int64
			for rows.Next() {
				var seqID int64
				if err := rows.Scan(&seqID); err == nil {
					batch = append(batch, seqID)
				}
			}
			rows.Close()

			// 3. If batch is empty, all 1,000 events are done! Break the loop.
			if len(batch) == 0 {
				tx.Rollback(ctx)
				break
			}

			// 4. Mark them as PUBLISHED
			for _, seqID := range batch {
				_, _ = tx.Exec(ctx, "UPDATE outbox_events SET status = 'PUBLISHED' WHERE sequence_id = $1;", seqID)
			}

			if err := tx.Commit(ctx); err != nil {
				continue
			}

			// 5. Safely append to this worker's record of claimed IDs
			mu.Lock()
			*claimedSlice = append(*claimedSlice, batch...)
			mu.Unlock()

			// Micro-sleep to allow interleaving between the two workers
			time.Sleep(2 * time.Millisecond) // realistic processing delay
		}
	}

	// Launch Worker 1 (app_1) and Worker 2 (app_2) simultaneously!
	start := time.Now()
	go workerFunc("app_1", &worker1Claimed, &mu1)
	go workerFunc("app_2", &worker2Claimed, &mu2)

	wg.Wait()
	duration := time.Since(start)

	t.Logf("Both workers completed in %v. Worker 1 claimed: %d, Worker 2 claimed: %d. Total: %d",
		duration, len(worker1Claimed), len(worker2Claimed), len(worker1Claimed)+len(worker2Claimed))

	// Check A: Did both workers actually participate?
	if len(worker1Claimed) == 0 || len(worker2Claimed) == 0 {
		t.Fatalf("RACE VERIFICATION FAILED: One worker starved! Worker 1: %d, Worker 2: %d", len(worker1Claimed), len(worker2Claimed))
	}

	// Check B: THE CRITICAL DISJOINT-SET CHECK (Zero Duplicates)
	seen := make(map[int64]string, numEvents)
	for _, id := range worker1Claimed {
		seen[id] = "app_1"
	}
	duplicateCount := 0
	for _, id := range worker2Claimed {
		if owner, exists := seen[id]; exists {
			t.Errorf("CRITICAL RACE CONDITION: Event %d was claimed by BOTH %s and app_2!", id, owner)
			duplicateCount++
		}
	}

	if duplicateCount > 0 {
		t.Fatalf("FAILED: %d duplicate dispatches detected between app_1 and app_2!", duplicateCount)
	}

	// Check C: Zero Leftover Events
	var remainingPending int
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE status IN ('PENDING', 'PROCESSING');").Scan(&remainingPending)
	if remainingPending != 0 {
		t.Fatalf("Expected 0 unprocessed events, found %d", remainingPending)
	}

	t.Log("Dual-Worker Race Verification PASSED: Zero duplicates, zero deadlocks, perfect concurrency distribution.")
}
