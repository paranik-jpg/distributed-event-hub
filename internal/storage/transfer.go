package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInsufficientBalance = errors.New("insufficient balance")
	ErrAccountNotFound     = errors.New("account not found")
	ErrCurrencyMismatch    = errors.New("currency mismatch between accounts")
	ErrSelfTransfer        = errors.New("cannot transfer to the same account")
	ErrInvalidAmount       = errors.New("transfer amount must be strictly positive")
)

// TransferParams encapsulates the input arguments required for an atomic money transfer.
type TransferParams struct {
	FromAccountID uuid.UUID `json:"from_account_id"`
	ToAccountID   uuid.UUID `json:"to_account_id"`
	Amount        int64     `json:"amount"` // represented in the smallest currency unit (e.g. cents)
	Currency      string    `json:"currency"`
}

// TransferResult encapsulates the committed transaction entities.
type TransferResult struct {
	Transfer    Transfer    `json:"transfer"`
	FromAccount Account     `json:"from_account"`
	ToAccount   Account     `json:"to_account"`
	FromEntry   Entry       `json:"from_entry"`
	ToEntry     Entry       `json:"to_entry"`
	OutboxEvent OutboxEvent `json:"outbox_event"`
}

type Account struct {
	ID        uuid.UUID `json:"id"`
	Owner     string    `json:"owner"`
	Currency  string    `json:"currency"`
	Balance   int64     `json:"balance"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Transfer struct {
	ID            uuid.UUID `json:"id"`
	FromAccountID uuid.UUID `json:"from_account_id"`
	ToAccountID   uuid.UUID `json:"to_account_id"`
	Amount        int64     `json:"amount"`
	CreatedAt     time.Time `json:"created_at"`
}

type Entry struct {
	ID         uuid.UUID `json:"id"`
	AccountID  uuid.UUID `json:"account_id"`
	TransferID uuid.UUID `json:"transfer_id"`
	Amount     int64     `json:"amount"`
	CreatedAt  time.Time `json:"created_at"`
}

// TransferTx executes an atomic double-entry balance transfer between two accounts.
// It enforces deterministic lock ordering to guarantee immunity from database deadlocks.
func TransferTx(ctx context.Context, pool *pgxpool.Pool, params TransferParams) (*TransferResult, error) {
	// 1. Invariant Validation: Basic Sanity Checks
	if params.FromAccountID == params.ToAccountID {
		return nil, ErrSelfTransfer
	}
	if params.Amount <= 0 {
		return nil, ErrInvalidAmount
	}

	// 2. Begin ACID Transaction with ReadCommitted Isolation
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{
		// Ensure transactions only see real, committed data.
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // Will trigger if the code returns early via error, Safe no-op if committed

	// 3. Deterministic Lock Ordering
	// To prevent deadlocks between concurrent bidirectional transfers (e.g., A->B and B->A),
	// we always acquire row locks in lexicographically sorted order of account IDs.
	firstID, secondID := params.FromAccountID, params.ToAccountID
	if firstID.String() > secondID.String() {
		firstID, secondID = secondID, firstID
	}

	// 4. Acquire Row Locks using SELECT ... FOR NO KEY UPDATE
	// FOR NO KEY UPDATE locks the row for balance modifications without blocking
	// foreign key checks from other concurrent transactions.
	accountsMap := make(map[uuid.UUID]*Account, 2)
	for _, id := range []uuid.UUID{firstID, secondID} {
		var acc Account
		query := `
			SELECT id, owner, currency, balance, created_at, updated_at
			FROM accounts
			WHERE id = $1
			FOR NO KEY UPDATE;
		`
		err := tx.QueryRow(ctx, query, id).Scan(
			&acc.ID,
			&acc.Owner,
			&acc.Currency,
			&acc.Balance,
			&acc.CreatedAt,
			&acc.UpdatedAt,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf("%w: %s", ErrAccountNotFound, id)
			}
			return nil, fmt.Errorf("failed to lock account %s: %w", id, err)
		}
		accountsMap[id] = &acc
	}

	fromAccount := accountsMap[params.FromAccountID]
	toAccount := accountsMap[params.ToAccountID]

	// 5. Invariant Validation: Currency Parity & Solvency
	if fromAccount.Currency != params.Currency || toAccount.Currency != params.Currency {
		return nil, ErrCurrencyMismatch
	}
	if fromAccount.Balance < params.Amount {
		return nil, ErrInsufficientBalance
	}

	// 6. Execute Balance Updates
	updateQuery := `
		UPDATE accounts
		SET balance = balance + $1, updated_at = NOW()
		WHERE id = $2
		RETURNING id, owner, currency, balance, created_at, updated_at;
	`
	var updatedFrom, updatedTo Account

	// Debit Source Account (-params.Amount)
	err = tx.QueryRow(ctx, updateQuery, -params.Amount, fromAccount.ID).Scan(
		&updatedFrom.ID,
		&updatedFrom.Owner,
		&updatedFrom.Currency,
		&updatedFrom.Balance,
		&updatedFrom.CreatedAt,
		&updatedFrom.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to debit from_account %s: %w", fromAccount.ID, err)
	}

	// Credit Destination Account (+params.Amount)
	err = tx.QueryRow(ctx, updateQuery, params.Amount, toAccount.ID).Scan(
		&updatedTo.ID,
		&updatedTo.Owner,
		&updatedTo.Currency,
		&updatedTo.Balance,
		&updatedTo.CreatedAt,
		&updatedTo.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to credit to_account %s: %w", toAccount.ID, err)
	}

	// 7. Insert the Transfer Record
	var transfer Transfer
	insertTransferQuery := `
		INSERT INTO transfers (from_account_id, to_account_id, amount)
		VALUES ($1, $2, $3)
		RETURNING id, from_account_id, to_account_id, amount, created_at;
	`
	err = tx.QueryRow(ctx, insertTransferQuery, fromAccount.ID, toAccount.ID, params.Amount).Scan(
		&transfer.ID,
		&transfer.FromAccountID,
		&transfer.ToAccountID,
		&transfer.Amount,
		&transfer.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert transfer record: %w", err)
	}

	// 8. Insert Symmetric Double-Entry Audit Rows (-amount and +amount)
	var fromEntry, toEntry Entry
	insertEntryQuery := `
		INSERT INTO entries (account_id, transfer_id, amount)
		VALUES ($1, $2, $3)
		RETURNING id, account_id, transfer_id, amount, created_at;
	`
	// Debit Entry
	err = tx.QueryRow(ctx, insertEntryQuery, fromAccount.ID, transfer.ID, -params.Amount).Scan(
		&fromEntry.ID,
		&fromEntry.AccountID,
		&fromEntry.TransferID,
		&fromEntry.Amount,
		&fromEntry.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert debit entry: %w", err)
	}

	// Credit Entry
	err = tx.QueryRow(ctx, insertEntryQuery, toAccount.ID, transfer.ID, params.Amount).Scan(
		&toEntry.ID,
		&toEntry.AccountID,
		&toEntry.TransferID,
		&toEntry.Amount,
		&toEntry.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert credit entry: %w", err)
	}

	// 9. Stage Outbox Event inside the exact same database transaction.
	// This guarantees zero dual-write anomalies: balance updates and event staging commit or abort together.
	outboxPayload := map[string]any{
		"transfer_id":     transfer.ID,
		"from_account_id": transfer.FromAccountID,
		"to_account_id":   transfer.ToAccountID,
		"amount":          transfer.Amount,
		"currency":        fromAccount.Currency,
		"timestamp":       transfer.CreatedAt,
	}

	stagedEvent, err := StageOutboxEvent(
		ctx,
		tx,
		"TRANSFER",
		transfer.ID.String(),
		"TRANSFER_COMPLETED",
		outboxPayload,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to stage transfer outbox event: %w", err)
	}

	// 10. Commit Transaction
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transfer transaction: %w", err)
	}

	return &TransferResult{
		Transfer:    transfer,
		FromAccount: updatedFrom,
		ToAccount:   updatedTo,
		FromEntry:   fromEntry,
		ToEntry:     toEntry,
		OutboxEvent: *stagedEvent,
	}, nil
}
