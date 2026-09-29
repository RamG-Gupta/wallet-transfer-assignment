package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/RamG-Gupta/wallet-transfer-assignment/internal/domain"

	_ "modernc.org/sqlite"
)

const timeLayout = time.RFC3339Nano

type Store struct {
	db *sql.DB
}

func Open(dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", withSQLitePragmas(dsn))
	if err != nil {
		return nil, err
	}
	// WAL + busy timeout lets concurrent tests contend instead of failing immediately.
	// Multiple conns are OK; writers still serialise on BEGIN IMMEDIATE.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &Store{db: db}, nil
}

func withSQLitePragmas(dsn string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "_pragma=busy_timeout(10000)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)"
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *Store) CreateWallet(ctx context.Context, id string, balance int64, at time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO wallets (id, balance, created_at) VALUES (?, ?, ?)`,
		id, balance, at.UTC().Format(timeLayout),
	)
	if err != nil {
		if isUnique(err) {
			return domain.ErrWalletExists
		}
		return err
	}
	return nil
}

func (s *Store) GetWallet(ctx context.Context, id string) (domain.Wallet, error) {
	return scanWallet(s.db.QueryRowContext(ctx,
		`SELECT id, balance, created_at FROM wallets WHERE id = ?`, id,
	))
}

// RunTransfer claims the idempotency key inside BEGIN IMMEDIATE, then runs fn.
// Replay is true when the key already completed with the same request hash.
func (s *Store) RunTransfer(
	ctx context.Context,
	claimKey, requestHash string,
	now time.Time,
	fn func(tx *Tx) (domain.Transfer, error),
) (domain.Transfer, bool, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return domain.Transfer{}, false, err
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return domain.Transfer{}, false, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()

	tx := &Tx{ctx: ctx, conn: conn}
	nowStr := now.UTC().Format(timeLayout)

	_, err = conn.ExecContext(ctx, `
		INSERT INTO idempotency_records (key, request_hash, transfer_id, status, created_at, updated_at)
		VALUES (?, ?, NULL, 'PROCESSING', ?, ?)`,
		claimKey, requestHash, nowStr, nowStr,
	)
	if err != nil {
		if !isUnique(err) {
			return domain.Transfer{}, false, err
		}
		existing, loadErr := tx.loadIdempotency(claimKey)
		if loadErr != nil {
			return domain.Transfer{}, false, loadErr
		}
		if existing.RequestHash != requestHash {
			return domain.Transfer{}, false, fmt.Errorf("%w: idempotencyKey reused with a different request", domain.ErrConflict)
		}
		if existing.ErrorCode != "" {
			if _, err := conn.ExecContext(ctx, `ROLLBACK`); err != nil {
				return domain.Transfer{}, false, err
			}
			committed = true
			return domain.Transfer{}, true, replayTerminal(existing.ErrorCode, existing.ErrorDetail)
		}
		if existing.TransferID == "" {
			return domain.Transfer{}, false, fmt.Errorf("incomplete idempotency record for key %s", claimKey)
		}
		tr, getErr := tx.GetTransfer(existing.TransferID)
		if getErr != nil {
			return domain.Transfer{}, false, getErr
		}
		tr.IdempotencyKey = claimKey
		if _, err := conn.ExecContext(ctx, `ROLLBACK`); err != nil {
			return domain.Transfer{}, false, err
		}
		committed = true
		return tr, true, nil
	}

	tr, err := fn(tx)
	if err != nil {
		fnErr := err
		code, detail, ok := terminalError(fnErr)
		if !ok {
			return domain.Transfer{}, false, fnErr
		}
		if _, err := conn.ExecContext(ctx, `
			UPDATE idempotency_records
			SET status = 'COMPLETED', error_code = ?, error_detail = ?, updated_at = ?
			WHERE key = ?`,
			code, detail, now.UTC().Format(timeLayout), claimKey,
		); err != nil {
			return domain.Transfer{}, false, err
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return domain.Transfer{}, false, err
		}
		committed = true
		return domain.Transfer{}, false, fnErr
	}
	tr.IdempotencyKey = claimKey

	if _, err := conn.ExecContext(ctx, `
		UPDATE idempotency_records
		SET status = 'COMPLETED', transfer_id = ?, updated_at = ?
		WHERE key = ?`,
		tr.ID, now.UTC().Format(timeLayout), claimKey,
	); err != nil {
		return domain.Transfer{}, false, err
	}

	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return domain.Transfer{}, false, err
	}
	committed = true
	return tr, false, nil
}

type idempotencyRow struct {
	RequestHash string
	TransferID  string
	ErrorCode   string
	ErrorDetail string
}

type Tx struct {
	ctx  context.Context
	conn *sql.Conn
}

func (t *Tx) loadIdempotency(key string) (idempotencyRow, error) {
	var row idempotencyRow
	var transferID, errorCode, errorDetail sql.NullString
	err := t.conn.QueryRowContext(t.ctx, `
		SELECT request_hash, transfer_id, error_code, error_detail
		FROM idempotency_records WHERE key = ?`, key,
	).Scan(&row.RequestHash, &transferID, &errorCode, &errorDetail)
	if errors.Is(err, sql.ErrNoRows) {
		return idempotencyRow{}, fmt.Errorf("idempotency key %s not found after conflict", key)
	}
	if err != nil {
		return idempotencyRow{}, err
	}
	if transferID.Valid {
		row.TransferID = transferID.String
	}
	if errorCode.Valid {
		row.ErrorCode = errorCode.String
	}
	if errorDetail.Valid {
		row.ErrorDetail = errorDetail.String
	}
	return row, nil
}

func (t *Tx) GetWallet(id string) (domain.Wallet, error) {
	return scanWallet(t.conn.QueryRowContext(t.ctx,
		`SELECT id, balance, created_at FROM wallets WHERE id = ?`, id,
	))
}

func (t *Tx) InsertTransfer(tr domain.Transfer) error {
	_, err := t.conn.ExecContext(t.ctx, `
		INSERT INTO transfers (id, from_wallet_id, to_wallet_id, amount, status, failure_reason, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		tr.ID, tr.FromWalletID, tr.ToWalletID, tr.Amount, string(tr.Status), nullIfEmpty(tr.FailureReason),
		tr.CreatedAt.UTC().Format(timeLayout), tr.UpdatedAt.UTC().Format(timeLayout),
	)
	return err
}

func (t *Tx) UpdateTransferStatus(id string, status domain.TransferStatus, failureReason string, at time.Time) error {
	_, err := t.conn.ExecContext(t.ctx, `
		UPDATE transfers SET status = ?, failure_reason = ?, updated_at = ? WHERE id = ?`,
		string(status), nullIfEmpty(failureReason), at.UTC().Format(timeLayout), id,
	)
	return err
}

func (t *Tx) InsertLedger(transferID, walletID string, entryType domain.EntryType, amount int64, at time.Time) error {
	_, err := t.conn.ExecContext(t.ctx, `
		INSERT INTO ledger_entries (transfer_id, wallet_id, entry_type, amount, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		transferID, walletID, string(entryType), amount, at.UTC().Format(timeLayout),
	)
	return err
}

func (t *Tx) UpdateBalance(walletID string, newBalance int64) error {
	res, err := t.conn.ExecContext(t.ctx,
		`UPDATE wallets SET balance = ? WHERE id = ?`, newBalance, walletID,
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: wallet %s", domain.ErrNotFound, walletID)
	}
	return nil
}

func (t *Tx) GetTransfer(id string) (domain.Transfer, error) {
	var tr domain.Transfer
	var failure sql.NullString
	var created, updated string
	err := t.conn.QueryRowContext(t.ctx, `
		SELECT id, from_wallet_id, to_wallet_id, amount, status, failure_reason, created_at, updated_at
		FROM transfers WHERE id = ?`, id,
	).Scan(&tr.ID, &tr.FromWalletID, &tr.ToWalletID, &tr.Amount, &tr.Status, &failure, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Transfer{}, fmt.Errorf("%w: transfer %s", domain.ErrNotFound, id)
	}
	if err != nil {
		return domain.Transfer{}, err
	}
	if failure.Valid {
		tr.FailureReason = failure.String
	}
	tr.CreatedAt, err = time.Parse(timeLayout, created)
	if err != nil {
		return domain.Transfer{}, err
	}
	tr.UpdatedAt, err = time.Parse(timeLayout, updated)
	if err != nil {
		return domain.Transfer{}, err
	}
	tr.Ledger, err = t.listLedger(id)
	return tr, err
}

func (t *Tx) listLedger(transferID string) ([]domain.LedgerEntry, error) {
	rows, err := t.conn.QueryContext(t.ctx, `
		SELECT wallet_id, entry_type, amount
		FROM ledger_entries WHERE transfer_id = ? ORDER BY id`, transferID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []domain.LedgerEntry
	for rows.Next() {
		var e domain.LedgerEntry
		if err := rows.Scan(&e.WalletID, &e.Type, &e.Amount); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	if entries == nil {
		entries = []domain.LedgerEntry{}
	}
	return entries, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanWallet(row rowScanner) (domain.Wallet, error) {
	var w domain.Wallet
	var created string
	err := row.Scan(&w.ID, &w.Balance, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Wallet{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Wallet{}, err
	}
	w.CreatedAt, err = time.Parse(timeLayout, created)
	return w, err
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func isUnique(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func terminalError(err error) (code, detail string, ok bool) {
	if errors.Is(err, domain.ErrNotFound) {
		return "NOT_FOUND", err.Error(), true
	}
	return "", "", false
}

func replayTerminal(code, detail string) error {
	switch code {
	case "NOT_FOUND":
		return fmt.Errorf("%s: %w", detail, domain.ErrNotFound)
	default:
		return fmt.Errorf("stored idempotent error %s: %s", code, detail)
	}
}
