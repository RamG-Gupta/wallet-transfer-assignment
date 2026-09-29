package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/RamG-Gupta/wallet-transfer-assignment/internal/domain"
	"github.com/RamG-Gupta/wallet-transfer-assignment/internal/store"
	"github.com/google/uuid"
)

type Clock func() time.Time

type Service struct {
	store *store.Store
	clock Clock
	log   *slog.Logger
}

func New(st *store.Store, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		store: st,
		clock: func() time.Time { return time.Now().UTC() },
		log:   log,
	}
}

func (s *Service) CreateWallet(ctx context.Context, req domain.CreateWalletRequest) (domain.Wallet, error) {
	if err := req.Validate(); err != nil {
		return domain.Wallet{}, err
	}
	now := s.clock()
	if err := s.store.CreateWallet(ctx, req.ID, req.InitialBalance, now); err != nil {
		return domain.Wallet{}, err
	}
	return domain.Wallet{ID: req.ID, Balance: req.InitialBalance, CreatedAt: now}, nil
}

func (s *Service) GetWallet(ctx context.Context, id string) (domain.Wallet, error) {
	if id == "" {
		return domain.Wallet{}, fmt.Errorf("%w: id is required", domain.ErrValidation)
	}
	w, err := s.store.GetWallet(ctx, id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return domain.Wallet{}, fmt.Errorf("%w: wallet %s", domain.ErrNotFound, id)
		}
		return domain.Wallet{}, err
	}
	return w, nil
}

func (s *Service) CreateTransfer(ctx context.Context, req domain.CreateTransferRequest) (domain.Transfer, bool, error) {
	if err := req.Validate(); err != nil {
		return domain.Transfer{}, false, err
	}
	now := s.clock()
	hash := requestHash(req)
	s.log.Info("transfer started",
		"idempotencyKey", req.IdempotencyKey,
		"fromWalletId", req.FromWalletID,
		"toWalletId", req.ToWalletID,
		"amount", req.Amount,
	)

	tr, replay, err := s.store.RunTransfer(ctx, req.IdempotencyKey, hash, now, func(tx *store.Tx) (store.Attempt, error) {
		return s.executeTransfer(tx, req, now)
	})
	if err != nil {
		s.log.Info("transfer failed",
			"idempotencyKey", req.IdempotencyKey,
			"replay", replay,
			"err", err,
		)
		return domain.Transfer{}, replay, err
	}

	s.log.Info("transfer finished",
		"idempotencyKey", req.IdempotencyKey,
		"transferId", tr.ID,
		"status", tr.Status,
		"replay", replay,
		"amount", req.Amount,
	)
	return tr, replay, nil
}

func (s *Service) executeTransfer(tx *store.Tx, req domain.CreateTransferRequest, now time.Time) (store.Attempt, error) {
	ids := []string{req.FromWalletID, req.ToWalletID}
	sort.Strings(ids)

	locked := make(map[string]domain.Wallet, 2)
	for _, id := range ids {
		w, err := tx.GetWallet(id)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return store.Attempt{Failure: domain.WalletNotFound(id)}, nil
			}
			return store.Attempt{}, err
		}
		locked[id] = w
	}

	from := locked[req.FromWalletID]
	to := locked[req.ToWalletID]
	id := uuid.NewString()

	tr := domain.Transfer{
		ID:             id,
		IdempotencyKey: req.IdempotencyKey,
		FromWalletID:   req.FromWalletID,
		ToWalletID:     req.ToWalletID,
		Amount:         req.Amount,
		Status:         domain.StatusPending,
		CreatedAt:      now,
		UpdatedAt:      now,
		Ledger:         []domain.LedgerEntry{},
	}

	if from.Balance < req.Amount {
		tr.Status = domain.StatusFailed
		tr.FailureReason = domain.FailureInsufficientFunds
		if err := tx.InsertTransfer(tr); err != nil {
			return store.Attempt{}, err
		}
		return store.Attempt{Transfer: tr}, nil
	}
	if to.Balance > math.MaxInt64-req.Amount {
		tr.Status = domain.StatusFailed
		tr.FailureReason = domain.FailureBalanceOverflow
		if err := tx.InsertTransfer(tr); err != nil {
			return store.Attempt{}, err
		}
		return store.Attempt{Transfer: tr}, nil
	}

	if err := tx.InsertTransfer(tr); err != nil {
		return store.Attempt{}, err
	}
	if err := tx.InsertLedger(id, from.ID, domain.EntryDebit, req.Amount, now); err != nil {
		return store.Attempt{}, err
	}
	if err := tx.InsertLedger(id, to.ID, domain.EntryCredit, req.Amount, now); err != nil {
		return store.Attempt{}, err
	}
	if err := tx.UpdateBalance(from.ID, from.Balance-req.Amount); err != nil {
		return store.Attempt{}, err
	}
	if err := tx.UpdateBalance(to.ID, to.Balance+req.Amount); err != nil {
		return store.Attempt{}, err
	}
	if err := tx.UpdateTransferStatus(id, domain.StatusProcessed, "", now); err != nil {
		return store.Attempt{}, err
	}

	tr.Status = domain.StatusProcessed
	tr.Ledger = []domain.LedgerEntry{
		{WalletID: from.ID, Type: domain.EntryDebit, Amount: req.Amount},
		{WalletID: to.ID, Type: domain.EntryCredit, Amount: req.Amount},
	}
	return store.Attempt{Transfer: tr}, nil
}

func requestHash(req domain.CreateTransferRequest) string {
	// %q quotes wallet IDs so newlines (or other bytes) cannot collide across fields.
	sum := sha256.Sum256([]byte(fmt.Sprintf("%q\n%q\n%d", req.FromWalletID, req.ToWalletID, req.Amount)))
	return hex.EncodeToString(sum[:])
}
