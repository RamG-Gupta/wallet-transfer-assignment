package httpapi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/RamG-Gupta/wallet-transfer-assignment/internal/httpapi"
	"github.com/RamG-Gupta/wallet-transfer-assignment/internal/service"
	"github.com/RamG-Gupta/wallet-transfer-assignment/internal/store"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "test.db")
	st, err := store.Open(dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := service.New(st, nil)
	return httptest.NewServer(httpapi.New(svc).Routes())
}

func postJSON(t *testing.T, client *http.Client, url string, body any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func readJSON(t *testing.T, res *http.Response) map[string]any {
	t.Helper()
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return out
}

func createWallet(t *testing.T, srv *httptest.Server, id string, balance int64) {
	t.Helper()
	res := postJSON(t, srv.Client(), srv.URL+"/wallets", map[string]any{
		"id": id, "initialBalance": balance,
	})
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create wallet: %d %v", res.StatusCode, readJSON(t, res))
	}
	res.Body.Close()
}

func getBalance(t *testing.T, srv *httptest.Server, id string) int64 {
	t.Helper()
	res, err := srv.Client().Get(srv.URL + "/wallets/" + id)
	if err != nil {
		t.Fatal(err)
	}
	body := readJSON(t, res)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("get wallet: %d %v", res.StatusCode, body)
	}
	return int64(body["balance"].(float64))
}

func TestTransferMovesBalancesAndWritesLedger(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	createWallet(t, srv, "wallet_1", 500)
	createWallet(t, srv, "wallet_2", 50)

	res := postJSON(t, srv.Client(), srv.URL+"/transfers", map[string]any{
		"idempotencyKey": "abc123",
		"fromWalletId":   "wallet_1",
		"toWalletId":     "wallet_2",
		"amount":         100,
	})
	body := readJSON(t, res)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("status %d body %v", res.StatusCode, body)
	}
	if body["status"] != "PROCESSED" {
		t.Fatalf("status: %v", body["status"])
	}
	assertPersistedLedger(t, body, "wallet_1", "wallet_2", 100)
	if getBalance(t, srv, "wallet_1") != 400 {
		t.Fatalf("from balance")
	}
	if getBalance(t, srv, "wallet_2") != 150 {
		t.Fatalf("to balance")
	}

	replay := postJSON(t, srv.Client(), srv.URL+"/transfers", map[string]any{
		"idempotencyKey": "abc123",
		"fromWalletId":   "wallet_1",
		"toWalletId":     "wallet_2",
		"amount":         100,
	})
	replayBody := readJSON(t, replay)
	if replay.StatusCode != http.StatusOK {
		t.Fatalf("replay status %d body %v", replay.StatusCode, replayBody)
	}
	if replayBody["id"] != body["id"] {
		t.Fatalf("replay id")
	}
	assertPersistedLedger(t, replayBody, "wallet_1", "wallet_2", 100)
}

func TestIdempotentReplayReturnsOriginalTransfer(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	createWallet(t, srv, "wallet_1", 500)
	createWallet(t, srv, "wallet_2", 0)

	payload := map[string]any{
		"idempotencyKey": "same-key",
		"fromWalletId":   "wallet_1",
		"toWalletId":     "wallet_2",
		"amount":         40,
	}
	first := postJSON(t, srv.Client(), srv.URL+"/transfers", payload)
	firstBody := readJSON(t, first)
	second := postJSON(t, srv.Client(), srv.URL+"/transfers", payload)
	secondBody := readJSON(t, second)

	if second.StatusCode != http.StatusOK {
		t.Fatalf("replay status %d", second.StatusCode)
	}
	if firstBody["id"] != secondBody["id"] {
		t.Fatalf("expected same transfer id %v vs %v", firstBody["id"], secondBody["id"])
	}
	if getBalance(t, srv, "wallet_1") != 460 {
		t.Fatalf("duplicate debit")
	}
	if getBalance(t, srv, "wallet_2") != 40 {
		t.Fatalf("duplicate credit")
	}
}

func TestIdempotencyKeyWithDifferentPayloadConflicts(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	createWallet(t, srv, "wallet_1", 500)
	createWallet(t, srv, "wallet_2", 0)

	res1 := postJSON(t, srv.Client(), srv.URL+"/transfers", map[string]any{
		"idempotencyKey": "k",
		"fromWalletId":   "wallet_1",
		"toWalletId":     "wallet_2",
		"amount":         10,
	})
	res1.Body.Close()
	res2 := postJSON(t, srv.Client(), srv.URL+"/transfers", map[string]any{
		"idempotencyKey": "k",
		"fromWalletId":   "wallet_1",
		"toWalletId":     "wallet_2",
		"amount":         11,
	})
	body := readJSON(t, res2)
	if res2.StatusCode != http.StatusConflict {
		t.Fatalf("status %d body %v", res2.StatusCode, body)
	}
	if getBalance(t, srv, "wallet_1") != 490 {
		t.Fatalf("side effect on conflict")
	}
}

func TestInsufficientFundsFailsWithoutLedgerAndIsReplayable(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	createWallet(t, srv, "wallet_1", 5)
	createWallet(t, srv, "wallet_2", 0)

	payload := map[string]any{
		"idempotencyKey": "poor",
		"fromWalletId":   "wallet_1",
		"toWalletId":     "wallet_2",
		"amount":         10,
	}
	first := postJSON(t, srv.Client(), srv.URL+"/transfers", payload)
	firstBody := readJSON(t, first)
	if first.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status %d body %v", first.StatusCode, firstBody)
	}
	if firstBody["status"] != "FAILED" {
		t.Fatalf("status %v", firstBody["status"])
	}
	if firstBody["failureReason"] != "INSUFFICIENT_FUNDS" {
		t.Fatalf("reason %v", firstBody["failureReason"])
	}
	ledger, _ := firstBody["ledger"].([]any)
	if len(ledger) != 0 {
		t.Fatalf("ledger on failed transfer: %v", ledger)
	}
	if getBalance(t, srv, "wallet_1") != 5 || getBalance(t, srv, "wallet_2") != 0 {
		t.Fatal("balances changed on failure")
	}

	second := postJSON(t, srv.Client(), srv.URL+"/transfers", payload)
	secondBody := readJSON(t, second)
	if second.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("replay status %d", second.StatusCode)
	}
	if firstBody["id"] != secondBody["id"] {
		t.Fatalf("failed replay should return original id")
	}
}

func TestValidationDoesNotCreateTransfer(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	createWallet(t, srv, "wallet_1", 100)
	createWallet(t, srv, "wallet_2", 0)

	res := postJSON(t, srv.Client(), srv.URL+"/transfers", map[string]any{
		"idempotencyKey": "bad",
		"fromWalletId":   "wallet_1",
		"toWalletId":     "wallet_1",
		"amount":         1,
	})
	body := readJSON(t, res)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d body %v", res.StatusCode, body)
	}
	if getBalance(t, srv, "wallet_1") != 100 {
		t.Fatal("balance changed")
	}
}

func TestUnknownWallet(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	createWallet(t, srv, "wallet_1", 100)
	res := postJSON(t, srv.Client(), srv.URL+"/transfers", map[string]any{
		"idempotencyKey": "missing",
		"fromWalletId":   "wallet_1",
		"toWalletId":     "nope",
		"amount":         1,
	})
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d", res.StatusCode)
	}
	res.Body.Close()
}

func TestConcurrentTransfersDoNotOverdraft(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	createWallet(t, srv, "wallet_1", 10)
	createWallet(t, srv, "wallet_2", 0)

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	codes := make([]int, n)
	bodies := make([]string, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			raw, err := json.Marshal(map[string]any{
				"idempotencyKey": fmt.Sprintf("c-%d", i),
				"fromWalletId":   "wallet_1",
				"toWalletId":     "wallet_2",
				"amount":         1,
			})
			if err != nil {
				codes[i] = -1
				bodies[i] = err.Error()
				return
			}
			res, err := srv.Client().Post(srv.URL+"/transfers", "application/json", bytes.NewReader(raw))
			if err != nil {
				codes[i] = -1
				bodies[i] = err.Error()
				return
			}
			b, _ := io.ReadAll(res.Body)
			res.Body.Close()
			codes[i] = res.StatusCode
			bodies[i] = string(b)
		}()
	}
	wg.Wait()

	var processed, failed int
	for i, c := range codes {
		switch c {
		case http.StatusCreated:
			processed++
		case http.StatusUnprocessableEntity:
			failed++
		default:
			t.Fatalf("unexpected status %d body %s", c, bodies[i])
		}
	}
	if processed != 10 {
		t.Fatalf("processed %d want 10 (codes=%v)", processed, codes)
	}
	if failed != 10 {
		t.Fatalf("failed %d want 10", failed)
	}
	if getBalance(t, srv, "wallet_1") != 0 {
		t.Fatalf("source overdraft or leftover")
	}
	if getBalance(t, srv, "wallet_2") != 10 {
		t.Fatalf("destination balance")
	}
}

func TestHealth(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	res, err := srv.Client().Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d", res.StatusCode)
	}
	res.Body.Close()
}

func TestTrailingJSONIsRejected(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	raw := []byte(`{"id":"wallet_1","initialBalance":1}{"id":"wallet_2"}`)
	res, err := srv.Client().Post(srv.URL+"/wallets", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	body := readJSON(t, res)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d body %v", res.StatusCode, body)
	}
}

func TestIdempotencyFingerprintDoesNotCollideOnNewlinesInWalletIDs(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	createWallet(t, srv, "a\nb", 50)
	createWallet(t, srv, "c", 0)
	createWallet(t, srv, "a", 50)
	createWallet(t, srv, "b\nc", 0)

	first := postJSON(t, srv.Client(), srv.URL+"/transfers", map[string]any{
		"idempotencyKey": "newline-key",
		"fromWalletId":   "a\nb",
		"toWalletId":     "c",
		"amount":         1,
	})
	firstBody := readJSON(t, first)
	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first status %d body %v", first.StatusCode, firstBody)
	}

	second := postJSON(t, srv.Client(), srv.URL+"/transfers", map[string]any{
		"idempotencyKey": "newline-key",
		"fromWalletId":   "a",
		"toWalletId":     "b\nc",
		"amount":         1,
	})
	secondBody := readJSON(t, second)
	if second.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 for distinct payloads, got %d %v", second.StatusCode, secondBody)
	}
}

func TestCreditOverflowFailsWithoutMovingFunds(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	createWallet(t, srv, "wallet_1", 1)
	createWalletRaw(t, srv, fmt.Sprintf(`{"id":"wallet_2","initialBalance":%d}`, int64(math.MaxInt64)))

	res := postJSON(t, srv.Client(), srv.URL+"/transfers", map[string]any{
		"idempotencyKey": "overflow",
		"fromWalletId":   "wallet_1",
		"toWalletId":     "wallet_2",
		"amount":         1,
	})
	body := readJSON(t, res)
	if res.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status %d body %v", res.StatusCode, body)
	}
	if body["status"] != "FAILED" || body["failureReason"] != "BALANCE_OVERFLOW" {
		t.Fatalf("overflow result %v", body)
	}
	ledger, _ := body["ledger"].([]any)
	if len(ledger) != 0 {
		t.Fatalf("ledger on overflow: %v", ledger)
	}
	if getBalance(t, srv, "wallet_1") != 1 {
		t.Fatal("source changed")
	}
}

func createWalletRaw(t *testing.T, srv *httptest.Server, raw string) {
	t.Helper()
	res, err := srv.Client().Post(srv.URL+"/wallets", "application/json", bytes.NewReader([]byte(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create wallet: %d %v", res.StatusCode, readJSON(t, res))
	}
	res.Body.Close()
}

func assertPersistedLedger(t *testing.T, body map[string]any, fromID, toID string, amount int64) {
	t.Helper()
	ledger, _ := body["ledger"].([]any)
	if len(ledger) != 2 {
		t.Fatalf("ledger len %d body %v", len(ledger), body)
	}
	debit, _ := ledger[0].(map[string]any)
	credit, _ := ledger[1].(map[string]any)
	if debit["walletId"] != fromID || debit["type"] != "DEBIT" || int64(debit["amount"].(float64)) != amount {
		t.Fatalf("debit %v", debit)
	}
	if credit["walletId"] != toID || credit["type"] != "CREDIT" || int64(credit["amount"].(float64)) != amount {
		t.Fatalf("credit %v", credit)
	}
}
