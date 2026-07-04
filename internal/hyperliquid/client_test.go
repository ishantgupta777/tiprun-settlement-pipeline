package hyperliquid

import (
	"testing"

	"github.com/tiprun/settlement-pipeline/internal/schema"
)

func TestTransform(t *testing.T) {
	wt := WsTrade{
		Coin: "BTC", Side: "B", Px: "65000.5", Sz: "0.01",
		Hash: "0xabc", Time: 1710000000000, Tid: 42,
	}
	got, err := Transform(wt)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if got.TradeID != "1710000000000-BTC-42" {
		t.Errorf("trade_id = %q", got.TradeID)
	}
	if got.Side != schema.SideBuy {
		t.Errorf("side = %q, want buy", got.Side)
	}
	if got.Notional != "650.005" {
		t.Errorf("notional = %q, want 650.005", got.Notional)
	}
	if got.Source != schema.SourceHyperliquid {
		t.Errorf("source = %q", got.Source)
	}
}

func TestTransformSellSide(t *testing.T) {
	got, err := Transform(WsTrade{Coin: "ETH", Side: "A", Px: "3000", Sz: "2", Time: 1, Tid: 7})
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if got.Side != schema.SideSell {
		t.Errorf("side = %q, want sell", got.Side)
	}
}

func TestTransformRejectsBadInput(t *testing.T) {
	cases := []WsTrade{
		{Coin: "", Side: "B", Px: "1", Sz: "1"},
		{Coin: "BTC", Side: "X", Px: "1", Sz: "1"},
		{Coin: "BTC", Side: "B", Px: "", Sz: "1"},
	}
	for i, wt := range cases {
		if _, err := Transform(wt); err == nil {
			t.Errorf("case %d: expected error, got nil", i)
		}
	}
}
