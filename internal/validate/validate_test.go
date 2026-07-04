package validate

import (
	"testing"

	"github.com/tiprun/settlement-pipeline/internal/schema"
)

func validTrade(id string) schema.Trade {
	return schema.Trade{
		TradeID: id, Coin: "BTC", Side: schema.SideBuy,
		Price: "100", Size: "2", Notional: "200",
	}
}

func TestTradeOK(t *testing.T) {
	if err := Trade(validTrade("t1")); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestTradeRejects(t *testing.T) {
	bad := []schema.Trade{
		{Coin: "BTC", Side: schema.SideBuy, Price: "1", Size: "1"},  // no id
		{TradeID: "x", Side: schema.SideBuy, Price: "1", Size: "1"}, // no coin
		{TradeID: "x", Coin: "BTC", Side: "weird", Price: "1", Size: "1"},
		{TradeID: "x", Coin: "BTC", Side: schema.SideBuy, Price: "0", Size: "1"},
		{TradeID: "x", Coin: "BTC", Side: schema.SideBuy, Price: "1", Size: "-1"},
	}
	for i, tr := range bad {
		if err := Trade(tr); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}

func TestBatchOK(t *testing.T) {
	b := schema.Batch{
		BatchID: "b1", TradeCount: 2, TotalNotional: "400",
		Trades: []schema.Trade{validTrade("t1"), validTrade("t2")},
	}
	if err := Batch(b); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestBatchRejects(t *testing.T) {
	cases := map[string]schema.Batch{
		"no id":          {TradeCount: 1, TotalNotional: "200", Trades: []schema.Trade{validTrade("t1")}},
		"count mismatch": {BatchID: "b", TradeCount: 2, TotalNotional: "200", Trades: []schema.Trade{validTrade("t1")}},
		"empty":          {BatchID: "b", TradeCount: 0},
		"dup trade":      {BatchID: "b", TradeCount: 2, TotalNotional: "400", Trades: []schema.Trade{validTrade("t1"), validTrade("t1")}},
		"notional wrong": {BatchID: "b", TradeCount: 1, TotalNotional: "999", Trades: []schema.Trade{validTrade("t1")}},
	}
	for name, b := range cases {
		if err := Batch(b); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
