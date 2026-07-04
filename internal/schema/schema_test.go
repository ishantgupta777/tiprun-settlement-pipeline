package schema

import "testing"

func TestTradeRoundTrip(t *testing.T) {
	in := Trade{
		TradeID:      "1710000000000-BTC-42",
		Coin:         "BTC",
		Side:         SideBuy,
		Price:        "65000.5",
		Size:         "0.01",
		Notional:     "650.005",
		EventTimeMS:  1710000000000,
		Hash:         "0xabc",
		IngestTimeMS: 1710000000123,
		Source:       SourceHyperliquid,
	}
	b, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := UnmarshalTrade(b)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != in {
		t.Fatalf("round-trip mismatch:\n got  %+v\n want %+v", out, in)
	}
}

func TestBatchRoundTrip(t *testing.T) {
	in := Batch{
		BatchID:       "batch-1",
		CreatedAtMS:   1710000000000,
		FlushReason:   FlushReasonSize,
		TradeCount:    1,
		TotalNotional: "650.005",
		Producer:      "batch-publisher",
		Trades: []Trade{{
			TradeID: "1710000000000-BTC-42", Coin: "BTC", Side: SideBuy,
			Price: "65000.5", Size: "0.01", Notional: "650.005",
			EventTimeMS: 1710000000000, Source: SourceHyperliquid,
		}},
	}
	b, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := UnmarshalBatch(b)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.BatchID != in.BatchID || out.TradeCount != in.TradeCount ||
		out.FlushReason != in.FlushReason || len(out.Trades) != 1 ||
		out.Trades[0] != in.Trades[0] {
		t.Fatalf("round-trip mismatch:\n got  %+v\n want %+v", out, in)
	}
}
