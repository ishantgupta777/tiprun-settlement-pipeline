package money

import "testing"

func TestNotional(t *testing.T) {
	got, err := Notional("65000.5", "0.01")
	if err != nil {
		t.Fatalf("notional: %v", err)
	}
	if got != "650.005" {
		t.Fatalf("notional = %q, want 650.005", got)
	}
}

func TestSumNotionals(t *testing.T) {
	got, err := SumNotionals([]string{"650.005", "1.1", "0.895"})
	if err != nil {
		t.Fatalf("sum: %v", err)
	}
	if !Equal(got, "652") {
		t.Fatalf("sum = %q, want 652", got)
	}
}

func TestIsPositive(t *testing.T) {
	cases := map[string]bool{"1": true, "0": false, "-1": false, "0.0001": true, "abc": false}
	for in, want := range cases {
		if got := IsPositive(in); got != want {
			t.Errorf("IsPositive(%q) = %v, want %v", in, got, want)
		}
	}
}
