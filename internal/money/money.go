// Package money centralizes exact decimal arithmetic for prices/sizes/notionals.
// We use shopspring/decimal rather than float64 so that notional computation
// and batch total reconciliation are exact (critical for settlement).
package money

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// Parse parses a decimal string.
func Parse(s string) (decimal.Decimal, error) {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("parse decimal %q: %w", s, err)
	}
	return d, nil
}

// Notional returns price*size as a decimal string.
func Notional(price, size string) (string, error) {
	p, err := Parse(price)
	if err != nil {
		return "", err
	}
	s, err := Parse(size)
	if err != nil {
		return "", err
	}
	return p.Mul(s).String(), nil
}

// SumNotionals sums a list of decimal strings and returns the total string.
func SumNotionals(notionals []string) (string, error) {
	total := decimal.Zero
	for _, n := range notionals {
		d, err := Parse(n)
		if err != nil {
			return "", err
		}
		total = total.Add(d)
	}
	return total.String(), nil
}

// IsPositive reports whether s parses to a strictly positive decimal.
func IsPositive(s string) bool {
	d, err := Parse(s)
	if err != nil {
		return false
	}
	return d.IsPositive()
}

// Equal reports whether two decimal strings are numerically equal.
func Equal(a, b string) bool {
	da, err := Parse(a)
	if err != nil {
		return false
	}
	db, err := Parse(b)
	if err != nil {
		return false
	}
	return da.Equal(db)
}
