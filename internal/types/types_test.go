package types

import (
	"testing"
	"time"
)

func TestDataTypeString(t *testing.T) {
	tests := []struct {
		dt   DataType
		want string
	}{
		{TypeNull, "NULL"},
		{TypeBool, "BOOL"},
		{TypeInt64, "INT64"},
		{TypeFloat64, "FLOAT64"},
		{TypeString, "STRING"},
		{TypeTimestamp, "TIMESTAMP"},
		{DataType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if got := tt.dt.String(); got != tt.want {
			t.Errorf("DataType(%d).String() = %q, want %q", tt.dt, got, tt.want)
		}
	}
}

func TestDataTypeSize(t *testing.T) {
	tests := []struct {
		dt   DataType
		want int
	}{
		{TypeBool, 1},
		{TypeInt64, 8},
		{TypeFloat64, 8},
		{TypeTimestamp, 8},
		{TypeString, -1},
		{TypeNull, 0},
	}

	for _, tt := range tests {
		if got := tt.dt.Size(); got != tt.want {
			t.Errorf("DataType(%v).Size() = %d, want %d", tt.dt, got, tt.want)
		}
	}
}

func TestDataTypeIsNumeric(t *testing.T) {
	tests := []struct {
		dt   DataType
		want bool
	}{
		{TypeInt64, true},
		{TypeFloat64, true},
		{TypeBool, false},
		{TypeString, false},
		{TypeTimestamp, false},
		{TypeNull, false},
	}

	for _, tt := range tests {
		if got := tt.dt.IsNumeric(); got != tt.want {
			t.Errorf("DataType(%v).IsNumeric() = %v, want %v", tt.dt, got, tt.want)
		}
	}
}

func TestNullValue(t *testing.T) {
	v := NewNullValue()

	if !v.IsNull {
		t.Error("expected null value")
	}

	if v.String() != "NULL" {
		t.Errorf("expected 'NULL', got %q", v.String())
	}

	// Null should not be convertible to any type
	if _, ok := v.AsBool(); ok {
		t.Error("null should not be convertible to bool")
	}
	if _, ok := v.AsInt64(); ok {
		t.Error("null should not be convertible to int64")
	}
	if _, ok := v.AsFloat64(); ok {
		t.Error("null should not be convertible to float64")
	}
	if _, ok := v.AsString(); ok {
		t.Error("null should not be convertible to string")
	}
	if _, ok := v.AsTimestamp(); ok {
		t.Error("null should not be convertible to timestamp")
	}
}

func TestBoolValue(t *testing.T) {
	vTrue := NewBoolValue(true)
	vFalse := NewBoolValue(false)

	if vTrue.Type != TypeBool {
		t.Errorf("expected TypeBool, got %v", vTrue.Type)
	}

	if val, ok := vTrue.AsBool(); !ok || val != true {
		t.Errorf("expected true, got %v, ok=%v", val, ok)
	}

	if val, ok := vFalse.AsBool(); !ok || val != false {
		t.Errorf("expected false, got %v, ok=%v", val, ok)
	}

	if vTrue.String() != "true" {
		t.Errorf("expected 'true', got %q", vTrue.String())
	}

	if vFalse.String() != "false" {
		t.Errorf("expected 'false', got %q", vFalse.String())
	}
}

func TestInt64Value(t *testing.T) {
	v := NewInt64Value(42)

	if v.Type != TypeInt64 {
		t.Errorf("expected TypeInt64, got %v", v.Type)
	}

	if v.IsNull {
		t.Error("expected non-null value")
	}

	val, ok := v.AsInt64()
	if !ok {
		t.Error("AsInt64 should return true")
	}
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}

	if v.String() != "42" {
		t.Errorf("expected '42', got %q", v.String())
	}

	// Should not be convertible to other types
	if _, ok := v.AsString(); ok {
		t.Error("int64 should not be directly convertible to string")
	}
}

func TestFloat64Value(t *testing.T) {
	v := NewFloat64Value(3.14159)

	if v.Type != TypeFloat64 {
		t.Errorf("expected TypeFloat64, got %v", v.Type)
	}

	val, ok := v.AsFloat64()
	if !ok {
		t.Error("AsFloat64 should return true")
	}
	if val != 3.14159 {
		t.Errorf("expected 3.14159, got %f", val)
	}
}

func TestStringValue(t *testing.T) {
	v := NewStringValue("hello")

	if v.Type != TypeString {
		t.Errorf("expected TypeString, got %v", v.Type)
	}

	str, ok := v.AsString()
	if !ok {
		t.Error("AsString should return true")
	}
	if str != "hello" {
		t.Errorf("expected 'hello', got %q", str)
	}

	if v.String() != "hello" {
		t.Errorf("expected 'hello', got %q", v.String())
	}
}

func TestTimestampValue(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	v := NewTimestampValue(ts)

	if v.Type != TypeTimestamp {
		t.Errorf("expected TypeTimestamp, got %v", v.Type)
	}

	val, ok := v.AsTimestamp()
	if !ok {
		t.Error("AsTimestamp should return true")
	}
	if !val.Equal(ts) {
		t.Errorf("expected %v, got %v", ts, val)
	}
}

func TestValueCompare(t *testing.T) {
	tests := []struct {
		name string
		a    Value
		b    Value
		want int
	}{
		// NULL comparisons
		{"null vs null", NewNullValue(), NewNullValue(), 0},
		{"null vs int", NewNullValue(), NewInt64Value(1), -1},
		{"int vs null", NewInt64Value(1), NewNullValue(), 1},

		// Int64 comparisons
		{"int 1 vs 2", NewInt64Value(1), NewInt64Value(2), -1},
		{"int 2 vs 1", NewInt64Value(2), NewInt64Value(1), 1},
		{"int 1 vs 1", NewInt64Value(1), NewInt64Value(1), 0},

		// Float64 comparisons
		{"float 1.0 vs 2.0", NewFloat64Value(1.0), NewFloat64Value(2.0), -1},
		{"float 2.0 vs 1.0", NewFloat64Value(2.0), NewFloat64Value(1.0), 1},
		{"float 1.0 vs 1.0", NewFloat64Value(1.0), NewFloat64Value(1.0), 0},

		// String comparisons
		{"string a vs b", NewStringValue("a"), NewStringValue("b"), -1},
		{"string b vs a", NewStringValue("b"), NewStringValue("a"), 1},
		{"string a vs a", NewStringValue("a"), NewStringValue("a"), 0},

		// Bool comparisons
		{"bool false vs true", NewBoolValue(false), NewBoolValue(true), -1},
		{"bool true vs false", NewBoolValue(true), NewBoolValue(false), 1},
		{"bool true vs true", NewBoolValue(true), NewBoolValue(true), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.want {
				t.Errorf("Compare() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestValueToNumeric(t *testing.T) {
	tests := []struct {
		name    string
		v       Value
		want    float64
		wantOk  bool
	}{
		{"int64", NewInt64Value(42), 42.0, true},
		{"float64", NewFloat64Value(3.14), 3.14, true},
		{"string", NewStringValue("42"), 0, false},
		{"bool", NewBoolValue(true), 0, false},
		{"null", NewNullValue(), 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.v.ToNumeric()
			if ok != tt.wantOk {
				t.Errorf("ToNumeric() ok = %v, want %v", ok, tt.wantOk)
			}
			if ok && got != tt.want {
				t.Errorf("ToNumeric() = %v, want %v", got, tt.want)
			}
		})
	}
}
