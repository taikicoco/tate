// Package types defines the data types used in the columnar database.
package types

import (
	"fmt"
	"time"
)

// DataType represents the type of a column value.
type DataType uint8

const (
	// TypeNull represents a NULL type.
	TypeNull DataType = iota
	// TypeBool represents a boolean type.
	TypeBool
	// TypeInt64 represents a 64-bit integer type.
	TypeInt64
	// TypeFloat64 represents a 64-bit floating point type.
	TypeFloat64
	// TypeString represents a string type.
	TypeString
	// TypeTimestamp represents a timestamp type.
	TypeTimestamp
)

// String returns the string representation of the data type.
func (t DataType) String() string {
	switch t {
	case TypeNull:
		return "NULL"
	case TypeBool:
		return "BOOL"
	case TypeInt64:
		return "INT64"
	case TypeFloat64:
		return "FLOAT64"
	case TypeString:
		return "STRING"
	case TypeTimestamp:
		return "TIMESTAMP"
	default:
		return "UNKNOWN"
	}
}

// Size returns the byte size of the data type.
// Returns -1 for variable-length types.
func (t DataType) Size() int {
	switch t {
	case TypeBool:
		return 1
	case TypeInt64, TypeFloat64, TypeTimestamp:
		return 8
	case TypeString:
		return -1 // variable length
	default:
		return 0
	}
}

// IsNumeric returns true if the type is numeric.
func (t DataType) IsNumeric() bool {
	return t == TypeInt64 || t == TypeFloat64
}

// Value represents a column value of any type.
type Value struct {
	Type   DataType
	IsNull bool
	data   interface{}
}

// NewNullValue creates a NULL value.
func NewNullValue() Value {
	return Value{Type: TypeNull, IsNull: true}
}

// NewBoolValue creates a boolean value.
func NewBoolValue(v bool) Value {
	return Value{Type: TypeBool, data: v}
}

// NewInt64Value creates an int64 value.
func NewInt64Value(v int64) Value {
	return Value{Type: TypeInt64, data: v}
}

// NewFloat64Value creates a float64 value.
func NewFloat64Value(v float64) Value {
	return Value{Type: TypeFloat64, data: v}
}

// NewStringValue creates a string value.
func NewStringValue(v string) Value {
	return Value{Type: TypeString, data: v}
}

// NewTimestampValue creates a timestamp value.
func NewTimestampValue(v time.Time) Value {
	return Value{Type: TypeTimestamp, data: v}
}

// AsBool returns the value as a bool.
// Returns false and ok=false if the value is not a bool or is null.
func (v Value) AsBool() (bool, bool) {
	if v.Type != TypeBool || v.IsNull {
		return false, false
	}
	return v.data.(bool), true
}

// AsInt64 returns the value as an int64.
// Returns 0 and ok=false if the value is not an int64 or is null.
func (v Value) AsInt64() (int64, bool) {
	if v.Type != TypeInt64 || v.IsNull {
		return 0, false
	}
	return v.data.(int64), true
}

// AsFloat64 returns the value as a float64.
// Returns 0 and ok=false if the value is not a float64 or is null.
func (v Value) AsFloat64() (float64, bool) {
	if v.Type != TypeFloat64 || v.IsNull {
		return 0, false
	}
	return v.data.(float64), true
}

// AsString returns the value as a string.
// Returns "" and ok=false if the value is not a string or is null.
func (v Value) AsString() (string, bool) {
	if v.Type != TypeString || v.IsNull {
		return "", false
	}
	return v.data.(string), true
}

// AsTimestamp returns the value as a time.Time.
// Returns zero time and ok=false if the value is not a timestamp or is null.
func (v Value) AsTimestamp() (time.Time, bool) {
	if v.Type != TypeTimestamp || v.IsNull {
		return time.Time{}, false
	}
	return v.data.(time.Time), true
}

// String returns the string representation of the value.
func (v Value) String() string {
	if v.IsNull {
		return "NULL"
	}
	switch v.Type {
	case TypeBool:
		return fmt.Sprintf("%t", v.data.(bool))
	case TypeInt64:
		return fmt.Sprintf("%d", v.data.(int64))
	case TypeFloat64:
		return fmt.Sprintf("%.6f", v.data.(float64))
	case TypeString:
		return v.data.(string)
	case TypeTimestamp:
		return v.data.(time.Time).Format(time.RFC3339)
	default:
		return "UNKNOWN"
	}
}

// Compare compares two values.
// Returns -1 if v < other, 0 if v == other, 1 if v > other.
// NULL values are considered less than non-NULL values.
func (v Value) Compare(other Value) int {
	// Handle NULL cases
	if v.IsNull && other.IsNull {
		return 0
	}
	if v.IsNull {
		return -1
	}
	if other.IsNull {
		return 1
	}

	// Type mismatch
	if v.Type != other.Type {
		return 0 // or could return an error
	}

	switch v.Type {
	case TypeBool:
		a, _ := v.AsBool()
		b, _ := other.AsBool()
		if a == b {
			return 0
		}
		if !a && b {
			return -1
		}
		return 1

	case TypeInt64:
		a, _ := v.AsInt64()
		b, _ := other.AsInt64()
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0

	case TypeFloat64:
		a, _ := v.AsFloat64()
		b, _ := other.AsFloat64()
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0

	case TypeString:
		a, _ := v.AsString()
		b, _ := other.AsString()
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0

	case TypeTimestamp:
		a, _ := v.AsTimestamp()
		b, _ := other.AsTimestamp()
		if a.Before(b) {
			return -1
		}
		if a.After(b) {
			return 1
		}
		return 0

	default:
		return 0
	}
}

// ToNumeric converts the value to a float64 for numeric operations.
// Returns 0 and ok=false if the value cannot be converted.
func (v Value) ToNumeric() (float64, bool) {
	if v.IsNull {
		return 0, false
	}
	switch v.Type {
	case TypeInt64:
		val, _ := v.AsInt64()
		return float64(val), true
	case TypeFloat64:
		val, _ := v.AsFloat64()
		return val, true
	default:
		return 0, false
	}
}
