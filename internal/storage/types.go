// Package storage implements the storage layer of the database.
package storage

import "fmt"

// DataType represents the type of a column value.
type DataType uint8

const (
	TypeNull DataType = iota
	TypeBool
	TypeInt64
	TypeFloat64
	TypeString
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
	default:
		return "UNKNOWN"
	}
}

// ParseDataType parses a string into a DataType.
func ParseDataType(s string) DataType {
	switch s {
	case "INT64", "INT", "INTEGER", "BIGINT":
		return TypeInt64
	case "FLOAT64", "FLOAT", "DOUBLE", "REAL":
		return TypeFloat64
	case "STRING", "VARCHAR", "TEXT":
		return TypeString
	case "BOOL", "BOOLEAN":
		return TypeBool
	default:
		return TypeNull
	}
}

// Value represents a column value of any type.
type Value struct {
	Type   DataType
	IsNull bool
	data   any
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

// AsBool returns the value as a bool.
func (v Value) AsBool() (bool, bool) {
	if v.Type != TypeBool || v.IsNull {
		return false, false
	}
	return v.data.(bool), true
}

// AsInt64 returns the value as an int64.
func (v Value) AsInt64() (int64, bool) {
	if v.Type != TypeInt64 || v.IsNull {
		return 0, false
	}
	return v.data.(int64), true
}

// AsFloat64 returns the value as a float64.
func (v Value) AsFloat64() (float64, bool) {
	if v.Type != TypeFloat64 || v.IsNull {
		return 0, false
	}
	return v.data.(float64), true
}

// AsString returns the value as a string.
func (v Value) AsString() (string, bool) {
	if v.Type != TypeString || v.IsNull {
		return "", false
	}
	return v.data.(string), true
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
	default:
		return "UNKNOWN"
	}
}
