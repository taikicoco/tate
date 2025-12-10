package encoding

import (
	"bytes"
	"testing"
)

func TestRLEEncode(t *testing.T) {
	encoder := NewRLEEncoder()

	data := []int64{1, 1, 1, 2, 2, 3, 3, 3, 3, 3}
	runs := encoder.Encode(data)

	expected := []Run{
		{Value: 1, Count: 3},
		{Value: 2, Count: 2},
		{Value: 3, Count: 5},
	}

	if len(runs) != len(expected) {
		t.Fatalf("expected %d runs, got %d", len(expected), len(runs))
	}

	for i, run := range runs {
		if run.Value != expected[i].Value || run.Count != expected[i].Count {
			t.Errorf("run %d: expected {%d, %d}, got {%d, %d}",
				i, expected[i].Value, expected[i].Count, run.Value, run.Count)
		}
	}
}

func TestRLEDecode(t *testing.T) {
	encoder := NewRLEEncoder()

	runs := []Run{
		{Value: 1, Count: 3},
		{Value: 2, Count: 2},
		{Value: 3, Count: 5},
	}

	decoded := encoder.Decode(runs)
	expected := []int64{1, 1, 1, 2, 2, 3, 3, 3, 3, 3}

	if len(decoded) != len(expected) {
		t.Fatalf("expected %d values, got %d", len(expected), len(decoded))
	}

	for i := range decoded {
		if decoded[i] != expected[i] {
			t.Errorf("index %d: expected %d, got %d", i, expected[i], decoded[i])
		}
	}
}

func TestRLERoundTrip(t *testing.T) {
	encoder := NewRLEEncoder()

	original := []int64{5, 5, 5, 5, 10, 10, 15}
	runs := encoder.Encode(original)
	decoded := encoder.Decode(runs)

	if len(decoded) != len(original) {
		t.Fatalf("length mismatch: expected %d, got %d", len(original), len(decoded))
	}

	for i := range original {
		if decoded[i] != original[i] {
			t.Errorf("index %d: expected %d, got %d", i, original[i], decoded[i])
		}
	}
}

func TestRLESerialization(t *testing.T) {
	encoder := NewRLEEncoder()

	original := []int64{1, 1, 2, 2, 2, 3}
	runs := encoder.Encode(original)

	// Write to buffer
	var buf bytes.Buffer
	if err := encoder.WriteTo(&buf, runs); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	// Read back
	encoder2 := NewRLEEncoder()
	runs2, err := encoder2.ReadData(&buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// Verify
	decoded := encoder2.Decode(runs2)
	for i := range original {
		if decoded[i] != original[i] {
			t.Errorf("index %d: expected %d, got %d", i, original[i], decoded[i])
		}
	}
}

func TestRLECompressionRatio(t *testing.T) {
	encoder := NewRLEEncoder()

	// Highly compressible data
	data := make([]int64, 1000)
	for i := range data {
		data[i] = 42 // All same value
	}

	encoder.Encode(data)
	ratio := encoder.CompressionRatio(len(data))

	if ratio <= 1 {
		t.Errorf("expected compression ratio > 1 for repetitive data, got %f", ratio)
	}
}

func TestDictionaryEncode(t *testing.T) {
	encoder := NewDictionaryEncoder()

	data := []string{"apple", "banana", "apple", "cherry", "banana", "apple"}
	indices := encoder.Encode(data)

	// Should have 3 unique values
	if encoder.Size() != 3 {
		t.Errorf("expected 3 unique values, got %d", encoder.Size())
	}

	// First occurrence of "apple" should be index 0
	if indices[0] != 0 {
		t.Errorf("expected index 0 for first 'apple', got %d", indices[0])
	}

	// All "apple" should have same index
	if indices[0] != indices[2] || indices[0] != indices[5] {
		t.Error("all 'apple' should have same index")
	}
}

func TestDictionaryDecode(t *testing.T) {
	encoder := NewDictionaryEncoder()

	original := []string{"apple", "banana", "apple"}
	indices := encoder.Encode(original)
	decoded := encoder.Decode(indices)

	for i := range original {
		if decoded[i] != original[i] {
			t.Errorf("index %d: expected %q, got %q", i, original[i], decoded[i])
		}
	}
}

func TestDictionaryRoundTrip(t *testing.T) {
	encoder := NewDictionaryEncoder()

	original := []string{"tokyo", "osaka", "tokyo", "nagoya", "osaka"}
	indices := encoder.Encode(original)

	// Write to buffer
	var buf bytes.Buffer
	if err := encoder.WriteTo(&buf, indices); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	// Read back
	encoder2 := NewDictionaryEncoder()
	indices2, err := encoder2.ReadData(&buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// Verify
	decoded := encoder2.Decode(indices2)
	for i := range original {
		if decoded[i] != original[i] {
			t.Errorf("index %d: expected %q, got %q", i, original[i], decoded[i])
		}
	}
}

func TestDictionaryContains(t *testing.T) {
	encoder := NewDictionaryEncoder()

	encoder.Encode([]string{"apple", "banana"})

	if !encoder.Contains("apple") {
		t.Error("should contain 'apple'")
	}

	if encoder.Contains("cherry") {
		t.Error("should not contain 'cherry'")
	}
}

func TestDictionaryGetIndex(t *testing.T) {
	encoder := NewDictionaryEncoder()

	encoder.Encode([]string{"apple", "banana", "cherry"})

	if idx := encoder.GetIndex("banana"); idx != 1 {
		t.Errorf("expected index 1 for 'banana', got %d", idx)
	}

	if idx := encoder.GetIndex("unknown"); idx != -1 {
		t.Errorf("expected -1 for unknown value, got %d", idx)
	}
}

func TestDeltaEncode(t *testing.T) {
	encoder := NewDeltaEncoder()

	data := []int64{100, 105, 110, 115, 120}
	base, deltas := encoder.Encode(data)

	if base != 100 {
		t.Errorf("expected base 100, got %d", base)
	}

	expectedDeltas := []int64{0, 5, 10, 15, 20}
	for i, d := range deltas {
		if d != expectedDeltas[i] {
			t.Errorf("delta %d: expected %d, got %d", i, expectedDeltas[i], d)
		}
	}
}

func TestDeltaDecode(t *testing.T) {
	encoder := NewDeltaEncoder()

	base := int64(100)
	deltas := []int64{0, 5, 10, 15, 20}

	decoded := encoder.Decode(base, deltas)
	expected := []int64{100, 105, 110, 115, 120}

	for i := range decoded {
		if decoded[i] != expected[i] {
			t.Errorf("index %d: expected %d, got %d", i, expected[i], decoded[i])
		}
	}
}

func TestDeltaSequentialEncode(t *testing.T) {
	encoder := NewDeltaEncoder()

	data := []int64{100, 102, 104, 106, 108} // Increment by 2
	first, deltas := encoder.EncodeSequential(data)

	if first != 100 {
		t.Errorf("expected first 100, got %d", first)
	}

	for _, d := range deltas {
		if d != 2 {
			t.Errorf("expected delta 2, got %d", d)
		}
	}
}

func TestDeltaSequentialDecode(t *testing.T) {
	encoder := NewDeltaEncoder()

	first := int64(100)
	deltas := []int64{2, 2, 2, 2}

	decoded := encoder.DecodeSequential(first, deltas)
	expected := []int64{100, 102, 104, 106, 108}

	for i := range decoded {
		if decoded[i] != expected[i] {
			t.Errorf("index %d: expected %d, got %d", i, expected[i], decoded[i])
		}
	}
}

func TestDeltaOfDelta(t *testing.T) {
	encoder := NewDeltaEncoder()

	// Timestamps with consistent interval
	data := []int64{1000, 1010, 1020, 1030, 1040}

	first, secondDelta, deltas := encoder.EncodeDeltaOfDelta(data)

	if first != 1000 {
		t.Errorf("expected first 1000, got %d", first)
	}

	if secondDelta != 10 {
		t.Errorf("expected second delta 10, got %d", secondDelta)
	}

	// All delta-of-deltas should be 0 (constant interval)
	for i, d := range deltas {
		if d != 0 {
			t.Errorf("delta-of-delta %d should be 0, got %d", i, d)
		}
	}

	// Decode and verify
	decoded := encoder.DecodeDeltaOfDelta(first, secondDelta, deltas)
	for i := range data {
		if decoded[i] != data[i] {
			t.Errorf("index %d: expected %d, got %d", i, data[i], decoded[i])
		}
	}
}

func TestDeltaSerialization(t *testing.T) {
	encoder := NewDeltaEncoder()

	original := []int64{100, 110, 120, 130}
	base, deltas := encoder.Encode(original)

	// Write to buffer
	var buf bytes.Buffer
	if err := encoder.WriteTo(&buf, base, deltas); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	// Read back
	encoder2 := NewDeltaEncoder()
	base2, deltas2, err := encoder2.ReadData(&buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// Verify
	decoded := encoder2.Decode(base2, deltas2)
	for i := range original {
		if decoded[i] != original[i] {
			t.Errorf("index %d: expected %d, got %d", i, original[i], decoded[i])
		}
	}
}

func TestAnalyzeDeltas(t *testing.T) {
	deltas := []int64{-5, 10, 0, 5, -2}
	min, max, avg := AnalyzeDeltas(deltas)

	if min != -5 {
		t.Errorf("expected min -5, got %d", min)
	}
	if max != 10 {
		t.Errorf("expected max 10, got %d", max)
	}
	// avg = (-5+10+0+5-2)/5 = 8/5 = 1
	if avg != 1 {
		t.Errorf("expected avg 1, got %d", avg)
	}
}

func TestBitsRequired(t *testing.T) {
	tests := []struct {
		min, max int64
		expected int
	}{
		{0, 1, 1},
		{0, 255, 8},
		{0, 256, 9},
		{-1, 1, 2},
		{-128, 127, 9}, // 128 needs 8 bits + 1 sign bit
	}

	for _, tt := range tests {
		bits := BitsRequired(tt.min, tt.max)
		if bits != tt.expected {
			t.Errorf("BitsRequired(%d, %d) = %d, expected %d",
				tt.min, tt.max, bits, tt.expected)
		}
	}
}

func TestRLEStringEncoder(t *testing.T) {
	encoder := NewRLEStringEncoder()

	indices := []int{0, 0, 0, 1, 1, 2, 2, 2, 2}
	runs := encoder.Encode(indices)

	expected := []StringRun{
		{Index: 0, Count: 3},
		{Index: 1, Count: 2},
		{Index: 2, Count: 4},
	}

	if len(runs) != len(expected) {
		t.Fatalf("expected %d runs, got %d", len(expected), len(runs))
	}

	for i, run := range runs {
		if run.Index != expected[i].Index || run.Count != expected[i].Count {
			t.Errorf("run %d: expected {%d, %d}, got {%d, %d}",
				i, expected[i].Index, expected[i].Count, run.Index, run.Count)
		}
	}

	// Decode
	decoded := encoder.Decode(runs)
	for i := range indices {
		if decoded[i] != indices[i] {
			t.Errorf("index %d: expected %d, got %d", i, indices[i], decoded[i])
		}
	}
}

func BenchmarkRLEEncode(b *testing.B) {
	// Prepare data with runs
	data := make([]int64, 10000)
	val := int64(1)
	for i := range data {
		if i%100 == 0 {
			val++
		}
		data[i] = val
	}

	encoder := NewRLEEncoder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(data)
	}
}

func BenchmarkDictionaryEncode(b *testing.B) {
	// Prepare data with repeated strings
	cities := []string{"Tokyo", "Osaka", "Nagoya", "Kyoto", "Fukuoka"}
	data := make([]string, 10000)
	for i := range data {
		data[i] = cities[i%len(cities)]
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder := NewDictionaryEncoder()
		encoder.Encode(data)
	}
}

func BenchmarkDeltaEncode(b *testing.B) {
	// Prepare sequential data (like timestamps)
	data := make([]int64, 10000)
	for i := range data {
		data[i] = int64(1000000 + i*10)
	}

	encoder := NewDeltaEncoder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(data)
	}
}
