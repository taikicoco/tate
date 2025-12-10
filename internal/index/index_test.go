package index

import (
	"bytes"
	"testing"

	"github.com/taikicoco/tate/internal/types"
)

func TestBitmap(t *testing.T) {
	bm := NewBitmap()

	// Test Set and Get
	bm.Set(0)
	bm.Set(10)
	bm.Set(64) // crosses word boundary
	bm.Set(100)

	if !bm.Get(0) {
		t.Error("bit 0 should be set")
	}
	if !bm.Get(10) {
		t.Error("bit 10 should be set")
	}
	if !bm.Get(64) {
		t.Error("bit 64 should be set")
	}
	if !bm.Get(100) {
		t.Error("bit 100 should be set")
	}
	if bm.Get(50) {
		t.Error("bit 50 should not be set")
	}
}

func TestBitmapClear(t *testing.T) {
	bm := NewBitmap()

	bm.Set(10)
	bm.Set(20)

	bm.Clear(10)

	if bm.Get(10) {
		t.Error("bit 10 should be cleared")
	}
	if !bm.Get(20) {
		t.Error("bit 20 should still be set")
	}
}

func TestBitmapCount(t *testing.T) {
	bm := NewBitmap()

	bm.Set(0)
	bm.Set(10)
	bm.Set(64)
	bm.Set(100)

	if bm.Count() != 4 {
		t.Errorf("expected count 4, got %d", bm.Count())
	}
}

func TestBitmapPositions(t *testing.T) {
	bm := NewBitmap()

	bm.Set(5)
	bm.Set(10)
	bm.Set(15)

	positions := bm.Positions()

	if len(positions) != 3 {
		t.Fatalf("expected 3 positions, got %d", len(positions))
	}

	expected := []int{5, 10, 15}
	for i, pos := range positions {
		if pos != expected[i] {
			t.Errorf("expected position %d, got %d", expected[i], pos)
		}
	}
}

func TestBitmapAnd(t *testing.T) {
	bm1 := NewBitmap()
	bm1.Set(1)
	bm1.Set(2)
	bm1.Set(3)

	bm2 := NewBitmap()
	bm2.Set(2)
	bm2.Set(3)
	bm2.Set(4)

	result := bm1.And(bm2)

	if !result.Get(2) || !result.Get(3) {
		t.Error("AND result should have bits 2 and 3 set")
	}
	if result.Get(1) || result.Get(4) {
		t.Error("AND result should not have bits 1 or 4 set")
	}
}

func TestBitmapOr(t *testing.T) {
	bm1 := NewBitmap()
	bm1.Set(1)
	bm1.Set(2)

	bm2 := NewBitmap()
	bm2.Set(3)
	bm2.Set(4)

	result := bm1.Or(bm2)

	if result.Count() != 4 {
		t.Errorf("expected 4 bits set, got %d", result.Count())
	}

	for _, pos := range []int{1, 2, 3, 4} {
		if !result.Get(pos) {
			t.Errorf("bit %d should be set", pos)
		}
	}
}

func TestBitmapAndNot(t *testing.T) {
	bm1 := NewBitmap()
	bm1.Set(1)
	bm1.Set(2)
	bm1.Set(3)

	bm2 := NewBitmap()
	bm2.Set(2)
	bm2.Set(3)

	result := bm1.AndNot(bm2)

	if !result.Get(1) {
		t.Error("bit 1 should be set")
	}
	if result.Get(2) || result.Get(3) {
		t.Error("bits 2 and 3 should not be set")
	}
}

func TestBitmapSerialization(t *testing.T) {
	bm := NewBitmap()
	bm.Set(0)
	bm.Set(64)
	bm.Set(128)

	// Write to buffer
	var buf bytes.Buffer
	if err := bm.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	// Read back
	bm2 := NewBitmap()
	if err := bm2.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// Verify
	if bm2.Count() != 3 {
		t.Errorf("expected 3 bits, got %d", bm2.Count())
	}
	if !bm2.Get(0) || !bm2.Get(64) || !bm2.Get(128) {
		t.Error("bits should match original")
	}
}

func TestBitmapIndex(t *testing.T) {
	bi := NewBitmapIndex("status")

	// Add values
	bi.Add("active", 0)
	bi.Add("active", 1)
	bi.Add("active", 2)
	bi.Add("inactive", 3)
	bi.Add("inactive", 4)
	bi.Add("pending", 5)

	// Lookup
	positions := bi.LookupEqual("active")
	if len(positions) != 3 {
		t.Errorf("expected 3 active rows, got %d", len(positions))
	}

	positions = bi.LookupEqual("inactive")
	if len(positions) != 2 {
		t.Errorf("expected 2 inactive rows, got %d", len(positions))
	}

	// Lookup non-existent value
	positions = bi.LookupEqual("unknown")
	if len(positions) != 0 {
		t.Errorf("expected 0 rows, got %d", len(positions))
	}
}

func TestBitmapIndexLookupIn(t *testing.T) {
	bi := NewBitmapIndex("status")

	bi.Add("active", 0)
	bi.Add("inactive", 1)
	bi.Add("pending", 2)

	positions := bi.LookupIn([]interface{}{"active", "pending"})
	if len(positions) != 2 {
		t.Errorf("expected 2 rows, got %d", len(positions))
	}
}

func TestBitmapIndexCardinality(t *testing.T) {
	bi := NewBitmapIndex("status")

	bi.Add("a", 0)
	bi.Add("b", 1)
	bi.Add("a", 2)
	bi.Add("c", 3)

	if bi.Cardinality() != 3 {
		t.Errorf("expected cardinality 3, got %d", bi.Cardinality())
	}
}

func TestZoneMap(t *testing.T) {
	zm := NewZoneMap("age")

	// Add zones
	zm.AddZone(Zone{
		RowGroupID: 0,
		MinValue:   types.NewInt64Value(20),
		MaxValue:   types.NewInt64Value(30),
		RowCount:   100,
		NullCount:  0,
	})

	zm.AddZone(Zone{
		RowGroupID: 1,
		MinValue:   types.NewInt64Value(31),
		MaxValue:   types.NewInt64Value(40),
		RowCount:   100,
		NullCount:  0,
	})

	zm.AddZone(Zone{
		RowGroupID: 2,
		MinValue:   types.NewInt64Value(41),
		MaxValue:   types.NewInt64Value(50),
		RowCount:   100,
		NullCount:  0,
	})

	// Test CanSkip for =
	// Value 25 is in zone 0 only
	if zm.CanSkip(0, "=", types.NewInt64Value(25)) {
		t.Error("zone 0 should not be skipped for age=25")
	}
	if !zm.CanSkip(1, "=", types.NewInt64Value(25)) {
		t.Error("zone 1 should be skipped for age=25")
	}
	if !zm.CanSkip(2, "=", types.NewInt64Value(25)) {
		t.Error("zone 2 should be skipped for age=25")
	}
}

func TestZoneMapRangeQueries(t *testing.T) {
	zm := NewZoneMap("value")

	zm.AddZone(Zone{
		RowGroupID: 0,
		MinValue:   types.NewInt64Value(0),
		MaxValue:   types.NewInt64Value(100),
		RowCount:   100,
	})

	zm.AddZone(Zone{
		RowGroupID: 1,
		MinValue:   types.NewInt64Value(101),
		MaxValue:   types.NewInt64Value(200),
		RowCount:   100,
	})

	// Test < operator
	// value < 50 can match zone 0
	if zm.CanSkip(0, "<", types.NewInt64Value(50)) {
		t.Error("zone 0 should not be skipped for value < 50")
	}
	// value < 50 cannot match zone 1 (min=101)
	if !zm.CanSkip(1, "<", types.NewInt64Value(50)) {
		t.Error("zone 1 should be skipped for value < 50")
	}

	// Test > operator
	// value > 150 can match zone 1
	if zm.CanSkip(1, ">", types.NewInt64Value(150)) {
		t.Error("zone 1 should not be skipped for value > 150")
	}
	// value > 150 cannot match zone 0 (max=100)
	if !zm.CanSkip(0, ">", types.NewInt64Value(150)) {
		t.Error("zone 0 should be skipped for value > 150")
	}
}

func TestZoneMapGetCandidateZones(t *testing.T) {
	zm := NewZoneMap("value")

	zm.AddZone(Zone{
		RowGroupID: 0,
		MinValue:   types.NewInt64Value(0),
		MaxValue:   types.NewInt64Value(100),
		RowCount:   100,
	})

	zm.AddZone(Zone{
		RowGroupID: 1,
		MinValue:   types.NewInt64Value(101),
		MaxValue:   types.NewInt64Value(200),
		RowCount:   100,
	})

	zm.AddZone(Zone{
		RowGroupID: 2,
		MinValue:   types.NewInt64Value(201),
		MaxValue:   types.NewInt64Value(300),
		RowCount:   100,
	})

	// Search for value = 150
	candidates := zm.GetCandidateZones("=", types.NewInt64Value(150))
	if len(candidates) != 1 || candidates[0] != 1 {
		t.Errorf("expected only zone 1, got %v", candidates)
	}

	// Search for value > 100
	candidates = zm.GetCandidateZones(">", types.NewInt64Value(100))
	if len(candidates) != 2 {
		t.Errorf("expected 2 zones, got %v", candidates)
	}
}

func TestZoneMapGlobalMinMax(t *testing.T) {
	zm := NewZoneMap("value")

	zm.AddZone(Zone{
		RowGroupID: 0,
		MinValue:   types.NewInt64Value(50),
		MaxValue:   types.NewInt64Value(100),
		RowCount:   100,
	})

	zm.AddZone(Zone{
		RowGroupID: 1,
		MinValue:   types.NewInt64Value(10),
		MaxValue:   types.NewInt64Value(40),
		RowCount:   100,
	})

	zm.AddZone(Zone{
		RowGroupID: 2,
		MinValue:   types.NewInt64Value(200),
		MaxValue:   types.NewInt64Value(300),
		RowCount:   100,
	})

	min, hasMin := zm.GlobalMin()
	if !hasMin {
		t.Fatal("expected to have global min")
	}
	if v, _ := min.AsInt64(); v != 10 {
		t.Errorf("expected global min 10, got %d", v)
	}

	max, hasMax := zm.GlobalMax()
	if !hasMax {
		t.Fatal("expected to have global max")
	}
	if v, _ := max.AsInt64(); v != 300 {
		t.Errorf("expected global max 300, got %d", v)
	}
}

func TestZoneMapWithNulls(t *testing.T) {
	zm := NewZoneMap("value")

	// Zone with all nulls
	zm.AddZone(Zone{
		RowGroupID: 0,
		MinValue:   types.NewNullValue(),
		MaxValue:   types.NewNullValue(),
		RowCount:   100,
		NullCount:  100,
	})

	// All-null zone should be skipped for any comparison
	if !zm.CanSkip(0, "=", types.NewInt64Value(50)) {
		t.Error("all-null zone should be skipped")
	}
}
