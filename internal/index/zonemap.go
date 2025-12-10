package index

import "github.com/taikicoco/tate/internal/types"

// Zone represents statistics for a row group.
type Zone struct {
	RowGroupID int
	MinValue   types.Value
	MaxValue   types.Value
	RowCount   int
	NullCount  int
}

// ZoneMap is an index that tracks min/max values for row groups.
type ZoneMap struct {
	ColumnName string
	Zones      []Zone
}

// NewZoneMap creates a new zone map.
func NewZoneMap(columnName string) *ZoneMap {
	return &ZoneMap{
		ColumnName: columnName,
		Zones:      make([]Zone, 0),
	}
}

// AddZone adds a new zone to the map.
func (zm *ZoneMap) AddZone(zone Zone) {
	zm.Zones = append(zm.Zones, zone)
}

// UpdateZone updates an existing zone or adds a new one.
func (zm *ZoneMap) UpdateZone(zoneID int, minVal, maxVal types.Value, rowCount, nullCount int) {
	// Extend zones if necessary
	for len(zm.Zones) <= zoneID {
		zm.Zones = append(zm.Zones, Zone{RowGroupID: len(zm.Zones)})
	}

	zone := &zm.Zones[zoneID]
	zone.MinValue = minVal
	zone.MaxValue = maxVal
	zone.RowCount = rowCount
	zone.NullCount = nullCount
}

// GetZone returns a zone by ID.
func (zm *ZoneMap) GetZone(zoneID int) (*Zone, bool) {
	if zoneID < 0 || zoneID >= len(zm.Zones) {
		return nil, false
	}
	return &zm.Zones[zoneID], true
}

// CanSkip determines if a zone can be skipped for a given condition.
func (zm *ZoneMap) CanSkip(zoneID int, op string, value types.Value) bool {
	if zoneID < 0 || zoneID >= len(zm.Zones) {
		return false
	}

	zone := zm.Zones[zoneID]

	// If zone has no data, skip it
	if zone.RowCount == 0 {
		return true
	}

	// If all values are NULL, can't match non-NULL comparisons
	if zone.NullCount == zone.RowCount {
		return true
	}

	switch op {
	case "=":
		// Value is outside [min, max] range
		if value.Compare(zone.MinValue) < 0 || value.Compare(zone.MaxValue) > 0 {
			return true
		}

	case "<":
		// All values >= search value
		if zone.MinValue.Compare(value) >= 0 {
			return true
		}

	case "<=":
		// All values > search value
		if zone.MinValue.Compare(value) > 0 {
			return true
		}

	case ">":
		// All values <= search value
		if zone.MaxValue.Compare(value) <= 0 {
			return true
		}

	case ">=":
		// All values < search value
		if zone.MaxValue.Compare(value) < 0 {
			return true
		}

	case "<>", "!=":
		// Can only skip if min == max == value
		if zone.MinValue.Compare(zone.MaxValue) == 0 &&
			zone.MinValue.Compare(value) == 0 {
			return true
		}
	}

	return false
}

// GetCandidateZones returns zones that may contain matching rows.
func (zm *ZoneMap) GetCandidateZones(op string, value types.Value) []int {
	var candidates []int

	for i := range zm.Zones {
		if !zm.CanSkip(i, op, value) {
			candidates = append(candidates, i)
		}
	}

	return candidates
}

// GetAllZoneIDs returns all zone IDs.
func (zm *ZoneMap) GetAllZoneIDs() []int {
	ids := make([]int, len(zm.Zones))
	for i := range zm.Zones {
		ids[i] = i
	}
	return ids
}

// ZoneCount returns the number of zones.
func (zm *ZoneMap) ZoneCount() int {
	return len(zm.Zones)
}

// TotalRowCount returns the total row count across all zones.
func (zm *ZoneMap) TotalRowCount() int {
	total := 0
	for _, zone := range zm.Zones {
		total += zone.RowCount
	}
	return total
}

// GlobalMin returns the minimum value across all zones.
func (zm *ZoneMap) GlobalMin() (types.Value, bool) {
	if len(zm.Zones) == 0 {
		return types.NewNullValue(), false
	}

	var min types.Value
	hasMin := false

	for _, zone := range zm.Zones {
		if zone.RowCount == 0 || zone.NullCount == zone.RowCount {
			continue
		}
		if !hasMin || zone.MinValue.Compare(min) < 0 {
			min = zone.MinValue
			hasMin = true
		}
	}

	return min, hasMin
}

// GlobalMax returns the maximum value across all zones.
func (zm *ZoneMap) GlobalMax() (types.Value, bool) {
	if len(zm.Zones) == 0 {
		return types.NewNullValue(), false
	}

	var max types.Value
	hasMax := false

	for _, zone := range zm.Zones {
		if zone.RowCount == 0 || zone.NullCount == zone.RowCount {
			continue
		}
		if !hasMax || zone.MaxValue.Compare(max) > 0 {
			max = zone.MaxValue
			hasMax = true
		}
	}

	return max, hasMax
}
