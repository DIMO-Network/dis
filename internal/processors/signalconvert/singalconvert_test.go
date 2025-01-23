package signalconvert

import (
	"testing"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPruneSignals(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name            string
		signals         []vss.Signal
		expectedSignals []vss.Signal
		expectError     []error
	}{
		{
			name: "future timestamp should be pruned",
			signals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldPowertrainCombustionEngineSpeed, Timestamp: now.Add(1 * time.Hour), ValueNumber: 3000},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 75.5},
				{Name: vss.FieldPowertrainCombustionEngineECT, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 90.0},
			},
			expectedSignals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 75.5},
				{Name: vss.FieldPowertrainCombustionEngineECT, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 90.0},
			},
			expectError: []error{errFutureTimestamp},
		},
		{
			name: "duplicate signals should be pruned",
			signals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 45.5},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 75.5},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 80.0},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: -122.6},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: -123.0},
			},
			expectedSignals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: -123.0},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 80.0},
			},
		},
		{
			name: "matching lat/long pairs should be kept with surrounding signals",
			signals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 45.5},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 75.5},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: -122.6},
				{Name: vss.FieldPowertrainCombustionEngineECT, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 90.0},
			},
			expectedSignals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 45.5},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: -122.6},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 75.5},
				{Name: vss.FieldPowertrainCombustionEngineECT, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 90.0},
			},
			expectError: nil,
		},
		{
			name: "matching lat/long pairs should be kept with .2 sec diff",
			signals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 45.5},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 75.5},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-1 * time.Hour).Add(time.Millisecond * 200), ValueNumber: -122.6},
				{Name: vss.FieldPowertrainCombustionEngineECT, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 90.0},
			},
			expectedSignals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 45.5},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 75.5},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-1 * time.Hour).Add(time.Millisecond * 200), ValueNumber: -122.6},
				{Name: vss.FieldPowertrainCombustionEngineECT, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 90.0},
			},
			expectError: nil,
		},
		{
			name: "matching long/lat pairs should be kept with .2 sec diff",
			signals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 45.5},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 75.5},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-1 * time.Hour).Add(time.Millisecond * 200), ValueNumber: -122.6},
				{Name: vss.FieldPowertrainCombustionEngineECT, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 90.0},
			},
			expectedSignals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 45.5},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 75.5},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-1 * time.Hour).Add(time.Millisecond * 200), ValueNumber: -122.6},
				{Name: vss.FieldPowertrainCombustionEngineECT, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 90.0},
			},
			expectError: nil,
		},
		{
			name: "missing longitude should prune latitude while keeping other signals",
			signals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 45.5},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 75.5},
				{Name: vss.FieldPowertrainCombustionEngineECT, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 90.0},
			},
			expectedSignals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 75.5},
				{Name: vss.FieldPowertrainCombustionEngineECT, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 90.0},
			},
			expectError: []error{errLatLongMismatch},
		},
		{
			name: "lat/long pairs with different timestamps and future timestamp should be pruned",
			signals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 45.5},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(1 * time.Hour), ValueNumber: 75.5},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-2 * time.Hour), ValueNumber: -122.6},
				{Name: vss.FieldPowertrainCombustionEngineECT, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 90.0},
			},
			expectedSignals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 50.0},
				{Name: vss.FieldPowertrainCombustionEngineECT, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 90.0},
			},
			expectError: []error{errLatLongMismatch, errFutureTimestamp},
		},
		{
			name: "multiple lat/long pairs should be handled correctly",
			signals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-3 * time.Hour), ValueNumber: 45.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 45.5},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-2 * time.Hour), ValueNumber: -122.6},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 75.5},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 45.6},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-30 * time.Minute), ValueNumber: -122.7},
				{Name: vss.FieldPowertrainCombustionEngineECT, Timestamp: now.Add(-15 * time.Minute), ValueNumber: 90.0},
			},
			expectedSignals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-3 * time.Hour), ValueNumber: 45.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 45.5},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-2 * time.Hour), ValueNumber: -122.6},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 75.5},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 45.6},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-30 * time.Minute), ValueNumber: -122.7},
				{Name: vss.FieldPowertrainCombustionEngineECT, Timestamp: now.Add(-15 * time.Minute), ValueNumber: 90.0},
			},
			expectError: nil,
		},
		{
			name: "double latitude should prune one latitude",
			signals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-3 * time.Hour), ValueNumber: 45.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-2 * time.Hour).Add(-200 * time.Millisecond), ValueNumber: 45.5},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 45.6},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: -122.6},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 75.5},
			},
			expectedSignals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-3 * time.Hour), ValueNumber: 45.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: 45.6},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: -122.6},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 75.5},
			},
			expectError: []error{errLatLongMismatch},
		},
		{
			name: "double longitude should prune one longitude",
			signals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-3 * time.Hour), ValueNumber: 45.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 45.5},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-2 * time.Hour), ValueNumber: -122.6},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-2 * time.Hour).Add(200 * time.Millisecond), ValueNumber: -122.6},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 75.5},
			},
			expectedSignals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-3 * time.Hour), ValueNumber: 45.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 45.5},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-2 * time.Hour), ValueNumber: -122.6},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 75.5},
			},
			expectError: []error{errLatLongMismatch},
		},
		{
			name: "latitude and longitude too far apart should prune both",
			signals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-3 * time.Hour), ValueNumber: 45.0},
				{Name: vss.FieldCurrentLocationLatitude, Timestamp: now.Add(-2 * time.Hour), ValueNumber: 45.5},
				{Name: vss.FieldCurrentLocationLongitude, Timestamp: now.Add(-1 * time.Hour), ValueNumber: -122.6},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 75.5},
			},
			expectedSignals: []vss.Signal{
				{Name: vss.FieldSpeed, Timestamp: now.Add(-3 * time.Hour), ValueNumber: 45.0},
				{Name: vss.FieldPowertrainFuelSystemRelativeLevel, Timestamp: now.Add(-30 * time.Minute), ValueNumber: 75.5},
			},
			expectError: []error{errLatLongMismatch},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := pruneSignals(tt.signals)

			if tt.expectError != nil {
				require.Error(t, err, "expected an error but got none")
				for _, expectedErr := range tt.expectError {
					require.ErrorIs(t, err, expectedErr, "expected error type does not match")
				}
			} else {
				require.NoError(t, err, "expected no error but got one")
			}

			require.Equal(t, len(tt.expectedSignals), len(result), "result length does not match expected length")

			for i := range result {
				require.Equal(t, tt.expectedSignals[i].Name, result[i].Name, "signal name mismatch at index %d", i)
				assert.Equal(t, tt.expectedSignals[i].ValueNumber, result[i].ValueNumber, "signal ValueNumber mismatch at index %d", i)
				assert.Equal(t, tt.expectedSignals[i].Timestamp.Unix(), result[i].Timestamp.Unix(), "signal timestamp mismatch at index %d", i)
			}

			// Verify no pruneSignalName signals in result
			for i, signal := range result {
				require.NotEqual(t, pruneSignalName, signal.Name, "found pruned signal at index %d", i)
			}
		})
	}
}
