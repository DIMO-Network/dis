package signalconvert

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// maxLatLongDur is the amount of time we'll wait before starting
// a new coordinate triple.
//
// TODO(elffjs): Should this be a setting?
const maxLatLongDur = 500 * time.Millisecond

// zeroTime is used to reset the timestamp on the coordinate store.
var zeroTime time.Time

// handleCoordinates transforms a slice of input signals in ways that
// simplify downstream processing. Currently this means:
//
//   - Remove location values with latitude and longitude both equal
//     to zero.
//   - Roughly, for each triple of the input signals named
//     currentLocationLatitude, currentLocationLongitude, and
//     dimoAftermarketHDOP with sufficiently
//     close timestamps, we will also emit a location-values signal
//     named currentLocationCoordinates which combines all three.
//   - Remove unpaired latitudes and longitudes.
//
// The returned slice of signals is always meaningful, even if an error
// is also returned.
//
// Note that this function may reorder the input slice.
func handleCoordinates(signals []vss.Signal) ([]vss.Signal, error) {
	return newCoordinateStore(signals).processSignals()
}

func newCoordinateStore(signals []vss.Signal) *coordinateStore {
	return &coordinateStore{
		signals:  signals,
		lastLat:  -1,
		lastLon:  -1,
		lastHDOP: -1,
	}
}

type coordinateStore struct {
	// lastLat is the index of the signals slice holding latitude for
	// the location triple under construction. If there is no latitude
	// yet found for the triple then the value of lastLat is -1.
	lastLat int
	// lastLon is like lastLat for longitude.
	lastLon int
	// lastHDOP is like lastLat for HDOP.
	lastHDOP int
	// lastTime is the timestamp of the earliest signal in the active
	// triple. If we have no parts for the active triple then this
	// will be the zero value of time.Time.
	lastTime time.Time

	// signals is the input slice of signals.
	signals []vss.Signal

	// created holds location signals that we've constructed while
	// iterating over signals.
	created []vss.Signal
	// errs contains errors arising from location construction.
	// Typically these have to do with unpaired coordinates, or
	// latitude = longitude = 0.
	errs []error
}

func (c *coordinateStore) processSignals() ([]vss.Signal, error) {
	if len(c.signals) == 0 {
		return c.signals, nil
	}

	// Sorting this way makes it easier to handle time gaps. Sorting
	// thereafter by name is not strictly necessary. Typically, this
	// sorting will already have been performed upstream by a
	// duplicate detector.
	slices.SortFunc(c.signals, func(a, b vss.Signal) int {
		return cmp.Or(a.Timestamp.Compare(b.Timestamp), cmp.Compare(a.Name, b.Name))
	})

	for i := range c.signals {
		c.processSignal(i)
	}

	// One last attempt, in case we're in the process of constructing
	// a location.
	c.tryCreateLocation()

	var out []vss.Signal
	for _, sig := range c.signals {
		if sig.Name != pruneSignalName {
			out = append(out, sig)
		}
	}

	// TODO(elffjs): Should we sort these?
	out = append(out, c.created...)

	return out, errors.Join(c.errs...)
}

func (c *coordinateStore) processSignal(index int) {
	sig := c.signals[index]

	if !c.lastTime.IsZero() && sig.Timestamp.Sub(c.lastTime) >= maxLatLongDur {
		c.tryCreateLocation()
	}

	// This logic could be made shorter and less repetitive by
	// playing around with *int.
	switch sig.Name {
	case vss.FieldCurrentLocationLatitude:
		if c.lastLat != -1 {
			// Start a new triple, but see if what's already being
			// tracked is enough to yield a row.
			c.tryCreateLocation()
		}
		c.lastLat = index
	case vss.FieldCurrentLocationLongitude:
		if c.lastLon != -1 {
			c.tryCreateLocation()
		}
		c.lastLon = index
	case vss.FieldDIMOAftermarketHDOP:
		if c.lastHDOP != -1 {
			c.tryCreateLocation()
		}
		c.lastHDOP = index
	default:
		return
	}

	if c.lastTime.IsZero() {
		c.lastTime = sig.Timestamp
	}
}

// tryCreateLocation tries to add a VSS location row using the active
// location triple.
//
// Only call this function when forced: if there is any chance that
// the triple can be completed by the next element of the slice then
// calling this function may discard the elements of the active triple
// on the grounds of being incomplete.
func (c *coordinateStore) tryCreateLocation() {
	var loc vss.Location
	var create bool

	template := c.signals[0]

	if c.lastLat != -1 && c.lastLon != -1 {
		lat := c.signals[c.lastLat].ValueNumber
		lon := c.signals[c.lastLon].ValueNumber

		if lat == 0 && lon == 0 {
			c.signals[c.lastLat].Name = pruneSignalName
			c.signals[c.lastLon].Name = pruneSignalName
			c.errs = append(c.errs, fmt.Errorf("%w: latitude and longitude at origin at time %s", errLatLongMismatch, c.lastTime))
		} else {
			loc.Latitude = lat
			loc.Longitude = lon
			create = true
		}
	} else if c.lastLat != -1 {
		c.signals[c.lastLat].Name = pruneSignalName
		c.errs = append(c.errs, fmt.Errorf("%w: unpaired latitude at time %s", errLatLongMismatch, c.lastTime))
	} else if c.lastLon != -1 {
		c.signals[c.lastLon].Name = pruneSignalName
		c.errs = append(c.errs, fmt.Errorf("%w: unpaired longitude at time %s", errLatLongMismatch, c.lastTime))
	}

	if c.lastHDOP != -1 {
		loc.HDOP = c.signals[c.lastHDOP].ValueNumber
		create = true
	}

	if create {
		c.created = append(c.created, vss.Signal{
			TokenID:       template.TokenID,
			Timestamp:     c.lastTime,
			Name:          vss.FieldCurrentLocationCoordinates,
			ValueLocation: loc,
			Source:        template.Source,
			Producer:      template.Producer,
			CloudEventID:  template.CloudEventID,
		})
	}

	c.lastLat = -1
	c.lastLon = -1
	c.lastHDOP = -1
	c.lastTime = zeroTime
}
