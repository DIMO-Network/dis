package compass

import "encoding/json"

type CompassEvent struct {
	Time           string          `json:"time"`
	Data           json.RawMessage `json:"data"`
	VehicleTokenID *uint32         `json:"vehicleTokenId"`
	DeviceTokenID  *uint32         `json:"deviceTokenId"`
}
