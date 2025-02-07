package macaron

import "encoding/json"

type MacaronEvent struct {
	Data           json.RawMessage `json:"data"`
	VehicleTokenID *uint64         `json:"vehicleTokenId"`
	DeviceTokenID  *uint32         `json:"deviceTokenId"`
	Signature      string          `json:"signature"`
	Time           string          `json:"time"`
	Type           string          `json:"type"`
}

type DataContent struct {
	Signals map[string]string `json:"signals"`
}
