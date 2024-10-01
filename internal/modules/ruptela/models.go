package ruptela

import (
	"encoding/json"
	dimoshared "github.com/DIMO-Network/shared"
)

type RuptelaEvent struct {
	DS             string          `json:"ds"`
	Signature      string          `json:"signature"`
	Time           string          `json:"time"`
	Data           json.RawMessage `json:"data"`
	VehicleTokenID uint64          `json:"vehicleTokenId"`
	TokenID        uint64          `json:"tokenId"`
}

type CloudEvent[A any] struct {
	dimoshared.CloudEvent[A]
	DataVersion string `json:"dataversion,omitempty"`
	Producer    string `json:"producer,omitempty"`
	Signature   string `json:"signature,omitempty"`
}

type DataContent struct {
	Signals map[string]string `json:"signals"`
}