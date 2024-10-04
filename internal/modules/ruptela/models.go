package ruptela

import (
	"encoding/json"
	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
)

type RuptelaEvent struct {
	DS             string          `json:"ds"`
	Signature      string          `json:"signature"`
	Time           string          `json:"time"`
	Data           json.RawMessage `json:"data"`
	VehicleTokenID uint64          `json:"vehicleTokenId"`
	DeviceTokenID  uint64          `json:"deviceTokenId"`
}

type CloudEvent[A any] struct {
	cloudevent.CloudEvent[A]
	DataVersion string `json:"dataversion,omitempty"`
	Producer    string `json:"producer,omitempty"`
	Signature   string `json:"signature,omitempty"`
}

type DataContent struct {
	Signals map[string]string `json:"signals"`
}
