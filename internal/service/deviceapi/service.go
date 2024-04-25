package deviceapi

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/DIMO-Network/devices-api/pkg/grpc"
	gocache "github.com/patrickmn/go-cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	deviceTokenCacheKey = "udID_%s"
	cacheDefaultExp     = 24 * time.Hour
)

// NotFoundError is an error type for when a device's token is not found.
type NotFoundError struct {
	DeviceID string
}

// Error returns the error message for a NotFoundError.
func (e NotFoundError) Error() string {
	return fmt.Sprintf("device token not found for userDevice '%s'", e.DeviceID)
}

// Service is a wrapper for a the device-api grpc client
type Service struct {
	devicesConn *grpc.ClientConn
	memoryCache *gocache.Cache
}

// NewService API wrapper to call device-telemetry-api to get the userDevices associated with a userId over grpc
func NewService(devicesConn *grpc.ClientConn) *Service {
	c := gocache.New(cacheDefaultExp, 15*time.Minute)
	return &Service{devicesConn: devicesConn, memoryCache: c}
}

// TokenIDFromSubject gets the tokenID from a userDevice subject
func (s *Service) TokenIDFromSubject(ctx context.Context, id string) (uint32, error) {
	deviceClient := pb.NewUserDeviceServiceClient(s.devicesConn)
	var err error
	var userDevice *pb.UserDevice

	get, found := s.memoryCache.Get(fmt.Sprintf(deviceTokenCacheKey, id))
	if found {
		userDevice = get.(*pb.UserDevice)
	} else {
		userDevice, err = deviceClient.GetUserDevice(ctx, &pb.GetUserDeviceRequest{
			Id: id,
		})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				notFound := fmt.Errorf("%w: no device exist", NotFoundError{DeviceID: id})
				return 0, errors.Join(notFound, err)
			}
			return 0, err
		}
		s.memoryCache.Set(fmt.Sprintf(deviceTokenCacheKey, id), userDevice, 0)
	}

	if userDevice.TokenId == nil {
		return 0, fmt.Errorf("%w: no tokenID set", NotFoundError{DeviceID: id})
	}
	return uint32(*userDevice.TokenId), nil
}
