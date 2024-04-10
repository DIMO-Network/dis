package deviceapi

import (
	"context"
	"fmt"
	"time"

	pb "github.com/DIMO-Network/devices-api/pkg/grpc"
	gocache "github.com/patrickmn/go-cache"
	"google.golang.org/grpc"
)

const (
	deviceTokenCacheKey = "udID_%s"
	cacheDefaultExp     = 24 * time.Hour
)

type Service struct {
	devicesConn *grpc.ClientConn
	memoryCache *gocache.Cache
}

// NewService API wrapper to call device-telemetry-api to get the userDevices associated with a userId over grpc
func NewService(devicesConn *grpc.ClientConn) *Service {
	c := gocache.New(cacheDefaultExp, 15*time.Minute)
	return &Service{devicesConn: devicesConn, memoryCache: c}
}

func (s *Service) GetTokenIDFromID(ctx context.Context, id string) (uint32, error) {
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
			return 0, err
		}
		s.memoryCache.Set(fmt.Sprintf(deviceTokenCacheKey, id), userDevice, 0)
	}

	if userDevice.TokenId == nil {
		return 0, fmt.Errorf("device token not found for userDevice %s", id)
	}
	return uint32(*userDevice.TokenId), nil
}
