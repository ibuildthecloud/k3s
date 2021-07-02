package server

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (s *KVServerBridge) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	return &etcdserverpb.LeaseGrantResponse{
		Header: &etcdserverpb.ResponseHeader{},
		ID:     req.TTL,
		TTL:    req.TTL,
	}, nil
}

func (s *KVServerBridge) LeaseRevoke(context.Context, *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	return nil, fmt.Errorf("lease revoke is not supported")
}

func (s *KVServerBridge) LeaseKeepAlive(etcdserverpb.Lease_LeaseKeepAliveServer) error {
	return fmt.Errorf("lease keep alive is not supported")
}

func (s *KVServerBridge) LeaseTimeToLive(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	return nil, fmt.Errorf("lease time to live is not supported")
}

func (s *KVServerBridge) LeaseLeases(context.Context, *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	return nil, fmt.Errorf("lease leases is not supported")
}
