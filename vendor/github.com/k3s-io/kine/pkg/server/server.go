package server

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var (
	_ etcdserverpb.KVServer    = (*KVServerBridge)(nil)
	_ etcdserverpb.WatchServer = (*KVServerBridge)(nil)
)

type KVServerBridge struct {
	limited *LimitedServer
}

func New(backend Backend) *KVServerBridge {
	return &KVServerBridge{
		limited: &LimitedServer{
			backend: backend,
		},
	}
}

func (k *KVServerBridge) Register(server *grpc.Server) {
	etcdserverpb.RegisterLeaseServer(server, k)
	etcdserverpb.RegisterWatchServer(server, k)
	etcdserverpb.RegisterKVServer(server, k)

	hsrv := health.NewServer()
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, hsrv)
}

func (k *KVServerBridge) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if r.KeysOnly {
		return nil, unsupported("keysOnly")
	}

	if r.MaxCreateRevision != 0 {
		return nil, unsupported("maxCreateRevision")
	}

	if r.SortOrder != 0 {
		return nil, unsupported("sortOrder")
	}

	if r.SortTarget != 0 {
		return nil, unsupported("sortTarget")
	}

	if r.Serializable {
		return nil, unsupported("serializable")
	}

	if r.KeysOnly {
		return nil, unsupported("keysOnly")
	}

	if r.MinModRevision != 0 {
		return nil, unsupported("minModRevision")
	}

	if r.MinCreateRevision != 0 {
		return nil, unsupported("minCreateRevision")
	}

	if r.MaxCreateRevision != 0 {
		return nil, unsupported("maxCreateRevision")
	}

	if r.MaxModRevision != 0 {
		return nil, unsupported("maxModRevision")
	}

	resp, err := k.limited.Range(ctx, r)
	if err != nil {
		logrus.Errorf("error while range on %s %s: %v", r.Key, r.RangeEnd, err)
		return nil, err
	}

	rangeResponse := &etcdserverpb.RangeResponse{
		More:   resp.More,
		Count:  resp.Count,
		Header: resp.Header,
		Kvs:    toKVs(resp.Kvs...),
	}

	return rangeResponse, nil
}

func toKVs(kvs ...*KeyValue) []*mvccpb.KeyValue {
	if len(kvs) == 0 || kvs[0] == nil {
		return nil
	}

	ret := make([]*mvccpb.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		newKV := toKV(kv)
		if newKV != nil {
			ret = append(ret, newKV)
		}
	}
	return ret
}

func toKV(kv *KeyValue) *mvccpb.KeyValue {
	if kv == nil {
		return nil
	}
	return &mvccpb.KeyValue{
		Key:            []byte(kv.Key),
		Value:          kv.Value,
		Lease:          kv.Lease,
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
	}
}

func (k *KVServerBridge) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	return nil, fmt.Errorf("put is not supported")
}

func (k *KVServerBridge) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	return nil, fmt.Errorf("delete is not supported")
}

func (k *KVServerBridge) Txn(ctx context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	res, err := k.limited.Txn(ctx, r)
	if err != nil {
		logrus.Errorf("error in txn: %v", err)
	}
	return res, err
}

func (k *KVServerBridge) Compact(ctx context.Context, r *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	return &etcdserverpb.CompactionResponse{
		Header: &etcdserverpb.ResponseHeader{
			Revision: r.Revision,
		},
	}, nil
}

func unsupported(field string) error {
	return fmt.Errorf("%s is unsupported", field)
}
