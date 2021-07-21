package server

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func isDelete(txn *etcdserverpb.TxnRequest) (int64, string, bool) {
	if len(txn.Compare) == 0 &&
		len(txn.Failure) == 0 &&
		len(txn.Success) == 2 &&
		txn.Success[0].GetRequestRange() != nil &&
		txn.Success[1].GetRequestDeleteRange() != nil {
		rng := txn.Success[1].GetRequestDeleteRange()
		return 0, string(rng.Key), true
	}
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		len(txn.Failure) == 1 &&
		txn.Failure[0].GetRequestRange() != nil &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestDeleteRange() != nil {
		return txn.Compare[0].GetModRevision(), string(txn.Success[0].GetRequestDeleteRange().Key), true
	}
	return 0, "", false
}

func (l *LimitedServer) delete(ctx context.Context, key string, revision int64) (*etcdserverpb.TxnResponse, error) {
	rev, kv, ok, err := l.backend.Delete(ctx, key, revision)
	if err != nil {
		return nil, err
	}

	return &etcdserverpb.TxnResponse{
		Header: txnHeader(rev),
		Responses: []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Header: txnHeader(rev),
						Kvs:    toKVs(kv),
					},
				},
			},
		},
		Succeeded: ok,
	}, nil
}
