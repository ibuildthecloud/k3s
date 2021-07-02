package server

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

var (
	watchID int64
)

func (s *KVServerBridge) Watch(ws etcdserverpb.Watch_WatchServer) error {
	w := watcher{
		server:  ws,
		backend: s.limited.backend,
		watches: map[int64]func(){},
	}
	defer w.Close()

	for {
		msg, err := ws.Recv()
		if err != nil {
			return err
		}

		if msg.GetCreateRequest() != nil {
			w.Start(ws.Context(), msg.GetCreateRequest())
		} else if msg.GetCancelRequest() != nil {
			logrus.Tracef("WATCH CANCEL REQ id=%d", msg.GetCancelRequest().GetWatchId())
			w.Cancel(msg.GetCancelRequest().WatchId, nil)
		}
	}
}

type watcher struct {
	sync.Mutex

	wg      sync.WaitGroup
	backend Backend
	server  etcdserverpb.Watch_WatchServer
	watches map[int64]func()
}

func (w *watcher) Start(ctx context.Context, r *etcdserverpb.WatchCreateRequest) {
	w.Lock()
	defer w.Unlock()

	ctx, cancel := context.WithCancel(ctx)

	id := atomic.AddInt64(&watchID, 1)
	w.watches[id] = cancel
	w.wg.Add(1)

	key := string(r.Key)

	logrus.Tracef("WATCH START id=%d, count=%d, key=%s, revision=%d", id, len(w.watches), key, r.StartRevision)

	go func() {
		defer w.wg.Done()
		if err := w.server.Send(&etcdserverpb.WatchResponse{
			Header:  &etcdserverpb.ResponseHeader{},
			Created: true,
			WatchId: id,
		}); err != nil {
			w.Cancel(id, err)
			return
		}

		for events := range w.backend.Watch(ctx, key, r.StartRevision) {
			if len(events) == 0 {
				continue
			}

			if logrus.IsLevelEnabled(logrus.DebugLevel) {
				for _, event := range events {
					logrus.Tracef("WATCH READ id=%d, key=%s, revision=%d", id, event.KV.Key, event.KV.ModRevision)
				}
			}

			if err := w.server.Send(&etcdserverpb.WatchResponse{
				Header:  txnHeader(events[len(events)-1].KV.ModRevision),
				WatchId: id,
				Events:  toEvents(events...),
			}); err != nil {
				w.Cancel(id, err)
				continue
			}
		}
		w.Cancel(id, nil)
		logrus.Tracef("WATCH CLOSE id=%d, key=%s", id, key)
	}()
}

func toEvents(events ...*Event) []*mvccpb.Event {
	ret := make([]*mvccpb.Event, 0, len(events))
	for _, e := range events {
		ret = append(ret, toEvent(e))
	}
	return ret
}

func toEvent(event *Event) *mvccpb.Event {
	e := &mvccpb.Event{
		Kv:     toKV(event.KV),
		PrevKv: toKV(event.PrevKV),
	}
	if event.Delete {
		e.Type = mvccpb.DELETE
	} else {
		e.Type = mvccpb.PUT
	}

	return e
}

func (w *watcher) Cancel(watchID int64, err error) {
	w.Lock()
	if cancel, ok := w.watches[watchID]; ok {
		cancel()
		delete(w.watches, watchID)
	}
	w.Unlock()

	reason := ""
	if err != nil {
		reason = err.Error()
	}
	logrus.Tracef("WATCH CANCEL id=%d reason=%s", watchID, reason)
	serr := w.server.Send(&etcdserverpb.WatchResponse{
		Header:       &etcdserverpb.ResponseHeader{},
		Canceled:     true,
		CancelReason: "watch closed",
		WatchId:      watchID,
	})
	if serr != nil && err != nil {
		logrus.Errorf("WATCH Failed to send cancel response for watchID %d: %v", watchID, serr)
	}
}

func (w *watcher) Close() {
	w.Lock()
	for _, v := range w.watches {
		v()
	}
	w.Unlock()
	w.wg.Wait()
}
