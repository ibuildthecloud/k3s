/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package task

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// NewBackgroundTaskManager provides a task manager. You can specify the
// concurrency of background tasks. When running a background task, this will be
// forced to wait until no prioritized task is running for some period. You can
// specify the period through the argument of this function, too.
func NewBackgroundTaskManager(concurrency int64, period time.Duration) *BackgroundTaskManager {
	return &BackgroundTaskManager{
		backgroundSem:                semaphore.NewWeighted(concurrency),
		prioritizedTaskSilencePeriod: period,
		prioritizedTaskStartNotify:   make(chan struct{}),
		prioritizedTaskDoneCond:      sync.NewCond(&sync.Mutex{}),
	}
}

// BackgroundTaskManager is a task manager which manages prioritized tasks and
// background tasks execution. Background tasks are less important than
// prioritized tasks. You can let these background tasks not to use compute
// resources (CPU, NW, etc...) during more important tasks(=prioritized tasks)
// running.
//
// When you run a prioritised task and don't want background tasks to use
// resources you can tell it this manager by calling DoPrioritizedTask method.
// DonePrioritizedTask method must be called at the end of the prioritised task
// execution.
//
// For running a background task, you can use InvokeBackgroundTask method. The
// background task must be able to be cancelled via context.Context argument.
// The task is forced to wait until no prioritized task is running for some
// period. You can specify the period when making this manager instance. The
// limited number of background tasks run simultaneously and you can specify the
// concurrency when making this manager instance too. If a prioritized task
// starts during the execution of background tasks, all background tasks running
// will be cancelled via context. These cancelled tasks will be executed again
// later, same as other background tasks (when no prioritized task is running
// for some period).
type BackgroundTaskManager struct {
	prioritizedTasks             int64
	backgroundSem                *semaphore.Weighted
	prioritizedTaskSilencePeriod time.Duration
	prioritizedTaskStartNotify   chan struct{}
	prioritizedTaskStartNotifyMu sync.Mutex
	prioritizedTaskDoneCond      *sync.Cond
}

// DoPrioritizedTask tells the manager that we are running a prioritized task
// and don't want background tasks to disturb resources(CPU, NW, etc...)
func (ts *BackgroundTaskManager) DoPrioritizedTask() {
	// Notify the prioritized task execution to background tasks.
	ts.prioritizedTaskStartNotifyMu.Lock()
	atomic.AddInt64(&ts.prioritizedTasks, 1)
	close(ts.prioritizedTaskStartNotify)
	ts.prioritizedTaskStartNotify = make(chan struct{})
	ts.prioritizedTaskStartNotifyMu.Unlock()
}

// DonePrioritizedTask tells the manager that we've done a prioritized task
// and don't want background tasks to disturb resources(CPU, NW, etc...)
func (ts *BackgroundTaskManager) DonePrioritizedTask() {
	go func() {
		// Notify the task completion after `ts.prioritizedTaskSilencePeriod`
		// so that background tasks aren't invoked immediately.
		time.Sleep(ts.prioritizedTaskSilencePeriod)
		atomic.AddInt64(&ts.prioritizedTasks, -1)
		ts.prioritizedTaskDoneCond.Broadcast()
	}()
}

// InvokeBackgroundTask invokes a background task. The task is started only when
// no prioritized tasks are running. Prioritized task's execution stops the
// execution of all background tasks. Background task must be able to be
// cancelled via context.Context argument and be able to be restarted again.
func (ts *BackgroundTaskManager) InvokeBackgroundTask(do func(context.Context), timeout time.Duration) {
	for {
		// Wait until all prioritized tasks are done
		for {
			if atomic.LoadInt64(&ts.prioritizedTasks) <= 0 {
				break
			}

			// waits until a prioritized task is done
			ts.prioritizedTaskDoneCond.L.Lock()
			if atomic.LoadInt64(&ts.prioritizedTasks) > 0 {
				ts.prioritizedTaskDoneCond.Wait()
			}
			ts.prioritizedTaskDoneCond.L.Unlock()
		}

		// limited number of background tasks can run at once.
		// if prioritized tasks are running, cancel this task.
		if func() bool {
			ts.backgroundSem.Acquire(context.Background(), 1)
			defer ts.backgroundSem.Release(1)

			// Get notify the prioritized tasks execution.
			ts.prioritizedTaskStartNotifyMu.Lock()
			ch := ts.prioritizedTaskStartNotify
			tasks := atomic.LoadInt64(&ts.prioritizedTasks)
			ts.prioritizedTaskStartNotifyMu.Unlock()
			if tasks > 0 {
				return false
			}

			// Invoke the background task. if some prioritized tasks added during
			// execution, cancel it and try it later.
			var (
				done        = make(chan struct{})
				ctx, cancel = context.WithTimeout(context.Background(), timeout)
			)
			defer cancel()
			go func() {
				do(ctx)
				close(done)
			}()

			// Wait until the background task is done or canceled.
			select {
			case <-ch: // some prioritized tasks started; retry it later
				cancel()
				return false
			case <-done: // All tasks completed
			}
			return true
		}() {
			break
		}
	}
}
