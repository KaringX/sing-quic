// karing
// https://github.com/hmgle/delaytask/blob/master/delaytask.go
package qtls //delaytask

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Job defines a job.
type Job struct {
	ID    string
	Fn    func()
	Delay time.Duration

	timer       *time.Timer
	whenExecute time.Duration
}

// Task defines a task.
type Task struct {
	running     bool
	lock        sync.RWMutex
	tm          sync.Map
	waitCh      chan string
	quitCh      chan bool
	ctx         context.Context
	cancel      context.CancelFunc
	atomWaitCnt int64
}

// New will create a task.
func New() *Task {
	ctx, cancel := context.WithCancel(context.Background())
	t := &Task{
		running: true,
		tm:      sync.Map{},
		waitCh:  make(chan string),
		quitCh:  make(chan bool),
		ctx:     ctx,
		cancel:  cancel,
	}
	go func() {
		wg := sync.WaitGroup{}
		for id := range t.waitCh {
			t.lock.Lock()
			v, ok := t.tm.Load(id)
			t.lock.Unlock()
			if ok {
				t.lock.Lock()
				t.tm.Delete(id)
				t.lock.Unlock()
				j := v.(*Job)
				wg.Add(1)
				go func() {
					j.Fn()
					wg.Done()
				}()
			}
		}
		wg.Wait()

		t.quitCh <- true
	}()
	return t
}

// AddJob add a delay job task.
func (t *Task) AddJob(j *Job) {
	if !t.running {
		return
	}
	if j.Delay > 0 {
		j.timer = time.NewTimer(j.Delay)
		j.whenExecute = time.Duration(time.Now().UnixNano()) + j.Delay
		atomic.AddInt64(&t.atomWaitCnt, 1)
		go func(ctx context.Context) {
			defer atomic.AddInt64(&t.atomWaitCnt, -1)
			select {
			case <-j.timer.C:
				if t.running {
					t.waitCh <- j.ID
				}
			case <-ctx.Done():
				if t.running {
					t.waitCh <- j.ID
				}
			}
		}(t.ctx)
	} else {
		j.whenExecute = -1
	}
	t.lock.Lock()
	t.tm.Store(j.ID, j)
	t.lock.Unlock()
}

// AddJobFn add a delay job by func to task.
func (t *Task) AddJobFn(id string, fn func(), delay ...time.Duration) {
	if !t.running {
		return
	}
	j := &Job{
		ID:          id,
		Fn:          fn,
		whenExecute: -1,
	}
	if len(delay) > 0 {
		j.Delay = delay[0]
		j.timer = time.NewTimer(j.Delay)
		j.whenExecute = time.Duration(time.Now().UnixNano()) + j.Delay
		atomic.AddInt64(&t.atomWaitCnt, 1)
		go func(ctx context.Context) {
			defer atomic.AddInt64(&t.atomWaitCnt, -1)
			select {
			case <-j.timer.C:
				if t.running {
					t.waitCh <- id
				}
			case <-ctx.Done():
				if t.running {
					t.waitCh <- id
				}
			}
		}(t.ctx)
	}
	t.lock.Lock()
	t.tm.Store(id, j)
	t.lock.Unlock()
}

// WhenExecute returns d as a Unix time, the number of nanoseconds
// elapsed since January 1, 1970 UTC of the delayed job's execution time.
func (t *Task) WhenExecute(id string) (d time.Duration) {
	t.lock.Lock()
	v, ok := t.tm.Load(id)
	t.lock.Unlock()
	if !ok {
		return -1
	}
	return v.(*Job).whenExecute
}

// Execute the job immediately.
func (t *Task) Execute(id string) {
	if !t.running {
		return
	}
	t.waitCh <- id
}

// Cancel the job.
func (t *Task) Cancel(id string) {
	t.lock.Lock()
	t.tm.Delete(id)
	t.lock.Unlock()
}

// Reset changes the job's timer to expire after duration d.
// It returns true if the timer had been active, false if the timer had
// expired or been stopped.
func (t *Task) Reset(id string, d time.Duration) bool {
	if !t.running {
		return false
	}
	t.lock.Lock()
	v, ok := t.tm.Load(id)
	t.lock.Unlock()
	if !ok {
		return false
	}
	j := v.(*Job)
	if j.timer != nil {
		return j.timer.Reset(d)
	}
	return false
}

// Stop the task, the unexpired jobs will be executed immediately.
func (t *Task) Stop() <-chan bool {
	t.cancel()
	for {
		if atomic.LoadInt64(&t.atomWaitCnt) == 0 {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	t.running = false
	close(t.waitCh)
	return t.quitCh
}

// GracefulExit the task until all jobs are completed.
func (t *Task) GracefulExit() <-chan bool {
	for {
		if atomic.LoadInt64(&t.atomWaitCnt) == 0 {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	t.running = false
	close(t.waitCh)
	return t.quitCh
}
