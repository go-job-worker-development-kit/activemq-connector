package internal

import (
	"errors"
	"sync/atomic"

	"github.com/go-job-worker-development-kit/jobworker"
	"github.com/go-stomp/stomp"
)

const (
	subStateActive  = 0
	subStateClosing = 1
	subStateClosed  = 2
)

func NewSubscription(name string,
	raw *stomp.Subscription) *Subscription {
	return &Subscription{
		name:  name,
		raw:   raw,
		queue: make(chan *jobworker.Job),
	}
}

type Subscription struct {
	name  string
	raw   *stomp.Subscription
	queue chan *jobworker.Job
	state int32
}

func (s *Subscription) Active() bool {
	return atomic.LoadInt32(&s.state) == subStateActive
}

func (s *Subscription) Queue() chan *jobworker.Job {
	return s.queue
}

var ErrCompletedSubscription = errors.New("subscription is unsubscribed")

func (s *Subscription) UnSubscribe() error {
	if !atomic.CompareAndSwapInt32(&s.state, subStateActive, subStateClosing) {
		return ErrCompletedSubscription
	}
	// TODO error handling
	return s.raw.Unsubscribe()
}

func (s *Subscription) ReadLoop(conn jobworker.Connector) {
	for {
		msg, ok := <-s.raw.C
		if !ok {
			state := atomic.LoadInt32(&s.state)
			if state == subStateActive || state == subStateClosing {
				s.closeQueue()
			}
			return
		}
		s.queue <- newJob(s.name, msg, conn)
	}
}

func (s *Subscription) closeQueue() {
	atomic.StoreInt32(&s.state, subStateClosed)
	close(s.queue)
}

func newJob(queue string, msg *stomp.Message, conn jobworker.Connector) *jobworker.Job {
	var payload jobworker.Payload
	payload.Content = string(msg.Body)
	payload.Metadata = newMetadata(msg)
	payload.CustomAttribute = make(map[string]*jobworker.CustomAttribute)
	payload.Raw = msg
	return jobworker.NewJob(conn, queue, &payload)
}

func newMetadata(msg *stomp.Message) map[string]string {
	metadata := make(map[string]string, msg.Header.Len())
	for i := 0; i < msg.Header.Len(); i++ {
		k, v := msg.Header.GetAt(i)
		metadata[k] = v
	}
	return metadata
}
