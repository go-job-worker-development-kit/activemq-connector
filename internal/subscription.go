package internal

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/go-job-worker-development-kit/jobworker"
	"github.com/go-stomp/stomp"
)

const (
	subStateActive  = 0
	subStateClosing = 1
	subStateClosed  = 2
)

type Subscription struct {
	Name  string
	Raw   *stomp.Subscription
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
	return s.Raw.Unsubscribe()
}

func (s *Subscription) ReadLoop(conn jobworker.Connector) {
	if s.queue == nil {
		s.queue = make(chan *jobworker.Job)
	}
	for {
		msg, ok := <-s.Raw.C
		if !ok {
			state := atomic.LoadInt32(&s.state)
			if state == subStateActive || state == subStateClosing {
				s.closeQueue()
			}
			return
		}
		// TODO remove
		printMsg(msg)
		s.queue <- newJob(s.Name, msg, conn)
	}
}

func (s *Subscription) closeQueue() {
	atomic.StoreInt32(&s.state, subStateClosed)
	close(s.queue)
}

func newJob(queue string, msg *stomp.Message, conn jobworker.Connector) *jobworker.Job {
	metadata := make(map[string]string, msg.Header.Len())
	for i := 0; i < len(metadata); i++ {
		k, v := msg.Header.GetAt(i)
		metadata[k] = v
	}
	job := jobworker.NewJob(
		queue,
		string(msg.Body),
		metadata,
		conn,
		msg,
	)
	return job
}

func printMsg(msg *stomp.Message) {
	fmt.Println("# ----------")

	for i := 0; i < msg.Header.Len(); i++ {
		k, v := msg.Header.GetAt(i)
		fmt.Println(k, ":", v)
	}

	fmt.Println("# ----------")

	fmt.Println("ContentType :", msg.ContentType)

	fmt.Println("# ----------")

	fmt.Println("Body :", string(msg.Body))

	fmt.Println("# ----------")

	fmt.Println("Destination :", msg.Destination)

	fmt.Println("# ----------")
}
