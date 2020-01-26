package activemq

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-job-worker-development-kit/activemq-connector/internal"
	"github.com/go-job-worker-development-kit/jobworker"
	"github.com/go-stomp/stomp"
	"github.com/go-stomp/stomp/frame"
	uuid "github.com/satori/go.uuid"
	"sync"
)

const connName = "activemq"

type Setting struct {
	Network  string
	Addr string

}

func Open(conn *stomp.Conn, ackMode stomp.AckMode) *Connector {
	return &Connector{
		name:connName,
		conn:conn,
		ackMode:ackMode,
	}

}

type Connector struct {
	name       string
	ackMode stomp.AckMode

	conn *stomp.Conn

	activeMsgs sync.Map
	name2destination sync.Map

	loggerFunc jobworker.LoggerFunc
}

func (c *Connector) SetLoggerFunc(f jobworker.LoggerFunc) {
	c.loggerFunc = f
}

func (c *Connector) GetName() string {
	return c.name
}

func (c *Connector) resolveQueue(ctx context.Context, name string) (*internal.Queue, error) {
	queue, ok := c.name2destination.Load(name)
	if !ok || queue == nil {
		s, err := c.conn.Subscribe(name, c.ackMode)
		if err != nil {
			return nil, err
		}
		queue = &internal.Queue{Name:name, Subscription:s}
		c.name2destination.Store(name, queue)
	}

	return queue.(*internal.Queue), nil
}

func newJob(queue *internal.Queue, msg *stomp.Message, conn jobworker.Connector) *jobworker.Job {
	id := uuid.NewV4().String()
	class := msg.Header.Get("type")
	args := string(msg.Body)
	metadata := make(map[string]string, msg.Header.Len())
	for i := 0; i < len(metadata); i++ {
		k, v := msg.Header.GetAt(i)
		metadata[k] = v
	}
	job := jobworker.NewJob(
		queue.Name,
		id,
		class,
		args,
		metadata,
		conn,
		)
	return job
}

func (c *Connector) ReceiveJobs(ctx context.Context, ch chan<- *jobworker.Job, input *jobworker.ReceiveJobsInput, opts ...func(*jobworker.Option)) (*jobworker.ReceiveJobsOutput, error) {
	queue, err := c.resolveQueue(ctx, input.Queue)
	if err != nil {
		return nil, err
	}

	msg, err := queue.Subscription.Read()
	if err != nil {
		return nil, err
	}

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


	job := newJob(queue, msg, c)
	c.activeMsgs.Store(job.ID, msg)
	ch <- job

	fmt.Println("done!!!!!")


	return &jobworker.ReceiveJobsOutput{
		NoJob:false,
	}, nil
}


func (c *Connector) EnqueueJob(ctx context.Context, input *jobworker.EnqueueJobInput, opts ...func(*jobworker.Option)) (*jobworker.EnqueueJobOutput, error) {
	queue, err := c.resolveQueue(ctx, input.Queue)
	if err != nil {
		// TODO
		return nil, err
	}

	var opt jobworker.Option
	opt.ApplyOptions(opts...)

	var sendOpts []func(*frame.Frame) error
	if opt.Metadata != nil {
		for k, v := range opt.Metadata {
			sendOpts = append(sendOpts, stomp.SendOpt.Header(k, v))
		}
	}
	sendOpts = append(sendOpts, stomp.SendOpt.Header("type", input.Payload.Class))
	sendOpts = append(sendOpts, stomp.SendOpt.Header("persistent", "true"))

	var contentType string
	if v, ok := opt.Metadata[frame.ContentType]; ok {
		contentType = v
	}

	err = c.conn.Send(queue.Subscription.Destination(), contentType, []byte(input.Payload.Args), sendOpts...)
	if err != nil {
		return nil, err
	}

	return &jobworker.EnqueueJobOutput{}, nil
}

func (c *Connector) EnqueueJobBatch(ctx context.Context, input *jobworker.EnqueueJobBatchInput, opts ...func(*jobworker.Option)) (*jobworker.EnqueueJobBatchOutput, error) {
	queue, err := c.resolveQueue(ctx, input.Queue)
	if err != nil {
		// TODO
		return nil, err
	}

	var opt jobworker.Option
	opt.ApplyOptions(opts...)

	err = withTransaction(c.conn, func(tx *stomp.Transaction) error {

		for _, v := range input.Id2Payload {
			var sendOpts []func(*frame.Frame) error
			if opt.Metadata != nil {
				for k, v := range opt.Metadata {
					sendOpts = append(sendOpts, stomp.SendOpt.Header(k, v))
				}
			}
			sendOpts = append(sendOpts, stomp.SendOpt.Header("type", v.Class))
			sendOpts = append(sendOpts, stomp.SendOpt.Header("persistent", "true"))

			var contentType string
			if v, ok := opt.Metadata[frame.ContentType]; ok {
				contentType = v
			}

			err = c.conn.Send(queue.Subscription.Destination(), contentType, []byte(v.Args), sendOpts...)
			if err != nil {
				return err
			}
		}

		return nil

	})


	if err != nil {
		return nil, err
	}

	var ids []string
	for id := range input.Id2Payload {
		ids = append(ids, id)
	}

	return &jobworker.EnqueueJobBatchOutput{
		Successful: ids,
	}, nil

}

func withTransaction(conn *stomp.Conn, ope func(tx *stomp.Transaction) error) (err error) {
	tx := conn.Begin()

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Abort()
			panic(p)
		} else if err != nil {
			_ = tx.Abort()
		} else {
			err = tx.Commit()
		}
	}()
	err = ope(tx)
	return
}

func (c *Connector) CompleteJob(ctx context.Context, input *jobworker.CompleteJobInput, opts ...func(*jobworker.Option)) (*jobworker.CompleteJobOutput, error) {
	v, ok := c.activeMsgs.Load(input.Job.ID)
	if !ok {
		// TODO
		return nil, nil
	}
	msg := v.(*stomp.Message)
	err := c.conn.Ack(msg)
	if err != nil {
		// TODO
	}
	return nil, nil
}

func (c *Connector) FailJob(ctx context.Context, input *jobworker.FailJobInput, opts ...func(*jobworker.Option)) (*jobworker.FailJobOutput, error) {
	v, ok := c.activeMsgs.Load(input.Job.ID)
	if !ok {
		// TODO
		return nil, nil
	}
	msg := v.(*stomp.Message)
	err := c.conn.Nack(msg)
	if err != nil {
		// TODO
	}
	return nil, nil
}

func (c *Connector) Close() error {
	_ = c.conn.Disconnect()
}
