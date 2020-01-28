package activemq

import (
	"context"
	"crypto/tls"
	"sync"

	"github.com/vvatanabe/goretryer/exponential"

	"github.com/go-job-worker-development-kit/activemq-connector/internal"
	"github.com/go-job-worker-development-kit/jobworker"
	"github.com/go-stomp/stomp"
	"github.com/go-stomp/stomp/frame"
)

const connName = "activemq"

type Setting struct {
	Network       string
	Addr          string
	Config        *tls.Config // tls only
	Opts          []func(*stomp.Conn) error
	AckMode       stomp.AckMode
	Persistent    bool
	NumMaxRetries int
	LoggerFunc    jobworker.LoggerFunc
}

func Open(s *Setting) (*Connector, error) {
	conn, err := dial(s.Network, s.Addr, s.Config, s.Opts...)
	if err != nil {
		return nil, err
	}
	var er exponential.Retryer
	if s.NumMaxRetries != 0 {
		er.NumMaxRetries = s.NumMaxRetries
	}

	var provider internal.ConnProvider
	provider.SetLoggerFunc(s.LoggerFunc)
	provider.Register(conn)

	return &Connector{
		name:     connName,
		setting:  s,
		retryer:  er,
		provider: provider,
	}, nil
}

func dial(network string, addr string, config *tls.Config, opts ...func(*stomp.Conn) error) (*stomp.Conn, error) {
	var conn *stomp.Conn
	if config == nil {
		stompConn, err := stomp.Dial(network, addr, opts...)
		if err != nil {
			return nil, err
		}
		conn = stompConn
	} else {
		netConn, err := tls.Dial(network, addr, config)
		if err != nil {
			return nil, err
		}
		stompConn, err := stomp.Connect(netConn, opts...)
		if err != nil {
			_ = netConn.Close()
			return nil, err
		}
		conn = stompConn
	}
	return conn, nil
}

type Connector struct {
	name    string
	setting *Setting

	retryer exponential.Retryer

	provider internal.ConnProvider

	name2Queue sync.Map

	loggerFunc jobworker.LoggerFunc

	mu sync.Mutex
}

func (c *Connector) SetLoggerFunc(f jobworker.LoggerFunc) {
	c.loggerFunc = f
}

func (c *Connector) GetName() string {
	return c.name
}

func (c *Connector) resolveQueue(ctx context.Context, name string) (*internal.Queue, error) {
	queue, ok := c.name2Queue.Load(name)
	if !ok || queue == nil {
		s, err := c.provider.Conn().Subscribe(name, c.setting.AckMode)
		if err != nil {
			return nil, err
		}
		queue = &internal.Queue{Name: name, Subscription: s}
		c.name2Queue.Store(name, queue)
	}

	return queue.(*internal.Queue), nil
}

func (c *Connector) Subscribe(ctx context.Context, input *jobworker.SubscribeInput, opts ...func(*jobworker.Option)) (*jobworker.SubscribeOutput, error) {

	var sub *internal.Subscription
	_, err := c.retryer.Do(ctx, func(ctx context.Context) error {

		queue, err := c.resolveQueue(ctx, input.Queue)
		if err != nil {
			return err
		}
		sub = internal.NewSubscription(queue.Name, queue.Subscription)

		return nil

	}, func(err error) bool {
		if !isInvalidStompConn(err) {
			return false
		}
		if err = c.reconnect(); err != nil {
			// add logging
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	go sub.ReadLoop(c)

	return &jobworker.SubscribeOutput{
		Subscription: sub,
	}, nil
}

func isInvalidStompConn(err error) bool {
	return err == stomp.ErrClosedUnexpectedly || err == stomp.ErrAlreadyClosed
}

func (c *Connector) Enqueue(ctx context.Context, input *jobworker.EnqueueInput, opts ...func(*jobworker.Option)) (*jobworker.EnqueueOutput, error) {

	var opt jobworker.Option
	opt.ApplyOptions(opts...)

	var sendOpts []func(*frame.Frame) error
	if opt.Metadata != nil {
		for k, v := range opt.Metadata {
			sendOpts = append(sendOpts, stomp.SendOpt.Header(k, v))
		}
	}
	if c.setting.Persistent {
		sendOpts = append(sendOpts, stomp.SendOpt.Header("persistent", "true"))
	}

	var contentType string
	if v, ok := opt.Metadata[frame.ContentType]; ok {
		contentType = v
	}

	_, err := c.retryer.Do(ctx, func(ctx context.Context) error {

		queue, err := c.resolveQueue(ctx, input.Queue)
		if err != nil {
			// TODO
			return err
		}

		err = c.provider.Conn().Send(queue.Subscription.Destination(), contentType, []byte(input.Payload), sendOpts...)
		if err != nil {
			return err
		}
		return nil

	}, func(err error) bool {
		if !isInvalidStompConn(err) {
			return false
		}
		if err = c.reconnect(); err != nil {
			// add logging
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return &jobworker.EnqueueOutput{}, nil
}

func (c *Connector) EnqueueBatch(ctx context.Context, input *jobworker.EnqueueBatchInput, opts ...func(*jobworker.Option)) (*jobworker.EnqueueBatchOutput, error) {

	var opt jobworker.Option
	opt.ApplyOptions(opts...)

	_, err := c.retryer.Do(ctx, func(ctx context.Context) error {

		queue, err := c.resolveQueue(ctx, input.Queue)
		if err != nil {
			// TODO
			return err
		}

		return withTransaction(c.provider.Conn(), func(tx *stomp.Transaction) error {

			for _, v := range input.Id2Payload {
				var sendOpts []func(*frame.Frame) error
				if opt.Metadata != nil {
					for k, v := range opt.Metadata {
						sendOpts = append(sendOpts, stomp.SendOpt.Header(k, v))
					}
				}
				if c.setting.Persistent {
					sendOpts = append(sendOpts, stomp.SendOpt.Header("persistent", "true"))
				}

				var contentType string
				if v, ok := opt.Metadata[frame.ContentType]; ok {
					contentType = v
				}

				err = tx.Send(queue.Subscription.Destination(), contentType, []byte(v), sendOpts...)
				if err != nil {
					return err
				}
			}

			return nil

		})

	}, func(err error) bool {
		if !isInvalidStompConn(err) {
			return false
		}
		if err = c.reconnect(); err != nil {
			// add logging
			return false
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	var ids []string
	for id := range input.Id2Payload {
		ids = append(ids, id)
	}

	return &jobworker.EnqueueBatchOutput{
		Successful: ids,
	}, nil

}

func (c *Connector) CompleteJob(ctx context.Context, input *jobworker.CompleteJobInput, opts ...func(*jobworker.Option)) (*jobworker.CompleteJobOutput, error) {
	_, err := c.retryer.Do(ctx, func(ctx context.Context) error {
		msg := input.Job.XXX_RawMsgPtr.(*stomp.Message)
		return c.provider.Conn().Ack(msg)
	}, func(err error) bool {
		if !isInvalidStompConn(err) {
			return false
		}
		if err = c.reconnect(); err != nil {
			// add logging
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return &jobworker.CompleteJobOutput{}, nil
}

func (c *Connector) FailJob(ctx context.Context, input *jobworker.FailJobInput, opts ...func(*jobworker.Option)) (*jobworker.FailJobOutput, error) {
	_, err := c.retryer.Do(ctx, func(ctx context.Context) error {
		msg := input.Job.XXX_RawMsgPtr.(*stomp.Message)
		return c.provider.Conn().Nack(msg)
	}, func(err error) bool {
		if !isInvalidStompConn(err) {
			return false
		}
		if err = c.reconnect(); err != nil {
			// add logging
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return &jobworker.FailJobOutput{}, nil
}

func (c *Connector) Close() error {
	return c.provider.Conn().Disconnect()
}

func (c *Connector) reconnect() error {
	newConn, err := dial(c.setting.Network, c.setting.Addr, c.setting.Config, c.setting.Opts...)
	if err != nil {
		// TODO add logging
		return err
	}
	c.provider.Replace(newConn)
	return nil
}

func withTransaction(conn *stomp.Conn, ope func(tx *stomp.Transaction) error) (err error) {
	tx, err := conn.BeginWithError()
	if err != nil {
		return err
	}
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
