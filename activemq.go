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
	return &Connector{
		name:    connName,
		setting: s,
		retryer: er,
		conn:    conn,
	}, nil
}

func dial(network string, addr string, config *tls.Config, opts ...func(*stomp.Conn) error) (*stomp.Conn, error) {
	var conn *stomp.Conn
	if config != nil {
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

	conn *stomp.Conn

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
		s, err := c.conn.Subscribe(name, c.setting.AckMode)
		if err != nil {
			return nil, err
		}
		queue = &internal.Queue{Name: name, Subscription: s}
		c.name2Queue.Store(name, queue)
	}

	return queue.(*internal.Queue), nil
}

func (c *Connector) Subscribe(ctx context.Context, input *jobworker.SubscribeInput, opts ...func(*jobworker.Option)) (*jobworker.SubscribeOutput, error) {
	queue, err := c.resolveQueue(ctx, input.Queue)
	if err != nil {
		return nil, err
	}
	sub := &internal.Subscription{
		Name: queue.Name,
		Raw:  queue.Subscription,
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

		err = c.conn.Send(queue.Subscription.Destination(), contentType, []byte(input.Payload), sendOpts...)
		if err != nil {
			return err
		}
		return nil

	}, func(err error) bool {
		if !isInvalidStompConn(err) {
			return false
		}
		c.conn, err = dial(c.setting.Network, c.setting.Addr, c.setting.Config, c.setting.Opts...)
		if err != nil {
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
			if c.setting.Persistent {
				sendOpts = append(sendOpts, stomp.SendOpt.Header("persistent", "true"))
			}

			var contentType string
			if v, ok := opt.Metadata[frame.ContentType]; ok {
				contentType = v
			}

			err = c.conn.Send(queue.Subscription.Destination(), contentType, []byte(v), sendOpts...)
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

	return &jobworker.EnqueueBatchOutput{
		Successful: ids,
	}, nil

}

func (c *Connector) CompleteJob(ctx context.Context, input *jobworker.CompleteJobInput, opts ...func(*jobworker.Option)) (*jobworker.CompleteJobOutput, error) {

	_, err := c.retryer.Do(ctx, func(ctx context.Context) error {
		msg := input.Job.XXX_RawMsgPtr.(*stomp.Message)
		return c.conn.Ack(msg)
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
	msg := input.Job.XXX_RawMsgPtr.(*stomp.Message)
	err := c.conn.Nack(msg)
	if err != nil {
		return nil, err
	}
	return &jobworker.FailJobOutput{}, nil
}

func (c *Connector) Close() error {
	// TODO
	return c.conn.Disconnect()
}

type connProvider struct {
	conn *stomp.Conn
	mu   sync.Mutex
}

func (p *connProvider) Register(conn *stomp.Conn) {
	p.mu.Lock()
	p.conn = conn
	p.mu.Unlock()
}

func (p *connProvider) Get() *stomp.Conn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.conn
}

func (p *connProvider) Replace(newConn *stomp.Conn) {
	p.mu.Lock()
	conn := p.conn
	p.conn = newConn
	p.mu.Unlock()
	_ = conn.Disconnect()
}

func (c *Connector) reconnect() error {
	newConn, err := dial(c.setting.Network, c.setting.Addr, c.setting.Config, c.setting.Opts...)
	if err != nil {
		// add logging
		return err
	}

	// TODO
	_ = conn.Disconnect()
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
