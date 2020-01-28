package internal

import (
	"sync"
	"time"

	"github.com/go-job-worker-development-kit/jobworker"

	"github.com/go-stomp/stomp"
)

type ConnProvider struct {
	conn       *stomp.Conn
	mu         sync.RWMutex
	loggerFunc func(...interface{})
}

func (p *ConnProvider) SetLoggerFunc(f jobworker.LoggerFunc) {
	p.loggerFunc = f
}

func (p *ConnProvider) Register(conn *stomp.Conn) {
	p.mu.Lock()
	p.conn = conn
	p.mu.Unlock()
}

func (p *ConnProvider) Conn() *stomp.Conn {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.conn
}

func (p *ConnProvider) Replace(newConn *stomp.Conn) {
	conn := p.conn

	p.mu.Lock()
	p.conn = newConn
	p.mu.Unlock()

	go func() {
		time.Sleep(10 * time.Second)
		err := conn.Disconnect()
		if err != nil && p.loggerFunc != nil {
			p.loggerFunc("conn disconnect error:", err)
		}
	}()
}
