package internal

import (
	"sync"
	"time"

	"github.com/go-stomp/stomp"
)

type ConnProvider struct {
	conn *stomp.Conn
	mu   sync.RWMutex
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

func (p *ConnProvider) Replace(newConn *stomp.Conn, disconnectDelay time.Duration) {
	conn := p.conn

	p.mu.Lock()
	p.conn = newConn
	p.mu.Unlock()

	go func() {
		time.Sleep(disconnectDelay)
		_ = conn.Disconnect()
	}()
}
