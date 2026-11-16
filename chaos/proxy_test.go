package chaos

import (
	"io"
	"net"
	"sync"
)

// proxy forwards TCP connections from one listening address to a target. Block makes it refuse new
// connections and cut the ones it has, which is how the test partitions the network between two
// real processes without touching the processes themselves.
type proxy struct {
	ln     net.Listener
	target string

	mu      sync.Mutex
	blocked bool
	conns   map[net.Conn]struct{}
}

func newProxy(listen, target string) (*proxy, error) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	p := &proxy{ln: ln, target: target, conns: make(map[net.Conn]struct{})}
	go p.accept()
	return p, nil
}

func (p *proxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.blocked {
		return false
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *proxy) untrack(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
	c.Close()
}

func (p *proxy) handle(in net.Conn) {
	if !p.track(in) {
		in.Close()
		return
	}
	defer p.untrack(in)
	out, err := net.Dial("tcp", p.target)
	if err != nil {
		return
	}
	if !p.track(out) {
		out.Close()
		return
	}
	defer p.untrack(out)
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(out, in); done <- struct{}{} }()
	go func() { _, _ = io.Copy(in, out); done <- struct{}{} }()
	<-done
}

// SetBlocked turns the cut on or off. Turning it on closes every open connection.
func (p *proxy) SetBlocked(b bool) {
	p.mu.Lock()
	p.blocked = b
	var cut []net.Conn
	if b {
		for c := range p.conns {
			cut = append(cut, c)
		}
	}
	p.mu.Unlock()
	for _, c := range cut {
		c.Close()
	}
}

func (p *proxy) Close() {
	p.ln.Close()
	p.SetBlocked(true)
}
