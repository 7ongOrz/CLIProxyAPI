package executor

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type countingWebsocketLifecycle struct {
	mu      sync.Mutex
	closeFn func() error
	once    sync.Once
	binds   atomic.Int32
}

func (l *countingWebsocketLifecycle) Bind(closeFn func() error) error {
	l.binds.Add(1)
	l.mu.Lock()
	l.closeFn = closeFn
	l.mu.Unlock()
	return nil
}

func (l *countingWebsocketLifecycle) End(string) {
	l.once.Do(func() {
		l.mu.Lock()
		closeFn := l.closeFn
		l.mu.Unlock()
		if closeFn != nil {
			_ = closeFn()
		}
	})
}

func TestCodexWebsocketSessionBindsSameLifecycleAndConnectionOnce(t *testing.T) {
	conn := &websocket.Conn{}
	closer := newWebsocketConnectionCloser(conn)
	sess := &codexWebsocketSession{conn: conn, connCloser: closer}
	lifecycle := &countingWebsocketLifecycle{}
	opts := cliproxyexecutor.Options{ExecutionLifecycle: lifecycle}

	if errBind := sess.bindExecutionLifecycle(opts, conn, closer, "gpt-5-codex"); errBind != nil {
		t.Fatalf("first bindExecutionLifecycle() error = %v", errBind)
	}
	if errBind := sess.bindExecutionLifecycle(opts, conn, closer, "gpt-5-codex"); errBind != nil {
		t.Fatalf("second bindExecutionLifecycle() error = %v", errBind)
	}
	if got := lifecycle.binds.Load(); got != 1 {
		t.Fatalf("lifecycle Bind calls = %d, want 1 for the same lifecycle and connection", got)
	}
}

func TestCodexWebsocketSessionLifecycleClosesRecoveredConnection(t *testing.T) {
	server, _ := newWebsocketTargetServer(t)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn1, physical1 := newCloseCountingWebsocketConn(t, wsURL)
	closer1 := newWebsocketConnectionCloser(conn1)
	sess := &codexWebsocketSession{
		sessionID:  "recovered-lifecycle",
		conn:       conn1,
		connCloser: closer1,
		readerConn: conn1,
		authID:     "auth-a",
		wsURL:      wsURL,
	}
	lifecycle := &countingWebsocketLifecycle{}
	opts := cliproxyexecutor.Options{ExecutionLifecycle: lifecycle}
	if errBind := sess.bindExecutionLifecycle(opts, conn1, closer1, "gpt-5-codex"); errBind != nil {
		t.Fatalf("bind first connection: %v", errBind)
	}

	executor := NewCodexWebsocketsExecutor(nil)
	if detached := executor.detachUpstreamConnForRecovery(sess, conn1, "test_recovery", errors.New("connection reset")); !detached {
		t.Fatal("detachUpstreamConnForRecovery() = false, want true")
	}
	if got := physical1.closes.Load(); got != 1 {
		t.Fatalf("first physical websocket closes = %d, want 1", got)
	}

	conn2, physical2 := newCloseCountingWebsocketConn(t, wsURL)
	closer2 := newWebsocketConnectionCloser(conn2)
	sess.connMu.Lock()
	sess.conn = conn2
	sess.connCloser = closer2
	sess.readerConn = conn2
	sess.connMu.Unlock()
	if errBind := sess.bindExecutionLifecycle(opts, conn2, closer2, "gpt-5-codex"); errBind != nil {
		t.Fatalf("bind recovered connection: %v", errBind)
	}
	if got := lifecycle.binds.Load(); got != 1 {
		t.Fatalf("lifecycle Bind calls = %d, want 1 across recovered connections", got)
	}

	lifecycle.End("test_complete")
	if got := physical2.closes.Load(); got != 1 {
		t.Fatalf("recovered physical websocket closes = %d, want 1", got)
	}
	sess.connMu.Lock()
	defer sess.connMu.Unlock()
	if sess.conn != nil || sess.connCloser != nil || sess.lifecycle != nil {
		t.Fatalf("closed session state = conn:%v closer:%v lifecycle:%v, want detached", sess.conn, sess.connCloser, sess.lifecycle)
	}
}
