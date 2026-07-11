package executor

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
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

func TestCodexWebsocketLifecycleEndReleasesActiveReader(t *testing.T) {
	server, _ := newWebsocketTargetServer(t)
	defer server.Close()
	conn, _ := newCloseCountingWebsocketConn(t, "ws"+strings.TrimPrefix(server.URL, "http"))
	closer := newWebsocketConnectionCloser(conn)
	sess := &codexWebsocketSession{conn: conn, connCloser: closer, readerConn: conn}
	t.Cleanup(func() { closeCodexWebsocketSession(sess, "test_complete") })
	lifecycle := &countingWebsocketLifecycle{}
	if errBind := sess.bindExecutionLifecycle(cliproxyexecutor.Options{ExecutionLifecycle: lifecycle}, conn, closer, "gpt-5-codex"); errBind != nil {
		t.Fatalf("bind lifecycle: %v", errBind)
	}
	readCh := sess.activate(conn)
	_, activeDone := sess.activeForConn(conn)
	readerDone := make(chan struct{})
	executor := NewCodexWebsocketsExecutor(nil)
	go func() {
		defer close(readerDone)
		executor.readUpstreamLoop(sess, conn)
	}()

	lifecycle.End("execution_drained")
	select {
	case <-readerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream reader remained active after lifecycle ended")
	}
	select {
	case <-activeDone:
	default:
		t.Fatal("execution reader remained blocked after lifecycle closed its connection")
	}
	_, _, errRead := readCodexWebsocketMessage(context.Background(), sess, conn, readCh)
	if errRead == nil {
		t.Fatal("closed lifecycle must return a terminal read error")
	}
	var reset codexWebsocketUpstreamResetError
	if errors.As(errRead, &reset) {
		t.Fatalf("lifecycle termination returned a recoverable upstream reset: %v", errRead)
	}
}

func TestCodexWebsocketLifecycleReplacementPreservesActiveReader(t *testing.T) {
	server, _ := newWebsocketTargetServer(t)
	defer server.Close()
	conn, physical := newCloseCountingWebsocketConn(t, "ws"+strings.TrimPrefix(server.URL, "http"))
	closer := newWebsocketConnectionCloser(conn)
	sess := &codexWebsocketSession{conn: conn, connCloser: closer, readerConn: conn}
	t.Cleanup(func() { closeCodexWebsocketSession(sess, "test_complete") })
	previous := &countingWebsocketLifecycle{}
	if errBind := sess.bindExecutionLifecycle(cliproxyexecutor.Options{ExecutionLifecycle: previous}, conn, closer, "gpt-5-codex"); errBind != nil {
		t.Fatalf("bind previous lifecycle: %v", errBind)
	}
	readCh := sess.activate(conn)
	_, activeDone := sess.activeForConn(conn)
	current := &countingWebsocketLifecycle{}
	if errBind := sess.bindExecutionLifecycle(cliproxyexecutor.Options{ExecutionLifecycle: current}, conn, closer, "gpt-5-codex"); errBind != nil {
		t.Fatalf("bind current lifecycle: %v", errBind)
	}
	if activeCh, _ := sess.activeForConn(conn); activeCh != readCh {
		t.Fatal("lifecycle replacement must preserve the active reader")
	}
	select {
	case <-activeDone:
		t.Fatal("previous lifecycle ended the current reader")
	default:
	}
	if got := physical.closes.Load(); got != 0 {
		t.Fatalf("physical websocket closes after replacement = %d, want 0", got)
	}

	current.End("execution_drained")
	select {
	case <-activeDone:
	default:
		t.Fatal("current lifecycle must release its reader")
	}
	if got := physical.closes.Load(); got != 1 {
		t.Fatalf("physical websocket closes after drain = %d, want 1", got)
	}
}
