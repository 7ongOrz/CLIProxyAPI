package executor

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

type codexWebsocketSessionStore struct {
	mu       sync.Mutex
	sessions map[string]*codexWebsocketSession
}

var globalCodexWebsocketSessionStore = &codexWebsocketSessionStore{
	sessions: make(map[string]*codexWebsocketSession),
}

type websocketConnectionCloser struct {
	conn *websocket.Conn
	once sync.Once
	err  error
}

func newWebsocketConnectionCloser(conn *websocket.Conn) *websocketConnectionCloser {
	if conn == nil {
		return nil
	}
	return &websocketConnectionCloser{conn: conn}
}

func (c *websocketConnectionCloser) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	c.once.Do(func() {
		c.err = c.conn.Close()
	})
	return c.err
}

type codexWebsocketSession struct {
	sessionID string

	reqMu sync.Mutex

	connMu                    sync.Mutex
	conn                      *websocket.Conn
	connCloser                *websocketConnectionCloser
	wsURL                     string
	authID                    string
	proxyURL                  string
	useSIWC                   bool
	connCfg                   *config.Config
	hasConnCfg                bool
	multiAgentV2OptimizedConn *websocket.Conn
	pendingHandshakeConn      *websocket.Conn
	pendingHandshakeHeaders   http.Header
	lifecycleBindMu           sync.Mutex
	lifecycle                 cliproxyexecutor.ExecutionLifecycle
	lifecycleModel            string

	writeMu sync.Mutex

	activeMu        sync.Mutex
	activeCh        chan codexWebsocketRead
	activeConn      *websocket.Conn
	activeDone      <-chan struct{}
	activeCancel    context.CancelFunc
	activeClosedCh  chan codexWebsocketRead
	activeClosedErr error
	terminalConn    *websocket.Conn
	terminalErr     error
	sessionErr      error

	responseStateConn   *websocket.Conn
	terminalResponseIDs map[string]struct{}
	// activeResponseID is the newest response known to run, or to have run,
	// on responseStateConn. The read loop sets it on response.created and on
	// the terminal event of that same response, before the payload is
	// delivered downstream. activeResponseTerminal then tells an interrupt
	// snapshot that the turn already ended even while its read channel is
	// still awaiting teardown. A duplex socket may also reject a queued
	// create while another response runs; only the matching id ends the
	// active response. An empty id means no response.created was seen yet.
	activeResponseID       string
	activeResponseTerminal bool
	interruptAckHandler    func([]byte)

	readerConn *websocket.Conn

	upstreamGeneration           uint64
	upstreamDisconnectGeneration uint64
	upstreamDisconnectCh         chan error
	upstreamDisconnectErrMu      sync.RWMutex
	upstreamDisconnectErrConn    *websocket.Conn
	upstreamDisconnectErr        error

	lastEventMu   sync.Mutex
	lastEventConn *websocket.Conn
	lastEventType string
}

type codexWebsocketRead struct {
	conn    *websocket.Conn
	msgType int
	payload []byte
	err     error
}

type websocketUpstreamDisconnectEvent struct {
	cause      error
	generation uint64
}

func (e websocketUpstreamDisconnectEvent) Error() string {
	if e.cause == nil {
		return "upstream websocket disconnected"
	}
	return e.cause.Error()
}

func (e websocketUpstreamDisconnectEvent) Unwrap() error {
	return e.cause
}

func (e websocketUpstreamDisconnectEvent) UpstreamDisconnectGeneration() uint64 {
	return e.generation
}

type codexWebsocketUpstreamResetError struct {
	cause error
}

func (e codexWebsocketUpstreamResetError) Error() string {
	if e.cause == nil {
		return "codex websockets executor: upstream websocket reset"
	}
	return fmt.Sprintf("codex websockets executor: upstream websocket reset: %v", e.cause)
}

func (e codexWebsocketUpstreamResetError) Unwrap() error { return e.cause }

func (s *codexWebsocketSession) setActiveLocked(conn *websocket.Conn, ch chan codexWebsocketRead) {
	if s.activeCancel != nil {
		s.activeCancel()
		s.activeCancel = nil
		s.activeDone = nil
	}
	s.activeClosedCh = nil
	s.activeClosedErr = nil
	s.activeCh = ch
	s.activeResponseID = ""
	s.activeResponseTerminal = false
	s.activeConn = conn
	if conn != nil && ch != nil {
		activeCtx, activeCancel := context.WithCancel(context.Background())
		s.activeDone = activeCtx.Done()
		s.activeCancel = activeCancel
	}
}

func (s *codexWebsocketSession) activate(conn *websocket.Conn) chan codexWebsocketRead {
	if s == nil || conn == nil {
		return nil
	}
	ch := make(chan codexWebsocketRead, 4096)
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if s.terminalConn == conn && s.terminalErr != nil {
		ch <- codexWebsocketRead{conn: conn, err: s.terminalErr}
		close(ch)
		return ch
	}
	s.setActiveLocked(conn, ch)
	return ch
}

func (s *codexWebsocketSession) resetTerminalError(conn *websocket.Conn) {
	if s == nil || conn == nil {
		return
	}
	s.activeMu.Lock()
	s.terminalConn = conn
	s.terminalErr = nil
	if s.responseStateConn != conn {
		s.responseStateConn = conn
		s.terminalResponseIDs = make(map[string]struct{})
		s.interruptAckHandler = nil
	}
	s.activeMu.Unlock()
}

func (s *codexWebsocketSession) markTerminalError(conn *websocket.Conn, err error) bool {
	if s == nil || conn == nil || err == nil {
		return false
	}
	s.connMu.Lock()
	if s.conn != conn {
		s.connMu.Unlock()
		return false
	}
	s.activeMu.Lock()
	s.terminalConn = conn
	s.terminalErr = err
	s.activeMu.Unlock()
	s.connMu.Unlock()
	return true
}

func (s *codexWebsocketSession) storeHandshakeHeadersForReplay(conn *websocket.Conn, headers http.Header) {
	if s == nil || conn == nil || len(headers) == 0 {
		return
	}
	s.connMu.Lock()
	if s.conn == conn {
		s.pendingHandshakeConn = conn
		s.pendingHandshakeHeaders = headers.Clone()
	}
	s.connMu.Unlock()
}

func (s *codexWebsocketSession) takeHandshakeHeadersForReplay(conn *websocket.Conn) http.Header {
	if s == nil || conn == nil {
		return nil
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn != conn || s.pendingHandshakeConn != conn {
		return nil
	}
	headers := s.pendingHandshakeHeaders
	s.pendingHandshakeConn = nil
	s.pendingHandshakeHeaders = nil
	return headers
}

func (s *codexWebsocketSession) activeForConn(conn *websocket.Conn) (chan codexWebsocketRead, <-chan struct{}) {
	if s == nil || conn == nil {
		return nil, nil
	}
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if s.activeConn != conn {
		return nil, nil
	}
	return s.activeCh, s.activeDone
}

func clearRetryActiveState(sess *codexWebsocketSession, conn *websocket.Conn, ch chan codexWebsocketRead) bool {
	if sess == nil {
		return false
	}
	return sess.clearActive(conn, ch)
}

func (s *codexWebsocketSession) clearActive(conn *websocket.Conn, ch chan codexWebsocketRead) bool {
	if s == nil {
		return false
	}
	s.activeMu.Lock()
	cleared := false
	if s.activeConn == conn && s.activeCh == ch {
		s.activeCh = nil
		s.activeConn = nil
		if s.activeCancel != nil {
			s.activeCancel()
		}
		s.activeCancel = nil
		s.activeDone = nil
		cleared = true
	}
	if s.activeClosedCh == ch {
		s.activeClosedCh = nil
		s.activeClosedErr = nil
	}
	s.activeMu.Unlock()
	return cleared
}

func (s *codexWebsocketSession) activeDoneFor(ch chan codexWebsocketRead) (<-chan struct{}, bool) {
	if s == nil || ch == nil {
		return nil, false
	}
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if s.activeCh != ch {
		return nil, false
	}
	return s.activeDone, true
}

func (s *codexWebsocketSession) closedActiveErrorFor(ch chan codexWebsocketRead) error {
	if s == nil || ch == nil {
		return nil
	}
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if s.activeClosedCh != ch {
		return nil
	}
	return s.activeClosedErr
}

func (s *codexWebsocketSession) sessionError() error {
	if s == nil {
		return nil
	}
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	return s.sessionErr
}

func (s *codexWebsocketSession) markSessionError(err error) {
	if s == nil || err == nil {
		return
	}
	s.activeMu.Lock()
	s.sessionErr = err
	if s.activeClosedCh != nil {
		s.activeClosedErr = err
	}
	s.activeMu.Unlock()
}

func (s *codexWebsocketSession) closeActiveReadForConn(conn *websocket.Conn, err error) bool {
	if s == nil || conn == nil {
		return false
	}
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if s.activeConn != conn || s.activeCh == nil {
		return false
	}
	ch := s.activeCh
	s.activeCh = nil
	s.activeResponseID = ""
	s.activeResponseTerminal = false
	s.activeConn = nil
	s.activeClosedCh = ch
	s.activeClosedErr = err
	if s.activeCancel != nil {
		s.activeCancel()
	}
	s.activeCancel = nil
	s.activeDone = nil
	select {
	case ch <- codexWebsocketRead{conn: conn, err: err}:
	default:
	}
	return true
}

const codexWebsocketWriteChunkSize = 32 * 1024

var (
	testWebsocketWritePayloadHook func(conn *websocket.Conn)
	testWebsocketWriteChunkHook   func(chunkIndex int, totalChunks int)
)

func (s *codexWebsocketSession) closeActiveRead(err error) {
	if s == nil {
		return
	}
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if s.activeCh == nil {
		return
	}
	ch := s.activeCh
	conn := s.activeConn
	s.activeCh = nil
	s.activeConn = nil
	s.activeClosedCh = ch
	s.activeClosedErr = err
	if s.activeCancel != nil {
		s.activeCancel()
	}
	s.activeCancel = nil
	s.activeDone = nil
	select {
	case ch <- codexWebsocketRead{conn: conn, err: err}:
	default:
	}
}

func (s *codexWebsocketSession) writeCodexMessage(conn *websocket.Conn, msgType int, payload []byte) error {
	if s == nil {
		return fmt.Errorf("codex websockets executor: session is nil")
	}
	if conn == nil {
		return fmt.Errorf("codex websockets executor: websocket conn is nil")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	netConn := conn.UnderlyingConn()
	if netConn != nil {
		// Keep the active read alive while the request is being written.
		_ = netConn.SetReadDeadline(time.Now().Add(codexResponsesWebsocketIdleTimeout))
	}
	if err := s.writeMessageLocked(conn, msgType, payload); err != nil {
		return err
	}
	if netConn != nil {
		// Give the response a full idle window after the write completes.
		_ = netConn.SetReadDeadline(time.Now().Add(codexResponsesWebsocketIdleTimeout))
	}
	return nil
}

func (s *codexWebsocketSession) writeMessage(conn *websocket.Conn, msgType int, payload []byte) error {
	if s == nil {
		return fmt.Errorf("codex websockets executor: session is nil")
	}
	if conn == nil {
		return fmt.Errorf("codex websockets executor: websocket conn is nil")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeMessageLocked(conn, msgType, payload)
}

func (s *codexWebsocketSession) writeMessageLocked(conn *websocket.Conn, msgType int, payload []byte) error {
	if testWebsocketWritePayloadHook != nil {
		testWebsocketWritePayloadHook(conn)
	}
	if len(payload) <= codexWebsocketWriteChunkSize {
		return conn.WriteMessage(msgType, payload)
	}
	w, errNext := conn.NextWriter(msgType)
	if errNext != nil {
		return errNext
	}
	totalChunks := (len(payload) + codexWebsocketWriteChunkSize - 1) / codexWebsocketWriteChunkSize
	for i := 0; i < len(payload); i += codexWebsocketWriteChunkSize {
		end := i + codexWebsocketWriteChunkSize
		if end > len(payload) {
			end = len(payload)
		}
		chunkIdx := i / codexWebsocketWriteChunkSize
		if testWebsocketWriteChunkHook != nil {
			testWebsocketWriteChunkHook(chunkIdx, totalChunks)
		}
		if _, errWrite := w.Write(payload[i:end]); errWrite != nil {
			_ = w.Close()
			return errWrite
		}
	}
	return w.Close()
}

func (s *codexWebsocketSession) setMultiAgentV2Optimized(conn *websocket.Conn, optimized bool) {
	if s == nil || conn == nil {
		return
	}
	s.connMu.Lock()
	if s.conn == conn {
		if optimized {
			s.multiAgentV2OptimizedConn = conn
		} else {
			s.multiAgentV2OptimizedConn = nil
		}
	}
	s.connMu.Unlock()
}

func (s *codexWebsocketSession) isMultiAgentV2Optimized(conn *websocket.Conn) bool {
	if s == nil || conn == nil {
		return false
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.conn == conn && s.multiAgentV2OptimizedConn == conn
}

// sendTerminalWebsocketRead reports whether it invalidated a full channel's connection before waiting.
func sendTerminalWebsocketRead(ch chan<- codexWebsocketRead, done <-chan struct{}, event codexWebsocketRead, invalidate func()) bool {
	select {
	case ch <- event:
		return false
	case <-done:
		return false
	default:
	}

	invalidated := invalidate != nil
	if invalidated {
		invalidate()
	}
	select {
	case ch <- event:
	case <-done:
	}
	return invalidated
}

func (s *codexWebsocketSession) configureConn(conn *websocket.Conn) {
	if s == nil || conn == nil {
		return
	}
	s.resetTerminalError(conn)
	s.resetUpstreamDisconnectError(conn)
	conn.SetPingHandler(func(appData string) error {
		sessionID := ""
		if s != nil {
			sessionID = s.sessionID
		}
		sessionKind := sessionObjectKind(s)
		log.Debugf("codex websockets: upstream ping received session=%s session_object=%s ping_bytes=%d", sessionID, sessionKind, len(appData))
		log.Debugf("codex websockets: upstream pong write started session=%s session_object=%s", sessionID, sessionKind)
		start := time.Now()
		// Gorilla websocket allows concurrent WriteControl with WriteMessage.
		// Avoid writeMu here so keepalive pongs are not starved by long payload writes.
		errPong := conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
		if errPong != nil {
			log.Warnf("codex websockets: upstream pong write failed session=%s session_object=%s duration=%v err=%v", sessionID, sessionKind, time.Since(start), errPong)
		} else {
			log.Debugf("codex websockets: upstream pong replied session=%s session_object=%s duration=%v", sessionID, sessionKind, time.Since(start))
		}
		return errPong
	})
	defaultCloseHandler := conn.CloseHandler()
	conn.SetCloseHandler(func(code int, text string) error {
		s.setUpstreamDisconnectError(conn, &websocket.CloseError{Code: code, Text: text})
		return defaultCloseHandler(code, text)
	})
}

func (s *codexWebsocketSession) bindExecutionLifecycle(opts cliproxyexecutor.Options, conn *websocket.Conn, closer *websocketConnectionCloser, model string) error {
	if closer == nil {
		return fmt.Errorf("codex websockets executor: websocket connection closer is nil")
	}
	if s == nil {
		return cliproxyexecutor.BindExecutionResource(opts, closer)
	}
	lifecycle := opts.ExecutionLifecycle
	if lifecycle == nil || conn == nil {
		return nil
	}

	s.lifecycleBindMu.Lock()
	defer s.lifecycleBindMu.Unlock()

	s.connMu.Lock()
	if s.conn == conn && s.connCloser == nil {
		s.connCloser = closer
	}
	if s.conn != conn || s.connCloser != closer {
		s.connMu.Unlock()
		return fmt.Errorf("codex websockets executor: websocket connection changed during lifecycle bind")
	}
	if s.lifecycle == lifecycle {
		s.connMu.Unlock()
		return nil
	}
	previous := s.lifecycle
	s.lifecycle = lifecycle
	s.lifecycleModel = strings.TrimSpace(model)
	s.connMu.Unlock()

	if errBind := lifecycle.Bind(func() error {
		return s.closeBoundLifecycle(lifecycle)
	}); errBind != nil {
		s.connMu.Lock()
		if s.lifecycle == lifecycle {
			s.lifecycle = nil
			s.lifecycleModel = ""
		}
		s.connMu.Unlock()
		if previous != nil && previous != lifecycle {
			previous.End("target_replaced")
		}
		return errBind
	}
	if retained, ok := lifecycle.(interface{ Retain() }); ok {
		retained.Retain()
	}

	s.connMu.Lock()
	if s.conn != conn || s.connCloser != closer || s.lifecycle != lifecycle {
		s.connMu.Unlock()
		if previous != nil && previous != lifecycle {
			previous.End("target_replaced")
		}
		return fmt.Errorf("codex websockets executor: websocket connection closed during lifecycle bind")
	}
	s.connMu.Unlock()
	if previous != nil && previous != lifecycle {
		previous.End("target_replaced")
	}
	return nil
}

func (s *codexWebsocketSession) closeBoundLifecycle(lifecycle cliproxyexecutor.ExecutionLifecycle) error {
	s.connMu.Lock()
	if s.lifecycle != lifecycle {
		s.connMu.Unlock()
		go lifecycle.End("connection_closed")
		return nil
	}
	conn := s.conn
	closer := s.connCloser
	s.lifecycle = nil
	s.lifecycleModel = ""
	s.conn = nil
	s.connCloser = nil
	s.connCfg = nil
	s.hasConnCfg = false
	if s.readerConn == conn {
		s.readerConn = nil
	}
	if s.pendingHandshakeConn == conn {
		s.pendingHandshakeConn = nil
		s.pendingHandshakeHeaders = nil
	}
	s.connMu.Unlock()

	s.closeActiveReadForConn(conn, fmt.Errorf("websockets executor: execution lifecycle ended"))
	var errClose error
	if closer != nil {
		errClose = closer.Close()
	}
	go lifecycle.End("connection_closed")
	return errClose
}

func (s *codexWebsocketSession) detachConnection(conn *websocket.Conn) {
	if s == nil || conn == nil {
		return
	}
	s.connMu.Lock()
	if s.conn == conn {
		s.conn = nil
		s.connCloser = nil
		s.connCfg = nil
		s.hasConnCfg = false
		s.multiAgentV2OptimizedConn = nil
		if s.readerConn == conn {
			s.readerConn = nil
		}
		if s.pendingHandshakeConn == conn {
			s.pendingHandshakeConn = nil
			s.pendingHandshakeHeaders = nil
		}
		s.lifecycle = nil
		s.lifecycleModel = ""
	}
	s.connMu.Unlock()
}

func closeWebsocketAfterBindFailure(sess *codexWebsocketSession, conn *websocket.Conn, closer *websocketConnectionCloser) {
	if conn == nil || closer == nil {
		return
	}
	if sess != nil {
		sess.detachConnection(conn)
	}
	if errClose := closer.Close(); errClose != nil {
		log.Errorf("websockets executor: close lifecycle bind failure connection error: %v", errClose)
	}
}

func websocketSessionTargetChanged(sess *codexWebsocketSession, authID string, wsURL string, proxyURL string, useSIWC ...bool) bool {
	if sess == nil {
		return false
	}

	sess.connMu.Lock()
	defer sess.connMu.Unlock()
	if strings.TrimSpace(sess.authID) == "" && strings.TrimSpace(sess.wsURL) == "" {
		return false
	}
	return !websocketSessionTargetMatches(sess, authID, wsURL, proxyURL, useSIWC...)
}

func existingWebsocketSessionConn(sess *codexWebsocketSession, authID string, wsURL string, proxyURL string, useSIWC ...bool) (*websocket.Conn, *websocketConnectionCloser) {
	if sess == nil {
		return nil, nil
	}
	sess.connMu.Lock()
	conn := sess.conn
	closer := sess.connCloser
	matches := conn != nil && closer != nil && websocketSessionTargetMatches(sess, authID, wsURL, proxyURL, useSIWC...)
	sess.connMu.Unlock()
	if !matches || sess.upstreamDisconnectError(conn) != nil {
		return nil, nil
	}
	return conn, closer
}

func (s *codexWebsocketSession) effectiveConfig(authID string, fallback *config.Config) *config.Config {
	if s == nil {
		return fallback
	}
	s.connMu.Lock()
	sameConn := s.conn != nil && s.hasConnCfg && strings.TrimSpace(s.authID) == strings.TrimSpace(authID)
	cfg := s.connCfg
	s.connMu.Unlock()
	if sameConn {
		return cfg
	}
	return fallback
}

func websocketSessionTargetMatches(sess *codexWebsocketSession, authID string, wsURL string, proxyURL string, useSIWC ...bool) bool {
	if len(useSIWC) > 0 && sess.useSIWC != useSIWC[0] {
		return false
	}
	return strings.TrimSpace(sess.authID) == strings.TrimSpace(authID) &&
		strings.TrimSpace(sess.wsURL) == strings.TrimSpace(wsURL) &&
		strings.TrimSpace(sess.proxyURL) == strings.TrimSpace(proxyURL)
}

func detachMismatchedWebsocketSessionConn(sess *codexWebsocketSession, authID string, wsURL string, proxyURL string, useSIWC ...bool) (*websocket.Conn, *websocketConnectionCloser, string, string, cliproxyexecutor.ExecutionLifecycle) {
	if sess == nil {
		return nil, nil, "", "", nil
	}

	sess.connMu.Lock()
	defer sess.connMu.Unlock()
	conn := sess.conn
	if conn == nil || websocketSessionTargetMatches(sess, authID, wsURL, proxyURL, useSIWC...) {
		return nil, nil, "", "", nil
	}

	previousAuthID := sess.authID
	previousWSURL := sess.wsURL
	lifecycle := sess.lifecycle
	closer := sess.connCloser
	sess.lifecycle = nil
	sess.lifecycleModel = ""
	sess.conn = nil
	sess.connCloser = nil
	sess.connCfg = nil
	sess.hasConnCfg = false
	sess.useSIWC = false
	sess.multiAgentV2OptimizedConn = nil
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	if sess.pendingHandshakeConn == conn {
		sess.pendingHandshakeConn = nil
		sess.pendingHandshakeHeaders = nil
	}
	return conn, closer, previousAuthID, previousWSURL, lifecycle
}

func configureRawCodexWebsocketConn(conn *websocket.Conn, authID string, wsURL string) {
	if conn == nil {
		return
	}
	conn.SetPingHandler(func(appData string) error {
		log.Debugf("codex websockets: upstream ping received session= session_object=none ping_bytes=%d", len(appData))
		log.Debugf("codex websockets: upstream pong write started session= session_object=none")
		start := time.Now()
		errPong := conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
		if errPong != nil {
			log.Warnf("codex websockets: upstream pong write failed session= session_object=none duration=%v err=%v", time.Since(start), errPong)
		} else {
			log.Debugf("codex websockets: upstream pong replied session= session_object=none duration=%v", time.Since(start))
		}
		return errPong
	})
}

func (s *codexWebsocketSession) setLastEventType(conn *websocket.Conn, eventType string) {
	if s == nil || conn == nil || eventType == "" {
		return
	}
	s.lastEventMu.Lock()
	if s.lastEventConn == conn {
		s.lastEventType = eventType
	}
	s.lastEventMu.Unlock()
}

func (s *codexWebsocketSession) getLastEventType(conn *websocket.Conn) string {
	if s == nil || conn == nil {
		return ""
	}
	s.lastEventMu.Lock()
	defer s.lastEventMu.Unlock()
	if s.lastEventConn != conn {
		return ""
	}
	return s.lastEventType
}

func newEphemeralCodexWebsocketSession() *codexWebsocketSession {
	return &codexWebsocketSession{
		sessionID:            "",
		upstreamDisconnectCh: make(chan error, 1),
	}
}

func (s *codexWebsocketSession) resetUpstreamDisconnectError(conn *websocket.Conn) {
	if s == nil || conn == nil {
		return
	}
	s.upstreamDisconnectErrMu.Lock()
	s.upstreamDisconnectErrConn = conn
	s.upstreamDisconnectErr = nil
	s.upstreamDisconnectErrMu.Unlock()

	s.lastEventMu.Lock()
	s.lastEventConn = conn
	s.lastEventType = ""
	s.lastEventMu.Unlock()
}

func (s *codexWebsocketSession) setUpstreamDisconnectError(conn *websocket.Conn, err error) {
	if s == nil || conn == nil || err == nil {
		return
	}
	s.upstreamDisconnectErrMu.Lock()
	if s.upstreamDisconnectErrConn == conn && s.upstreamDisconnectErr == nil {
		s.upstreamDisconnectErr = err
	}
	s.upstreamDisconnectErrMu.Unlock()
}

func (s *codexWebsocketSession) upstreamDisconnectError(conn *websocket.Conn) error {
	if s == nil || conn == nil {
		return nil
	}
	s.upstreamDisconnectErrMu.RLock()
	defer s.upstreamDisconnectErrMu.RUnlock()
	if s.upstreamDisconnectErrConn != conn {
		return nil
	}
	return s.upstreamDisconnectErr
}

func (s *codexWebsocketSession) queueUpstreamDisconnectLocked(err error, generation uint64) {
	if s == nil || s.upstreamDisconnectCh == nil {
		return
	}
	event := websocketUpstreamDisconnectEvent{cause: err, generation: generation}
	select {
	case s.upstreamDisconnectCh <- event:
		return
	default:
	}
	// A queued event can belong to an inactive connection; retain the latest disconnect.
	select {
	case <-s.upstreamDisconnectCh:
	default:
	}
	s.upstreamDisconnectCh <- event
}

func executionSessionIDFromOptions(opts cliproxyexecutor.Options) string {
	if len(opts.Metadata) == 0 {
		return ""
	}
	raw, ok := opts.Metadata[cliproxyexecutor.ExecutionSessionMetadataKey]
	if !ok || raw == nil {
		return ""
	}
	switch v := raw.(type) {
	case string:
		return strings.TrimSpace(v)
	case []byte:
		return strings.TrimSpace(string(v))
	default:
		return ""
	}
}

func (e *CodexWebsocketsExecutor) getOrCreateSession(sessionID string) *codexWebsocketSession {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	if e == nil {
		return nil
	}
	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.sessions == nil {
		store.sessions = make(map[string]*codexWebsocketSession)
	}
	if sess, ok := store.sessions[sessionID]; ok && sess != nil {
		return sess
	}
	sess := &codexWebsocketSession{
		sessionID:            sessionID,
		upstreamDisconnectCh: make(chan error, 1),
	}
	store.sessions[sessionID] = sess
	return sess
}

func (e *CodexWebsocketsExecutor) UpstreamDisconnectChan(sessionID string) <-chan error {
	sess := e.getOrCreateSession(sessionID)
	if sess == nil {
		return nil
	}
	return sess.upstreamDisconnectCh
}

func (e *CodexWebsocketsExecutor) UpstreamDisconnectGeneration(sessionID string) uint64 {
	sess := e.getOrCreateSession(sessionID)
	if sess == nil {
		return 0
	}
	sess.connMu.Lock()
	defer sess.connMu.Unlock()
	return sess.upstreamDisconnectGeneration
}

func (e *CodexWebsocketsExecutor) UpstreamGeneration(sessionID string) uint64 {
	sessionID = strings.TrimSpace(sessionID)
	if e == nil || sessionID == "" {
		return 0
	}
	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	store.mu.Lock()
	sess := store.sessions[sessionID]
	store.mu.Unlock()
	if sess == nil {
		return 0
	}
	sess.connMu.Lock()
	generation := sess.upstreamGeneration
	sess.connMu.Unlock()
	return generation
}

func (e *CodexWebsocketsExecutor) DropUpstreamSession(sessionID string, reason string) {
	sessionID = strings.TrimSpace(sessionID)
	if e == nil || sessionID == "" {
		return
	}
	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	store.mu.Lock()
	sess := store.sessions[sessionID]
	store.mu.Unlock()
	if sess == nil {
		return
	}
	sess.connMu.Lock()
	conn := sess.conn
	sess.connMu.Unlock()
	if conn == nil {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "upstream_session_dropped"
	}
	e.dropUpstreamConn(sess, conn, reason, nil, false)
}

func (e *CodexWebsocketsExecutor) ensureUpstreamConn(ctx context.Context, auth *cliproxyauth.Auth, sess *codexWebsocketSession, authID string, wsURL string, headers http.Header, useSIWC bool) (*websocket.Conn, *websocketConnectionCloser, *http.Response, bool, error) {
	if sess == nil {
		conn, closer, resp, err := e.dialCodexWebsocket(ctx, auth, wsURL, headers)
		if conn != nil {
			configureRawCodexWebsocketConn(conn, authID, wsURL)
		}
		return conn, closer, resp, true, err
	}
	if errSession := sess.sessionError(); errSession != nil {
		return nil, nil, nil, false, errSession
	}
	proxyURL := executionProxyURL(ctx, sess.effectiveConfig(authID, e.cfg), auth)
	if staleConn, staleCloser, staleAuthID, staleWSURL, staleLifecycle := detachMismatchedWebsocketSessionConn(sess, authID, wsURL, proxyURL, useSIWC); staleConn != nil {
		sess.connMu.Lock()
		sess.upstreamGeneration++
		sess.connMu.Unlock()
		staleLastEvent := sess.getLastEventType(staleConn)
		logCodexWebsocketDisconnectedWithLastEvent(sess, sess.sessionID, staleAuthID, staleWSURL, "target_changed", staleLastEvent, nil)
		if staleCloser != nil {
			if errClose := staleCloser.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close stale websocket error: %v", errClose)
			}
		}
		if staleLifecycle != nil {
			staleLifecycle.End("target_changed")
		}
	}

	sess.connMu.Lock()
	conn := sess.conn
	closer := sess.connCloser
	readerConn := sess.readerConn
	sess.connMu.Unlock()
	if conn != nil {
		if readerConn != conn {
			sess.connMu.Lock()
			sess.readerConn = conn
			sess.connMu.Unlock()
			sess.configureConn(conn)
			go e.readUpstreamLoop(sess, conn)
		}
		logCodexWebsocketConnectedWithReused(sess, sess.sessionID, authID, wsURL, true)
		return conn, closer, nil, false, nil
	}

	dialProxyURL := executionProxyURL(ctx, e.cfg, auth)
	conn, closer, resp, errDial := e.dialCodexWebsocket(ctx, auth, wsURL, headers)
	if errDial != nil {
		return nil, closer, resp, false, errDial
	}

	sess.connMu.Lock()
	if errSession := sess.sessionError(); errSession != nil {
		sess.connMu.Unlock()
		if errClose := closer.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
		closeHTTPResponseBody(resp, "codex websockets executor: closed session handshake")
		return nil, nil, nil, false, errSession
	}
	if sess.conn != nil && websocketSessionTargetMatches(sess, authID, wsURL, dialProxyURL, useSIWC) {
		previous := sess.conn
		previousCloser := sess.connCloser
		sess.connMu.Unlock()
		if errClose := closer.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
		logCodexWebsocketConnectedWithReused(sess, sess.sessionID, authID, wsURL, true)
		return previous, previousCloser, nil, false, nil
	}
	sess.conn = conn
	sess.connCloser = closer
	sess.connCfg = e.cfg
	sess.hasConnCfg = true
	sess.multiAgentV2OptimizedConn = nil
	sess.wsURL = wsURL
	sess.authID = authID
	sess.proxyURL = dialProxyURL
	sess.useSIWC = useSIWC
	sess.readerConn = conn
	sess.connMu.Unlock()

	sess.configureConn(conn)
	go e.readUpstreamLoop(sess, conn)
	logCodexWebsocketConnectedWithReused(sess, sess.sessionID, authID, wsURL, false)
	return conn, closer, resp, true, nil
}

func (e *CodexWebsocketsExecutor) readUpstreamLoop(sess *codexWebsocketSession, conn *websocket.Conn) {
	if e == nil || sess == nil || conn == nil {
		return
	}
	for {
		_ = conn.SetReadDeadline(time.Now().Add(codexResponsesWebsocketIdleTimeout))
		msgType, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			mappedErr := mapCodexWebsocketReadError(errRead)
			if shouldRetryCodexWebsocketSend(mappedErr) {
				sess.markTerminalError(conn, codexWebsocketUpstreamResetError{cause: errRead})
				if !e.detachUpstreamConnForRecovery(sess, conn, "upstream_disconnected", errRead) {
					return
				}
				sess.closeActiveReadForConn(conn, codexWebsocketUpstreamResetError{cause: errRead})
				return
			}
			sess.markTerminalError(conn, errRead)
			if sess.closeActiveReadForConn(conn, errRead) {
				return
			}
			e.invalidateUpstreamConn(sess, conn, "upstream_disconnected", errRead)
			return
		}

		if msgType != websocket.TextMessage {
			if msgType == websocket.BinaryMessage {
				errBinary := fmt.Errorf("codex websockets executor: unexpected binary message")
				sess.markTerminalError(conn, errBinary)
				if sess.closeActiveReadForConn(conn, errBinary) {
					return
				}
				e.invalidateUpstreamConn(sess, conn, "unexpected_binary", errBinary)
				return
			}
			continue
		}

		payload = bytes.TrimSpace(payload)
		if len(payload) > 0 {
			eventType := gjson.GetBytes(payload, "type").String()
			if eventType != "" {
				sess.setLastEventType(conn, eventType)
			}
			// Control acknowledgements belong to the connection: a response terminal
			// event can arrive first and finish its per-response forwarding channel.
			if eventType == "response.interrupt.accepted" || eventType == "response.interrupt.failed" {
				sess.activeMu.Lock()
				var acknowledge func([]byte)
				if sess.responseStateConn == conn {
					acknowledge = sess.interruptAckHandler
				}
				sess.activeMu.Unlock()
				if acknowledge != nil {
					acknowledge(payload)
					continue
				}
			}
			// Track the running response before delivery: downstream may
			// still be processing this turn after its active read channel is
			// cleared, and interrupts arriving after the completion must not
			// forward while teardown is still pending. Terminal events only
			// end their own response; a duplex socket can reject a queued
			// create while another response is still running.
			//
			// Failure events (error, response.failed) use the duplex
			// attribution shapes: an explicit response id (response.id or
			// top-level response_id) ends only that response, while an
			// id-less failure ends the running one. Success terminals only
			// carry response.id, and one without an id is left unattributed,
			// as before.
			responseID := gjson.GetBytes(payload, "response.id").String()
			if eventType == "response.created" {
				if responseID != "" {
					sess.activeMu.Lock()
					if sess.responseStateConn == conn && sess.activeConn == conn && sess.activeCh != nil {
						sess.activeResponseID = responseID
						sess.activeResponseTerminal = false
					}
					sess.activeMu.Unlock()
				}
			} else if isTerminalEvent(eventType) {
				failure := eventType == "error" || eventType == "response.failed"
				if failure && responseID == "" {
					responseID = gjson.GetBytes(payload, "response_id").String()
				}
				if responseID != "" || failure {
					sess.activeMu.Lock()
					if sess.responseStateConn == conn {
						terminalID := responseID
						if terminalID == "" {
							// Id-less failure: the running response ended.
							terminalID = sess.activeResponseID
						}
						if terminalID != "" && eventType != "error" {
							// Match the pre-existing terminal-ID recording,
							// which never covered error events.
							sess.terminalResponseIDs[terminalID] = struct{}{}
						}
						if terminalID != "" && sess.activeResponseID == terminalID {
							sess.activeResponseTerminal = true
						}
					}
					sess.activeMu.Unlock()
				}
			}
		}

		ch, done := sess.activeForConn(conn)
		if ch == nil {
			continue
		}
		select {
		case ch <- codexWebsocketRead{conn: conn, msgType: msgType, payload: payload}:
		case <-done:
		}
	}
}

func (e *CodexWebsocketsExecutor) invalidateUpstreamConn(sess *codexWebsocketSession, conn *websocket.Conn, reason string, err error) {
	e.dropUpstreamConn(sess, conn, reason, err, true)
}

func (e *CodexWebsocketsExecutor) invalidateUpstreamConnWithoutDisconnectNotify(sess *codexWebsocketSession, conn *websocket.Conn, reason string, err error) {
	e.dropUpstreamConn(sess, conn, reason, err, false)
}

func (e *CodexWebsocketsExecutor) detachUpstreamConnForRecovery(sess *codexWebsocketSession, conn *websocket.Conn, reason string, err error) bool {
	if sess == nil || conn == nil {
		return false
	}

	sess.connMu.Lock()
	if sess.conn != conn {
		sess.connMu.Unlock()
		return false
	}
	authID := sess.authID
	wsURL := sess.wsURL
	sessionID := sess.sessionID
	closer := sess.connCloser
	sess.conn = nil
	sess.connCloser = nil
	sess.connCfg = nil
	sess.hasConnCfg = false
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	if sess.pendingHandshakeConn == conn {
		sess.pendingHandshakeConn = nil
		sess.pendingHandshakeHeaders = nil
	}
	sess.upstreamGeneration++
	sess.connMu.Unlock()

	logCodexWebsocketDisconnected(sessionID, authID, wsURL, reason, err)
	if closer != nil {
		if errClose := closer.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
	}
	return true
}

func (e *CodexWebsocketsExecutor) dropUpstreamConn(sess *codexWebsocketSession, conn *websocket.Conn, reason string, err error, notifyDownstream bool) {
	if sess == nil || conn == nil {
		return
	}

	sess.connMu.Lock()
	current := sess.conn
	authID := sess.authID
	wsURL := sess.wsURL
	sessionID := sess.sessionID
	if current == nil || current != conn {
		sess.connMu.Unlock()
		return
	}
	lifecycle := sess.lifecycle
	closer := sess.connCloser
	sess.lifecycle = nil
	sess.lifecycleModel = ""
	sess.conn = nil
	sess.connCloser = nil
	sess.connCfg = nil
	sess.hasConnCfg = false
	sess.multiAgentV2OptimizedConn = nil
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	if sess.pendingHandshakeConn == conn {
		sess.pendingHandshakeConn = nil
		sess.pendingHandshakeHeaders = nil
	}
	if notifyDownstream {
		sess.upstreamDisconnectGeneration++
		sess.queueUpstreamDisconnectLocked(err, sess.upstreamDisconnectGeneration)
	}
	if !notifyDownstream {
		sess.upstreamGeneration++
	}
	sess.connMu.Unlock()

	lastEvent := sess.getLastEventType(conn)
	logCodexWebsocketDisconnectedWithLastEvent(sess, sessionID, authID, wsURL, reason, lastEvent, err)
	if closer != nil {
		if errClose := closer.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
	}
	if lifecycle != nil {
		lifecycle.End(reason)
	}
}

func (e *CodexWebsocketsExecutor) sessionStore() *codexWebsocketSessionStore {
	if e == nil {
		return nil
	}
	if e.store != nil {
		return e.store
	}
	return globalCodexWebsocketSessionStore
}

func (e *CodexWebsocketsExecutor) SharesExecutionSessionsWith(other cliproxyauth.ProviderExecutor) bool {
	if e == nil || other == nil {
		return false
	}
	store := e.sessionStore()
	if store == nil {
		return false
	}
	switch next := other.(type) {
	case *CodexWebsocketsExecutor:
		return next != nil && store == next.sessionStore()
	case *CodexAutoExecutor:
		return next != nil && next.wsExec != nil && store == next.wsExec.sessionStore()
	default:
		return false
	}
}

func (e *CodexWebsocketsExecutor) CloseAuthExecutionSessions(authID string) {
	if e == nil {
		return
	}
	closeCodexWebsocketSessionsInStoreForAuthID(e.sessionStore(), authID, "auth_removed")
}

func (e *CodexWebsocketsExecutor) CloseExecutionSession(sessionID string) {
	sessionID = strings.TrimSpace(sessionID)
	if e == nil {
		return
	}
	if sessionID == "" {
		return
	}
	if sessionID == cliproxyauth.CloseAllExecutionSessionsID {
		e.closeAllExecutionSessions("executor_shutdown")
		return
	}

	store := e.sessionStore()
	store.mu.Lock()
	sess := store.sessions[sessionID]
	delete(store.sessions, sessionID)
	store.mu.Unlock()

	e.closeExecutionSession(sess, "session_closed")
}

func (e *CodexWebsocketsExecutor) closeAllExecutionSessions(reason string) {
	if e == nil {
		return
	}

	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	store.mu.Lock()
	sessions := make([]*codexWebsocketSession, 0, len(store.sessions))
	for sessionID, sess := range store.sessions {
		delete(store.sessions, sessionID)
		if sess != nil {
			sessions = append(sessions, sess)
		}
	}
	store.mu.Unlock()

	for i := range sessions {
		e.closeExecutionSession(sessions[i], reason)
	}
}

func (e *CodexWebsocketsExecutor) closeExecutionSession(sess *codexWebsocketSession, reason string) {
	closeCodexWebsocketSession(sess, reason)
}

func closeCodexWebsocketSession(sess *codexWebsocketSession, reason string) {
	if sess == nil {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "session_closed"
	}
	sessionClosedErr := fmt.Errorf("codex websockets executor: execution session closed")
	sess.markSessionError(sessionClosedErr)
	sess.closeActiveRead(sessionClosedErr)

	sess.connMu.Lock()
	conn := sess.conn
	authID := sess.authID
	wsURL := sess.wsURL
	lifecycle := sess.lifecycle
	closer := sess.connCloser
	sess.lifecycle = nil
	sess.lifecycleModel = ""
	sess.conn = nil
	sess.connCloser = nil
	sess.connCfg = nil
	sess.hasConnCfg = false
	sess.multiAgentV2OptimizedConn = nil
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	sessionID := sess.sessionID
	sess.pendingHandshakeConn = nil
	sess.pendingHandshakeHeaders = nil
	sess.connMu.Unlock()

	lastEvent := sess.getLastEventType(conn)
	if conn != nil {
		logCodexWebsocketDisconnectedWithLastEvent(sess, sessionID, authID, wsURL, reason, lastEvent, nil)
		if closer != nil {
			if errClose := closer.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
		}
	}
	if lifecycle != nil {
		lifecycle.End(reason)
	}
}

func sessionObjectKind(sess *codexWebsocketSession) string {
	if sess == nil {
		return "none"
	}
	if strings.TrimSpace(sess.sessionID) != "" {
		return "persistent"
	}
	return "ephemeral"
}

func logCodexWebsocketConnected(sessionID string, authID string, wsURL string) {
	logCodexWebsocketConnectedWithReused(nil, sessionID, authID, wsURL, false)
}

func logCodexWebsocketConnectedWithReused(sess *codexWebsocketSession, sessionID string, authID string, wsURL string, reused bool) {
	sessionStr := strings.TrimSpace(sessionID)
	sessionKind := sessionObjectKind(sess)
	if reused {
		log.Infof("codex websockets: upstream connected session=%s auth=%s url=%s session_object=%s reused=true", sessionStr, strings.TrimSpace(authID), strings.TrimSpace(wsURL), sessionKind)
		return
	}
	log.Infof("codex websockets: upstream connected session=%s auth=%s url=%s session_object=%s reused=false", sessionStr, strings.TrimSpace(authID), strings.TrimSpace(wsURL), sessionKind)
}

func logCodexWebsocketDisconnected(sessionID string, authID string, wsURL string, reason string, err error) {
	logCodexWebsocketDisconnectedWithLastEvent(nil, sessionID, authID, wsURL, reason, "", err)
}

func isTerminalEvent(eventType string) bool {
	switch eventType {
	case "response.completed", "response.done", "response.incomplete", "response.failed", "error":
		return true
	default:
		return false
	}
}

func logCodexWebsocketDisconnectedWithLastEvent(sess *codexWebsocketSession, sessionID string, authID string, wsURL string, reason string, lastEvent string, err error) {
	sessionStr := strings.TrimSpace(sessionID)
	sessionKind := sessionObjectKind(sess)
	terminalStatus := isTerminalEvent(lastEvent)
	if err != nil {
		if lastEvent != "" {
			log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s session_object=%s reason=%s last_event=%s is_terminal=%t err=%v", sessionStr, strings.TrimSpace(authID), strings.TrimSpace(wsURL), sessionKind, strings.TrimSpace(reason), lastEvent, terminalStatus, err)
			return
		}
		log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s session_object=%s reason=%s is_terminal=false err=%v", sessionStr, strings.TrimSpace(authID), strings.TrimSpace(wsURL), sessionKind, strings.TrimSpace(reason), err)
		return
	}
	if lastEvent != "" {
		log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s session_object=%s reason=%s last_event=%s is_terminal=%t", sessionStr, strings.TrimSpace(authID), strings.TrimSpace(wsURL), sessionKind, strings.TrimSpace(reason), lastEvent, terminalStatus)
		return
	}
	log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s session_object=%s reason=%s is_terminal=false", sessionStr, strings.TrimSpace(authID), strings.TrimSpace(wsURL), sessionKind, strings.TrimSpace(reason))
}

// CloseCodexWebsocketSessionsForAuthID closes all active Codex upstream websocket sessions
// associated with the supplied auth ID.
func CloseCodexWebsocketSessionsForAuthID(authID string, reason string) {
	closeCodexWebsocketSessionsInStoreForAuthID(globalCodexWebsocketSessionStore, authID, reason)
}

func closeCodexWebsocketSessionsInStoreForAuthID(store *codexWebsocketSessionStore, authID string, reason string) {
	authID = strings.TrimSpace(authID)
	if authID == "" || store == nil {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "auth_removed"
	}

	type sessionItem struct {
		sessionID string
		sess      *codexWebsocketSession
	}

	store.mu.Lock()
	items := make([]sessionItem, 0, len(store.sessions))
	for sessionID, sess := range store.sessions {
		items = append(items, sessionItem{sessionID: sessionID, sess: sess})
	}
	store.mu.Unlock()

	matches := make([]sessionItem, 0)
	for i := range items {
		sess := items[i].sess
		if sess == nil {
			continue
		}
		sess.connMu.Lock()
		sessAuthID := strings.TrimSpace(sess.authID)
		sess.connMu.Unlock()
		if sessAuthID == authID {
			matches = append(matches, items[i])
		}
	}
	if len(matches) == 0 {
		return
	}

	toClose := make([]*codexWebsocketSession, 0, len(matches))
	store.mu.Lock()
	for i := range matches {
		current, ok := store.sessions[matches[i].sessionID]
		if !ok || current == nil || current != matches[i].sess {
			continue
		}
		delete(store.sessions, matches[i].sessionID)
		toClose = append(toClose, current)
	}
	store.mu.Unlock()

	for i := range toClose {
		closeCodexWebsocketSession(toClose[i], reason)
	}
}
