// Package executor provides runtime execution capabilities for various AI service providers.
// This file implements a Codex executor that uses the Responses API WebSocket transport.
package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/misc"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"golang.org/x/net/proxy"
)

const (
	codexResponsesWebsocketBetaHeaderValue = "responses_websockets=2026-02-06"
	codexResponsesWebsocketIdleTimeout     = 5 * time.Minute
	codexResponsesWebsocketHandshakeTO     = 30 * time.Second
)

// CodexWebsocketsExecutor executes Codex Responses requests using a WebSocket transport.
//
// It preserves the existing CodexExecutor HTTP implementation as a fallback for endpoints
// not available over WebSocket (e.g. /responses/compact) and for websocket upgrade failures.
type CodexWebsocketsExecutor struct {
	*CodexExecutor

	store *codexWebsocketSessionStore
}

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

	connMu                  sync.Mutex
	conn                    *websocket.Conn
	connCloser              *websocketConnectionCloser
	wsURL                   string
	authID                  string
	pendingHandshakeConn    *websocket.Conn
	pendingHandshakeHeaders http.Header
	lifecycleBindMu         sync.Mutex
	lifecycle               cliproxyexecutor.ExecutionLifecycle
	lifecycleModel          string

	writeMu sync.Mutex

	activeMu        sync.Mutex
	activeCh        chan codexWebsocketRead
	activeConn      *websocket.Conn
	activeDone      <-chan struct{}
	activeCancel    context.CancelFunc
	activeClosedCh  chan codexWebsocketRead
	activeClosedErr error
	terminalErr     error

	readerConn *websocket.Conn

	upstreamGeneration        uint64
	upstreamDisconnectOnce    sync.Once
	upstreamDisconnectCh      chan error
	upstreamDisconnectErrMu   sync.RWMutex
	upstreamDisconnectErrConn *websocket.Conn
	upstreamDisconnectErr     error
}

func NewCodexWebsocketsExecutor(cfg *config.Config) *CodexWebsocketsExecutor {
	return &CodexWebsocketsExecutor{
		CodexExecutor: NewCodexExecutor(cfg),
		store:         globalCodexWebsocketSessionStore,
	}
}

type codexWebsocketRead struct {
	conn    *websocket.Conn
	msgType int
	payload []byte
	err     error
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

func (s *codexWebsocketSession) setActive(conn *websocket.Conn, ch chan codexWebsocketRead) {
	if s == nil {
		return
	}
	s.activeMu.Lock()
	if s.activeCancel != nil {
		s.activeCancel()
		s.activeCancel = nil
		s.activeDone = nil
	}
	s.activeClosedCh = nil
	s.activeClosedErr = nil
	s.activeCh = ch
	s.activeConn = conn
	if conn != nil && ch != nil {
		activeCtx, activeCancel := context.WithCancel(context.Background())
		s.activeDone = activeCtx.Done()
		s.activeCancel = activeCancel
	}
	s.activeMu.Unlock()
}

func (s *codexWebsocketSession) activate(conn *websocket.Conn) chan codexWebsocketRead {
	if s == nil || conn == nil {
		return nil
	}
	ch := make(chan codexWebsocketRead, 4096)
	s.setActive(conn, ch)
	return ch
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

func (s *codexWebsocketSession) terminalError() error {
	if s == nil {
		return nil
	}
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	return s.terminalErr
}

func (s *codexWebsocketSession) markTerminalError(err error) {
	if s == nil || err == nil {
		return
	}
	s.activeMu.Lock()
	s.terminalErr = err
	if s.activeClosedCh != nil {
		s.activeClosedErr = err
	}
	s.activeMu.Unlock()
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

func (s *codexWebsocketSession) writeMessage(conn *websocket.Conn, msgType int, payload []byte) error {
	if s == nil {
		return fmt.Errorf("codex websockets executor: session is nil")
	}
	if conn == nil {
		return fmt.Errorf("codex websockets executor: websocket conn is nil")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return conn.WriteMessage(msgType, payload)
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
	s.resetUpstreamDisconnectError(conn)
	conn.SetPingHandler(func(appData string) error {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		// Reply pongs from the same write lock to avoid concurrent writes.
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
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
	if s.readerConn == conn {
		s.readerConn = nil
	}
	if s.pendingHandshakeConn == conn {
		s.pendingHandshakeConn = nil
		s.pendingHandshakeHeaders = nil
	}
	s.connMu.Unlock()

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

func websocketSessionTargetChanged(sess *codexWebsocketSession, authID string, wsURL string) bool {
	if sess == nil {
		return false
	}

	sess.connMu.Lock()
	defer sess.connMu.Unlock()
	if strings.TrimSpace(sess.authID) == "" && strings.TrimSpace(sess.wsURL) == "" {
		return false
	}
	return strings.TrimSpace(sess.authID) != strings.TrimSpace(authID) || strings.TrimSpace(sess.wsURL) != strings.TrimSpace(wsURL)
}

func existingWebsocketSessionConn(sess *codexWebsocketSession, authID string, wsURL string) (*websocket.Conn, *websocketConnectionCloser) {
	if sess == nil {
		return nil, nil
	}
	sess.connMu.Lock()
	conn := sess.conn
	closer := sess.connCloser
	matches := conn != nil && closer != nil &&
		strings.TrimSpace(sess.authID) == strings.TrimSpace(authID) &&
		strings.TrimSpace(sess.wsURL) == strings.TrimSpace(wsURL)
	sess.connMu.Unlock()
	if !matches || sess.upstreamDisconnectError(conn) != nil {
		return nil, nil
	}
	return conn, closer
}

func detachMismatchedWebsocketSessionConn(sess *codexWebsocketSession, authID string, wsURL string) (*websocket.Conn, *websocketConnectionCloser, string, string, cliproxyexecutor.ExecutionLifecycle) {
	if sess == nil {
		return nil, nil, "", "", nil
	}

	sess.connMu.Lock()
	defer sess.connMu.Unlock()
	conn := sess.conn
	if conn == nil || (strings.TrimSpace(sess.authID) == strings.TrimSpace(authID) && strings.TrimSpace(sess.wsURL) == strings.TrimSpace(wsURL)) {
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
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	if sess.pendingHandshakeConn == conn {
		sess.pendingHandshakeConn = nil
		sess.pendingHandshakeHeaders = nil
	}
	return conn, closer, previousAuthID, previousWSURL, lifecycle
}

func (s *codexWebsocketSession) resetUpstreamDisconnectError(conn *websocket.Conn) {
	if s == nil || conn == nil {
		return
	}
	s.upstreamDisconnectErrMu.Lock()
	s.upstreamDisconnectErrConn = conn
	s.upstreamDisconnectErr = nil
	s.upstreamDisconnectErrMu.Unlock()
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

func (s *codexWebsocketSession) notifyUpstreamDisconnect(err error) {
	if s == nil {
		return
	}
	s.upstreamDisconnectOnce.Do(func() {
		if s.upstreamDisconnectCh == nil {
			return
		}
		select {
		case s.upstreamDisconnectCh <- err:
		default:
		}
		close(s.upstreamDisconnectCh)
	})
}

func (e *CodexWebsocketsExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Alt == "responses/compact" {
		return e.CodexExecutor.executeCompact(ctx, auth, req, opts)
	}

	baseModel := thinking.ParseSuffix(req.Model).ModelName
	apiKey, baseURL := codexCreds(auth)
	if baseURL == "" {
		baseURL = "https://chatgpt.com/backend-api/codex"
	}

	reporter := helps.NewExecutorUsageReporter(ctx, e, baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	to := sdktranslator.FromString("codex")
	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalPayload := originalPayloadSource
	originalTranslated, body := translateCodexRequestPair(from, to, baseModel, originalPayload, req.Payload, false)

	body, err = thinking.ApplyThinking(body, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return resp, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	requestPath := helps.PayloadRequestPath(opts)
	body = helps.ApplyPayloadConfigWithRequest(e.cfg, baseModel, to.String(), from.String(), "", body, originalTranslated, requestedModel, requestPath, opts.Headers)
	body = helps.SetStringIfDifferent(body, "model", baseModel)
	body = helps.SetBoolIfDifferent(body, "stream", true)
	body, _ = sjson.DeleteBytes(body, "prompt_cache_retention")
	body, _ = sjson.DeleteBytes(body, "safety_identifier")
	body = normalizeCodexInstructions(body)
	body, responsesLite := normalizeCodexResponsesLiteRequest(body, opts.Headers)
	if !responsesLite && (e.cfg == nil || e.cfg.DisableImageGeneration == config.DisableImageGenerationOff) {
		body = ensureImageGenerationTool(body, baseModel, auth, opts.Headers)
	}
	body = sanitizeOpenAIResponsesReasoningEncryptedContent(ctx, "codex websockets executor", body)
	body = normalizeCodexWebsocketParallelToolCalls(body, opts.Headers)
	body, replayScope, errReplay := applyCodexReasoningReplayCacheRequired(ctx, from, req, opts, body)
	if errReplay != nil {
		return resp, errReplay
	}

	httpURL := strings.TrimSuffix(baseURL, "/") + "/responses"
	wsURL, err := buildCodexResponsesWebsocketURL(httpURL)
	if err != nil {
		return resp, err
	}

	body, wsHeaders, errPromptCache := applyCodexPromptCacheHeadersWithContext(ctx, from, req, body, opts.Headers)
	if errPromptCache != nil {
		return resp, errPromptCache
	}
	clientBody := body
	var identityState codexIdentityConfuseState
	upstreamBody, identityState := applyCodexIdentityConfuseBody(e.cfg, auth, originalPayloadSource, body)
	reporter.SetTranslatedReasoningEffort(clientBody, to.String())
	wsHeaders = applyCodexWebsocketHeaders(ctx, wsHeaders, auth, apiKey, e.cfg)
	finalizeCodexWebsocketHeaders(wsHeaders, upstreamBody, baseModel, &identityState)

	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}

	executionSessionID := executionSessionIDFromOptions(opts)
	var sess *codexWebsocketSession
	sessionLocked := false
	unlockSession := func() {
		if sess != nil && sessionLocked {
			sess.reqMu.Unlock()
			sessionLocked = false
		}
	}
	if executionSessionID != "" {
		sess = e.getOrCreateSession(executionSessionID)
		sess.reqMu.Lock()
		sessionLocked = true
		defer unlockSession()
	}

	wsReqBody := buildCodexWebsocketRequestBody(upstreamBody)
	wsReqLog := helps.UpstreamRequestLog{
		URL:       wsURL,
		Method:    "WEBSOCKET",
		Headers:   wsHeaders.Clone(),
		Body:      wsReqBody,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	}
	helps.RecordAPIWebsocketRequest(ctx, e.cfg, wsReqLog)

	var conn *websocket.Conn
	var closer *websocketConnectionCloser
	var respHS *http.Response
	var upstreamCreated bool
	var errDial error
	if cliproxyexecutor.RequiredUpstreamWebsocket(ctx) {
		conn, closer = existingWebsocketSessionConn(sess, authID, wsURL)
		if conn == nil {
			return resp, cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
		}
	} else {
		conn, closer, respHS, upstreamCreated, errDial = e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
	}
	if errDial != nil {
		bodyErr := websocketHandshakeBody(respHS)
		if respHS != nil {
			helps.RecordAPIWebsocketUpgradeRejection(ctx, e.cfg, websocketUpgradeRequestLog(wsReqLog), respHS.StatusCode, respHS.Header.Clone(), bodyErr)
		}
		if respHS != nil && respHS.StatusCode == http.StatusUpgradeRequired {
			if opts.ExecutionLifecycle != nil || cliproxyexecutor.DownstreamWebsocket(ctx) {
				return resp, statusErr{code: respHS.StatusCode, msg: string(bodyErr)}
			}
			return e.CodexExecutor.Execute(ctx, auth, req, opts)
		}
		if respHS != nil && respHS.StatusCode > 0 {
			return resp, statusErr{code: respHS.StatusCode, msg: string(bodyErr)}
		}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "dial", errDial)
		return resp, errDial
	}
	if errBind := sess.bindExecutionLifecycle(opts, conn, closer, req.Model); errBind != nil {
		unlockSession()
		closeWebsocketAfterBindFailure(sess, conn, closer)
		return resp, errBind
	}
	recordAPIWebsocketHandshake(ctx, e.cfg, respHS)
	reporter.StartResponseTTFT()
	if sess != nil && cliproxyexecutor.DownstreamWebsocket(ctx) && upstreamCreated && codexWebsocketRequestNeedsTranscriptReplayOnReset(wsReqBody) {
		errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "upstream_recreated"}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
		return resp, errReplay
	}
	if sess == nil {
		logCodexWebsocketConnected(executionSessionID, authID, wsURL)
		defer func() {
			reason := "completed"
			if err != nil {
				reason = "error"
			}
			logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, reason, err)
			if errClose := closer.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
		}()
	}

	var readCh chan codexWebsocketRead
	if sess != nil {
		readCh = sess.activate(conn)
		defer func() {
			sess.clearActive(conn, readCh)
		}()
	}

	if errSend := writeCodexWebsocketMessage(sess, conn, wsReqBody); errSend != nil {
		errSend = mapCodexWebsocketWriteError(sess, conn, errSend)
		if sess != nil {
			if cliproxyexecutor.RequiredUpstreamWebsocket(ctx) {
				if !shouldRetryCodexWebsocketSend(errSend) {
					e.invalidateUpstreamConnWithoutDisconnectNotify(sess, conn, "send_error", errSend)
					helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
					return resp, errSend
				}
				e.detachUpstreamConnForRecovery(sess, conn, "send_error", errSend)
				return resp, cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
			}
			if !shouldRetryCodexWebsocketSend(errSend) {
				e.invalidateUpstreamConn(sess, conn, "send_error", errSend)
				helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
				return resp, errSend
			}
			e.detachUpstreamConnForRecovery(sess, conn, "send_error", errSend)
			sess.clearActive(conn, readCh)
			if codexWebsocketRequestNeedsTranscriptReplayOnReset(wsReqBody) {
				errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "send_error", cause: errSend}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
				return resp, errReplay
			}

			// Retry once with a fresh websocket connection. This is mainly to handle
			// upstream closing the socket between sequential requests within the same
			// execution session.
			connRetry, closerRetry, respHSRetry, _, errDialRetry := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
			if errDialRetry != nil || connRetry == nil {
				closeHTTPResponseBody(respHSRetry, "codex websockets executor: close handshake response body error")
				helps.RecordAPIWebsocketError(ctx, e.cfg, "dial_retry", errDialRetry)
				return resp, errDialRetry
			}
			previousConn, previousReadCh := conn, readCh
			conn = connRetry
			closer = closerRetry
			if errBind := sess.bindExecutionLifecycle(opts, conn, closer, req.Model); errBind != nil {
				clearRetryActiveState(sess, previousConn, previousReadCh)
				unlockSession()
				closeWebsocketAfterBindFailure(sess, conn, closer)
				return resp, errBind
			}
			readCh = sess.activate(conn)
			wsReqBodyRetry := buildCodexWebsocketRequestBody(upstreamBody)
			helps.RecordAPIWebsocketRequest(ctx, e.cfg, helps.UpstreamRequestLog{
				URL:       wsURL,
				Method:    "WEBSOCKET",
				Headers:   wsHeaders.Clone(),
				Body:      wsReqBodyRetry,
				Provider:  e.Identifier(),
				AuthID:    authID,
				AuthLabel: authLabel,
				AuthType:  authType,
				AuthValue: authValue,
			})
			recordAPIWebsocketHandshake(ctx, e.cfg, respHSRetry)
			reporter.StartResponseTTFT()
			if errSendRetry := writeCodexWebsocketMessage(sess, conn, wsReqBodyRetry); errSendRetry != nil {
				errSendRetry = mapCodexWebsocketWriteError(sess, conn, errSendRetry)
				e.invalidateUpstreamConn(sess, conn, "send_error", errSendRetry)
				sess.clearActive(conn, readCh)
				helps.RecordAPIWebsocketError(ctx, e.cfg, "send_retry", errSendRetry)
				return resp, errSendRetry
			}
			wsReqBody = wsReqBodyRetry
		} else {
			helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
			return resp, errSend
		}
	}

	outputItemsByIndex := make(map[int64][]byte)
	var outputItemsFallback [][]byte
	for {
		if ctx != nil && ctx.Err() != nil {
			return resp, ctx.Err()
		}
		msgType, payload, errRead := readCodexWebsocketMessage(ctx, sess, conn, readCh)
		if errRead != nil {
			if sess != nil && ctx != nil && ctx.Err() != nil {
				return resp, ctx.Err()
			}
			if codexWebsocketReadErrorRequiresTranscriptReplay(wsReqBody, errRead, cliproxyexecutor.DownstreamWebsocket(ctx)) {
				errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "read_error", cause: errRead}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
				return resp, errReplay
			}
			mappedErr := mapCodexWebsocketReadError(errRead)
			var upstreamReset codexWebsocketUpstreamResetError
			if sess != nil && !errors.As(errRead, &upstreamReset) {
				e.invalidateUpstreamConn(sess, conn, "read_error", mappedErr)
			}
			helps.RecordAPIWebsocketError(ctx, e.cfg, "read", mappedErr)
			return resp, mappedErr
		}
		if msgType != websocket.TextMessage {
			if msgType == websocket.BinaryMessage {
				err = fmt.Errorf("codex websockets executor: unexpected binary message")
				if sess != nil {
					e.invalidateUpstreamConn(sess, conn, "unexpected_binary", err)
				}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "unexpected_binary", err)
				return resp, err
			}
			continue
		}

		payload = bytes.TrimSpace(payload)
		if len(payload) == 0 {
			continue
		}
		reporter.MarkFirstResponseByte()
		payload = applyCodexIdentityConfuseResponsePayload(payload, identityState)
		helps.AppendAPIWebsocketResponse(ctx, e.cfg, payload)

		if wsErr, ok := parseCodexWebsocketError(payload); ok {
			clientErr := exposeCodexWebsocketError(payload, identityState, wsErr)
			if sess != nil {
				if shouldDropCodexWebsocketUpstreamErrorQuietly(payload, wsErr) {
					e.dropUpstreamConn(sess, conn, codexWebsocketUpstreamErrorDropReason(payload, wsErr), wsErr, false)
				} else {
					e.invalidateUpstreamConn(sess, conn, "upstream_error", wsErr)
				}
				unlockSession()
			}
			if errClearReplay := clearCodexReasoningReplayOnWebsocketError(ctx, replayScope, payload); errClearReplay != nil {
				return resp, errClearReplay
			}
			helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", wsErr)
			reporter.PublishFailure(ctx, wsErr)
			return resp, clientErr
		}

		if upstreamTerminalErr, terminalReason, ok := parseCodexWebsocketTerminalResponseError(payload); ok {
			if errClearReplay := clearCodexReasoningReplayOnWebsocketTerminalError(ctx, replayScope, payload); errClearReplay != nil {
				return resp, errClearReplay
			}
			clientTerminalErr := upstreamTerminalErr
			clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
			if exposedTerminalErr, _, okExposed := parseCodexWebsocketTerminalResponseError(clientPayload); okExposed {
				clientTerminalErr = exposedTerminalErr
			}
			if sess != nil {
				e.dropUpstreamConn(sess, conn, terminalReason, upstreamTerminalErr, false)
				unlockSession()
			}
			helps.RecordAPIWebsocketError(ctx, e.cfg, terminalReason, upstreamTerminalErr)
			reporter.PublishFailure(ctx, upstreamTerminalErr)
			return resp, clientTerminalErr
		}
		if streamErr, terminalBody, ok := codexTerminalFailureErr(payload); ok {
			if sess != nil {
				e.invalidateUpstreamConn(sess, conn, "terminal_failure", streamErr)
				unlockSession()
			}
			if errClearReplay := clearCodexReasoningReplayOnInvalidSignature(ctx, replayScope, streamErr.StatusCode(), terminalBody); errClearReplay != nil {
				return resp, errClearReplay
			}
			return resp, streamErr
		}

		payload = normalizeCodexWebsocketCompletion(payload)
		eventType := gjson.GetBytes(payload, "type").String()
		switch eventType {
		case "response.output_item.done":
			collectCodexOutputItemDone(payload, outputItemsByIndex, &outputItemsFallback)
		case "response.completed":
			payload = patchCodexCompletedOutput(payload, outputItemsByIndex, outputItemsFallback)
			cacheCodexReasoningReplayFromCompleted(replayScope, payload)
			if detail, ok := helps.ParseCodexUsage(payload); ok {
				reporter.Publish(ctx, detail)
			}
			var param any
			clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
			out := sdktranslator.TranslateNonStream(ctx, to, responseFormat, req.Model, originalPayload, clientBody, clientPayload, &param)
			resp = cliproxyexecutor.Response{Payload: out}
			return resp, nil
		}
	}
}

func (e *CodexWebsocketsExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	log.Debugf("Executing Codex Websockets stream request with auth ID: %s, model: %s", auth.ID, req.Model)
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Alt == "responses/compact" {
		return nil, statusErr{code: http.StatusBadRequest, msg: "streaming not supported for /responses/compact"}
	}

	baseModel := thinking.ParseSuffix(req.Model).ModelName
	apiKey, baseURL := codexCreds(auth)
	if baseURL == "" {
		baseURL = "https://chatgpt.com/backend-api/codex"
	}

	reporter := helps.NewExecutorUsageReporter(ctx, e, baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	to := sdktranslator.FromString("codex")
	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalPayload := originalPayloadSource
	originalTranslated, body := translateCodexRequestPair(from, to, baseModel, originalPayload, req.Payload, true)

	body, err = thinking.ApplyThinking(body, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return nil, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	requestPath := helps.PayloadRequestPath(opts)
	body = helps.ApplyPayloadConfigWithRequest(e.cfg, baseModel, to.String(), from.String(), "", body, originalTranslated, requestedModel, requestPath, opts.Headers)
	body = helps.SetStringIfDifferent(body, "model", baseModel)
	body = normalizeCodexInstructions(body)
	body, responsesLite := normalizeCodexResponsesLiteRequest(body, opts.Headers)
	if !responsesLite && (e.cfg == nil || e.cfg.DisableImageGeneration == config.DisableImageGenerationOff) {
		body = ensureImageGenerationTool(body, baseModel, auth, opts.Headers)
	}
	body = sanitizeOpenAIResponsesReasoningEncryptedContent(ctx, "codex websockets executor", body)
	body = normalizeCodexWebsocketParallelToolCalls(body, opts.Headers)
	body, replayScope, errReplay := applyCodexReasoningReplayCacheRequired(ctx, from, req, opts, body)
	if errReplay != nil {
		return nil, errReplay
	}

	httpURL := strings.TrimSuffix(baseURL, "/") + "/responses"
	wsURL, err := buildCodexResponsesWebsocketURL(httpURL)
	if err != nil {
		return nil, err
	}

	body, wsHeaders, errPromptCache := applyCodexPromptCacheHeadersWithContext(ctx, from, req, body, opts.Headers)
	if errPromptCache != nil {
		return nil, errPromptCache
	}
	clientBody := body
	var identityState codexIdentityConfuseState
	upstreamBody, identityState := applyCodexIdentityConfuseBody(e.cfg, auth, originalPayloadSource, body)
	reporter.SetTranslatedReasoningEffort(clientBody, to.String())
	wsHeaders = applyCodexWebsocketHeaders(ctx, wsHeaders, auth, apiKey, e.cfg)
	finalizeCodexWebsocketHeaders(wsHeaders, upstreamBody, baseModel, &identityState)

	var authID, authLabel, authType, authValue string
	authID = auth.ID
	authLabel = auth.Label
	authType, authValue = auth.AccountInfo()

	executionSessionID := executionSessionIDFromOptions(opts)
	var sess *codexWebsocketSession
	if executionSessionID != "" {
		sess = e.getOrCreateSession(executionSessionID)
		if sess != nil {
			sess.reqMu.Lock()
		}
	}
	streamSessionLocked := sess != nil
	unlockSessionRequest := func() {
		if sess != nil && streamSessionLocked {
			sess.reqMu.Unlock()
			streamSessionLocked = false
		}
	}

	wsReqBody := buildCodexWebsocketRequestBody(upstreamBody)
	wsReqLog := helps.UpstreamRequestLog{
		URL:       wsURL,
		Method:    "WEBSOCKET",
		Headers:   wsHeaders.Clone(),
		Body:      wsReqBody,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	}
	helps.RecordAPIWebsocketRequest(ctx, e.cfg, wsReqLog)

	var conn *websocket.Conn
	var closer *websocketConnectionCloser
	var respHS *http.Response
	var upstreamCreated bool
	var errDial error
	if cliproxyexecutor.RequiredUpstreamWebsocket(ctx) {
		conn, closer = existingWebsocketSessionConn(sess, authID, wsURL)
		if conn == nil {
			unlockSessionRequest()
			return nil, cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
		}
	} else {
		conn, closer, respHS, upstreamCreated, errDial = e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
	}
	var upstreamHeaders http.Header
	if respHS != nil {
		upstreamHeaders = respHS.Header.Clone()
	} else if sess != nil {
		upstreamHeaders = sess.takeHandshakeHeadersForReplay(conn)
	}
	if errDial != nil {
		bodyErr := websocketHandshakeBody(respHS)
		if respHS != nil {
			helps.RecordAPIWebsocketUpgradeRejection(ctx, e.cfg, websocketUpgradeRequestLog(wsReqLog), respHS.StatusCode, respHS.Header.Clone(), bodyErr)
		}
		if respHS != nil && respHS.StatusCode == http.StatusUpgradeRequired {
			if opts.ExecutionLifecycle != nil || cliproxyexecutor.DownstreamWebsocket(ctx) {
				unlockSessionRequest()
				return nil, statusErr{code: respHS.StatusCode, msg: string(bodyErr)}
			}
			unlockSessionRequest()
			return e.CodexExecutor.ExecuteStream(ctx, auth, req, opts)
		}
		if respHS != nil && respHS.StatusCode > 0 {
			unlockSessionRequest()
			return nil, statusErr{code: respHS.StatusCode, msg: string(bodyErr)}
		}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "dial", errDial)
		unlockSessionRequest()
		return nil, errDial
	}
	if errBind := sess.bindExecutionLifecycle(opts, conn, closer, req.Model); errBind != nil {
		unlockSessionRequest()
		closeWebsocketAfterBindFailure(sess, conn, closer)
		return nil, errBind
	}
	recordAPIWebsocketHandshake(ctx, e.cfg, respHS)
	reporter.StartResponseTTFT()
	if sess != nil && cliproxyexecutor.DownstreamWebsocket(ctx) && upstreamCreated && codexWebsocketRequestNeedsTranscriptReplayOnReset(wsReqBody) {
		errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "upstream_recreated"}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
		sess.storeHandshakeHeadersForReplay(conn, upstreamHeaders)
		unlockSessionRequest()
		return nil, errReplay
	}

	if sess == nil {
		logCodexWebsocketConnected(executionSessionID, authID, wsURL)
	}

	var readCh chan codexWebsocketRead
	if sess != nil {
		readCh = sess.activate(conn)
	}

	if errSend := writeCodexWebsocketMessage(sess, conn, wsReqBody); errSend != nil {
		errSend = mapCodexWebsocketWriteError(sess, conn, errSend)
		helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
		if sess != nil {
			if cliproxyexecutor.RequiredUpstreamWebsocket(ctx) {
				if shouldRetryCodexWebsocketSend(errSend) {
					e.detachUpstreamConnForRecovery(sess, conn, "send_error", errSend)
				} else {
					e.invalidateUpstreamConnWithoutDisconnectNotify(sess, conn, "send_error", errSend)
				}
				sess.clearActive(conn, readCh)
				unlockSessionRequest()
				if !shouldRetryCodexWebsocketSend(errSend) {
					return nil, errSend
				}
				return nil, cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
			}
			if !shouldRetryCodexWebsocketSend(errSend) {
				e.invalidateUpstreamConn(sess, conn, "send_error", errSend)
				sess.clearActive(conn, readCh)
				unlockSessionRequest()
				return nil, errSend
			}
			e.detachUpstreamConnForRecovery(sess, conn, "send_error", errSend)
			sess.clearActive(conn, readCh)
			if codexWebsocketRequestNeedsTranscriptReplayOnReset(wsReqBody) {
				errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "send_error", cause: errSend}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
				unlockSessionRequest()
				return nil, errReplay
			}

			// Retry once with a new websocket connection for the same execution session.
			connRetry, closerRetry, respHSRetry, _, errDialRetry := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
			if errDialRetry != nil || connRetry == nil {
				closeHTTPResponseBody(respHSRetry, "codex websockets executor: close handshake response body error")
				helps.RecordAPIWebsocketError(ctx, e.cfg, "dial_retry", errDialRetry)
				unlockSessionRequest()
				return nil, errDialRetry
			}
			previousConn, previousReadCh := conn, readCh
			conn = connRetry
			closer = closerRetry
			if errBind := sess.bindExecutionLifecycle(opts, conn, closer, req.Model); errBind != nil {
				clearRetryActiveState(sess, previousConn, previousReadCh)
				unlockSessionRequest()
				closeWebsocketAfterBindFailure(sess, conn, closer)
				return nil, errBind
			}
			readCh = sess.activate(conn)
			wsReqBodyRetry := buildCodexWebsocketRequestBody(upstreamBody)
			helps.RecordAPIWebsocketRequest(ctx, e.cfg, helps.UpstreamRequestLog{
				URL:       wsURL,
				Method:    "WEBSOCKET",
				Headers:   wsHeaders.Clone(),
				Body:      wsReqBodyRetry,
				Provider:  e.Identifier(),
				AuthID:    authID,
				AuthLabel: authLabel,
				AuthType:  authType,
				AuthValue: authValue,
			})
			recordAPIWebsocketHandshake(ctx, e.cfg, respHSRetry)
			reporter.StartResponseTTFT()
			if errSendRetry := writeCodexWebsocketMessage(sess, conn, wsReqBodyRetry); errSendRetry != nil {
				errSendRetry = mapCodexWebsocketWriteError(sess, conn, errSendRetry)
				helps.RecordAPIWebsocketError(ctx, e.cfg, "send_retry", errSendRetry)
				e.invalidateUpstreamConn(sess, conn, "send_error", errSendRetry)
				sess.clearActive(conn, readCh)
				unlockSessionRequest()
				return nil, errSendRetry
			}
			if respHSRetry != nil {
				upstreamHeaders = respHSRetry.Header.Clone()
			}
			wsReqBody = wsReqBodyRetry
		} else {
			logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, "send_error", errSend)
			if errClose := closer.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
			return nil, errSend
		}
	}

	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		terminateReason := "completed"
		var terminateErr error

		defer close(out)
		defer func() {
			if sess != nil {
				sess.clearActive(conn, readCh)
				unlockSessionRequest()
				return
			}
			logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, terminateReason, terminateErr)
			if errClose := closer.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
		}()

		send := func(chunk cliproxyexecutor.StreamChunk) bool {
			if ctx == nil {
				out <- chunk
				return true
			}
			select {
			case out <- chunk:
				return true
			case <-ctx.Done():
				return false
			}
		}
		dropTerminalConnection := func(reason string, terminalErr error) {
			if sess == nil {
				return
			}
			if cliproxyexecutor.DownstreamWebsocket(ctx) {
				e.dropUpstreamConn(sess, conn, reason, terminalErr, false)
			} else {
				e.invalidateUpstreamConn(sess, conn, reason, terminalErr)
			}
			unlockSessionRequest()
		}

		claudeInputTokens := helps.NewClaudeInputTokenState(from, to, responseFormat, originalPayload)
		var param any
		outputItemsByIndex := make(map[int64][]byte)
		var outputItemsFallback [][]byte
		for {
			if ctx != nil && ctx.Err() != nil {
				terminateReason = "context_done"
				terminateErr = ctx.Err()
				_ = send(cliproxyexecutor.StreamChunk{Err: ctx.Err()})
				return
			}
			msgType, payload, errRead := readCodexWebsocketMessage(ctx, sess, conn, readCh)
			if errRead != nil {
				if sess != nil && ctx != nil && ctx.Err() != nil {
					terminateReason = "context_done"
					terminateErr = ctx.Err()
					_ = send(cliproxyexecutor.StreamChunk{Err: ctx.Err()})
					return
				}
				terminateReason = "read_error"
				if codexWebsocketReadErrorRequiresTranscriptReplay(wsReqBody, errRead, cliproxyexecutor.DownstreamWebsocket(ctx)) {
					errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "read_error", cause: errRead}
					terminateErr = errReplay
					helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
					reporter.PublishFailure(ctx, errReplay)
					_ = send(cliproxyexecutor.StreamChunk{Err: errReplay})
					return
				}
				mappedErr := mapCodexWebsocketReadError(errRead)
				var upstreamReset codexWebsocketUpstreamResetError
				if sess != nil && !errors.As(errRead, &upstreamReset) {
					defer func() {
						dropTerminalConnection("read_error", mappedErr)
					}()
				}
				terminateErr = mappedErr
				helps.RecordAPIWebsocketError(ctx, e.cfg, "read", mappedErr)
				reporter.PublishFailure(ctx, mappedErr)
				_ = send(cliproxyexecutor.StreamChunk{Err: mappedErr})
				return
			}
			if msgType != websocket.TextMessage {
				if msgType == websocket.BinaryMessage {
					errBinary := fmt.Errorf("codex websockets executor: unexpected binary message")
					terminateReason = "unexpected_binary"
					terminateErr = errBinary
					helps.RecordAPIWebsocketError(ctx, e.cfg, "unexpected_binary", errBinary)
					reporter.PublishFailure(ctx, errBinary)
					if sess != nil {
						defer func() {
							dropTerminalConnection("unexpected_binary", errBinary)
						}()
					}
					_ = send(cliproxyexecutor.StreamChunk{Err: errBinary})
					return
				}
				continue
			}

			payload = bytes.TrimSpace(payload)
			if len(payload) == 0 {
				continue
			}
			reporter.MarkFirstResponseByte()
			payload = applyCodexIdentityConfuseResponsePayload(payload, identityState)
			helps.AppendAPIWebsocketResponse(ctx, e.cfg, payload)

			if wsErr, ok := parseCodexWebsocketError(payload); ok {
				terminateReason = "upstream_error"
				terminateErr = wsErr
				if sess != nil {
					defer func() {
						if cliproxyexecutor.DownstreamWebsocket(ctx) {
							e.dropUpstreamConn(sess, conn, "upstream_error", wsErr, false)
						} else if shouldDropCodexWebsocketUpstreamErrorQuietly(payload, wsErr) {
							e.dropUpstreamConn(sess, conn, codexWebsocketUpstreamErrorDropReason(payload, wsErr), wsErr, false)
						} else {
							e.invalidateUpstreamConn(sess, conn, "upstream_error", wsErr)
						}
						unlockSessionRequest()
					}()
				}
				if errClearReplay := clearCodexReasoningReplayOnWebsocketError(ctx, replayScope, payload); errClearReplay != nil {
					terminateErr = errClearReplay
					helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_clear_error", errClearReplay)
					reporter.PublishFailure(ctx, errClearReplay)
					_ = send(cliproxyexecutor.StreamChunk{Err: errClearReplay})
					return
				}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", wsErr)
				reporter.PublishFailure(ctx, wsErr)
				if cliproxyexecutor.DownstreamWebsocket(ctx) {
					clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
					_ = send(cliproxyexecutor.StreamChunk{Payload: clientPayload, ResultErr: wsErr})
					return
				}
				clientErr := exposeCodexWebsocketError(payload, identityState, wsErr)
				_ = send(cliproxyexecutor.StreamChunk{Err: clientErr})
				return
			}
			if _, _, responseTerminal := parseCodexWebsocketTerminalResponseError(payload); !responseTerminal {
				if streamErr, terminalBody, ok := codexTerminalFailureErr(payload); ok {
					terminateReason = "upstream_error"
					terminateErr = streamErr
					if sess != nil {
						defer func() {
							dropTerminalConnection("terminal_failure", streamErr)
						}()
					}
					if errClearReplay := clearCodexReasoningReplayOnInvalidSignature(ctx, replayScope, streamErr.StatusCode(), terminalBody); errClearReplay != nil {
						terminateErr = errClearReplay
						helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_clear_error", errClearReplay)
						reporter.PublishFailure(ctx, errClearReplay)
						_ = send(cliproxyexecutor.StreamChunk{Err: errClearReplay})
						return
					}
					helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", streamErr)
					reporter.PublishFailure(ctx, streamErr)
					_ = send(cliproxyexecutor.StreamChunk{Err: streamErr})
					return
				}
			}

			eventType := gjson.GetBytes(payload, "type").String()
			if eventType == "response.output_item.done" {
				collectCodexOutputItemDone(payload, outputItemsByIndex, &outputItemsFallback)
			}
			completedPayload := payload
			if eventType == "response.completed" || eventType == "response.done" {
				completedPayload = normalizeCodexWebsocketCompletion(completedPayload)
				completedPayload = patchCodexCompletedOutput(completedPayload, outputItemsByIndex, outputItemsFallback)
				cacheCodexReasoningReplayFromCompleted(replayScope, completedPayload)
				if detail, ok := helps.ParseCodexUsage(completedPayload); ok {
					reporter.Publish(ctx, detail)
				}
			}
			clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
			upstreamTerminalErr, terminalReason, isResponseTerminalError := parseCodexWebsocketTerminalResponseError(payload)
			clientTerminalErr := upstreamTerminalErr
			if isResponseTerminalError {
				if exposedTerminalErr, _, okExposed := parseCodexWebsocketTerminalResponseError(clientPayload); okExposed {
					clientTerminalErr = exposedTerminalErr
				}
				if sess != nil {
					defer func() {
						e.dropUpstreamConn(sess, conn, terminalReason, upstreamTerminalErr, false)
						unlockSessionRequest()
					}()
				}
			}
			isTerminalEvent := eventType == "response.completed" || eventType == "response.done" || eventType == "error" || isResponseTerminalError
			if cliproxyexecutor.DownstreamWebsocket(ctx) {
				if isResponseTerminalError {
					if errClearReplay := clearCodexReasoningReplayOnWebsocketTerminalError(ctx, replayScope, payload); errClearReplay != nil {
						terminateErr = errClearReplay
						helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_clear_error", errClearReplay)
						reporter.PublishFailure(ctx, errClearReplay)
						_ = send(cliproxyexecutor.StreamChunk{Err: errClearReplay})
						return
					}
					terminateReason = terminalReason
					terminateErr = upstreamTerminalErr
					helps.RecordAPIWebsocketError(ctx, e.cfg, terminalReason, upstreamTerminalErr)
					reporter.PublishFailure(ctx, upstreamTerminalErr)
				}
				chunk := cliproxyexecutor.StreamChunk{Payload: clientPayload}
				if isResponseTerminalError {
					chunk.ResultErr = upstreamTerminalErr
				}
				if !send(chunk) {
					terminateReason = "context_done"
					terminateErr = ctx.Err()
					return
				}
				if isTerminalEvent {
					return
				}
				continue
			}
			if isResponseTerminalError {
				if errClearReplay := clearCodexReasoningReplayOnWebsocketTerminalError(ctx, replayScope, payload); errClearReplay != nil {
					terminateErr = errClearReplay
					helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_clear_error", errClearReplay)
					reporter.PublishFailure(ctx, errClearReplay)
					_ = send(cliproxyexecutor.StreamChunk{Err: errClearReplay})
					return
				}
				terminateReason = terminalReason
				terminateErr = upstreamTerminalErr
				helps.RecordAPIWebsocketError(ctx, e.cfg, terminalReason, upstreamTerminalErr)
				reporter.PublishFailure(ctx, upstreamTerminalErr)
				_ = send(cliproxyexecutor.StreamChunk{Err: clientTerminalErr})
				return
			}

			payload = normalizeCodexWebsocketCompletion(payload)
			if eventType == "response.completed" || eventType == "response.done" {
				payload = completedPayload
			}
			eventType = gjson.GetBytes(payload, "type").String()
			clientPayload = applyCodexIdentityExposeResponsePayload(payload, identityState)
			line := encodeCodexWebsocketAsSSE(clientPayload)
			chunks := helps.TranslateStreamWithClaudeInputTokens(ctx, to, responseFormat, req.Model, originalPayload, clientBody, line, &param, claudeInputTokens)
			for i := range chunks {
				if !send(cliproxyexecutor.StreamChunk{Payload: chunks[i]}) {
					terminateReason = "context_done"
					terminateErr = ctx.Err()
					return
				}
			}
			if eventType == "response.completed" || eventType == "response.done" {
				return
			}
		}
	}()

	return &cliproxyexecutor.StreamResult{Headers: upstreamHeaders, Chunks: out}, nil
}

func (e *CodexWebsocketsExecutor) dialCodexWebsocket(ctx context.Context, auth *cliproxyauth.Auth, wsURL string, headers http.Header) (*websocket.Conn, *websocketConnectionCloser, *http.Response, error) {
	dialer := newProxyAwareWebsocketDialer(e.cfg, auth)
	dialer.HandshakeTimeout = codexResponsesWebsocketHandshakeTO
	dialer.EnableCompression = true
	if ctx == nil {
		ctx = context.Background()
	}
	conn, resp, err := dialer.DialContext(ctx, wsURL, headers)
	closer := newWebsocketConnectionCloser(conn)
	if conn != nil {
		// Avoid gorilla/websocket flate tail validation issues on some upstreams/Go versions.
		// Negotiating permessage-deflate is fine; we just don't compress outbound messages.
		conn.EnableWriteCompression(false)
	}
	return conn, closer, resp, err
}

func writeCodexWebsocketMessage(sess *codexWebsocketSession, conn *websocket.Conn, payload []byte) error {
	if sess != nil {
		return sess.writeMessage(conn, websocket.TextMessage, payload)
	}
	if conn == nil {
		return fmt.Errorf("codex websockets executor: websocket conn is nil")
	}
	return conn.WriteMessage(websocket.TextMessage, payload)
}

func mapCodexWebsocketWriteError(sess *codexWebsocketSession, conn *websocket.Conn, err error) error {
	if err == nil || sess == nil || conn == nil {
		return err
	}
	upstreamErr := sess.upstreamDisconnectError(conn)
	var closeErr *websocket.CloseError
	if !errors.As(upstreamErr, &closeErr) || closeErr.Code != websocket.CloseMessageTooBig {
		return err
	}
	return mapCodexWebsocketReadError(upstreamErr)
}

func shouldRetryCodexWebsocketSend(err error) bool {
	if err == nil {
		return false
	}
	var requestErr cliproxyexecutor.RequestScopedError
	return !errors.As(err, &requestErr) || !requestErr.IsRequestScoped()
}

type codexWebsocketMessageTooBigError struct {
	statusErr
}

func (codexWebsocketMessageTooBigError) IsRequestScoped() bool {
	return true
}

func mapCodexWebsocketReadError(err error) error {
	if err == nil {
		return nil
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) && closeErr.Code == websocket.CloseMessageTooBig {
		return codexWebsocketMessageTooBigError{statusErr: statusErr{
			code: http.StatusRequestEntityTooLarge,
			msg:  `{"error":{"message":"upstream websocket message too big","type":"invalid_request_error","code":"message_too_big"}}`,
		}}
	}
	return err
}

func normalizeCodexWebsocketParallelToolCalls(body []byte, headers http.Header) []byte {
	if !isCodexResponsesLiteRequest(body, headers) {
		return body
	}
	body = helps.SetBoolIfDifferent(body, "parallel_tool_calls", false)
	return body
}

func buildCodexWebsocketRequestBody(body []byte) []byte {
	if len(body) == 0 {
		return nil
	}

	// Match codex-rs websocket v2 semantics: every request is `response.create`.
	// Incremental follow-up turns continue on the same websocket using
	// `previous_response_id` + incremental `input`, not `response.append`.
	body = helps.SanitizeCodexInputItemIDs(body)
	wsReqBody, errSet := sjson.SetBytes(bytes.Clone(body), "type", "response.create")
	if errSet == nil && len(wsReqBody) > 0 {
		return wsReqBody
	}
	fallback := bytes.Clone(body)
	fallback, _ = sjson.SetBytes(fallback, "type", "response.create")
	return fallback
}

func readCodexWebsocketMessage(ctx context.Context, sess *codexWebsocketSession, conn *websocket.Conn, readCh chan codexWebsocketRead) (int, []byte, error) {
	if sess == nil {
		if conn == nil {
			return 0, nil, fmt.Errorf("codex websockets executor: websocket conn is nil")
		}
		_ = conn.SetReadDeadline(time.Now().Add(codexResponsesWebsocketIdleTimeout))
		msgType, payload, errRead := conn.ReadMessage()
		return msgType, payload, errRead
	}
	if conn == nil {
		return 0, nil, fmt.Errorf("codex websockets executor: websocket conn is nil")
	}
	if readCh == nil {
		return 0, nil, fmt.Errorf("codex websockets executor: session read channel is nil")
	}
	readBuffered := func() (int, []byte, error, bool) {
		for {
			select {
			case ev, ok := <-readCh:
				if !ok {
					return 0, nil, fmt.Errorf("codex websockets executor: session read channel closed"), true
				}
				if ev.conn != conn {
					continue
				}
				if ev.err != nil {
					if terminalErr := sess.terminalError(); terminalErr != nil {
						return 0, nil, terminalErr, true
					}
					return 0, nil, ev.err, true
				}
				return ev.msgType, ev.payload, nil, true
			default:
				return 0, nil, nil, false
			}
		}
	}
	activeDone, active := sess.activeDoneFor(readCh)
	if !active {
		if msgType, payload, errRead, ok := readBuffered(); ok {
			return msgType, payload, errRead
		}
		if errRead := sess.closedActiveErrorFor(readCh); errRead != nil {
			return 0, nil, errRead
		}
		if terminalErr := sess.terminalError(); terminalErr != nil {
			return 0, nil, terminalErr
		}
		return 0, nil, fmt.Errorf("codex websockets executor: session read channel inactive")
	}
	for {
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-activeDone:
			if msgType, payload, errRead, ok := readBuffered(); ok {
				return msgType, payload, errRead
			}
			if errRead := sess.closedActiveErrorFor(readCh); errRead != nil {
				return 0, nil, errRead
			}
			if terminalErr := sess.terminalError(); terminalErr != nil {
				return 0, nil, terminalErr
			}
			return 0, nil, fmt.Errorf("codex websockets executor: session read channel closed")
		case ev, ok := <-readCh:
			if !ok {
				return 0, nil, fmt.Errorf("codex websockets executor: session read channel closed")
			}
			if ev.conn != conn {
				continue
			}
			if ev.err != nil {
				if terminalErr := sess.terminalError(); terminalErr != nil {
					return 0, nil, terminalErr
				}
				return 0, nil, ev.err
			}
			return ev.msgType, ev.payload, nil
		}
	}
}

func newProxyAwareWebsocketDialer(cfg *config.Config, auth *cliproxyauth.Auth) *websocket.Dialer {
	dialer := &websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  codexResponsesWebsocketHandshakeTO,
		EnableCompression: true,
		NetDialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	proxyURL := ""
	if auth != nil {
		proxyURL = strings.TrimSpace(auth.ProxyURL)
	}
	if proxyURL == "" && cfg != nil {
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
	}
	if proxyURL == "" {
		return dialer
	}

	setting, errParse := proxyutil.Parse(proxyURL)
	if errParse != nil {
		log.Errorf("codex websockets executor: %v", errParse)
		return dialer
	}

	switch setting.Mode {
	case proxyutil.ModeDirect:
		dialer.Proxy = nil
		return dialer
	case proxyutil.ModeProxy:
	default:
		return dialer
	}

	switch setting.URL.Scheme {
	case "socks5", "socks5h":
		var proxyAuth *proxy.Auth
		if setting.URL.User != nil {
			username := setting.URL.User.Username()
			password, _ := setting.URL.User.Password()
			proxyAuth = &proxy.Auth{User: username, Password: password}
		}
		socksDialer, errSOCKS5 := proxy.SOCKS5("tcp", setting.URL.Host, proxyAuth, proxy.Direct)
		if errSOCKS5 != nil {
			log.Errorf("codex websockets executor: create SOCKS5 dialer failed: %v", errSOCKS5)
			return dialer
		}
		dialer.Proxy = nil
		dialer.NetDialContext = func(_ context.Context, network, addr string) (net.Conn, error) {
			return socksDialer.Dial(network, addr)
		}
	case "http", "https":
		dialer.Proxy = http.ProxyURL(setting.URL)
	default:
		log.Errorf("codex websockets executor: unsupported proxy scheme: %s", setting.URL.Scheme)
	}

	return dialer
}

func buildCodexResponsesWebsocketURL(httpURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(httpURL))
	if err != nil {
		return "", err
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	default:
		return "", fmt.Errorf("codex websockets executor: unsupported responses websocket URL scheme %q", parsed.Scheme)
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return "", fmt.Errorf("codex websockets executor: responses websocket URL host is empty")
	}
	return parsed.String(), nil
}

func applyCodexPromptCacheHeaders(from sdktranslator.Format, req cliproxyexecutor.Request, rawJSON []byte) ([]byte, http.Header) {
	body, headers, _ := applyCodexPromptCacheHeadersWithContext(context.Background(), from, req, rawJSON)
	return body, headers
}

func applyCodexPromptCacheHeadersWithContext(ctx context.Context, from sdktranslator.Format, req cliproxyexecutor.Request, rawJSON []byte, headerSets ...http.Header) ([]byte, http.Header, error) {
	headers := http.Header{}
	if len(rawJSON) == 0 {
		return rawJSON, headers, nil
	}

	var requestHeaders http.Header
	if len(headerSets) > 0 {
		requestHeaders = headerSets[0]
	}
	var cache helps.CodexCache
	if sourceFormatEqual(from, sdktranslator.FormatClaude) {
		modelName := strings.TrimSpace(gjson.GetBytes(rawJSON, "model").String())
		if modelName == "" {
			modelName = thinking.ParseSuffix(req.Model).ModelName
		}
		cached, ok, errCache := helps.ClaudeCodePromptCache(ctx, modelName, req.Payload, requestHeaders)
		if errCache != nil {
			return nil, nil, errCache
		}
		if ok {
			cache = cached
		}
	} else if sourceFormatEqual(from, sdktranslator.FormatOpenAIResponse) {
		if promptCacheKey := gjson.GetBytes(req.Payload, "prompt_cache_key"); promptCacheKey.Exists() {
			cache.ID = promptCacheKey.String()
		}
	}

	if cache.ID != "" {
		rawJSON = helps.SetStringIfDifferent(rawJSON, "prompt_cache_key", cache.ID)
		setHeaderCasePreserved(headers, "session_id", cache.ID)
		headers.Set("Conversation_id", cache.ID)
	}

	return rawJSON, headers, nil
}

func applyCodexWebsocketHeaders(ctx context.Context, headers http.Header, auth *cliproxyauth.Auth, token string, cfg *config.Config) http.Header {
	if headers == nil {
		headers = http.Header{}
	}
	if strings.TrimSpace(token) != "" {
		headers.Set("Authorization", "Bearer "+token)
	}

	var ginHeaders http.Header
	if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
		ginHeaders = ginCtx.Request.Header.Clone()
	}

	isAPIKey := codexAuthUsesAPIKey(auth)
	cfgUserAgent, cfgBetaFeatures := codexHeaderDefaults(cfg, auth)
	ensureHeaderWithPriority(headers, ginHeaders, "x-codex-beta-features", cfgBetaFeatures, "")
	misc.EnsureHeader(headers, ginHeaders, "x-codex-turn-state", "")
	misc.EnsureHeader(headers, ginHeaders, "x-codex-turn-metadata", "")
	misc.EnsureHeader(headers, ginHeaders, "x-codex-window-id", "")
	misc.EnsureHeader(headers, ginHeaders, "x-codex-parent-thread-id", "")
	misc.EnsureHeader(headers, ginHeaders, "x-client-request-id", "")
	misc.EnsureHeader(headers, ginHeaders, "thread-id", "")
	misc.EnsureHeader(headers, ginHeaders, "x-responsesapi-include-timing-metrics", "")
	misc.EnsureHeader(headers, ginHeaders, "Version", "")
	if isAPIKey {
		ensureHeaderWithPriority(headers, ginHeaders, "User-Agent", "", "")
	} else {
		ensureHeaderWithConfigPrecedence(headers, ginHeaders, "User-Agent", cfgUserAgent, codexUserAgent)
	}

	betaHeader := strings.TrimSpace(headers.Get("OpenAI-Beta"))
	if betaHeader == "" && ginHeaders != nil {
		betaHeader = strings.TrimSpace(ginHeaders.Get("OpenAI-Beta"))
	}
	if betaHeader == "" || !strings.Contains(betaHeader, "responses_websockets=") {
		betaHeader = codexResponsesWebsocketBetaHeaderValue
	}
	headers.Set("OpenAI-Beta", betaHeader)
	sessionFallback := ""
	if strings.Contains(headers.Get("User-Agent"), "Mac OS") {
		sessionFallback = uuid.NewString()
	}
	ensureCodexWebsocketSessionHeader(headers, ginHeaders, sessionFallback)
	if originator := strings.TrimSpace(ginHeaders.Get("Originator")); originator != "" {
		headers.Set("Originator", originator)
	} else if !isAPIKey {
		headers.Set("Originator", codexOriginator)
	}
	if !isAPIKey {
		if auth != nil && auth.Metadata != nil {
			if accountID, ok := auth.Metadata["account_id"].(string); ok {
				if trimmed := strings.TrimSpace(accountID); trimmed != "" {
					setHeaderCasePreserved(headers, "ChatGPT-Account-ID", trimmed)
				}
			}
		}
	}

	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(&http.Request{Header: headers}, attrs)

	return headers
}

func finalizeCodexWebsocketHeaders(headers http.Header, body []byte, modelName string, identityState *codexIdentityConfuseState) {
	applyCodexClientMetadataCompatibilityHeaders(headers, body)
	applyModelHeaderOverrides(headers, modelName)
	applyCodexIdentityConfuseHeaders(headers, identityState)
}

func ensureCodexWebsocketSessionHeader(target http.Header, source http.Header, fallbackValue string) {
	if target == nil {
		return
	}
	sessionID := codexSessionHeaderValue(target)
	if sessionID == "" {
		sessionID = codexSessionHeaderValue(source)
	}
	if sessionID == "" {
		sessionID = strings.TrimSpace(fallbackValue)
	}
	if sessionID != "" {
		setHeaderCasePreserved(target, "session_id", sessionID)
	}
	deleteHeaderCaseInsensitive(target, "Session-Id")
}

func codexSessionHeaderValue(headers http.Header) string {
	for _, key := range []string{"Session-Id", "Session_id", "session_id"} {
		if value := strings.TrimSpace(headerValueCaseInsensitive(headers, key)); value != "" {
			return value
		}
	}
	return ""
}

func codexAuthUsesAPIKey(auth *cliproxyauth.Auth) bool {
	if auth == nil || auth.Attributes == nil {
		return false
	}
	return strings.TrimSpace(auth.Attributes["api_key"]) != ""
}

func ensureHeaderCasePreserved(target http.Header, source http.Header, key, configValue, fallbackValue string) {
	if target == nil {
		return
	}
	if strings.TrimSpace(headerValueCaseInsensitive(target, key)) != "" {
		return
	}
	if source != nil {
		if val := strings.TrimSpace(headerValueCaseInsensitive(source, key)); val != "" {
			setHeaderCasePreserved(target, key, val)
			return
		}
	}
	if val := strings.TrimSpace(configValue); val != "" {
		setHeaderCasePreserved(target, key, val)
		return
	}
	if val := strings.TrimSpace(fallbackValue); val != "" {
		setHeaderCasePreserved(target, key, val)
	}
}

func setHeaderCasePreserved(headers http.Header, key string, value string) {
	if headers == nil {
		return
	}
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" {
		return
	}
	deleteHeaderCaseInsensitive(headers, key)
	headers[key] = []string{value}
}

func setCodexSessionHeaderCasePreserved(headers http.Header, fallbackKey string, value string) {
	if headers == nil {
		return
	}
	fallbackKey = strings.TrimSpace(fallbackKey)
	value = strings.TrimSpace(value)
	if fallbackKey == "" || value == "" {
		return
	}

	selectedKey := ""
	if _, ok := headers[fallbackKey]; ok && codexSessionHeaderKey(fallbackKey) {
		selectedKey = fallbackKey
	} else {
		for existingKey := range headers {
			if codexSessionHeaderKey(existingKey) {
				selectedKey = existingKey
				break
			}
		}
	}
	if selectedKey == "" {
		selectedKey = fallbackKey
	}
	for existingKey := range headers {
		if codexSessionHeaderKey(existingKey) && existingKey != selectedKey {
			delete(headers, existingKey)
		}
	}
	headers[selectedKey] = []string{value}
}

func setCodexSessionHeader(headers http.Header, key string, value string) {
	if headers == nil {
		return
	}
	for existingKey := range headers {
		if codexSessionHeaderKey(existingKey) {
			delete(headers, existingKey)
		}
	}
	setHeaderCasePreserved(headers, key, value)
}

func codexSessionHeaderKey(key string) bool {
	normalized := strings.ToLower(strings.TrimSpace(key))
	return normalized == "session_id" || normalized == "session-id"
}

func headerValueCaseInsensitive(headers http.Header, key string) string {
	key = strings.TrimSpace(key)
	if headers == nil || key == "" {
		return ""
	}
	if val := strings.TrimSpace(headers.Get(key)); val != "" {
		return val
	}
	for existingKey, values := range headers {
		if !strings.EqualFold(existingKey, key) {
			continue
		}
		for _, value := range values {
			if trimmed := strings.TrimSpace(value); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func deleteHeaderCaseInsensitive(headers http.Header, key string) {
	for existingKey := range headers {
		if strings.EqualFold(existingKey, key) {
			delete(headers, existingKey)
		}
	}
}

func codexHeaderDefaults(cfg *config.Config, auth *cliproxyauth.Auth) (string, string) {
	if cfg == nil || auth == nil {
		return "", ""
	}
	if auth.Attributes != nil {
		if v := strings.TrimSpace(auth.Attributes["api_key"]); v != "" {
			return "", ""
		}
	}
	return strings.TrimSpace(cfg.CodexHeaderDefaults.UserAgent), strings.TrimSpace(cfg.CodexHeaderDefaults.BetaFeatures)
}

func ensureHeaderWithPriority(target http.Header, source http.Header, key, configValue, fallbackValue string) {
	if target == nil {
		return
	}
	if strings.TrimSpace(target.Get(key)) != "" {
		return
	}
	if source != nil {
		if val := strings.TrimSpace(source.Get(key)); val != "" {
			target.Set(key, val)
			return
		}
	}
	if val := strings.TrimSpace(configValue); val != "" {
		target.Set(key, val)
		return
	}
	if val := strings.TrimSpace(fallbackValue); val != "" {
		target.Set(key, val)
	}
}

func ensureHeaderWithConfigPrecedence(target http.Header, source http.Header, key, configValue, fallbackValue string) {
	if target == nil {
		return
	}
	if strings.TrimSpace(target.Get(key)) != "" {
		return
	}
	if val := strings.TrimSpace(configValue); val != "" {
		target.Set(key, val)
		return
	}
	if source != nil {
		if val := strings.TrimSpace(source.Get(key)); val != "" {
			target.Set(key, val)
			return
		}
	}
	if val := strings.TrimSpace(fallbackValue); val != "" {
		target.Set(key, val)
	}
}

type statusErrWithHeaders struct {
	statusErr
	headers http.Header
}

type codexWebsocketTranscriptReplayRequiredError struct {
	reason string
	cause  error
}

func (e codexWebsocketTranscriptReplayRequiredError) Error() string {
	reason := strings.TrimSpace(e.reason)
	if reason == "" {
		reason = "upstream_reset"
	}
	msg := "codex websocket upstream reset requires transcript replay: invalid_request_error"
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", msg, reason, e.cause)
	}
	return fmt.Sprintf("%s: %s", msg, reason)
}

func (e codexWebsocketTranscriptReplayRequiredError) Unwrap() error { return e.cause }

func (e codexWebsocketTranscriptReplayRequiredError) StatusCode() int { return http.StatusBadRequest }

func (e codexWebsocketTranscriptReplayRequiredError) IsRequestScoped() bool { return true }

func (e codexWebsocketTranscriptReplayRequiredError) CodexWebsocketReplayRequired() bool { return true }

func (e statusErrWithHeaders) Headers() http.Header {
	if e.headers == nil {
		return nil
	}
	return e.headers.Clone()
}

func parseCodexWebsocketError(payload []byte) (error, bool) {
	if len(payload) == 0 {
		return nil, false
	}
	if strings.TrimSpace(gjson.GetBytes(payload, "type").String()) != "error" {
		return nil, false
	}
	status := int(gjson.GetBytes(payload, "status").Int())
	if status == 0 {
		status = int(gjson.GetBytes(payload, "status_code").Int())
	}
	if status <= 0 {
		return nil, false
	}

	out := buildCodexWebsocketErrorPayload(payload, status)
	headers := parseCodexWebsocketErrorHeaders(payload)
	statusError := statusErr{code: status, msg: string(out)}
	if retryAfter := parseCodexRetryAfter(status, out, time.Now()); retryAfter != nil {
		statusError.retryAfter = retryAfter
	} else if isCodexWebsocketConnectionLimitError(payload) {
		retryAfter := time.Duration(0)
		statusError.retryAfter = &retryAfter
	}
	return statusErrWithHeaders{
		statusErr: statusError,
		headers:   headers,
	}, true
}

func clearCodexReasoningReplayOnWebsocketError(ctx context.Context, scope codexReasoningReplayScope, payload []byte) error {
	status := int(gjson.GetBytes(payload, "status").Int())
	if status == 0 {
		status = int(gjson.GetBytes(payload, "status_code").Int())
	}
	if status <= 0 {
		return nil
	}
	return clearCodexReasoningReplayOnInvalidSignature(ctx, scope, status, buildCodexWebsocketErrorPayload(payload, status))
}

func clearCodexReasoningReplayOnWebsocketTerminalError(ctx context.Context, scope codexReasoningReplayScope, payload []byte) error {
	streamErr, terminalBody, ok := codexTerminalFailureErr(payload)
	if !ok {
		return nil
	}
	return clearCodexReasoningReplayOnInvalidSignature(ctx, scope, streamErr.StatusCode(), terminalBody)
}

func exposeCodexWebsocketError(payload []byte, identityState codexIdentityConfuseState, fallback error) error {
	clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
	if clientErr, ok := parseCodexWebsocketError(clientPayload); ok {
		return clientErr
	}
	return fallback
}

func parseCodexWebsocketResponseFailed(payload []byte) (statusErr, bool) {
	if len(payload) == 0 || strings.TrimSpace(gjson.GetBytes(payload, "type").String()) != "response.failed" {
		return statusErr{}, false
	}
	if streamErr, _, ok := codexTerminalStreamErr(payload); ok {
		return streamErr, true
	}

	body := codexTerminalErrorBody(payload, "response.error")
	if len(body) == 0 {
		body = codexTerminalErrorBody(payload, "error")
	}
	if len(body) == 0 {
		body = []byte(`{"error":{"message":"response.failed event received"}}`)
	}
	return newCodexStatusErr(codexWebsocketResponseFailedStatus(payload, body), body), true
}

func parseCodexWebsocketResponseIncomplete(payload []byte) (statusErr, bool) {
	if len(payload) == 0 || strings.TrimSpace(gjson.GetBytes(payload, "type").String()) != "response.incomplete" {
		return statusErr{}, false
	}
	reason := strings.TrimSpace(gjson.GetBytes(payload, "response.incomplete_details.reason").String())
	if reason == "" {
		reason = "unknown"
	}
	body := []byte(`{"error":{"type":"invalid_request_error","code":"response_incomplete"}}`)
	body, _ = sjson.SetBytes(body, "error.message", fmt.Sprintf("Incomplete response returned, reason: %s", reason))
	return newCodexStatusErr(http.StatusBadRequest, body), true
}

func parseCodexWebsocketTerminalResponseError(payload []byte) (statusErr, string, bool) {
	if streamErr, ok := parseCodexWebsocketResponseFailed(payload); ok {
		return streamErr, "response_failed", true
	}
	if streamErr, ok := parseCodexWebsocketResponseIncomplete(payload); ok {
		return streamErr, "response_incomplete", true
	}
	return statusErr{}, "", false
}

func codexWebsocketResponseFailedStatus(payload, body []byte) int {
	for _, path := range []string{"status", "status_code", "response.status_code", "response.error.status", "response.error.status_code", "error.status", "error.status_code"} {
		status := int(gjson.GetBytes(payload, path).Int())
		if status > 0 {
			return status
		}
	}

	errType := strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "error.type").String()))
	errCode := strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "error.code").String()))
	switch {
	case isCodexUsageLimitError(body) || isCodexModelCapacityError(body) || errType == "rate_limit_error" || errCode == "rate_limit_exceeded" || errCode == "insufficient_quota":
		return http.StatusTooManyRequests
	case errType == "authentication_error":
		return http.StatusUnauthorized
	case errType == "permission_error":
		return http.StatusForbidden
	case errType == "invalid_request_error",
		errCode == "invalid_request_error",
		errCode == "previous_response_not_found",
		errCode == "context_length_exceeded",
		errCode == "context_too_large",
		errCode == "invalid_prompt",
		errCode == "bio_policy",
		errCode == "cyber_policy":
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

func isCodexWebsocketPreviousResponseNotFound(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	lower := strings.ToLower(strings.TrimSpace(string(payload)))
	upstreamCode := strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "error.code").String()))
	return upstreamCode == "previous_response_not_found" ||
		strings.Contains(lower, "previous_response_not_found") ||
		(strings.Contains(lower, "previous_response") || strings.Contains(lower, "previous response")) && strings.Contains(lower, "not found")
}

func shouldDropCodexWebsocketUpstreamErrorQuietly(payload []byte, err error) bool {
	if isCodexWebsocketPreviousResponseNotFound(payload) {
		return true
	}
	switch codexWebsocketErrorStatusCode(err) {
	case http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden, http.StatusTooManyRequests:
		return true
	default:
		return false
	}
}

func codexWebsocketUpstreamErrorDropReason(payload []byte, err error) string {
	if isCodexWebsocketPreviousResponseNotFound(payload) {
		return "previous_response_not_found"
	}
	switch codexWebsocketErrorStatusCode(err) {
	case http.StatusUnauthorized:
		return "upstream_unauthorized"
	case http.StatusPaymentRequired:
		return "upstream_payment_required"
	case http.StatusForbidden:
		return "upstream_forbidden"
	case http.StatusTooManyRequests:
		return "upstream_rate_limited"
	default:
		return "upstream_error"
	}
}

func codexWebsocketErrorStatusCode(err error) int {
	if err == nil {
		return 0
	}
	var statusProvider interface{ StatusCode() int }
	if errors.As(err, &statusProvider) && statusProvider != nil {
		return statusProvider.StatusCode()
	}
	return 0
}

func codexWebsocketRequestNeedsTranscriptReplayOnReset(payload []byte) bool {
	return strings.TrimSpace(gjson.GetBytes(payload, "previous_response_id").String()) != ""
}

func codexWebsocketReadErrorRequiresTranscriptReplay(payload []byte, err error, allowFullRequestReplay bool) bool {
	var upstreamReset codexWebsocketUpstreamResetError
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) && closeErr.Code == websocket.CloseMessageTooBig {
		return false
	}
	return errors.As(err, &upstreamReset) &&
		(allowFullRequestReplay || codexWebsocketRequestNeedsTranscriptReplayOnReset(payload))
}

func buildCodexWebsocketErrorPayload(payload []byte, status int) []byte {
	out := []byte(`{}`)
	out, _ = sjson.SetBytes(out, "status", status)

	if bodyNode := gjson.GetBytes(payload, "body"); bodyNode.Exists() {
		out, _ = sjson.SetRawBytes(out, "body", []byte(bodyNode.Raw))
		if bodyErrorNode := bodyNode.Get("error"); bodyErrorNode.Exists() {
			out, _ = sjson.SetRawBytes(out, "error", []byte(bodyErrorNode.Raw))
			return out
		}
	}

	if errNode := gjson.GetBytes(payload, "error"); errNode.Exists() {
		out, _ = sjson.SetRawBytes(out, "error", []byte(errNode.Raw))
		return out
	}

	out, _ = sjson.SetBytes(out, "error.type", "server_error")
	out, _ = sjson.SetBytes(out, "error.message", http.StatusText(status))
	return out
}

func isCodexWebsocketConnectionLimitError(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	for _, path := range []string{"error.code", "error.type", "body.error.code", "body.error.type", "code", "error"} {
		if strings.TrimSpace(gjson.GetBytes(payload, path).String()) == "websocket_connection_limit_reached" {
			return true
		}
	}
	return false
}

func parseCodexWebsocketErrorHeaders(payload []byte) http.Header {
	headersNode := gjson.GetBytes(payload, "headers")
	if !headersNode.Exists() || !headersNode.IsObject() {
		return nil
	}
	mapped := make(http.Header)
	headersNode.ForEach(func(key, value gjson.Result) bool {
		name := strings.TrimSpace(key.String())
		if name == "" {
			return true
		}
		switch value.Type {
		case gjson.String:
			if v := strings.TrimSpace(value.String()); v != "" {
				mapped.Set(name, v)
			}
		case gjson.Number, gjson.True, gjson.False:
			if v := strings.TrimSpace(value.Raw); v != "" {
				mapped.Set(name, v)
			}
		default:
		}
		return true
	})
	if len(mapped) == 0 {
		return nil
	}
	return mapped
}

func normalizeCodexWebsocketCompletion(payload []byte) []byte {
	if strings.TrimSpace(gjson.GetBytes(payload, "type").String()) == "response.done" {
		updated, err := sjson.SetBytes(payload, "type", "response.completed")
		if err == nil && len(updated) > 0 {
			return updated
		}
	}
	return payload
}

func encodeCodexWebsocketAsSSE(payload []byte) []byte {
	if len(payload) == 0 {
		return nil
	}
	line := make([]byte, 0, len("data: ")+len(payload))
	line = append(line, []byte("data: ")...)
	line = append(line, payload...)
	return line
}

func websocketUpgradeRequestLog(info helps.UpstreamRequestLog) helps.UpstreamRequestLog {
	upgradeInfo := info
	upgradeInfo.URL = helps.WebsocketUpgradeRequestURL(info.URL)
	upgradeInfo.Method = http.MethodGet
	upgradeInfo.Body = nil
	upgradeInfo.Headers = info.Headers.Clone()
	if upgradeInfo.Headers == nil {
		upgradeInfo.Headers = make(http.Header)
	}
	if strings.TrimSpace(upgradeInfo.Headers.Get("Connection")) == "" {
		upgradeInfo.Headers.Set("Connection", "Upgrade")
	}
	if strings.TrimSpace(upgradeInfo.Headers.Get("Upgrade")) == "" {
		upgradeInfo.Headers.Set("Upgrade", "websocket")
	}
	return upgradeInfo
}

func recordAPIWebsocketHandshake(ctx context.Context, cfg *config.Config, resp *http.Response) {
	if resp == nil {
		return
	}
	helps.RecordAPIWebsocketHandshake(ctx, cfg, resp.StatusCode, resp.Header.Clone())
	closeHTTPResponseBody(resp, "codex websockets executor: close handshake response body error")
}

func websocketHandshakeBody(resp *http.Response) []byte {
	if resp == nil || resp.Body == nil {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	closeHTTPResponseBody(resp, "codex websockets executor: close handshake response body error")
	if len(body) == 0 {
		return nil
	}
	return body
}

func closeHTTPResponseBody(resp *http.Response, logPrefix string) {
	if resp == nil || resp.Body == nil {
		return
	}
	if errClose := resp.Body.Close(); errClose != nil {
		log.Errorf("%s: %v", logPrefix, errClose)
	}
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

func (e *CodexWebsocketsExecutor) SendResponseProcessed(sessionID string, responseID string) error {
	sessionID = strings.TrimSpace(sessionID)
	responseID = strings.TrimSpace(responseID)
	if e == nil || sessionID == "" || responseID == "" {
		return nil
	}
	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	store.mu.Lock()
	sess := store.sessions[sessionID]
	store.mu.Unlock()
	if sess == nil {
		return fmt.Errorf("codex websockets executor: session is unavailable")
	}
	sess.connMu.Lock()
	conn := sess.conn
	sess.connMu.Unlock()
	if conn == nil {
		return fmt.Errorf("codex websockets executor: upstream websocket is unavailable")
	}
	payload, errSet := sjson.SetBytes([]byte(`{"type":"response.processed"}`), "response_id", responseID)
	if errSet != nil {
		return fmt.Errorf("codex websockets executor: encode response.processed: %w", errSet)
	}
	return writeCodexWebsocketMessage(sess, conn, payload)
}

func (e *CodexWebsocketsExecutor) ensureUpstreamConn(ctx context.Context, auth *cliproxyauth.Auth, sess *codexWebsocketSession, authID string, wsURL string, headers http.Header) (*websocket.Conn, *websocketConnectionCloser, *http.Response, bool, error) {
	if sess == nil {
		conn, closer, resp, err := e.dialCodexWebsocket(ctx, auth, wsURL, headers)
		return conn, closer, resp, true, err
	}

	if staleConn, staleCloser, staleAuthID, staleWSURL, staleLifecycle := detachMismatchedWebsocketSessionConn(sess, authID, wsURL); staleConn != nil {
		sess.connMu.Lock()
		sess.upstreamGeneration++
		sess.connMu.Unlock()
		logCodexWebsocketDisconnected(sess.sessionID, staleAuthID, staleWSURL, "target_changed", nil)
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
		return conn, closer, nil, false, nil
	}

	conn, closer, resp, errDial := e.dialCodexWebsocket(ctx, auth, wsURL, headers)
	if errDial != nil {
		return nil, closer, resp, false, errDial
	}

	sess.connMu.Lock()
	if sess.conn != nil {
		previous := sess.conn
		previousCloser := sess.connCloser
		sess.connMu.Unlock()
		if errClose := closer.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
		return previous, previousCloser, nil, false, nil
	}
	sess.conn = conn
	sess.connCloser = closer
	sess.wsURL = wsURL
	sess.authID = authID
	sess.readerConn = conn
	sess.connMu.Unlock()

	sess.configureConn(conn)
	go e.readUpstreamLoop(sess, conn)
	logCodexWebsocketConnected(sess.sessionID, authID, wsURL)
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
				if !e.detachUpstreamConnForRecovery(sess, conn, "upstream_disconnected", errRead) {
					return
				}
				sess.closeActiveReadForConn(conn, codexWebsocketUpstreamResetError{cause: errRead})
				return
			}
			if sess.closeActiveReadForConn(conn, errRead) {
				return
			}
			e.invalidateUpstreamConn(sess, conn, "upstream_disconnected", errRead)
			return
		}

		if msgType != websocket.TextMessage {
			if msgType == websocket.BinaryMessage {
				errBinary := fmt.Errorf("codex websockets executor: unexpected binary message")
				if sess.closeActiveReadForConn(conn, errBinary) {
					return
				}
				e.invalidateUpstreamConn(sess, conn, "unexpected_binary", errBinary)
				return
			}
			continue
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
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	if sess.pendingHandshakeConn == conn {
		sess.pendingHandshakeConn = nil
		sess.pendingHandshakeHeaders = nil
	}
	if !notifyDownstream {
		sess.upstreamGeneration++
	}
	sess.connMu.Unlock()

	logCodexWebsocketDisconnected(sessionID, authID, wsURL, reason, err)
	if notifyDownstream {
		sess.notifyUpstreamDisconnect(err)
	}
	if closer != nil {
		if errClose := closer.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
	}
	if lifecycle != nil {
		lifecycle.End(reason)
	}
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

	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
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
	sess.markTerminalError(sessionClosedErr)
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
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	sessionID := sess.sessionID
	sess.pendingHandshakeConn = nil
	sess.pendingHandshakeHeaders = nil
	sess.connMu.Unlock()

	if conn != nil {
		logCodexWebsocketDisconnected(sessionID, authID, wsURL, reason, nil)
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

func logCodexWebsocketConnected(sessionID string, authID string, wsURL string) {
	log.Infof("codex websockets: upstream connected session=%s auth=%s url=%s", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL))
}

func logCodexWebsocketDisconnected(sessionID string, authID string, wsURL string, reason string, err error) {
	if err != nil {
		log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s reason=%s err=%v", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL), strings.TrimSpace(reason), err)
		return
	}
	log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s reason=%s", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL), strings.TrimSpace(reason))
}

// CloseCodexWebsocketSessionsForAuthID closes all active Codex upstream websocket sessions
// associated with the supplied auth ID.
func CloseCodexWebsocketSessionsForAuthID(authID string, reason string) {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "auth_removed"
	}

	store := globalCodexWebsocketSessionStore
	if store == nil {
		return
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

// CodexAutoExecutor routes Codex requests to the websocket transport only when:
//  1. The downstream transport is websocket, and
//  2. The selected auth enables websockets.
//
// For non-websocket downstream requests, it always uses the legacy HTTP implementation.
type CodexAutoExecutor struct {
	httpExec *CodexExecutor
	wsExec   *CodexWebsocketsExecutor
}

func NewCodexAutoExecutor(cfg *config.Config) *CodexAutoExecutor {
	return &CodexAutoExecutor{
		httpExec: NewCodexExecutor(cfg),
		wsExec:   NewCodexWebsocketsExecutor(cfg),
	}
}

func (e *CodexAutoExecutor) Identifier() string { return "codex" }

func (e *CodexAutoExecutor) PrepareRequest(req *http.Request, auth *cliproxyauth.Auth) error {
	if e == nil || e.httpExec == nil {
		return nil
	}
	return e.httpExec.PrepareRequest(req, auth)
}

func (e *CodexAutoExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	if e == nil || e.httpExec == nil {
		return nil, fmt.Errorf("codex auto executor: http executor is nil")
	}
	return e.httpExec.HttpRequest(ctx, auth, req)
}

func (e *CodexAutoExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e == nil || e.httpExec == nil || e.wsExec == nil {
		return cliproxyexecutor.Response{}, fmt.Errorf("codex auto executor: executor is nil")
	}
	if cliproxyexecutor.DownstreamWebsocket(ctx) && codexWebsocketsEnabled(auth) {
		return e.wsExec.Execute(ctx, auth, req, opts)
	}
	if cliproxyexecutor.RequiredUpstreamWebsocket(ctx) {
		return cliproxyexecutor.Response{}, cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
	}
	return e.httpExec.Execute(ctx, auth, req, opts)
}

func (e *CodexAutoExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	if e == nil || e.httpExec == nil || e.wsExec == nil {
		return nil, fmt.Errorf("codex auto executor: executor is nil")
	}
	if cliproxyexecutor.DownstreamWebsocket(ctx) && codexWebsocketsEnabled(auth) {
		return e.wsExec.ExecuteStream(ctx, auth, req, opts)
	}
	if cliproxyexecutor.RequiredUpstreamWebsocket(ctx) {
		return nil, cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
	}
	return e.httpExec.ExecuteStream(ctx, auth, req, opts)
}

func (e *CodexAutoExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if e == nil || e.httpExec == nil {
		return nil, fmt.Errorf("codex auto executor: http executor is nil")
	}
	return e.httpExec.Refresh(ctx, auth)
}

func (e *CodexAutoExecutor) CountTokens(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e == nil || e.httpExec == nil {
		return cliproxyexecutor.Response{}, fmt.Errorf("codex auto executor: http executor is nil")
	}
	return e.httpExec.CountTokens(ctx, auth, req, opts)
}

func (e *CodexAutoExecutor) CloseExecutionSession(sessionID string) {
	if e == nil || e.wsExec == nil {
		return
	}
	e.wsExec.CloseExecutionSession(sessionID)
}

func (e *CodexAutoExecutor) UpstreamDisconnectChan(sessionID string) <-chan error {
	if e == nil || e.wsExec == nil {
		return nil
	}
	return e.wsExec.UpstreamDisconnectChan(sessionID)
}

func (e *CodexAutoExecutor) UpstreamGeneration(sessionID string) uint64 {
	if e == nil || e.wsExec == nil {
		return 0
	}
	return e.wsExec.UpstreamGeneration(sessionID)
}

func (e *CodexAutoExecutor) DropUpstreamSession(sessionID string, reason string) {
	if e == nil || e.wsExec == nil {
		return
	}
	e.wsExec.DropUpstreamSession(sessionID, reason)
}

func (e *CodexAutoExecutor) SendResponseProcessed(sessionID string, responseID string) error {
	if e == nil || e.wsExec == nil {
		return nil
	}
	return e.wsExec.SendResponseProcessed(sessionID, responseID)
}

func codexWebsocketsEnabled(auth *cliproxyauth.Auth) bool {
	if auth == nil {
		return false
	}
	if len(auth.Attributes) > 0 {
		if raw := strings.TrimSpace(auth.Attributes["websockets"]); raw != "" {
			parsed, errParse := strconv.ParseBool(raw)
			if errParse == nil {
				return parsed
			}
		}
	}
	if len(auth.Metadata) == 0 {
		return false
	}
	raw, ok := auth.Metadata["websockets"]
	if !ok || raw == nil {
		return false
	}
	switch v := raw.(type) {
	case bool:
		return v
	case string:
		parsed, errParse := strconv.ParseBool(strings.TrimSpace(v))
		if errParse == nil {
			return parsed
		}
	default:
	}
	return false
}
