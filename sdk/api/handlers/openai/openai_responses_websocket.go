package openai

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/MoeForge/moego/json"
	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	log "github.com/sirupsen/logrus"
)

const (
	wsRequestTypeCreate                   = "response.create"
	wsRequestTypeAppend                   = "response.append"
	wsEventTypeError                      = "error"
	wsEventTypeCompleted                  = "response.completed"
	wsEventTypeDone                       = "response.done"
	wsEventTypeFailed                     = "response.failed"
	wsEventTypeIncomplete                 = "response.incomplete"
	wsDoneMarker                          = "[DONE]"
	wsTurnStateHeader                     = "x-codex-turn-state"
	wsTimelineBodyKey                     = "WEBSOCKET_TIMELINE_OVERRIDE"
	wsCloseReasonMaxBytes                 = 123
	wsHTTPReplayRequiredCloseReason       = "upstream requires HTTP replay"
	responsesWebsocketUpstreamModeUnknown = ""
	responsesWebsocketUpstreamModeWS      = "websocket"
	responsesWebsocketUpstreamModeHTTP    = "http"

	wsTranscriptReplayMaxRetries      = 2
	wsResponsesLiteMetadataKey        = "ws_request_header_x_openai_internal_codex_responses_lite"
	codexLocalCompactionSummaryPrefix = "Another language model started to solve this problem and produced a summary of its thinking process. You also have access to the state of the tools that were used by that language model. Use this to build on the work that has already been done and avoid duplicating work. Here is the summary produced by the other language model, use the information in this summary to assist with your own analysis:"
)

var responsesWebsocketCompressionEnabled atomic.Bool

func init() {
	responsesWebsocketCompressionEnabled.Store(true)
}

// SetResponsesWebsocketCompressionEnabled controls compression for future upgrades.
func SetResponsesWebsocketCompressionEnabled(enabled bool) {
	responsesWebsocketCompressionEnabled.Store(enabled)
}

var responsesWebsocketUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func websocketClosePayloadForUpstreamError(err error) (bool, []byte) {
	if err == nil {
		return false, nil
	}

	errText := err.Error()
	if cliproxyexecutor.IsUpstreamWebsocketReplayRequired(err) {
		return true, websocket.FormatCloseMessage(
			websocket.CloseServiceRestart,
			truncateWebsocketCloseReason(wsHTTPReplayRequiredCloseReason, wsCloseReasonMaxBytes),
		)
	}

	code := 0
	reason := ""
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) && closeErr.Code == websocket.CloseMessageTooBig {
		code = closeErr.Code
		reason = closeErr.Text
	} else {
		type statusCoder interface {
			StatusCode() int
		}
		var statusErr statusCoder
		if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusRequestEntityTooLarge {
			return false, nil
		}
		fields := ojson.GetMany(errText, "error.code", "error.message")
		if fields[0].String() != "message_too_big" {
			return false, nil
		}
		code = websocket.CloseMessageTooBig
		reason = strings.TrimSpace(fields[1].String())
	}
	if reason == "" {
		reason = "message too big"
	}
	reason = truncateWebsocketCloseReason(reason, wsCloseReasonMaxBytes)
	return true, websocket.FormatCloseMessage(code, reason)
}

type responsesWebsocketWriter struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
	closing atomic.Bool
}

func newResponsesWebsocketWriter(conn *websocket.Conn) *responsesWebsocketWriter {
	return &responsesWebsocketWriter{conn: conn}
}

// closeForUpstreamError sends a best-effort close frame without waiting behind
// an active downstream writer. If a data or Ping write already owns writeMu,
// the connection is closed immediately so the blocked writer and session can exit.
func (w *responsesWebsocketWriter) closeForUpstreamError(err error) (bool, error) {
	if w == nil || w.conn == nil {
		return false, nil
	}
	matched, payload := websocketClosePayloadForUpstreamError(err)
	if !matched {
		return false, nil
	}
	if !w.closing.CompareAndSwap(false, true) {
		return true, nil
	}
	if !w.writeMu.TryLock() {
		return true, w.conn.Close()
	}
	defer w.writeMu.Unlock()

	errWrite := w.conn.WriteControl(websocket.CloseMessage, payload, time.Time{})
	errClose := w.conn.Close()
	if errWrite != nil {
		return true, errWrite
	}
	return true, errClose
}

func (w *responsesWebsocketWriter) closeWithoutError() (bool, error) {
	if w == nil || w.conn == nil {
		return false, nil
	}
	if !w.closing.CompareAndSwap(false, true) {
		return false, nil
	}
	return true, w.conn.Close()
}

func (w *responsesWebsocketWriter) writePing() error {
	if w == nil || w.conn == nil {
		return errors.New("responses websocket: writer is nil")
	}
	if w.closing.Load() {
		return websocket.ErrCloseSent
	}
	if !w.writeMu.TryLock() {
		if w.closing.Load() {
			return websocket.ErrCloseSent
		}
		return nil
	}
	defer w.writeMu.Unlock()
	if w.closing.Load() {
		return websocket.ErrCloseSent
	}
	return w.conn.WriteControl(websocket.PingMessage, nil, time.Time{})
}

func (w *responsesWebsocketWriter) keepAlive(ctx context.Context, cancel context.CancelCauseFunc, intervals <-chan time.Duration) {
	w.keepAliveWithPredicate(ctx, cancel, intervals, nil)
}

func (w *responsesWebsocketWriter) keepAliveWithPredicate(ctx context.Context, cancel context.CancelCauseFunc, intervals <-chan time.Duration, shouldPing func() bool) {
	if errPing := runResponsesWebsocketKeepAliveWithTicker(ctx, intervals, shouldPing, w.writePing, nil); errPing != nil {
		cancel(errPing)
		_, _ = w.closeWithoutError()
	}
}

func runResponsesWebsocketKeepAlive(ctx context.Context, intervals <-chan time.Duration, ping func() error) error {
	return runResponsesWebsocketKeepAliveWithTicker(ctx, intervals, nil, ping, nil)
}

func runResponsesWebsocketKeepAliveWithTicker(
	ctx context.Context,
	intervals <-chan time.Duration,
	shouldPing func() bool,
	ping func() error,
	newTicker func(time.Duration) (<-chan time.Time, func()),
) error {
	var stopTicker func()
	var ticks <-chan time.Time
	stopCurrent := func() {
		if stopTicker != nil {
			stopTicker()
			stopTicker = nil
		}
		ticks = nil
	}
	defer stopCurrent()
	for {
		select {
		case <-ctx.Done():
			return nil
		case interval := <-intervals:
			if interval <= 0 {
				stopCurrent()
				continue
			}
			stopCurrent()
			if newTicker != nil {
				ticks, stopTicker = newTicker(interval)
			} else {
				ticker := time.NewTicker(interval)
				ticks = ticker.C
				stopTicker = ticker.Stop
			}
		case <-ticks:
			if shouldPing != nil && !shouldPing() {
				stopCurrent()
				continue
			}
			if errPing := ping(); errPing != nil {
				return errPing
			}
		}
	}
}

type responsesWebsocketContinuityObserver struct {
	setActive func(bool)
}

func (o responsesWebsocketContinuityObserver) OnContinuityWaitStart() {
	if o.setActive != nil {
		o.setActive(true)
	}
}

func (o responsesWebsocketContinuityObserver) OnContinuityWaitEnd() {
	if o.setActive != nil {
		o.setActive(false)
	}
}

func queueResponsesWebsocketKeepAliveInterval(intervals chan time.Duration, interval time.Duration) {
	// A single producer replaces a pending interval without waiting for a Ping write.
	select {
	case <-intervals:
	default:
	}
	intervals <- interval
}

func (w *responsesWebsocketWriter) closeWithPayload(payload []byte) (bool, error) {
	if w == nil || w.conn == nil {
		return false, nil
	}
	if !w.closing.CompareAndSwap(false, true) {
		return false, nil
	}
	if !w.writeMu.TryLock() {
		return false, w.conn.Close()
	}
	defer w.writeMu.Unlock()

	errWrite := w.conn.WriteMessage(websocket.TextMessage, payload)
	errClose := w.conn.Close()
	if errWrite != nil {
		return false, errWrite
	}
	return true, errClose
}

func (w *responsesWebsocketWriter) closeForUpstreamDisconnect(err error, wsTimelineLog ...websocketTimelineAppender) bool {
	if w == nil || w.conn == nil {
		return false
	}
	var timelineLog websocketTimelineAppender
	if len(wsTimelineLog) > 0 {
		timelineLog = wsTimelineLog[0]
	}
	if matched, _ := w.closeForUpstreamError(err); matched {
		return true
	}

	errMsg := handlers.ExecutionErrorMessage(err)
	if !shouldExposeResponsesUpstreamError(errMsg) {
		closed, _ := w.closeWithoutError()
		if closed && timelineLog != nil && errMsg != nil {
			appendWebsocketTimelineDisconnect(timelineLog, errMsg.Error, time.Now())
		}
		return closed
	}
	payload, errBuild := buildResponsesWebsocketErrorPayload(errMsg)
	if errBuild != nil {
		closed, _ := w.closeWithoutError()
		return closed
	}
	wrote, errClose := w.closeWithPayload(payload)
	if wrote {
		if timelineLog != nil {
			timelineLog.Append("response", payload, time.Now())
		}
		logResponsesWebsocketDownstreamError("", payload)
	}
	if errClose != nil && !errors.Is(errClose, websocket.ErrCloseSent) {
		log.Debugf("responses websocket: upstream disconnect close failed: %v", errClose)
	}
	return wrote
}

// isWebsocketConnectionClosedError reports whether the error only means the
// connection was already torn down. These are expected during shutdown races
// (the proxy closes after sending a terminal frame, or the client hangs up mid
// write) and must not be logged as proxy failures.
func isWebsocketConnectionClosedError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, websocket.ErrCloseSent) {
		return true
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}

func truncateWebsocketCloseReason(reason string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(reason) <= maxBytes && utf8.ValidString(reason) {
		return reason
	}

	// Decode from the front so work and output stay bounded by maxBytes.
	var truncated strings.Builder
	truncated.Grow(min(len(reason), maxBytes))
	remaining := maxBytes
	runeErrorSize := utf8.RuneLen(utf8.RuneError)
	for len(reason) > 0 && remaining > 0 {
		r, size := utf8.DecodeRuneInString(reason)
		if r == utf8.RuneError && size == 1 {
			if runeErrorSize > remaining {
				break
			}
			truncated.WriteRune(utf8.RuneError)
			reason = reason[1:]
			remaining -= runeErrorSize
			continue
		}
		if size > remaining {
			break
		}
		truncated.WriteString(reason[:size])
		reason = reason[size:]
		remaining -= size
	}
	return truncated.String()
}

func (h *OpenAIResponsesAPIHandler) authenticateResponsesWebsocketTurn(c *gin.Context, baseReq *http.Request, payload []byte, requestModelName string, updateRequestContext bool) (string, *interfaces.ErrorMessage) {
	if h == nil || h.AccessManager == nil {
		return "", nil
	}
	sourceReq := baseReq
	if sourceReq == nil && c != nil {
		sourceReq = c.Request
	}
	if sourceReq == nil {
		return "", nil
	}
	authBody := payload
	if requestModelName != "" && json.Valid(payload) && strings.TrimSpace(ojson.GetBytesNoCopy(payload, "model").String()) == "" {
		doc := ojson.NewDoc(payload)
		if errSet := doc.Set("model", requestModelName); errSet == nil {
			authBody = doc.Bytes()
		}
	}
	req := sourceReq.Clone(sourceReq.Context())
	req.Method = http.MethodPost
	if req.Header != nil {
		for key := range req.Header {
			lower := strings.ToLower(key)
			if lower == "upgrade" || lower == "connection" || lower == "content-length" || strings.HasPrefix(lower, "sec-websocket-") {
				delete(req.Header, key)
			}
		}
	}
	req.Body = io.NopCloser(bytes.NewReader(authBody))
	req.ContentLength = int64(len(authBody))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(authBody)), nil
	}
	result, authErr := h.AccessManager.Authenticate(req.Context(), req)
	if authErr != nil {
		msg := strings.TrimSpace(authErr.Message)
		if msg == "" {
			msg = authErr.Error()
		}
		return "", &interfaces.ErrorMessage{
			StatusCode: authErr.HTTPStatusCode(),
			Error:      errors.New(msg),
		}
	}
	if result == nil {
		return "", nil
	}
	if c != nil && updateRequestContext {
		c.Set("userApiKey", result.Principal)
		c.Set("accessProvider", result.Provider)
		if len(result.Metadata) > 0 {
			c.Set("accessMetadata", result.Metadata)
		} else {
			c.Set("accessMetadata", nil)
		}
		if c.Request != nil {
			reqCtx := usage.WithAPIKey(c.Request.Context(), result.Principal)
			reqCtx = usage.WithAccessProvider(reqCtx, result.Provider)
			reqCtx = usage.WithIsNativeKey(reqCtx, usage.IsNativeAccessProvider(result.Provider))
			c.Request = c.Request.WithContext(reqCtx)
		}
	}
	return result.Principal, nil
}

// ResponsesWebsocket handles websocket requests for /v1/responses.
// It accepts `response.create` and `response.append` requests and streams
// response events back as JSON websocket text messages. `response.interrupt`
// is forwarded to the current Codex upstream socket without starting a new turn.
func (h *OpenAIResponsesAPIHandler) ResponsesWebsocket(c *gin.Context) {
	upgrader := responsesWebsocketUpgrader
	upgrader.EnableCompression = responsesWebsocketCompressionEnabled.Load()
	conn, err := upgrader.Upgrade(c.Writer, c.Request, websocketUpgradeHeaders(c.Request))
	if err != nil {
		return
	}
	writer := newResponsesWebsocketWriter(conn)
	passthroughSessionID := uuid.NewString()
	// After the upgrade, error-only HTTP logs cannot consume deferred bodies.
	// Apply the policy before routing so plugin callbacks inherit it as well.
	socketCtx, cancelSocket := context.WithCancelCause(logging.WithDeferredAPIRequestCaptureDisabled(c.Request.Context()))
	defer cancelSocket(nil)
	c.Request = c.Request.WithContext(socketCtx)
	upgradeRequest := c.Request.Clone(socketCtx)
	// Start Ping at upgrade, before opening an upstream stream can block.
	var keepAliveMu sync.Mutex
	var keepAliveIntervals chan time.Duration
	var keepAliveInterval time.Duration
	var keepAliveActive atomic.Bool
	var continuityWaitCount int
	updateKeepAliveLocked := func() {
		if h == nil {
			return
		}
		interval := handlers.StreamingKeepAliveInterval(h.CurrentConfig())
		if interval <= 0 && continuityWaitCount > 0 {
			interval = handlers.ContinuityKeepAliveInterval(h.CurrentConfig())
		}
		keepAliveActive.Store(interval > 0)
		if interval == keepAliveInterval {
			return
		}
		keepAliveInterval = interval
		if keepAliveIntervals == nil {
			keepAliveIntervals = make(chan time.Duration, 1)
			go writer.keepAliveWithPredicate(socketCtx, cancelSocket, keepAliveIntervals, keepAliveActive.Load)
		}
		queueResponsesWebsocketKeepAliveInterval(keepAliveIntervals, interval)
	}
	updateKeepAlive := func() {
		keepAliveMu.Lock()
		defer keepAliveMu.Unlock()
		updateKeepAliveLocked()
	}
	setContinuityWaitActive := func(active bool) {
		keepAliveMu.Lock()
		defer keepAliveMu.Unlock()
		wasActive := continuityWaitCount > 0
		if active {
			continuityWaitCount++
		} else if continuityWaitCount > 0 {
			continuityWaitCount--
		}
		nowActive := continuityWaitCount > 0
		if wasActive == nowActive {
			return
		}
		updateKeepAliveLocked()
	}
	updateKeepAlive()
	// The reader owns the client socket so an in-flight interrupt is not stuck
	// behind the response currently being forwarded.
	requestLogEnabled := false
	codexResponseSteering := false
	if h != nil {
		if cfg := h.CurrentConfig(); cfg != nil {
			requestLogEnabled = cfg.RequestLog
			codexResponseSteering = cfg.CodexResponseSteering
		}
	}
	wsTimelineLog := newWebsocketTimelineLog(requestLogEnabled, websocketTimelineSourceFromContext(c))
	localInterrupt := newResponsesLocalInterrupt()
	interruptCtx := cliproxyexecutor.WithWebsocketInterruptAckHandler(socketCtx, func(payload []byte) {
		if errWrite := writeResponsesWebsocketPayload(writer, wsTimelineLog, payload, time.Now()); errWrite != nil {
			cancelSocket(errWrite)
		}
	})
	input, inputDone := readResponsesWebsocketInput(socketCtx, cancelSocket, conn, func(payload []byte) error {
		return h.forwardResponsesWebsocketInterrupt(interruptCtx, passthroughSessionID, payload)
	}, localInterrupt, writer, wsTimelineLog)
	var duplexInput <-chan cliproxyexecutor.WebsocketInput
	if codexResponseSteering {
		duplexInput = input
	}
	downstreamSessionKey := websocketDownstreamSessionKey(c.Request)
	retainResponsesWebsocketToolCaches(downstreamSessionKey)
	clientIP := websocketClientAddress(c)
	log.Infof("responses websocket: client connected id=%s remote=%s", passthroughSessionID, clientIP)

	wsDone := make(chan struct{})
	var disconnectWg sync.WaitGroup
	var recordedDisconnectErr atomic.Pointer[interfaces.ErrorMessage]
	var codexDuplexStream atomic.Bool

	type upstreamGenerationProvider interface {
		UpstreamGeneration(sessionID string) uint64
	}
	type upstreamSessionDropper interface {
		DropUpstreamSession(sessionID string, reason string)
	}
	type upstreamDisconnectSubscriber interface {
		UpstreamDisconnectChan(sessionID string) <-chan error
	}
	type upstreamDisconnectGenerationProvider interface {
		UpstreamDisconnectGeneration(sessionID string) uint64
	}
	type upstreamDisconnectSubscription struct {
		provider     string
		disconnectCh <-chan error
	}
	type activeUpstreamState struct {
		provider             string
		disconnectGeneration uint64
	}
	upstreamGenerations := make(map[string]upstreamGenerationProvider)
	upstreamSessionDroppers := make(map[string]upstreamSessionDropper)
	upstreamDisconnectGenerations := make(map[string]upstreamDisconnectGenerationProvider)
	var upstreamDisconnectSubscriptions []upstreamDisconnectSubscription
	if h != nil && h.AuthManager != nil {
		for _, provider := range []string{"codex", "xai"} {
			exec, ok := h.AuthManager.Executor(provider)
			if !ok || exec == nil {
				continue
			}
			if generationProvider, ok := exec.(upstreamGenerationProvider); ok && generationProvider != nil {
				upstreamGenerations[provider] = generationProvider
			}
			if dropper, ok := exec.(upstreamSessionDropper); ok && dropper != nil {
				upstreamSessionDroppers[provider] = dropper
			}
			if generationProvider, ok := exec.(upstreamDisconnectGenerationProvider); ok && generationProvider != nil {
				upstreamDisconnectGenerations[provider] = generationProvider
			}
			if subscriber, ok := exec.(upstreamDisconnectSubscriber); ok && subscriber != nil {
				disconnectCh := subscriber.UpstreamDisconnectChan(passthroughSessionID)
				if disconnectCh != nil {
					upstreamDisconnectSubscriptions = append(upstreamDisconnectSubscriptions, upstreamDisconnectSubscription{
						provider:     provider,
						disconnectCh: disconnectCh,
					})
				}
			}
		}
	}

	var activeUpstreamProvider atomic.Value
	activeUpstreamProvider.Store(activeUpstreamState{})
	activateUpstreamProvider := func(provider string) {
		if activeUpstreamProvider.Load().(activeUpstreamState).provider == provider {
			return
		}
		state := activeUpstreamState{provider: provider}
		if generationProvider := upstreamDisconnectGenerations[provider]; generationProvider != nil {
			state.disconnectGeneration = generationProvider.UpstreamDisconnectGeneration(passthroughSessionID)
		}
		activeUpstreamProvider.Store(state)
	}
	for _, subscription := range upstreamDisconnectSubscriptions {
		disconnectWg.Add(1)
		go func(subscription upstreamDisconnectSubscription) {
			defer disconnectWg.Done()
			for {
				select {
				case <-wsDone:
					return
				case disconnectErr, ok := <-subscription.disconnectCh:
					if !ok {
						return
					}
					if subscription.provider == "codex" && codexDuplexStream.Load() {
						// The selected duplex stream drains acknowledgements and events
						// before closing. Keep watching for later non-duplex selections.
						continue
					}
					disconnectGeneration := uint64(0)
					if event, versioned := disconnectErr.(interface{ UpstreamDisconnectGeneration() uint64 }); versioned {
						disconnectGeneration = event.UpstreamDisconnectGeneration()
					}
					active := activeUpstreamProvider.Load().(activeUpstreamState)
					if active.provider == subscription.provider &&
						(disconnectGeneration == 0 || disconnectGeneration > active.disconnectGeneration) {
						if writer.closeForUpstreamDisconnect(disconnectErr, wsTimelineLog) && disconnectErr != nil {
							recordedDisconnectErr.CompareAndSwap(nil, handlers.ExecutionErrorMessage(disconnectErr))
						}
						return
					}
				}
			}
		}(subscription)
	}

	var wsTerminateErr error
	defer func() {
		releaseResponsesWebsocketToolCaches(downstreamSessionKey)
		if wsTerminateErr != nil {
			appendWebsocketTimelineDisconnect(wsTimelineLog, wsTerminateErr, time.Now())
			// log.Infof("responses websocket: session closing id=%s reason=%v", passthroughSessionID, wsTerminateErr)
		} else {
			log.Infof("responses websocket: session closing id=%s", passthroughSessionID)
		}
		if h != nil && h.AuthManager != nil {
			h.AuthManager.CloseExecutionSession(passthroughSessionID)
			log.Infof("responses websocket: upstream execution session closed id=%s", passthroughSessionID)
		}
		close(wsDone)
		cancelSocket(nil)
		_, _ = writer.closeWithoutError()
		if errClose := conn.Close(); errClose != nil && !isWebsocketConnectionClosedError(errClose) {
			log.Warnf("responses websocket: close connection error: %v", errClose)
		}
		disconnectWg.Wait()
		if errMsg := recordedDisconnectErr.Load(); errMsg != nil {
			h.LoggingAPIResponseError(c, errMsg)
		}
		// Finish control-frame diagnostics before snapshotting the timeline.
		<-inputDone
		wsTimelineLog.SetContext(c)
	}()

	var lastRequest []byte
	lastResponseOutput := []byte("[]")
	var observedCompaction responsesWebsocketObservedCompactionState
	localCompactionResponseID := ""
	// Remains pending until a generating request commits successfully.
	pendingPrewarmID := ""
	pinnedAuthID := ""
	// Preserve independent upstream auth affinity when a downstream session switches providers.
	pinnedAuthByProvider := make(map[string]responsesWebsocketPinnedAuthState)
	passthroughModelName := ""
	upstreamMode := responsesWebsocketUpstreamModeUnknown
	upstreamProvider := ""
	upstreamAuthID := ""
	seenUpstreamGenerations := make(map[string]uint64, len(upstreamGenerations))
	for provider, generationProvider := range upstreamGenerations {
		seenUpstreamGenerations[provider] = generationProvider.UpstreamGeneration(passthroughSessionID)
	}
	seenUpstreamDisconnectGenerations := make(map[string]uint64, len(upstreamDisconnectGenerations))
	for provider, generationProvider := range upstreamDisconnectGenerations {
		seenUpstreamDisconnectGenerations[provider] = generationProvider.UpstreamDisconnectGeneration(passthroughSessionID)
	}
	sessionAuthByIDWithSource := func(authID string) (*coreauth.Auth, bool, bool) {
		if h == nil || h.AuthManager == nil {
			return nil, false, false
		}
		// Prefer the current manager view so hot-reloaded transport eligibility is
		// observed even when the execution session still holds an older auth snapshot.
		if auth, ok := h.AuthManager.GetByID(authID); ok {
			return auth, false, true
		}
		if auth, ok := h.AuthManager.GetExecutionSessionAuthByID(passthroughSessionID, authID); ok {
			return auth, true, true
		}
		return nil, false, false
	}
	sessionAuthByID := func(authID string) (*coreauth.Auth, bool) {
		auth, _, ok := sessionAuthByIDWithSource(authID)
		return auth, ok
	}
	upstreamModeForAuth := func(auth *coreauth.Auth) string {
		if auth != nil && websocketUpstreamSupportsIncrementalInput(auth.Attributes, auth.Metadata) {
			provider := strings.ToLower(strings.TrimSpace(auth.Provider))
			if provider == "codex" || provider == "xai" {
				return responsesWebsocketUpstreamModeWS
			}
		}
		return responsesWebsocketUpstreamModeHTTP
	}
	rememberPinnedAuth := func(authID string, modelName string) {
		authID = strings.TrimSpace(authID)
		auth, ok := sessionAuthByID(authID)
		if authID == "" || !ok || auth == nil {
			return
		}
		pinnedAuthID = authID
		providerKey := strings.ToLower(strings.TrimSpace(auth.Provider))
		_, modelKey := responsesWebsocketProviderSetForModel(responsesWebsocketResolvedModelName(modelName))
		if providerKey != "" {
			pinnedAuthByProvider[providerKey] = responsesWebsocketPinnedAuthState{authID: authID, modelKey: modelKey}
		}
	}
	forgetPinnedAuth := func() {
		for providerKey, state := range pinnedAuthByProvider {
			if state.authID == pinnedAuthID {
				delete(pinnedAuthByProvider, providerKey)
			}
		}
		pinnedAuthID = ""
	}

	for {
		var msgType int
		var payload []byte
		var errReadMessage error
		select {
		case message, ok := <-input:
			if !ok {
				errReadMessage = context.Cause(socketCtx)
			} else {
				msgType, payload, errReadMessage = websocket.TextMessage, message.Payload, message.Err
			}
		case <-socketCtx.Done():
			errReadMessage = context.Cause(socketCtx)
		}
		if errReadMessage != nil && errors.Is(errReadMessage, context.Canceled) {
			return
		}
		if errReadMessage != nil {
			wsTerminateErr = errReadMessage
			if websocket.IsCloseError(errReadMessage, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseNoStatusReceived) {
				log.Infof("responses websocket: client disconnected id=%s error=%v", passthroughSessionID, errReadMessage)
			} else {
				// log.Warnf("responses websocket: read message failed id=%s error=%v", passthroughSessionID, errReadMessage)
			}
			return
		}
		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			continue
		}
		updateKeepAlive()
		// log.Infof(
		// 	"responses websocket: downstream_in id=%s type=%d event=%s payload=%s",
		// 	passthroughSessionID,
		// 	msgType,
		// 	websocketPayloadEventType(payload),
		// 	websocketPayloadPreview(payload),
		// )
		wsTimelineLog.BeginRequest()
		wsTimelineLog.Append("request", payload, time.Now())
		requestInput := ojson.GetBytes(payload, "input")
		requestInputContainsFullTranscript := inputContainsFullTranscript(requestInput)
		requestReplacesTranscript := requestInputContainsFullTranscript ||
			responsesWebsocketRequestReplacesTranscript(payload, requestInput, lastRequest, lastResponseOutput)

		explicitRequestModelName := strings.Clone(strings.TrimSpace(ojson.GetBytesNoCopy(payload, "model").String()))
		requestModelName := explicitRequestModelName
		if requestModelName == "" {
			requestModelName = passthroughModelName
		}
		if requestModelName == "" {
			requestModelName = strings.Clone(strings.TrimSpace(ojson.GetBytesNoCopy(lastRequest, "model").String()))
		}
		if _, errAuth := h.authenticateResponsesWebsocketTurn(c, upgradeRequest, payload, requestModelName, true); errAuth != nil {
			h.LoggingAPIResponseError(context.WithValue(context.Background(), "gin", c), errAuth)
			markAPIResponseTimestamp(c)
			errorPayload, errWrite := writeResponsesWebsocketError(writer, wsTimelineLog, errAuth)
			logResponsesWebsocketDownstreamError(passthroughSessionID, errorPayload)
			if errWrite != nil {
				log.Warnf(
					"responses websocket: downstream_out write failed id=%s event=%s error=%v",
					passthroughSessionID,
					websocketPayloadEventType(errorPayload),
					errWrite,
				)
				return
			}
			continue
		}
		executionParent := context.WithValue(c.Request.Context(), "gin", c)
		executionParent, routeOverridesModelResolution := h.PrepareStreamModelRoute(
			executionParent,
			h.HandlerType(),
			requestModelName,
			payload,
		)
		pluginExecutorID := handlers.PreparedStreamPluginExecutor(executionParent)
		isPluginExecutorRoute := pluginExecutorID != ""
		routedProvider := handlers.PreparedStreamModelRouteProvider(executionParent)
		if !routeOverridesModelResolution {
			if pinnedAuthID != "" {
				pinnedAuth, homeRuntime, ok := sessionAuthByIDWithSource(pinnedAuthID)
				providerKey := ""
				if pinnedAuth != nil {
					providerKey = strings.ToLower(strings.TrimSpace(pinnedAuth.Provider))
				}
				state, hasState := pinnedAuthByProvider[providerKey]
				if !ok || !hasState || state.authID != pinnedAuthID || !h.responsesWebsocketPinnedAuthMatchesModel(executionParent, pinnedAuth, requestModelName, state.modelKey, homeRuntime) {
					pinnedAuthID = ""
				}
			}
			if pinnedAuthID == "" {
				providerSet, _ := responsesWebsocketProviderSetForModel(responsesWebsocketResolvedModelName(requestModelName))
				if len(providerSet) == 1 {
					for providerKey := range providerSet {
						state, ok := pinnedAuthByProvider[providerKey]
						candidateAuth, homeRuntime, okAuth := sessionAuthByIDWithSource(state.authID)
						if ok && okAuth && h.responsesWebsocketPinnedAuthMatchesModel(executionParent, candidateAuth, requestModelName, state.modelKey, homeRuntime) {
							pinnedAuthID = state.authID
						} else {
							delete(pinnedAuthByProvider, providerKey)
						}
					}
				}
			}
		}
		homeEnabled := h != nil && h.AuthManager != nil && h.AuthManager.HomeEnabled()
		keepPinnedHomeWebsocket := false
		if homeEnabled && !routeOverridesModelResolution && pinnedAuthID != "" {
			if pinnedAuth, ok := sessionAuthByID(pinnedAuthID); ok {
				keepPinnedHomeWebsocket = pinnedAuth != nil && strings.EqualFold(strings.TrimSpace(pinnedAuth.Provider), "codex")
			}
		}
		useUpstreamWebsocketPassthrough := h.responsesWebsocketUsesUpstreamWebsocketPassthrough(executionParent, requestModelName)
		requestUpstreamProvider := ""
		if routeOverridesModelResolution {
			if routedProvider == "codex" || routedProvider == "xai" {
				requestUpstreamProvider = routedProvider
			}
		} else {
			if pinnedAuthID != "" {
				if pinnedAuth, ok := sessionAuthByID(pinnedAuthID); ok && responsesWebsocketAuthSupportsIncrementalInput(pinnedAuth) {
					provider := strings.ToLower(strings.TrimSpace(pinnedAuth.Provider))
					useUpstreamWebsocketPassthrough = provider == "codex" || provider == "xai"
					if useUpstreamWebsocketPassthrough {
						requestUpstreamProvider = provider
					}
				}
			}
			if requestUpstreamProvider == "" {
				providerSet, _ := responsesWebsocketProviderSetForModel(responsesWebsocketResolvedModelName(requestModelName))
				if len(providerSet) == 1 {
					for provider := range providerSet {
						if provider == "codex" || provider == "xai" {
							requestUpstreamProvider = provider
						}
					}
				}
			}
			if requestUpstreamProvider == "" && explicitRequestModelName == "" {
				requestUpstreamProvider = upstreamProvider
			}
		}
		var replacementDropper upstreamSessionDropper
		if len(lastRequest) > 0 && requestReplacesTranscript && !keepPinnedHomeWebsocket {
			replacementDropper = upstreamSessionDroppers[requestUpstreamProvider]
		}
		nativeWebsocketPassthrough := !routeOverridesModelResolution && responsesWebsocketNativePassthroughAllowed(
			upstreamMode,
			useUpstreamWebsocketPassthrough,
			pinnedAuthID,
			upstreamAuthID,
		)
		requestRequiresCurrentUpstreamWebsocket := responsesWebsocketRequestRequiresCurrentUpstream(payload)
		if upstreamMode == responsesWebsocketUpstreamModeWS && !nativeWebsocketPassthrough {
			if requestRequiresCurrentUpstreamWebsocket && !requestReplacesTranscript {
				replayErr := responsesWebsocketHTTPReplayRequiredError()
				wsTerminateErr = replayErr
				matched, errClose := writer.closeForUpstreamError(replayErr)
				if !matched {
					_ = conn.Close()
				} else if errClose != nil && !errors.Is(errClose, websocket.ErrCloseSent) {
					log.Debugf("responses websocket: replay close failed id=%s error=%v", passthroughSessionID, errClose)
				}
				return
			}
			// A full transcript replacement is self-contained and can safely
			// establish a new upstream transport without another replay.
		}
		if explicitRequestModelName != "" && !useUpstreamWebsocketPassthrough {
			passthroughModelName = ""
		}
		replayCurrentRequest := false
		turnExecutionParent := coreauth.WithFreshRequestContinuitySession(executionParent)
		transcriptReplayRetries := 0
		var excludedAuthIDs []string
		retriedPinnedFailover := false
		forceHTTPUpstream := !routeOverridesModelResolution &&
			localCompactionResponseID == "" &&
			upstreamMode == responsesWebsocketUpstreamModeHTTP &&
			strings.TrimSpace(pinnedAuthID) != "" &&
			strings.TrimSpace(pinnedAuthID) == strings.TrimSpace(upstreamAuthID)
		httpFallbackAttempted := forceHTTPUpstream
		httpFallbackAuthID := ""
	retryCurrentRequest:
		if forceHTTPUpstream {
			activateUpstreamProvider("")
		}
		currentUpstreamGeneration := seenUpstreamGenerations[requestUpstreamProvider]
		upstreamGenerationChanged := false
		if generationProvider := upstreamGenerations[requestUpstreamProvider]; generationProvider != nil {
			currentUpstreamGeneration = generationProvider.UpstreamGeneration(passthroughSessionID)
			upstreamGenerationChanged = currentUpstreamGeneration != seenUpstreamGenerations[requestUpstreamProvider]
		}
		currentUpstreamDisconnectGeneration := seenUpstreamDisconnectGenerations[requestUpstreamProvider]
		upstreamDisconnectGenerationChanged := false
		if generationProvider := upstreamDisconnectGenerations[requestUpstreamProvider]; generationProvider != nil {
			currentUpstreamDisconnectGeneration = generationProvider.UpstreamDisconnectGeneration(passthroughSessionID)
			upstreamDisconnectGenerationChanged =
				currentUpstreamDisconnectGeneration != seenUpstreamDisconnectGenerations[requestUpstreamProvider]
		}

		forcedTranscriptReplay := replacementDropper != nil || upstreamGenerationChanged ||
			upstreamDisconnectGenerationChanged || replayCurrentRequest
		executeNativeWebsocketPassthrough := nativeWebsocketPassthrough && !forcedTranscriptReplay
		// A completed compaction response is direct evidence that the active target
		// (whether a specific plugin executor, a specific auth on standard route, or a specific auth on provider route)
		// supports compaction replay.
		observedCompactionReplayAuthID := ""
		observedCompactionSupported := false
		if observedCompaction.modelName != "" &&
			responsesWebsocketResolvedModelName(observedCompaction.modelName) == responsesWebsocketResolvedModelName(requestModelName) {
			if isPluginExecutorRoute {
				if observedCompaction.pluginID != "" && observedCompaction.pluginID == pluginExecutorID {
					observedCompactionSupported = true
				}
			} else if observedCompaction.pluginID == "" {
				currentProvider, currentTargetModel := handlers.PreparedStreamProviderRoute(executionParent)
				providerRouteMatches := (observedCompaction.provider == currentProvider && observedCompaction.targetModel == currentTargetModel)
				if providerRouteMatches && observedCompaction.authID != "" {
					if currentProvider != "" {
						// Provider route override: validate against the auth that observed compaction on this provider route.
						// A leftover pinnedAuthID from an earlier unrouted turn must not veto the routed compaction auth.
						if compactionAuth, _, ok := sessionAuthByIDWithSource(observedCompaction.authID); ok && compactionAuth != nil {
							if compactionAuth.Status == coreauth.StatusActive {
								observedCompactionSupported = true
								observedCompactionReplayAuthID = observedCompaction.authID
							}
						}
					} else {
						// Standard auth route:
						if pinnedAuthID != "" {
							if pinnedAuthID == observedCompaction.authID {
								observedCompactionSupported = true
								observedCompactionReplayAuthID = observedCompaction.authID
							}
						} else if compactionAuth, homeRuntime, ok := sessionAuthByIDWithSource(observedCompaction.authID); ok && compactionAuth != nil {
							if homeRuntime {
								if compactionAuth.Status == coreauth.StatusActive {
									observedCompactionSupported = true
									observedCompactionReplayAuthID = observedCompaction.authID
								}
							} else if h.responsesWebsocketPinnedAuthMatchesModel(executionParent, compactionAuth, requestModelName, observedCompaction.modelKey, false) {
								observedCompactionSupported = true
								observedCompactionReplayAuthID = observedCompaction.authID
							}
						}
					}
				}
			}
		}
		supportsCompactionReplay := func(input ojson.Result) bool {
			if observedCompactionSupported {
				return true
			}
			if routeOverridesModelResolution {
				return responsesWebsocketProviderSupportsCompactionReplay(routedProvider, input)
			}
			if pinnedAuthID != "" {
				pinnedAuth, _ := sessionAuthByID(pinnedAuthID)
				return responsesWebsocketAuthSupportsCompactionReplay(pinnedAuth, input)
			}
			if homeEnabled {
				return true
			}
			return h.websocketUpstreamSupportsCompactionReplayForModel(executionParent, requestModelName, input)
		}

		var requestJSON []byte
		var updatedLastRequest []byte
		var errMsg *interfaces.ErrorMessage
		previousResponseID := strings.TrimSpace(ojson.GetBytesNoCopy(payload, "previous_response_id").String())
		isPrewarm := (!useUpstreamWebsocketPassthrough || routeOverridesModelResolution || forceHTTPUpstream) &&
			shouldHandleResponsesWebsocketPrewarmLocally(payload)
		createAfterPrewarm := pendingPrewarmID != "" && ojson.GetBytesNoCopy(payload, "type").String() == wsRequestTypeCreate
		startsPrewarmTranscript := previousResponseID == "" && (isPrewarm || createAfterPrewarm)
		// A synthetic acknowledgement has no executor to consume the reserved marker.
		if isPrewarm && helps.ValidateCodexSessionInstructionMarker(payload, translator.FormatOpenAIResponse) != nil {
			errMsg = &interfaces.ErrorMessage{
				StatusCode: http.StatusBadRequest,
				Error:      errors.New("send cpa_session_instructions on a full response.create with generation enabled after local prewarm"),
			}
		} else if pendingPrewarmID != "" && previousResponseID != "" {
			if previousResponseID != pendingPrewarmID {
				errMsg = responsesWebsocketPreviousResponseNotFoundError()
			} else {
				requestJSON, updatedLastRequest, errMsg = normalizeResponsesWebsocketPrewarmFollowup(payload, lastRequest, supportsCompactionReplay)
			}
		} else if startsPrewarmTranscript {
			// A parentless create starts a new transcript and inherits session defaults.
			requestJSON, updatedLastRequest, errMsg = normalizeResponseCreateRequest(ojson.NewDoc(payload), lastRequest, supportsCompactionReplay(requestInput))
		} else if executeNativeWebsocketPassthrough {
			requestJSON, errMsg = normalizeResponsesWebsocketPassthroughRequest(payload, requestModelName)
			if errMsg == nil && requestReplacesTranscript {
				reqDoc := ojson.NewDoc(requestJSON)
				if reqDoc.Delete("previous_response_id") == nil {
					requestJSON = reqDoc.Bytes()
				}
			}
			if errMsg == nil {
				_, updatedLastRequest, errMsg = normalizeResponsesWebsocketRequest(
					payload,
					lastRequest,
					lastResponseOutput,
					supportsCompactionReplay(requestInput),
				)
			}
		} else if len(lastRequest) == 0 && previousResponseID != "" && !requestReplacesTranscript {
			errMsg = responsesWebsocketPreviousResponseNotFoundError()
		} else if previousResponseID != "" && (localCompactionResponseID != "" || strings.HasPrefix(previousResponseID, "resp_ag_compact_")) && previousResponseID != localCompactionResponseID {
			errMsg = responsesWebsocketPreviousResponseNotFoundError()
		} else {
			requestJSON, updatedLastRequest, errMsg = normalizeResponsesWebsocketRequest(
				payload,
				lastRequest,
				lastResponseOutput,
				supportsCompactionReplay(requestInput),
			)
		}
		if errMsg != nil {
			h.LoggingAPIResponseError(context.WithValue(context.Background(), "gin", c), errMsg)
			markAPIResponseTimestamp(c)
			errorPayload, errWrite := writeResponsesWebsocketError(writer, wsTimelineLog, errMsg)
			logResponsesWebsocketDownstreamError(passthroughSessionID, errorPayload)
			if errWrite != nil {
				log.Warnf(
					"responses websocket: downstream_out write failed id=%s event=%s error=%v",
					passthroughSessionID,
					websocketPayloadEventType(errorPayload),
					errWrite,
				)
				return
			}
			continue
		}

		if replacementDropper != nil {
			// Retire the old socket once the replacement transcript passes validation.
			replacementDropper.DropUpstreamSession(passthroughSessionID, "compact_replay")
			replacementDropper = nil
			if provider := upstreamGenerations[requestUpstreamProvider]; provider != nil {
				currentUpstreamGeneration = provider.UpstreamGeneration(passthroughSessionID)
			}
		}

		requestBeforePreparation := bytes.Clone(requestJSON)
		requestJSON = h.prepareCodexRequestPayload(c, requestJSON)
		if bytes.Equal(updatedLastRequest, requestBeforePreparation) {
			updatedLastRequest = bytes.Clone(requestJSON)
		} else {
			updatedLastRequest = h.prepareCodexRequestPayload(c, updatedLastRequest)
		}

		resetToolRepairState := requestReplacesTranscript || startsPrewarmTranscript
		if isPrewarm {
			toolCacheTurn := newResponsesWebsocketToolCacheTurn(downstreamSessionKey)
			if resetToolRepairState {
				toolCacheTurn.resetOnCommit()
			}
			reqDoc := ojson.NewDoc(requestJSON)
			if reqDoc.Delete("generate") == nil {
				requestJSON = reqDoc.Bytes()
			}
			lastDoc := ojson.NewDoc(updatedLastRequest)
			if lastDoc.Delete("generate") == nil {
				updatedLastRequest = lastDoc.Bytes()
			}
			lastRequest = updatedLastRequest
			lastResponseOutput = []byte("[]")
			// A local acknowledgement preserves the capability observed on the same target.
			if !observedCompactionSupported {
				observedCompaction.clear()
			}
			localCompactionResponseID = ""
			prewarmID, errWrite := writeResponsesWebsocketSyntheticPrewarm(c, writer, requestJSON, wsTimelineLog, passthroughSessionID)
			if errWrite != nil {
				wsTerminateErr = errWrite
				return
			}
			pendingPrewarmID = prewarmID
			if httpFallbackAttempted && httpFallbackAuthID != "" {
				rememberPinnedAuth(httpFallbackAuthID, requestModelName)
				upstreamMode = responsesWebsocketUpstreamModeHTTP
				upstreamAuthID = httpFallbackAuthID
			}
			toolCacheTurn.commit()
			continue
		}

		var toolCacheTurn *responsesWebsocketToolCacheTurn
		nextLastRequest := updatedLastRequest
		if executeNativeWebsocketPassthrough {
			if resetToolRepairState {
				_, toolCacheTurn = prepareResponsesWebsocketFallbackTurn(downstreamSessionKey, requestJSON, true)
			}
			if modelName := strings.TrimSpace(ojson.GetBytesNoCopy(requestJSON, "model").String()); modelName != "" {
				passthroughModelName = strings.Clone(modelName)
			}
		} else {
			requestBeforeRepair := bytes.Clone(requestJSON)
			requestJSON, toolCacheTurn = prepareResponsesWebsocketFallbackTurn(downstreamSessionKey, requestJSON, resetToolRepairState)
			if bytes.Equal(updatedLastRequest, requestBeforeRepair) {
				nextLastRequest = bytes.Clone(requestJSON)
			} else if resetToolRepairState {
				nextLastRequest = dedupeResponsesWebsocketInputItemsByID(updatedLastRequest)
			} else {
				nextLastRequest = repairResponsesWebsocketToolCallsWithoutRecording(downstreamSessionKey, updatedLastRequest)
			}
		}

	retryExecution:
		modelName := strings.Clone(ojson.GetBytesNoCopy(requestJSON, "model").String())
		lastAttemptedAuthID := pinnedAuthID
		attemptedUpstreamMode := responsesWebsocketUpstreamModeUnknown
		attemptedUpstreamProvider := requestUpstreamProvider
		selectedAuthObserved := false
		nativeRequest := util.IsCodexResponsesLiteRequest(payload, c.Request.Header)
		var preserveNativeOutput atomic.Bool
		codexDuplexStream.Store(false)
		pinnedAuthAttempted := false
		cliCtx, cliCancel := h.GetContextWithCancel(h, c, turnExecutionParent)
		if !forceHTTPUpstream {
			cliCtx = cliproxyexecutor.WithDownstreamWebsocket(cliCtx)
		}
		if requestRequiresCurrentUpstreamWebsocket && !responsesWebsocketRequestRequiresCurrentUpstream(requestJSON) {
			cliCtx = cliproxyexecutor.WithRebuiltWebsocketContinuation(cliCtx, true)
		}
		if !executeNativeWebsocketPassthrough && !ojson.GetBytesNoCopy(payload, "instructions").Exists() {
			if inherited := ojson.GetBytesNoCopy(requestJSON, "instructions"); inherited.Exists() {
				cliCtx = cliproxyexecutor.WithInheritedWebsocketInstructions(cliCtx, strings.Clone(inherited.Raw))
			}
		}
		cliCtx = cliproxyexecutor.WithLocalWebsocketTurnTracker(cliCtx)
		if duplexInput != nil {
			cliCtx = cliproxyexecutor.WithWebsocketInput(cliCtx, duplexInput)
			cliCtx = cliproxyexecutor.WithWebsocketAuthCheck(cliCtx, func(authID string) bool {
				current, ok := sessionAuthByID(authID)
				return ok && current != nil && !current.Disabled && current.Status != coreauth.StatusDisabled
			})
			boundPrincipal := usage.APIKeyFromContext(cliCtx)
			cliCtx = cliproxyexecutor.WithWebsocketTurnAuthorizer(cliCtx, func(turnPayload []byte) ([]byte, error) {
				principal, errAuth := h.authenticateResponsesWebsocketTurn(c, upgradeRequest, turnPayload, modelName, false)
				if errAuth == nil && principal != "" && principal != boundPrincipal {
					errAuth = &interfaces.ErrorMessage{
						StatusCode: http.StatusUnauthorized,
						Error:      errors.New("client credential changed during the websocket stream; reconnect"),
					}
				}
				if errAuth == nil {
					return nil, nil
				}
				return buildResponsesWebsocketErrorPayload(errAuth)
			})
		}
		if executeNativeWebsocketPassthrough && requestRequiresCurrentUpstreamWebsocket {
			cliCtx = cliproxyexecutor.WithRequiredUpstreamWebsocket(cliCtx)
		}
		cliCtx = handlers.WithExecutionSessionID(cliCtx, passthroughSessionID)
		cliCtx = handlers.WithSelectedAuthIDCallback(cliCtx, func(authID string) {
			preserveNativeOutput.Store(false)
			codexDuplexStream.Store(false)
			cliproxyexecutor.ResetLocalWebsocketTurn(cliCtx)
			authID = strings.TrimSpace(authID)
			if authID == "" || h == nil || h.AuthManager == nil {
				return
			}
			lastAttemptedAuthID = authID
			selectedAuthObserved = true
			pinnedAuthAttempted = pinnedAuthAttempted || (pinnedAuthID != "" && authID == pinnedAuthID)
			selectedAuth, ok := sessionAuthByID(authID)
			if !ok || selectedAuth == nil {
				return
			}
			attemptedUpstreamMode = upstreamModeForAuth(selectedAuth)
			provider := strings.ToLower(strings.TrimSpace(selectedAuth.Provider))
			codexDuplexStream.Store(duplexInput != nil && attemptedUpstreamMode == responsesWebsocketUpstreamModeWS && provider == "codex" && !forceHTTPUpstream)
			preserveNativeOutput.Store(nativeRequest && provider == "codex")
			if attemptedUpstreamMode == responsesWebsocketUpstreamModeWS && !forceHTTPUpstream {
				attemptedUpstreamProvider = provider
				activateUpstreamProvider(provider)
			} else {
				attemptedUpstreamProvider = ""
				activateUpstreamProvider("")
			}
		})
		executionAuthID := ""
		if !routeOverridesModelResolution {
			executionAuthID = pinnedAuthID
		}
		if executionAuthID == "" {
			executionAuthID = observedCompactionReplayAuthID
		}
		if executionAuthID != "" && !isPluginExecutorRoute {
			cliCtx = handlers.WithPinnedAuthID(cliCtx, executionAuthID)
		}
		if len(excludedAuthIDs) > 0 {
			cliCtx = handlers.WithExcludedAuthIDs(cliCtx, excludedAuthIDs)
		}
		if h.HasSchedulerContinuity() {
			cliCtx = cliproxyexecutor.WithContinuityWaitObserver(cliCtx, responsesWebsocketContinuityObserver{setActive: setContinuityWaitActive})
		}
		dataChan, upstreamHeaders, errChan := h.ExecuteStreamWithAuthManager(cliCtx, h.HandlerType(), modelName, requestJSON, "")
		if forceHTTPUpstream || !selectedAuthObserved || cliproxyexecutor.LocalWebsocketTurnExecuted(cliCtx) {
			// Local compaction and plugin/alternate routes execute outside the upstream
			// websocket. Record their HTTP mode for subsequent transcript reconstruction.
			attemptedUpstreamMode = responsesWebsocketUpstreamModeHTTP
		}
		if attemptedUpstreamMode != responsesWebsocketUpstreamModeWS {
			attemptedUpstreamProvider = ""
			activateUpstreamProvider("")
		}
		// A connection-scoped continuation cannot rotate credentials in place. Suppress
		// credential errors and make the client replay the full turn on a new socket
		// when another eligible target can serve the replayed turn.
		var replayedPinnedAuthFailure bool
		var failoverInPlace bool
		replayPinnedAuthFailure := func(errMsg *interfaces.ErrorMessage) bool {
			if h == nil || h.AuthManager == nil || errMsg == nil || errMsg.Error == nil {
				return false
			}
			if coreauth.IsContinuityTerminatedError(errMsg.Error) {
				return false
			}
			if !executeNativeWebsocketPassthrough || !pinnedAuthAttempted {
				return false
			}
			if !shouldReplayResponsesWebsocketPinnedAuthFailure(errMsg) && !isResponsesWebsocketQuotaOrCooldownError(errMsg.Error) {
				return false
			}
			if !requestRequiresCurrentUpstreamWebsocket {
				if retriedPinnedFailover {
					return false
				}
				proceed, gateErr := h.AuthManager.EvaluatePreOutputStreamFailoverQuotaGate(cliCtx, modelName, lastAttemptedAuthID, errMsg.Error)
				if !proceed {
					if gateErr != nil {
						errMsg.Error = gateErr
					}
					return false
				}
				if !h.responsesWebsocketHasAlternativeReplayTarget(cliCtx, modelName, requestJSON, lastAttemptedAuthID) {
					return false
				}
				failoverInPlace = true
				return true
			}
			if (responsesWebsocketErrorStatus(errMsg) == http.StatusTooManyRequests || isResponsesWebsocketQuotaOrCooldownError(errMsg.Error)) &&
				!h.responsesWebsocketHasAlternativeReplayTarget(cliCtx, modelName, requestJSON, lastAttemptedAuthID) {
				return false
			}
			replayedPinnedAuthFailure = true
			return true
		}

		allowTranscriptReplayBeforeOutput := transcriptReplayRetries < wsTranscriptReplayMaxRetries
		var httpInterrupt *responsesLocalInterrupt
		if attemptedUpstreamMode == responsesWebsocketUpstreamModeHTTP {
			httpInterrupt = localInterrupt
		}
		var completedResponseID string
		completedOutput, forwardErrMsg, replayAllowed, errForward := h.forwardResponsesWebsocket(
			c,
			writer,
			cliCancel,
			dataChan,
			errChan,
			upstreamHeaders,
			wsTimelineLog,
			passthroughSessionID,
			responsesWebsocketForwardOptions{
				duplexStream: func() bool {
					return codexDuplexStream.Load() && !cliproxyexecutor.LocalWebsocketTurnExecuted(cliCtx)
				},
				completedResponse:                 func(id string) { completedResponseID = id },
				preserveCompletionOutput:          preserveNativeOutput.Load,
				toolCacheTurn:                     toolCacheTurn,
				suppressError:                     replayPinnedAuthFailure,
				allowTranscriptReplayBeforeOutput: allowTranscriptReplayBeforeOutput,
				allowHTTPFallbackBeforeOutput: !executeNativeWebsocketPassthrough &&
					!httpFallbackAttempted &&
					attemptedUpstreamMode == responsesWebsocketUpstreamModeWS,
				localInterrupt: httpInterrupt,
			},
		)
		if errForward != nil {
			wsTerminateErr = errForward
			switch {
			case errors.Is(errForward, websocket.ErrCloseSent):
			case isWebsocketConnectionClosedError(errForward):
				// The client hung up while a downstream write was in flight. This is a
				// normal shutdown race, not a proxy failure.
				log.Debugf("responses websocket: client closed during forward id=%s error=%v", passthroughSessionID, errForward)
			default:
				log.Warnf("responses websocket: forward failed id=%s error=%v", passthroughSessionID, errForward)
			}
			return
		}
		if replayAllowed {
			switch {
			case !httpFallbackAttempted && shouldRetryResponsesWebsocketHTTPFallback(forwardErrMsg):
				httpFallbackAttempted = true
				forceHTTPUpstream = true
				if selectedAuthObserved && lastAttemptedAuthID != "" {
					httpFallbackAuthID = lastAttemptedAuthID
				}
				replayCurrentRequest = true
				goto retryCurrentRequest
			case allowTranscriptReplayBeforeOutput && shouldRetryResponsesWebsocketTranscriptReplay(forwardErrMsg):
				transcriptReplayRetries++
				replayCurrentRequest = true
				goto retryCurrentRequest
			}
		}
		if failoverInPlace {
			// The forwarder permits this transition only before downstream output.
			// Reuse the prepared full request and exclude its rejected credential.
			retriedPinnedFailover = true
			if lastAttemptedAuthID != "" {
				excludedAuthIDs = append(excludedAuthIDs, lastAttemptedAuthID)
			}
			forgetPinnedAuth()
			upstreamMode = responsesWebsocketUpstreamModeUnknown
			upstreamAuthID = ""
			observedCompactionReplayAuthID = ""
			executeNativeWebsocketPassthrough = false
			failoverDoc := ojson.NewDoc(requestJSON)
			_ = failoverDoc.Delete("type")
			requestJSON, toolCacheTurn = prepareResponsesWebsocketFallbackTurn(downstreamSessionKey, failoverDoc.Bytes(), resetToolRepairState)
			nextLastRequest = responsesWebsocketSnapshotWithoutCompactionTriggers(requestJSON)
			goto retryExecution
		}
		// Commit local compaction only after its completed response reaches the client.
		interruptedLocalCompaction := cliproxyexecutor.LocalWebsocketTurnExecuted(cliCtx) && completedResponseID == ""
		if forwardErrMsg != nil || interruptedLocalCompaction {
			activateUpstreamProvider(upstreamProvider)
			if pinnedAuthAttempted && shouldReleaseResponsesWebsocketPinnedAuth(forwardErrMsg) {
				forgetPinnedAuth()
			}
			if replayedPinnedAuthFailure {
				replayErr := responsesWebsocketHTTPReplayRequiredError()
				wsTerminateErr = replayErr
				matched, errClose := writer.closeForUpstreamError(replayErr)
				if !matched {
					_ = conn.Close()
				} else if errClose != nil && !errors.Is(errClose, websocket.ErrCloseSent) {
					log.Debugf("responses websocket: credential replay close failed id=%s error=%v", passthroughSessionID, errClose)
				}
				return
			}
			continue
		}

		pendingPrewarmID = ""
		localCompactionResponseID = ""
		if cliproxyexecutor.LocalWebsocketTurnExecuted(cliCtx) {
			if lastAttemptedAuthID != "" {
				rememberPinnedAuth(lastAttemptedAuthID, modelName)
			}
			nextLastRequest = responsesWebsocketLocalCompactionBoundaryRequest(requestJSON)
			localCompactionResponseID = completedResponseID
			// The summary replaces the source history and its tool-repair entries.
			toolCacheTurn = newResponsesWebsocketToolCacheTurn(downstreamSessionKey)
			toolCacheTurn.resetOnCommit()
		}
		toolCacheTurn.commit()
		lastRequest = nextLastRequest
		upstreamMode = attemptedUpstreamMode
		upstreamProvider = attemptedUpstreamProvider
		if selectedAuthObserved {
			upstreamAuthID = lastAttemptedAuthID
		} else {
			upstreamAuthID = ""
		}
		if upstreamMode == responsesWebsocketUpstreamModeWS {
			if lastAttemptedAuthID != "" {
				rememberPinnedAuth(lastAttemptedAuthID, modelName)
			}
			passthroughModelName = modelName
			observedCompaction.clear()
		} else {
			if httpFallbackAttempted && selectedAuthObserved && lastAttemptedAuthID != "" {
				rememberPinnedAuth(lastAttemptedAuthID, modelName)
			}
			if inputContainsFullTranscript(ojson.ParseBytes(completedOutput)) {
				_, modelKey := responsesWebsocketProviderSetForModel(responsesWebsocketResolvedModelName(modelName))
				if isPluginExecutorRoute {
					observedCompaction = responsesWebsocketObservedCompactionState{
						modelName: modelName,
						modelKey:  modelKey,
						pluginID:  pluginExecutorID,
					}
				} else {
					currentProvider, currentTargetModel := handlers.PreparedStreamProviderRoute(executionParent)
					observedCompaction = responsesWebsocketObservedCompactionState{
						modelName:   modelName,
						modelKey:    modelKey,
						provider:    currentProvider,
						targetModel: currentTargetModel,
						authID:      lastAttemptedAuthID,
					}
				}
			} else if observedCompaction.modelName != "" {
				targetMismatch := false
				currentProvider, currentTargetModel := handlers.PreparedStreamProviderRoute(executionParent)
				if responsesWebsocketResolvedModelName(observedCompaction.modelName) != responsesWebsocketResolvedModelName(modelName) {
					targetMismatch = true
				} else if isPluginExecutorRoute {
					if pluginExecutorID != observedCompaction.pluginID {
						targetMismatch = true
					}
				} else if observedCompaction.pluginID != "" {
					targetMismatch = true
				} else if currentProvider != observedCompaction.provider || currentTargetModel != observedCompaction.targetModel {
					targetMismatch = true
				} else if observedCompaction.authID != "" && lastAttemptedAuthID != "" && observedCompaction.authID != lastAttemptedAuthID {
					targetMismatch = true
				}
				if targetMismatch {
					observedCompaction.clear()
				}
			}
		}
		lastResponseOutput = completedOutput
		if generationProvider := upstreamGenerations[attemptedUpstreamProvider]; generationProvider != nil {
			if attemptedUpstreamProvider == requestUpstreamProvider {
				seenUpstreamGenerations[attemptedUpstreamProvider] = currentUpstreamGeneration
			} else {
				seenUpstreamGenerations[attemptedUpstreamProvider] = generationProvider.UpstreamGeneration(passthroughSessionID)
			}
		}
		if generationProvider := upstreamDisconnectGenerations[attemptedUpstreamProvider]; generationProvider != nil {
			if attemptedUpstreamProvider == requestUpstreamProvider {
				seenUpstreamDisconnectGenerations[attemptedUpstreamProvider] = currentUpstreamDisconnectGeneration
			} else {
				seenUpstreamDisconnectGenerations[attemptedUpstreamProvider] = generationProvider.UpstreamDisconnectGeneration(passthroughSessionID)
			}
		}
	}
}

type responsesWebsocketObservedCompactionState struct {
	modelName   string
	modelKey    string
	authID      string
	pluginID    string
	provider    string
	targetModel string
}

func (s *responsesWebsocketObservedCompactionState) clear() {
	*s = responsesWebsocketObservedCompactionState{}
}

func responsesWebsocketLocalCompactionBoundaryRequest(requestJSON []byte) []byte {
	if len(requestJSON) == 0 {
		return []byte(`{"input":[]}`)
	}
	doc := ojson.NewDoc(requestJSON)
	_ = doc.Delete("type")
	_ = doc.Delete("previous_response_id")
	_ = doc.SetRawBytes("input", []byte("[]"))
	return doc.Bytes()
}

func responsesWebsocketHTTPReplayRequiredError() error {
	return cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
}

func responsesWebsocketRequestRequiresCurrentUpstream(payload []byte) bool {
	var fieldsBuf [2]ojson.Result
	fields := ojson.GetManyBytesNoCopy(fieldsBuf[:0], payload, "previous_response_id", "type")
	return strings.TrimSpace(fields[0].String()) != "" ||
		strings.TrimSpace(fields[1].String()) == wsRequestTypeAppend
}

func responsesWebsocketNativePassthroughAllowed(upstreamMode string, useUpstreamWebsocket bool, pinnedAuthID string, upstreamAuthID string) bool {
	return upstreamMode == responsesWebsocketUpstreamModeWS && useUpstreamWebsocket &&
		strings.TrimSpace(pinnedAuthID) != "" && strings.TrimSpace(pinnedAuthID) == strings.TrimSpace(upstreamAuthID)
}

func websocketClientAddress(c *gin.Context) string {
	if c == nil || c.Request == nil {
		return ""
	}
	return strings.TrimSpace(c.ClientIP())
}

func websocketUpgradeHeaders(req *http.Request) http.Header {
	headers := http.Header{}
	if req == nil {
		return headers
	}

	// Keep the same sticky turn-state across reconnects when provided by the client.
	turnState := strings.TrimSpace(req.Header.Get(wsTurnStateHeader))
	if turnState != "" {
		headers.Set(wsTurnStateHeader, turnState)
	}
	return headers
}

func responsesWebsocketPreviousResponseNotFoundError() *interfaces.ErrorMessage {
	return &interfaces.ErrorMessage{
		StatusCode: http.StatusConflict,
		Error: errors.New(
			`{"error":{"message":"Previous response is not available on this websocket; resend the full conversation input without previous_response_id","type":"invalid_request_error","code":"previous_response_not_found","param":"previous_response_id"}}`,
		),
	}
}
