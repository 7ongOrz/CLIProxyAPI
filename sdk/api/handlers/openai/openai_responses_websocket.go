package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	requestlogging "github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	wsRequestTypeCreate                   = "response.create"
	wsRequestTypeAppend                   = "response.append"
	wsRequestTypeProcessed                = "response.processed"
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

	wsHeartbeatInterval               = 30 * time.Second
	wsTranscriptReplayMaxRetries      = 2
	wsConnectionLimitReachedCode      = "websocket_connection_limit_reached"
	wsResponsesLiteMetadataKey        = "ws_request_header_x_openai_internal_codex_responses_lite"
	codexLocalCompactionSummaryPrefix = "Another language model started to solve this problem and produced a summary of its thinking process. You also have access to the state of the tools that were used by that language model. Use this to build on the work that has already been done and avoid duplicating work. Here is the summary produced by the other language model, use the information in this summary to assist with your own analysis:"
)

var responsesWebsocketUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// writeWebsocketCloseForUpstreamError mirrors transport-level upstream close
// codes to the downstream WebSocket client before the connection is torn down.
// Without this the client only observes an abnormal closure (1006) and cannot
// apply its own close-code based handling (e.g. falling back to SSE on 1009).
func writeWebsocketCloseForUpstreamError(conn *websocket.Conn, err error) (bool, error) {
	if conn == nil {
		return false, nil
	}
	matched, payload := websocketClosePayloadForUpstreamError(err)
	if !matched {
		return false, nil
	}
	return true, conn.WriteControl(websocket.CloseMessage, payload, time.Time{})
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
		if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusRequestEntityTooLarge ||
			gjson.Get(errText, "error.code").String() != "message_too_big" {
			return false, nil
		}
		code = websocket.CloseMessageTooBig
		reason = strings.TrimSpace(gjson.Get(errText, "error.message").String())
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
// an active downstream data writer. If a data write already owns writeMu, the
// connection is closed immediately so the blocked writer and session can exit.
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

func (w *responsesWebsocketWriter) closeForUpstreamDisconnect(err error) {
	if w == nil || w.conn == nil {
		return
	}
	if matched, _ := w.closeForUpstreamError(err); matched {
		return
	}
	_ = w.conn.Close()
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

type websocketTimelineAppender interface {
	Append(eventType string, payload []byte, timestamp time.Time)
}

type responsesWebsocketPinnedAuthState struct {
	authID   string
	modelKey string
}

type websocketTimelineLog struct {
	enabled bool
	source  *requestlogging.FileBodySource
	builder *strings.Builder

	currentPart       io.WriteCloser
	currentPartHasLog bool
}

func newWebsocketTimelineLog(enabled bool, source *requestlogging.FileBodySource) *websocketTimelineLog {
	if !enabled {
		return &websocketTimelineLog{}
	}
	if source == nil {
		return newInMemoryWebsocketTimelineLog()
	}
	return &websocketTimelineLog{
		enabled: true,
		source:  source,
	}
}

func newInMemoryWebsocketTimelineLog() *websocketTimelineLog {
	return &websocketTimelineLog{
		enabled: true,
		builder: &strings.Builder{},
	}
}

func websocketTimelineSourceFromContext(c *gin.Context) *requestlogging.FileBodySource {
	if c == nil {
		return nil
	}
	value, exists := c.Get(requestlogging.WebsocketTimelineSourceContextKey)
	if !exists {
		return nil
	}
	source, ok := value.(*requestlogging.FileBodySource)
	if !ok {
		return nil
	}
	return source
}

func (l *websocketTimelineLog) BeginRequest() {
	if l == nil || !l.enabled || l.source == nil {
		return
	}
	l.closeCurrentPart()
	part, errCreate := l.source.CreatePart("request")
	if errCreate != nil {
		log.WithError(errCreate).Warn("failed to create websocket request detail log")
		return
	}
	l.currentPart = part
	l.currentPartHasLog = false
}

func (l *websocketTimelineLog) Append(eventType string, payload []byte, timestamp time.Time) {
	if l == nil || !l.enabled {
		return
	}
	data := formatWebsocketTimelineEvent(eventType, payload, timestamp)
	if len(data) == 0 {
		return
	}
	if l.source != nil {
		if l.currentPart == nil {
			l.BeginRequest()
		}
		if l.currentPart == nil {
			return
		}
		if errWrite := writeWebsocketTimelinePart(l.currentPart, data, l.currentPartHasLog); errWrite != nil {
			log.WithError(errWrite).Warn("failed to write websocket request detail log")
			return
		}
		l.currentPartHasLog = true
		return
	}
	if l.builder != nil {
		writeWebsocketTimelineBuilder(l.builder, data)
	}
}

func (l *websocketTimelineLog) SetContext(c *gin.Context) {
	if l == nil || !l.enabled {
		return
	}
	l.closeCurrentPart()
	if l.source != nil {
		if l.source.HasPayload() {
			c.Set(requestlogging.WebsocketTimelineSourceContextKey, l.source)
			return
		}
		if errCleanup := l.source.Cleanup(); errCleanup != nil {
			log.WithError(errCleanup).Warn("failed to clean up empty websocket timeline log parts")
		}
	}
	if l.builder != nil {
		setWebsocketTimelineBody(c, l.builder.String())
	}
}

func (l *websocketTimelineLog) String() string {
	if l == nil || !l.enabled {
		return ""
	}
	l.closeCurrentPart()
	if l.source != nil {
		data, errRead := l.source.Bytes()
		if errRead != nil {
			return ""
		}
		return string(data)
	}
	if l.builder == nil {
		return ""
	}
	return l.builder.String()
}

func (l *websocketTimelineLog) closeCurrentPart() {
	if l == nil || l.currentPart == nil {
		return
	}
	if errClose := l.currentPart.Close(); errClose != nil {
		log.WithError(errClose).Warn("failed to close websocket request detail log")
	}
	l.currentPart = nil
	l.currentPartHasLog = false
}

func writeWebsocketTimelinePart(w io.Writer, data []byte, prependNewline bool) error {
	if w == nil || len(data) == 0 {
		return nil
	}
	if prependNewline {
		if _, errWrite := io.WriteString(w, "\n"); errWrite != nil {
			return errWrite
		}
	}
	_, errWrite := w.Write(data)
	return errWrite
}

func writeWebsocketTimelineBuilder(builder *strings.Builder, data []byte) {
	if builder == nil || len(data) == 0 {
		return
	}
	if builder.Len() > 0 {
		builder.WriteString("\n")
	}
	builder.Write(data)
}

// ResponsesWebsocket handles websocket requests for /v1/responses.
// It accepts `response.create` and `response.append` requests and streams
// response events back as JSON websocket text messages.
func (h *OpenAIResponsesAPIHandler) ResponsesWebsocket(c *gin.Context) {
	conn, err := responsesWebsocketUpgrader.Upgrade(c.Writer, c.Request, websocketUpgradeHeaders(c.Request))
	if err != nil {
		return
	}
	writer := newResponsesWebsocketWriter(conn)
	passthroughSessionID := uuid.NewString()
	downstreamSessionKey := websocketDownstreamSessionKey(c.Request)
	retainResponsesWebsocketToolCaches(downstreamSessionKey)
	clientIP := websocketClientAddress(c)
	log.Infof("responses websocket: client connected id=%s remote=%s", passthroughSessionID, clientIP)

	requestLogEnabled := h != nil && h.Cfg != nil && h.Cfg.RequestLog
	wsTimelineLog := newWebsocketTimelineLog(requestLogEnabled, websocketTimelineSourceFromContext(c))

	wsDone := make(chan struct{})
	defer close(wsDone)
	startResponsesWebsocketHeartbeat(conn, wsDone, passthroughSessionID)

	var upstreamGeneration func() uint64
	var dropUpstreamSession func(reason string)
	var sendResponseProcessed func(responseID string) error
	if h != nil && h.AuthManager != nil {
		type upstreamDisconnectSubscriber interface {
			UpstreamDisconnectChan(sessionID string) <-chan error
		}
		for _, provider := range []string{"codex", "xai"} {
			exec, ok := h.AuthManager.Executor(provider)
			if !ok || exec == nil {
				continue
			}
			type upstreamGenerationProvider interface {
				UpstreamGeneration(sessionID string) uint64
			}
			type upstreamSessionDropper interface {
				DropUpstreamSession(sessionID string, reason string)
			}
			type responseProcessedSender interface {
				SendResponseProcessed(sessionID string, responseID string) error
			}
			if provider, ok := exec.(upstreamGenerationProvider); ok && provider != nil {
				upstreamGeneration = func() uint64 {
					return provider.UpstreamGeneration(passthroughSessionID)
				}
			}
			if dropper, ok := exec.(upstreamSessionDropper); ok && dropper != nil {
				dropUpstreamSession = func(reason string) {
					dropper.DropUpstreamSession(passthroughSessionID, reason)
				}
			}
			if sender, ok := exec.(responseProcessedSender); ok && sender != nil {
				sendResponseProcessed = func(responseID string) error {
					return sender.SendResponseProcessed(passthroughSessionID, responseID)
				}
			}
			if subscriber, ok := exec.(upstreamDisconnectSubscriber); ok && subscriber != nil {
				disconnectCh := subscriber.UpstreamDisconnectChan(passthroughSessionID)
				if disconnectCh != nil {
					go func() {
						select {
						case <-wsDone:
							return
						case disconnectErr := <-disconnectCh:
							writer.closeForUpstreamDisconnect(disconnectErr)
						}
					}()
				}
			}
		}
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
		wsTimelineLog.SetContext(c)
		if errClose := conn.Close(); errClose != nil {
			log.Warnf("responses websocket: close connection error: %v", errClose)
		}
	}()

	var lastRequest []byte
	lastResponseOutput := []byte("[]")
	lastResponseID := ""
	var lastResponsePendingToolCallIDs []string
	pinnedAuthID := ""
	// Preserve independent upstream auth affinity when a downstream session switches providers.
	pinnedAuthByProvider := make(map[string]responsesWebsocketPinnedAuthState)
	passthroughModelName := ""
	upstreamMode := responsesWebsocketUpstreamModeUnknown
	upstreamAuthID := ""
	seenUpstreamGeneration := uint64(0)
	if upstreamGeneration != nil {
		seenUpstreamGeneration = upstreamGeneration()
	}
	forceTranscriptReplayNextRequest := false
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
		msgType, payload, errReadMessage := conn.ReadMessage()
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
		// log.Infof(
		// 	"responses websocket: downstream_in id=%s type=%d event=%s payload=%s",
		// 	passthroughSessionID,
		// 	msgType,
		// 	websocketPayloadEventType(payload),
		// 	websocketPayloadPreview(payload),
		// )
		wsTimelineLog.BeginRequest()
		wsTimelineLog.Append("request", payload, time.Now())
		if strings.TrimSpace(gjson.GetBytes(payload, "type").String()) == wsRequestTypeProcessed {
			responseID := strings.TrimSpace(gjson.GetBytes(payload, "response_id").String())
			if responseID != "" && sendResponseProcessed != nil {
				if errSendProcessed := sendResponseProcessed(responseID); errSendProcessed != nil {
					log.Debugf("responses websocket: failed to send response.processed id=%s response_id=%s error=%v", passthroughSessionID, responseID, errSendProcessed)
				}
			}
			continue
		}
		requestInput := gjson.GetBytes(payload, "input")
		requestInputContainsFullTranscript := inputContainsFullTranscript(requestInput)
		requestReplacesTranscript := requestInputContainsFullTranscript ||
			responsesWebsocketRequestReplacesTranscript(payload, requestInput, lastRequest)
		if requestReplacesTranscript && dropUpstreamSession != nil {
			dropUpstreamSession("compact_replay")
		}

		explicitRequestModelName := strings.TrimSpace(gjson.GetBytes(payload, "model").String())
		requestModelName := explicitRequestModelName
		if requestModelName == "" {
			requestModelName = passthroughModelName
		}
		if requestModelName == "" {
			requestModelName = strings.TrimSpace(gjson.GetBytes(lastRequest, "model").String())
		}
		executionParent := context.WithValue(c.Request.Context(), "gin", c)
		executionParent, routeOverridesModelResolution := h.PrepareStreamModelRoute(
			executionParent,
			h.HandlerType(),
			requestModelName,
			payload,
		)
		if pinnedAuthID != "" {
			pinnedAuth, homeRuntime, ok := sessionAuthByIDWithSource(pinnedAuthID)
			providerKey := ""
			if pinnedAuth != nil {
				providerKey = strings.ToLower(strings.TrimSpace(pinnedAuth.Provider))
			}
			state, hasState := pinnedAuthByProvider[providerKey]
			if !ok || !hasState || state.authID != pinnedAuthID || !responsesWebsocketPinnedAuthMatchesModel(pinnedAuth, requestModelName, state.modelKey, homeRuntime) {
				pinnedAuthID = ""
			}
		}
		if pinnedAuthID == "" {
			providerSet, _ := responsesWebsocketProviderSetForModel(responsesWebsocketResolvedModelName(requestModelName))
			if len(providerSet) == 1 {
				for providerKey := range providerSet {
					state, ok := pinnedAuthByProvider[providerKey]
					candidateAuth, homeRuntime, okAuth := sessionAuthByIDWithSource(state.authID)
					if ok && okAuth && responsesWebsocketPinnedAuthMatchesModel(candidateAuth, requestModelName, state.modelKey, homeRuntime) {
						pinnedAuthID = state.authID
					} else {
						delete(pinnedAuthByProvider, providerKey)
					}
				}
			}
		}
		useUpstreamWebsocketPassthrough := h.responsesWebsocketUsesUpstreamWebsocketPassthrough(requestModelName)
		if pinnedAuthID != "" {
			if pinnedAuth, ok := sessionAuthByID(pinnedAuthID); ok && responsesWebsocketAuthSupportsIncrementalInput(pinnedAuth) {
				provider := strings.ToLower(strings.TrimSpace(pinnedAuth.Provider))
				useUpstreamWebsocketPassthrough = provider == "codex" || provider == "xai"
			}
		}
		nativeWebsocketPassthrough := !routeOverridesModelResolution && responsesWebsocketNativePassthroughAllowed(
			upstreamMode,
			useUpstreamWebsocketPassthrough,
			pinnedAuthID,
			upstreamAuthID,
		)
		requestRequiresCurrentUpstreamWebsocket := responsesWebsocketRequestRequiresCurrentUpstream(payload)
		if upstreamMode == responsesWebsocketUpstreamModeWS && !nativeWebsocketPassthrough {
			if requestRequiresCurrentUpstreamWebsocket {
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
			// A full response.create is already a self-contained reset and can safely
			// establish a new upstream transport without another replay.
		}
		if explicitRequestModelName != "" && !useUpstreamWebsocketPassthrough {
			passthroughModelName = ""
		}
		replayCurrentRequest := false
		transcriptReplayRetries := 0
		forceHTTPUpstream := upstreamMode == responsesWebsocketUpstreamModeHTTP &&
			strings.TrimSpace(pinnedAuthID) != "" &&
			strings.TrimSpace(pinnedAuthID) == strings.TrimSpace(upstreamAuthID)
		httpFallbackAttempted := forceHTTPUpstream
	retryCurrentRequest:
		currentUpstreamGeneration := seenUpstreamGeneration
		upstreamGenerationChanged := false
		if upstreamGeneration != nil {
			currentUpstreamGeneration = upstreamGeneration()
			upstreamGenerationChanged = currentUpstreamGeneration != seenUpstreamGeneration
		}

		forcedTranscriptReplay := forceTranscriptReplayNextRequest || upstreamGenerationChanged || replayCurrentRequest
		executeNativeWebsocketPassthrough := nativeWebsocketPassthrough && !forcedTranscriptReplay
		allowCompactionReplayBypass := false
		if pinnedAuthID != "" {
			if pinnedAuth, ok := sessionAuthByID(pinnedAuthID); ok && pinnedAuth != nil {
				allowCompactionReplayBypass = responsesWebsocketAuthSupportsCompactionReplay(pinnedAuth)
			}
		} else {
			allowCompactionReplayBypass = h.websocketUpstreamSupportsCompactionReplayForModel(requestModelName)
		}

		var requestJSON []byte
		var updatedLastRequest []byte
		var errMsg *interfaces.ErrorMessage
		if executeNativeWebsocketPassthrough {
			requestJSON, errMsg = normalizeResponsesWebsocketPassthroughRequest(payload, requestModelName)
			if errMsg == nil && requestReplacesTranscript {
				requestJSON, _ = sjson.DeleteBytes(requestJSON, "previous_response_id")
			}
			if errMsg == nil {
				_, updatedLastRequest, errMsg = normalizeResponsesWebsocketRequestWithReplayMode(
					payload,
					lastRequest,
					lastResponseOutput,
					lastResponseID,
					lastResponsePendingToolCallIDs,
					false,
					allowCompactionReplayBypass,
					false,
					false,
				)
			}
		} else if len(lastRequest) == 0 && strings.TrimSpace(gjson.GetBytes(payload, "previous_response_id").String()) != "" {
			errMsg = responsesWebsocketPreviousResponseNotFoundError()
		} else {
			requestJSON, updatedLastRequest, errMsg = normalizeResponsesWebsocketRequestWithReplayMode(
				payload,
				lastRequest,
				lastResponseOutput,
				lastResponseID,
				lastResponsePendingToolCallIDs,
				false,
				allowCompactionReplayBypass,
				false,
				forcedTranscriptReplay,
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
		resetToolRepairState := requestReplacesTranscript
		toolCacheTurn := newResponsesWebsocketToolCacheTurn(downstreamSessionKey)
		if resetToolRepairState {
			toolCacheTurn.resetOnCommit()
		}
		if !executeNativeWebsocketPassthrough && shouldHandleResponsesWebsocketPrewarmLocally(payload, lastRequest, false) {
			if updated, errDelete := sjson.DeleteBytes(requestJSON, "generate"); errDelete == nil {
				requestJSON = updated
			}
			if updated, errDelete := sjson.DeleteBytes(updatedLastRequest, "generate"); errDelete == nil {
				updatedLastRequest = updated
			}
			lastRequest = updatedLastRequest
			lastResponseOutput = []byte("[]")
			lastResponseID = ""
			lastResponsePendingToolCallIDs = nil
			if errWrite := writeResponsesWebsocketSyntheticPrewarm(c, writer, requestJSON, wsTimelineLog, passthroughSessionID); errWrite != nil {
				wsTerminateErr = errWrite
				return
			}
			toolCacheTurn.commit()
			continue
		}

		requestBeforeRepair := bytes.Clone(requestJSON)
		toolCacheTurn.recordRequest(requestJSON)
		if !executeNativeWebsocketPassthrough && !resetToolRepairState {
			requestJSON = repairResponsesWebsocketToolCallsWithoutRecording(downstreamSessionKey, requestJSON)
		}
		if !executeNativeWebsocketPassthrough {
			requestJSON = dedupeResponsesWebsocketInputItemsByID(requestJSON)
		}
		if bytes.Equal(updatedLastRequest, requestBeforeRepair) {
			updatedLastRequest = bytes.Clone(requestJSON)
		} else {
			if !resetToolRepairState {
				updatedLastRequest = repairResponsesWebsocketToolCallsWithoutRecording(downstreamSessionKey, updatedLastRequest)
			}
			updatedLastRequest = dedupeResponsesWebsocketInputItemsByID(updatedLastRequest)
		}
		previousLastRequest := bytes.Clone(lastRequest)
		previousLastResponseOutput := bytes.Clone(lastResponseOutput)
		previousLastResponseID := lastResponseID
		previousLastResponsePendingToolCallIDs := append([]string(nil), lastResponsePendingToolCallIDs...)
		previousForceTranscriptReplayNextRequest := forceTranscriptReplayNextRequest
		if executeNativeWebsocketPassthrough {
			if modelName := strings.TrimSpace(gjson.GetBytes(requestJSON, "model").String()); modelName != "" {
				passthroughModelName = modelName
			}
			if len(updatedLastRequest) > 0 {
				lastRequest = updatedLastRequest
			}
			if forcedTranscriptReplay {
				forceTranscriptReplayNextRequest = false
			}
		} else {
			lastRequest = updatedLastRequest
			if forcedTranscriptReplay {
				forceTranscriptReplayNextRequest = false
			}
		}

		modelName := gjson.GetBytes(requestJSON, "model").String()
		lastAttemptedAuthID := pinnedAuthID
		attemptedUpstreamMode := responsesWebsocketUpstreamModeUnknown
		selectedAuthObserved := false
		pinnedAuthAttempted := false
		cliCtx, cliCancel := h.GetContextWithCancel(h, c, executionParent)
		if !forceHTTPUpstream {
			cliCtx = cliproxyexecutor.WithDownstreamWebsocket(cliCtx)
		}
		if executeNativeWebsocketPassthrough && requestRequiresCurrentUpstreamWebsocket {
			cliCtx = cliproxyexecutor.WithRequiredUpstreamWebsocket(cliCtx)
		}
		cliCtx = handlers.WithExecutionSessionID(cliCtx, passthroughSessionID)
		cliCtx = handlers.WithSelectedAuthIDCallback(cliCtx, func(authID string) {
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
		})
		if pinnedAuthID != "" && !routeOverridesModelResolution {
			cliCtx = handlers.WithPinnedAuthID(cliCtx, pinnedAuthID)
		}
		dataChan, upstreamHeaders, errChan := h.ExecuteStreamWithAuthManager(cliCtx, h.HandlerType(), modelName, requestJSON, "")
		if forceHTTPUpstream || !selectedAuthObserved {
			// Plugin/alternate routes bypass auth selection. Keep canonical HTTP-mode
			// state instead of inheriting the previous pinned websocket mode.
			attemptedUpstreamMode = responsesWebsocketUpstreamModeHTTP
		}
		// A connection-scoped continuation cannot rotate credentials in place. Suppress
		// credential errors and make the client replay the full turn on a new socket.
		replayPinnedAuthFailure := func(errMsg *interfaces.ErrorMessage) bool {
			return executeNativeWebsocketPassthrough && requestRequiresCurrentUpstreamWebsocket && pinnedAuthAttempted &&
				shouldReplayResponsesWebsocketPinnedAuthFailure(errMsg)
		}

		allowTranscriptReplayBeforeOutput := transcriptReplayRetries < wsTranscriptReplayMaxRetries
		completedOutput, completedResponseID, completedPendingToolCallIDs, forwardErrMsg, replayAllowed, errForward := h.forwardResponsesWebsocket(
			c,
			writer,
			cliCancel,
			dataChan,
			errChan,
			upstreamHeaders,
			wsTimelineLog,
			passthroughSessionID,
			responsesWebsocketForwardOptions{
				toolCacheTurn:                     toolCacheTurn,
				suppressError:                     replayPinnedAuthFailure,
				allowTranscriptReplayBeforeOutput: allowTranscriptReplayBeforeOutput,
				allowHTTPFallbackBeforeOutput: !executeNativeWebsocketPassthrough &&
					!httpFallbackAttempted &&
					attemptedUpstreamMode == responsesWebsocketUpstreamModeWS,
			},
		)
		if errForward != nil {
			wsTerminateErr = errForward
			if !errors.Is(errForward, websocket.ErrCloseSent) {
				log.Warnf("responses websocket: forward failed id=%s error=%v", passthroughSessionID, errForward)
			}
			return
		}
		if replayAllowed {
			switch {
			case !httpFallbackAttempted && shouldRetryResponsesWebsocketHTTPFallback(forwardErrMsg):
				httpFallbackAttempted = true
				forceHTTPUpstream = true
				replayCurrentRequest = true
				forceTranscriptReplayNextRequest = false
				lastRequest = previousLastRequest
				lastResponseOutput = previousLastResponseOutput
				lastResponseID = previousLastResponseID
				lastResponsePendingToolCallIDs = previousLastResponsePendingToolCallIDs
				goto retryCurrentRequest
			case allowTranscriptReplayBeforeOutput && shouldRetryResponsesWebsocketTranscriptReplay(forwardErrMsg):
				transcriptReplayRetries++
				replayCurrentRequest = true
				forceTranscriptReplayNextRequest = false
				lastRequest = previousLastRequest
				lastResponseOutput = previousLastResponseOutput
				lastResponseID = previousLastResponseID
				lastResponsePendingToolCallIDs = previousLastResponsePendingToolCallIDs
				goto retryCurrentRequest
			}
		}
		if forwardErrMsg != nil {
			lastRequest = previousLastRequest
			lastResponseOutput = previousLastResponseOutput
			lastResponseID = previousLastResponseID
			lastResponsePendingToolCallIDs = previousLastResponsePendingToolCallIDs
			forceTranscriptReplayNextRequest = previousForceTranscriptReplayNextRequest
			if shouldRetryResponsesWebsocketTranscriptReplay(forwardErrMsg) {
				forceTranscriptReplayNextRequest = true
			}
			if pinnedAuthAttempted && shouldReleaseResponsesWebsocketPinnedAuth(forwardErrMsg) {
				forgetPinnedAuth()
			}
			if replayPinnedAuthFailure(forwardErrMsg) {
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

		toolCacheTurn.commit()
		upstreamMode = attemptedUpstreamMode
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
		} else {
			if httpFallbackAttempted && selectedAuthObserved && lastAttemptedAuthID != "" {
				rememberPinnedAuth(lastAttemptedAuthID, modelName)
			}
		}
		lastResponseOutput = completedOutput
		lastResponseID = strings.TrimSpace(completedResponseID)
		lastResponsePendingToolCallIDs = append([]string(nil), completedPendingToolCallIDs...)
		seenUpstreamGeneration = currentUpstreamGeneration
	}
}

func responsesWebsocketHTTPReplayRequiredError() error {
	return cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
}

func responsesWebsocketRequestRequiresCurrentUpstream(payload []byte) bool {
	return strings.TrimSpace(gjson.GetBytes(payload, "previous_response_id").String()) != "" ||
		strings.TrimSpace(gjson.GetBytes(payload, "type").String()) == wsRequestTypeAppend
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

func normalizeResponsesWebsocketRequest(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte) ([]byte, []byte, *interfaces.ErrorMessage) {
	return normalizeResponsesWebsocketRequestWithMode(rawJSON, lastRequest, lastResponseOutput, true, true, false)
}

func normalizeResponsesWebsocketRequestWithMode(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte, allowIncrementalInputWithPreviousResponseID bool, allowCompactionReplayBypass bool, forceTranscriptReplacement bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	return normalizeResponsesWebsocketRequestWithReplayMode(rawJSON, lastRequest, lastResponseOutput, "", nil, allowIncrementalInputWithPreviousResponseID, allowCompactionReplayBypass, forceTranscriptReplacement, false)
}

func normalizeResponsesWebsocketRequestWithLastResponseID(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte, lastResponseID string, allowIncrementalInputWithPreviousResponseID bool, allowCompactionReplayBypass bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	return normalizeResponsesWebsocketRequestWithIncrementalState(rawJSON, lastRequest, lastResponseOutput, lastResponseID, nil, allowIncrementalInputWithPreviousResponseID, allowCompactionReplayBypass)
}

func normalizeResponsesWebsocketRequestWithIncrementalState(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte, lastResponseID string, lastResponsePendingToolCallIDs []string, allowIncrementalInputWithPreviousResponseID bool, allowCompactionReplayBypass bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	return normalizeResponsesWebsocketRequestWithReplayMode(rawJSON, lastRequest, lastResponseOutput, lastResponseID, lastResponsePendingToolCallIDs, allowIncrementalInputWithPreviousResponseID, allowCompactionReplayBypass, false, false)
}

func normalizeResponsesWebsocketRequestWithReplayMode(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte, lastResponseID string, lastResponsePendingToolCallIDs []string, allowIncrementalInputWithPreviousResponseID bool, allowCompactionReplayBypass bool, forceTranscriptReplacement bool, forceTranscriptReplay bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	requestType := strings.TrimSpace(gjson.GetBytes(rawJSON, "type").String())
	switch requestType {
	case wsRequestTypeCreate:
		// log.Infof("responses websocket: response.create request")
		if len(lastRequest) == 0 {
			dropPreviousResponseID := forceTranscriptReplacement || inputContainsFullTranscript(gjson.GetBytes(rawJSON, "input"))
			return normalizeResponseCreateRequest(rawJSON, dropPreviousResponseID, allowCompactionReplayBypass)
		}
		return normalizeResponseSubsequentRequest(rawJSON, lastRequest, lastResponseOutput, lastResponseID, lastResponsePendingToolCallIDs, allowIncrementalInputWithPreviousResponseID, allowCompactionReplayBypass, forceTranscriptReplacement, forceTranscriptReplay)
	case wsRequestTypeAppend:
		// log.Infof("responses websocket: response.append request")
		return normalizeResponseSubsequentRequest(rawJSON, lastRequest, lastResponseOutput, lastResponseID, lastResponsePendingToolCallIDs, allowIncrementalInputWithPreviousResponseID, allowCompactionReplayBypass, forceTranscriptReplacement, forceTranscriptReplay)
	default:
		return nil, lastRequest, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("unsupported websocket request type: %s", requestType),
		}
	}
}

func normalizeResponseCreateRequest(rawJSON []byte, dropPreviousResponseID bool, allowCompactionReplayBypass bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	normalized, errDelete := sjson.DeleteBytes(rawJSON, "type")
	if errDelete != nil {
		normalized = bytes.Clone(rawJSON)
	}
	if dropPreviousResponseID {
		normalized, _ = sjson.DeleteBytes(normalized, "previous_response_id")
	}
	normalized, _ = sjson.SetBytes(normalized, "stream", true)
	if !gjson.GetBytes(normalized, "input").Exists() {
		normalized, _ = sjson.SetRawBytes(normalized, "input", []byte("[]"))
	}
	input := gjson.GetBytes(normalized, "input")
	if inputContainsFullTranscript(input) && !allowCompactionReplayBypass {
		normalized, _ = sjson.SetRawBytes(normalized, "input", []byte(inputWithoutCompactionItems(input)))
	}

	modelName := strings.TrimSpace(gjson.GetBytes(normalized, "model").String())
	if modelName == "" {
		return nil, nil, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("missing model in response.create request"),
		}
	}
	return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
}

func normalizeResponseSubsequentRequest(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte, lastResponseID string, lastResponsePendingToolCallIDs []string, allowIncrementalInputWithPreviousResponseID bool, allowCompactionReplayBypass bool, forceTranscriptReplacement bool, forceTranscriptReplay bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	if len(lastRequest) == 0 {
		return nil, lastRequest, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("websocket request received before response.create"),
		}
	}

	nextInput := gjson.GetBytes(rawJSON, "input")
	if !nextInput.Exists() || !nextInput.IsArray() {
		return nil, lastRequest, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("websocket request requires array field: input"),
		}
	}

	if inputContainsFullTranscript(nextInput) {
		normalized, errMsg := buildResponsesWebsocketTranscriptState(rawJSON, lastRequest, lastResponseOutput, nextInput, allowCompactionReplayBypass)
		if errMsg != nil {
			return nil, lastRequest, errMsg
		}
		return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
	}

	// When the input already contains historical model output items but no compact
	// marker, treating it as an incremental append
	// duplicates stale turn-state and can leave late orphaned function_call items.
	if responsesWebsocketRequestReplacesTranscript(rawJSON, nextInput, lastRequest) {
		normalized := normalizeResponseTranscriptReplacement(rawJSON, lastRequest)
		return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
	}

	if forceTranscriptReplacement {
		normalized := normalizeResponseTranscriptReplacement(rawJSON, lastRequest)
		return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
	}

	if inputContainsCompactionTrigger(nextInput) {
		normalized, errMsg := buildResponsesWebsocketTranscriptState(rawJSON, lastRequest, lastResponseOutput, nextInput, allowCompactionReplayBypass)
		if errMsg != nil {
			return nil, lastRequest, errMsg
		}
		return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
	}

	if forceTranscriptReplay {
		normalized, errMsg := buildResponsesWebsocketTranscriptState(rawJSON, lastRequest, lastResponseOutput, nextInput, allowCompactionReplayBypass)
		if errMsg != nil {
			return nil, lastRequest, errMsg
		}
		return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
	}

	// Websocket v2 mode uses response.create with previous_response_id + incremental input.
	// Do not expand it into a full input transcript; upstream expects the incremental payload.
	if allowIncrementalInputWithPreviousResponseID {
		prev := strings.TrimSpace(gjson.GetBytes(rawJSON, "previous_response_id").String())
		if prev == "" {
			if !inputSatisfiesPendingToolCalls(nextInput, lastResponsePendingToolCallIDs) {
				normalized := normalizeResponseTranscriptReplacement(rawJSON, lastRequest)
				return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
			}
			prev = strings.TrimSpace(lastResponseID)
		}
		if prev != "" {
			normalized, errDelete := sjson.DeleteBytes(rawJSON, "type")
			if errDelete != nil {
				normalized = bytes.Clone(rawJSON)
			}
			normalized, _ = sjson.SetBytes(normalized, "previous_response_id", prev)
			if !gjson.GetBytes(normalized, "model").Exists() {
				modelName := strings.TrimSpace(gjson.GetBytes(lastRequest, "model").String())
				if modelName != "" {
					normalized, _ = sjson.SetBytes(normalized, "model", modelName)
				}
			}
			if !gjson.GetBytes(normalized, "instructions").Exists() {
				instructions := gjson.GetBytes(lastRequest, "instructions")
				if instructions.Exists() {
					normalized, _ = sjson.SetRawBytes(normalized, "instructions", []byte(instructions.Raw))
				}
			}
			normalized, _ = sjson.SetBytes(normalized, "stream", true)
			updatedLastRequest, errMsg := buildResponsesWebsocketTranscriptState(rawJSON, lastRequest, lastResponseOutput, nextInput, allowCompactionReplayBypass)
			if errMsg != nil {
				return nil, lastRequest, errMsg
			}
			return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(updatedLastRequest), nil
		}
	}

	normalized, errMsg := buildResponsesWebsocketTranscriptState(rawJSON, lastRequest, lastResponseOutput, nextInput, allowCompactionReplayBypass)
	if errMsg != nil {
		return nil, lastRequest, errMsg
	}
	return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
}

func buildResponsesWebsocketTranscriptState(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte, nextInput gjson.Result, allowCompactionReplayBypass bool) ([]byte, *interfaces.ErrorMessage) {
	// When the client sends a compact replay, the input already carries the
	// canonical post-compaction history. In that case, skip merging with stale
	// lastRequest/lastResponseOutput to avoid re-inflating compacted context or
	// breaking function_call / function_call_output pairings.
	// See: https://github.com/router-for-me/CLIProxyAPI/issues/2207
	var mergedInput string
	if inputContainsFullTranscript(nextInput) {
		if allowCompactionReplayBypass {
			log.Infof("responses websocket: full transcript detected, skipping stale merge (input items=%d)", len(nextInput.Array()))
			mergedInput = nextInput.Raw
		} else {
			log.Infof("responses websocket: full transcript detected, stripping compaction items for unsupported upstream (input items=%d)", len(nextInput.Array()))
			mergedInput = inputWithoutCompactionItems(nextInput)
		}
	} else {
		appendInputRaw := nextInput.Raw
		existingInput := gjson.GetBytes(lastRequest, "input")
		var errMerge error
		mergedInput, errMerge = mergeJSONArrayRaw(existingInput.Raw, normalizeJSONArrayRaw(lastResponseOutput))
		if errMerge != nil {
			return nil, &interfaces.ErrorMessage{
				StatusCode: http.StatusBadRequest,
				Error:      fmt.Errorf("invalid previous response output: %w", errMerge),
			}
		}

		mergedInput, errMerge = mergeJSONArrayRaw(mergedInput, appendInputRaw)
		if errMerge != nil {
			return nil, &interfaces.ErrorMessage{
				StatusCode: http.StatusBadRequest,
				Error:      fmt.Errorf("invalid request input: %w", errMerge),
			}
		}
	}
	dedupedInput, errDedupeFunctionCalls := dedupeFunctionCallsByCallID(mergedInput)
	if errDedupeFunctionCalls == nil {
		mergedInput = dedupedInput
	}
	dedupedInput, errDedupeItemIDs := dedupeInputItemsByID(mergedInput)
	if errDedupeItemIDs == nil {
		mergedInput = dedupedInput
	}

	normalized, errDelete := sjson.DeleteBytes(rawJSON, "type")
	if errDelete != nil {
		normalized = bytes.Clone(rawJSON)
	}
	normalized, _ = sjson.DeleteBytes(normalized, "previous_response_id")
	var errSet error
	normalized, errSet = sjson.SetRawBytes(normalized, "input", []byte(mergedInput))
	if errSet != nil {
		return nil, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("failed to merge websocket input: %w", errSet),
		}
	}
	if !gjson.GetBytes(normalized, "model").Exists() {
		modelName := strings.TrimSpace(gjson.GetBytes(lastRequest, "model").String())
		if modelName != "" {
			normalized, _ = sjson.SetBytes(normalized, "model", modelName)
		}
	}
	if !gjson.GetBytes(normalized, "instructions").Exists() {
		instructions := gjson.GetBytes(lastRequest, "instructions")
		if instructions.Exists() {
			normalized, _ = sjson.SetRawBytes(normalized, "instructions", []byte(instructions.Raw))
		}
	}
	normalized, _ = sjson.SetBytes(normalized, "stream", true)
	return normalized, nil
}

func responsesWebsocketSnapshotWithoutCompactionTriggers(payload []byte) []byte {
	input := gjson.GetBytes(payload, "input")
	if !input.IsArray() {
		return bytes.Clone(payload)
	}
	filtered := make([]string, 0, len(input.Array()))
	removed := false
	for _, item := range input.Array() {
		if strings.TrimSpace(item.Get("type").String()) == "compaction_trigger" {
			removed = true
			continue
		}
		filtered = append(filtered, item.Raw)
	}
	if !removed {
		return bytes.Clone(payload)
	}
	out, errSet := sjson.SetRawBytes(payload, "input", []byte("["+strings.Join(filtered, ",")+"]"))
	if errSet != nil {
		return bytes.Clone(payload)
	}
	return out
}

func shouldReplaceWebsocketTranscript(rawJSON []byte, nextInput gjson.Result, lastRequest []byte) bool {
	requestType := strings.TrimSpace(gjson.GetBytes(rawJSON, "type").String())
	if requestType != wsRequestTypeCreate && requestType != wsRequestTypeAppend {
		return false
	}
	previousResponseID := gjson.GetBytes(rawJSON, "previous_response_id")
	if strings.TrimSpace(previousResponseID.String()) != "" {
		return false
	}
	if !nextInput.Exists() || !nextInput.IsArray() {
		return false
	}
	if requestType == wsRequestTypeCreate && !previousResponseID.Exists() && inputHasCodexLocalCompactionSummary(nextInput) {
		return true
	}

	for _, item := range nextInput.Array() {
		switch strings.TrimSpace(item.Get("type").String()) {
		case "function_call", "custom_tool_call":
			return true
		case "message":
			if strings.TrimSpace(item.Get("role").String()) == "assistant" {
				return true
			}
		}
	}

	return inputStartsWithPreviousRequestInput(nextInput, lastRequest)
}

func responsesWebsocketRequestReplacesTranscript(rawJSON []byte, nextInput gjson.Result, lastRequest []byte) bool {
	return shouldReplaceWebsocketTranscript(rawJSON, nextInput, lastRequest) ||
		isCodexFullWebsocketCreateWithoutPreviousResponseID(rawJSON)
}

func isCodexFullWebsocketCreateWithoutPreviousResponseID(rawJSON []byte) bool {
	if strings.TrimSpace(gjson.GetBytes(rawJSON, "type").String()) != wsRequestTypeCreate {
		return false
	}
	if strings.TrimSpace(gjson.GetBytes(rawJSON, "previous_response_id").String()) != "" {
		return false
	}
	if strings.TrimSpace(gjson.GetBytes(rawJSON, "model").String()) == "" {
		return false
	}
	clientMetadata := gjson.GetBytes(rawJSON, "client_metadata")
	if !clientMetadata.IsObject() {
		return false
	}
	responsesLite := strings.EqualFold(strings.TrimSpace(clientMetadata.Get(wsResponsesLiteMetadataKey).String()), "true")
	if (!gjson.GetBytes(rawJSON, "tools").Exists() && !responsesLite) ||
		!gjson.GetBytes(rawJSON, "tool_choice").Exists() ||
		!gjson.GetBytes(rawJSON, "parallel_tool_calls").Exists() ||
		!gjson.GetBytes(rawJSON, "store").Exists() ||
		!gjson.GetBytes(rawJSON, "stream").Exists() ||
		!gjson.GetBytes(rawJSON, "include").Exists() {
		return false
	}
	return strings.TrimSpace(clientMetadata.Get("x-codex-installation-id").String()) != "" &&
		strings.TrimSpace(clientMetadata.Get("x-codex-window-id").String()) != ""
}

func inputStartsWithPreviousRequestInput(nextInput gjson.Result, lastRequest []byte) bool {
	if !nextInput.Exists() || !nextInput.IsArray() {
		return false
	}
	previousInput := gjson.GetBytes(lastRequest, "input")
	if !previousInput.Exists() || !previousInput.IsArray() {
		return false
	}
	previousItems := previousInput.Array()
	nextItems := nextInput.Array()
	if len(previousItems) == 0 || len(nextItems) < len(previousItems) {
		return false
	}
	for i := range previousItems {
		if !jsonRawValuesEqual(previousItems[i].Raw, nextItems[i].Raw) {
			return false
		}
	}
	return true
}

func jsonRawValuesEqual(a, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if a == b {
		return true
	}
	normalizedA, okA := normalizeJSONValueRaw(a)
	normalizedB, okB := normalizeJSONValueRaw(b)
	if okA && okB {
		return bytes.Equal(normalizedA, normalizedB)
	}
	return false
}

func inputHasCodexLocalCompactionSummary(input gjson.Result) bool {
	if !input.IsArray() {
		return false
	}

	hasSummary := false
	for index, item := range input.Array() {
		itemType := strings.TrimSpace(item.Get("type").String())
		if itemType == "additional_tools" {
			tools := item.Get("tools")
			if index != 0 || strings.TrimSpace(item.Get("role").String()) != "developer" || !tools.IsArray() {
				return false
			}
			for _, tool := range tools.Array() {
				if !tool.IsObject() || strings.TrimSpace(tool.Get("type").String()) == "" {
					return false
				}
			}
			continue
		}
		if itemType != "" && itemType != "message" {
			return false
		}

		role := strings.TrimSpace(item.Get("role").String())
		if role != "user" && role != "developer" {
			return false
		}
		if role == "user" && strings.HasPrefix(codexLocalCompactionMessageText(item), codexLocalCompactionSummaryPrefix+"\n") {
			hasSummary = true
		}
	}
	return hasSummary
}

func codexLocalCompactionMessageText(message gjson.Result) string {
	content := message.Get("content")
	if content.Type == gjson.String {
		return content.String()
	}
	if !content.IsArray() {
		return ""
	}

	var text strings.Builder
	for _, part := range content.Array() {
		if strings.TrimSpace(part.Get("type").String()) == "input_text" {
			text.WriteString(part.Get("text").String())
		}
	}
	return text.String()
}

func inputSatisfiesPendingToolCalls(input gjson.Result, pendingCallIDs []string) bool {
	if len(pendingCallIDs) == 0 {
		return true
	}
	if !input.IsArray() {
		return false
	}
	outputs := make(map[string]struct{}, len(pendingCallIDs))
	for _, item := range input.Array() {
		switch strings.TrimSpace(item.Get("type").String()) {
		case "function_call_output", "custom_tool_call_output":
			callID := strings.TrimSpace(item.Get("call_id").String())
			if callID != "" {
				outputs[callID] = struct{}{}
			}
		}
	}
	for _, callID := range pendingCallIDs {
		callID = strings.TrimSpace(callID)
		if callID == "" {
			continue
		}
		if _, ok := outputs[callID]; !ok {
			return false
		}
	}
	return true
}

func normalizeJSONValueRaw(raw string) ([]byte, bool) {
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, false
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}
	return normalized, true
}

func normalizeResponseTranscriptReplacement(rawJSON []byte, lastRequest []byte) []byte {
	normalized, errDelete := sjson.DeleteBytes(rawJSON, "type")
	if errDelete != nil {
		normalized = bytes.Clone(rawJSON)
	}
	normalized, _ = sjson.DeleteBytes(normalized, "previous_response_id")
	if !gjson.GetBytes(normalized, "model").Exists() {
		modelName := strings.TrimSpace(gjson.GetBytes(lastRequest, "model").String())
		if modelName != "" {
			normalized, _ = sjson.SetBytes(normalized, "model", modelName)
		}
	}
	if !gjson.GetBytes(normalized, "instructions").Exists() {
		instructions := gjson.GetBytes(lastRequest, "instructions")
		if instructions.Exists() {
			normalized, _ = sjson.SetRawBytes(normalized, "instructions", []byte(instructions.Raw))
		}
	}
	normalized, _ = sjson.SetBytes(normalized, "stream", true)
	return bytes.Clone(normalized)
}

func dedupeFunctionCallsByCallID(rawArray string) (string, error) {
	rawArray = strings.TrimSpace(rawArray)
	if rawArray == "" {
		return "[]", nil
	}
	var items []json.RawMessage
	if errUnmarshal := json.Unmarshal([]byte(rawArray), &items); errUnmarshal != nil {
		return "", errUnmarshal
	}

	seenCallIDs := make(map[string]struct{}, len(items))
	filtered := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		if len(item) == 0 {
			continue
		}
		itemType := strings.TrimSpace(gjson.GetBytes(item, "type").String())
		if isResponsesToolCallType(itemType) {
			callID := strings.TrimSpace(gjson.GetBytes(item, "call_id").String())
			if callID != "" {
				if _, ok := seenCallIDs[callID]; ok {
					continue
				}
				seenCallIDs[callID] = struct{}{}
			}
		}
		filtered = append(filtered, item)
	}

	out, errMarshal := json.Marshal(filtered)
	if errMarshal != nil {
		return "", errMarshal
	}
	return string(out), nil
}

func dedupeResponsesWebsocketInputItemsByID(payload []byte) []byte {
	input := gjson.GetBytes(payload, "input")
	if !input.Exists() || !input.IsArray() {
		return payload
	}
	dedupedInput, errDedupe := dedupeInputItemsByID(input.Raw)
	if errDedupe != nil || dedupedInput == input.Raw {
		return payload
	}
	updated, errSet := sjson.SetRawBytes(payload, "input", []byte(dedupedInput))
	if errSet != nil {
		return payload
	}
	return updated
}

func dedupeInputItemsByID(rawArray string) (string, error) {
	rawArray = strings.TrimSpace(rawArray)
	if rawArray == "" {
		return "[]", nil
	}
	var items []json.RawMessage
	if errUnmarshal := json.Unmarshal([]byte(rawArray), &items); errUnmarshal != nil {
		return "", errUnmarshal
	}

	// Parse each item's type, id and call_id once; gjson is a scan-based
	// parser, so reusing this metadata avoids rescanning every item in each of
	// the loops below as the conversation history grows.
	type itemMetadata struct {
		itemType string
		id       string
		callID   string
	}
	meta := make([]itemMetadata, len(items))
	for i, item := range items {
		if len(item) == 0 {
			continue
		}
		res := gjson.GetManyBytes(item, "type", "id", "call_id")
		meta[i] = itemMetadata{
			itemType: strings.TrimSpace(res[0].String()),
			id:       strings.TrimSpace(res[1].String()),
			callID:   strings.TrimSpace(res[2].String()),
		}
	}

	// Collect the call_ids that are still referenced by tool-call output
	// items. When several input items share the same id, the one we keep must
	// preserve any call_id that has a matching output; otherwise the upstream
	// rejects the request with "No tool call found for function call output".
	referencedCallIDs := make(map[string]struct{}, len(items))
	for i := range items {
		switch meta[i].itemType {
		case "function_call_output", "custom_tool_call_output":
			if meta[i].callID != "" {
				referencedCallIDs[meta[i].callID] = struct{}{}
			}
		}
	}

	// For each id, choose the index that keeps it. The default is the last
	// occurrence (matching the original dedupe behavior), but we never replace
	// an item whose call_id still has a matching output with one that does not.
	// Additional referenced calls sharing that id are retained without their
	// optional id below so their outputs remain paired.
	keepIndexByID := make(map[string]int, len(items))
	keepReferencedByID := make(map[string]bool, len(items))
	for i := range items {
		itemID := meta[i].id
		if itemID == "" {
			continue
		}
		_, referenced := referencedCallIDs[meta[i].callID]
		referenced = referenced && meta[i].callID != ""
		if _, seen := keepIndexByID[itemID]; !seen {
			keepIndexByID[itemID] = i
			keepReferencedByID[itemID] = referenced
			continue
		}
		if referenced || !keepReferencedByID[itemID] {
			keepIndexByID[itemID] = i
			keepReferencedByID[itemID] = referenced
		}
	}

	filtered := make([]json.RawMessage, 0, len(items))
	for i, item := range items {
		if len(item) == 0 {
			continue
		}
		itemID := meta[i].id
		if itemID != "" {
			keepIndex := keepIndexByID[itemID]
			if keepIndex != i {
				_, referenced := referencedCallIDs[meta[i].callID]
				if !isResponsesToolCallType(meta[i].itemType) ||
					meta[i].callID == "" ||
					meta[i].callID == meta[keepIndex].callID ||
					!referenced {
					continue
				}
				itemWithoutID, errDeleteID := sjson.DeleteBytes(item, "id")
				if errDeleteID != nil {
					return "", errDeleteID
				}
				item = itemWithoutID
			}
		}
		filtered = append(filtered, item)
	}

	out, errMarshal := json.Marshal(filtered)
	if errMarshal != nil {
		return "", errMarshal
	}
	return string(out), nil
}

func websocketUpstreamSupportsIncrementalInput(attributes map[string]string, metadata map[string]any) bool {
	if len(attributes) > 0 {
		if raw := strings.TrimSpace(attributes["websockets"]); raw != "" {
			parsed, errParse := strconv.ParseBool(raw)
			if errParse == nil {
				return parsed
			}
		}
	}
	if len(metadata) == 0 {
		return false
	}
	raw, ok := metadata["websockets"]
	if !ok || raw == nil {
		return false
	}
	switch value := raw.(type) {
	case bool:
		return value
	case string:
		parsed, errParse := strconv.ParseBool(strings.TrimSpace(value))
		if errParse == nil {
			return parsed
		}
	default:
	}
	return false
}

func (h *OpenAIResponsesAPIHandler) websocketUpstreamSupportsIncrementalInputForModel(modelName string) bool {
	auths, _ := h.responsesWebsocketAvailableAuthsForModel(modelName)
	for _, auth := range auths {
		if responsesWebsocketAuthSupportsIncrementalInput(auth) {
			return true
		}
	}
	return false
}

func (h *OpenAIResponsesAPIHandler) websocketUpstreamSupportsCompactionReplayForModel(modelName string) bool {
	auths, _ := h.responsesWebsocketAvailableAuthsForModel(modelName)
	if len(auths) == 0 {
		return false
	}
	for _, auth := range auths {
		if !responsesWebsocketAuthSupportsCompactionReplay(auth) {
			return false
		}
	}
	return true
}

func (h *OpenAIResponsesAPIHandler) responsesWebsocketAvailableAuthsForModel(modelName string) ([]*coreauth.Auth, string) {
	if h == nil || h.AuthManager == nil {
		return nil, ""
	}
	resolvedModelName := responsesWebsocketResolvedModelName(modelName)
	providerSet, modelKey := responsesWebsocketProviderSetForModel(resolvedModelName)
	if len(providerSet) == 0 {
		return nil, modelKey
	}

	registryRef := registry.GetGlobalRegistry()
	now := time.Now()
	auths := h.AuthManager.List()
	available := make([]*coreauth.Auth, 0, len(auths))
	for _, auth := range auths {
		if !responsesWebsocketAuthMatchesModel(auth, providerSet, modelKey, registryRef, now) {
			continue
		}
		available = append(available, auth)
	}
	return available, modelKey
}

func (h *OpenAIResponsesAPIHandler) responsesWebsocketUsesCodexWebsocketPassthrough(modelName string) bool {
	return h.responsesWebsocketUsesUpstreamWebsocketPassthrough(modelName)
}

func (h *OpenAIResponsesAPIHandler) responsesWebsocketUsesUpstreamWebsocketPassthrough(modelName string) bool {
	modelName = strings.TrimSpace(modelName)
	if h == nil || h.AuthManager == nil || modelName == "" {
		return false
	}
	auths, _ := h.responsesWebsocketAvailableAuthsForModel(modelName)
	if len(auths) == 0 {
		return false
	}
	provider := ""
	for _, auth := range auths {
		if auth == nil {
			return false
		}
		authProvider := strings.ToLower(strings.TrimSpace(auth.Provider))
		if authProvider != "codex" && authProvider != "xai" {
			return false
		}
		if provider == "" {
			provider = authProvider
			if _, ok := h.AuthManager.Executor(provider); !ok {
				return false
			}
		} else if authProvider != provider {
			return false
		}
		if !websocketUpstreamSupportsIncrementalInput(auth.Attributes, auth.Metadata) {
			return false
		}
	}
	return provider != ""
}

func responsesWebsocketAuthSupportsIncrementalInput(auth *coreauth.Auth) bool {
	if auth == nil {
		return false
	}
	return websocketUpstreamSupportsIncrementalInput(auth.Attributes, auth.Metadata)
}

func responsesWebsocketPinnedAuthMatchesModel(auth *coreauth.Auth, modelName string, pinnedModelKey string, homeRuntime bool) bool {
	if auth == nil {
		return false
	}
	providerSet, modelKey := responsesWebsocketProviderSetForModel(responsesWebsocketResolvedModelName(modelName))
	providerKey := strings.ToLower(strings.TrimSpace(auth.Provider))
	if _, ok := providerSet[providerKey]; !ok {
		return false
	}
	if !responsesWebsocketAuthAvailableForModel(auth, modelKey, time.Now()) {
		return false
	}

	if homeRuntime {
		return strings.EqualFold(strings.TrimSpace(pinnedModelKey), strings.TrimSpace(modelKey))
	}
	return registry.GetGlobalRegistry().ClientSupportsModel(auth.ID, modelKey)
}

func normalizeResponsesWebsocketPassthroughRequest(rawJSON []byte, modelName string) ([]byte, *interfaces.ErrorMessage) {
	if !json.Valid(rawJSON) {
		return nil, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("invalid websocket request JSON"),
		}
	}

	requestType := strings.TrimSpace(gjson.GetBytes(rawJSON, "type").String())
	switch requestType {
	case wsRequestTypeCreate, wsRequestTypeAppend:
	default:
		return nil, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("unsupported websocket request type: %s", requestType),
		}
	}

	normalized := bytes.Clone(rawJSON)
	if strings.TrimSpace(gjson.GetBytes(normalized, "model").String()) == "" {
		modelName = strings.TrimSpace(modelName)
		if modelName == "" {
			return nil, &interfaces.ErrorMessage{
				StatusCode: http.StatusBadRequest,
				Error:      fmt.Errorf("missing model in response.create request"),
			}
		}
		normalized, _ = sjson.SetBytes(normalized, "model", modelName)
	}
	normalized, _ = sjson.SetBytes(normalized, "stream", true)
	return normalized, nil
}

func responsesWebsocketResolvedModelName(modelName string) string {
	initialSuffix := thinking.ParseSuffix(modelName)
	if initialSuffix.ModelName == "auto" {
		resolvedBase := util.ResolveAutoModel(initialSuffix.ModelName)
		if initialSuffix.HasSuffix {
			return fmt.Sprintf("%s(%s)", resolvedBase, initialSuffix.RawSuffix)
		}
		return resolvedBase
	}
	return util.ResolveAutoModel(modelName)
}

func responsesWebsocketProviderSetForModel(resolvedModelName string) (map[string]struct{}, string) {
	parsed := thinking.ParseSuffix(resolvedModelName)
	baseModel := strings.TrimSpace(parsed.ModelName)
	providers := util.GetProviderName(baseModel)
	if len(providers) == 0 && baseModel != resolvedModelName {
		providers = util.GetProviderName(resolvedModelName)
	}
	providerSet := make(map[string]struct{}, len(providers))
	for _, provider := range providers {
		providerKey := strings.TrimSpace(strings.ToLower(provider))
		if providerKey == "" {
			continue
		}
		providerSet[providerKey] = struct{}{}
	}
	modelKey := baseModel
	if modelKey == "" {
		modelKey = strings.TrimSpace(resolvedModelName)
	}
	return providerSet, modelKey
}

func responsesWebsocketAuthMatchesModel(auth *coreauth.Auth, providerSet map[string]struct{}, modelKey string, registryRef *registry.ModelRegistry, now time.Time) bool {
	if auth == nil {
		return false
	}
	providerKey := strings.TrimSpace(strings.ToLower(auth.Provider))
	if _, ok := providerSet[providerKey]; !ok {
		return false
	}
	if modelKey != "" && registryRef != nil && !registryRef.ClientSupportsModel(auth.ID, modelKey) {
		return false
	}
	return responsesWebsocketAuthAvailableForModel(auth, modelKey, now)
}

func responsesWebsocketAuthSupportsCompactionReplay(auth *coreauth.Auth) bool {
	if auth == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(auth.Provider), "codex")
}

func responsesWebsocketAuthAvailableForModel(auth *coreauth.Auth, modelName string, now time.Time) bool {
	if auth == nil {
		return false
	}
	if auth.Disabled || auth.Status == coreauth.StatusDisabled {
		return false
	}
	if modelName != "" && len(auth.ModelStates) > 0 {
		state, ok := auth.ModelStates[modelName]
		if (!ok || state == nil) && modelName != "" {
			baseModel := strings.TrimSpace(thinking.ParseSuffix(modelName).ModelName)
			if baseModel != "" && baseModel != modelName {
				state, ok = auth.ModelStates[baseModel]
			}
		}
		if ok && state != nil {
			if state.Status == coreauth.StatusDisabled {
				return false
			}
			if state.Unavailable && !state.NextRetryAfter.IsZero() && state.NextRetryAfter.After(now) {
				return false
			}
			return true
		}
	}
	if auth.Unavailable && !auth.NextRetryAfter.IsZero() && auth.NextRetryAfter.After(now) {
		return false
	}
	return true
}

func shouldHandleResponsesWebsocketPrewarmLocally(rawJSON []byte, lastRequest []byte, allowIncrementalInputWithPreviousResponseID bool) bool {
	if allowIncrementalInputWithPreviousResponseID || len(lastRequest) != 0 {
		return false
	}
	if strings.TrimSpace(gjson.GetBytes(rawJSON, "type").String()) != wsRequestTypeCreate {
		return false
	}
	generateResult := gjson.GetBytes(rawJSON, "generate")
	return generateResult.Exists() && !generateResult.Bool()
}

func writeResponsesWebsocketSyntheticPrewarm(
	c *gin.Context,
	writer *responsesWebsocketWriter,
	requestJSON []byte,
	wsTimelineLog websocketTimelineAppender,
	sessionID string,
) error {
	payloads, errPayloads := syntheticResponsesWebsocketPrewarmPayloads(requestJSON)
	if errPayloads != nil {
		return errPayloads
	}
	for i := 0; i < len(payloads); i++ {
		markAPIResponseTimestamp(c)
		// log.Infof(
		// 	"responses websocket: downstream_out id=%s type=%d event=%s payload=%s",
		// 	sessionID,
		// 	websocket.TextMessage,
		// 	websocketPayloadEventType(payloads[i]),
		// 	websocketPayloadPreview(payloads[i]),
		// )
		if errWrite := writeResponsesWebsocketPayload(writer, wsTimelineLog, payloads[i], time.Now()); errWrite != nil {
			log.Warnf(
				"responses websocket: downstream_out write failed id=%s event=%s error=%v",
				sessionID,
				websocketPayloadEventType(payloads[i]),
				errWrite,
			)
			return errWrite
		}
	}
	return nil
}

func syntheticResponsesWebsocketPrewarmPayloads(requestJSON []byte) ([][]byte, error) {
	responseID := "resp_prewarm_" + uuid.NewString()
	createdAt := time.Now().Unix()
	modelName := strings.TrimSpace(gjson.GetBytes(requestJSON, "model").String())

	createdPayload := []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"","object":"response","created_at":0,"status":"in_progress","background":false,"error":null,"output":[]}}`)
	var errSet error
	createdPayload, errSet = sjson.SetBytes(createdPayload, "response.id", responseID)
	if errSet != nil {
		return nil, errSet
	}
	createdPayload, errSet = sjson.SetBytes(createdPayload, "response.created_at", createdAt)
	if errSet != nil {
		return nil, errSet
	}
	if modelName != "" {
		createdPayload, errSet = sjson.SetBytes(createdPayload, "response.model", modelName)
		if errSet != nil {
			return nil, errSet
		}
	}

	completedPayload := []byte(`{"type":"response.completed","sequence_number":1,"response":{"id":"","object":"response","created_at":0,"status":"completed","background":false,"error":null,"output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
	completedPayload, errSet = sjson.SetBytes(completedPayload, "response.id", responseID)
	if errSet != nil {
		return nil, errSet
	}
	completedPayload, errSet = sjson.SetBytes(completedPayload, "response.created_at", createdAt)
	if errSet != nil {
		return nil, errSet
	}
	if modelName != "" {
		completedPayload, errSet = sjson.SetBytes(completedPayload, "response.model", modelName)
		if errSet != nil {
			return nil, errSet
		}
	}

	return [][]byte{createdPayload, completedPayload}, nil
}

func mergeJSONArrayRaw(existingRaw, appendRaw string) (string, error) {
	existingRaw = strings.TrimSpace(existingRaw)
	appendRaw = strings.TrimSpace(appendRaw)
	if existingRaw == "" {
		existingRaw = "[]"
	}
	if appendRaw == "" {
		appendRaw = "[]"
	}

	var existing []json.RawMessage
	if err := json.Unmarshal([]byte(existingRaw), &existing); err != nil {
		return "", err
	}
	var appendItems []json.RawMessage
	if err := json.Unmarshal([]byte(appendRaw), &appendItems); err != nil {
		return "", err
	}

	merged := append(existing, appendItems...)
	out, err := json.Marshal(merged)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// inputContainsFullTranscript returns true when the input array carries compact
// replay markers. These indicate that the client already sent the full
// post-compaction transcript, so the caller should use the input as-is.
//
// Assistant messages alone are not enough to classify the payload as a replay:
// incremental websocket requests may legitimately append assistant items.
func inputContainsFullTranscript(input gjson.Result) bool {
	if !input.IsArray() {
		return false
	}
	for _, item := range input.Array() {
		if isResponsesWebsocketCompactionReplayItemType(item.Get("type").String()) {
			return true
		}
	}
	return false
}

func inputContainsCompactionTrigger(input gjson.Result) bool {
	if !input.IsArray() {
		return false
	}
	for _, item := range input.Array() {
		if strings.TrimSpace(item.Get("type").String()) == "compaction_trigger" {
			return true
		}
	}
	return false
}

func isResponsesWebsocketCompactionItemType(t string) bool {
	switch strings.TrimSpace(t) {
	case "compaction", "compaction_summary", "compaction_trigger", "context_compaction":
		return true
	default:
		return false
	}
}

func isResponsesWebsocketCompactionReplayItemType(t string) bool {
	switch strings.TrimSpace(t) {
	case "compaction", "compaction_summary", "context_compaction":
		return true
	default:
		return false
	}
}

func inputWithoutCompactionItems(input gjson.Result) string {
	if !input.IsArray() {
		return normalizeJSONArrayRaw([]byte(input.Raw))
	}
	filtered := make([]string, 0, len(input.Array()))
	for _, item := range input.Array() {
		if isResponsesWebsocketCompactionReplayItemType(item.Get("type").String()) {
			continue
		}
		filtered = append(filtered, item.Raw)
	}
	return "[" + strings.Join(filtered, ",") + "]"
}

func normalizeJSONArrayRaw(raw []byte) string {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return "[]"
	}
	result := gjson.Parse(trimmed)
	if result.Type == gjson.JSON && result.IsArray() {
		return trimmed
	}
	return "[]"
}

type responsesWebsocketForwardOptions struct {
	toolCacheTurn                     *responsesWebsocketToolCacheTurn
	suppressError                     func(*interfaces.ErrorMessage) bool
	allowTranscriptReplayBeforeOutput bool
	allowHTTPFallbackBeforeOutput     bool
}

func (h *OpenAIResponsesAPIHandler) forwardResponsesWebsocket(
	c *gin.Context,
	writer *responsesWebsocketWriter,
	cancel handlers.APIHandlerCancelFunc,
	data <-chan []byte,
	errs <-chan *interfaces.ErrorMessage,
	upstreamHeaders http.Header,
	wsTimelineLog websocketTimelineAppender,
	sessionID string,
	options ...responsesWebsocketForwardOptions,
) ([]byte, string, []string, *interfaces.ErrorMessage, bool, error) {
	var opts responsesWebsocketForwardOptions
	if len(options) > 0 {
		opts = options[0]
	}
	toolCacheTurn := opts.toolCacheTurn
	allowTranscriptReplayBeforeOutput := opts.allowTranscriptReplayBeforeOutput
	completed := false
	forwardedReplayBoundary := false
	protocolMetadataHandled := false
	completedOutput := []byte("[]")
	completedResponseID := ""
	pendingToolCallIDs := make(map[string]struct{})
	outputItemsByIndex := make(map[int64][]byte)
	var outputItemsFallback [][]byte
	var pendingProtocolPayloads [][]byte
	downstreamSessionKey := ""
	if c != nil && c.Request != nil {
		downstreamSessionKey = websocketDownstreamSessionKey(c.Request)
	}

	writePayload := func(payload []byte) error {
		markAPIResponseTimestamp(c)
		if errWrite := writeResponsesWebsocketPayload(writer, wsTimelineLog, payload, time.Now()); errWrite != nil {
			log.Warnf(
				"responses websocket: downstream_out write failed id=%s event=%s error=%v",
				sessionID,
				websocketPayloadEventType(payload),
				errWrite,
			)
			return errWrite
		}
		return nil
	}
	flushPendingProtocolPayloads := func() error {
		if len(pendingProtocolPayloads) == 0 {
			return nil
		}
		if !protocolMetadataHandled {
			protocolMetadataHandled = true
			if metadataPayload := responsesWebsocketTurnStateMetadataPayload(upstreamHeaders, pendingProtocolPayloads[0]); len(metadataPayload) > 0 {
				if errWrite := writePayload(metadataPayload); errWrite != nil {
					return errWrite
				}
			}
		}
		for _, payload := range pendingProtocolPayloads {
			if errWrite := writePayload(payload); errWrite != nil {
				return errWrite
			}
		}
		pendingProtocolPayloads = nil
		forwardedReplayBoundary = true
		return nil
	}

	handleError := func(errMsg *interfaces.ErrorMessage) ([]byte, string, []string, *interfaces.ErrorMessage, bool, error) {
		if errMsg != nil {
			if opts.allowHTTPFallbackBeforeOutput && !forwardedReplayBoundary && shouldRetryResponsesWebsocketHTTPFallback(errMsg) {
				cancel(errMsg.Error)
				return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), errMsg, true, nil
			}
			if allowTranscriptReplayBeforeOutput && !forwardedReplayBoundary && shouldRetryResponsesWebsocketTranscriptReplay(errMsg) {
				cancel(errMsg.Error)
				return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), errMsg, true, nil
			}
			if opts.suppressError != nil && opts.suppressError(errMsg) {
				cancel(errMsg.Error)
				return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), errMsg, false, nil
			}
			if responsesWebsocketErrorRequiresInternalReplay(errMsg) {
				errMsg = responsesWebsocketTerminalReplayFailure(errMsg)
			}
			if h != nil {
				h.LoggingAPIResponseError(context.WithValue(context.Background(), "gin", c), errMsg)
			}
			if errFlush := flushPendingProtocolPayloads(); errFlush != nil {
				cancel(errFlush)
				return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), errMsg, false, errFlush
			}
			if matched, errClose := writer.closeForUpstreamError(errMsg.Error); matched {
				cancel(errMsg.Error)
				if errClose != nil {
					return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), errMsg, false, errClose
				}
				return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), errMsg, false, websocket.ErrCloseSent
			}
			markAPIResponseTimestamp(c)
			errorPayload, errWrite := writeResponsesWebsocketError(writer, wsTimelineLog, errMsg)
			logResponsesWebsocketDownstreamError(sessionID, errorPayload)
			if errWrite != nil {
				// log.Warnf(
				// 	"responses websocket: downstream_out write failed id=%s event=%s error=%v",
				// 	sessionID,
				// 	websocketPayloadEventType(errorPayload),
				// 	errWrite,
				// )
				cancel(errMsg.Error)
				return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), errMsg, false, errWrite
			}
		}
		if errMsg != nil {
			cancel(errMsg.Error)
		} else {
			cancel(nil)
		}
		return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), errMsg, false, nil
	}

	for {
		if errMsg, hasErr := receivePendingResponsesWebsocketError(errs); hasErr {
			return handleError(errMsg)
		}
		select {
		case <-c.Request.Context().Done():
			cancel(c.Request.Context().Err())
			return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), nil, false, c.Request.Context().Err()
		case errMsg, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			return handleError(errMsg)
		case chunk, ok := <-data:
			if !ok {
				if !completed {
					if errMsg, hasErr := receiveResponsesWebsocketFinalError(errs); hasErr {
						return handleError(errMsg)
					}
					errMsg := &interfaces.ErrorMessage{
						StatusCode: http.StatusRequestTimeout,
						Error:      fmt.Errorf("stream closed before response.completed"),
					}
					return handleError(errMsg)
				}
				cancel(nil)
				return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), nil, false, nil
			}

			payloads := websocketJSONPayloadsFromChunk(chunk)
			for i := range payloads {
				eventType := gjson.GetBytes(payloads[i], "type").String()
				if eventType == "response.output_item.done" {
					collectResponsesWebsocketOutputItemDone(payloads[i], outputItemsByIndex, &outputItemsFallback)
				}
				if isResponsesWebsocketCompletionEvent(eventType) {
					payloads[i] = restoreResponsesWebsocketCompletionOutput(payloads[i], outputItemsByIndex, outputItemsFallback)
				}
				if toolCacheTurn != nil {
					toolCacheTurn.recordResponse(payloads[i])
				} else {
					recordResponsesWebsocketToolCallsFromPayload(downstreamSessionKey, payloads[i])
				}
				recordPendingToolCallIDsFromPayload(pendingToolCallIDs, payloads[i])
				var payloadErrMsg *interfaces.ErrorMessage
				if eventType == wsEventTypeError || eventType == wsEventTypeFailed {
					payloadErrMsg = responsesWebsocketErrorMessageFromPayload(payloads[i])
				} else if eventType == wsEventTypeIncomplete {
					payloadErrMsg = responsesWebsocketIncompleteErrorMessageFromPayload(payloads[i])
				} else if isResponsesWebsocketCompletionEvent(eventType) {
					completed = true
					completedOutput = responseCompletedOutputFromPayload(payloads[i], outputItemsByIndex, outputItemsFallback)
					completedResponseID = responseCompletedIDFromPayload(payloads[i])
				}
				if payloadErrMsg != nil && h != nil {
					h.LoggingAPIResponseError(context.WithValue(context.Background(), "gin", c), payloadErrMsg)
				}
				if payloadErrMsg != nil && opts.suppressError != nil && opts.suppressError(payloadErrMsg) {
					cancel(payloadErrMsg.Error)
					return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), payloadErrMsg, false, nil
				}
				if payloadErrMsg != nil && eventType != wsEventTypeIncomplete && allowTranscriptReplayBeforeOutput && !forwardedReplayBoundary && shouldRetryResponsesWebsocketTranscriptReplay(payloadErrMsg) {
					cancel(payloadErrMsg.Error)
					return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), payloadErrMsg, true, nil
				}
				payloadPrecludesReplay := responsesWebsocketPayloadPrecludesTranscriptReplay(payloads[i])
				if allowTranscriptReplayBeforeOutput && !forwardedReplayBoundary &&
					(responsesWebsocketTurnStateOnlyMetadata(payloads[i]) || len(pendingProtocolPayloads) > 0 && !payloadPrecludesReplay) {
					pendingProtocolPayloads = append(pendingProtocolPayloads, payloads[i])
					continue
				}
				if payloadPrecludesReplay {
					if errFlush := flushPendingProtocolPayloads(); errFlush != nil {
						cancel(errFlush)
						return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), nil, false, errFlush
					}
				}
				if payloadPrecludesReplay && !protocolMetadataHandled {
					protocolMetadataHandled = true
					metadataPayload := responsesWebsocketTurnStateMetadataPayload(upstreamHeaders, payloads[i])
					if len(metadataPayload) > 0 {
						if errWrite := writePayload(metadataPayload); errWrite != nil {
							cancel(errWrite)
							return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), nil, false, errWrite
						}
						forwardedReplayBoundary = true
					}
				}
				// log.Infof(
				// 	"responses websocket: downstream_out id=%s type=%d event=%s payload=%s",
				// 	sessionID,
				// 	websocket.TextMessage,
				// 	websocketPayloadEventType(payloads[i]),
				// 	websocketPayloadPreview(payloads[i]),
				// )
				if errWrite := writePayload(payloads[i]); errWrite != nil {
					cancel(errWrite)
					return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), nil, false, errWrite
				}
				if payloadErrMsg != nil {
					cancel(payloadErrMsg.Error)
					return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), payloadErrMsg, false, nil
				}
				if payloadPrecludesReplay {
					forwardedReplayBoundary = true
				}
				if isResponsesWebsocketCompletionEvent(eventType) {
					cancel(nil)
					return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), nil, false, nil
				}
			}
		}
	}
}

func receivePendingResponsesWebsocketError(errs <-chan *interfaces.ErrorMessage) (*interfaces.ErrorMessage, bool) {
	if errs == nil {
		return nil, false
	}
	select {
	case errMsg, ok := <-errs:
		return errMsg, ok && errMsg != nil
	default:
		return nil, false
	}
}

func receiveResponsesWebsocketFinalError(errs <-chan *interfaces.ErrorMessage) (*interfaces.ErrorMessage, bool) {
	return receivePendingResponsesWebsocketError(errs)
}

func responsesWebsocketTurnStateMetadataPayload(headers http.Header, nextPayload []byte) []byte {
	turnState := strings.TrimSpace(headers.Get(wsTurnStateHeader))
	if turnState == "" {
		return nil
	}
	if strings.TrimSpace(gjson.GetBytes(nextPayload, "type").String()) == "response.metadata" &&
		strings.TrimSpace(gjson.GetBytes(nextPayload, "headers").Get(wsTurnStateHeader).String()) == turnState {
		return nil
	}
	payload, _ := json.Marshal(map[string]any{
		"type": "response.metadata",
		"headers": map[string]string{
			wsTurnStateHeader: turnState,
		},
	})
	return payload
}

func responsesWebsocketTurnStateOnlyMetadata(payload []byte) bool {
	root := gjson.ParseBytes(payload)
	if !root.IsObject() {
		return false
	}
	fields := root.Map()
	if len(fields) != 2 || strings.TrimSpace(fields["type"].String()) != "response.metadata" {
		return false
	}
	headers := fields["headers"]
	if !headers.IsObject() {
		return false
	}
	headerFields := headers.Map()
	if len(headerFields) != 1 {
		return false
	}
	for key, value := range headerFields {
		return strings.EqualFold(key, wsTurnStateHeader) && strings.TrimSpace(value.String()) != ""
	}
	return false
}

func responsesWebsocketPayloadPrecludesTranscriptReplay(payload []byte) bool {
	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	switch eventType {
	case "codex.rate_limits":
		return false
	default:
		return true
	}
}

func responsesWebsocketErrorStatus(errMsg *interfaces.ErrorMessage) int {
	if errMsg == nil {
		return 0
	}
	status := errMsg.StatusCode
	if status <= 0 && errMsg.Error != nil {
		if se, ok := errMsg.Error.(interface{ StatusCode() int }); ok && se != nil {
			status = se.StatusCode()
		}
	}
	return status
}

func shouldReplayResponsesWebsocketPinnedAuthFailure(errMsg *interfaces.ErrorMessage) bool {
	switch responsesWebsocketErrorStatus(errMsg) {
	case http.StatusUnauthorized, http.StatusTooManyRequests:
		return true
	default:
		return false
	}
}

func shouldReleaseResponsesWebsocketPinnedAuth(errMsg *interfaces.ErrorMessage) bool {
	if errMsg == nil {
		return false
	}
	var terminalReplay responsesWebsocketTerminalReplayError
	if errMsg.Error != nil && errors.As(errMsg.Error, &terminalReplay) {
		return false
	}
	switch responsesWebsocketErrorStatus(errMsg) {
	case http.StatusUnauthorized,
		http.StatusPaymentRequired,
		http.StatusForbidden,
		http.StatusTooManyRequests,
		http.StatusRequestTimeout,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
	}
	if errMsg.Error != nil {
		msg := strings.ToLower(errMsg.Error.Error())
		switch {
		case strings.Contains(msg, "stream closed before response.completed"),
			strings.Contains(msg, "previous_response_not_found"),
			strings.Contains(msg, "ws_failed"),
			strings.Contains(msg, "upstream stream closed before first payload"),
			strings.Contains(msg, "empty_stream"):
			return true
		}
	}
	return false
}

func shouldRetryResponsesWebsocketTranscriptReplay(errMsg *interfaces.ErrorMessage) bool {
	if errMsg == nil || errMsg.Error == nil {
		return false
	}
	if responsesWebsocketErrorRequiresInternalReplay(errMsg) {
		return true
	}
	if responsesWebsocketErrorIndicatesConnectionLimitReached(errMsg.Error.Error()) {
		return true
	}
	status := errMsg.StatusCode
	if status <= 0 {
		if se, ok := errMsg.Error.(interface{ StatusCode() int }); ok && se != nil {
			status = se.StatusCode()
		}
	}
	if status > 0 && status != http.StatusBadRequest {
		return false
	}
	return responsesWebsocketErrorIndicatesPreviousResponseNotFound(errMsg.Error.Error())
}

func shouldRetryResponsesWebsocketHTTPFallback(errMsg *interfaces.ErrorMessage) bool {
	return responsesWebsocketErrorStatus(errMsg) == http.StatusUpgradeRequired &&
		!responsesWebsocketErrorRequiresInternalReplay(errMsg)
}

func responsesWebsocketErrorRequiresInternalReplay(errMsg *interfaces.ErrorMessage) bool {
	if errMsg == nil || errMsg.Error == nil {
		return false
	}
	var terminalReplay responsesWebsocketTerminalReplayError
	if errors.As(errMsg.Error, &terminalReplay) {
		return false
	}
	if cliproxyexecutor.IsUpstreamWebsocketReplayRequired(errMsg.Error) {
		return true
	}
	var replayRequired interface {
		CodexWebsocketReplayRequired() bool
	}
	if errors.As(errMsg.Error, &replayRequired) && replayRequired != nil && replayRequired.CodexWebsocketReplayRequired() {
		return true
	}
	return false
}

type responsesWebsocketTerminalReplayError struct {
	cause error
}

func (e responsesWebsocketTerminalReplayError) Error() string {
	return "upstream websocket reset before response completion"
}

func (e responsesWebsocketTerminalReplayError) Unwrap() error {
	return e.cause
}

func responsesWebsocketTerminalReplayFailure(errMsg *interfaces.ErrorMessage) *interfaces.ErrorMessage {
	var cause error
	var addon http.Header
	if errMsg != nil {
		cause = errMsg.Error
		addon = errMsg.Addon
	}
	return &interfaces.ErrorMessage{
		StatusCode: http.StatusBadGateway,
		Error:      responsesWebsocketTerminalReplayError{cause: cause},
		Addon:      addon,
	}
}

func responsesWebsocketErrorIndicatesConnectionLimitReached(rawError string) bool {
	rawError = strings.TrimSpace(rawError)
	if rawError == "" || !json.Valid([]byte(rawError)) {
		return false
	}
	for _, path := range []string{"error.code", "error.type", "body.error.code", "body.error.type", "response.error.code", "response.error.type", "code", "error"} {
		if strings.EqualFold(strings.TrimSpace(gjson.Get(rawError, path).String()), wsConnectionLimitReachedCode) {
			return true
		}
	}
	return false
}

func responsesWebsocketErrorIndicatesPreviousResponseNotFound(rawError string) bool {
	rawError = strings.TrimSpace(rawError)
	if rawError == "" {
		return false
	}
	if json.Valid([]byte(rawError)) {
		hasCode := false
		for _, path := range []string{"error.code", "body.error.code", "response.error.code", "code"} {
			code := strings.ToLower(strings.TrimSpace(gjson.Get(rawError, path).String()))
			if code == "" {
				continue
			}
			hasCode = true
			if code == "previous_response_not_found" {
				return true
			}
		}
		if hasCode {
			return false
		}
		for _, path := range []string{"error.message", "body.error.message", "response.error.message", "message"} {
			if responsesWebsocketErrorMessageIndicatesPreviousResponseNotFound(gjson.Get(rawError, path).String()) {
				return true
			}
		}
		return false
	}
	return responsesWebsocketErrorTextIndicatesPreviousResponseNotFound(rawError)
}

func responsesWebsocketErrorTextIndicatesPreviousResponseNotFound(text string) bool {
	lower := strings.ToLower(strings.TrimSpace(text))
	return strings.Contains(lower, "previous_response_not_found") ||
		(strings.Contains(lower, "previous_response") || strings.Contains(lower, "previous response")) &&
			(strings.Contains(lower, "not found") || strings.Contains(lower, "no response found"))
}

func responsesWebsocketErrorMessageIndicatesPreviousResponseNotFound(text string) bool {
	lower := strings.ToLower(strings.TrimSpace(text))
	mentionsPreviousResponse := strings.Contains(lower, "previous_response") || strings.Contains(lower, "previous response")
	mentionsMissingResponse := strings.Contains(lower, "not found") || strings.Contains(lower, "no response found")
	return mentionsPreviousResponse && mentionsMissingResponse
}

func responseCompletedOutputFromPayload(payload []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback [][]byte) []byte {
	output := gjson.GetBytes(payload, "response.output")
	if output.Exists() && output.IsArray() && len(output.Array()) > 0 {
		return bytes.Clone([]byte(output.Raw))
	}
	if collected := responsesWebsocketCollectedOutputItems(outputItemsByIndex, outputItemsFallback); len(collected) > 0 {
		return collected
	}
	return []byte("[]")
}

func restoreResponsesWebsocketCompletionOutput(payload []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback [][]byte) []byte {
	output := gjson.GetBytes(payload, "response.output")
	if output.Exists() && output.IsArray() && len(output.Array()) > 0 {
		reconciledOutput, changed := reconcileResponsesWebsocketCompletionToolCalls(output, outputItemsByIndex, outputItemsFallback)
		if !changed {
			return payload
		}
		restored, errSet := sjson.SetRawBytes(payload, "response.output", reconciledOutput)
		if errSet != nil {
			return payload
		}
		return restored
	}
	if len(outputItemsByIndex) == 0 && len(outputItemsFallback) == 0 {
		return payload
	}
	restoredOutput := responseCompletedOutputFromPayload(payload, outputItemsByIndex, outputItemsFallback)
	restored, errSet := sjson.SetRawBytes(payload, "response.output", restoredOutput)
	if errSet != nil {
		return payload
	}
	return restored
}

func reconcileResponsesWebsocketCompletionToolCalls(output gjson.Result, outputItemsByIndex map[int64][]byte, outputItemsFallback [][]byte) ([]byte, bool) {
	collectedToolCalls := make(map[string]json.RawMessage)
	recordCollectedToolCall := func(raw []byte) {
		item := gjson.ParseBytes(raw)
		if !isCompleteResponsesWebsocketToolCall(item) {
			return
		}
		callID := strings.TrimSpace(item.Get("call_id").String())
		collectedToolCalls[callID] = append(json.RawMessage(nil), raw...)
	}

	indexes := make([]int64, 0, len(outputItemsByIndex))
	for index := range outputItemsByIndex {
		indexes = append(indexes, index)
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] < indexes[j]
	})
	for _, index := range indexes {
		recordCollectedToolCall(outputItemsByIndex[index])
	}
	for _, item := range outputItemsFallback {
		recordCollectedToolCall(item)
	}
	if len(collectedToolCalls) == 0 {
		return nil, false
	}

	items := output.Array()
	reconciled := make([]json.RawMessage, 0, len(items))
	changed := false
	for _, item := range items {
		raw := json.RawMessage(item.Raw)
		if isResponsesToolCallType(item.Get("type").String()) {
			callID := strings.TrimSpace(item.Get("call_id").String())
			if collected, ok := collectedToolCalls[callID]; ok && !bytes.Equal(raw, collected) {
				raw = collected
				changed = true
			}
		}
		reconciled = append(reconciled, raw)
	}
	if !changed {
		return nil, false
	}

	marshaledOutput, errMarshal := json.Marshal(reconciled)
	if errMarshal != nil {
		return nil, false
	}
	return marshaledOutput, true
}

func isCompleteResponsesWebsocketToolCall(item gjson.Result) bool {
	if !item.Exists() || !item.IsObject() {
		return false
	}
	callID := item.Get("call_id")
	name := item.Get("name")
	if callID.Type != gjson.String || strings.TrimSpace(callID.String()) == "" || name.Type != gjson.String || strings.TrimSpace(name.String()) == "" {
		return false
	}

	switch strings.TrimSpace(item.Get("type").String()) {
	case "function_call":
		arguments := item.Get("arguments")
		return arguments.Exists() && arguments.Type == gjson.String
	case "custom_tool_call":
		input := item.Get("input")
		return input.Exists() && input.Type == gjson.String
	default:
		return false
	}
}

func responseCompletedIDFromPayload(payload []byte) string {
	return strings.TrimSpace(gjson.GetBytes(payload, "response.id").String())
}

func recordPendingToolCallIDsFromPayload(pending map[string]struct{}, payload []byte) {
	if pending == nil || len(payload) == 0 {
		return
	}
	updatePendingToolCallIDsFromItem(pending, gjson.GetBytes(payload, "item"))
	output := gjson.GetBytes(payload, "response.output")
	if output.IsArray() {
		for _, item := range output.Array() {
			updatePendingToolCallIDsFromItem(pending, item)
		}
	}
}

func updatePendingToolCallIDsFromItem(pending map[string]struct{}, item gjson.Result) {
	if pending == nil || !item.Exists() {
		return
	}
	switch strings.TrimSpace(item.Get("type").String()) {
	case "function_call", "custom_tool_call":
		if !isCompleteResponsesWebsocketToolCall(item) {
			return
		}
		callID := strings.TrimSpace(item.Get("call_id").String())
		pending[callID] = struct{}{}
	case "function_call_output", "custom_tool_call_output":
		callID := strings.TrimSpace(item.Get("call_id").String())
		if callID != "" {
			delete(pending, callID)
		}
	}
}

func sortedStringSet(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

func collectResponsesWebsocketOutputItemDone(payload []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback *[][]byte) {
	item := gjson.GetBytes(payload, "item")
	if !item.Exists() || item.Type != gjson.JSON {
		return
	}
	raw := bytes.Clone([]byte(item.Raw))
	outputIndex := gjson.GetBytes(payload, "output_index")
	if outputIndex.Exists() {
		outputItemsByIndex[outputIndex.Int()] = raw
		return
	}
	*outputItemsFallback = append(*outputItemsFallback, raw)
}

func responsesWebsocketCollectedOutputItems(outputItemsByIndex map[int64][]byte, outputItemsFallback [][]byte) []byte {
	if len(outputItemsByIndex) == 0 && len(outputItemsFallback) == 0 {
		return nil
	}
	items := make([]string, 0, len(outputItemsByIndex)+len(outputItemsFallback))
	appendItem := func(raw []byte) {
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 {
			return
		}
		item := gjson.ParseBytes(trimmed)
		if isResponsesToolCallType(item.Get("type").String()) && !isCompleteResponsesWebsocketToolCall(item) {
			return
		}
		items = append(items, string(trimmed))
	}
	indexes := make([]int64, 0, len(outputItemsByIndex))
	for idx := range outputItemsByIndex {
		indexes = append(indexes, idx)
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] < indexes[j]
	})
	for _, idx := range indexes {
		appendItem(outputItemsByIndex[idx])
	}
	for _, item := range outputItemsFallback {
		appendItem(item)
	}
	if len(items) == 0 {
		return nil
	}
	return []byte("[" + strings.Join(items, ",") + "]")
}

func websocketJSONPayloadsFromChunk(chunk []byte) [][]byte {
	payloads := make([][]byte, 0, 2)
	lines := bytes.Split(chunk, []byte("\n"))
	for i := range lines {
		line := bytes.TrimSpace(lines[i])
		if len(line) == 0 || bytes.HasPrefix(line, []byte("event:")) {
			continue
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			line = bytes.TrimSpace(line[len("data:"):])
		}
		if len(line) == 0 || bytes.Equal(line, []byte(wsDoneMarker)) {
			continue
		}
		if json.Valid(line) {
			payloads = append(payloads, bytes.Clone(line))
		}
	}

	if len(payloads) > 0 {
		return payloads
	}

	trimmed := bytes.TrimSpace(chunk)
	if bytes.HasPrefix(trimmed, []byte("data:")) {
		trimmed = bytes.TrimSpace(trimmed[len("data:"):])
	}
	if len(trimmed) > 0 && !bytes.Equal(trimmed, []byte(wsDoneMarker)) && json.Valid(trimmed) {
		payloads = append(payloads, bytes.Clone(trimmed))
	}
	return payloads
}

func writeResponsesWebsocketError(writer *responsesWebsocketWriter, wsTimelineLog websocketTimelineAppender, errMsg *interfaces.ErrorMessage) ([]byte, error) {
	status := http.StatusInternalServerError
	errText := http.StatusText(status)
	if errMsg != nil {
		if errMsg.StatusCode > 0 {
			status = errMsg.StatusCode
			errText = http.StatusText(status)
		}
		if errMsg.Error != nil && strings.TrimSpace(errMsg.Error.Error()) != "" {
			errText = errMsg.Error.Error()
		}
	}

	body := handlers.BuildErrorResponseBody(status, errText)
	payload := []byte(`{}`)
	var errSet error
	payload, errSet = sjson.SetBytes(payload, "type", wsEventTypeError)
	if errSet != nil {
		return nil, errSet
	}
	payload, errSet = sjson.SetBytes(payload, "status", status)
	if errSet != nil {
		return nil, errSet
	}

	if errMsg != nil && errMsg.Addon != nil {
		headers := []byte(`{}`)
		hasHeaders := false
		for key, values := range errMsg.Addon {
			if len(values) == 0 {
				continue
			}
			headerPath := strings.ReplaceAll(strings.ReplaceAll(key, `\\`, `\\\\`), ".", `\\.`)
			headers, errSet = sjson.SetBytes(headers, headerPath, values[0])
			if errSet != nil {
				return nil, errSet
			}
			hasHeaders = true
		}
		if hasHeaders {
			payload, errSet = sjson.SetRawBytes(payload, "headers", headers)
			if errSet != nil {
				return nil, errSet
			}
		}
	}

	if len(body) > 0 && json.Valid(body) {
		errorNode := gjson.GetBytes(body, "error")
		if errorNode.Exists() {
			payload, errSet = sjson.SetRawBytes(payload, "error", []byte(errorNode.Raw))
		} else {
			payload, errSet = sjson.SetRawBytes(payload, "error", body)
		}
		if errSet != nil {
			return nil, errSet
		}
	}

	if !gjson.GetBytes(payload, "error").Exists() {
		payload, errSet = sjson.SetBytes(payload, "error.type", "server_error")
		if errSet != nil {
			return nil, errSet
		}
		payload, errSet = sjson.SetBytes(payload, "error.message", errText)
		if errSet != nil {
			return nil, errSet
		}
	}

	return payload, writeResponsesWebsocketPayload(writer, wsTimelineLog, payload, time.Now())
}

func appendWebsocketEvent(builder *strings.Builder, eventType string, payload []byte) {
	if builder == nil {
		return
	}
	trimmedPayload := bytes.TrimSpace(payload)
	if len(trimmedPayload) == 0 {
		return
	}
	if builder.Len() > 0 {
		builder.WriteString("\n")
	}
	builder.WriteString("websocket.")
	builder.WriteString(eventType)
	builder.WriteString("\n")
	builder.Write(trimmedPayload)
	builder.WriteString("\n")
}

func websocketPayloadEventType(payload []byte) string {
	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	if eventType == "" {
		return "-"
	}
	return eventType
}

func websocketPayloadPreview(payload []byte) string {
	trimmedPayload := bytes.TrimSpace(payload)
	if len(trimmedPayload) == 0 {
		return "<empty>"
	}
	previewText := strings.ReplaceAll(string(trimmedPayload), "\n", "\\n")
	previewText = strings.ReplaceAll(previewText, "\r", "\\r")
	return previewText
}

func logResponsesWebsocketDownstreamError(sessionID string, payload []byte) {
	log.WithFields(log.Fields{
		"id":     sessionID,
		"type":   websocket.TextMessage,
		"event":  websocketPayloadEventType(payload),
		"status": int(gjson.GetBytes(payload, "status").Int()),
	}).Info("responses websocket: downstream_out")
}

func isResponsesWebsocketCompletionEvent(eventType string) bool {
	return eventType == wsEventTypeCompleted || eventType == wsEventTypeDone
}

func responsesWebsocketErrorMessageFromPayload(payload []byte) *interfaces.ErrorMessage {
	status, hasExplicitStatus := responsesWebsocketExplicitErrorStatus(payload)

	errText := strings.TrimSpace(gjson.GetBytes(payload, "error.message").String())
	if errText == "" {
		errText = strings.TrimSpace(gjson.GetBytes(payload, "response.error.message").String())
	}
	if errText == "" {
		errText = strings.TrimSpace(gjson.GetBytes(payload, "body.error.message").String())
	}
	if errText == "" {
		errText = strings.TrimSpace(gjson.GetBytes(payload, "message").String())
	}
	if errText == "" {
		errText = strings.TrimSpace(gjson.GetBytes(payload, "error.code").String())
	}
	if errText == "" {
		errText = strings.TrimSpace(gjson.GetBytes(payload, "response.error.code").String())
	}
	if errText == "" {
		errText = strings.TrimSpace(gjson.GetBytes(payload, "body.error.code").String())
	}
	errPayload := responsesWebsocketStructuredErrorPayload(payload)
	if !hasExplicitStatus {
		status = responsesWebsocketInferredErrorStatus(payload, errText, errPayload)
	}
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	if errText == "" {
		errText = strings.TrimSpace(string(payload))
	}
	if errText == "" {
		errText = http.StatusText(status)
	}
	if errPayload == "" {
		errPayload = errText
	}
	return &interfaces.ErrorMessage{StatusCode: status, Error: fmt.Errorf("%s", errPayload)}
}

func responsesWebsocketIncompleteErrorMessageFromPayload(payload []byte) *interfaces.ErrorMessage {
	reason := strings.TrimSpace(gjson.GetBytes(payload, "response.incomplete_details.reason").String())
	if reason == "" {
		reason = "unknown"
	}
	return &interfaces.ErrorMessage{
		StatusCode: http.StatusBadRequest,
		Error:      fmt.Errorf("Incomplete response returned, reason: %s", reason),
	}
}

func responsesWebsocketExplicitErrorStatus(payload []byte) (int, bool) {
	for _, path := range []string{
		"status",
		"status_code",
		"response.status_code",
		"response.error.status",
		"response.error.status_code",
		"error.status",
		"error.status_code",
		"body.error.status",
		"body.error.status_code",
	} {
		status := int(gjson.GetBytes(payload, path).Int())
		if status > 0 {
			return status, true
		}
	}
	return 0, false
}

func responsesWebsocketInferredErrorStatus(payload []byte, errText string, errPayload string) int {
	if responsesWebsocketErrorTextIndicatesPreviousResponseNotFound(errText) ||
		responsesWebsocketErrorIndicatesPreviousResponseNotFound(errPayload) {
		return http.StatusBadRequest
	}

	errType := responsesWebsocketLowerPayloadString(payload, "error.type", "response.error.type", "body.error.type")
	errCode := responsesWebsocketLowerPayloadString(payload, "error.code", "response.error.code", "body.error.code", "code")
	switch {
	case errType == "usage_limit_reached" ||
		errType == "insufficient_quota" ||
		errType == "rate_limit_error" ||
		errCode == "insufficient_quota" ||
		errCode == "rate_limit_exceeded" ||
		responsesWebsocketErrorTextIndicatesModelCapacity(errText):
		return http.StatusTooManyRequests
	case errType == "authentication_error":
		return http.StatusUnauthorized
	case errType == "permission_error":
		return http.StatusForbidden
	case errType == "invalid_request_error" ||
		errCode == "invalid_request_error" ||
		errCode == "previous_response_not_found" ||
		errCode == "context_length_exceeded" ||
		errCode == "context_too_large" ||
		errCode == "invalid_prompt" ||
		errCode == "bio_policy" ||
		errCode == "cyber_policy":
		return http.StatusBadRequest
	default:
		return 0
	}
}

func responsesWebsocketLowerPayloadString(payload []byte, paths ...string) string {
	for _, path := range paths {
		value := strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, path).String()))
		if value != "" {
			return value
		}
	}
	return ""
}

func responsesWebsocketErrorTextIndicatesModelCapacity(text string) bool {
	lower := strings.ToLower(strings.TrimSpace(text))
	return strings.Contains(lower, "selected model is at capacity") ||
		strings.Contains(lower, "model is at capacity. please try a different model")
}

func responsesWebsocketStructuredErrorPayload(payload []byte) string {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 || !json.Valid(payload) {
		return ""
	}
	for _, path := range []string{"error", "body.error", "response.error", "code"} {
		if gjson.GetBytes(payload, path).Exists() {
			return string(payload)
		}
	}
	return ""
}

func setWebsocketTimelineBody(c *gin.Context, body string) {
	setWebsocketBody(c, wsTimelineBodyKey, body)
}

func setWebsocketBody(c *gin.Context, key string, body string) {
	if c == nil {
		return
	}
	trimmedBody := strings.TrimSpace(body)
	if trimmedBody == "" {
		return
	}
	c.Set(key, []byte(trimmedBody))
}

func writeResponsesWebsocketPayload(writer *responsesWebsocketWriter, wsTimelineLog websocketTimelineAppender, payload []byte, timestamp time.Time) error {
	if wsTimelineLog != nil {
		wsTimelineLog.Append("response", payload, timestamp)
	}
	if writer == nil || writer.conn == nil {
		return fmt.Errorf("responses websocket: writer is nil")
	}
	writer.writeMu.Lock()
	defer writer.writeMu.Unlock()
	if writer.closing.Load() {
		return websocket.ErrCloseSent
	}
	return writer.conn.WriteMessage(websocket.TextMessage, payload)
}

func startResponsesWebsocketHeartbeat(conn *websocket.Conn, done <-chan struct{}, sessionID string) {
	startResponsesWebsocketHeartbeatWithInterval(conn, done, sessionID, wsHeartbeatInterval)
}

func startResponsesWebsocketHeartbeatWithInterval(conn *websocket.Conn, done <-chan struct{}, sessionID string, interval time.Duration) {
	if conn == nil || interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if errWrite := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Time{}); errWrite != nil {
					log.Debugf("responses websocket: heartbeat ping failed id=%s error=%v", strings.TrimSpace(sessionID), errWrite)
					_ = conn.Close()
					return
				}
			}
		}
	}()
}

func appendWebsocketTimelineDisconnect(timeline websocketTimelineAppender, err error, timestamp time.Time) {
	if err == nil {
		return
	}
	if timeline != nil {
		timeline.Append("disconnect", []byte(err.Error()), timestamp)
	}
}

func appendWebsocketTimelineEvent(builder *strings.Builder, eventType string, payload []byte, timestamp time.Time) {
	if builder == nil {
		return
	}
	writeWebsocketTimelineBuilder(builder, formatWebsocketTimelineEvent(eventType, payload, timestamp))
}

func formatWebsocketTimelineEvent(eventType string, payload []byte, timestamp time.Time) []byte {
	trimmedPayload := bytes.TrimSpace(payload)
	if len(trimmedPayload) == 0 {
		return nil
	}
	var builder strings.Builder
	builder.WriteString("Timestamp: ")
	builder.WriteString(timestamp.Format(time.RFC3339Nano))
	builder.WriteString("\n")
	builder.WriteString("Event: websocket.")
	builder.WriteString(eventType)
	builder.WriteString("\n")
	builder.Write(trimmedPayload)
	builder.WriteString("\n")
	return []byte(builder.String())
}

func markAPIResponseTimestamp(c *gin.Context) {
	if c == nil {
		return
	}
	if _, exists := c.Get("API_RESPONSE_TIMESTAMP"); exists {
		return
	}
	c.Set("API_RESPONSE_TIMESTAMP", time.Now())
}
