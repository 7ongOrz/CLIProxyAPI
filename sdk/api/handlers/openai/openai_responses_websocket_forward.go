package openai

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/MoeForge/moego/json"
	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/clienterror"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
)

type responsesWebsocketForwardOptions struct {
	duplexStream                      func() bool
	preserveCompletionOutput          func() bool
	toolCacheTurn                     *responsesWebsocketToolCacheTurn
	suppressError                     func(*interfaces.ErrorMessage) bool
	allowTranscriptReplayBeforeOutput bool
	allowHTTPFallbackBeforeOutput     bool
	completedResponse                 func(string)
	localInterrupt                    *responsesLocalInterrupt
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
) ([]byte, *interfaces.ErrorMessage, bool, error) {
	var opts responsesWebsocketForwardOptions
	if len(options) > 0 {
		opts = options[0]
	}
	toolCacheTurn := opts.toolCacheTurn
	allowTranscriptReplayBeforeOutput := opts.allowTranscriptReplayBeforeOutput
	codexClient := isCodexResponsesClientRequest(c)
	completed := false
	responseStarted := false
	downstreamOutputSent := false
	protocolMetadataHandled := false
	completedOutput := []byte("[]")
	outputItemsByIndex := make(map[int64][]byte)
	var outputItemsFallback [][]byte
	var pendingProtocolPayloads [][]byte
	downstreamSessionKey := ""
	if c != nil && c.Request != nil {
		downstreamSessionKey = websocketDownstreamSessionKey(c.Request)
	}

	if opts.localInterrupt != nil {
		defer opts.localInterrupt.end()
	}
	localInterrupts := opts.localInterrupt.interruptsChan()

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
	flushMetadataBeforePayload := func(nextPayload []byte) error {
		if codexClient && !protocolMetadataHandled && (len(pendingProtocolPayloads) > 0 || len(nextPayload) > 0) {
			protocolMetadataHandled = true
			if metadataPayload := responsesWebsocketTurnStateMetadataPayload(upstreamHeaders, pendingProtocolPayloads, nextPayload); len(metadataPayload) > 0 {
				if errWrite := writePayload(metadataPayload); errWrite != nil {
					return errWrite
				}
				downstreamOutputSent = true
			}
		}
		for _, payload := range pendingProtocolPayloads {
			if errWrite := writePayload(payload); errWrite != nil {
				return errWrite
			}
		}
		if len(pendingProtocolPayloads) > 0 {
			pendingProtocolPayloads = nil
			downstreamOutputSent = true
		}
		return nil
	}

	handleError := func(errMsg *interfaces.ErrorMessage, terminalPayload []byte) ([]byte, *interfaces.ErrorMessage, bool, error) {
		if errMsg != nil {
			if opts.allowHTTPFallbackBeforeOutput && !downstreamOutputSent && shouldRetryResponsesWebsocketHTTPFallback(errMsg) {
				cancel(errMsg.Error)
				return completedOutput, errMsg, true, nil
			}
			if allowTranscriptReplayBeforeOutput && !downstreamOutputSent && shouldRetryResponsesWebsocketTranscriptReplay(errMsg) {
				cancel(errMsg.Error)
				return completedOutput, errMsg, true, nil
			}
			if !downstreamOutputSent && opts.suppressError != nil && opts.suppressError(errMsg) {
				cancel(errMsg.Error)
				return completedOutput, errMsg, false, nil
			}
			if responsesWebsocketErrorRequiresInternalReplay(errMsg) {
				errMsg = responsesWebsocketTerminalReplayFailure(errMsg)
			}
			if h != nil {
				h.LoggingAPIResponseError(context.WithValue(context.Background(), "gin", c), errMsg)
			}
			hadDownstreamOutput := downstreamOutputSent
			if cliproxyexecutor.IsUpstreamWebsocketTurnError(errMsg.Error) {
				if errFlush := flushMetadataBeforePayload(terminalPayload); errFlush != nil {
					cancel(errFlush)
					return completedOutput, errMsg, false, errFlush
				}
				markAPIResponseTimestamp(c)
				_, errWrite := writeResponsesWebsocketError(writer, wsTimelineLog, errMsg)
				cancel(errMsg.Error)
				return completedOutput, errMsg, false, errWrite
			}
			if matched, errClose := writer.closeForUpstreamError(errMsg.Error); matched {
				cancel(errMsg.Error)
				if errClose != nil {
					return completedOutput, errMsg, false, errClose
				}
				return completedOutput, errMsg, false, websocket.ErrCloseSent
			}
			exposePayload := shouldExposeResponsesUpstreamError(errMsg) || (hadDownstreamOutput && len(terminalPayload) > 0)
			if exposePayload {
				if errFlush := flushMetadataBeforePayload(terminalPayload); errFlush != nil {
					cancel(errFlush)
					return completedOutput, errMsg, false, errFlush
				}
			}
			markAPIResponseTimestamp(c)
			errorPayload, wrote, errTerminate := writeResponsesWebsocketTerminalError(writer, wsTimelineLog, errMsg, terminalPayload, hadDownstreamOutput)
			if wrote {
				logResponsesWebsocketDownstreamError(sessionID, errorPayload)
			}
			cancel(errMsg.Error)
			return completedOutput, errMsg, false, errTerminate
		}
		cancel(nil)
		return completedOutput, errMsg, false, nil
	}

	for {
		select {
		case responseID := <-localInterrupts:
			return completeResponsesWebsocketLocalInterrupt(writer, wsTimelineLog, cancel, responseID, outputItemsByIndex, outputItemsFallback)
		default:
		}
		select {
		case responseID := <-localInterrupts:
			return completeResponsesWebsocketLocalInterrupt(writer, wsTimelineLog, cancel, responseID, outputItemsByIndex, outputItemsFallback)
		case <-c.Request.Context().Done():
			cause := context.Cause(c.Request.Context())
			cancel(cause)
			return completedOutput, nil, false, cause
		case errMsg, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			return handleError(errMsg, nil)
		case chunk, ok := <-data:
			if !ok {
				isDuplex := opts.duplexStream != nil && opts.duplexStream()
				if isDuplex || !completed {
					// The stream producers send a terminal error before they close errs and then
					// data, so it is already buffered here. The select picks between the closed
					// data channel and that error at random; take the error. Otherwise a pinned-auth
					// failure that needs a replay close (1012) ends as a plain disconnect (1006),
					// and a duplex stream loses the error its executor reported.
					if pendingErrMsg, hasPending := handlers.PendingStreamError(errs); hasPending {
						return handleError(pendingErrMsg, nil)
					}
				}
				if isDuplex {
					// A duplex stream ends with its socket, not an individual response.
					// The data channel may close before select observes its final error.
					_, errClose := writer.closeWithoutError()
					cancel(nil)
					if errClose != nil {
						return completedOutput, nil, false, errClose
					}
					return completedOutput, nil, false, websocket.ErrCloseSent
				}
				if !completed {
					errMsg := &interfaces.ErrorMessage{
						StatusCode: http.StatusRequestTimeout,
						Error:      fmt.Errorf("stream closed before response.completed"),
					}
					return handleError(errMsg, nil)
				}
				cancel(nil)
				return completedOutput, nil, false, nil
			}

			payloads := websocketJSONPayloadsFromChunk(chunk)
			for i := range payloads {
				eventType := helps.CodexResponseEventType(payloads[i])
				if eventType == wsEventTypeError && strings.TrimSpace(ojson.GetBytesNoCopy(payloads[i], "type").String()) == "" {
					doc := ojson.NewDoc(payloads[i])
					_ = doc.Set("type", eventType)
					payloads[i] = doc.Bytes()
				}
				if eventType == "response.subscription_usage" ||
					(eventType == "response.metadata" && !codexClient) {
					continue
				}
				isDuplex := opts.duplexStream != nil && opts.duplexStream()
				if eventType == "response.created" {
					opts.localInterrupt.begin(strings.Clone(ojson.GetBytesNoCopy(payloads[i], "response.id").String()))
					responseStarted = true
					completed = false
					outputItemsByIndex = make(map[int64][]byte)
					outputItemsFallback = nil
				}
				interrupted := eventType == wsEventTypeIncomplete && ojson.GetBytesNoCopy(payloads[i], "response.incomplete_details.reason").String() == "interrupted"
				if eventType == "response.output_item.done" {
					collectResponsesWebsocketOutputItemDone(payloads[i], outputItemsByIndex, &outputItemsFallback)
				}
				statePayload := payloads[i]
				if isResponsesWebsocketCompletionEvent(eventType) {
					statePayload = restoreResponsesWebsocketCompletionOutput(statePayload, outputItemsByIndex, outputItemsFallback)
					if opts.preserveCompletionOutput == nil || !opts.preserveCompletionOutput() {
						payloads[i] = statePayload
					}
				}
				// Single-response recovery uses the repair cache. Duplex continuations
				// are owned by the upstream socket for its entire lifetime.
				if !isDuplex {
					if toolCacheTurn != nil {
						toolCacheTurn.recordResponse(statePayload)
					} else {
						recordResponsesWebsocketToolCallsFromPayload(downstreamSessionKey, statePayload)
					}
				}
				var payloadErrMsg *interfaces.ErrorMessage
				// Duplex request errors remain recoverable on the current socket.
				// Credential quota failures enter terminal handling, whose replay
				// guard preserves every response already delivered downstream.
				keepDuplexErrorInBand := responseStarted && isDuplex
				if eventType == wsEventTypeError || eventType == wsEventTypeFailed {
					errMsg := responsesWebsocketErrorMessageFromPayload(payloads[i])
					if errMsg != nil && isResponsesWebsocketQuotaOrCooldownError(errMsg.Error) {
						keepDuplexErrorInBand = false
					}
					if keepDuplexErrorInBand {
						if errMsg != nil && h != nil {
							h.LoggingAPIResponseError(context.WithValue(context.Background(), "gin", c), errMsg)
						}
					} else {
						payloadErrMsg = errMsg
					}
				} else if eventType == wsEventTypeIncomplete && !interrupted && !keepDuplexErrorInBand {
					payloadErrMsg = responsesWebsocketIncompleteErrorMessageFromPayload(payloads[i])
				} else if isResponsesWebsocketCompletionEvent(eventType) {
					completed = true
					if opts.completedResponse != nil {
						opts.completedResponse(strings.Clone(ojson.GetBytesNoCopy(statePayload, "response.id").String()))
					}
					completedOutput = responseCompletedOutputFromPayload(statePayload, outputItemsByIndex, outputItemsFallback)
				} else if interrupted {
					completed = true
					completedOutput = responseCompletedOutputFromPayload(statePayload, outputItemsByIndex, outputItemsFallback)
				}
				if payloadErrMsg != nil && eventType != wsEventTypeIncomplete {
					return handleError(payloadErrMsg, payloads[i])
				}
				establishesReplayBoundary := responsesWebsocketPayloadEstablishesReplayBoundary(payloads[i])
				awaitsReplayBoundary := eventType == "codex.response.metadata" || responsesWebsocketTurnStateOnlyMetadata(payloads[i])
				if allowTranscriptReplayBeforeOutput && !downstreamOutputSent &&
					(awaitsReplayBoundary || len(pendingProtocolPayloads) > 0 && !establishesReplayBoundary) {
					pendingProtocolPayloads = append(pendingProtocolPayloads, payloads[i])
					continue
				}
				if establishesReplayBoundary {
					if errFlush := flushMetadataBeforePayload(payloads[i]); errFlush != nil {
						cancel(errFlush)
						return completedOutput, nil, false, errFlush
					}
				}
				if errWrite := writePayload(payloads[i]); errWrite != nil {
					cancel(errWrite)
					return completedOutput, nil, false, errWrite
				}
				if payloadErrMsg != nil {
					cancel(payloadErrMsg.Error)
					return completedOutput, payloadErrMsg, false, nil
				}
				if establishesReplayBoundary {
					downstreamOutputSent = true
				}
				if !isDuplex && (isResponsesWebsocketCompletionEvent(eventType) || interrupted) {
					cancel(nil)
					return completedOutput, nil, false, nil
				}
			}
		}
	}
}

func completeResponsesWebsocketLocalInterrupt(
	writer *responsesWebsocketWriter,
	wsTimelineLog websocketTimelineAppender,
	cancel handlers.APIHandlerCancelFunc,
	responseID string,
	outputItemsByIndex map[int64][]byte,
	outputItemsFallback [][]byte,
) ([]byte, *interfaces.ErrorMessage, bool, error) {
	output := responseCompletedOutputFromPayload([]byte(`{"response":{"output":[]}}`), outputItemsByIndex, outputItemsFallback)
	incompleteDoc := ojson.NewDoc([]byte(`{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"interrupted"}}}`))
	if errSet := incompleteDoc.Set("response.id", responseID); errSet != nil {
		cancel(errSet)
		return output, nil, false, errSet
	}
	if errSet := incompleteDoc.SetRawBytes("response.output", output); errSet != nil {
		cancel(errSet)
		return output, nil, false, errSet
	}
	cancel(context.Canceled)
	if errWrite := writeResponsesWebsocketPayload(writer, wsTimelineLog, incompleteDoc.Bytes(), time.Now()); errWrite != nil {
		return output, nil, false, errWrite
	}
	return output, nil, false, nil
}

func responsesWebsocketTurnStateMetadataPayload(headers http.Header, pendingPayloads [][]byte, nextPayload []byte) []byte {
	turnState := strings.TrimSpace(headers.Get(wsTurnStateHeader))
	if turnState == "" {
		return nil
	}
	hasTurnState := func(payload []byte) bool {
		return len(payload) > 0 &&
			strings.TrimSpace(ojson.GetBytesNoCopy(payload, "type").String()) == "response.metadata" &&
			strings.TrimSpace(ojson.GetBytesNoCopy(payload, "headers").Get(wsTurnStateHeader).String()) == turnState
	}
	for _, pending := range pendingPayloads {
		if hasTurnState(pending) {
			return nil
		}
	}
	if hasTurnState(nextPayload) {
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
	root := ojson.ParseBytes(payload)
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

func responsesWebsocketPayloadEstablishesReplayBoundary(payload []byte) bool {
	eventType := strings.TrimSpace(ojson.GetBytes(payload, "type").String())
	switch eventType {
	case "codex.rate_limits", "codex.response.metadata":
		return false
	default:
		return true
	}
}

func responsesWebsocketErrorStatus(errMsg *interfaces.ErrorMessage) int {
	if errMsg == nil {
		return 0
	}
	if errMsg.StatusCode > 0 {
		return errMsg.StatusCode
	}
	return clienterror.HTTPStatusFromError(errMsg.Error)
}

// shouldExposeResponsesUpstreamError reports whether a terminal upstream error
// must reach the downstream client.
//
// Request-shape failures, permanent auth failures, and terminal 429 quota or
// model-cooldown failures (where credential failover has already exhausted all
// eligible candidates) are exposed so the client can surface the final error
// instead of reconnecting in a loop. Transient credential and transport failures
// stay silent so the client reconnects with a full context resend.
func shouldExposeResponsesUpstreamError(errMsg *interfaces.ErrorMessage) bool {
	if errMsg == nil {
		return false
	}
	if coreauth.IsTerminalAuthError(errMsg.Error) {
		return true
	}
	status := responsesWebsocketErrorStatus(errMsg)
	if status == http.StatusTooManyRequests || isResponsesWebsocketQuotaOrCooldownError(errMsg.Error) {
		return true
	}
	return clienterror.IsRequestFault(status, errMsg.Error)
}

func isResponsesWebsocketQuotaOrCooldownError(err error) bool {
	if err == nil {
		return false
	}
	type modelCooldownMarker interface {
		IsModelCooldown() bool
	}
	var mcm modelCooldownMarker
	if errors.As(err, &mcm) && mcm != nil && mcm.IsModelCooldown() {
		return true
	}
	var authErr *coreauth.Error
	if errors.As(err, &authErr) && authErr != nil && strings.EqualFold(strings.TrimSpace(authErr.Code), "model_cooldown") {
		return true
	}
	body := strings.TrimSpace(err.Error())
	type responseBodyProvider interface {
		ResponseBody() []byte
	}
	var responseBody responseBodyProvider
	if errors.As(err, &responseBody) && responseBody != nil && len(responseBody.ResponseBody()) > 0 {
		body = strings.TrimSpace(string(responseBody.ResponseBody()))
	}
	if body == "" || !ojson.Valid(body) {
		return false
	}
	for _, path := range []string{"error.type", "type", "response.error.type", "body.error.type"} {
		switch strings.ToLower(strings.TrimSpace(ojson.Get(body, path).String())) {
		case "usage_limit_reached", "model_cooldown":
			return true
		}
	}
	for _, path := range []string{"error.code", "code", "response.error.code", "body.error.code"} {
		switch strings.ToLower(strings.TrimSpace(ojson.Get(body, path).String())) {
		case "usage_limit_reached", "model_cooldown":
			return true
		}
	}
	return false
}

func writeResponsesWebsocketTerminalError(
	writer *responsesWebsocketWriter,
	wsTimelineLog websocketTimelineAppender,
	errMsg *interfaces.ErrorMessage,
	payload []byte,
	preservePayload bool,
) ([]byte, bool, error) {
	exposePayload := shouldExposeResponsesUpstreamError(errMsg) || preservePayload && len(payload) > 0
	if !exposePayload {
		// Keep the upstream reason in the request-log timeline even though the client
		// only observes a closed connection, otherwise silent failures are
		// undiagnosable after the fact.
		if wsTimelineLog != nil && errMsg != nil {
			appendWebsocketTimelineDisconnect(wsTimelineLog, errMsg.Error, time.Now())
		}
		_, errClose := writer.closeWithoutError()
		if errClose != nil {
			return nil, false, errClose
		}
		return nil, false, websocket.ErrCloseSent
	}

	if len(payload) == 0 {
		var errBuild error
		payload, errBuild = buildResponsesWebsocketErrorPayload(errMsg)
		if errBuild != nil {
			_, _ = writer.closeWithoutError()
			return nil, false, errBuild
		}
	}

	wrote, errClose := writer.closeWithPayload(payload)
	if wrote && wsTimelineLog != nil {
		wsTimelineLog.Append("response", payload, time.Now())
	}
	if errClose != nil {
		return payload, wrote, errClose
	}
	return payload, wrote, websocket.ErrCloseSent
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
	if errMsg == nil || cliproxyexecutor.IsUpstreamWebsocketTurnError(errMsg.Error) {
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
		if clienterror.ClassifyCodexWebsocketError(errMsg.Error.Error()) == clienterror.CodexPreviousResponseNotFound {
			return true
		}
		msg := strings.ToLower(errMsg.Error.Error())
		switch {
		case strings.Contains(msg, "stream closed before response.completed"),
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
	code := clienterror.ClassifyCodexWebsocketError(errMsg.Error.Error())
	if code == clienterror.CodexWebsocketConnectionLimitReached {
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
	return code == clienterror.CodexPreviousResponseNotFound
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
	return errors.As(errMsg.Error, &replayRequired) &&
		replayRequired != nil &&
		replayRequired.CodexWebsocketReplayRequired()
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

func responseCompletedOutputFromPayload(payload []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback [][]byte) []byte {
	output := ojson.GetBytes(payload, "response.output")
	if output.Exists() && output.IsArray() && len(output.Array()) > 0 {
		return bytes.Clone([]byte(output.Raw))
	}
	if collected := responsesWebsocketCollectedOutputItems(outputItemsByIndex, outputItemsFallback); len(collected) > 0 {
		return collected
	}
	return []byte("[]")
}

func restoreResponsesWebsocketCompletionOutput(payload []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback [][]byte) []byte {
	output := ojson.GetBytesNoCopy(payload, "response.output")
	if output.Exists() && output.IsArray() && len(output.Array()) > 0 {
		reconciledOutput, changed := reconcileResponsesWebsocketCompletionToolCalls(output, outputItemsByIndex, outputItemsFallback)
		if !changed {
			return payload
		}
		doc := ojson.NewDoc(payload)
		if errSet := doc.SetRawBytes("response.output", reconciledOutput); errSet != nil {
			return payload
		}
		return doc.Bytes()
	}
	if len(outputItemsByIndex) == 0 && len(outputItemsFallback) == 0 {
		return payload
	}
	restoredOutput := responseCompletedOutputFromPayload(payload, outputItemsByIndex, outputItemsFallback)
	doc := ojson.NewDoc(payload)
	if errSet := doc.SetRawBytes("response.output", restoredOutput); errSet != nil {
		return payload
	}
	return doc.Bytes()
}

func reconcileResponsesWebsocketCompletionToolCalls(output ojson.Result, outputItemsByIndex map[int64][]byte, outputItemsFallback [][]byte) ([]byte, bool) {
	collectedToolCalls := make(map[string]json.RawMessage)
	recordCollectedToolCall := func(raw []byte) {
		item := util.ParseGJSONBytesNoCopy(raw)
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

func isCompleteResponsesWebsocketToolCall(item ojson.Result) bool {
	if !item.Exists() || !item.IsObject() {
		return false
	}
	callID := item.Get("call_id")
	name := item.Get("name")
	if callID.Type != ojson.String || strings.TrimSpace(callID.String()) == "" || name.Type != ojson.String || strings.TrimSpace(name.String()) == "" {
		return false
	}

	switch strings.TrimSpace(item.Get("type").String()) {
	case "function_call":
		arguments := item.Get("arguments")
		return arguments.Exists() && arguments.Type == ojson.String
	case "custom_tool_call":
		input := item.Get("input")
		return input.Exists() && input.Type == ojson.String
	default:
		return false
	}
}

func collectResponsesWebsocketOutputItemDone(payload []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback *[][]byte) {
	item := ojson.GetBytes(payload, "item")
	if !item.Exists() || item.Type != ojson.JSON {
		return
	}
	raw := bytes.Clone([]byte(item.Raw))
	outputIndex := ojson.GetBytes(payload, "output_index")
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
	items := make([]json.RawMessage, 0, len(outputItemsByIndex)+len(outputItemsFallback))
	appendItem := func(raw []byte) {
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 {
			return
		}
		item := util.ParseGJSONBytesNoCopy(trimmed)
		if isResponsesToolCallType(item.Get("type").String()) && !isCompleteResponsesWebsocketToolCall(item) {
			return
		}
		items = append(items, append(json.RawMessage(nil), trimmed...))
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
	marshaled, errMarshal := json.Marshal(items)
	if errMarshal != nil {
		return nil
	}
	return marshaled
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

func buildResponsesWebsocketErrorPayload(errMsg *interfaces.ErrorMessage) ([]byte, error) {
	status := http.StatusInternalServerError
	errText := http.StatusText(status)
	if errMsg != nil {
		if code := responsesWebsocketErrorStatus(errMsg); code > 0 {
			status = code
			errText = http.StatusText(status)
		}
		if isResponsesWebsocketQuotaOrCooldownError(errMsg.Error) && (status <= 0 || status >= http.StatusInternalServerError) {
			status = http.StatusTooManyRequests
			errText = http.StatusText(status)
		}
		if errMsg.Error != nil && strings.TrimSpace(errMsg.Error.Error()) != "" {
			errText = errMsg.Error.Error()
		}
	}

	var errCause error
	if errMsg != nil {
		errCause = errMsg.Error
	}
	body := handlers.BuildErrorResponseBodyWithError(status, errText, errCause)
	payloadDoc := ojson.NewDoc([]byte(`{}`))
	if errSet := payloadDoc.Set("type", wsEventTypeError); errSet != nil {
		return nil, errSet
	}
	if errSet := payloadDoc.Set("status", status); errSet != nil {
		return nil, errSet
	}

	if errMsg != nil {
		headersDoc := ojson.NewDoc([]byte(`{}`))
		hasHeaders := false
		trustedDeadline := coreauth.HasTrustedRetryDeadline(errMsg.Error)
		for key, values := range coreauth.SafeResponseHeaders(errMsg.Error) {
			if len(values) == 0 || strings.TrimSpace(values[0]) == "" {
				continue
			}
			headerPath := strings.ReplaceAll(strings.ReplaceAll(key, `\\`, `\\\\`), ".", `\\.`)
			if errSet := headersDoc.Set(headerPath, values[0]); errSet != nil {
				return nil, errSet
			}
			hasHeaders = true
		}
		for key, values := range errMsg.Addon {
			if len(values) == 0 || strings.TrimSpace(values[0]) == "" {
				continue
			}
			if trustedDeadline && http.CanonicalHeaderKey(key) == "Retry-After" {
				continue
			}
			headerPath := strings.ReplaceAll(strings.ReplaceAll(key, `\\`, `\\\\`), ".", `\\.`)
			if errSet := headersDoc.Set(headerPath, values[0]); errSet != nil {
				return nil, errSet
			}
			hasHeaders = true
		}
		if hasHeaders {
			if errSet := payloadDoc.SetRawBytes("headers", headersDoc.Bytes()); errSet != nil {
				return nil, errSet
			}
		}
	}

	hasErrorField := false
	if len(body) > 0 && json.Valid(body) {
		errorNode := ojson.GetBytesNoCopy(body, "error")
		var errSet error
		if errorNode.Exists() {
			errSet = payloadDoc.SetRawBytes("error", []byte(errorNode.Raw))
		} else {
			errSet = payloadDoc.SetRawBytes("error", body)
		}
		if errSet != nil {
			return nil, errSet
		}
		hasErrorField = true
	}

	if !hasErrorField {
		if errSet := payloadDoc.Set("error.type", "server_error"); errSet != nil {
			return nil, errSet
		}
		if errSet := payloadDoc.Set("error.message", errText); errSet != nil {
			return nil, errSet
		}
	}

	var turnErr *cliproxyexecutor.UpstreamWebsocketTurnError
	if errMsg != nil && errors.As(errMsg.Error, &turnErr) && turnErr.EventID != "" {
		if errSet := payloadDoc.Set("error.event_id", turnErr.EventID); errSet != nil {
			return nil, errSet
		}
	}
	return payloadDoc.Bytes(), nil
}

func writeResponsesWebsocketError(writer *responsesWebsocketWriter, wsTimelineLog websocketTimelineAppender, errMsg *interfaces.ErrorMessage) ([]byte, error) {
	payload, errBuild := buildResponsesWebsocketErrorPayload(errMsg)
	if errBuild != nil {
		return nil, errBuild
	}
	return payload, writeResponsesWebsocketPayload(writer, wsTimelineLog, payload, time.Now())
}
