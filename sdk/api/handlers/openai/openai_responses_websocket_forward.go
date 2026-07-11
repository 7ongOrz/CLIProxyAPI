package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/clienterror"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

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

	handleError := func(errMsg *interfaces.ErrorMessage, terminalPayload []byte) ([]byte, string, []string, *interfaces.ErrorMessage, bool, error) {
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
			errorPayload, wrote, errTerminate := writeResponsesWebsocketTerminalError(writer, wsTimelineLog, errMsg, terminalPayload)
			if wrote {
				logResponsesWebsocketDownstreamError(sessionID, errorPayload)
			}
			cancel(errMsg.Error)
			return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), errMsg, false, errTerminate
		}
		cancel(nil)
		return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), errMsg, false, nil
	}

	for {
		if errMsg, hasErr := receivePendingResponsesWebsocketError(errs); hasErr {
			return handleError(errMsg, nil)
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
			return handleError(errMsg, nil)
		case chunk, ok := <-data:
			if !ok {
				if !completed {
					if errMsg, hasErr := receiveResponsesWebsocketFinalError(errs); hasErr {
						return handleError(errMsg, nil)
					}
					errMsg := &interfaces.ErrorMessage{
						StatusCode: http.StatusRequestTimeout,
						Error:      fmt.Errorf("stream closed before response.completed"),
					}
					return handleError(errMsg, nil)
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
				if payloadErrMsg != nil && opts.suppressError != nil && opts.suppressError(payloadErrMsg) {
					cancel(payloadErrMsg.Error)
					return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), payloadErrMsg, false, nil
				}
				if payloadErrMsg != nil && eventType != wsEventTypeIncomplete && allowTranscriptReplayBeforeOutput && !forwardedReplayBoundary && shouldRetryResponsesWebsocketTranscriptReplay(payloadErrMsg) {
					cancel(payloadErrMsg.Error)
					return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), payloadErrMsg, true, nil
				}
				establishesReplayBoundary := responsesWebsocketPayloadEstablishesReplayBoundary(payloads[i])
				awaitsReplayBoundary := eventType == "codex.response.metadata" || responsesWebsocketTurnStateOnlyMetadata(payloads[i])
				if allowTranscriptReplayBeforeOutput && !forwardedReplayBoundary &&
					(awaitsReplayBoundary || len(pendingProtocolPayloads) > 0 && !establishesReplayBoundary) {
					pendingProtocolPayloads = append(pendingProtocolPayloads, payloads[i])
					continue
				}
				if establishesReplayBoundary {
					if errFlush := flushPendingProtocolPayloads(); errFlush != nil {
						cancel(errFlush)
						return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), nil, false, errFlush
					}
				}
				if establishesReplayBoundary && !protocolMetadataHandled {
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
				if payloadErrMsg != nil && eventType != wsEventTypeIncomplete {
					return handleError(payloadErrMsg, payloads[i])
				}
				if errWrite := writePayload(payloads[i]); errWrite != nil {
					cancel(errWrite)
					return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), nil, false, errWrite
				}
				if payloadErrMsg != nil {
					cancel(payloadErrMsg.Error)
					return completedOutput, completedResponseID, sortedStringSet(pendingToolCallIDs), payloadErrMsg, false, nil
				}
				if establishesReplayBoundary {
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

func responsesWebsocketPayloadEstablishesReplayBoundary(payload []byte) bool {
	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
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
// Only request-shape failures are exposed: the client can act on them and no
// credential rotation or retry can make the request succeed. Credential, quota
// and transport failures stay silent so the client simply reconnects and retries;
// a fresh connection carries no server-side transcript, so reconnecting already
// implies a full context resend.
func shouldExposeResponsesUpstreamError(errMsg *interfaces.ErrorMessage) bool {
	if errMsg == nil {
		return false
	}
	return clienterror.IsRequestFault(responsesWebsocketErrorStatus(errMsg), errMsg.Error)
}

func writeResponsesWebsocketTerminalError(
	writer *responsesWebsocketWriter,
	wsTimelineLog websocketTimelineAppender,
	errMsg *interfaces.ErrorMessage,
	payload []byte,
) ([]byte, bool, error) {
	if !shouldExposeResponsesUpstreamError(errMsg) {
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

func buildResponsesWebsocketErrorPayload(errMsg *interfaces.ErrorMessage) ([]byte, error) {
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

	return payload, nil
}

func writeResponsesWebsocketError(writer *responsesWebsocketWriter, wsTimelineLog websocketTimelineAppender, errMsg *interfaces.ErrorMessage) ([]byte, error) {
	payload, errBuild := buildResponsesWebsocketErrorPayload(errMsg)
	if errBuild != nil {
		return nil, errBuild
	}
	return payload, writeResponsesWebsocketPayload(writer, wsTimelineLog, payload, time.Now())
}
