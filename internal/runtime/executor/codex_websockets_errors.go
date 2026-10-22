package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/clienterror"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
)

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

func (e codexWebsocketTranscriptReplayRequiredError) CodexWebsocketReplayRequired() bool {
	return true
}

func codexMissingRequiredWebsocketError(turn helps.CodexInstructionTurn) error {
	if err := turn.OnConnection(nil); err != nil {
		return err
	}
	return cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
}

func codexDownstreamWebsocketReplayError(ctx context.Context, payload []byte, cause error) (error, bool) {
	if !cliproxyexecutor.DownstreamWebsocket(ctx) {
		return cause, false
	}
	reason := clienterror.ClassifyCodexWebsocketError(string(payload))
	if reason == "" {
		return cause, false
	}
	if reason == clienterror.CodexPreviousResponseNotFound {
		if status := codexWebsocketErrorStatusCode(cause); status > 0 && status != http.StatusBadRequest {
			return cause, false
		}
	}
	return codexWebsocketTranscriptReplayRequiredError{reason: string(reason), cause: cause}, true
}

func (e statusErrWithHeaders) Headers() http.Header {
	if e.headers == nil {
		return nil
	}
	return e.headers.Clone()
}

func parseCodexWebsocketError(payload []byte) (error, bool) {
	return parseCodexWebsocketErrorWithCooling(payload, false)
}

func codexWebsocketErrorStatus(payload []byte) int {
	for _, path := range []string{"status", "status_code"} {
		if status := int(ojson.GetBytesNoCopy(payload, path).Int()); status > 0 {
			return status
		}
	}
	for _, path := range []string{
		"body.status", "body.status_code",
		"error.status", "error.status_code",
		"body.error.status", "body.error.status_code",
		"response.error.status", "response.error.status_code",
	} {
		if status := int(ojson.GetBytesNoCopy(payload, path).Int()); status >= 400 && status <= 599 {
			return status
		}
	}
	return 0
}

func codexWebsocketErrorStatusWithHeaders(payload []byte, headers http.Header) int {
	if status := codexWebsocketErrorStatus(payload); status > 0 {
		return status
	}
	switch clienterror.ClassifyCodexWebsocketError(string(payload)) {
	case clienterror.CodexWebsocketConnectionLimitReached:
		return http.StatusTooManyRequests
	case clienterror.CodexPreviousResponseNotFound:
		return http.StatusBadRequest
	}
	if len(headers) == 0 {
		return 0
	}
	return inferCodexWebsocketErrorStatus(payload)
}

func inferCodexWebsocketErrorStatus(payload []byte) int {
	if clienterror.ClassifyCodexWebsocketError(string(payload)) == clienterror.CodexWebsocketConnectionLimitReached {
		return http.StatusTooManyRequests
	}
	body, ok := codexTerminalFailureBody(payload)
	if !ok {
		return 0
	}
	if isCodexUsageLimitError(body) || isCodexBillingQuotaError(body) || isCodexTransientCapacityError(body) {
		return http.StatusTooManyRequests
	}
	if codexTerminalErrorIsContextLength(body) {
		return http.StatusBadRequest
	}
	if code, _, classified := codexStatusErrorClassification(http.StatusBadRequest, body); classified {
		if code == "auth_unavailable" {
			return http.StatusUnauthorized
		}
		return http.StatusBadRequest
	}
	errorType := strings.ToLower(strings.TrimSpace(ojson.GetBytesNoCopy(body, "error.type").String()))
	errorCode := strings.ToLower(strings.TrimSpace(ojson.GetBytesNoCopy(body, "error.code").String()))
	if errorType == "service_unavailable_error" || errorCode == "server_is_overloaded" {
		return http.StatusServiceUnavailable
	}
	return codexResponseFailureStatus(payload, body)
}

func parseCodexWebsocketErrorWithCooling(payload []byte, modelLevelCooling bool) (error, bool) {
	if len(payload) == 0 {
		return nil, false
	}
	if helps.CodexResponseEventType(payload) != "error" {
		return nil, false
	}
	headers := parseCodexWebsocketErrorHeaders(payload)
	status := codexWebsocketErrorStatusWithHeaders(payload, headers)
	if status <= 0 {
		return nil, false
	}

	out := buildCodexWebsocketErrorPayload(payload, status)
	isUsageLimit := isCodexUsageLimitError(out)
	isBillingQuota := isCodexBillingQuotaError(out)
	transientCapacity := !isUsageLimit && !isBillingQuota && isCodexTransientCapacityError(out)
	errCode := status
	if isBillingQuota {
		errCode = http.StatusTooManyRequests
	}
	statusError := statusErr{
		code:               errCode,
		msg:                string(out),
		credentialScoped:   isBillingQuota || (isUsageLimit && !modelLevelCooling),
		transientRateLimit: transientCapacity && errCode == http.StatusTooManyRequests,
	}
	// Hint precedence: the quota body reset wins, then the connection-limit
	// immediate-retry contract, then the generic Retry-After header.
	if retryAfter := parseCodexRetryAfter(errCode, out, time.Now()); retryAfter != nil {
		statusError.retryAfter = retryAfter
	} else if clienterror.ClassifyCodexWebsocketError(string(payload)) == clienterror.CodexWebsocketConnectionLimitReached {
		retryAfter := time.Duration(0)
		statusError.retryAfter = &retryAfter
	} else if shouldParseCodexRetryAfterHeader(errCode) {
		if retryAfter := parseRetryAfterHeader(headers, time.Now()); retryAfter != nil {
			statusError.retryAfter = retryAfter
		}
	}
	return statusErrWithHeaders{
		statusErr: statusError,
		headers:   headers,
	}, true
}

func clearCodexReasoningReplayOnWebsocketError(ctx context.Context, scope codexReasoningReplayScope, payload []byte) error {
	headers := parseCodexWebsocketErrorHeaders(payload)
	status := codexWebsocketErrorStatusWithHeaders(payload, headers)
	if status <= 0 {
		return nil
	}
	return clearCodexReasoningReplayOnInvalidSignature(ctx, scope, status, buildCodexWebsocketErrorPayload(payload, status))
}

func parseCodexResponseFailed(payload []byte, modelLevelCooling bool) (statusErr, bool) {
	if len(payload) == 0 || helps.CodexResponseEventType(payload) != "response.failed" {
		return statusErr{}, false
	}

	streamErr, _, ok := codexTerminalFailureErrWithCooling(payload, modelLevelCooling)
	return streamErr, ok
}

func parseCodexResponseIncomplete(payload []byte) (statusErr, bool) {
	if len(payload) == 0 || strings.TrimSpace(ojson.GetBytesNoCopy(payload, "type").String()) != "response.incomplete" {
		return statusErr{}, false
	}
	reason := strings.TrimSpace(ojson.GetBytesNoCopy(payload, "response.incomplete_details.reason").String())
	if reason == "interrupted" {
		return statusErr{}, false
	}
	if reason == "" {
		reason = "unknown"
	}
	doc := ojson.NewDocString(`{"error":{"type":"invalid_request_error","code":"response_incomplete"}}`)
	_ = doc.Set("error.message", fmt.Sprintf("Incomplete response returned, reason: %s", reason))
	return newCodexStatusErr(http.StatusBadRequest, doc.Bytes()), true
}

type codexWebsocketTerminalFailure struct {
	err              error
	rawErr           error
	body             []byte
	reason           string
	isWebsocketError bool
	replayRequired   bool
}

func parseCodexWebsocketTerminalFailure(ctx context.Context, payload []byte, modelLevelCooling bool) (codexWebsocketTerminalFailure, bool) {
	if wsErr, ok := parseCodexWebsocketErrorWithCooling(payload, modelLevelCooling); ok {
		terminalErr, replayRequired := codexDownstreamWebsocketReplayError(ctx, payload, wsErr)
		return codexWebsocketTerminalFailure{
			err:              terminalErr,
			rawErr:           wsErr,
			body:             []byte(wsErr.Error()),
			reason:           "upstream_error",
			isWebsocketError: true,
			replayRequired:   replayRequired,
		}, true
	}
	if streamErr, terminalBody, ok := codexTerminalFailureErrWithCooling(payload, modelLevelCooling); ok {
		reason := "terminal_failure"
		if helps.CodexResponseEventType(payload) == "response.failed" {
			reason = "response_failed"
		}
		terminalErr, replayRequired := codexDownstreamWebsocketReplayError(ctx, payload, streamErr)
		return codexWebsocketTerminalFailure{
			err:            terminalErr,
			rawErr:         streamErr,
			body:           terminalBody,
			reason:         reason,
			replayRequired: replayRequired,
		}, true
	}
	return codexWebsocketTerminalFailure{}, false
}

func codexWebsocketUpstreamErrorDropReason(payload []byte, err error) string {
	switch codexWebsocketErrorStatusCode(err) {
	case http.StatusUnauthorized:
		return "upstream_unauthorized"
	case http.StatusPaymentRequired:
		return "upstream_payment_required"
	case http.StatusForbidden:
		return "upstream_forbidden"
	case http.StatusTooManyRequests:
		return "upstream_rate_limited"
	}
	if clienterror.ClassifyCodexWebsocketError(string(payload)) == clienterror.CodexPreviousResponseNotFound {
		return "previous_response_not_found"
	}
	return ""
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
	return strings.TrimSpace(ojson.GetBytesNoCopy(payload, "previous_response_id").String()) != ""
}

func codexWebsocketReadResetError(cause error, replayable bool, turn helps.CodexInstructionTurn) error {
	var reset codexWebsocketUpstreamResetError
	var closeErr *websocket.CloseError
	if errors.As(cause, &closeErr) && closeErr.Code == websocket.CloseMessageTooBig {
		return nil
	}
	if errors.As(cause, &reset) {
		// A completed write leaves the outcome of a connection-bound turn unknown.
		if turn.ConnectionBound() {
			return turn.ConnectionLost(cause)
		}
		if replayable {
			return codexWebsocketTranscriptReplayRequiredError{reason: "read_error", cause: cause}
		}
	}
	return nil
}

func buildCodexWebsocketErrorPayload(payload []byte, status int) []byte {
	doc := ojson.NewDocString(`{"type":"error"}`)
	_ = doc.Set("status", status)
	if seq := ojson.GetBytesNoCopy(payload, "sequence_number"); seq.Exists() {
		_ = doc.Set("sequence_number", seq.Int())
	}

	fillBlankErrorMessage := func() {
		if node := doc.Get("error"); !node.IsObject() {
			scalar := ""
			if node.Type != ojson.Null && strings.TrimSpace(node.String()) != "" {
				scalar = node.String()
			}
			_ = doc.SetRawBytes("error", []byte(`{}`))
			if scalar != "" {
				_ = doc.Set("error.message", scalar)
				return
			}
		}
		if strings.TrimSpace(doc.Get("error.message").String()) != "" {
			return
		}
		if code := strings.TrimSpace(doc.Get("error.code").String()); code != "" {
			_ = doc.Set("error.message", codexPolicyFallbackMessage(code))
		} else if errType := strings.TrimSpace(doc.Get("error.type").String()); errType != "" {
			_ = doc.Set("error.message", errType)
		} else {
			_ = doc.Set("error.message", http.StatusText(status))
		}
	}

	if bodyNode := ojson.GetBytesNoCopy(payload, "body"); bodyNode.Exists() {
		_ = doc.SetRawBytes("body", []byte(bodyNode.Raw))
		if bodyErrorNode := bodyNode.Get("error"); bodyErrorNode.Exists() && bodyErrorNode.Type != ojson.Null {
			_ = doc.SetRawBytes("error", []byte(bodyErrorNode.Raw))
			fillBlankErrorMessage()
			return doc.Bytes()
		}
	}

	for _, path := range []string{"error", "response.error"} {
		if errNode := ojson.GetBytesNoCopy(payload, path); errNode.Exists() && errNode.Type != ojson.Null {
			_ = doc.SetRawBytes("error", []byte(errNode.Raw))
			fillBlankErrorMessage()
			return doc.Bytes()
		}
	}
	if topLevel := codexTerminalTopLevelErrorBody(payload); topLevel != nil {
		_ = doc.SetRawBytes("error", []byte(topLevel.Get("error").Raw))
		fillBlankErrorMessage()
		return doc.Bytes()
	}

	_ = doc.Set("error.type", "server_error")
	_ = doc.Set("error.message", http.StatusText(status))
	return doc.Bytes()
}

func shouldParseCodexRetryAfterHeader(statusCode int) bool {
	switch statusCode {
	case http.StatusTooManyRequests,
		http.StatusRequestTimeout,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
		520, 521, 522, 523, 524, 525, 526:
		return true
	default:
		return false
	}
}

func parseCodexWebsocketErrorHeaders(payload []byte) http.Header {
	if len(payload) == 0 {
		return nil
	}
	mapped := make(http.Header)
	seen := make(map[string]struct{})
	for _, path := range []string{"headers", "body.headers", "error.headers", "body.error.headers", "response.error.headers"} {
		headersNode := ojson.GetBytesNoCopy(payload, path)
		if !headersNode.Exists() || !headersNode.IsObject() {
			continue
		}
		headersNode.ForEach(func(key, value ojson.Result) bool {
			name := strings.TrimSpace(key.String())
			if name == "" {
				return true
			}
			canonical := http.CanonicalHeaderKey(name)
			if _, exists := seen[canonical]; exists {
				return true
			}
			// Mark the header as seen before validating its value so an explicit outer
			// blank or unsupported entry still shadows any nested fallback value.
			seen[canonical] = struct{}{}
			switch value.Type {
			case ojson.String:
				if v := strings.TrimSpace(value.String()); v != "" {
					mapped.Set(strings.Clone(name), strings.Clone(v))
				}
			case ojson.Number, ojson.True, ojson.False:
				if v := strings.TrimSpace(value.Raw); v != "" {
					mapped.Set(strings.Clone(name), strings.Clone(v))
				}
			default:
			}
			return true
		})
	}
	if len(mapped) == 0 {
		return nil
	}
	return mapped
}

func normalizeCodexWebsocketCompletion(payload []byte) []byte {
	if strings.TrimSpace(ojson.GetBytesNoCopy(payload, "type").String()) == "response.done" {
		doc := ojson.NewDoc(payload)
		if err := doc.Set("type", "response.completed"); err == nil {
			return doc.Bytes()
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
	body, _ := websocketHandshakeBodyWithReadErr(resp)
	return body
}

func websocketHandshakeBodyWithReadErr(resp *http.Response) ([]byte, error) {
	if resp == nil || resp.Body == nil {
		return nil, nil
	}
	body, errRead := io.ReadAll(resp.Body)
	closeHTTPResponseBody(resp, "codex websockets executor: close handshake response body error")
	if len(body) == 0 {
		return nil, errRead
	}
	return body, errRead
}

func recordWebsocketUpgradeRejection(ctx context.Context, cfg *config.Config, wsReqLog helps.UpstreamRequestLog, resp *http.Response) []byte {
	bodyErr, errRead := websocketHandshakeBodyWithReadErr(resp)
	if resp != nil {
		helps.RecordAPIWebsocketUpgradeRejection(ctx, cfg, websocketUpgradeRequestLog(wsReqLog), resp.StatusCode, resp.Header.Clone(), bodyErr)
		if errRead != nil {
			helps.RecordAPIResponseError(ctx, cfg, errRead)
		}
	}
	return bodyErr
}

func recordWebsocketDialFailure(ctx context.Context, cfg *config.Config, wsReqLog helps.UpstreamRequestLog, resp *http.Response, errDial error) []byte {
	bodyErr := recordWebsocketUpgradeRejection(ctx, cfg, wsReqLog, resp)
	helps.RecordAPIWebsocketError(ctx, cfg, "dial", errDial)
	return bodyErr
}

func recordWebsocketDialRetryFailure(ctx context.Context, cfg *config.Config, wsReqLog helps.UpstreamRequestLog, resp *http.Response, errDial error) []byte {
	if resp != nil {
		bodyErr := recordWebsocketUpgradeRejection(ctx, cfg, wsReqLog, resp)
		helps.RecordAPIWebsocketError(ctx, cfg, "dial_retry", errDial)
		return bodyErr
	}
	if cfg != nil && !cfg.CommercialMode && !cfg.RequestLog {
		helps.RecordAPIRequest(ctx, cfg, websocketUpgradeRequestLog(wsReqLog))
	}
	helps.RecordAPIWebsocketError(ctx, cfg, "dial_retry", errDial)
	return nil
}

func closeHTTPResponseBody(resp *http.Response, logPrefix string) {
	if resp == nil || resp.Body == nil {
		return
	}
	if errClose := resp.Body.Close(); errClose != nil {
		log.Errorf("%s: %v", logPrefix, errClose)
	}
}
