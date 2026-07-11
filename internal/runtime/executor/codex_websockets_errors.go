package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type statusErrWithHeaders struct {
	statusErr
	headers http.Header
}

type codexIdentityStatusErrWithHeaders struct {
	statusErrWithHeaders
	clientErr error
}

func (e codexIdentityStatusErrWithHeaders) ClientError() error {
	return e.clientErr
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

func withCodexWebsocketIdentityClientError(payload []byte, identityState codexIdentityConfuseState, fallback error) error {
	clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
	if bytes.Equal(clientPayload, payload) {
		return fallback
	}
	if clientErr, ok := parseCodexWebsocketError(clientPayload); ok {
		if statusError, okStatus := fallback.(statusErrWithHeaders); okStatus {
			return codexIdentityStatusErrWithHeaders{
				statusErrWithHeaders: statusError,
				clientErr:            clientErr,
			}
		}
	}
	return fallback
}

func parseCodexResponseFailed(payload []byte) (statusErr, bool) {
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
	return newCodexStatusErr(codexResponseFailedStatus(payload, body), body), true
}

func parseCodexResponseIncomplete(payload []byte) (statusErr, bool) {
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

func parseCodexResponseTerminalError(payload []byte) (statusErr, string, bool) {
	if streamErr, ok := parseCodexResponseFailed(payload); ok {
		return streamErr, "response_failed", true
	}
	if streamErr, ok := parseCodexResponseIncomplete(payload); ok {
		return streamErr, "response_incomplete", true
	}
	return statusErr{}, "", false
}

func codexResponseFailedStatus(payload, body []byte) int {
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
