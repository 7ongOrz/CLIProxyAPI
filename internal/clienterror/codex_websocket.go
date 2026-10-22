package clienterror

import (
	"strings"

	"github.com/tidwall/gjson"
)

// CodexWebsocketErrorCode identifies errors that allow rebuilding upstream state.
type CodexWebsocketErrorCode string

const (
	CodexPreviousResponseNotFound        CodexWebsocketErrorCode = "previous_response_not_found"
	CodexWebsocketConnectionLimitReached CodexWebsocketErrorCode = "websocket_connection_limit_reached"
)

// ClassifyCodexWebsocketError reads the upstream error envelope. An explicit code
// takes precedence over legacy types and messages. Other errors return an empty code.
func ClassifyCodexWebsocketError(raw string) CodexWebsocketErrorCode {
	if gjson.Valid(raw) {
		for _, path := range []string{"error.code", "body.error.code", "response.error.code", "code"} {
			code := CodexWebsocketErrorCode(strings.ToLower(strings.TrimSpace(gjson.Get(raw, path).String())))
			if code == "" {
				continue
			}
			switch code {
			case CodexPreviousResponseNotFound, CodexWebsocketConnectionLimitReached:
				return code
			default:
				return ""
			}
		}
		for _, path := range []string{"error.type", "body.error.type", "response.error.type", "error"} {
			if strings.EqualFold(strings.TrimSpace(gjson.Get(raw, path).String()), string(CodexWebsocketConnectionLimitReached)) {
				return CodexWebsocketConnectionLimitReached
			}
		}
		for _, path := range []string{"error.message", "body.error.message", "response.error.message", "message"} {
			if codexPreviousResponseMissingMessage(gjson.Get(raw, path).String()) {
				return CodexPreviousResponseNotFound
			}
		}
		return ""
	}
	if codexPreviousResponseMissingMessage(raw) {
		return CodexPreviousResponseNotFound
	}
	return ""
}

func codexPreviousResponseMissingMessage(message string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(message)), "no response found for previous_response_id")
}
