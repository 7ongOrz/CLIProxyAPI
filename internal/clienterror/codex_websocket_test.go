package clienterror

import (
	"fmt"
	"testing"
)

func TestClassifyCodexWebsocketError(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want CodexWebsocketErrorCode
	}{
		{"plain code", "previous_response_not_found", ""},
		{"plain message", "No response found for previous_response_id resp_123", CodexPreviousResponseNotFound},
		{"message envelope", `{"error":{"message":"No response found for previous_response_id resp_123"}}`, CodexPreviousResponseNotFound},
		{"code precedence", `{"error":{"code":"invalid_prompt","message":"No response found for previous_response_id resp_123"}}`, ""},
		{"code precedence over type", `{"error":{"code":"rate_limit_exceeded","type":"websocket_connection_limit_reached"}}`, ""},
		{"quoted code", `{"error":{"code":"invalid_prompt","message":"input mentioned previous_response_not_found"}}`, ""},
		{"general missing response", `{"error":{"message":"previous response with id 'resp-1' not found"}}`, ""},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyCodexWebsocketError(tt.raw); got != tt.want {
				t.Fatalf("code = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClassifyCodexWebsocketErrorEnvelopes(t *testing.T) {
	for _, code := range []CodexWebsocketErrorCode{CodexPreviousResponseNotFound, CodexWebsocketConnectionLimitReached} {
		for _, envelope := range []string{
			`{"type":"error","error":{"code":%q}}`,
			`{"body":{"error":{"code":%q}}}`,
			`{"type":"response.failed","response":{"error":{"code":%q}}}`,
			`{"code":%q}`,
		} {
			raw := fmt.Sprintf(envelope, code)
			if got := ClassifyCodexWebsocketError(raw); got != code {
				t.Errorf("classify %s = %q, want %q", raw, got, code)
			}
		}
	}
	for _, raw := range []string{
		`{"error":{"type":"websocket_connection_limit_reached"}}`,
		`{"body":{"error":{"type":"websocket_connection_limit_reached"}}}`,
		`{"response":{"error":{"type":"websocket_connection_limit_reached"}}}`,
		`{"error":"websocket_connection_limit_reached"}`,
	} {
		if got := ClassifyCodexWebsocketError(raw); got != CodexWebsocketConnectionLimitReached {
			t.Errorf("classify %s = %q, want %q", raw, got, CodexWebsocketConnectionLimitReached)
		}
	}
}
