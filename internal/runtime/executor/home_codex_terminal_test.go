package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executionregistry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

type terminalCodexHomeDispatcher struct {
	auth  cliproxyauth.Auth
	calls atomic.Int32
}

func (*terminalCodexHomeDispatcher) HeartbeatOK() bool { return true }
func (d *terminalCodexHomeDispatcher) RPopAuth(context.Context, string, string, http.Header, int) ([]byte, error) {
	d.calls.Add(1)
	return json.Marshal(d.auth)
}
func (*terminalCodexHomeDispatcher) AbortAmbiguousDispatch() {}

func TestHomeCodexTerminalStreamFailureUsesFreshDispatchOnNextRequest(t *testing.T) {
	for _, test := range []struct {
		name            string
		responseCreated bool
	}{
		{name: "bootstrap"},
		{name: "after bootstrap", responseCreated: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			var connections atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, errUpgrade := upgrader.Upgrade(w, r, nil)
				if errUpgrade != nil {
					t.Errorf("upgrade websocket: %v", errUpgrade)
					return
				}
				defer func() { _ = conn.Close() }()
				if _, _, errRead := conn.ReadMessage(); errRead != nil {
					return
				}
				if connections.Add(1) == 1 {
					if test.responseCreated {
						_ = conn.WriteJSON(map[string]any{"type": "response.created", "response": map[string]any{"id": "response-1"}})
					}
					status := http.StatusBadGateway
					errorBody := map[string]any{"message": "terminal failure"}
					if !test.responseCreated {
						status = http.StatusBadRequest
						errorBody["type"] = "invalid_request_error"
					}
					_ = conn.WriteJSON(map[string]any{"type": "error", "status": status, "error": errorBody})
				} else {
					_ = conn.WriteJSON(map[string]any{"type": "response.completed", "response": map[string]any{"id": "response-2", "output": []any{}}})
				}
				for {
					if _, _, errRead := conn.ReadMessage(); errRead != nil {
						return
					}
				}
			}))
			defer server.Close()

			dispatcher := &terminalCodexHomeDispatcher{auth: cliproxyauth.Auth{
				ID:       "home-codex",
				Provider: "codex",
				Status:   cliproxyauth.StatusActive,
				Attributes: map[string]string{
					"api_key":  "test-key",
					"base_url": server.URL,
				},
			}}
			manager := cliproxyauth.NewManager(nil, nil, nil)
			manager.SetConfig(&config.Config{Home: config.HomeConfig{Enabled: true}})
			manager.PublishHomeDispatch(dispatcher, executionregistry.New(), 1)
			manager.RegisterExecutor(NewCodexWebsocketsExecutor(&config.Config{}))

			ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
			opts := cliproxyexecutor.Options{
				Stream:         true,
				SourceFormat:   sdktranslator.FormatOpenAIResponse,
				ResponseFormat: sdktranslator.FormatOpenAIResponse,
				Metadata: map[string]any{
					cliproxyexecutor.ExecutionSessionMetadataKey: "terminal-home-session",
				},
			}
			request := cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: []byte(`{"model":"gpt-5-codex","input":[]}`)}

			first, errFirst := manager.ExecuteStream(ctx, []string{"codex"}, request, opts)
			if errFirst != nil {
				t.Fatalf("first ExecuteStream() error = %v", errFirst)
			}
			terminalDelivered := false
			for chunk := range first.Chunks {
				if bytes.Contains(chunk.Payload, []byte(`"type":"error"`)) && bytes.Contains(chunk.Payload, []byte("terminal failure")) {
					terminalDelivered = true
				}
			}
			if !terminalDelivered {
				t.Fatal("terminal error frame was not delivered")
			}

			second, errSecond := manager.ExecuteStream(ctx, []string{"codex"}, request, opts)
			if errSecond != nil {
				t.Fatalf("second ExecuteStream() error = %v", errSecond)
			}
			for range second.Chunks {
			}
			if got := dispatcher.calls.Load(); got != 2 {
				t.Fatalf("Home RPOP calls = %d, want 2 after terminal failure", got)
			}
			if got := connections.Load(); got != 2 {
				t.Fatalf("websocket connections = %d, want 2", got)
			}

			manager.CloseExecutionSession("terminal-home-session")
		})
	}
}
