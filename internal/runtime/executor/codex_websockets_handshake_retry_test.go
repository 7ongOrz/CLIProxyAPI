package executor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexWebsocketSendRetryPreservesHandshakeRejection(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       string
		retryAfter string
		wantRetry  time.Duration
	}{
		{
			name: "authentication", status: http.StatusUnauthorized,
			body: `{"error":{"type":"authentication_error","message":"retry rejected"}}`,
		},
		{
			name: "quota", status: http.StatusTooManyRequests,
			body:       `{"error":{"type":"usage_limit_reached","message":"retry rejected","resets_in_seconds":120}}`,
			retryAfter: "30", wantRetry: 120 * time.Second,
		},
		{
			name: "turn capacity", status: http.StatusTooManyRequests,
			body:       `{"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"retry rejected"}}`,
			retryAfter: "256", wantRetry: 256 * time.Second,
		},
	}
	for _, stream := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("stream=%t/%s", stream, tc.name), func(t *testing.T) {
				var handshakes atomic.Int32
				upgrader := websocket.Upgrader{}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if handshakes.Add(1) == 1 {
						conn, errUpgrade := upgrader.Upgrade(w, r, nil)
						if errUpgrade != nil {
							t.Errorf("upgrade initial connection: %v", errUpgrade)
							return
						}
						defer func() { _ = conn.Close() }()
						_, _, _ = conn.ReadMessage()
						return
					}
					w.Header().Set("Content-Type", "application/json")
					if tc.retryAfter != "" {
						w.Header().Set("Retry-After", tc.retryAfter)
					}
					w.WriteHeader(tc.status)
					if _, errWrite := w.Write([]byte(tc.body)); errWrite != nil {
						t.Errorf("write handshake rejection: %v", errWrite)
					}
				}))
				defer server.Close()

				wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/responses"
				staleConn, response, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
				closeHTTPResponseBody(response, "test handshake")
				if errDial != nil {
					t.Fatalf("dial initial connection: %v", errDial)
				}
				if errClose := staleConn.Close(); errClose != nil {
					t.Fatalf("close initial connection: %v", errClose)
				}

				executor := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
				sess := &codexWebsocketSession{
					sessionID: "retry-rejection", conn: staleConn,
					connCloser: newWebsocketConnectionCloser(staleConn), readerConn: staleConn,
					authID: "auth", wsURL: wsURL,
				}
				executor.store = &codexWebsocketSessionStore{sessions: map[string]*codexWebsocketSession{sess.sessionID: sess}}
				defer executor.CloseExecutionSession(sess.sessionID)
				auth := &cliproxyauth.Auth{ID: "auth", Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL}}
				req := cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: []byte(`{"model":"gpt-5-codex","input":[]}`)}
				opts := cliproxyexecutor.Options{
					SourceFormat: sdktranslator.FormatOpenAIResponse,
					Metadata:     map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: sess.sessionID},
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				var errExecute error
				if stream {
					_, errExecute = executor.ExecuteStream(ctx, auth, req, opts)
				} else {
					_, errExecute = executor.Execute(ctx, auth, req, opts)
				}
				if got := handshakes.Load(); got != 2 {
					t.Fatalf("handshakes = %d, want initial connection and retry", got)
				}
				var rejection interface{ StatusCode() int }
				if !errors.As(errExecute, &rejection) || rejection.StatusCode() != tc.status {
					t.Fatalf("retry error = %v, want HTTP %d", errExecute, tc.status)
				}
				if got := gjson.Get(errExecute.Error(), "error.message").String(); got != "retry rejected" {
					t.Errorf("error message = %q, want upstream rejection", got)
				}
				if tc.status == http.StatusTooManyRequests {
					var retry interface{ RetryAfter() *time.Duration }
					if !errors.As(errExecute, &retry) || retry.RetryAfter() == nil || *retry.RetryAfter() != tc.wantRetry {
						t.Errorf("retry error = %#v, want %v cooldown", errExecute, tc.wantRetry)
					}
				}
			})
		}
	}
}
