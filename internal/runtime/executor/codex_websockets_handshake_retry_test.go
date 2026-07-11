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
	for _, stream := range []bool{false, true} {
		for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests} {
			t.Run(fmt.Sprintf("stream=%t/status=%d", stream, status), func(t *testing.T) {
				body := `{"error":{"type":"authentication_error","message":"retry rejected"}}`
				if status == http.StatusTooManyRequests {
					body = `{"error":{"type":"usage_limit_reached","message":"retry rejected","resets_in_seconds":120}}`
				}
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
					w.WriteHeader(status)
					if _, errWrite := w.Write([]byte(body)); errWrite != nil {
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
				if !errors.As(errExecute, &rejection) || rejection.StatusCode() != status {
					t.Fatalf("retry error = %v, want HTTP %d", errExecute, status)
				}
				if got := gjson.Get(errExecute.Error(), "error.message").String(); got != "retry rejected" {
					t.Errorf("error message = %q, want upstream rejection", got)
				}
				if status == http.StatusTooManyRequests {
					var retry interface{ RetryAfter() *time.Duration }
					if !errors.As(errExecute, &retry) || retry.RetryAfter() == nil || *retry.RetryAfter() != 120*time.Second {
						t.Errorf("retry error = %#v, want 120s cooldown", errExecute)
					}
				}
			})
		}
	}
}
