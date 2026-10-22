package openai

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

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestResponsesWebsocketEarly429RetriesAnotherCredentialWithoutClosingDownstream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	early429Frames := map[string]string{
		"error_frame":     `{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","plan_type":"plus","resets_in_seconds":600}}`,
		"response_failed": `{"type":"response.failed","response":{"error":{"type":"usage_limit_reached","status_code":429,"message":"The usage limit has been reached","plan_type":"plus","resets_in_seconds":600}}}`,
	}

	for frameName, early429Frame := range early429Frames {
		for _, buffering := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/buffering=%t", frameName, buffering), func(t *testing.T) {
				var authAConns, authBConns atomic.Int32
				var authACreates, authBCreates atomic.Int32
				var authA429Next, authB429Next atomic.Bool
				authA429Next.Store(true)

				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
					switch authHeader {
					case "Bearer key-a":
						authAConns.Add(1)
					case "Bearer key-b":
						authBConns.Add(1)
					default:
						t.Errorf("unexpected Authorization header: %q", authHeader)
						http.Error(w, "unauthorized", http.StatusUnauthorized)
						return
					}

					c, errUpgrade := (&websocket.Upgrader{}).Upgrade(w, r, nil)
					if errUpgrade != nil {
						t.Error(errUpgrade)
						return
					}
					defer func() {
						if errClose := c.Close(); errClose != nil {
							t.Logf("close upstream websocket: %v", errClose)
						}
					}()
					_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))

					for {
						_, payload, errRead := c.ReadMessage()
						if errRead != nil {
							return
						}
						if gjson.GetBytes(payload, "type").String() != "response.create" {
							continue
						}
						if authHeader == "Bearer key-a" {
							turn := authACreates.Add(1)
							if authA429Next.Load() {
								if buffering && turn == 1 {
									_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"resp-a-buffered","status":"in_progress","output":[]}}`))
									_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.in_progress","response":{"id":"resp-a-buffered","status":"in_progress","output":[]}}`))
								}
								if errWrite := c.WriteMessage(websocket.TextMessage, []byte(early429Frame)); errWrite != nil {
									return
								}
								continue
							}
							respID := fmt.Sprintf("resp-a-%d", turn)
							_ = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"status":"in_progress","output":[]}}`, respID)))
							_ = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"status":"completed","output":[{"id":"msg-a-%d","type":"message","role":"assistant","content":[{"type":"output_text","text":"from-a"}]}]}}`, respID, turn)))
							continue
						}
						turn := authBCreates.Add(1)
						if authB429Next.Load() {
							_ = c.WriteMessage(websocket.TextMessage, []byte(early429Frame))
							continue
						}
						respID := fmt.Sprintf("resp-b-%d", turn)
						_ = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"status":"in_progress","output":[]}}`, respID)))
						_ = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"status":"completed","output":[{"id":"msg-b-%d","type":"message","role":"assistant","content":[{"type":"output_text","text":"from-b"}]}]}}`, respID, turn)))
					}
				}))
				t.Cleanup(upstream.Close)

				cfg := &config.Config{
					DisableCooling:      true,
					RequestRetry:        0,
					MaxRetryCredentials: 2,
					Codex: config.CodexConfig{
						StreamBootstrapBuffering: buffering,
					},
				}
				manager := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
				manager.SetConfig(cfg)
				manager.SetRetryConfig(cfg.RequestRetry, 0, cfg.MaxRetryCredentials)
				manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))

				modelName := "codex-early-429-" + strings.ReplaceAll(t.Name(), "/", "-")
				authAID := "auth-a-" + modelName
				authBID := "auth-b-" + modelName
				for _, spec := range []struct {
					id       string
					key      string
					priority string
				}{
					{id: authAID, key: "key-a", priority: "4"},
					{id: authBID, key: "key-b", priority: "3"},
				} {
					if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{
						ID:       spec.id,
						Provider: "codex",
						Status:   coreauth.StatusActive,
						Attributes: map[string]string{
							"api_key":    spec.key,
							"base_url":   upstream.URL,
							"websockets": "true",
							"priority":   spec.priority,
						},
						Metadata: map[string]any{
							"disable_cooling": true,
						},
					}); errRegister != nil {
						t.Fatal(errRegister)
					}
					registry.GetGlobalRegistry().RegisterClient(spec.id, "codex", []*registry.ModelInfo{{ID: modelName}})
					id := spec.id
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				}

				base := handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager)
				h := NewOpenAIResponsesAPIHandler(base)
				router := gin.New()
				router.GET("/v1/responses", h.ResponsesWebsocket)
				downstream := httptest.NewServer(router)
				t.Cleanup(downstream.Close)
				wsURL := "ws" + strings.TrimPrefix(downstream.URL, "http") + "/v1/responses"

				// 1. Request 1 selects auth-a first, receives early 429, fails over to auth-b
				// within the same downstream WebSocket request, and completes without closing downstream.
				conn1, resp1, errDial1 := websocket.DefaultDialer.Dial(wsURL, nil)
				if resp1 != nil && resp1.Body != nil {
					defer func() {
						if errClose := resp1.Body.Close(); errClose != nil {
							t.Logf("close handshake response body: %v", errClose)
						}
					}()
				}
				if errDial1 != nil {
					t.Fatalf("dial first downstream websocket: %v", errDial1)
				}
				defer func() {
					if errClose := conn1.Close(); errClose != nil {
						t.Logf("close first downstream websocket: %v", errClose)
					}
				}()

				req1 := fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-1","role":"user","content":"hello"}]}`, modelName)
				if errWrite := conn1.WriteMessage(websocket.TextMessage, []byte(req1)); errWrite != nil {
					t.Fatalf("write request 1: %v", errWrite)
				}

				var completed1 []byte
				for {
					_ = conn1.SetReadDeadline(time.Now().Add(5 * time.Second))
					_, frame, errRead := conn1.ReadMessage()
					if errRead != nil {
						t.Fatalf("request 1 downstream websocket closed before completion: %v (authACreates=%d authBCreates=%d)", errRead, authACreates.Load(), authBCreates.Load())
					}
					if gotID := gjson.GetBytes(frame, "response.id").String(); gotID == "resp-a-buffered" {
						t.Fatalf("buffered handshake from failed credential A leaked to downstream: %s", frame)
					}
					if gjson.GetBytes(frame, "type").String() == "response.completed" {
						completed1 = frame
						break
					}
				}
				if got := gjson.GetBytes(completed1, "response.id").String(); got != "resp-b-1" {
					t.Fatalf("request 1 completed response.id = %q, want resp-b-1; frame=%s", got, completed1)
				}
				if authACreates.Load() != 1 || authBCreates.Load() != 1 {
					t.Fatalf("after request 1: authACreates=%d authBCreates=%d, want 1 and 1", authACreates.Load(), authBCreates.Load())
				}

				followup1 := fmt.Sprintf(`{"type":"response.create","model":%q,"previous_response_id":"resp-b-1","input":[{"type":"message","id":"msg-1b","role":"user","content":"followup on b"}]}`, modelName)
				if errWrite := conn1.WriteMessage(websocket.TextMessage, []byte(followup1)); errWrite != nil {
					t.Fatalf("write followup on conn1: %v", errWrite)
				}
				var completed1b []byte
				for {
					_ = conn1.SetReadDeadline(time.Now().Add(5 * time.Second))
					_, frame, errRead := conn1.ReadMessage()
					if errRead != nil {
						t.Fatalf("followup on conn1 closed before completion: %v", errRead)
					}
					if gjson.GetBytes(frame, "type").String() == "response.completed" {
						completed1b = frame
						break
					}
				}
				if got := gjson.GetBytes(completed1b, "response.id").String(); got != "resp-b-2" {
					t.Fatalf("followup on conn1 response.id = %q, want resp-b-2; frame=%s", got, completed1b)
				}
				if authBConns.Load() != 1 || authBCreates.Load() != 2 {
					t.Fatalf("after followup on conn1: authBConns=%d authBCreates=%d, want 1 and 2", authBConns.Load(), authBCreates.Load())
				}

				// 2. A subsequent independent request can still select auth-a when cooling is disabled.
				authA429Next.Store(false)
				conn2, resp2, errDial2 := websocket.DefaultDialer.Dial(wsURL, nil)
				if resp2 != nil && resp2.Body != nil {
					defer func() {
						if errClose := resp2.Body.Close(); errClose != nil {
							t.Logf("close second handshake response body: %v", errClose)
						}
					}()
				}
				if errDial2 != nil {
					t.Fatalf("dial second downstream websocket: %v", errDial2)
				}
				defer func() {
					if errClose := conn2.Close(); errClose != nil {
						t.Logf("close second downstream websocket: %v", errClose)
					}
				}()

				req2 := fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-2","role":"user","content":"independent"}]}`, modelName)
				if errWrite := conn2.WriteMessage(websocket.TextMessage, []byte(req2)); errWrite != nil {
					t.Fatalf("write request 2: %v", errWrite)
				}
				var completed2 []byte
				for {
					_ = conn2.SetReadDeadline(time.Now().Add(5 * time.Second))
					_, frame, errRead := conn2.ReadMessage()
					if errRead != nil {
						t.Fatalf("request 2 downstream websocket closed before completion: %v", errRead)
					}
					if gjson.GetBytes(frame, "type").String() == "response.completed" {
						completed2 = frame
						break
					}
				}
				if got := gjson.GetBytes(completed2, "response.id").String(); got != "resp-a-2" {
					t.Fatalf("request 2 completed response.id = %q, want resp-a-2; frame=%s", got, completed2)
				}
				if authACreates.Load() != 2 || authBCreates.Load() != 2 {
					t.Fatalf("after request 2: authACreates=%d authBCreates=%d, want 2 and 2", authACreates.Load(), authBCreates.Load())
				}

				// 3. A continuation with previous_response_id on the pinned connection refuses cross-credential replay.
				authA429Next.Store(true)
				req3 := fmt.Sprintf(`{"type":"response.create","model":%q,"previous_response_id":"resp-a-2","input":[{"type":"message","id":"msg-3","role":"user","content":"continuation"}]}`, modelName)
				if errWrite := conn2.WriteMessage(websocket.TextMessage, []byte(req3)); errWrite != nil {
					t.Fatalf("write request 3: %v", errWrite)
				}
				_ = conn2.SetReadDeadline(time.Now().Add(5 * time.Second))
				_, _, errReadClose := conn2.ReadMessage()
				var closeErr *websocket.CloseError
				if !errors.As(errReadClose, &closeErr) || closeErr.Code != websocket.CloseServiceRestart || closeErr.Text != wsHTTPReplayRequiredCloseReason {
					t.Fatalf("continuation failure close = %v, want %d %q", errReadClose, websocket.CloseServiceRestart, wsHTTPReplayRequiredCloseReason)
				}
				if authACreates.Load() != 3 || authBCreates.Load() != 2 {
					t.Fatalf("after continuation failure: authACreates=%d authBCreates=%d, want 3 and 2 (must not replay previous_response_id on auth-b)", authACreates.Load(), authBCreates.Load())
				}

				// 4. When all eligible credentials return early 429, the downstream WebSocket closes cleanly after exhausting retries.
				authA429Next.Store(true)
				authB429Next.Store(true)
				conn3, resp3, errDial3 := websocket.DefaultDialer.Dial(wsURL, nil)
				if resp3 != nil && resp3.Body != nil {
					defer func() {
						if errClose := resp3.Body.Close(); errClose != nil {
							t.Logf("close third handshake response body: %v", errClose)
						}
					}()
				}
				if errDial3 != nil {
					t.Fatalf("dial third downstream websocket: %v", errDial3)
				}
				defer func() {
					if errClose := conn3.Close(); errClose != nil {
						t.Logf("close third downstream websocket: %v", errClose)
					}
				}()
				req4 := fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-4","role":"user","content":"exhausted"}]}`, modelName)
				if errWrite := conn3.WriteMessage(websocket.TextMessage, []byte(req4)); errWrite != nil {
					t.Fatalf("write request 4: %v", errWrite)
				}
				_ = conn3.SetReadDeadline(time.Now().Add(5 * time.Second))
				_, frame4, errRead4 := conn3.ReadMessage()
				var closeErr4 *websocket.CloseError
				if !errors.As(errRead4, &closeErr4) || closeErr4.Code != websocket.CloseAbnormalClosure {
					t.Fatalf("exhausted credentials close = %v (frame=%s), want CloseAbnormalClosure (%d)", errRead4, frame4, websocket.CloseAbnormalClosure)
				}
				if authACreates.Load() != 4 || authBCreates.Load() != 3 {
					t.Fatalf("after all credentials exhausted: authACreates=%d authBCreates=%d, want 4 and 3", authACreates.Load(), authBCreates.Load())
				}
			})
		}
	}
}
