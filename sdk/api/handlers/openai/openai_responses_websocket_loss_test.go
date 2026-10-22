package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executionregistry"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
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
						if ojson.GetBytesNoCopy(payload, "type").String() != "response.create" {
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
					if gotID := ojson.GetBytesNoCopy(frame, "response.id").String(); gotID == "resp-a-buffered" {
						t.Fatalf("buffered handshake from failed credential A leaked to downstream: %s", frame)
					}
					if ojson.GetBytesNoCopy(frame, "type").String() == "response.completed" {
						completed1 = frame
						break
					}
				}
				if got := ojson.GetBytesNoCopy(completed1, "response.id").String(); got != "resp-b-1" {
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
					if ojson.GetBytesNoCopy(frame, "type").String() == "response.completed" {
						completed1b = frame
						break
					}
				}
				if got := ojson.GetBytesNoCopy(completed1b, "response.id").String(); got != "resp-b-2" {
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
					if ojson.GetBytesNoCopy(frame, "type").String() == "response.completed" {
						completed2 = frame
						break
					}
				}
				if got := ojson.GetBytesNoCopy(completed2, "response.id").String(); got != "resp-a-2" {
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

				// 4. When all eligible credentials return early 429, the downstream WebSocket receives a terminal 429 error frame before closing.
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
				msgType4, frame4, errRead4 := conn3.ReadMessage()
				if errRead4 != nil {
					t.Fatalf("expected terminal 429 error frame after exhausting credentials, got read error: %v", errRead4)
				}
				if msgType4 != websocket.TextMessage {
					t.Fatalf("frame4 msgType = %d, want TextMessage (%d)", msgType4, websocket.TextMessage)
				}
				errorBody := ojson.GetBytesNoCopy(frame4, "error")
				status := ojson.GetBytesNoCopy(frame4, "status").Int()
				switch eventType := ojson.GetBytesNoCopy(frame4, "type").String(); eventType {
				case "error":
				case "response.failed":
					if buffering || frameName != "response_failed" {
						t.Fatalf("unexpected native terminal event: %s", frame4)
					}
					errorBody = ojson.GetBytesNoCopy(frame4, "response.error")
					status = errorBody.Get("status_code").Int()
				default:
					t.Fatalf("terminal event type = %q: %s", eventType, frame4)
				}
				if status != http.StatusTooManyRequests {
					t.Fatalf("terminal status = %d, want %d: %s", status, http.StatusTooManyRequests, frame4)
				}
				if got := errorBody.Get("type").String(); got != "usage_limit_reached" {
					t.Fatalf("frame4 error.type = %q, want usage_limit_reached: %s", got, frame4)
				}
				if got := int(errorBody.Get("resets_in_seconds").Int()); got != 600 {
					t.Fatalf("frame4 error.resets_in_seconds = %d, want 600: %s", got, frame4)
				}
				// Buffered bootstrap failures carry a trusted retry header. Native
				// error events retain the upstream reset timing in the error body.
				if buffering {
					if retryAfter := ojson.GetBytesNoCopy(frame4, "headers.Retry-After").String(); retryAfter == "" {
						t.Fatalf("buffered failure requires Retry-After: %s", frame4)
					}
				}
				if _, extraFrame, errAfter := conn3.ReadMessage(); errAfter == nil {
					t.Fatalf("expected socket close after terminal 429 error frame, got frame: %s", extraFrame)
				}
				if authACreates.Load() != 4 || authBCreates.Load() != 3 {
					t.Fatalf("after all credentials exhausted: authACreates=%d authBCreates=%d, want 4 and 3", authACreates.Load(), authBCreates.Load())
				}
			})
		}
	}
}

func TestResponsesWebsocketTerminal429WhenAllCredentialsExhaustedOrCooling(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const early429Frame = `{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","plan_type":"plus","resets_in_seconds":600}}`

	var creates atomic.Int32
	var return429 atomic.Bool

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			if ojson.GetBytesNoCopy(payload, "type").String() != "response.create" {
				continue
			}
			turn := creates.Add(1)
			if return429.Load() {
				_ = c.WriteMessage(websocket.TextMessage, []byte(early429Frame))
				continue
			}
			respID := fmt.Sprintf("resp-single-%d", turn)
			_ = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"status":"in_progress","output":[]}}`, respID)))
			_ = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"status":"completed","output":[{"id":"msg-%d","type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}}`, respID, turn)))
		}
	}))
	t.Cleanup(upstream.Close)

	cfg := &config.Config{
		DisableCooling:      false,
		RequestRetry:        0,
		MaxRetryCredentials: 1,
	}
	manager := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
	manager.SetConfig(cfg)
	manager.SetRetryConfig(cfg.RequestRetry, 0, cfg.MaxRetryCredentials)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))

	modelName := "codex-single-cred-429"
	authID := "auth-single-429"
	if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"api_key":    "key-single",
			"base_url":   upstream.URL,
			"websockets": "true",
		},
	}); errRegister != nil {
		t.Fatal(errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: modelName}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	base := handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	t.Cleanup(downstream.Close)
	wsURL := "ws" + strings.TrimPrefix(downstream.URL, "http") + "/v1/responses"

	// 1. First turn succeeds and pins the single credential.
	conn1, resp1, errDial1 := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp1 != nil && resp1.Body != nil {
		defer func() {
			if errClose := resp1.Body.Close(); errClose != nil {
				t.Logf("close first handshake response body: %v", errClose)
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
	for {
		_ = conn1.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, frame, errRead := conn1.ReadMessage()
		if errRead != nil {
			t.Fatalf("request 1 failed: %v", errRead)
		}
		if ojson.GetBytesNoCopy(frame, "type").String() == "response.completed" {
			break
		}
	}

	// 2. Continuation on the pinned single credential hits 429 usage_limit_reached.
	// With no alternative credential to fail over to, it must emit a terminal 429 error frame directly instead of 1012.
	return429.Store(true)
	req2 := fmt.Sprintf(`{"type":"response.create","model":%q,"previous_response_id":"resp-single-1","input":[{"type":"message","id":"msg-2","role":"user","content":"followup"}]}`, modelName)
	if errWrite := conn1.WriteMessage(websocket.TextMessage, []byte(req2)); errWrite != nil {
		t.Fatalf("write request 2: %v", errWrite)
	}
	_ = conn1.SetReadDeadline(time.Now().Add(5 * time.Second))
	msgType2, frame2, errRead2 := conn1.ReadMessage()
	if errRead2 != nil {
		t.Fatalf("expected terminal 429 error frame on single-credential continuation, got error: %v", errRead2)
	}
	if msgType2 != websocket.TextMessage {
		t.Fatalf("frame2 msgType = %d, want TextMessage (%d)", msgType2, websocket.TextMessage)
	}
	if got := ojson.GetBytesNoCopy(frame2, "type").String(); got != "error" {
		t.Fatalf("frame2 type = %q, want error: %s", got, frame2)
	}
	if got := int(ojson.GetBytesNoCopy(frame2, "status").Int()); got != http.StatusTooManyRequests {
		t.Fatalf("frame2 status = %d, want %d: %s", got, http.StatusTooManyRequests, frame2)
	}
	if got := ojson.GetBytesNoCopy(frame2, "error.type").String(); got != "usage_limit_reached" {
		t.Fatalf("frame2 error.type = %q, want usage_limit_reached: %s", got, frame2)
	}
	if got := int(ojson.GetBytesNoCopy(frame2, "error.resets_in_seconds").Int()); got != 600 {
		t.Fatalf("frame2 error.resets_in_seconds = %d, want 600: %s", got, frame2)
	}
	if _, extraFrame, errAfter := conn1.ReadMessage(); errAfter == nil {
		t.Fatalf("expected socket close after terminal 429 error frame, got frame: %s", extraFrame)
	}

	// 3. The single credential is now cooling down. A subsequent connection must receive
	// a terminal 429 model_cooldown error frame with reset_seconds and Retry-After.
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

	req3 := fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-3","role":"user","content":"while cooling"}]}`, modelName)
	if errWrite := conn2.WriteMessage(websocket.TextMessage, []byte(req3)); errWrite != nil {
		t.Fatalf("write request 3: %v", errWrite)
	}
	_ = conn2.SetReadDeadline(time.Now().Add(5 * time.Second))
	msgType3, frame3, errRead3 := conn2.ReadMessage()
	if errRead3 != nil {
		t.Fatalf("expected terminal 429 model_cooldown error frame, got error: %v", errRead3)
	}
	if msgType3 != websocket.TextMessage {
		t.Fatalf("frame3 msgType = %d, want TextMessage (%d)", msgType3, websocket.TextMessage)
	}
	if got := ojson.GetBytesNoCopy(frame3, "type").String(); got != "error" {
		t.Fatalf("frame3 type = %q, want error: %s", got, frame3)
	}
	if got := int(ojson.GetBytesNoCopy(frame3, "status").Int()); got != http.StatusTooManyRequests {
		t.Fatalf("frame3 status = %d, want %d: %s", got, http.StatusTooManyRequests, frame3)
	}
	if got := ojson.GetBytesNoCopy(frame3, "error.code").String(); got != "model_cooldown" {
		t.Fatalf("frame3 error.code = %q, want model_cooldown: %s", got, frame3)
	}
	if got := int(ojson.GetBytesNoCopy(frame3, "error.reset_seconds").Int()); got <= 0 {
		t.Fatalf("frame3 error.reset_seconds = %d, want > 0: %s", got, frame3)
	}
	if got := ojson.GetBytesNoCopy(frame3, "headers.Retry-After").String(); got == "" {
		t.Fatalf("frame3 headers.Retry-After is empty: %s", frame3)
	}
	if _, extraFrame, errAfter := conn2.ReadMessage(); errAfter == nil {
		t.Fatalf("expected socket close after terminal 429 model_cooldown frame, got frame: %s", extraFrame)
	}
	if creates.Load() != 2 {
		t.Fatalf("upstream creates = %d, want 2 (cooling credential must not be called again)", creates.Load())
	}
}

func TestResponsesWebsocketPinned429ExposesErrorWhenOnlyAlternativeHasExpiredToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const modelName = "codex-expired-alt-429"
	upstreamUpgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upstreamUpgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		for turn := 1; ; turn++ {
			_, raw, errRead := conn.ReadMessage()
			if errRead != nil {
				return
			}
			if ojson.GetBytesNoCopy(raw, "type").String() != "response.create" {
				return
			}
			if turn == 1 {
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[]}}`))
				continue
			}
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","plan_type":"plus","resets_in_seconds":600}}`))
			return
		}
	}))
	defer upstream.Close()

	cfg := &config.Config{
		SDKConfig:           config.SDKConfig{PassthroughHeaders: true},
		RequestRetry:        0,
		MaxRetryCredentials: 0,
		QuotaExceeded: config.QuotaExceeded{
			SwitchProject:      true,
			SwitchPreviewModel: true,
		},
	}
	manager := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
	manager.SetConfig(cfg)
	manager.SetRetryConfig(cfg.RequestRetry, 0, cfg.MaxRetryCredentials)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))

	authA := &coreauth.Auth{
		ID:       "auth-active-429",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"api_key":    "key-active",
			"base_url":   upstream.URL,
			"websockets": "true",
		},
	}
	authB := &coreauth.Auth{
		ID:       "auth-expired-alt",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"api_key":    "key-expired",
			"base_url":   upstream.URL,
			"websockets": "true",
		},
		Metadata: map[string]any{
			"access_token": "expired-token",
			"expired":      time.Now().Add(-time.Hour).Format(time.RFC3339),
		},
	}
	for _, a := range []*coreauth.Auth{authA, authB} {
		if _, errReg := manager.Register(context.Background(), a); errReg != nil {
			t.Fatalf("register auth %s: %v", a.ID, errReg)
		}
		registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: modelName}})
	}
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(authA.ID)
		registry.GetGlobalRegistry().UnregisterClient(authB.ID)
	})

	baseHandler := handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager)
	handler := NewOpenAIResponsesAPIHandler(baseHandler)
	router := gin.New()
	router.GET("/v1/responses", handler.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	defer downstream.Close()

	wsURL := "ws" + strings.TrimPrefix(downstream.URL, "http") + "/v1/responses"
	conn, resp, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil && resp.Body != nil {
		defer func() {
			_ = resp.Body.Close()
		}()
	}
	if errDial != nil {
		t.Fatalf("dial downstream websocket: %v", errDial)
	}
	defer func() {
		_ = conn.Close()
	}()

	req1 := fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-1","role":"user","content":"turn 1"}]}`, modelName)
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(req1)); errWrite != nil {
		t.Fatalf("write request 1: %v", errWrite)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, frame1, errRead1 := conn.ReadMessage(); errRead1 != nil || ojson.GetBytesNoCopy(frame1, "type").String() != "response.completed" {
		t.Fatalf("read request 1: frame=%s err=%v", frame1, errRead1)
	}

	req2 := fmt.Sprintf(`{"type":"response.create","model":%q,"previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2","role":"user","content":"turn 2"}]}`, modelName)
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(req2)); errWrite != nil {
		t.Fatalf("write request 2: %v", errWrite)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, frame2, errRead2 := conn.ReadMessage()
	if errRead2 != nil {
		t.Fatalf("expected terminal 429 error frame when only alternative credential is expired, got error: %v", errRead2)
	}
	if got := ojson.GetBytesNoCopy(frame2, "type").String(); got != "error" {
		t.Fatalf("frame2 type = %q, want error: %s", got, frame2)
	}
	if got := int(ojson.GetBytesNoCopy(frame2, "status").Int()); got != http.StatusTooManyRequests {
		t.Fatalf("frame2 status = %d, want 429: %s", got, frame2)
	}
	if got := ojson.GetBytesNoCopy(frame2, "error.type").String(); got != "usage_limit_reached" {
		t.Fatalf("frame2 error.type = %q, want usage_limit_reached: %s", got, frame2)
	}
}

type homeCallbackStreamExecutor struct {
	provider string
	streamFn func(ctx context.Context, auth *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error)
}

func (e *homeCallbackStreamExecutor) Identifier() string {
	if e != nil && strings.TrimSpace(e.provider) != "" {
		return strings.TrimSpace(e.provider)
	}
	return "codex"
}

func (*homeCallbackStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *homeCallbackStreamExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return e.streamFn(ctx, auth, req, opts)
}

func (*homeCallbackStreamExecutor) Refresh(context.Context, *coreauth.Auth) (*coreauth.Auth, error) {
	return nil, errors.New("not implemented")
}

func (*homeCallbackStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (*homeCallbackStreamExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func TestResponsesWebsocketHomePinned429ExposesErrorWhenNoAlternativeAvailable(t *testing.T) {
	gin.SetMode(gin.TestMode)

	dispatcher := &homeResponsesWebsocketDispatcher{}
	var turnCount atomic.Int32
	executor := &homeCallbackStreamExecutor{
		provider: "codex",
		streamFn: func(_ context.Context, _ *coreauth.Auth, _ coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
			if turnCount.Add(1) == 1 {
				if lifecycle, ok := opts.ExecutionLifecycle.(interface{ Retain() }); ok {
					lifecycle.Retain()
				}
				chunks := make(chan coreexecutor.StreamChunk, 1)
				chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.completed","response":{"id":"resp-home-1","output":[]}}`)}
				close(chunks)
				return &coreexecutor.StreamResult{Chunks: chunks}, nil
			}
			return nil, websocketPinnedFailoverStatusError{
				status: http.StatusTooManyRequests,
				msg:    `{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","plan_type":"team","resets_in_seconds":600}}`,
			}
		},
	}

	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(&config.Config{Home: config.HomeConfig{Enabled: true}})
	manager.PublishHomeDispatch(dispatcher, executionregistry.New(), 1)
	manager.RegisterExecutor(executor)
	registry.GetGlobalRegistry().RegisterClient("home-responses-websocket-auth", "codex", []*registry.ModelInfo{{ID: "gpt-5.4"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient("home-responses-websocket-auth")
	})

	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, resp, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
	if resp != nil && resp.Body != nil {
		defer func() {
			_ = resp.Body.Close()
		}()
	}
	if errDial != nil {
		t.Fatalf("dial websocket: %v", errDial)
	}
	defer func() {
		_ = conn.Close()
	}()

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5.4","input":[]}`)); errWrite != nil {
		t.Fatalf("write turn 1: %v", errWrite)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, frame1, errRead1 := conn.ReadMessage(); errRead1 != nil || ojson.GetBytesNoCopy(frame1, "type").String() != "response.completed" {
		t.Fatalf("read turn 1: frame=%s err=%v", frame1, errRead1)
	}

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5.4","previous_response_id":"resp-home-1","input":[]}`)); errWrite != nil {
		t.Fatalf("write turn 2: %v", errWrite)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, frame2, errRead2 := conn.ReadMessage()
	if errRead2 != nil {
		t.Fatalf("expected terminal 429 error frame on single-credential Home continuation, got error: %v", errRead2)
	}
	if got := ojson.GetBytesNoCopy(frame2, "type").String(); got != "error" {
		t.Fatalf("frame2 type = %q, want error: %s", got, frame2)
	}
	if got := int(ojson.GetBytesNoCopy(frame2, "status").Int()); got != http.StatusTooManyRequests {
		t.Fatalf("frame2 status = %d, want 429: %s", got, frame2)
	}
	if got := ojson.GetBytesNoCopy(frame2, "error.type").String(); got != "usage_limit_reached" {
		t.Fatalf("frame2 error.type = %q, want usage_limit_reached: %s", got, frame2)
	}
}

type authHeaderCheckingHomeDispatcher struct {
	mu           sync.Mutex
	requiredAuth string
	authIDs      []string
}

func (*authHeaderCheckingHomeDispatcher) HeartbeatOK() bool       { return true }
func (*authHeaderCheckingHomeDispatcher) AbortAmbiguousDispatch() {}

func (d *authHeaderCheckingHomeDispatcher) RPopAuth(ctx context.Context, model string, sessionID string, headers http.Header, count int) ([]byte, error) {
	return d.RPopAuthWithConstraints(ctx, model, sessionID, headers, count, nil, "")
}

func (d *authHeaderCheckingHomeDispatcher) RPopAuthWithConstraints(_ context.Context, _ string, _ string, headers http.Header, _ int, excludedAuthIDs []string, pinnedAuthID string) ([]byte, error) {
	if strings.TrimSpace(headers.Get("Authorization")) != d.requiredAuth {
		return []byte(`{"error":{"code":"auth_not_found","message":"missing or invalid client authorization header"}}`), nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	excluded := make(map[string]struct{}, len(excludedAuthIDs))
	for _, id := range excludedAuthIDs {
		excluded[id] = struct{}{}
	}
	for _, id := range d.authIDs {
		if pinnedAuthID != "" && id != pinnedAuthID {
			continue
		}
		if _, skip := excluded[id]; skip {
			continue
		}
		return json.Marshal(coreauth.Auth{
			ID:       id,
			Provider: "codex",
			Status:   coreauth.StatusActive,
			Attributes: map[string]string{
				"websockets": "true",
			},
		})
	}
	return []byte(`{"error":{"code":"auth_not_found","message":"no more auths"}}`), nil
}

func TestResponsesWebsocketHomePinned429ReplaysWith1012WhenAuthenticatedAlternativeExists(t *testing.T) {
	gin.SetMode(gin.TestMode)

	dispatcher := &authHeaderCheckingHomeDispatcher{
		requiredAuth: "Bearer client-secret-key",
		authIDs:      []string{"home-auth-a", "home-auth-b"},
	}
	var turnCount atomic.Int32
	executor := &homeCallbackStreamExecutor{
		provider: "codex",
		streamFn: func(_ context.Context, auth *coreauth.Auth, _ coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
			if turnCount.Add(1) == 1 {
				if lifecycle, ok := opts.ExecutionLifecycle.(interface{ Retain() }); ok {
					lifecycle.Retain()
				}
				chunks := make(chan coreexecutor.StreamChunk, 1)
				chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.completed","response":{"id":"resp-home-1","output":[]}}`)}
				close(chunks)
				return &coreexecutor.StreamResult{Chunks: chunks}, nil
			}
			if auth != nil && auth.ID == "home-auth-a" {
				return nil, websocketPinnedFailoverStatusError{
					status: http.StatusTooManyRequests,
					msg:    `{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","plan_type":"team","resets_in_seconds":600}}`,
				}
			}
			chunks := make(chan coreexecutor.StreamChunk, 1)
			chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.completed","response":{"id":"resp-home-2","output":[]}}`)}
			close(chunks)
			return &coreexecutor.StreamResult{Chunks: chunks}, nil
		},
	}

	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(&config.Config{Home: config.HomeConfig{Enabled: true}})
	manager.PublishHomeDispatch(dispatcher, executionregistry.New(), 1)
	manager.RegisterExecutor(executor)
	for _, id := range []string{"home-auth-a", "home-auth-b"} {
		registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "gpt-5.4"}})
	}
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient("home-auth-a")
		registry.GetGlobalRegistry().UnregisterClient("home-auth-b")
	})

	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	dialHeaders := http.Header{"Authorization": {"Bearer client-secret-key"}}
	conn, resp, errDial := websocket.DefaultDialer.Dial(wsURL, dialHeaders)
	if resp != nil && resp.Body != nil {
		defer func() {
			_ = resp.Body.Close()
		}()
	}
	if errDial != nil {
		t.Fatalf("dial websocket: %v", errDial)
	}
	defer func() {
		_ = conn.Close()
	}()

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5.4","input":[]}`)); errWrite != nil {
		t.Fatalf("write turn 1: %v", errWrite)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, frame1, errRead1 := conn.ReadMessage(); errRead1 != nil || ojson.GetBytesNoCopy(frame1, "type").String() != "response.completed" {
		t.Fatalf("read turn 1: frame=%s err=%v", frame1, errRead1)
	}

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5.4","previous_response_id":"resp-home-1","input":[]}`)); errWrite != nil {
		t.Fatalf("write turn 2: %v", errWrite)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, frame2, errRead2 := conn.ReadMessage()
	if errRead2 == nil {
		t.Fatalf("expected 1012 replay close when authenticated Home alternative exists, got frame: %s", frame2)
	}
	if !websocket.IsCloseError(errRead2, websocket.CloseServiceRestart) {
		t.Fatalf("turn 2 close error = %v, want 1012 ServiceRestart", errRead2)
	}
}

type websocketHintTestError struct {
	status int
	msg    string
	hint   *time.Duration
}

func (e websocketHintTestError) Error() string              { return e.msg }
func (e websocketHintTestError) StatusCode() int            { return e.status }
func (e websocketHintTestError) RetryAfter() *time.Duration { return e.hint }

type websocketTwoAuthHomeDispatcher struct {
	mu      sync.Mutex
	authIDs []string
}

func (*websocketTwoAuthHomeDispatcher) HeartbeatOK() bool       { return true }
func (*websocketTwoAuthHomeDispatcher) AbortAmbiguousDispatch() {}

func (d *websocketTwoAuthHomeDispatcher) RPopAuth(ctx context.Context, model string, sessionID string, headers http.Header, count int) ([]byte, error) {
	return d.RPopAuthWithConstraints(ctx, model, sessionID, headers, count, nil, "")
}

func (d *websocketTwoAuthHomeDispatcher) RPopAuthWithConstraints(_ context.Context, _ string, _ string, _ http.Header, _ int, excludedAuthIDs []string, _ string) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	excluded := make(map[string]struct{}, len(excludedAuthIDs))
	for _, id := range excludedAuthIDs {
		excluded[id] = struct{}{}
	}
	for _, id := range d.authIDs {
		if _, skip := excluded[id]; skip {
			continue
		}
		return json.Marshal(coreauth.Auth{
			ID:       id,
			Provider: "codex",
			Status:   coreauth.StatusActive,
		})
	}
	return []byte(`{"error":{"code":"request_retry_exceeded","message":"no more auths"}}`), nil
}

func TestBuildResponsesWebsocketErrorPayloadSuppressesStaleRetryAfterWhenTrustedDeadlineHasNoHeader(t *testing.T) {
	zero := time.Duration(0)
	positive := 256 * time.Second
	dispatcher := &websocketTwoAuthHomeDispatcher{authIDs: []string{"home-a", "home-b"}}
	executor := &homeCallbackStreamExecutor{
		provider: "codex",
		streamFn: func(_ context.Context, auth *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
			hint := &positive
			if auth != nil && auth.ID == "home-a" {
				hint = &zero
			}
			chunks := make(chan coreexecutor.StreamChunk, 1)
			chunks <- coreexecutor.StreamChunk{Err: websocketHintTestError{
				status: http.StatusTooManyRequests,
				msg:    `{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached"}}`,
				hint:   hint,
			}}
			close(chunks)
			return &coreexecutor.StreamResult{
				Headers: http.Header{"Retry-After": {"30"}, "X-Request-Id": {"req-1"}},
				Chunks:  chunks,
			}, nil
		},
	}

	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(&config.Config{Home: config.HomeConfig{Enabled: true}})
	manager.SetRetryConfig(0, time.Second, 2)
	manager.PublishHomeDispatch(dispatcher, executionregistry.New(), 1)
	manager.RegisterExecutor(executor)

	result, errExec := manager.ExecuteStream(context.Background(), []string{"codex"}, coreexecutor.Request{Model: "gpt-5.4"}, coreexecutor.Options{Stream: true})
	if errExec != nil {
		t.Fatalf("ExecuteStream() error = %v", errExec)
	}
	chunk, ok := <-result.Chunks
	if !ok || chunk.Err == nil {
		t.Fatalf("stream chunk = %#v, %t; want error", chunk, ok)
	}
	if !coreauth.HasTrustedRetryDeadline(chunk.Err) {
		t.Fatal("expected HasTrustedRetryDeadline(chunk.Err) = true")
	}
	if got := coreauth.SafeResponseHeaders(chunk.Err).Get("Retry-After"); got != "" {
		t.Fatalf("expected empty SafeResponseHeaders Retry-After, got %q", got)
	}

	payload, errBuild := buildResponsesWebsocketErrorPayload(&interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      chunk.Err,
		Addon:      http.Header{"Retry-After": {"30"}, "X-Request-Id": {"req-1"}},
	})
	if errBuild != nil {
		t.Fatalf("buildResponsesWebsocketErrorPayload() error = %v", errBuild)
	}
	if got := ojson.GetBytesNoCopy(payload, "headers.Retry-After").String(); got != "" {
		t.Fatalf("stale Addon Retry-After leaked into websocket error payload: %q (%s)", got, payload)
	}
	if got := ojson.GetBytesNoCopy(payload, "headers.X-Request-Id").String(); got != "req-1" {
		t.Fatalf("headers.X-Request-Id = %q, want req-1 (%s)", got, payload)
	}
}
