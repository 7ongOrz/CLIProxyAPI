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
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	auth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	translator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexDuplexLaterCredentialFailure(t *testing.T) {
	for _, kind := range []string{"error", "response.failed"} {
		for _, status := range []int{401, 402, 403, 429} {
			for _, scenario := range []struct {
				name      string
				queued    bool
				ambiguous bool
			}{
				{name: "current"},
				{name: "queued", queued: true},
				{name: "ambiguous", queued: true, ambiguous: true},
			} {
				t.Run(fmt.Sprintf("%s/%d/%s", kind, status, scenario.name), func(t *testing.T) {
					queued := scenario.queued
					// Native Codex cache keys stay stable across creates on the execution session.
					wantKey := helps.ProviderSessionUUID("codex", map[string]any{core.ExecutionSessionMetadataKey: t.Name()})
					var attempts atomic.Int32
					done := make(chan struct{})
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						defer close(done)
						attempts.Add(1)
						c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = c.Close() }()
						_ = c.SetReadDeadline(time.Now().Add(8 * time.Second))
						_, request, err := c.ReadMessage()
						if err != nil {
							t.Error(err)
							return
						}
						write := func(p string) {
							if e := c.WriteMessage(websocket.TextMessage, []byte(p)); e != nil {
								t.Error(e)
							}
						}
						write(`{"type":"response.created","response":{"id":"started","output":[]}}`)
						failedID := "started"
						if queued {
							_, request, err = c.ReadMessage()
							if err != nil {
								t.Error(err)
								return
							}
							failedID = "queued"
						} else {
							write(`{"type":"response.completed","response":{"id":"started","output":[]}}`)
						}
						if scenario.ambiguous {
							failedID = ""
						}
						errorType := "authentication_error"
						if status == 402 {
							errorType = "payment_required"
						}
						if status == 403 {
							errorType = "permission_error"
						}
						if status == 429 {
							errorType = "usage_limit_reached"
						}
						key := gjson.GetBytes(request, "prompt_cache_key").String()
						errorBody := fmt.Sprintf(`{"type":%q,"status":%d,"message":%q,"resets_in_seconds":3600}`, errorType, status, "credential rejected for "+key)
						if kind == "error" {
							write(fmt.Sprintf(`{"type":"error","response_id":%q,"status":%d,"headers":{"X-Request-Id":"later-rejection"},"error":%s}`, failedID, status, errorBody))
						} else {
							write(fmt.Sprintf(`{"type":"response.failed","response":{"id":%q,"error":%s}}`, failedID, errorBody))
						}
						// The proxy must terminate this still-open socket on its own.
						_, _, _ = c.ReadMessage()
					}))
					defer upstream.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					input := make(chan core.WebsocketInput, 1)
					ctx = core.WithWebsocketInput(core.WithDownstreamWebsocket(ctx), input)
					cfg := &config.Config{}
					cfg.Codex.ResponseSteering = true
					cfg.CodexResponseSteering = true
					cfg.Routing.SessionAffinity = true
					executor := NewCodexWebsocketsExecutor(cfg)
					executor.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
					manager := auth.NewManager(nil, &auth.FillFirstSelector{}, nil)
					manager.SetConfig(cfg)
					manager.SetRetryConfig(3, 30*time.Second, 0)
					manager.RegisterExecutor(executor)
					model := "later-credential-model"
					bad := &auth.Auth{ID: t.Name() + "-bad", Provider: "codex", Status: auth.StatusActive, Attributes: map[string]string{"api_key": "test", "base_url": upstream.URL, "websockets": "true", "priority": "4"}}
					good := &auth.Auth{ID: t.Name() + "-good", Provider: "codex", Status: auth.StatusActive, Attributes: map[string]string{"api_key": "unused", "base_url": upstream.URL, "websockets": "true", "priority": "3"}}
					for _, candidate := range []*auth.Auth{bad, good} {
						registry.GetGlobalRegistry().RegisterClient(candidate.ID, "codex", []*registry.ModelInfo{{ID: model}})
						t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(candidate.ID) })
						if _, err := manager.Register(ctx, candidate); err != nil {
							t.Fatal(err)
						}
					}
					req := core.Request{Model: model, Payload: []byte(`{"model":"later-credential-model","prompt_cache_key":"first-client-key","input":[]}`)}
					opts := core.Options{SourceFormat: translator.FromString("codex"), Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name()}}
					before := time.Now()
					result, err := manager.ExecuteStream(ctx, []string{"codex"}, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					payloadSeen, terminalSeen := false, false
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							terminalSeen = true
							var coded interface{ StatusCode() int }
							if !errors.As(chunk.Err, &coded) || coded.StatusCode() != status {
								t.Errorf("terminal classification lost: %v", chunk.Err)
							}
							var scoped interface{ IsRequestScoped() bool }
							if errors.As(chunk.Err, &scoped) && scoped.IsRequestScoped() {
								t.Error("credential failure became request-scoped")
							}
							if !payloadSeen {
								t.Error("original failure not forwarded before terminal error")
							}
							if !strings.Contains(chunk.Err.Error(), "credential rejected for "+wantKey) {
								t.Errorf("terminal failure lost request identity: %v", chunk.Err)
							}
							continue
						}
						event := gjson.GetBytes(chunk.Payload, "type").String()
						if event == "response.created" && queued {
							input <- core.WebsocketInput{Payload: []byte(`{"type":"response.create","prompt_cache_key":"queued-client-key","input":[]}`)}
						}
						if event == kind {
							payloadSeen = true
							if !scenario.ambiguous && !strings.Contains(string(chunk.Payload), "credential rejected for "+wantKey) {
								t.Errorf("failure uses another request's identity: %s", chunk.Payload)
							}
						}
					}
					if !payloadSeen || !terminalSeen {
						t.Errorf("failure payload=%t terminal=%t", payloadSeen, terminalSeen)
					}
					current, _ := manager.GetByID(bad.ID)
					state := current.ModelStates[model]
					if state == nil || state.LastError == nil || state.LastError.HTTPStatus != status || !state.Unavailable {
						t.Errorf("account failure not recorded: %+v", state)
					}
					if status == 429 && (current.Quota.Reason != "credential_quota" || current.Quota.NextRecoverAt.Before(before.Add(time.Hour))) {
						t.Errorf("quota scope or retry delay lost: %+v", current.Quota)
					}
					healthy, _ := manager.GetByID(good.ID)
					if healthy.Unavailable || healthy.LastError != nil {
						t.Error("unrelated healthy account changed")
					}
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Fatal("socket cleanup stalled")
					}
					if attempts.Load() != 1 {
						t.Fatalf("started response replayed across %d attempts", attempts.Load())
					}
				})
			}
		}
	}
}
