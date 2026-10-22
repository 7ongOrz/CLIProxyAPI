package openai

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

func TestResponsesWebsocketUsesRequestCredentialTrack(t *testing.T) {
	gin.SetMode(gin.TestMode)
	future := time.Now().Add(time.Hour).Format(time.RFC3339)
	past := time.Now().Add(-time.Hour).Format(time.RFC3339)
	for _, tc := range []struct {
		name, userAgent, outerExpiry, siwcExpiry, rejectedSIWC string
		available                                              bool
	}{
		{"native_expired_siwc", "codex_cli_rs/0.100.0", future, past, "", true},
		{"native_rejected_siwc", "codex_cli_rs/0.100.0", future, future, "siwc-token", true},
		{"siwc_expired_outer", "test-client", past, future, "", true},
		{"native_expired_outer", "codex_cli_rs/0.100.0", past, future, "", false},
		{"siwc_expired_siwc", "test-client", future, past, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := coreauth.NewManager(nil, nil, nil)
			executor := &websocketDirectCaptureExecutor{}
			manager.RegisterExecutor(executor)
			auth := &coreauth.Auth{
				ID: tc.name, Provider: "codex", Status: coreauth.StatusActive,
				Attributes:              map[string]string{"websockets": "true"},
				RejectedSIWCAccessToken: tc.rejectedSIWC,
				Metadata: map[string]any{
					"access_token": "outer-token", "expired": tc.outerExpiry, "use_siwc": true,
					"siwc": map[string]any{"access_token": "siwc-token", "expired": tc.siwcExpiry},
				},
			}
			if _, err := manager.Register(context.Background(), auth); err != nil {
				t.Fatal(err)
			}
			const model = "credential-track-model"
			registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
			router := gin.New()
			router.GET("/v1/responses", h.ResponsesWebsocket)
			server := httptest.NewServer(router)
			defer server.Close()
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", http.Header{"User-Agent": {tc.userAgent}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			for _, request := range []string{
				`{"type":"response.create","model":"credential-track-model","input":[{"type":"compaction","encrypted_content":"native-checkpoint"}]}`,
				`{"type":"response.create","model":"credential-track-model","previous_response_id":"resp-1","input":[{"type":"message","role":"user","content":"continue"}]}`,
			} {
				if !tc.available {
					if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(request)); errWrite != nil {
						t.Fatal(errWrite)
					}
					_, response, errRead := readResponsesWebsocketTestMessage(t, conn)
					if errRead != nil {
						t.Fatal(errRead)
					}
					if ojson.GetBytes(response, "status").Int() != http.StatusBadRequest || len(executor.Payloads()) != 0 {
						t.Fatalf("expired request credential reached execution: %s", response)
					}
					return
				}
				sendResponsesWebsocketTestRequest(t, conn, request)
			}
			payloads := executor.Payloads()
			if len(payloads) != 2 {
				t.Fatalf("upstream requests = %d, want 2", len(payloads))
			}
			if got := ojson.GetBytes(payloads[0], "input.0.encrypted_content").String(); got != "native-checkpoint" {
				t.Fatalf("compaction checkpoint = %q", got)
			}
			if got := ojson.GetBytes(payloads[1], "previous_response_id").String(); got != "resp-1" {
				t.Fatalf("continuation parent = %q, want resp-1", got)
			}
		})
	}
}
