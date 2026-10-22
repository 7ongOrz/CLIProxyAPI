package openai

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type interruptedCompactionExecutor struct {
	websocketProviderCaptureExecutor
	requests chan []byte
}

func (e *interruptedCompactionExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.requests <- bytes.Clone(req.Payload)
	if helps.HasResponsesCompactionTrigger(req.Payload) {
		coreexecutor.MarkLocalWebsocketTurn(ctx)
		chunks := make(chan coreexecutor.StreamChunk, 1)
		chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.created","response":{"id":"resp_ag_compact_interrupted","output":[]}}`)}
		// Hold summary delivery between created and completed until the client interrupts.
		go func() {
			<-ctx.Done()
			close(chunks)
		}()
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}
	return e.websocketCaptureExecutor.ExecuteStream(ctx, auth, req, opts)
}

func TestResponsesWebsocketInterruptedLocalCompactionPreservesTranscript(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &interruptedCompactionExecutor{
		websocketProviderCaptureExecutor: websocketProviderCaptureExecutor{provider: "codex"},
		requests:                         make(chan []byte, 3),
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	const model = "interrupted-local-compaction"
	const authID = "interrupted-local-compaction-auth"
	if _, err := manager.Register(context.Background(), &coreauth.Auth{ID: authID, Provider: "codex", Status: coreauth.StatusActive}); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	defer registry.GetGlobalRegistry().UnregisterClient(authID)
	handler := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(nil, manager))
	router := gin.New()
	router.GET("/v1/responses", handler.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", http.Header{"Session-Id": {t.Name()}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	sendResponsesWebsocketTestRequest(t, conn, fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"role":"user","content":"ORIGINAL_HISTORY"}]}`, model))
	<-executor.requests
	if err = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","input":[{"type":"compaction_trigger"}]}`)); err != nil {
		t.Fatal(err)
	}
	_, created, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if got := ojson.GetBytes(created, "type").String(); got != "response.created" {
		t.Fatalf("event = %s", created)
	}
	<-executor.requests
	if err = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.interrupt","response_id":"resp_ag_compact_interrupted"}`)); err != nil {
		t.Fatal(err)
	}
	// Acceptance and incomplete may be written by either goroutine first.
	for accepted, interrupted := false, false; !accepted || !interrupted; {
		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Fatal(errRead)
		}
		switch ojson.GetBytes(payload, "type").String() {
		case "response.interrupt.accepted":
			accepted = true
		case "response.incomplete":
			interrupted = true
			if got := ojson.GetBytes(payload, "response.incomplete_details.reason").String(); got != "interrupted" {
				t.Fatalf("incomplete = %s", payload)
			}
		default:
			t.Fatalf("unexpected interrupt event: %s", payload)
		}
	}
	sendResponsesWebsocketTestRequest(t, conn, `{"type":"response.create","input":[{"role":"user","content":"Continue the original task"}]}`)
	next := <-executor.requests
	if !bytes.Contains(next, []byte("ORIGINAL_HISTORY")) {
		t.Fatalf("interrupted compaction discarded committed history: %s", next)
	}
	if bytes.Contains(next, []byte("compaction_trigger")) {
		t.Fatalf("interrupted compaction was replayed: %s", next)
	}
}
