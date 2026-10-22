package openai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

// websocketLossInterruptExecutor runs a turn whose upstream websocket is
// lost mid-flight: InterruptExecutionSession stops seeing a live socket only
// after the test releases the loss, mirroring disconnect teardown winning
// the race against the handler's error delivery.
type websocketLossInterruptExecutor struct {
	lost          chan struct{}
	interruptSeen chan struct{}
}

func (*websocketLossInterruptExecutor) Identifier() string { return "codex" }

func (*websocketLossInterruptExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketLossInterruptExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	chunks := make(chan coreexecutor.StreamChunk, 2)
	go func() {
		defer close(chunks)
		chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.created","response":{"id":"r-ws"}}`)}
		select {
		case <-e.lost:
			select {
			case <-e.interruptSeen:
				chunks <- coreexecutor.StreamChunk{Err: errors.New("upstream websocket lost")}
			case <-ctx.Done():
			}
		case <-ctx.Done():
		}
	}()
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (*websocketLossInterruptExecutor) Refresh(context.Context, *coreauth.Auth) (*coreauth.Auth, error) {
	return nil, errors.New("not implemented")
}

func (*websocketLossInterruptExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (*websocketLossInterruptExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *websocketLossInterruptExecutor) InterruptExecutionSession(context.Context, string, []byte) error {
	select {
	case <-e.lost:
		close(e.interruptSeen)
		return coreexecutor.ErrNoActiveUpstreamWebsocket
	default:
		return errors.New("interrupt reached a live websocket")
	}
}

// When the upstream websocket dies mid-turn, an interrupt racing the loss
// delivery must not be answered with a fabricated local incomplete: the turn
// is websocket-owned, so the client reconnects after the transport closes.
// Error delivery waits for InterruptExecutionSession to observe the lost
// socket, making the interleaving deterministic.
func TestResponsesInterruptRacingWebsocketLossIsNotAnsweredLocally(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &websocketLossInterruptExecutor{
		lost: make(chan struct{}), interruptSeen: make(chan struct{}),
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(nil)
	manager.RegisterExecutor(executor)
	const authID = "interrupt-ws-loss-auth"
	const model = "interrupt-ws-loss-model"
	_, errRegister := manager.Register(context.Background(), &coreauth.Auth{
		ID:         authID,
		Provider:   "codex",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"api_key": "test-key", "websockets": "true"},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	defer registry.GetGlobalRegistry().UnregisterClient(authID)

	handler := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(nil, manager))
	router := gin.New()
	router.GET("/v1/responses", handler.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	defer downstream.Close()
	client, _, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
	if errDial != nil {
		t.Fatal(errDial)
	}
	defer func() { _ = client.Close() }()
	_ = client.SetReadDeadline(time.Now().Add(8 * time.Second))
	send := func(payload string) {
		t.Helper()
		if errSend := client.WriteMessage(websocket.TextMessage, []byte(payload)); errSend != nil {
			t.Fatal(errSend)
		}
	}
	send(fmt.Sprintf(`{"type":"response.create","model":%q,"event_id":"evt-loss","input":[]}`, model))
	_, created, errRead := client.ReadMessage()
	if errRead != nil {
		t.Fatal(errRead)
	}
	if got := gjson.GetBytes(created, "type").String(); got != "response.created" {
		t.Fatalf("expected response.created, got %s", created)
	}

	// The upstream socket is gone before the interrupt is dispatched, but
	// the handler has not consumed the loss yet.
	close(executor.lost)
	send(`{"type":"response.interrupt","response_id":"r-ws"}`)

	// The fork closes a failed stream after response.created so the client
	// can reconnect. An interrupt may be rejected first; it must leave the
	// websocket turn under the executor's ownership until that close.
	for {
		_, payload, errNext := client.ReadMessage()
		if websocket.IsCloseError(errNext, websocket.CloseAbnormalClosure) {
			return
		}
		if errNext != nil {
			t.Fatalf("waiting for upstream loss close: %v", errNext)
		}
		if gjson.GetBytes(payload, "type").String() == "response.incomplete" {
			t.Fatalf("fabricated local incomplete after websocket loss: %s", payload)
		}
	}
}
