package executor

import (
	"bytes"
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
	internalcache "github.com/router-for-me/CLIProxyAPI/v7/internal/cache"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/tidwall/gjson"
)

type captureCodexUsagePlugin struct {
	records chan usage.Record
}

func (p *captureCodexUsagePlugin) HandleUsage(_ context.Context, record usage.Record) {
	select {
	case p.records <- record:
	default:
	}
}

func TestBuildCodexWebsocketRequestBodyPreservesPreviousResponseID(t *testing.T) {
	body := []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-1"}]}`)

	wsReqBody := buildCodexWebsocketRequestBody(body)

	if got := gjson.GetBytes(wsReqBody, "type").String(); got != "response.create" {
		t.Fatalf("type = %s, want response.create", got)
	}
	if got := gjson.GetBytes(wsReqBody, "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("previous_response_id = %s, want resp-1", got)
	}
	if gjson.GetBytes(wsReqBody, "input.0.id").String() != "msg-1" {
		t.Fatalf("input item id mismatch")
	}
	if got := gjson.GetBytes(wsReqBody, "type").String(); got == "response.append" {
		t.Fatalf("unexpected websocket request type: %s", got)
	}
}

func TestBuildCodexWebsocketRequestBodySanitizesOverlongInputItemIDs(t *testing.T) {
	longReasoningItemID := "rs_" + strings.Repeat("a", 64)
	longCallItemID := strings.Repeat("grok-call-item-", 6)
	longOutputItemID := strings.Repeat("grok-output-item-", 6)
	body := []byte(`{"model":"gpt-5-codex","input":[{"type":"reasoning","id":"` + longReasoningItemID + `","encrypted_content":"gAAAA-encrypted","summary":[]},{"type":"function_call","id":"` + longCallItemID + `","call_id":"call-1","name":"lookup"},{"type":"function_call_output","id":"` + longOutputItemID + `","call_id":"call-1","output":"ok"},{"type":"message","id":"msg-1"}]}`)

	first := buildCodexWebsocketRequestBody(body)
	second := buildCodexWebsocketRequestBody(body)

	if input := gjson.GetBytes(first, "input").Array(); len(input) != 3 {
		t.Fatalf("input length = %d, want 3: %s", len(input), first)
	}
	if gotType := gjson.GetBytes(first, "input.0.type").String(); gotType != "function_call" {
		t.Fatalf("input.0.type = %q, want function_call: %s", gotType, first)
	}

	shortCallItemID := gjson.GetBytes(first, "input.0.id").String()
	shortOutputItemID := gjson.GetBytes(first, "input.1.id").String()
	if len([]rune(shortCallItemID)) > 64 || shortCallItemID == longCallItemID {
		t.Fatalf("input.0.id was not shortened to at most 64 characters: %q", shortCallItemID)
	}
	if len([]rune(shortOutputItemID)) > 64 || shortOutputItemID == longOutputItemID {
		t.Fatalf("input.1.id was not shortened to at most 64 characters: %q", shortOutputItemID)
	}
	if shortCallItemID == shortOutputItemID {
		t.Fatalf("distinct long IDs produced the same shortened ID: %q", shortCallItemID)
	}
	if got := gjson.GetBytes(second, "input.0.id").String(); got != shortCallItemID {
		t.Fatalf("input item ID shortening is not deterministic: first=%q second=%q", shortCallItemID, got)
	}
	if got := gjson.GetBytes(first, "input.0.call_id").String(); got != "call-1" {
		t.Fatalf("function call_id = %q, want call-1", got)
	}
	if got := gjson.GetBytes(first, "input.1.call_id").String(); got != "call-1" {
		t.Fatalf("function call output call_id = %q, want call-1", got)
	}
	if got := gjson.GetBytes(first, "input.2.id").String(); got != "msg-1" {
		t.Fatalf("valid input item ID changed: %q", got)
	}
}

func TestCodexWebsocketsExecuteRestoresClaudeAgentReasoningReplay(t *testing.T) {
	internalcache.ClearCodexReasoningReplayCache()
	t.Cleanup(internalcache.ClearCodexReasoningReplayCache)

	encryptedContent := validCodexReasoningEncryptedContentForTestSeed(31)
	cacheCodexReasoningReplayFromCompleted(codexReasoningReplayScope{
		modelName:  "gpt-5.4",
		sessionKey: "claude:ws-replay-session:agent:agent-a",
	}, []byte(`{"response":{"output":[`+
		`{"type":"reasoning","summary":[],"content":null,"encrypted_content":"`+encryptedContent+`"},`+
		`{"type":"message","role":"assistant","content":[{"type":"output_text","text":"previous answer"}]}`+
		`]}}`))

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	capturedPayload := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Fatalf("upgrade websocket: %v", errUpgrade)
		}
		defer func() { _ = conn.Close() }()

		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Fatalf("read upstream websocket message: %v", errRead)
		}
		capturedPayload <- bytes.Clone(payload)
		completed := []byte(`{"type":"response.completed","response":{"id":"resp-ws-replay","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"next answer"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Fatalf("write completed websocket message: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{Provider: "codex", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model: "gpt-5.4",
		Payload: []byte(`{
			"model":"gpt-5.4",
			"messages":[
				{"role":"user","content":"first"},
				{"role":"assistant","content":"previous answer"},
				{"role":"user","content":"next"}
			]
		}`),
	}
	headers := http.Header{}
	headers.Set("X-Claude-Code-Session-Id", "ws-replay-session")
	headers.Set("X-Claude-Code-Agent-Id", "agent-a")
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("claude"), Headers: headers}

	if _, errExecute := exec.Execute(context.Background(), auth, req, opts); errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}

	select {
	case payload := <-capturedPayload:
		input := gjson.GetBytes(payload, "input").Array()
		if len(input) != 4 {
			t.Fatalf("upstream input length = %d, want 4; payload=%s", len(input), payload)
		}
		if input[1].Get("type").String() != "reasoning" || input[1].Get("encrypted_content").String() != encryptedContent {
			t.Fatalf("websocket reasoning replay missing before assistant message: %s", payload)
		}
		if input[2].Get("role").String() != "assistant" {
			t.Fatalf("input.2.role = %q, want assistant; payload=%s", input[2].Get("role").String(), payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream websocket payload")
	}
}

func TestClearCodexReasoningReplayOnWebsocketInvalidSignature(t *testing.T) {
	internalcache.ClearCodexReasoningReplayCache()
	t.Cleanup(internalcache.ClearCodexReasoningReplayCache)

	scope := codexReasoningReplayScope{modelName: "gpt-5.4", sessionKey: "claude:ws-invalid:agent:main"}
	encryptedContent := validCodexReasoningEncryptedContentForTestSeed(32)
	if !internalcache.CacheCodexReasoningReplayItem(scope.modelName, scope.sessionKey, []byte(`{"type":"reasoning","summary":[],"content":null,"encrypted_content":"`+encryptedContent+`"}`)) {
		t.Fatal("failed to seed websocket replay cache")
	}
	payload := []byte(`{"type":"error","status":400,"body":{"error":{"message":"Invalid signature in thinking block","type":"invalid_request_error","code":"invalid_request_error"}}}`)
	if errClear := clearCodexReasoningReplayOnWebsocketError(context.Background(), scope, payload); errClear != nil {
		t.Fatalf("clear websocket replay error: %v", errClear)
	}
	if _, ok := internalcache.GetCodexReasoningReplayItem(scope.modelName, scope.sessionKey); ok {
		t.Fatal("websocket invalid signature did not clear replay state")
	}
}

func TestCodexWebsocketsExecuteResponsesLiteDoesNotInjectImageGenerationTool(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	capturedPayload := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade websocket: %v", err)
		}
		defer func() { _ = conn.Close() }()

		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Fatalf("read upstream websocket message: %v", errRead)
		}
		capturedPayload <- bytes.Clone(payload)

		completed := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Fatalf("write completed websocket message: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Attributes: map[string]string{
			"api_key":   "sk-test",
			"base_url":  server.URL,
			"plan_type": "pro",
		},
	}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5.6-sol",
		Payload: []byte(`{"model":"gpt-5.6-sol","input":[{"type":"additional_tools","role":"developer","tools":[{"type":"custom","name":"exec"}]},{"role":"user","content":"hello"}],"parallel_tool_calls":true,"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"true"}}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")}

	if _, err := exec.Execute(context.Background(), auth, req, opts); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	select {
	case payload := <-capturedPayload:
		if tools := gjson.GetBytes(payload, "tools"); tools.Exists() {
			t.Fatalf("unexpected tools in responses-lite upstream payload: %s", tools.Raw)
		}
		if got := gjson.GetBytes(payload, "input.0.type").String(); got != "additional_tools" {
			t.Fatalf("input.0.type = %q, want additional_tools; payload=%s", got, payload)
		}
		if got := gjson.GetBytes(payload, "client_metadata.ws_request_header_x_openai_internal_codex_responses_lite").String(); got != "true" {
			t.Fatalf("responses-lite metadata = %q, want true; payload=%s", got, payload)
		}
		parallelToolCalls := gjson.GetBytes(payload, "parallel_tool_calls")
		if !parallelToolCalls.Exists() || parallelToolCalls.Bool() {
			t.Fatalf("responses-lite parallel_tool_calls should be false: %s", payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream websocket payload")
	}
}

func TestCodexWebsocketsExecuteStreamResponsesLiteForcesParallelToolCallsFalse(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	capturedPayload := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()

		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		capturedPayload <- bytes.Clone(payload)

		completed := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Errorf("write completed websocket message: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Attributes: map[string]string{
			"api_key":   "sk-test",
			"base_url":  server.URL,
			"plan_type": "pro",
		},
	}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5.6-luna",
		Payload: []byte(`{"model":"gpt-5.6-luna","input":[{"type":"additional_tools","role":"developer","tools":[{"type":"custom","name":"exec"}]},{"role":"user","content":"hello"}],"parallel_tool_calls":true,"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"true"}}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")}

	result, errExecute := exec.ExecuteStream(context.Background(), auth, req, opts)
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	streamComplete := false
	for !streamComplete {
		select {
		case chunk, ok := <-result.Chunks:
			if !ok {
				streamComplete = true
				continue
			}
			if chunk.Err != nil {
				t.Fatalf("stream chunk error = %v", chunk.Err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for websocket stream completion")
		}
	}

	select {
	case payload := <-capturedPayload:
		parallelToolCalls := gjson.GetBytes(payload, "parallel_tool_calls")
		if !parallelToolCalls.Exists() || parallelToolCalls.Bool() {
			t.Fatalf("responses-lite parallel_tool_calls should be false: %s", payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream websocket payload")
	}
}

func TestCodexWebsocketsExecutePreservesPreviousResponseIDUpstream(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	capturedPayload := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("request path = %s, want /responses", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade websocket: %v", err)
		}
		defer func() { _ = conn.Close() }()

		msgType, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read upstream websocket message: %v", err)
		}
		if msgType != websocket.TextMessage {
			t.Fatalf("message type = %d, want text", msgType)
		}
		capturedPayload <- bytes.Clone(payload)

		completed := []byte(`{"type":"response.completed","response":{"id":"resp-2","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Fatalf("write completed websocket message: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-1"}]}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")}

	if _, err := exec.Execute(context.Background(), auth, req, opts); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	select {
	case payload := <-capturedPayload:
		if got := gjson.GetBytes(payload, "type").String(); got != "response.create" {
			t.Fatalf("upstream type = %s, want response.create; payload=%s", got, payload)
		}
		if got := gjson.GetBytes(payload, "previous_response_id").String(); got != "resp-1" {
			t.Fatalf("upstream previous_response_id = %s, want resp-1; payload=%s", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream websocket payload")
	}
}

func TestCodexWebsocketsExecuteStreamUpgradeRequiredReturnsWithoutLockingSession(t *testing.T) {
	upgradeAttempts := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			t.Errorf("unexpected HTTP fallback request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		upgradeAttempts <- struct{}{}
		w.WriteHeader(http.StatusUpgradeRequired)
		_, _ = w.Write([]byte(`{"error":{"message":"websocket unavailable"}}`))
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	const executionSessionID = "ws-upgrade-required-session"
	t.Cleanup(func() { exec.CloseExecutionSession(executionSessionID) })
	auth := &cliproxyauth.Auth{
		ID:       "codex-test",
		Provider: "codex",
		Attributes: map[string]string{
			"api_key":  "sk-test",
			"base_url": server.URL,
		},
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: executionSessionID,
		},
	}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())

	execute := func(payload string) {
		t.Helper()
		done := make(chan error, 1)
		go func() {
			_, errExecute := exec.ExecuteStream(ctx, auth, cliproxyexecutor.Request{
				Model:   "gpt-5.4",
				Payload: []byte(payload),
			}, opts)
			done <- errExecute
		}()

		select {
		case errExecute := <-done:
			if errExecute == nil {
				t.Fatal("upgrade-required error = nil")
			}
			statusErr, ok := errExecute.(interface{ StatusCode() int })
			if !ok || statusErr.StatusCode() != http.StatusUpgradeRequired {
				t.Fatalf("upgrade-required error = %T %v, want status 426", errExecute, errExecute)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for upgrade-required error; execution session may still be locked")
		}
	}

	execute(`{"model":"gpt-5.4","generate":false,"input":[]}`)
	execute(`{"model":"gpt-5.4","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`)

	if got := len(upgradeAttempts); got != 2 {
		t.Fatalf("websocket upgrade attempts = %d, want 2", got)
	}
}

func TestCodexWebsocketsExecuteStreamHandshakeErrorReturnsWithoutLockingSession(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"message":"unauthorized"}}`))
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	const executionSessionID = "ws-handshake-error-session"
	t.Cleanup(func() { exec.CloseExecutionSession(executionSessionID) })
	auth := &cliproxyauth.Auth{
		ID:       "codex-test",
		Provider: "codex",
		Attributes: map[string]string{
			"api_key":  "sk-test",
			"base_url": server.URL,
		},
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai-response"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: executionSessionID,
		},
	}

	for i := 0; i < 2; i++ {
		done := make(chan error, 1)
		go func() {
			_, errExecute := exec.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
				Model:   "gpt-5.4",
				Payload: []byte(`{"model":"gpt-5.4","input":[{"type":"message","id":"msg-1"}]}`),
			}, opts)
			done <- errExecute
		}()
		select {
		case errExecute := <-done:
			statusErr, ok := errExecute.(interface{ StatusCode() int })
			if !ok || statusErr.StatusCode() != http.StatusUnauthorized {
				t.Fatalf("attempt %d error = %T %v, want status 401", i+1, errExecute, errExecute)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("attempt %d timed out; execution session remained locked", i+1)
		}
	}
}

func TestExistingWebsocketSessionConnRequiresMatchingHealthyConnection(t *testing.T) {
	conn := &websocket.Conn{}
	closer := newWebsocketConnectionCloser(conn)
	sess := &codexWebsocketSession{
		conn:       conn,
		connCloser: closer,
		authID:     "auth-a",
		wsURL:      "ws://example.test/responses",
	}
	sess.resetUpstreamDisconnectError(conn)
	if gotConn, gotCloser := existingWebsocketSessionConn(sess, "auth-a", "ws://example.test/responses"); gotConn != conn || gotCloser != closer {
		t.Fatal("matching healthy websocket session was not reusable")
	}
	if got, _ := existingWebsocketSessionConn(sess, "auth-b", "ws://example.test/responses"); got != nil {
		t.Fatal("websocket session matched a different auth")
	}
	if got, _ := existingWebsocketSessionConn(sess, "auth-a", "ws://other.test/responses"); got != nil {
		t.Fatal("websocket session matched a different URL")
	}
	sess.setUpstreamDisconnectError(conn, errors.New("upstream disconnected"))
	if got, _ := existingWebsocketSessionConn(sess, "auth-a", "ws://example.test/responses"); got != nil {
		t.Fatal("disconnected websocket session remained reusable")
	}
}

func TestCodexAutoExecutorRequiredUpstreamWebsocketRejectsHTTPFallback(t *testing.T) {
	exec := NewCodexAutoExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{
		ID:       "codex-http-only",
		Provider: "codex",
		Attributes: map[string]string{
			"api_key": "sk-test",
		},
	}
	ctx := cliproxyexecutor.WithRequiredUpstreamWebsocket(
		cliproxyexecutor.WithDownstreamWebsocket(context.Background()),
	)
	_, errExecute := exec.ExecuteStream(ctx, auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4",
		Payload: []byte(`{"model":"gpt-5.4","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")})
	if errExecute == nil {
		t.Fatal("ExecuteStream() error = nil, want replay-required error")
	}
	statusErr, ok := errExecute.(interface{ StatusCode() int })
	if !ok || statusErr.StatusCode() != http.StatusUpgradeRequired {
		t.Fatalf("ExecuteStream() error = %T %v, want status 426", errExecute, errExecute)
	}
	if got := gjson.Get(errExecute.Error(), "error.code").String(); got != "upstream_http_replay_required" {
		t.Fatalf("ExecuteStream() error code = %q, want upstream_http_replay_required", got)
	}
	requestScoped, ok := errExecute.(cliproxyexecutor.RequestScopedError)
	if !ok || !requestScoped.IsRequestScoped() {
		t.Fatalf("ExecuteStream() error = %T, want request-scoped replay signal", errExecute)
	}
}

func TestCodexWebsocketsExecuteStreamPassesThroughUpstreamWebsocketPayloadForDownstreamWebsocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	capturedPayload := make(chan []byte, 1)
	delta := []byte(`{"type":"response.output_text.delta","delta":"hello"}`)
	completed := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		capturedPayload <- bytes.Clone(payload)
		if errWrite := conn.WriteMessage(websocket.TextMessage, delta); errWrite != nil {
			t.Errorf("write delta websocket message: %v", errWrite)
			return
		}
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Errorf("write completed websocket message: %v", errWrite)
			return
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"prolite/gpt-5-codex","input":[{"type":"additional_tools","role":"developer","tools":[{"type":"custom","name":"exec"}]},{"type":"message","role":"user","content":"hello"}],"parallel_tool_calls":true}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
	}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())

	result, err := exec.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed before first chunk")
		}
		if chunk.Err != nil {
			t.Fatalf("first chunk error = %v", chunk.Err)
		}
		if !bytes.Equal(bytes.TrimSpace(chunk.Payload), delta) {
			t.Fatalf("first chunk = %q, want raw upstream websocket payload %q", chunk.Payload, delta)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first stream chunk")
	}

	select {
	case payload := <-capturedPayload:
		if got := gjson.GetBytes(payload, "model").String(); got != "gpt-5-codex" {
			t.Fatalf("upstream model = %s, want gpt-5-codex; payload=%s", got, payload)
		}
		parallelToolCalls := gjson.GetBytes(payload, "parallel_tool_calls")
		if !parallelToolCalls.Exists() || !parallelToolCalls.Bool() {
			t.Fatalf("non-lite parallel_tool_calls should be preserved: %s", payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream websocket payload")
	}
}

func TestCodexWebsocketsExecuteStreamSeparatesClientAndAccountingErrors(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	const originalPromptCacheKey = "cache-type-error-1"
	confusedPromptCacheKeyCh := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		_, upstreamPayload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		confusedPromptCacheKey := strings.TrimSpace(gjson.GetBytes(upstreamPayload, "prompt_cache_key").String())
		if confusedPromptCacheKey == "" || confusedPromptCacheKey == originalPromptCacheKey {
			t.Errorf("upstream prompt_cache_key was not confused: %s", upstreamPayload)
			return
		}
		confusedPromptCacheKeyCh <- confusedPromptCacheKey
		errorPayload := []byte(fmt.Sprintf(`{"type":"error","status":429,"error":{"code":"rate_limit_exceeded","message":%q}}`, "upstream rejected "+confusedPromptCacheKey))
		if errWrite := conn.WriteMessage(websocket.TextMessage, errorPayload); errWrite != nil {
			t.Errorf("write error websocket message: %v", errWrite)
			return
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{
		SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll},
		Routing:   config.RoutingConfig{SessionAffinity: true},
		Codex:     config.CodexConfig{IdentityConfuse: true},
	})
	auth := &cliproxyauth.Auth{ID: "auth-type-error-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","prompt_cache_key":"cache-type-error-1","input":[{"type":"message","role":"user","content":"hello"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
	}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())

	result, err := exec.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var confusedPromptCacheKey string
	select {
	case confusedPromptCacheKey = <-confusedPromptCacheKeyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for confused prompt cache key")
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed before error chunk")
		}
		if chunk.Err != nil {
			t.Fatalf("error chunk Err = %v, want nil", chunk.Err)
		}
		if !strings.Contains(string(chunk.Payload), originalPromptCacheKey) || strings.Contains(string(chunk.Payload), confusedPromptCacheKey) {
			t.Fatalf("client payload identity was not exposed correctly: %s", chunk.Payload)
		}
		if chunk.ResultErr == nil {
			t.Fatal("error chunk ResultErr = nil")
		}
		if strings.Contains(chunk.ResultErr.Error(), originalPromptCacheKey) || !strings.Contains(chunk.ResultErr.Error(), confusedPromptCacheKey) {
			t.Fatalf("accounting error identity was not kept confused: %v", chunk.ResultErr)
		}
		statusErr, ok := chunk.ResultErr.(interface{ StatusCode() int })
		if !ok {
			t.Fatalf("result error type %T does not expose StatusCode", chunk.ResultErr)
		}
		if got := statusErr.StatusCode(); got != http.StatusTooManyRequests {
			t.Fatalf("status = %d, want %d", got, http.StatusTooManyRequests)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for error stream chunk")
	}
}

func TestSendTerminalWebsocketReadInvalidatesBeforeWaitingForCapacity(t *testing.T) {
	terminalErr := &websocket.CloseError{Code: websocket.CloseMessageTooBig}

	t.Run("available channel keeps fast path ordering", func(t *testing.T) {
		ch := make(chan codexWebsocketRead, 1)
		done := make(chan struct{})
		invalidateCalls := 0
		invalidated := sendTerminalWebsocketRead(ch, done, codexWebsocketRead{err: terminalErr}, func() {
			invalidateCalls++
		})
		if invalidated {
			t.Fatal("available channel should not invalidate before delivery")
		}
		if invalidateCalls != 0 {
			t.Fatalf("invalidate calls = %d, want 0", invalidateCalls)
		}
		event := <-ch
		if !errors.Is(event.err, terminalErr) {
			t.Fatalf("terminal error = %v, want %v", event.err, terminalErr)
		}
	})

	t.Run("full channel invalidates before waiting", func(t *testing.T) {
		ch := make(chan codexWebsocketRead, 1)
		ch <- codexWebsocketRead{payload: []byte("queued")}
		done := make(chan struct{})
		invalidateCalled := make(chan struct{})
		result := make(chan bool, 1)

		go func() {
			result <- sendTerminalWebsocketRead(ch, done, codexWebsocketRead{err: terminalErr}, func() {
				close(invalidateCalled)
			})
		}()

		select {
		case <-invalidateCalled:
		case <-time.After(time.Second):
			t.Fatal("invalidation did not happen before waiting for channel capacity")
		}
		select {
		case <-result:
			t.Fatal("terminal sender returned before capacity was released")
		default:
		}

		<-ch
		select {
		case event := <-ch:
			if !errors.Is(event.err, terminalErr) {
				t.Fatalf("terminal error = %v, want %v", event.err, terminalErr)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for terminal read")
		}
		select {
		case invalidated := <-result:
			if !invalidated {
				t.Fatal("full channel should report early invalidation")
			}
		case <-time.After(time.Second):
			t.Fatal("terminal sender did not finish")
		}
	})

	t.Run("full channel stops when invalidation cancels active read", func(t *testing.T) {
		ch := make(chan codexWebsocketRead, 1)
		ch <- codexWebsocketRead{payload: []byte("queued")}
		done := make(chan struct{})
		invalidated := sendTerminalWebsocketRead(ch, done, codexWebsocketRead{err: terminalErr}, func() {
			close(done)
		})
		if !invalidated {
			t.Fatal("full channel should report early invalidation")
		}
		if len(ch) != 1 {
			t.Fatalf("channel length = %d, want queued payload only", len(ch))
		}
	})
}

func TestMapCodexWebsocketWriteErrorStopsRetryForMessageTooBig(t *testing.T) {
	networkWriteErr := errors.New("write: broken pipe")
	tests := []struct {
		name       string
		closeCode  int
		writeErr   error
		wantStatus int
		wantRetry  bool
	}{
		{
			name:       "close sent after message too big is request scoped",
			closeCode:  websocket.CloseMessageTooBig,
			writeErr:   websocket.ErrCloseSent,
			wantStatus: http.StatusRequestEntityTooLarge,
			wantRetry:  false,
		},
		{
			name:       "network write error after message too big is request scoped",
			closeCode:  websocket.CloseMessageTooBig,
			writeErr:   networkWriteErr,
			wantStatus: http.StatusRequestEntityTooLarge,
			wantRetry:  false,
		},
		{
			name:      "other close keeps stale connection retry",
			closeCode: websocket.CloseNormalClosure,
			writeErr:  websocket.ErrCloseSent,
			wantRetry: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sess := &codexWebsocketSession{}
			conn := &websocket.Conn{}
			sess.resetUpstreamDisconnectError(conn)
			sess.setUpstreamDisconnectError(conn, &websocket.CloseError{Code: tt.closeCode})

			mappedErr := mapCodexWebsocketWriteError(sess, conn, tt.writeErr)
			if got := shouldRetryCodexWebsocketSend(mappedErr); got != tt.wantRetry {
				t.Fatalf("shouldRetryCodexWebsocketSend() = %v, want %v; err=%v", got, tt.wantRetry, mappedErr)
			}
			if tt.wantStatus == 0 {
				if !errors.Is(mappedErr, tt.writeErr) {
					t.Fatalf("mapped error = %v, want %v", mappedErr, tt.writeErr)
				}
				return
			}
			statusErr, ok := mappedErr.(interface{ StatusCode() int })
			if !ok || statusErr.StatusCode() != tt.wantStatus {
				t.Fatalf("mapped status = %v, want %d; err=%v", statusErr, tt.wantStatus, mappedErr)
			}
			requestErr, ok := mappedErr.(interface{ IsRequestScoped() bool })
			if !ok || !requestErr.IsRequestScoped() {
				t.Fatalf("mapped error should be request scoped, got %T", mappedErr)
			}
		})
	}
}

func TestMapCodexWebsocketWriteErrorDoesNotReusePriorConnectionClose(t *testing.T) {
	sess := &codexWebsocketSession{}
	priorConn := &websocket.Conn{}
	replacementConn := &websocket.Conn{}

	sess.resetUpstreamDisconnectError(priorConn)
	sess.setUpstreamDisconnectError(priorConn, &websocket.CloseError{Code: websocket.CloseMessageTooBig})
	priorErr := mapCodexWebsocketWriteError(sess, priorConn, websocket.ErrCloseSent)
	if shouldRetryCodexWebsocketSend(priorErr) {
		t.Fatalf("prior connection 1009 should not retry, got %v", priorErr)
	}

	sess.resetUpstreamDisconnectError(replacementConn)
	// A late close callback from the prior connection must not overwrite the
	// replacement connection's close state.
	sess.setUpstreamDisconnectError(priorConn, &websocket.CloseError{Code: websocket.CloseMessageTooBig})
	sess.setUpstreamDisconnectError(replacementConn, &websocket.CloseError{Code: websocket.CloseNormalClosure})
	replacementErr := mapCodexWebsocketWriteError(sess, replacementConn, websocket.ErrCloseSent)
	if !errors.Is(replacementErr, websocket.ErrCloseSent) {
		t.Fatalf("replacement connection error = %v, want %v", replacementErr, websocket.ErrCloseSent)
	}
	if !shouldRetryCodexWebsocketSend(replacementErr) {
		t.Fatalf("replacement connection should keep stale-connection retry, got %v", replacementErr)
	}
}

func TestCodexWebsocketsExecuteStreamMapsMessageTooBigClose(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		deadline := time.Now().Add(time.Second)
		closeMessage := websocket.FormatCloseMessage(websocket.CloseMessageTooBig, "message too big")
		if errWrite := conn.WriteControl(websocket.CloseMessage, closeMessage, deadline); errWrite != nil {
			t.Errorf("write close websocket message: %v", errWrite)
			return
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"hello"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
	}

	result, err := exec.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed before error chunk")
		}
		if chunk.Err == nil {
			t.Fatal("error chunk Err = nil, want message-too-big error")
		}
		statusErr, ok := chunk.Err.(interface{ StatusCode() int })
		if !ok {
			t.Fatalf("error type %T does not expose StatusCode", chunk.Err)
		}
		if got := statusErr.StatusCode(); got != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want %d", got, http.StatusRequestEntityTooLarge)
		}
		if got := gjson.Get(chunk.Err.Error(), "error.code").String(); got != "message_too_big" {
			t.Fatalf("error code = %q, want message_too_big; err=%v", got, chunk.Err)
		}
		requestErr, ok := chunk.Err.(interface{ IsRequestScoped() bool })
		if !ok || !requestErr.IsRequestScoped() {
			t.Fatalf("message-too-big error should be request scoped, got %T", chunk.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for error stream chunk")
	}
}

func TestCodexWebsocketsExecuteStreamMapsSessionMessageTooBigClose(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		closeMessage := websocket.FormatCloseMessage(websocket.CloseMessageTooBig, "message too big")
		if errWrite := conn.WriteControl(websocket.CloseMessage, closeMessage, time.Now().Add(time.Second)); errWrite != nil {
			t.Errorf("write close websocket message: %v", errWrite)
			return
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-message-too-big"
	defer exec.CloseExecutionSession(sessionID)
	auth := &cliproxyauth.Auth{ID: "auth-message-too-big", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"hello"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}

	result, err := exec.ExecuteStream(cliproxyexecutor.WithDownstreamWebsocket(context.Background()), auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed before error chunk")
		}
		if chunk.Err == nil {
			t.Fatal("error chunk Err = nil, want message-too-big error")
		}
		var replayRequired interface {
			CodexWebsocketReplayRequired() bool
		}
		if errors.As(chunk.Err, &replayRequired) && replayRequired != nil && replayRequired.CodexWebsocketReplayRequired() {
			t.Fatalf("error chunk Err = %v, must not require transcript replay", chunk.Err)
		}
		statusErr, ok := chunk.Err.(interface{ StatusCode() int })
		if !ok {
			t.Fatalf("error type %T does not expose StatusCode", chunk.Err)
		}
		if got := statusErr.StatusCode(); got != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want %d", got, http.StatusRequestEntityTooLarge)
		}
		if got := gjson.Get(chunk.Err.Error(), "error.code").String(); got != "message_too_big" {
			t.Fatalf("error code = %q, want message_too_big; err=%v", got, chunk.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for error stream chunk")
	}
}

func TestCodexWebsocketsExecuteStreamDoesNotNotifyDownstreamBeforeForwardingError(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	confusedPromptCacheKeyCh := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		_, upstreamPayload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		confusedPromptCacheKey := strings.TrimSpace(gjson.GetBytes(upstreamPayload, "prompt_cache_key").String())
		if confusedPromptCacheKey == "" {
			t.Errorf("upstream prompt_cache_key is empty: %s", upstreamPayload)
			return
		}
		confusedPromptCacheKeyCh <- confusedPromptCacheKey
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"codex.rate_limits","rate_limits":[]}`)); errWrite != nil {
			t.Errorf("write rate limits websocket message: %v", errWrite)
			return
		}
		errorPayload := []byte(`{"type":"error","status":500,"error":{"code":"server_error","message":"upstream failed for ` + confusedPromptCacheKey + `:0"}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, errorPayload); errWrite != nil {
			t.Errorf("write error websocket message: %v", errWrite)
			return
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{
		SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll},
		Routing:   config.RoutingConfig{SessionAffinity: true},
		Codex:     config.CodexConfig{IdentityConfuse: true},
	})
	sessionID := "test-error-forward-session"
	defer exec.CloseExecutionSession(sessionID)
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	auth := &cliproxyauth.Auth{
		ID:         "auth-error-forward",
		Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL},
	}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","prompt_cache_key":"cache-error-forward","input":[{"type":"message","role":"user","content":"hello"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())

	result, err := exec.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var confusedPromptCacheKey string
	select {
	case confusedPromptCacheKey = <-confusedPromptCacheKeyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream prompt cache key")
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed before rate limits chunk")
		}
		if chunk.Err != nil || chunk.ResultErr != nil || gjson.GetBytes(chunk.Payload, "type").String() != "codex.rate_limits" {
			t.Fatalf("rate limits chunk = payload %q err=%v result_err=%v", chunk.Payload, chunk.Err, chunk.ResultErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for rate limits stream chunk")
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed before error chunk")
		}
		if chunk.Err != nil {
			t.Fatalf("error chunk Err = %v, want nil", chunk.Err)
		}
		if !strings.Contains(string(chunk.Payload), "cache-error-forward:0") || strings.Contains(string(chunk.Payload), confusedPromptCacheKey) {
			t.Fatalf("client error payload identity was not exposed correctly: %s", chunk.Payload)
		}
		if chunk.ResultErr == nil {
			t.Fatal("error chunk ResultErr = nil")
		}
		if strings.Contains(chunk.ResultErr.Error(), "cache-error-forward") || !strings.Contains(chunk.ResultErr.Error(), confusedPromptCacheKey) {
			t.Fatalf("accounting error identity was not kept confused: %v", chunk.ResultErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for error stream chunk")
	}

	select {
	case errDisconnect, ok := <-disconnectCh:
		t.Fatalf("downstream disconnect notified before error forwarding completed: ok=%v err=%v", ok, errDisconnect)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestCodexWebsocketsExecuteStreamPassesThroughResponseFailedForDownstreamWebsocket(t *testing.T) {
	previousLevel := log.GetLevel()
	log.SetLevel(log.InfoLevel)
	hook := logtest.NewLocal(log.StandardLogger())
	t.Cleanup(func() {
		hook.Reset()
		log.SetLevel(previousLevel)
	})

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	const originalPromptCacheKey = "cache-response-failed"
	expectedFailedPayload := []byte(`{"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"type":"server_error","message":"upstream failed for cache-response-failed:0"}}}`)
	confusedPromptCacheKeyCh := make(chan string, 1)
	done := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		_, upstreamPayload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		confusedPromptCacheKey := strings.TrimSpace(gjson.GetBytes(upstreamPayload, "prompt_cache_key").String())
		if confusedPromptCacheKey == "" || confusedPromptCacheKey == originalPromptCacheKey {
			t.Errorf("upstream prompt_cache_key was not confused: %s", upstreamPayload)
			return
		}
		confusedPromptCacheKeyCh <- confusedPromptCacheKey
		failedPayload := []byte(fmt.Sprintf(`{"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"type":"server_error","message":%q}}}`, "upstream failed for "+confusedPromptCacheKey+":0"))
		if errWrite := conn.WriteMessage(websocket.TextMessage, failedPayload); errWrite != nil {
			t.Errorf("write response.failed websocket message: %v", errWrite)
			return
		}
		<-done
	}))
	defer server.Close()
	defer close(done)

	exec := NewCodexWebsocketsExecutor(&config.Config{
		SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll},
		Routing:   config.RoutingConfig{SessionAffinity: true},
		Codex:     config.CodexConfig{IdentityConfuse: true},
	})
	sessionID := "sess-response-failed-downstream"
	defer exec.CloseExecutionSession(sessionID)
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	if disconnectCh == nil {
		t.Fatal("expected disconnect channel")
	}

	auth := &cliproxyauth.Auth{ID: "auth-response-failed", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","prompt_cache_key":"cache-response-failed","input":[{"type":"message","role":"user","content":"hello"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())

	result, err := exec.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var confusedPromptCacheKey string
	select {
	case confusedPromptCacheKey = <-confusedPromptCacheKeyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for confused prompt cache key")
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed before response.failed chunk")
		}
		if chunk.Err != nil {
			t.Fatalf("response.failed chunk error = %v", chunk.Err)
		}
		if !bytes.Equal(bytes.TrimSpace(chunk.Payload), expectedFailedPayload) {
			t.Fatalf("response.failed chunk = %q, want %q", chunk.Payload, expectedFailedPayload)
		}
		if chunk.ResultErr == nil {
			t.Fatal("response.failed chunk ResultErr = nil")
		}
		if statusErr, ok := chunk.ResultErr.(cliproxyexecutor.StatusError); !ok || statusErr.StatusCode() != http.StatusInternalServerError {
			t.Fatalf("response.failed result error = %v, want status %d", chunk.ResultErr, http.StatusInternalServerError)
		}
		if strings.Contains(chunk.ResultErr.Error(), originalPromptCacheKey) {
			t.Fatalf("response.failed result error leaked original prompt cache key: %v", chunk.ResultErr)
		}
		if !strings.Contains(chunk.ResultErr.Error(), confusedPromptCacheKey) {
			t.Fatalf("response.failed result error = %v, want confused prompt cache key", chunk.ResultErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for response.failed chunk")
	}

	select {
	case chunk, ok := <-result.Chunks:
		if ok {
			t.Fatalf("unexpected chunk after response.failed: payload=%q err=%v result_err=%v", chunk.Payload, chunk.Err, chunk.ResultErr)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("stream did not close after response.failed")
	}
	if got := exec.UpstreamGeneration(sessionID); got != 1 {
		t.Fatalf("upstream generation = %d, want 1", got)
	}
	select {
	case errRead, ok := <-disconnectCh:
		t.Fatalf("unexpected disconnect signal ok=%t err=%v", ok, errRead)
	default:
	}
	entries := hook.AllEntries()
	if len(entries) == 0 {
		t.Fatal("expected response.failed lifecycle log entry")
	}
	for _, entry := range entries {
		if strings.Contains(entry.Message, originalPromptCacheKey) {
			t.Fatalf("response.failed log leaked original prompt cache key: %q", entry.Message)
		}
		for key, value := range entry.Data {
			if strings.Contains(fmt.Sprint(value), originalPromptCacheKey) {
				t.Fatalf("response.failed log field %q leaked original prompt cache key: %v", key, value)
			}
		}
	}
}

func TestCodexWebsocketsExecuteStreamTerminatesOnResponseIncompleteForDownstreamWebsocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	incompletePayload := []byte(`{"type":"response.incomplete","response":{"id":"resp-1","status":"incomplete","incomplete_details":{"reason":"content_filter"}}}`)
	upstreamClosed := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		if errWrite := conn.WriteMessage(websocket.TextMessage, incompletePayload); errWrite != nil {
			t.Errorf("write response.incomplete websocket message: %v", errWrite)
			return
		}
		_, _, errRead := conn.ReadMessage()
		upstreamClosed <- errRead
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-response-incomplete-downstream"
	defer exec.CloseExecutionSession(sessionID)
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	auth := &cliproxyauth.Auth{ID: "auth-response-incomplete", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"hello"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx = cliproxyexecutor.WithDownstreamWebsocket(ctx)

	result, err := exec.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed before response.incomplete chunk")
		}
		if chunk.Err != nil {
			t.Fatalf("response.incomplete chunk error = %v", chunk.Err)
		}
		if !bytes.Equal(bytes.TrimSpace(chunk.Payload), incompletePayload) {
			t.Fatalf("response.incomplete chunk = %q, want %q", chunk.Payload, incompletePayload)
		}
		if chunk.ResultErr == nil {
			t.Fatal("response.incomplete chunk ResultErr = nil")
		}
		if got := statusCodeFromTestError(t, chunk.ResultErr); got != http.StatusBadRequest {
			t.Fatalf("response.incomplete status = %d, want %d", got, http.StatusBadRequest)
		}
		if !strings.Contains(chunk.ResultErr.Error(), "Incomplete response returned, reason: content_filter") {
			t.Fatalf("response.incomplete result error = %v", chunk.ResultErr)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for response.incomplete chunk")
	}

	select {
	case chunk, ok := <-result.Chunks:
		if ok {
			t.Fatalf("unexpected chunk after response.incomplete: payload=%q err=%v result_err=%v", chunk.Payload, chunk.Err, chunk.ResultErr)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("stream did not close after response.incomplete")
	}
	if got := exec.UpstreamGeneration(sessionID); got != 1 {
		t.Fatalf("upstream generation = %d, want 1", got)
	}
	select {
	case errRead := <-upstreamClosed:
		if errRead == nil {
			t.Fatal("upstream connection remained open after response.incomplete")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("upstream connection was not dropped after response.incomplete")
	}
	select {
	case errDisconnect, ok := <-disconnectCh:
		t.Fatalf("unexpected downstream disconnect signal ok=%t err=%v", ok, errDisconnect)
	default:
	}
}

func TestCodexWebsocketsExecuteExposesResponseFailedIdentity(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	const originalPromptCacheKey = "cache-response-failed-nonstream"
	confusedPromptCacheKeyCh := make(chan string, 1)
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()

		_, upstreamPayload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		confusedPromptCacheKey := strings.TrimSpace(gjson.GetBytes(upstreamPayload, "prompt_cache_key").String())
		if confusedPromptCacheKey == "" || confusedPromptCacheKey == originalPromptCacheKey {
			t.Errorf("upstream prompt_cache_key was not confused: %s", upstreamPayload)
			return
		}
		confusedPromptCacheKeyCh <- confusedPromptCacheKey
		failedPayload := []byte(fmt.Sprintf(`{"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"type":"server_error","message":%q}}}`, "upstream failed for "+confusedPromptCacheKey+":0"))
		if errWrite := conn.WriteMessage(websocket.TextMessage, failedPayload); errWrite != nil {
			t.Errorf("write response.failed websocket message: %v", errWrite)
			return
		}
		<-done
	}))
	defer server.Close()
	defer close(done)

	exec := NewCodexWebsocketsExecutor(&config.Config{
		SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll},
		Routing:   config.RoutingConfig{SessionAffinity: true},
		Codex:     config.CodexConfig{IdentityConfuse: true},
	})
	sessionID := "sess-response-failed-nonstream"
	defer exec.CloseExecutionSession(sessionID)
	usagePlugin := &captureCodexUsagePlugin{records: make(chan usage.Record, 4)}
	usage.RegisterNamedPlugin("codex-websocket-response-failed-identity", usagePlugin)
	auth := &cliproxyauth.Auth{ID: "auth-response-failed-nonstream", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","prompt_cache_key":"cache-response-failed-nonstream","input":[{"type":"message","role":"user","content":"hello"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai-response"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}

	_, errExecute := exec.Execute(context.Background(), auth, req, opts)
	if errExecute == nil {
		t.Fatal("Execute() error = nil, want response.failed")
	}
	var confusedPromptCacheKey string
	select {
	case confusedPromptCacheKey = <-confusedPromptCacheKeyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for confused prompt cache key")
	}
	if !strings.Contains(errExecute.Error(), originalPromptCacheKey) {
		t.Fatalf("Execute() error = %v, want exposed prompt cache key", errExecute)
	}
	if strings.Contains(errExecute.Error(), confusedPromptCacheKey) {
		t.Fatalf("Execute() error leaked confused prompt cache key: %v", errExecute)
	}
	for {
		select {
		case record := <-usagePlugin.records:
			if record.Provider != "codex" || record.AuthID != auth.ID {
				continue
			}
			if strings.Contains(record.Fail.Body, originalPromptCacheKey) {
				t.Fatalf("usage failure leaked original prompt cache key: %s", record.Fail.Body)
			}
			if !strings.Contains(record.Fail.Body, confusedPromptCacheKey) {
				t.Fatalf("usage failure = %q, want confused prompt cache key", record.Fail.Body)
			}
			return
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for Codex usage failure")
		}
	}
}

func TestCodexWebsocketsExecuteStreamRecreatesConnectionWhenAuthChanges(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connectionCount atomic.Int32
	authHeaders := make(chan string, 2)
	payloads := make(chan []byte, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connectionIndex := connectionCount.Add(1)
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		authHeaders <- r.Header.Get("Authorization")

		for {
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				return
			}
			payloads <- bytes.Clone(payload)
			completed := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp-%d","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`, connectionIndex))
			if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
				t.Errorf("write completed websocket message: %v", errWrite)
				return
			}
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-auth-switch"
	defer exec.CloseExecutionSession(sessionID)
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}
	authA := &cliproxyauth.Auth{ID: "auth-a", Attributes: map[string]string{"api_key": "token-a", "base_url": server.URL}}
	authB := &cliproxyauth.Auth{ID: "auth-b", Attributes: map[string]string{"api_key": "token-b", "base_url": server.URL}}

	firstPayload := []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"first"}]}`)
	firstResult, errFirst := exec.ExecuteStream(ctx, authA, cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: firstPayload}, opts)
	if errFirst != nil {
		t.Fatalf("first ExecuteStream() error = %v", errFirst)
	}
	for chunk := range firstResult.Chunks {
		if chunk.Err != nil {
			t.Fatalf("first stream error = %v", chunk.Err)
		}
	}

	incrementalPayload := []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","role":"user","content":"second"}]}`)
	if result, errSwitch := exec.ExecuteStream(ctx, authB, cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: incrementalPayload}, opts); errSwitch == nil {
		if result != nil {
			for range result.Chunks {
			}
		}
		t.Fatal("auth switch with previous_response_id must require transcript replay")
	} else {
		var replayRequired interface {
			CodexWebsocketReplayRequired() bool
		}
		if !errors.As(errSwitch, &replayRequired) || replayRequired == nil || !replayRequired.CodexWebsocketReplayRequired() {
			t.Fatalf("auth switch error = %v, want replay-required marker", errSwitch)
		}
	}

	replayPayload := []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"first"},{"type":"message","role":"assistant","content":"done"},{"type":"message","role":"user","content":"second"}]}`)
	replayResult, errReplay := exec.ExecuteStream(ctx, authB, cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: replayPayload}, opts)
	if errReplay != nil {
		t.Fatalf("replay ExecuteStream() error = %v", errReplay)
	}
	for chunk := range replayResult.Chunks {
		if chunk.Err != nil {
			t.Fatalf("replay stream error = %v", chunk.Err)
		}
	}

	if got := connectionCount.Load(); got != 2 {
		t.Fatalf("upstream connection count = %d, want 2", got)
	}
	for i, want := range []string{"Bearer token-a", "Bearer token-b"} {
		select {
		case got := <-authHeaders:
			if got != want {
				t.Fatalf("connection %d Authorization = %q, want %q", i+1, got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for connection %d headers", i+1)
		}
	}
	for i := range 2 {
		select {
		case payload := <-payloads:
			if i == 1 && gjson.GetBytes(payload, "previous_response_id").Exists() {
				t.Fatalf("replay payload retained previous_response_id: %s", payload)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for upstream payload %d", i+1)
		}
	}
	select {
	case payload := <-payloads:
		t.Fatalf("stale incremental payload reached upstream: %s", payload)
	default:
	}
	if got := exec.UpstreamGeneration(sessionID); got != 1 {
		t.Fatalf("upstream generation = %d, want 1", got)
	}
}

func TestCodexWebsocketsExecuteStreamSurfacesResponseFailedForNonWebsocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	done := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		failedPayload := []byte(`{"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"type":"invalid_request_error","message":"bad request"}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, failedPayload); errWrite != nil {
			t.Errorf("write response.failed websocket message: %v", errWrite)
			return
		}
		<-done
	}))
	defer server.Close()
	defer close(done)

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"hello"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
	}

	result, err := exec.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed before response.failed error")
		}
		if chunk.Err == nil {
			t.Fatalf("response.failed chunk Err = nil, payload=%q", chunk.Payload)
		}
		if got := statusCodeFromTestError(t, chunk.Err); got != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d; err=%v", got, http.StatusBadRequest, chunk.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for response.failed error")
	}
}

func TestCodexWebsocketsExecuteStreamSurfacesResponseIncompleteForNonWebsocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	releaseUpstream := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		incompletePayload := []byte(`{"type":"response.incomplete","response":{"id":"resp-1","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, incompletePayload); errWrite != nil {
			t.Errorf("write response.incomplete websocket message: %v", errWrite)
			return
		}
		<-releaseUpstream
	}))
	defer server.Close()
	defer close(releaseUpstream)

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"hello"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
	}

	result, err := exec.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed before response.incomplete error")
		}
		if len(bytes.TrimSpace(chunk.Payload)) != 0 || chunk.ResultErr != nil || chunk.Err == nil {
			t.Fatalf("response.incomplete chunk = payload %q err=%v result_err=%v", chunk.Payload, chunk.Err, chunk.ResultErr)
		}
		if got := statusCodeFromTestError(t, chunk.Err); got != http.StatusBadRequest {
			t.Fatalf("response.incomplete status = %d, want %d", got, http.StatusBadRequest)
		}
		if !strings.Contains(chunk.Err.Error(), "Incomplete response returned, reason: max_output_tokens") {
			t.Fatalf("response.incomplete error = %v", chunk.Err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for response.incomplete error")
	}
}

func TestCodexWebsocketsExecutePreviousResponseNotFoundDropsQuietly(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("request path = %s, want /responses", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade websocket: %v", err)
		}
		defer func() { _ = conn.Close() }()

		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Fatalf("read upstream websocket message: %v", errRead)
		}
		errPayload := []byte(`{"type":"error","status":400,"error":{"code":"previous_response_not_found","message":"previous response with id 'resp-1' not found"}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, errPayload); errWrite != nil {
			t.Fatalf("write websocket error: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-previous-response-missing"
	defer exec.CloseExecutionSession(sessionID)
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	if disconnectCh == nil {
		t.Fatal("expected disconnect channel")
	}

	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-1"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}

	if _, err := exec.Execute(context.Background(), auth, req, opts); err == nil || !strings.Contains(err.Error(), "previous_response_not_found") {
		t.Fatalf("Execute() error = %v, want previous_response_not_found", err)
	}
	if got := exec.UpstreamGeneration(sessionID); got != 1 {
		t.Fatalf("upstream generation = %d, want 1", got)
	}
	select {
	case errRead, ok := <-disconnectCh:
		t.Fatalf("unexpected disconnect signal ok=%t err=%v", ok, errRead)
	default:
	}
}

func TestCodexWebsocketsExecuteStreamRecoverableUpstreamErrorDropsQuietly(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("request path = %s, want /responses", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade websocket: %v", err)
		}
		defer func() { _ = conn.Close() }()

		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Fatalf("read upstream websocket message: %v", errRead)
		}
		errPayload := []byte(`{"type":"error","status":429,"error":{"code":"rate_limit_exceeded","message":"rate limited"}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, errPayload); errWrite != nil {
			t.Fatalf("write websocket error: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-recoverable-upstream-error"
	defer exec.CloseExecutionSession(sessionID)
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	if disconnectCh == nil {
		t.Fatal("expected disconnect channel")
	}

	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","id":"msg-1"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}

	result, err := exec.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	if result == nil || result.Chunks == nil {
		t.Fatal("expected stream result")
	}

	var chunk cliproxyexecutor.StreamChunk
	select {
	case chunk = <-result.Chunks:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream error chunk")
	}
	if chunk.Err == nil {
		t.Fatalf("stream chunk error = nil, want 429")
	}
	status, ok := chunk.Err.(interface{ StatusCode() int })
	if !ok || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("stream chunk error = %T %v, want 429 status", chunk.Err, chunk.Err)
	}
	if got := exec.UpstreamGeneration(sessionID); got != 1 {
		t.Fatalf("upstream generation = %d, want 1", got)
	}
	select {
	case errRead, ok := <-disconnectCh:
		t.Fatalf("unexpected disconnect signal ok=%t err=%v", ok, errRead)
	default:
	}
}

func TestCodexWebsocketsUpstreamDisconnectChanSignalsOnInvalidate(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	exec := NewCodexWebsocketsExecutor(&config.Config{})
	exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	sessionID := "sess-1"
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	if disconnectCh == nil {
		t.Fatal("expected disconnect channel")
	}
	if got := exec.UpstreamGeneration(sessionID); got != 0 {
		t.Fatalf("initial upstream generation = %d, want 0", got)
	}

	sess := exec.getOrCreateSession(sessionID)
	if sess == nil {
		t.Fatal("expected session")
	}
	sess.connMu.Lock()
	sess.conn = conn
	sess.authID = "auth-1"
	sess.wsURL = "ws://example.test/responses"
	sess.readerConn = conn
	sess.connMu.Unlock()

	upstreamErr := errors.New("upstream gone")
	exec.invalidateUpstreamConn(sess, conn, "test_invalidate", upstreamErr)

	select {
	case errRead, ok := <-disconnectCh:
		if !ok {
			t.Fatal("expected disconnect channel to deliver error before closing")
		}
		if errRead == nil || errRead.Error() != upstreamErr.Error() {
			t.Fatalf("disconnect error = %v, want %v", errRead, upstreamErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for disconnect signal")
	}
}

func TestCodexWebsocketsDropUpstreamConnQuietlyDoesNotSignalDisconnect(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	exec := NewCodexWebsocketsExecutor(&config.Config{})
	sessionID := "sess-quiet"
	defer exec.CloseExecutionSession(sessionID)
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	if disconnectCh == nil {
		t.Fatal("expected disconnect channel")
	}

	sess := exec.getOrCreateSession(sessionID)
	if sess == nil {
		t.Fatal("expected session")
	}
	sess.connMu.Lock()
	sess.conn = conn
	sess.authID = "auth-1"
	sess.wsURL = "ws://example.test/responses"
	sess.readerConn = conn
	sess.connMu.Unlock()

	exec.dropUpstreamConn(sess, conn, "upstream_disconnected", errors.New("idle timeout"), false)

	select {
	case errRead, ok := <-disconnectCh:
		t.Fatalf("unexpected disconnect signal ok=%t err=%v", ok, errRead)
	default:
	}

	sess.connMu.Lock()
	current := sess.conn
	readerConn := sess.readerConn
	sess.connMu.Unlock()
	if current != nil || readerConn != nil {
		t.Fatalf("expected upstream connection to be cleared")
	}
	if got := exec.UpstreamGeneration(sessionID); got != 1 {
		t.Fatalf("upstream generation after quiet drop = %d, want 1", got)
	}
}

func TestCodexWebsocketsDropUpstreamSessionQuietly(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	exec := NewCodexWebsocketsExecutor(&config.Config{})
	sessionID := "sess-drop-session"
	defer exec.CloseExecutionSession(sessionID)
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	if disconnectCh == nil {
		t.Fatal("expected disconnect channel")
	}
	sess := exec.getOrCreateSession(sessionID)
	if sess == nil {
		t.Fatal("expected session")
	}
	sess.connMu.Lock()
	sess.conn = conn
	sess.authID = "auth-1"
	sess.wsURL = "ws://example.test/responses"
	sess.readerConn = conn
	sess.connMu.Unlock()

	exec.DropUpstreamSession(sessionID, "compact_replay")

	select {
	case errRead, ok := <-disconnectCh:
		t.Fatalf("unexpected disconnect signal ok=%t err=%v", ok, errRead)
	default:
	}
	sess.connMu.Lock()
	current := sess.conn
	readerConn := sess.readerConn
	sess.connMu.Unlock()
	if current != nil || readerConn != nil {
		t.Fatalf("expected upstream connection to be cleared")
	}
	if got := exec.UpstreamGeneration(sessionID); got != 1 {
		t.Fatalf("upstream generation after session drop = %d, want 1", got)
	}
}

func TestCodexAutoExecutorDropUpstreamSessionForwardsToWebsocketExecutor(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	exec := NewCodexAutoExecutor(&config.Config{})
	sessionID := "sess-auto-drop-session"
	defer exec.CloseExecutionSession(sessionID)
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	if disconnectCh == nil {
		t.Fatal("expected disconnect channel")
	}
	sess := exec.wsExec.getOrCreateSession(sessionID)
	if sess == nil {
		t.Fatal("expected websocket session")
	}
	sess.connMu.Lock()
	sess.conn = conn
	sess.authID = "auth-1"
	sess.wsURL = "ws://example.test/responses"
	sess.readerConn = conn
	sess.connMu.Unlock()

	exec.DropUpstreamSession(sessionID, "compact_replay")

	select {
	case errRead, ok := <-disconnectCh:
		t.Fatalf("unexpected disconnect signal ok=%t err=%v", ok, errRead)
	default:
	}
	sess.connMu.Lock()
	current := sess.conn
	readerConn := sess.readerConn
	sess.connMu.Unlock()
	if current != nil || readerConn != nil {
		t.Fatalf("expected upstream connection to be cleared")
	}
	if got := exec.UpstreamGeneration(sessionID); got != 1 {
		t.Fatalf("upstream generation after auto session drop = %d, want 1", got)
	}
}

func TestCodexWebsocketsSendResponseProcessedUsesExistingUpstream(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	captured := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Errorf("read websocket message: %v", errRead)
			return
		}
		captured <- bytes.Clone(payload)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	exec := NewCodexWebsocketsExecutor(&config.Config{})
	sessionID := "sess-response-processed"
	defer exec.CloseExecutionSession(sessionID)
	sess := exec.getOrCreateSession(sessionID)
	if sess == nil {
		t.Fatal("expected session")
	}
	sess.connMu.Lock()
	sess.conn = conn
	sess.authID = "auth-1"
	sess.wsURL = "ws://example.test/responses"
	sess.readerConn = conn
	sess.connMu.Unlock()

	if errSend := exec.SendResponseProcessed(sessionID, "resp-processed-1"); errSend != nil {
		t.Fatalf("SendResponseProcessed error: %v", errSend)
	}

	select {
	case payload := <-captured:
		if got := gjson.GetBytes(payload, "type").String(); got != "response.processed" {
			t.Fatalf("type = %q, want response.processed; payload=%s", got, string(payload))
		}
		if got := gjson.GetBytes(payload, "response_id").String(); got != "resp-processed-1" {
			t.Fatalf("response_id = %q, want resp-processed-1; payload=%s", got, string(payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for response.processed upstream frame")
	}
}

func TestCloseCodexWebsocketSessionSignalsActiveReader(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	sess := &codexWebsocketSession{sessionID: "sess-close-active"}
	readCh := make(chan codexWebsocketRead, 1)
	sess.connMu.Lock()
	sess.conn = conn
	sess.authID = "auth-1"
	sess.wsURL = "ws://example.test/responses"
	sess.readerConn = conn
	sess.connMu.Unlock()
	sess.setActive(conn, readCh)

	closeCodexWebsocketSession(sess, "test_close")

	select {
	case ev, ok := <-readCh:
		if !ok {
			t.Fatalf("expected active reader error before close")
		}
		if ev.err == nil || !strings.Contains(ev.err.Error(), "execution session closed") {
			t.Fatalf("active reader error = %v, want execution session closed", ev.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for active reader close signal")
	}

	sess.connMu.Lock()
	current := sess.conn
	readerConn := sess.readerConn
	sess.connMu.Unlock()
	if current != nil || readerConn != nil {
		t.Fatalf("expected session websocket connection to be cleared")
	}
}

func TestReadCodexWebsocketMessageDrainsBufferedPayloadAfterActiveDone(t *testing.T) {
	conn := &websocket.Conn{}
	sess := &codexWebsocketSession{sessionID: "sess-buffered-active-done"}
	readCh := make(chan codexWebsocketRead, 2)
	sess.connMu.Lock()
	sess.conn = conn
	sess.readerConn = conn
	sess.connMu.Unlock()
	sess.setActive(conn, readCh)

	readCh <- codexWebsocketRead{conn: conn, msgType: websocket.TextMessage, payload: []byte(`{"type":"response.completed"}`)}
	sess.closeActiveReadForConn(conn, errors.New("upstream closed"))

	msgType, payload, err := readCodexWebsocketMessage(context.Background(), sess, conn, readCh)
	if err != nil {
		t.Fatalf("readCodexWebsocketMessage() error = %v", err)
	}
	if msgType != websocket.TextMessage {
		t.Fatalf("message type = %d, want text", msgType)
	}
	if string(payload) != `{"type":"response.completed"}` {
		t.Fatalf("payload = %s, want buffered completed", payload)
	}

	_, _, err = readCodexWebsocketMessage(context.Background(), sess, conn, readCh)
	if err == nil || !strings.Contains(err.Error(), "upstream closed") {
		t.Fatalf("second read error = %v, want upstream closed", err)
	}
}

func TestReadCodexWebsocketMessagePreservesClosedErrorWhenBufferFull(t *testing.T) {
	conn := &websocket.Conn{}
	sess := &codexWebsocketSession{sessionID: "sess-buffered-full-active-done"}
	readCh := make(chan codexWebsocketRead, 1)
	sess.connMu.Lock()
	sess.conn = conn
	sess.readerConn = conn
	sess.connMu.Unlock()
	sess.setActive(conn, readCh)

	readCh <- codexWebsocketRead{conn: conn, msgType: websocket.TextMessage, payload: []byte(`{"type":"response.created"}`)}
	sess.closeActiveReadForConn(conn, errors.New("upstream closed"))

	msgType, payload, err := readCodexWebsocketMessage(context.Background(), sess, conn, readCh)
	if err != nil {
		t.Fatalf("readCodexWebsocketMessage() error = %v", err)
	}
	if msgType != websocket.TextMessage {
		t.Fatalf("message type = %d, want text", msgType)
	}
	if string(payload) != `{"type":"response.created"}` {
		t.Fatalf("payload = %s, want buffered created", payload)
	}

	_, _, err = readCodexWebsocketMessage(context.Background(), sess, conn, readCh)
	if err == nil || !strings.Contains(err.Error(), "upstream closed") {
		t.Fatalf("second read error = %v, want preserved upstream closed", err)
	}
}

func TestReadCodexWebsocketMessagePrefersTerminalErrorOverUpstreamReset(t *testing.T) {
	conn := &websocket.Conn{}
	sess := &codexWebsocketSession{sessionID: "sess-terminal-over-reset"}
	readCh := make(chan codexWebsocketRead, 1)
	sess.connMu.Lock()
	sess.conn = conn
	sess.readerConn = conn
	sess.connMu.Unlock()
	sess.setActive(conn, readCh)

	sess.closeActiveReadForConn(conn, codexWebsocketUpstreamResetError{cause: errors.New("read tcp: i/o timeout")})
	sessionClosedErr := errors.New("codex websockets executor: execution session closed")
	sess.markTerminalError(sessionClosedErr)

	_, _, err := readCodexWebsocketMessage(context.Background(), sess, conn, readCh)
	if err == nil || !strings.Contains(err.Error(), "execution session closed") {
		t.Fatalf("read error = %v, want execution session closed", err)
	}
	var replayRequired interface {
		CodexWebsocketReplayRequired() bool
	}
	if errors.As(err, &replayRequired) && replayRequired != nil && replayRequired.CodexWebsocketReplayRequired() {
		t.Fatalf("read error = %v, must not be replay-required marker", err)
	}
}

func TestCloseExecutionSessionOverridesDroppedUpstreamReset(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-close-after-drop"
	sess := exec.getOrCreateSession(sessionID)
	readCh := make(chan codexWebsocketRead, 1)
	sess.connMu.Lock()
	sess.conn = conn
	sess.readerConn = conn
	sess.authID = "auth-1"
	sess.wsURL = wsURL
	sess.connMu.Unlock()
	sess.setActive(conn, readCh)

	resetErr := codexWebsocketUpstreamResetError{cause: errors.New("read tcp: i/o timeout")}
	exec.dropUpstreamConn(sess, conn, "upstream_disconnected", resetErr, false)
	sess.closeActiveReadForConn(conn, resetErr)
	exec.CloseExecutionSession(sessionID)

	_, _, err = readCodexWebsocketMessage(context.Background(), sess, conn, readCh)
	if err == nil || !strings.Contains(err.Error(), "execution session closed") {
		t.Fatalf("read error = %v, want execution session closed", err)
	}
	var upstreamReset codexWebsocketUpstreamResetError
	if errors.As(err, &upstreamReset) {
		t.Fatalf("read error = %v, must not expose upstream reset after session close", err)
	}
	var replayRequired interface {
		CodexWebsocketReplayRequired() bool
	}
	if errors.As(err, &replayRequired) && replayRequired != nil && replayRequired.CodexWebsocketReplayRequired() {
		t.Fatalf("read error = %v, must not be replay-required marker", err)
	}
}

func TestCodexWebsocketsExecuteSendErrorWithPreviousResponseIDRequiresReplay(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	accepted := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		accepted <- struct{}{}
		msgType, _, errRead := conn.ReadMessage()
		if errRead != nil {
			return
		}
		if msgType != websocket.TextMessage {
			t.Errorf("message type = %d, want text", msgType)
			return
		}
		completed := []byte(`{"type":"response.completed","response":{"id":"resp-retry","output":[]}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Errorf("write completed websocket message: %v", errWrite)
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	staleConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial stale websocket: %v", err)
	}
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stale websocket accept")
	}
	if errClose := staleConn.Close(); errClose != nil {
		t.Fatalf("close stale websocket: %v", errClose)
	}

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-execute-send-replay-required"
	defer exec.CloseExecutionSession(sessionID)
	sess := exec.getOrCreateSession(sessionID)
	sess.connMu.Lock()
	sess.conn = staleConn
	sess.connCloser = newWebsocketConnectionCloser(staleConn)
	sess.readerConn = staleConn
	sess.authID = "auth-1"
	sess.wsURL = wsURL + "/responses"
	sess.connMu.Unlock()

	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}

	_, err = exec.Execute(context.Background(), auth, req, opts)
	if err == nil {
		t.Fatalf("Execute() error = nil, want replay-required error")
	}
	var replayRequired interface {
		CodexWebsocketReplayRequired() bool
	}
	if !errors.As(err, &replayRequired) || replayRequired == nil || !replayRequired.CodexWebsocketReplayRequired() {
		t.Fatalf("Execute() error = %v, want replay-required marker", err)
	}
	if got := exec.UpstreamGeneration(sessionID); got != 1 {
		t.Fatalf("upstream generation after send error = %d, want 1", got)
	}
	select {
	case <-accepted:
		t.Fatalf("unexpected retry dial with stale previous_response_id")
	default:
	}
}

func TestCodexWebsocketsExecuteStreamSendErrorWithPreviousResponseIDRequiresReplay(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	accepted := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		accepted <- struct{}{}
		msgType, _, errRead := conn.ReadMessage()
		if errRead != nil {
			return
		}
		if msgType != websocket.TextMessage {
			t.Errorf("message type = %d, want text", msgType)
			return
		}
		completed := []byte(`{"type":"response.completed","response":{"id":"resp-retry","output":[]}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Errorf("write completed websocket message: %v", errWrite)
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	staleConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial stale websocket: %v", err)
	}
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stale websocket accept")
	}
	if errClose := staleConn.Close(); errClose != nil {
		t.Fatalf("close stale websocket: %v", errClose)
	}

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-send-replay-required"
	defer exec.CloseExecutionSession(sessionID)
	sess := exec.getOrCreateSession(sessionID)
	sess.connMu.Lock()
	sess.conn = staleConn
	sess.connCloser = newWebsocketConnectionCloser(staleConn)
	sess.readerConn = staleConn
	sess.authID = "auth-1"
	sess.wsURL = wsURL + "/responses"
	sess.connMu.Unlock()

	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}

	_, err = exec.ExecuteStream(context.Background(), auth, req, opts)
	if err == nil {
		t.Fatalf("ExecuteStream() error = nil, want replay-required error")
	}
	var replayRequired interface {
		CodexWebsocketReplayRequired() bool
	}
	if !errors.As(err, &replayRequired) || replayRequired == nil || !replayRequired.CodexWebsocketReplayRequired() {
		t.Fatalf("ExecuteStream() error = %v, want replay-required marker", err)
	}
	if got := exec.UpstreamGeneration(sessionID); got != 1 {
		t.Fatalf("upstream generation after send error = %d, want 1", got)
	}
	select {
	case <-accepted:
		t.Fatalf("unexpected retry dial with stale previous_response_id")
	default:
	}
}

func TestCodexWebsocketsExecuteStreamSendRetryReturnsHandshakeHeaders(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	accepted := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers := make(http.Header)
		headers.Set("X-Codex-Turn-State", "state-retry")
		conn, err := upgrader.Upgrade(w, r, headers)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		accepted <- struct{}{}
		msgType, _, errRead := conn.ReadMessage()
		if errRead != nil {
			return
		}
		if msgType != websocket.TextMessage {
			t.Errorf("message type = %d, want text", msgType)
			return
		}
		completed := []byte(`{"type":"response.completed","response":{"id":"resp-retry","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Errorf("write completed websocket message: %v", errWrite)
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	staleConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial stale websocket: %v", err)
	}
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stale websocket accept")
	}
	if errClose := staleConn.Close(); errClose != nil {
		t.Fatalf("close stale websocket: %v", errClose)
	}

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-send-retry-headers"
	defer exec.CloseExecutionSession(sessionID)
	sess := exec.getOrCreateSession(sessionID)
	sess.connMu.Lock()
	sess.conn = staleConn
	sess.connCloser = newWebsocketConnectionCloser(staleConn)
	sess.readerConn = staleConn
	sess.authID = "auth-1"
	sess.wsURL = wsURL + "/responses"
	sess.connMu.Unlock()

	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","id":"msg-1"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())

	result, err := exec.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	if got := result.Headers.Get("X-Codex-Turn-State"); got != "state-retry" {
		t.Fatalf("retry handshake turn-state = %q, want state-retry", got)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
	}
}

func TestCodexWebsocketsExecuteStreamCarriesRecreatedHandshakeHeadersIntoReplay(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	accepted := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers := make(http.Header)
		headers.Set("X-Codex-Turn-State", "state-recreated")
		conn, err := upgrader.Upgrade(w, r, headers)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		accepted <- struct{}{}
		for range 2 {
			msgType, _, errRead := conn.ReadMessage()
			if errRead != nil {
				return
			}
			if msgType != websocket.TextMessage {
				t.Errorf("message type = %d, want text", msgType)
				return
			}
			completed := []byte(`{"type":"response.completed","response":{"id":"resp-replay","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
			if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
				t.Errorf("write completed websocket message: %v", errWrite)
				return
			}
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-recreated-replay-headers"
	defer exec.CloseExecutionSession(sessionID)
	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())

	requestWithPreviousResponse := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"type":"response.create","model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`),
	}
	_, err := exec.ExecuteStream(ctx, auth, requestWithPreviousResponse, opts)
	if err == nil {
		t.Fatal("ExecuteStream() error = nil, want replay-required error")
	}
	var replayRequired interface {
		CodexWebsocketReplayRequired() bool
	}
	if !errors.As(err, &replayRequired) || replayRequired == nil || !replayRequired.CodexWebsocketReplayRequired() {
		t.Fatalf("ExecuteStream() error = %v, want replay-required marker", err)
	}

	replayRequest := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","id":"msg-1"},{"type":"message","id":"msg-2"}]}`),
	}
	result, err := exec.ExecuteStream(ctx, auth, replayRequest, opts)
	if err != nil {
		t.Fatalf("replay ExecuteStream() error = %v", err)
	}
	if got := result.Headers.Get("X-Codex-Turn-State"); got != "state-recreated" {
		t.Fatalf("recreated handshake turn-state = %q, want state-recreated", got)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
	}
	nextRequest := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","id":"msg-3"}]}`),
	}
	nextResult, err := exec.ExecuteStream(ctx, auth, nextRequest, opts)
	if err != nil {
		t.Fatalf("next ExecuteStream() error = %v", err)
	}
	if got := nextResult.Headers.Get("X-Codex-Turn-State"); got != "" {
		t.Fatalf("recreated handshake turn-state was replayed twice: %q", got)
	}
	for chunk := range nextResult.Chunks {
		if chunk.Err != nil {
			t.Fatalf("next stream chunk error = %v", chunk.Err)
		}
	}

	select {
	case <-accepted:
	default:
		t.Fatal("upstream websocket was not created")
	}
	select {
	case <-accepted:
		t.Fatal("transcript replay opened an unnecessary second websocket")
	default:
	}
}

func TestCodexWebsocketsExecuteStreamMissingUpstreamWithPreviousResponseIDRequiresReplay(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstreamPayload := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
		msgType, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			return
		}
		if msgType != websocket.TextMessage {
			t.Errorf("message type = %d, want text", msgType)
			return
		}
		upstreamPayload <- bytes.Clone(payload)
		completed := []byte(`{"type":"response.completed","response":{"id":"resp-new","output":[]}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Errorf("write completed websocket message: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-missing-upstream-replay-required"
	defer exec.CloseExecutionSession(sessionID)
	sess := exec.getOrCreateSession(sessionID)
	sess.connMu.Lock()
	sess.upstreamGeneration = 1
	sess.conn = nil
	sess.readerConn = nil
	sess.connMu.Unlock()

	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}

	_, err := exec.ExecuteStream(cliproxyexecutor.WithDownstreamWebsocket(context.Background()), auth, req, opts)
	if err == nil {
		t.Fatalf("ExecuteStream() error = nil, want replay-required error")
	}
	var replayRequired interface {
		CodexWebsocketReplayRequired() bool
	}
	if !errors.As(err, &replayRequired) || replayRequired == nil || !replayRequired.CodexWebsocketReplayRequired() {
		t.Fatalf("ExecuteStream() error = %v, want replay-required marker", err)
	}
	select {
	case payload := <-upstreamPayload:
		t.Fatalf("stale previous_response_id request was sent to fresh upstream: %s", payload)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestCodexWebsocketsExecuteStreamReadErrorWithPreviousResponseIDRequiresReplay(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	accepted := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		accepted <- struct{}{}
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			_ = conn.Close()
			return
		}
		_ = conn.Close()
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-read-replay-required"
	defer exec.CloseExecutionSession(sessionID)

	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}

	result, err := exec.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v, want stream result", err)
	}
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for websocket accept")
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed without replay-required error")
		}
		if chunk.Err == nil {
			t.Fatalf("stream chunk error = nil, want replay-required error")
		}
		var replayRequired interface {
			CodexWebsocketReplayRequired() bool
		}
		if !errors.As(chunk.Err, &replayRequired) || replayRequired == nil || !replayRequired.CodexWebsocketReplayRequired() {
			t.Fatalf("stream chunk error = %v, want replay-required marker", chunk.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stream replay-required error")
	}
	if got := exec.UpstreamGeneration(sessionID); got != 1 {
		t.Fatalf("upstream generation after read error = %d, want 1", got)
	}
}

func TestCodexWebsocketsExecuteStreamReadErrorWithoutPreviousResponseIDDoesNotRequireReplay(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	accepted := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		accepted <- struct{}{}
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			_ = conn.Close()
			return
		}
		_ = conn.Close()
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-read-reset-no-replay"
	defer exec.CloseExecutionSession(sessionID)

	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","id":"msg-1"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}

	result, err := exec.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v, want stream result", err)
	}
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for websocket accept")
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed without read error")
		}
		if chunk.Err == nil {
			t.Fatalf("stream chunk error = nil, want read error")
		}
		var replayRequired interface {
			CodexWebsocketReplayRequired() bool
		}
		if errors.As(chunk.Err, &replayRequired) && replayRequired != nil && replayRequired.CodexWebsocketReplayRequired() {
			t.Fatalf("stream chunk error = %v, must not be replay-required marker", chunk.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stream read error")
	}
	if got := exec.UpstreamGeneration(sessionID); got != 1 {
		t.Fatalf("upstream generation after read error = %d, want 1", got)
	}
}

func TestCodexWebsocketsExecuteReadErrorWithPreviousResponseIDRequiresReplay(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	accepted := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		accepted <- struct{}{}
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			_ = conn.Close()
			return
		}
		_ = conn.Close()
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-execute-read-replay-required"
	defer exec.CloseExecutionSession(sessionID)

	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := exec.Execute(context.Background(), auth, req, opts)
		errCh <- err
	}()

	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for websocket accept")
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Execute() error = nil, want replay-required error")
		}
		var replayRequired interface {
			CodexWebsocketReplayRequired() bool
		}
		if !errors.As(err, &replayRequired) || replayRequired == nil || !replayRequired.CodexWebsocketReplayRequired() {
			t.Fatalf("Execute() error = %v, want replay-required marker", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Execute replay-required error")
	}
	if got := exec.UpstreamGeneration(sessionID); got != 1 {
		t.Fatalf("upstream generation after read error = %d, want 1", got)
	}
}

func TestCodexWebsocketsExecuteStreamUnexpectedBinaryDoesNotRequireReplay(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	receivedRequest := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			return
		}
		receivedRequest <- struct{}{}
		if errWrite := conn.WriteMessage(websocket.BinaryMessage, []byte("unexpected")); errWrite != nil {
			t.Errorf("write binary websocket message: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-stream-binary-no-replay"
	defer exec.CloseExecutionSession(sessionID)

	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}

	result, err := exec.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v, want stream result", err)
	}
	select {
	case <-receivedRequest:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for websocket request")
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed without unexpected binary error")
		}
		if chunk.Err == nil || !strings.Contains(chunk.Err.Error(), "unexpected binary message") {
			t.Fatalf("stream chunk error = %v, want unexpected binary", chunk.Err)
		}
		var replayRequired interface {
			CodexWebsocketReplayRequired() bool
		}
		if errors.As(chunk.Err, &replayRequired) && replayRequired != nil && replayRequired.CodexWebsocketReplayRequired() {
			t.Fatalf("stream chunk error = %v, must not be replay-required marker", chunk.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for unexpected binary error")
	}
}

func TestCodexWebsocketsExecuteStreamSessionCloseWithPreviousResponseIDDoesNotRequireReplay(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	receivedRequest := make(chan struct{}, 1)
	releaseServer := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			return
		}
		receivedRequest <- struct{}{}
		<-releaseServer
	}))
	defer server.Close()
	defer close(releaseServer)

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-stream-session-close-no-replay"
	defer exec.CloseExecutionSession(sessionID)

	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}

	result, err := exec.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v, want stream result", err)
	}
	select {
	case <-receivedRequest:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for websocket request")
	}

	exec.CloseExecutionSession(sessionID)

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed without session close error")
		}
		if chunk.Err == nil || !strings.Contains(chunk.Err.Error(), "execution session closed") {
			t.Fatalf("stream chunk error = %v, want execution session closed", chunk.Err)
		}
		var replayRequired interface {
			CodexWebsocketReplayRequired() bool
		}
		if errors.As(chunk.Err, &replayRequired) && replayRequired != nil && replayRequired.CodexWebsocketReplayRequired() {
			t.Fatalf("stream chunk error = %v, must not be replay-required marker", chunk.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stream session close error")
	}
}

func TestCodexWebsocketsExecuteReadCancelWithPreviousResponseIDDoesNotRequireReplay(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	receivedRequest := make(chan struct{}, 1)
	releaseServer := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			return
		}
		receivedRequest <- struct{}{}
		<-releaseServer
	}))
	defer server.Close()
	defer close(releaseServer)

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-execute-read-cancel"
	defer exec.CloseExecutionSession(sessionID)

	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := exec.Execute(ctx, auth, req, opts)
		errCh <- err
	}()

	select {
	case <-receivedRequest:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for websocket request")
	}
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Execute() error = %v, want context.Canceled", err)
		}
		var replayRequired interface {
			CodexWebsocketReplayRequired() bool
		}
		if errors.As(err, &replayRequired) && replayRequired != nil && replayRequired.CodexWebsocketReplayRequired() {
			t.Fatalf("Execute() error = %v, must not be replay-required marker", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Execute cancellation")
	}
}

func TestCodexWebsocketsExecuteStreamHandshakeStatusUnlocksSessionRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	sessionID := "sess-handshake-status-unlock"
	defer exec.CloseExecutionSession(sessionID)
	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","id":"msg-1"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("codex"),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}

	if _, err := exec.ExecuteStream(context.Background(), auth, req, opts); err == nil {
		t.Fatalf("first ExecuteStream() error = nil, want handshake status error")
	}

	done := make(chan error, 1)
	go func() {
		_, err := exec.ExecuteStream(context.Background(), auth, req, opts)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("second ExecuteStream() error = nil, want handshake status error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second ExecuteStream() blocked; session request mutex was not released")
	}
}

func TestCodexWebsocketsReadLoopIgnoresStaleConnErrors(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	staleConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial stale websocket: %v", err)
	}
	currentConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial current websocket: %v", err)
	}
	defer func() { _ = currentConn.Close() }()

	exec := NewCodexWebsocketsExecutor(&config.Config{})
	sessionID := "sess-stale-reader"
	defer exec.CloseExecutionSession(sessionID)
	sess := exec.getOrCreateSession(sessionID)
	if sess == nil {
		t.Fatal("expected session")
	}
	readCh := make(chan codexWebsocketRead, 1)
	defer func() {
		sess.clearActive(currentConn, readCh)
	}()
	sess.connMu.Lock()
	sess.conn = currentConn
	sess.readerConn = currentConn
	sess.connMu.Unlock()
	sess.setActive(currentConn, readCh)

	done := make(chan struct{})
	go func() {
		exec.readUpstreamLoop(sess, staleConn)
		close(done)
	}()
	if errClose := staleConn.Close(); errClose != nil {
		t.Fatalf("close stale websocket: %v", errClose)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stale read loop")
	}

	select {
	case ev, ok := <-readCh:
		t.Fatalf("unexpected stale read event ok=%t ev=%+v", ok, ev)
	default:
	}
	sess.activeMu.Lock()
	activeCh := sess.activeCh
	sess.activeMu.Unlock()
	if activeCh != readCh {
		t.Fatalf("expected active read channel to remain current")
	}
}

func TestCodexWebsocketsReadLoopKeepsCurrentConnWithoutActiveReader(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	sent := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created"}`)); errWrite != nil {
			t.Errorf("write unsolicited websocket message: %v", errWrite)
			return
		}
		sent <- struct{}{}
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}

	exec := NewCodexWebsocketsExecutor(&config.Config{})
	sessionID := "sess-current-no-active-reader"
	defer exec.CloseExecutionSession(sessionID)
	sess := exec.getOrCreateSession(sessionID)
	if sess == nil {
		t.Fatal("expected session")
	}
	sess.connMu.Lock()
	sess.conn = conn
	sess.readerConn = conn
	sess.connMu.Unlock()

	done := make(chan struct{})
	go func() {
		exec.readUpstreamLoop(sess, conn)
		close(done)
	}()

	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for unsolicited upstream message")
	}
	select {
	case <-done:
		t.Fatal("read loop exited after current unsolicited message without active reader")
	case <-time.After(50 * time.Millisecond):
	}

	if errClose := conn.Close(); errClose != nil {
		t.Fatalf("close websocket: %v", errClose)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for read loop after connection close")
	}
}

func TestApplyCodexWebsocketHeadersDefaultsToCurrentResponsesBeta(t *testing.T) {
	headers := applyCodexWebsocketHeaders(context.Background(), http.Header{}, nil, "", nil)

	if got := headers.Get("OpenAI-Beta"); got != codexResponsesWebsocketBetaHeaderValue {
		t.Fatalf("OpenAI-Beta = %s, want %s", got, codexResponsesWebsocketBetaHeaderValue)
	}
	if got := headers.Get("User-Agent"); got != codexUserAgent {
		t.Fatalf("User-Agent = %s, want %s", got, codexUserAgent)
	}
	if !strings.HasPrefix(codexUserAgent, codexOriginator+"/") {
		t.Fatalf("default Codex User-Agent = %s, want prefix %s/", codexUserAgent, codexOriginator)
	}
	if !strings.HasPrefix(codexUserAgent, "codex-tui/") {
		t.Fatalf("default Codex User-Agent = %s, want codex-tui prefix", codexUserAgent)
	}
	if !strings.Contains(codexUserAgent, "(codex-tui;") {
		t.Fatalf("default Codex User-Agent = %s, want codex-tui suffix", codexUserAgent)
	}
	if got := headers.Get("Originator"); got != codexOriginator {
		t.Fatalf("Originator = %s, want %s", got, codexOriginator)
	}
	if got := headers.Get("Version"); got != "" {
		t.Fatalf("Version = %q, want empty", got)
	}
	if got := headers.Get("x-codex-beta-features"); got != "" {
		t.Fatalf("x-codex-beta-features = %q, want empty", got)
	}
	if got := headers.Get("X-Codex-Turn-Metadata"); got != "" {
		t.Fatalf("X-Codex-Turn-Metadata = %q, want empty", got)
	}
	if got := headers.Get("X-Client-Request-Id"); got != "" {
		t.Fatalf("X-Client-Request-Id = %q, want empty", got)
	}
}

func TestApplyCodexWebsocketHeadersPassesThroughClientIdentityHeaders(t *testing.T) {
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	ctx := contextWithGinHeaders(map[string]string{
		"Originator":            "Codex Desktop",
		"User-Agent":            "codex_cli_rs/0.1.0",
		"Version":               "0.115.0-alpha.27",
		"X-Codex-Turn-Metadata": `{"turn_id":"turn-1"}`,
		"X-Client-Request-Id":   "019d2233-e240-7162-992d-38df0a2a0e0d",
		"session-id":            "legacy-session",
	})

	headers := applyCodexWebsocketHeaders(ctx, http.Header{}, auth, "", nil)

	if got := headers.Get("Originator"); got != "Codex Desktop" {
		t.Fatalf("Originator = %s, want %s", got, "Codex Desktop")
	}
	if got := headers.Get("User-Agent"); got != "codex_cli_rs/0.1.0" {
		t.Fatalf("User-Agent = %s, want %s", got, "codex_cli_rs/0.1.0")
	}
	if got := headers.Get("Version"); got != "0.115.0-alpha.27" {
		t.Fatalf("Version = %s, want %s", got, "0.115.0-alpha.27")
	}
	if got := headers.Get("X-Codex-Turn-Metadata"); got != `{"turn_id":"turn-1"}` {
		t.Fatalf("X-Codex-Turn-Metadata = %s, want %s", got, `{"turn_id":"turn-1"}`)
	}
	if got := headers.Get("X-Client-Request-Id"); got != "019d2233-e240-7162-992d-38df0a2a0e0d" {
		t.Fatalf("X-Client-Request-Id = %s, want %s", got, "019d2233-e240-7162-992d-38df0a2a0e0d")
	}
	if got := headers["session_id"]; len(got) != 1 || got[0] != "legacy-session" {
		t.Fatalf("session_id = %#v, want [legacy-session]", got)
	}
	if got := headers.Get("Session-Id"); got != "" {
		t.Fatalf("Session-Id = %s, want empty", got)
	}
}

func TestApplyCodexWebsocketHeadersCanonicalizesLegacyUnderscoreSessionHeader(t *testing.T) {
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	ctx := contextWithGinHeaders(map[string]string{
		"Originator": "Codex Desktop",
		"User-Agent": "codex_cli_rs/0.1.0",
		"Session_id": "legacy-underscore-session",
	})

	headers := applyCodexWebsocketHeaders(ctx, http.Header{}, auth, "", nil)

	if got := headers["session_id"]; len(got) != 1 || got[0] != "legacy-underscore-session" {
		t.Fatalf("session_id = %#v, want [legacy-underscore-session]", got)
	}
	if got := headers.Get("Session-Id"); got != "" {
		t.Fatalf("Session-Id = %s, want empty", got)
	}
}

func TestApplyCodexWebsocketHeadersUsesConfigDefaultsForOAuth(t *testing.T) {
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "my-codex-client/1.0",
			BetaFeatures: "feature-a,feature-b",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}

	headers := applyCodexWebsocketHeaders(context.Background(), http.Header{}, auth, "", cfg)

	if got := headers.Get("User-Agent"); got != "my-codex-client/1.0" {
		t.Fatalf("User-Agent = %s, want %s", got, "my-codex-client/1.0")
	}
	if got := headers.Get("x-codex-beta-features"); got != "feature-a,feature-b" {
		t.Fatalf("x-codex-beta-features = %s, want %s", got, "feature-a,feature-b")
	}
	if got := headers.Get("OpenAI-Beta"); got != codexResponsesWebsocketBetaHeaderValue {
		t.Fatalf("OpenAI-Beta = %s, want %s", got, codexResponsesWebsocketBetaHeaderValue)
	}
}

func TestApplyCodexWebsocketHeadersPrefersExistingHeadersOverClientAndConfig(t *testing.T) {
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "config-ua",
			BetaFeatures: "config-beta",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	ctx := contextWithGinHeaders(map[string]string{
		"User-Agent":            "client-ua",
		"X-Codex-Beta-Features": "client-beta",
	})
	headers := http.Header{}
	headers.Set("User-Agent", "existing-ua")
	headers.Set("X-Codex-Beta-Features", "existing-beta")

	got := applyCodexWebsocketHeaders(ctx, headers, auth, "", cfg)

	if gotVal := got.Get("User-Agent"); gotVal != "existing-ua" {
		t.Fatalf("User-Agent = %s, want %s", gotVal, "existing-ua")
	}
	if gotVal := got.Get("x-codex-beta-features"); gotVal != "existing-beta" {
		t.Fatalf("x-codex-beta-features = %s, want %s", gotVal, "existing-beta")
	}
}

func TestApplyCodexWebsocketHeadersConfigUserAgentOverridesClientHeader(t *testing.T) {
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "config-ua",
			BetaFeatures: "config-beta",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	ctx := contextWithGinHeaders(map[string]string{
		"User-Agent":            "client-ua",
		"X-Codex-Beta-Features": "client-beta",
	})

	headers := applyCodexWebsocketHeaders(ctx, http.Header{}, auth, "", cfg)

	if got := headers.Get("User-Agent"); got != "config-ua" {
		t.Fatalf("User-Agent = %s, want %s", got, "config-ua")
	}
	if got := headers.Get("x-codex-beta-features"); got != "client-beta" {
		t.Fatalf("x-codex-beta-features = %s, want %s", got, "client-beta")
	}
}

func TestApplyCodexWebsocketHeadersIgnoresConfigForAPIKeyAuth(t *testing.T) {
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "config-ua",
			BetaFeatures: "config-beta",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider:   "codex",
		Attributes: map[string]string{"api_key": "sk-test"},
	}

	headers := applyCodexWebsocketHeaders(context.Background(), http.Header{}, auth, "sk-test", cfg)

	if got := headers.Get("User-Agent"); got != "" {
		t.Fatalf("User-Agent = %s, want empty", got)
	}
	if got := headers.Get("x-codex-beta-features"); got != "" {
		t.Fatalf("x-codex-beta-features = %q, want empty", got)
	}
	if got := headers.Get("Originator"); got != "" {
		t.Fatalf("Originator = %s, want empty", got)
	}
}

func TestApplyCodexWebsocketHeadersPreservesExplicitAPIKeyUserAgent(t *testing.T) {
	auth := &cliproxyauth.Auth{Provider: "codex", Attributes: map[string]string{"api_key": "sk-test"}}
	ctx := contextWithGinHeaders(map[string]string{"User-Agent": "api-key-client/1.0", "Originator": "explicit-origin"})

	headers := applyCodexWebsocketHeaders(ctx, http.Header{}, auth, "sk-test", nil)

	if got := headers.Get("User-Agent"); got != "api-key-client/1.0" {
		t.Fatalf("User-Agent = %s, want api-key-client/1.0", got)
	}
	if got := headers.Get("Originator"); got != "explicit-origin" {
		t.Fatalf("Originator = %s, want explicit-origin", got)
	}
}

func TestApplyCodexWebsocketHeadersUsesCanonicalAccountHeader(t *testing.T) {
	auth := &cliproxyauth.Auth{Provider: "codex", Metadata: map[string]any{"account_id": "acct-1"}}

	headers := applyCodexWebsocketHeaders(context.Background(), http.Header{}, auth, "", nil)

	if got := headerValueCaseInsensitive(headers, "ChatGPT-Account-ID"); got != "acct-1" {
		t.Fatalf("ChatGPT-Account-ID = %s, want acct-1", got)
	}
	values, ok := headers["ChatGPT-Account-ID"]
	if !ok {
		t.Fatalf("expected exact ChatGPT-Account-ID key, got %#v", headers)
	}
	if len(values) != 1 || values[0] != "acct-1" {
		t.Fatalf("ChatGPT-Account-ID values = %#v, want [acct-1]", values)
	}
}

func TestApplyCodexPromptCacheHeadersSetsSessionIDAndLegacyConversation(t *testing.T) {
	req := cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: []byte(`{"prompt_cache_key":"cache-1"}`)}

	_, headers := applyCodexPromptCacheHeaders("openai-response", req, []byte(`{"model":"gpt-5-codex"}`))

	if got := headers["session_id"]; len(got) != 1 || got[0] != "cache-1" {
		t.Fatalf("session_id = %#v, want [cache-1]", got)
	}
	if got := headers.Get("Session-Id"); got != "" {
		t.Fatalf("Session-Id = %s, want empty", got)
	}
	if got := headers.Get("Conversation_id"); got != "cache-1" {
		t.Fatalf("Conversation_id = %s, want cache-1", got)
	}
}

func TestApplyCodexPromptCacheHeadersClaudeUsesClaudeCodeSessionID(t *testing.T) {
	firstReq := cliproxyexecutor.Request{
		Model: "gpt-5-codex-claude-ws-cache-session",
		Payload: []byte(`{
			"metadata":{"user_id":"{\"device_id\":\"device-a\",\"account_uuid\":\"\",\"session_id\":\"ws-cache-session-1\"}"},
			"messages":[{"role":"user","content":[{"type":"text","text":"first"}]}]
		}`),
	}
	secondReq := cliproxyexecutor.Request{
		Model: "gpt-5-codex-claude-ws-cache-session",
		Payload: []byte(`{
			"metadata":{"user_id":"{\"device_id\":\"device-b\",\"account_uuid\":\"\",\"session_id\":\"ws-cache-session-1\"}"},
			"messages":[{"role":"user","content":[{"type":"text","text":"next"}]}]
		}`),
	}

	firstBody, firstHeaders := applyCodexPromptCacheHeaders("claude", firstReq, []byte(`{"model":"gpt-5-codex"}`))
	secondBody, secondHeaders := applyCodexPromptCacheHeaders("claude", secondReq, []byte(`{"model":"gpt-5-codex"}`))

	firstKey := gjson.GetBytes(firstBody, "prompt_cache_key").String()
	secondKey := gjson.GetBytes(secondBody, "prompt_cache_key").String()
	if firstKey == "" {
		t.Fatalf("first prompt_cache_key is empty; body=%s", string(firstBody))
	}
	if secondKey != firstKey {
		t.Fatalf("same Claude Code session_id produced different websocket prompt_cache_key: first=%q second=%q", firstKey, secondKey)
	}
	if got := firstHeaders["session_id"]; len(got) != 1 || got[0] != firstKey {
		t.Fatalf("first session_id = %#v, want [%q]", got, firstKey)
	}
	if got := secondHeaders["session_id"]; len(got) != 1 || got[0] != firstKey {
		t.Fatalf("second session_id = %#v, want [%q]", got, firstKey)
	}
}

func TestApplyCodexPromptCacheHeadersClaudeRejectsBareUserID(t *testing.T) {
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex-claude-ws-cache-bare-user",
		Payload: []byte(`{"metadata":{"user_id":"same-user-across-chats"},"messages":[{"role":"user","content":[{"type":"text","text":"first"}]}]}`),
	}

	body, headers := applyCodexPromptCacheHeaders("claude", req, []byte(`{"model":"gpt-5-codex"}`))

	if got := gjson.GetBytes(body, "prompt_cache_key").String(); got != "" {
		t.Fatalf("bare metadata.user_id must not create websocket prompt_cache_key, got %q; body=%s", got, string(body))
	}
	if got := headers["session_id"]; len(got) != 0 {
		t.Fatalf("bare metadata.user_id must not create websocket session_id, got %#v", got)
	}
	if got := headers.Get("Session-Id"); got != "" {
		t.Fatalf("bare metadata.user_id must not create websocket Session-Id, got %q", got)
	}
	if got := headers.Get("Conversation_id"); got != "" {
		t.Fatalf("bare metadata.user_id must not create websocket Conversation_id, got %q", got)
	}
}

func TestApplyCodexWebsocketHeadersIdentityConfuseRemapsPromptCacheKey(t *testing.T) {
	cfg := &config.Config{
		Routing: config.RoutingConfig{SessionAffinity: true},
		Codex:   config.CodexConfig{IdentityConfuse: true},
	}
	auth := &cliproxyauth.Auth{ID: "auth-ws-1", Provider: "codex"}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"prompt_cache_key":"cache-ws-1","client_metadata":{"x-codex-installation-id":"install-ws-1"}}`),
	}

	body, headers := applyCodexPromptCacheHeaders("openai-response", req, []byte(`{"model":"gpt-5-codex"}`))
	body, identityState := applyCodexIdentityConfuseBody(cfg, auth, req.Payload, body)
	ctx := contextWithGinHeaders(map[string]string{
		"X-Codex-Turn-Metadata": `{"prompt_cache_key":"cache-ws-1","turn_id":"turn-ws-1","window_id":"cache-ws-1:3"}`,
		"X-Codex-Window-Id":     "cache-ws-1:3",
		"X-Client-Request-Id":   "client-request-1",
		"Thread-Id":             "thread-ws-1",
	})
	headers = applyCodexWebsocketHeaders(ctx, headers, auth, "oauth-token", cfg)
	applyCodexIdentityConfuseHeaders(headers, &identityState)

	expectedPromptCacheKey := codexIdentityConfuseUUID("auth-ws-1", "prompt-cache", "cache-ws-1")
	expectedThreadID := codexIdentityConfuseUUID("auth-ws-1", "prompt-cache", "thread-ws-1")
	expectedTurnID := codexIdentityConfuseUUID("auth-ws-1", "turn", "turn-ws-1")
	if gotKey := gjson.GetBytes(body, "prompt_cache_key").String(); gotKey != expectedPromptCacheKey {
		t.Fatalf("prompt_cache_key = %q, want %q", gotKey, expectedPromptCacheKey)
	}
	if strings.Contains(string(body), "cache-ws-1") {
		t.Fatalf("upstream websocket body still contains original prompt cache key: %s", string(body))
	}
	if strings.Contains(headers.Get("X-Codex-Turn-Metadata"), "cache-ws-1") {
		t.Fatalf("upstream websocket metadata still contains original prompt cache key: %s", headers.Get("X-Codex-Turn-Metadata"))
	}
	if gotSession := headers["session_id"]; len(gotSession) != 1 || gotSession[0] != expectedPromptCacheKey {
		t.Fatalf("session_id = %#v, want [%q]", gotSession, expectedPromptCacheKey)
	}
	if gotCanonicalSession := headers.Get("Session-Id"); gotCanonicalSession != "" {
		t.Fatalf("Session-Id = %q, want empty", gotCanonicalSession)
	}
	if gotRequestID := headers.Get("X-Client-Request-Id"); gotRequestID != expectedThreadID {
		t.Fatalf("X-Client-Request-Id = %q, want %q", gotRequestID, expectedThreadID)
	}
	if gotThreadID := headers.Get("Thread-Id"); gotThreadID != expectedThreadID {
		t.Fatalf("Thread-Id = %q, want %q", gotThreadID, expectedThreadID)
	}
	if gotConversation := headers.Get("Conversation_id"); gotConversation != expectedPromptCacheKey {
		t.Fatalf("Conversation_id = %q, want %q", gotConversation, expectedPromptCacheKey)
	}
	if gotWindowID := headers.Get("X-Codex-Window-Id"); gotWindowID != expectedPromptCacheKey+":3" {
		t.Fatalf("X-Codex-Window-Id = %q, want %q", gotWindowID, expectedPromptCacheKey+":3")
	}
	gotMetadata := headers.Get("X-Codex-Turn-Metadata")
	if gotMetadataPromptCacheKey := gjson.Get(gotMetadata, "prompt_cache_key").String(); gotMetadataPromptCacheKey != expectedPromptCacheKey {
		t.Fatalf("X-Codex-Turn-Metadata.prompt_cache_key = %q, want %q", gotMetadataPromptCacheKey, expectedPromptCacheKey)
	}
	if gotMetadataTurnID := gjson.Get(gotMetadata, "turn_id").String(); gotMetadataTurnID != expectedTurnID {
		t.Fatalf("X-Codex-Turn-Metadata.turn_id = %q, want %q", gotMetadataTurnID, expectedTurnID)
	}
	if gotMetadataWindowID := gjson.Get(gotMetadata, "window_id").String(); gotMetadataWindowID != expectedPromptCacheKey+":3" {
		t.Fatalf("X-Codex-Turn-Metadata.window_id = %q, want %q", gotMetadataWindowID, expectedPromptCacheKey+":3")
	}
	expectedInstallationID := codexIdentityConfuseUUID("auth-ws-1", "installation", "install-ws-1")
	if gotInstallationID := gjson.GetBytes(body, "client_metadata.x-codex-installation-id").String(); gotInstallationID != expectedInstallationID {
		t.Fatalf("installation id = %q, want %q", gotInstallationID, expectedInstallationID)
	}
}

func TestApplyCodexWebsocketHeadersPreservesWindowAndThreadHeaders(t *testing.T) {
	ctx := contextWithGinHeaders(map[string]string{
		"X-Codex-Window-Id": "cache-ws-1:3",
		"Thread-Id":         "thread-ws-1",
	})
	headers := applyCodexWebsocketHeaders(ctx, nil, &cliproxyauth.Auth{Provider: "codex"}, "oauth-token", nil)

	if got := headers.Get("X-Codex-Window-Id"); got != "cache-ws-1:3" {
		t.Fatalf("X-Codex-Window-Id = %q, want cache-ws-1:3", got)
	}
	if got := headers.Get("Thread-Id"); got != "thread-ws-1" {
		t.Fatalf("Thread-Id = %q, want thread-ws-1", got)
	}
}

func TestApplyCodexClientMetadataCompatibilityHeadersOverridesStaleHeaders(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-Codex-Turn-Metadata", `{"prompt_cache_key":"cache-old","window_id":"cache-old:0"}`)
	headers.Set("X-Codex-Window-Id", "cache-old:0")
	body := []byte(`{"client_metadata":{"x-codex-turn-state":"request-state","ws_request_header_x_openai_internal_codex_responses_lite":"true","x-codex-turn-metadata":"{\"prompt_cache_key\":\"cache-new\",\"window_id\":\"cache-new:2\"}","x-codex-window-id":"cache-new:2","x-codex-parent-thread-id":"parent-1","x-openai-subagent":"compact"}}`)

	applyCodexClientMetadataCompatibilityHeaders(headers, body)

	if got := headers.Get("X-Codex-Turn-Metadata"); !strings.Contains(got, "cache-new:2") || strings.Contains(got, "cache-old") {
		t.Fatalf("X-Codex-Turn-Metadata was not refreshed from client_metadata: %s", got)
	}
	if got := headers.Get("X-Codex-Window-Id"); got != "cache-new:2" {
		t.Fatalf("X-Codex-Window-Id = %q, want cache-new:2", got)
	}
	if got := headers.Get("X-Codex-Parent-Thread-Id"); got != "parent-1" {
		t.Fatalf("X-Codex-Parent-Thread-Id = %q, want parent-1", got)
	}
	if got := headers.Get("X-Openai-Subagent"); got != "compact" {
		t.Fatalf("X-Openai-Subagent = %q, want compact", got)
	}
	if got := headers.Get(codexTurnStateHeader); got != "" {
		t.Fatalf("%s = %q, want empty on websocket handshake", codexTurnStateHeader, got)
	}
	if got := headers.Get(codexResponsesLiteHeader); got != "" {
		t.Fatalf("%s = %q, want empty on websocket handshake", codexResponsesLiteHeader, got)
	}
}

func TestCodexIdentityConfuseResponsePayloadHidesUpstreamAndRestoresClient(t *testing.T) {
	state := codexIdentityConfuseState{
		enabled:                true,
		authID:                 "auth-ws-1",
		originalPromptCacheKey: "cache-ws-1",
		promptCacheKey:         codexIdentityConfuseUUID("auth-ws-1", "prompt-cache", "cache-ws-1"),
	}
	expectedTurnID := state.confuseTurnID("turn-ws-1")
	rawPayload := []byte(`{"type":"response.completed","response":{"prompt_cache_key":"cache-ws-1","turn_id":"turn-ws-1"},"prompt_cache_key":"cache-ws-1","turn_id":"turn-ws-1"}`)

	upstreamPayload := applyCodexIdentityConfuseResponsePayload(rawPayload, state)
	if bytes.Contains(upstreamPayload, []byte(`cache-ws-1`)) {
		t.Fatalf("upstream payload still contains original prompt_cache_key: %s", string(upstreamPayload))
	}
	if bytes.Contains(upstreamPayload, []byte(`turn-ws-1`)) {
		t.Fatalf("upstream payload still contains original turn_id: %s", string(upstreamPayload))
	}
	if !bytes.Contains(upstreamPayload, []byte(state.promptCacheKey)) {
		t.Fatalf("upstream payload missing confused prompt_cache_key: %s", string(upstreamPayload))
	}
	if !bytes.Contains(upstreamPayload, []byte(expectedTurnID)) {
		t.Fatalf("upstream payload missing confused turn_id: %s", string(upstreamPayload))
	}

	clientPayload := applyCodexIdentityExposeResponsePayload(upstreamPayload, state)
	if bytes.Contains(clientPayload, []byte(state.promptCacheKey)) {
		t.Fatalf("client payload still contains confused prompt_cache_key: %s", string(clientPayload))
	}
	if bytes.Contains(clientPayload, []byte(expectedTurnID)) {
		t.Fatalf("client payload still contains confused turn_id: %s", string(clientPayload))
	}
	if !bytes.Contains(clientPayload, []byte(`cache-ws-1`)) {
		t.Fatalf("client payload missing original prompt_cache_key: %s", string(clientPayload))
	}
	if !bytes.Contains(clientPayload, []byte(`turn-ws-1`)) {
		t.Fatalf("client payload missing original turn_id: %s", string(clientPayload))
	}

	rawSSE := []byte(`data: {"type":"response.completed","response":{"prompt_cache_key":"cache-ws-1","turn_id":"turn-ws-1"}}`)
	upstreamSSE := applyCodexIdentityConfuseResponsePayload(rawSSE, state)
	if bytes.Contains(upstreamSSE, []byte(`cache-ws-1`)) {
		t.Fatalf("upstream SSE still contains original prompt_cache_key: %s", string(upstreamSSE))
	}
	if bytes.Contains(upstreamSSE, []byte(`turn-ws-1`)) {
		t.Fatalf("upstream SSE still contains original turn_id: %s", string(upstreamSSE))
	}
	clientSSE := applyCodexIdentityExposeResponsePayload(upstreamSSE, state)
	if !bytes.Contains(clientSSE, []byte(`cache-ws-1`)) || bytes.Contains(clientSSE, []byte(state.promptCacheKey)) {
		t.Fatalf("client SSE prompt_cache_key was not restored: %s", string(clientSSE))
	}
	if !bytes.Contains(clientSSE, []byte(`turn-ws-1`)) || bytes.Contains(clientSSE, []byte(expectedTurnID)) {
		t.Fatalf("client SSE turn_id was not restored: %s", string(clientSSE))
	}
}

func TestBuildCodexResponsesWebsocketURLRequiresHTTPURL(t *testing.T) {
	if got, err := buildCodexResponsesWebsocketURL("https://example.com/backend/responses"); err != nil || got != "wss://example.com/backend/responses" {
		t.Fatalf("https URL = %q, %v; want wss URL", got, err)
	}
	if _, err := buildCodexResponsesWebsocketURL("ftp://example.com/responses"); err == nil {
		t.Fatalf("expected unsupported scheme error")
	}
	if _, err := buildCodexResponsesWebsocketURL("https:///responses"); err == nil {
		t.Fatalf("expected empty host error")
	}
}

func TestParseCodexWebsocketErrorMarksConnectionLimitRetryable(t *testing.T) {
	err, ok := parseCodexWebsocketError([]byte(`{"type":"error","status":429,"error":{"code":"websocket_connection_limit_reached","message":"too many websockets"},"headers":{"retry-after":"1"}}`))
	if !ok {
		t.Fatalf("expected websocket error")
	}
	status, ok := err.(interface{ StatusCode() int })
	if !ok || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("status = %#v, want 429", err)
	}
	retryable, ok := err.(interface{ RetryAfter() *time.Duration })
	if !ok || retryable.RetryAfter() == nil {
		t.Fatalf("expected retryable websocket connection limit error")
	}
	if got := *retryable.RetryAfter(); got != 0 {
		t.Fatalf("retryAfter = %v, want connection-limit fallback 0", got)
	}
	withHeaders, ok := err.(interface{ Headers() http.Header })
	if !ok || withHeaders.Headers().Get("retry-after") != "1" {
		t.Fatalf("headers = %#v, want retry-after", err)
	}
}

func TestParseCodexWebsocketErrorUsesUsageLimitRetryMetadata(t *testing.T) {
	err, ok := parseCodexWebsocketError([]byte(`{"type":"error","status":429,"body":{"error":{"type":"usage_limit_reached","message":"usage limit reached","resets_in_seconds":7}}}`))
	if !ok {
		t.Fatalf("expected websocket error")
	}

	retryable, ok := err.(interface{ RetryAfter() *time.Duration })
	if !ok || retryable.RetryAfter() == nil {
		t.Fatalf("expected retryable usage limit websocket error")
	}
	if got := *retryable.RetryAfter(); got != 7*time.Second {
		t.Fatalf("retryAfter = %v, want 7s", got)
	}
}

func TestParseCodexWebsocketErrorPreservesWrappedBodyAndHeaders(t *testing.T) {
	err, ok := parseCodexWebsocketError([]byte(`{"type":"error","status":429,"body":{"error":{"code":"websocket_connection_limit_reached","type":"server_error","message":"too many websocket connections"}},"headers":{"x-request-id":"req-1"}}`))
	if !ok {
		t.Fatalf("expected websocket error")
	}

	parsed := gjson.Parse(err.Error())
	if got := parsed.Get("status").Int(); got != http.StatusTooManyRequests {
		t.Fatalf("wrapped status = %d, want 429; payload=%s", got, err.Error())
	}
	if got := parsed.Get("body.error.code").String(); got != "websocket_connection_limit_reached" {
		t.Fatalf("wrapped body error code = %s, want websocket_connection_limit_reached; payload=%s", got, err.Error())
	}
	if got := parsed.Get("error.code").String(); got != "websocket_connection_limit_reached" {
		t.Fatalf("surface error code = %s, want websocket_connection_limit_reached; payload=%s", got, err.Error())
	}
	retryable, ok := err.(interface{ RetryAfter() *time.Duration })
	if !ok || retryable.RetryAfter() == nil {
		t.Fatalf("expected body.error.code websocket connection limit to be retryable")
	}
	withHeaders, ok := err.(interface{ Headers() http.Header })
	if !ok || withHeaders.Headers().Get("x-request-id") != "req-1" {
		t.Fatalf("headers = %#v, want x-request-id", err)
	}
}

func TestIsCodexWebsocketPreviousResponseNotFound(t *testing.T) {
	if !isCodexWebsocketPreviousResponseNotFound([]byte(`{"type":"error","status":400,"error":{"code":"previous_response_not_found","message":"previous response with id 'resp-1' not found"}}`)) {
		t.Fatalf("expected previous response not found websocket error")
	}
	if isCodexWebsocketPreviousResponseNotFound([]byte(`{"type":"error","status":429,"error":{"code":"websocket_connection_limit_reached","message":"too many websockets"}}`)) {
		t.Fatalf("connection limit error must not be classified as previous response not found")
	}
}

func TestCodexWebsocketRequestNeedsTranscriptReplayOnReset(t *testing.T) {
	if !codexWebsocketRequestNeedsTranscriptReplayOnReset([]byte(`{"previous_response_id":"resp-1","input":[]}`)) {
		t.Fatalf("expected previous_response_id request to require transcript replay")
	}
	if codexWebsocketRequestNeedsTranscriptReplayOnReset([]byte(`{"input":[]}`)) {
		t.Fatalf("request without previous_response_id must not require transcript replay")
	}
	if codexWebsocketRequestNeedsTranscriptReplayOnReset([]byte(`{"previous_response_id":" ","input":[]}`)) {
		t.Fatalf("blank previous_response_id must not require transcript replay")
	}
}

func TestCodexWebsocketReadErrorRequiresTranscriptReplay(t *testing.T) {
	resetErr := codexWebsocketUpstreamResetError{cause: errors.New("read tcp: i/o timeout")}
	if !codexWebsocketReadErrorRequiresTranscriptReplay([]byte(`{"previous_response_id":"resp-1","input":[]}`), resetErr, false) {
		t.Fatalf("previous_response_id upstream reset should require transcript replay")
	}
	if codexWebsocketReadErrorRequiresTranscriptReplay([]byte(`{"input":[]}`), resetErr, false) {
		t.Fatalf("non-websocket upstream reset without previous_response_id must not require transcript replay")
	}
	if !codexWebsocketReadErrorRequiresTranscriptReplay([]byte(`{"input":[]}`), resetErr, true) {
		t.Fatalf("downstream websocket upstream reset without previous_response_id should require transcript replay")
	}
	if codexWebsocketReadErrorRequiresTranscriptReplay([]byte(`{"previous_response_id":"resp-1","input":[]}`), errors.New("other read error"), true) {
		t.Fatalf("non-reset read error must not require transcript replay")
	}
	messageTooBigErr := codexWebsocketUpstreamResetError{cause: &websocket.CloseError{Code: websocket.CloseMessageTooBig, Text: "message too big"}}
	if codexWebsocketReadErrorRequiresTranscriptReplay([]byte(`{"previous_response_id":"resp-1","input":[]}`), messageTooBigErr, true) {
		t.Fatalf("message_too_big close must not require transcript replay")
	}
}

func TestCodexWebsocketTranscriptReplayRequiredError(t *testing.T) {
	cause := errors.New("write failed")
	errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "send_error", cause: cause}

	if !errors.Is(errReplay, cause) {
		t.Fatalf("expected replay error to unwrap cause")
	}
	if got := errReplay.StatusCode(); got != http.StatusBadRequest {
		t.Fatalf("StatusCode() = %d, want %d", got, http.StatusBadRequest)
	}
	if !errReplay.CodexWebsocketReplayRequired() {
		t.Fatalf("expected replay-required marker")
	}
	if got := errReplay.Error(); !strings.Contains(got, "invalid_request_error") || !strings.Contains(got, "send_error") {
		t.Fatalf("unexpected replay error message: %s", got)
	}
}

func TestApplyCodexHeadersUsesConfigUserAgentForOAuth(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://example.com/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "config-ua",
			BetaFeatures: "config-beta",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	req = req.WithContext(contextWithGinHeaders(map[string]string{
		"User-Agent": "client-ua",
	}))

	applyCodexHeaders(req, auth, "oauth-token", true, cfg)

	if got := req.Header.Get("User-Agent"); got != "config-ua" {
		t.Fatalf("User-Agent = %s, want %s", got, "config-ua")
	}
	if got := req.Header.Get("x-codex-beta-features"); got != "" {
		t.Fatalf("x-codex-beta-features = %q, want empty", got)
	}
}

func TestApplyModelHeaderOverridesFromModelConfig(t *testing.T) {
	const wantUA = "codex-tui/0.144.0 (Mac OS 26.5.1; arm64) iTerm.app/3.6.11 (codex-tui; 0.144.0)"
	req, err := http.NewRequest(http.MethodPost, "https://example.com/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent: "config-ua",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}

	applyCodexHeaders(req, auth, "oauth-token", true, cfg)
	applyModelHeaderOverrides(req.Header, "gpt-5.6-luna")

	if got := req.Header.Get("User-Agent"); got != wantUA {
		t.Fatalf("User-Agent = %q, want %q", got, wantUA)
	}
	if got := codexSessionHeaderValue(req.Header); got == "" {
		t.Fatal("expected Session_id to be set for Mac OS User-Agent override")
	}

	applyModelHeaderOverrides(req.Header, "gpt-5.4")
	if got := req.Header.Get("User-Agent"); got != wantUA {
		t.Fatalf("User-Agent after no-op override = %q, want %q", got, wantUA)
	}
}

func TestApplyModelHeaderOverridesMultipleHeaders(t *testing.T) {
	reg := registry.GetGlobalRegistry()
	clientID := "test-model-header-override"
	reg.RegisterClient(clientID, "codex", []*registry.ModelInfo{{
		ID: "test-override-headers-model",
		Config: &registry.ModelConfig{
			OverrideHeader: map[string]string{
				"user-agent":    "custom-ua/1.0",
				"originator":    "custom-origin",
				"x-test-header": "forced-value",
			},
		},
	}})
	t.Cleanup(func() { reg.UnregisterClient(clientID) })

	headers := http.Header{}
	headers.Set("User-Agent", "old-ua")
	headers.Set("Originator", "old-origin")
	headers.Set("X-Test-Header", "old-value")

	applyModelHeaderOverrides(headers, "test-override-headers-model")

	if got := headers.Get("User-Agent"); got != "custom-ua/1.0" {
		t.Fatalf("User-Agent = %q, want custom-ua/1.0", got)
	}
	if got := headers.Get("Originator"); got != "custom-origin" {
		t.Fatalf("Originator = %q, want custom-origin", got)
	}
	if got := headers.Get("X-Test-Header"); got != "forced-value" {
		t.Fatalf("X-Test-Header = %q, want forced-value", got)
	}
}

func TestFinalizeCodexHeadersPreservesModelOverridePriority(t *testing.T) {
	reg := registry.GetGlobalRegistry()
	clientID := "test-model-metadata-header-override"
	modelID := "test-metadata-header-override-model"
	reg.RegisterClient(clientID, "codex", []*registry.ModelInfo{{
		ID: modelID,
		Config: &registry.ModelConfig{OverrideHeader: map[string]string{
			codexTurnStateHeader:     "forced-state",
			codexResponsesLiteHeader: "false",
			"X-Codex-Turn-Metadata":  `{"turn_id":"forced-turn"}`,
			"X-Codex-Window-Id":      "forced-window",
		}},
	}})
	t.Cleanup(func() { reg.UnregisterClient(clientID) })

	body := []byte(`{"client_metadata":{"x-codex-turn-state":"client-state","ws_request_header_x_openai_internal_codex_responses_lite":"true","x-codex-turn-metadata":"{\"turn_id\":\"client-turn\"}","x-codex-window-id":"client-window"}}`)

	httpHeaders := http.Header{}
	finalizeCodexHTTPHeaders(httpHeaders, body, modelID, nil)
	if got := httpHeaders.Get(codexTurnStateHeader); got != "forced-state" {
		t.Fatalf("HTTP %s = %q, want forced-state", codexTurnStateHeader, got)
	}
	if got := httpHeaders.Get(codexResponsesLiteHeader); got != "false" {
		t.Fatalf("HTTP %s = %q, want false", codexResponsesLiteHeader, got)
	}

	websocketHeaders := http.Header{}
	finalizeCodexWebsocketHeaders(websocketHeaders, body, modelID, nil)
	if got := websocketHeaders.Get("X-Codex-Turn-Metadata"); got != `{"turn_id":"forced-turn"}` {
		t.Fatalf("WebSocket X-Codex-Turn-Metadata = %q, want forced metadata", got)
	}
	if got := websocketHeaders.Get("X-Codex-Window-Id"); got != "forced-window" {
		t.Fatalf("WebSocket X-Codex-Window-Id = %q, want forced-window", got)
	}
}

func TestApplyCodexHeadersPassesThroughClientIdentityHeaders(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://example.com/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	req = req.WithContext(contextWithGinHeaders(map[string]string{
		"Originator":            "Codex Desktop",
		"Version":               "0.115.0-alpha.27",
		"X-Codex-Turn-Metadata": `{"turn_id":"turn-1"}`,
		"X-Client-Request-Id":   "019d2233-e240-7162-992d-38df0a2a0e0d",
	}))

	applyCodexHeaders(req, auth, "oauth-token", true, nil)

	if got := req.Header.Get("Originator"); got != "Codex Desktop" {
		t.Fatalf("Originator = %s, want %s", got, "Codex Desktop")
	}
	if got := req.Header.Get("Version"); got != "0.115.0-alpha.27" {
		t.Fatalf("Version = %s, want %s", got, "0.115.0-alpha.27")
	}
	if got := req.Header.Get("X-Codex-Turn-Metadata"); got != `{"turn_id":"turn-1"}` {
		t.Fatalf("X-Codex-Turn-Metadata = %s, want %s", got, `{"turn_id":"turn-1"}`)
	}
	if got := req.Header.Get("X-Client-Request-Id"); got != "019d2233-e240-7162-992d-38df0a2a0e0d" {
		t.Fatalf("X-Client-Request-Id = %s, want %s", got, "019d2233-e240-7162-992d-38df0a2a0e0d")
	}
}

func TestApplyCodexHeadersDoesNotInjectClientOnlyHeadersByDefault(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://example.com/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	applyCodexHeaders(req, nil, "oauth-token", true, nil)

	if got := req.Header.Get("Version"); got != "" {
		t.Fatalf("Version = %q, want empty", got)
	}
	if got := req.Header.Get("X-Codex-Turn-Metadata"); got != "" {
		t.Fatalf("X-Codex-Turn-Metadata = %q, want empty", got)
	}
	if got := req.Header.Get("X-Client-Request-Id"); got != "" {
		t.Fatalf("X-Client-Request-Id = %q, want empty", got)
	}
}

func contextWithGinHeaders(headers map[string]string) context.Context {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	ginCtx.Request.Header = make(http.Header, len(headers))
	for key, value := range headers {
		ginCtx.Request.Header.Set(key, value)
	}
	return context.WithValue(context.Background(), "gin", ginCtx)
}

func TestNewProxyAwareWebsocketDialerDirectDisablesProxy(t *testing.T) {
	t.Parallel()

	dialer := newProxyAwareWebsocketDialer(
		&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"}},
		&cliproxyauth.Auth{ProxyURL: "direct"},
	)

	if dialer.Proxy != nil {
		t.Fatal("expected websocket proxy function to be nil for direct mode")
	}
}

func TestCodexWebsocketUpgradeRequiredDoesNotFallbackToHTTPWithLifecycle(t *testing.T) {
	var httpFallbackCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			httpFallbackCalls.Add(1)
			http.Error(w, "unexpected HTTP fallback", http.StatusInternalServerError)
			return
		}
		http.Error(w, "websocket upgrade required", http.StatusUpgradeRequired)
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-a", Provider: "codex", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"hello"}]}`)}
	opts := cliproxyexecutor.Options{
		SourceFormat:       sdktranslator.FromString("openai-response"),
		ResponseFormat:     sdktranslator.FromString("openai-response"),
		ExecutionLifecycle: newTerminalFailureLifecycle(),
	}

	if _, errExecute := exec.ExecuteStream(context.Background(), auth, req, opts); errExecute == nil {
		t.Fatal("ExecuteStream() error = nil, want failed Home lifecycle attempt")
	}
	if got := httpFallbackCalls.Load(); got != 0 {
		t.Fatalf("HTTP fallback calls = %d, want 0 with an execution lifecycle", got)
	}
}

func TestCodexWebsocketHandshakeFailureReleasesSessionRequestLock(t *testing.T) {
	for _, statusCode := range []int{http.StatusUpgradeRequired, http.StatusBadGateway} {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "upstream rejected websocket", statusCode)
			}))
			defer server.Close()

			exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
			exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
			auth := &cliproxyauth.Auth{ID: "auth-a", Provider: "codex", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
			req := cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"hello"}]}`)}
			opts := cliproxyexecutor.Options{
				SourceFormat:   sdktranslator.FromString("openai-response"),
				ResponseFormat: sdktranslator.FromString("openai-response"),
				Metadata: map[string]any{
					cliproxyexecutor.ExecutionSessionMetadataKey: "failed-handshake",
				},
			}

			_, _ = exec.ExecuteStream(context.Background(), auth, req, opts)
			sess := exec.getOrCreateSession("failed-handshake")
			acquired := make(chan struct{})
			go func() {
				sess.reqMu.Lock()
				close(acquired)
				sess.reqMu.Unlock()
			}()
			select {
			case <-acquired:
			case <-time.After(time.Second):
				t.Fatal("websocket handshake failure left the session request lock held")
			}
		})
	}
}

func TestParseCodexWebsocketResponseFailedInfersStatus(t *testing.T) {
	tests := []struct {
		name     string
		payload  string
		wantCode int
	}{
		{
			name:     "usage limit reached maps to 429",
			payload:  `{"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"type":"usage_limit_reached","message":"usage limit reached"}}}`,
			wantCode: http.StatusTooManyRequests,
		},
		{
			name:     "model capacity maps to 429",
			payload:  `{"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"message":"The selected model is at capacity. Please try a different model."}}}`,
			wantCode: http.StatusTooManyRequests,
		},
		{
			name:     "rate limit error maps to 429",
			payload:  `{"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"too many requests"}}}`,
			wantCode: http.StatusTooManyRequests,
		},
		{
			name:     "insufficient quota maps to 429",
			payload:  `{"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"code":"insufficient_quota","message":"quota exceeded"}}}`,
			wantCode: http.StatusTooManyRequests,
		},
		{
			name:     "authentication error maps to 401",
			payload:  `{"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"type":"authentication_error","message":"expired token"}}}`,
			wantCode: http.StatusUnauthorized,
		},
		{
			name:     "context length exceeded maps to 400",
			payload:  `{"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"code":"context_length_exceeded","message":"context too large"}}}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid prompt maps to 400",
			payload:  `{"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"code":"invalid_prompt","message":"invalid prompt"}}}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "bio policy maps to 400",
			payload:  `{"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"code":"bio_policy","message":"biological policy rejection"}}}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "cyber policy maps to 400",
			payload:  `{"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"code":"cyber_policy","message":"cyber policy rejection"}}}`,
			wantCode: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streamErr, ok := parseCodexWebsocketResponseFailed([]byte(tt.payload))
			if !ok {
				t.Fatalf("parseCodexWebsocketResponseFailed returned ok=false for %s: %s", tt.name, tt.payload)
			}
			if got := streamErr.StatusCode(); got != tt.wantCode {
				t.Fatalf("StatusCode() = %d, want %d for %s: %s", got, tt.wantCode, tt.name, tt.payload)
			}
		})
	}
}

type terminalFailureLifecycle struct {
	active atomic.Bool
	ends   atomic.Int32
}

func newTerminalFailureLifecycle() *terminalFailureLifecycle {
	lifecycle := &terminalFailureLifecycle{}
	lifecycle.active.Store(true)
	return lifecycle
}

func (*terminalFailureLifecycle) Bind(func() error) error { return nil }
func (l *terminalFailureLifecycle) End(string) {
	l.ends.Add(1)
	l.active.Store(false)
}
func (*terminalFailureLifecycle) Retain() {}

func TestCodexWebsocketTerminalFailureInvalidatesRetainedLifecycle(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	firstRelease := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		connection := connections.Add(1)
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			return
		}
		terminal := []byte(`{"type":"response.failed","response":{"error":{"type":"authentication_error","code":"invalid_api_key","message":"Invalid token."}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, terminal); errWrite != nil {
			t.Errorf("write terminal response: %v", errWrite)
		}
		if connection == 1 {
			<-firstRelease
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	auth := &cliproxyauth.Auth{ID: "auth-a", Provider: "codex", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"hello"}]}`)}
	opts := cliproxyexecutor.Options{
		SourceFormat:       sdktranslator.FromString("openai-response"),
		ResponseFormat:     sdktranslator.FromString("openai-response"),
		ExecutionLifecycle: newTerminalFailureLifecycle(),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: "terminal-failure",
		},
	}

	result, errExecute := exec.ExecuteStream(context.Background(), auth, req, opts)
	if errExecute != nil {
		t.Fatalf("first ExecuteStream() error = %v", errExecute)
	}
	for chunk := range result.Chunks {
		if chunk.Err == nil {
			continue
		}
	}
	lifecycle := opts.ExecutionLifecycle.(*terminalFailureLifecycle)
	if lifecycle.active.Load() {
		t.Fatal("terminal failure left the retained lifecycle active")
	}
	if got := lifecycle.ends.Load(); got != 1 {
		t.Fatalf("retained lifecycle End calls = %d, want 1", got)
	}
	sess := exec.getOrCreateSession("terminal-failure")
	sess.connMu.Lock()
	connected := sess.conn != nil
	sess.connMu.Unlock()
	if connected {
		t.Fatal("terminal failure left the upstream session connection cached")
	}
	close(firstRelease)

	opts.ExecutionLifecycle = newTerminalFailureLifecycle()
	result, errExecute = exec.ExecuteStream(context.Background(), auth, req, opts)
	if errExecute != nil {
		t.Fatalf("second ExecuteStream() error = %v", errExecute)
	}
	for range result.Chunks {
	}
	if got := connections.Load(); got != 2 {
		t.Fatalf("websocket connections = %d, want 2 after terminal invalidation", got)
	}
}

type rejectingExecutionLifecycle struct{}

func (rejectingExecutionLifecycle) Bind(func() error) error {
	return errors.New("lifecycle bind rejected")
}
func (rejectingExecutionLifecycle) End(string) {}

func TestCodexWebsocketNonstreamLifecycleBindFailureDetachesConnection(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	closed := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		connection := connections.Add(1)
		defer func() {
			_ = conn.Close()
			if connection == 1 {
				closed <- struct{}{}
			}
		}()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			return
		}
		completed := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Errorf("write completed response: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	auth := &cliproxyauth.Auth{ID: "auth-a", Provider: "codex", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"hello"}]}`)}
	opts := cliproxyexecutor.Options{
		SourceFormat:       sdktranslator.FromString("openai-response"),
		ResponseFormat:     sdktranslator.FromString("openai-response"),
		ExecutionLifecycle: rejectingExecutionLifecycle{},
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: "nonstream-bind-failed",
		},
	}
	if _, errExecute := exec.Execute(context.Background(), auth, req, opts); errExecute == nil {
		t.Fatal("Execute() error = nil, want lifecycle bind failure")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("nonstream lifecycle bind failure did not close the upstream websocket")
	}
	sess := exec.getOrCreateSession("nonstream-bind-failed")
	sess.connMu.Lock()
	connected := sess.conn != nil
	sess.connMu.Unlock()
	if connected {
		t.Fatal("nonstream lifecycle bind failure left the closed connection attached to the session")
	}

	opts.ExecutionLifecycle = nil
	if _, errExecute := exec.Execute(context.Background(), auth, req, opts); errExecute != nil {
		t.Fatalf("second Execute() error = %v", errExecute)
	}
	if got := connections.Load(); got != 2 {
		t.Fatalf("websocket connections = %d, want 2 after bind failure", got)
	}
}

func TestCodexWebsocketLifecycleBindFailureReleasesSessionRequestLock(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	closed := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		defer func() {
			_ = conn.Close()
			closed <- struct{}{}
		}()
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	auth := &cliproxyauth.Auth{ID: "auth-a", Provider: "codex", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"hello"}]}`)}
	opts := cliproxyexecutor.Options{
		SourceFormat:       sdktranslator.FromString("openai-response"),
		ResponseFormat:     sdktranslator.FromString("openai-response"),
		ExecutionLifecycle: rejectingExecutionLifecycle{},
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: "bind-failed",
		},
	}
	if _, errExecute := exec.ExecuteStream(context.Background(), auth, req, opts); errExecute == nil {
		t.Fatal("ExecuteStream() error = nil, want lifecycle bind failure")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("lifecycle bind failure did not close the upstream websocket")
	}

	sess := exec.getOrCreateSession("bind-failed")
	acquired := make(chan struct{})
	go func() {
		sess.reqMu.Lock()
		close(acquired)
		sess.reqMu.Unlock()
	}()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("lifecycle bind failure left the session request lock held")
	}
}
