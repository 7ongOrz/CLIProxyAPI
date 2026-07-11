package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexExecutorCompactPreservesResponsesLiteContract(t *testing.T) {
	var gotHeaders http.Header
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp-1","object":"response.compaction","output":[]}`))
	}))
	defer server.Close()

	headers := http.Header{}
	headers.Set(codexTurnStateHeader, "state-1")
	headers.Set(codexResponsesLiteHeader, "true")
	headers.Set("X-Codex-Installation-Id", "install-compact-1")
	headers.Set("X-OpenAI-Subagent", "compact")
	headers.Set("Session-Id", "session-compact-1")
	headers.Set("Thread-Id", "thread-compact-1")
	exec := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Attributes: map[string]string{
			"api_key":  "sk-test",
			"base_url": server.URL,
		},
	}
	payload := []byte(`{
		"model":"gpt-5.6-sol",
		"input":[{"type":"additional_tools","role":"developer","tools":[{"type":"function","name":"shell","parameters":{}}]}],
		"parallel_tool_calls":false
	}`)

	_, err := exec.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.6-sol",
		Payload: payload,
	}, cliproxyexecutor.Options{
		Alt:             "responses/compact",
		Headers:         headers,
		OriginalRequest: payload,
		SourceFormat:    sdktranslator.FromString("openai-response"),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := gotHeaders.Get(codexTurnStateHeader); got != "state-1" {
		t.Fatalf("%s = %q, want state-1", codexTurnStateHeader, got)
	}
	if got := gotHeaders.Get(codexResponsesLiteHeader); got != "true" {
		t.Fatalf("%s = %q, want true", codexResponsesLiteHeader, got)
	}
	for _, tt := range []struct {
		name string
		want string
	}{
		{name: "X-Codex-Installation-Id", want: "install-compact-1"},
		{name: "X-OpenAI-Subagent", want: "compact"},
		{name: "Session-Id", want: "session-compact-1"},
		{name: "Thread-Id", want: "thread-compact-1"},
	} {
		if got := gotHeaders.Get(tt.name); got != tt.want {
			t.Fatalf("%s = %q, want %q", tt.name, got, tt.want)
		}
	}
	if gjson.GetBytes(gotBody, "tools").Exists() {
		t.Fatalf("Responses Lite compact request contains top-level tools: %s", gotBody)
	}
	if gjson.GetBytes(gotBody, "instructions").Exists() {
		t.Fatalf("Responses Lite compact request contains top-level instructions: %s", gotBody)
	}
	if got := gjson.GetBytes(gotBody, "input.0.type").String(); got != "additional_tools" {
		t.Fatalf("input.0.type = %q, want additional_tools; body=%s", got, gotBody)
	}
	if got := gjson.GetBytes(gotBody, "input.0.tools.0.name").String(); got != "shell" {
		t.Fatalf("additional_tools changed: body=%s", gotBody)
	}
	if got := gjson.GetBytes(gotBody, "parallel_tool_calls"); !got.Exists() || got.Bool() {
		t.Fatalf("parallel_tool_calls = %s, want false", got.Raw)
	}
}

func TestCodexWebsocketsExecuteStreamPreservesResponsesLiteContractOnHTTPFallback(t *testing.T) {
	var gotHeaders http.Header
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if websocket.IsWebSocketUpgrade(r) {
			w.WriteHeader(http.StatusUpgradeRequired)
			return
		}
		gotHeaders = r.Header.Clone()
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[],\"usage\":{\"input_tokens\":0,\"output_tokens\":0,\"total_tokens\":0}}}\n\n"))
	}))
	defer server.Close()

	payload := []byte(`{
		"model":"gpt-5.6-sol",
		"input":[{"type":"additional_tools","role":"developer","tools":[{"type":"function","name":"shell","parameters":{}}]}],
		"parallel_tool_calls":false,
		"client_metadata":{
			"x-codex-turn-state":"request-state",
			"ws_request_header_x_openai_internal_codex_responses_lite":"true",
			"x-codex-turn-metadata":"{\"turn_id\":\"request-turn\"}",
			"x-codex-window-id":"request-window",
			"x-codex-parent-thread-id":"request-parent",
			"x-openai-subagent":"compact",
			"session_id":"request-session",
			"thread_id":"request-thread"
		}
	}`)
	exec := NewCodexWebsocketsExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		ID:       "auth-http-fallback",
		Provider: "codex",
		Attributes: map[string]string{
			"api_key":  "sk-test",
			"base_url": server.URL,
		},
	}
	result, err := exec.ExecuteStream(
		context.Background(),
		auth,
		cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: payload},
		cliproxyexecutor.Options{
			Headers: http.Header{
				codexTurnStateHeader:       {"handshake-state"},
				"X-Codex-Turn-Metadata":    {`{"turn_id":"handshake-turn"}`},
				"X-Codex-Window-Id":        {"handshake-window"},
				"X-Codex-Parent-Thread-Id": {"handshake-parent"},
				"X-OpenAI-Subagent":        {"handshake-subagent"},
				"Session-Id":               {"handshake-session"},
				"Thread-Id":                {"handshake-thread"},
				"X-Client-Request-Id":      {"handshake-thread"},
			},
			OriginalRequest: payload,
			SourceFormat:    sdktranslator.FromString("openai-response"),
		},
	)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream error = %v", chunk.Err)
		}
	}

	if got := gotHeaders.Get(codexTurnStateHeader); got != "request-state" {
		t.Fatalf("%s = %q, want request-state", codexTurnStateHeader, got)
	}
	if got := gotHeaders.Get(codexResponsesLiteHeader); got != "true" {
		t.Fatalf("%s = %q, want true", codexResponsesLiteHeader, got)
	}
	for _, tt := range []struct {
		name string
		want string
	}{
		{name: "X-Codex-Turn-Metadata", want: `{"turn_id":"request-turn"}`},
		{name: "X-Codex-Window-Id", want: "request-window"},
		{name: "X-Codex-Parent-Thread-Id", want: "request-parent"},
		{name: "X-OpenAI-Subagent", want: "compact"},
		{name: "Session-Id", want: "request-session"},
		{name: "Thread-Id", want: "request-thread"},
		{name: "X-Client-Request-Id", want: "request-thread"},
	} {
		if got := gotHeaders.Get(tt.name); got != tt.want {
			t.Fatalf("%s = %q, want %q", tt.name, got, tt.want)
		}
	}
	if gjson.GetBytes(gotBody, "tools").Exists() {
		t.Fatalf("Responses Lite HTTP fallback contains top-level tools: %s", gotBody)
	}
	if gjson.GetBytes(gotBody, "instructions").Exists() {
		t.Fatalf("Responses Lite HTTP fallback contains top-level instructions: %s", gotBody)
	}
	if got := gjson.GetBytes(gotBody, "parallel_tool_calls"); !got.Exists() || got.Bool() {
		t.Fatalf("parallel_tool_calls = %s, want false", got.Raw)
	}
}

func TestCodexWebsocketsExecuteStreamPreservesResponsesLiteToolContract(t *testing.T) {
	tests := []struct {
		name          string
		payload       []byte
		responsesLite bool
	}{
		{
			name: "lite",
			payload: []byte(`{
				"model":"gpt-5.6-sol",
				"input":[{"type":"additional_tools","role":"developer","tools":[{"type":"function","name":"shell","parameters":{}}]}],
				"parallel_tool_calls":false,
				"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"true"}
			}`),
			responsesLite: true,
		},
		{
			name:    "non-lite",
			payload: []byte(`{"model":"gpt-5.4","input":[{"type":"message","role":"user","content":"hello"}]}`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
				ID:       "auth-" + tc.name,
				Provider: "codex",
				Attributes: map[string]string{
					"api_key":  "sk-test",
					"base_url": server.URL,
				},
			}
			result, err := exec.ExecuteStream(
				cliproxyexecutor.WithDownstreamWebsocket(context.Background()),
				auth,
				cliproxyexecutor.Request{Model: gjson.GetBytes(tc.payload, "model").String(), Payload: tc.payload},
				cliproxyexecutor.Options{
					OriginalRequest: tc.payload,
					SourceFormat:    sdktranslator.FromString("openai-response"),
				},
			)
			if err != nil {
				t.Fatalf("ExecuteStream() error = %v", err)
			}
			for chunk := range result.Chunks {
				if chunk.Err != nil {
					t.Fatalf("stream error = %v", chunk.Err)
				}
			}

			select {
			case payload := <-capturedPayload:
				tools := gjson.GetBytes(payload, "tools")
				if tc.responsesLite {
					if tools.Exists() {
						t.Fatalf("Responses Lite websocket request contains top-level tools: %s", payload)
					}
					if gjson.GetBytes(payload, "instructions").Exists() {
						t.Fatalf("Responses Lite websocket request contains top-level instructions: %s", payload)
					}
					if got := gjson.GetBytes(payload, "input.0.tools.0.name").String(); got != "shell" {
						t.Fatalf("additional_tools changed: payload=%s", payload)
					}
					if got := gjson.GetBytes(payload, "parallel_tool_calls"); !got.Exists() || got.Bool() {
						t.Fatalf("parallel_tool_calls = %s, want false", got.Raw)
					}
					return
				}
				if got := tools.Get("0.type").String(); got != "image_generation" {
					t.Fatalf("tools.0.type = %q, want image_generation; payload=%s", got, payload)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for upstream websocket payload")
			}
		})
	}
}
