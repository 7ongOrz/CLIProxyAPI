package executor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	_ "github.com/router-for-me/CLIProxyAPI/v7/internal/translator"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

type interruptedStreamReader struct{}

func (interruptedStreamReader) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

type interruptedStreamRoundTripper func(*http.Request) (*http.Response, error)

func (f interruptedStreamRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func interruptedStreamResponse(req *http.Request, payload string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body:       io.NopCloser(io.MultiReader(strings.NewReader(payload), interruptedStreamReader{})),
		Request:    req,
	}
}

func assertStreamErrorWithoutCompletion(t *testing.T, result *cliproxyexecutor.StreamResult) {
	t.Helper()
	if result == nil {
		t.Fatal("stream result is nil")
	}
	var output bytes.Buffer
	var streamErr error
	for chunk := range result.Chunks {
		output.Write(chunk.Payload)
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if !errors.Is(streamErr, io.ErrUnexpectedEOF) {
		t.Fatalf("stream error = %v, want unexpected EOF", streamErr)
	}
	if strings.Contains(output.String(), `"type":"response.completed"`) {
		t.Fatalf("interrupted stream emitted response.completed: %s", output.String())
	}
}

func TestKimiExecutorStreamErrorPrecedesSyntheticCompletion(t *testing.T) {
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", interruptedStreamRoundTripper(func(req *http.Request) (*http.Response, error) {
		return interruptedStreamResponse(req, "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n"), nil
	}))
	exec := NewKimiExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		Attributes: map[string]string{},
		Metadata:   map[string]any{"access_token": "test-token"},
	}
	payload := []byte(`{"model":"kimi-k3","input":"hello","stream":true}`)
	result, errExecute := exec.ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "kimi-k3", Payload: payload}, cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FromString("openai-response"),
		ResponseFormat:  sdktranslator.FromString("openai-response"),
		OriginalRequest: payload,
		Stream:          true,
	})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	assertStreamErrorWithoutCompletion(t, result)
}

func TestGeminiExecutorStreamErrorPrecedesSyntheticCompletion(t *testing.T) {
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", interruptedStreamRoundTripper(func(req *http.Request) (*http.Response, error) {
		return interruptedStreamResponse(req, "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"partial\"}]}}]}\n\n"), nil
	}))
	exec := NewGeminiExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "test-key"}}
	payload := []byte(`{"model":"gemini-3.5-flash","input":"hello","stream":true}`)
	result, errExecute := exec.ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "gemini-3.5-flash", Payload: payload}, cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FromString("openai-response"),
		ResponseFormat:  sdktranslator.FromString("openai-response"),
		OriginalRequest: payload,
		Stream:          true,
	})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	assertStreamErrorWithoutCompletion(t, result)
}

func TestGeminiVertexExecutorStreamErrorPrecedesSyntheticCompletion(t *testing.T) {
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", interruptedStreamRoundTripper(func(req *http.Request) (*http.Response, error) {
		return interruptedStreamResponse(req, "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"partial\"}]}}]}\n\n"), nil
	}))
	exec := NewGeminiVertexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "test-key", "base_url": "https://vertex.test"}}
	payload := []byte(`{"model":"gemini-3.5-flash","input":"hello","stream":true}`)
	result, errExecute := exec.ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "gemini-3.5-flash", Payload: payload}, cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FromString("openai-response"),
		ResponseFormat:  sdktranslator.FromString("openai-response"),
		OriginalRequest: payload,
		Stream:          true,
	})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	assertStreamErrorWithoutCompletion(t, result)
}

func TestAntigravityExecutorStreamErrorPrecedesSyntheticCompletion(t *testing.T) {
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", interruptedStreamRoundTripper(func(req *http.Request) (*http.Response, error) {
		return interruptedStreamResponse(req, "data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"partial\"}]}}]}}\n\n"), nil
	}))
	exec := NewAntigravityExecutor(&config.Config{RequestRetry: 1})
	auth := &cliproxyauth.Auth{
		ID:         "antigravity-interrupted-stream",
		Provider:   "antigravity",
		Attributes: map[string]string{"base_url": "https://antigravity.test"},
		Metadata: map[string]any{
			"access_token": "test-token",
			"project_id":   "test-project",
			"expired":      time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}
	payload := []byte(`{"model":"gemini-3.5-flash-low","input":"hello","stream":true}`)
	result, errExecute := exec.ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "gemini-3.5-flash-low", Payload: payload}, cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FromString("openai-response"),
		ResponseFormat:  sdktranslator.FromString("openai-response"),
		OriginalRequest: payload,
		Stream:          true,
	})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	assertStreamErrorWithoutCompletion(t, result)
}
