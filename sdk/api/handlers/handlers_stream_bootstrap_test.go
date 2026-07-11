package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executionregistry"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

type failOnceStreamExecutor struct {
	mu             sync.Mutex
	calls          int
	refreshes      int
	resultErrFirst bool
}

func (e *failOnceStreamExecutor) Identifier() string { return "codex" }

func (e *failOnceStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "Execute not implemented"}
}

func (e *failOnceStreamExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()

	ch := make(chan coreexecutor.StreamChunk, 1)
	if call == 1 {
		streamErr := &coreauth.Error{
			Code:       "unauthorized",
			Message:    "unauthorized for confused upstream identity",
			Retryable:  false,
			HTTPStatus: http.StatusUnauthorized,
		}
		chunk := coreexecutor.StreamChunk{Err: streamErr}
		if e.resultErrFirst {
			chunk = coreexecutor.StreamChunk{
				Payload:   []byte(`{"type":"error","status":401,"error":{"message":"unauthorized for original client identity"}}`),
				ResultErr: streamErr,
			}
		}
		ch <- chunk
		close(ch)
		return &coreexecutor.StreamResult{
			Headers: http.Header{"X-Upstream-Attempt": {"1"}},
			Chunks:  ch,
		}, nil
	}

	ch <- coreexecutor.StreamChunk{Payload: []byte("ok")}
	close(ch)
	return &coreexecutor.StreamResult{
		Headers: http.Header{
			"X-Upstream-Attempt": {"2"},
			"X-Codex-Turn-State": {"state-2"},
		},
		Chunks: ch,
	}, nil
}

func (e *failOnceStreamExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	e.mu.Lock()
	e.refreshes++
	e.mu.Unlock()
	return auth, nil
}

func (e *failOnceStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "CountTokens not implemented"}
}

func (e *failOnceStreamExecutor) HttpRequest(ctx context.Context, auth *coreauth.Auth, req *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{
		Code:       "not_implemented",
		Message:    "HttpRequest not implemented",
		HTTPStatus: http.StatusNotImplemented,
	}
}

func (e *failOnceStreamExecutor) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

func (e *failOnceStreamExecutor) Refreshes() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.refreshes
}

type blockingRetryStreamExecutor struct {
	mu           sync.Mutex
	calls        int
	retryStarted chan struct{}
	allowRetry   chan struct{}
}

func (e *blockingRetryStreamExecutor) Identifier() string { return "codex" }

func (e *blockingRetryStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "Execute not implemented"}
}

func (e *blockingRetryStreamExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()

	if call == 1 {
		chunks := make(chan coreexecutor.StreamChunk, 1)
		chunks <- coreexecutor.StreamChunk{Err: &coreauth.Error{Code: "unauthorized", Message: "unauthorized", HTTPStatus: http.StatusUnauthorized}}
		close(chunks)
		return &coreexecutor.StreamResult{Headers: http.Header{"X-Upstream-Attempt": {"1"}}, Chunks: chunks}, nil
	}

	close(e.retryStarted)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.allowRetry:
	}
	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Payload: []byte("ok")}
	close(chunks)
	return &coreexecutor.StreamResult{Headers: http.Header{"X-Upstream-Attempt": {"2"}}, Chunks: chunks}, nil
}

func (e *blockingRetryStreamExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *blockingRetryStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "CountTokens not implemented"}
}

func (e *blockingRetryStreamExecutor) HttpRequest(ctx context.Context, auth *coreauth.Auth, req *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{Code: "not_implemented", Message: "HttpRequest not implemented", HTTPStatus: http.StatusNotImplemented}
}

type payloadThenErrorStreamExecutor struct {
	mu                    sync.Mutex
	calls                 int
	resultErrOnly         bool
	resultErr             error
	preludePayload        []byte
	resultPayload         []byte
	succeedAfterResultErr bool
	successPayload        []byte
}

type streamResultError struct {
	message    string
	status     int
	retryAfter time.Duration
}

func (e *streamResultError) Error() string { return e.message }

func (e *streamResultError) StatusCode() int { return e.status }

func (e *streamResultError) RetryAfter() *time.Duration { return &e.retryAfter }

func (e *payloadThenErrorStreamExecutor) Identifier() string { return "codex" }

func (e *payloadThenErrorStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "Execute not implemented"}
}

func (e *payloadThenErrorStreamExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()

	if e.succeedAfterResultErr && call > 1 {
		ch := make(chan coreexecutor.StreamChunk, 1)
		ch <- coreexecutor.StreamChunk{Payload: e.successPayload}
		close(ch)
		return &coreexecutor.StreamResult{Chunks: ch}, nil
	}

	streamErr := &coreauth.Error{
		Code:       "upstream_closed",
		Message:    "upstream closed",
		Retryable:  false,
		HTTPStatus: http.StatusBadGateway,
	}
	if e.resultErrOnly {
		resultErr := error(streamErr)
		if e.resultErr != nil {
			resultErr = e.resultErr
		}
		ch := make(chan coreexecutor.StreamChunk, 2)
		if len(e.preludePayload) > 0 {
			ch <- coreexecutor.StreamChunk{Payload: e.preludePayload}
		}
		resultPayload := e.resultPayload
		if len(resultPayload) == 0 {
			resultPayload = []byte("partial")
		}
		ch <- coreexecutor.StreamChunk{Payload: resultPayload, ResultErr: resultErr}
		close(ch)
		return &coreexecutor.StreamResult{Chunks: ch}, nil
	}

	ch := make(chan coreexecutor.StreamChunk, 2)
	ch <- coreexecutor.StreamChunk{Payload: []byte("partial")}
	ch <- coreexecutor.StreamChunk{Err: streamErr}
	close(ch)
	return &coreexecutor.StreamResult{Chunks: ch}, nil
}

func (e *payloadThenErrorStreamExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *payloadThenErrorStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "CountTokens not implemented"}
}

func (e *payloadThenErrorStreamExecutor) HttpRequest(ctx context.Context, auth *coreauth.Auth, req *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{
		Code:       "not_implemented",
		Message:    "HttpRequest not implemented",
		HTTPStatus: http.StatusNotImplemented,
	}
}

func (e *payloadThenErrorStreamExecutor) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

type authAwareStreamExecutor struct {
	mu      sync.Mutex
	calls   int
	authIDs []string
}

type invalidJSONStreamExecutor struct{}

type splitResponsesEventStreamExecutor struct{}

func (e *invalidJSONStreamExecutor) Identifier() string { return "codex" }

func (e *invalidJSONStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "Execute not implemented"}
}

func (e *invalidJSONStreamExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	ch := make(chan coreexecutor.StreamChunk, 1)
	ch <- coreexecutor.StreamChunk{Payload: []byte("event: response.completed\ndata: {\"type\"")}
	close(ch)
	return &coreexecutor.StreamResult{Chunks: ch}, nil
}

func (e *invalidJSONStreamExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *invalidJSONStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "CountTokens not implemented"}
}

func (e *invalidJSONStreamExecutor) HttpRequest(ctx context.Context, auth *coreauth.Auth, req *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{
		Code:       "not_implemented",
		Message:    "HttpRequest not implemented",
		HTTPStatus: http.StatusNotImplemented,
	}
}

func (e *splitResponsesEventStreamExecutor) Identifier() string { return "split-sse" }

func (e *splitResponsesEventStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "Execute not implemented"}
}

func (e *splitResponsesEventStreamExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	ch := make(chan coreexecutor.StreamChunk, 2)
	ch <- coreexecutor.StreamChunk{Payload: []byte("event: response.completed")}
	ch <- coreexecutor.StreamChunk{Payload: []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[]}}")}
	close(ch)
	return &coreexecutor.StreamResult{Chunks: ch}, nil
}

func (e *splitResponsesEventStreamExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *splitResponsesEventStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "CountTokens not implemented"}
}

func (e *splitResponsesEventStreamExecutor) HttpRequest(ctx context.Context, auth *coreauth.Auth, req *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{
		Code:       "not_implemented",
		Message:    "HttpRequest not implemented",
		HTTPStatus: http.StatusNotImplemented,
	}
}

func (e *authAwareStreamExecutor) Identifier() string { return "codex" }

func (e *authAwareStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "Execute not implemented"}
}

func (e *authAwareStreamExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	_ = ctx
	_ = req
	_ = opts
	ch := make(chan coreexecutor.StreamChunk, 1)

	authID := ""
	if auth != nil {
		authID = auth.ID
	}

	e.mu.Lock()
	e.calls++
	e.authIDs = append(e.authIDs, authID)
	e.mu.Unlock()

	if authID == "auth1" {
		ch <- coreexecutor.StreamChunk{
			Err: &coreauth.Error{
				Code:       "unauthorized",
				Message:    "unauthorized",
				Retryable:  false,
				HTTPStatus: http.StatusUnauthorized,
			},
		}
		close(ch)
		return &coreexecutor.StreamResult{Chunks: ch}, nil
	}

	ch <- coreexecutor.StreamChunk{Payload: []byte("ok")}
	close(ch)
	return &coreexecutor.StreamResult{Chunks: ch}, nil
}

func (e *authAwareStreamExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *authAwareStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "CountTokens not implemented"}
}

func (e *authAwareStreamExecutor) HttpRequest(ctx context.Context, auth *coreauth.Auth, req *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{
		Code:       "not_implemented",
		Message:    "HttpRequest not implemented",
		HTTPStatus: http.StatusNotImplemented,
	}
}

func (e *authAwareStreamExecutor) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

func (e *authAwareStreamExecutor) AuthIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.authIDs))
	copy(out, e.authIDs)
	return out
}

func TestExecuteStreamWithAuthManager_RetriesBootstrapResultErrorBeforeForwarding(t *testing.T) {
	executor := &failOnceStreamExecutor{resultErrFirst: true}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{
		ID:       "auth1",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"email":         "test1@example.com",
			"access_token":  "access-token-1",
			"refresh_token": "refresh-token-1",
		},
	}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}

	auth2 := &coreauth.Auth{
		ID:       "auth2",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"email":         "test2@example.com",
			"access_token":  "access-token-2",
			"refresh_token": "refresh-token-2",
		},
	}
	if _, err := manager.Register(context.Background(), auth2); err != nil {
		t.Fatalf("manager.Register(auth2): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	registry.GetGlobalRegistry().RegisterClient(auth2.ID, auth2.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
		registry.GetGlobalRegistry().UnregisterClient(auth2.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		PassthroughHeaders: true,
		Streaming: sdkconfig.StreamingConfig{
			BootstrapRetries: 1,
		},
	}, manager)
	dataChan, upstreamHeaders, errChan := handler.ExecuteStreamWithAuthManager(context.Background(), "openai", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}

	for msg := range errChan {
		if msg != nil {
			t.Fatalf("unexpected error: %+v", msg)
		}
	}

	if string(got) != "ok" {
		t.Fatalf("expected payload ok, got %q", string(got))
	}
	if executor.Calls() != 2 {
		t.Fatalf("expected 2 stream attempts, got %d", executor.Calls())
	}
	if executor.Refreshes() != 1 {
		t.Fatalf("expected 1 credential refresh, got %d", executor.Refreshes())
	}
	upstreamAttemptHeader := upstreamHeaders.Get("X-Upstream-Attempt")
	if upstreamAttemptHeader != "2" {
		t.Fatalf("expected upstream header from retry attempt, got %q", upstreamAttemptHeader)
	}
}

func TestExecuteStreamWithAuthManager_ResolvesBootstrapRetryHeadersBeforeReturn(t *testing.T) {
	executor := &blockingRetryStreamExecutor{
		retryStarted: make(chan struct{}),
		allowRetry:   make(chan struct{}),
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth1 := &coreauth.Auth{ID: "auth1", Provider: "codex", Status: coreauth.StatusActive, Metadata: map[string]any{"email": "test1@example.com"}}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}
	auth2 := &coreauth.Auth{ID: "auth2", Provider: "codex", Status: coreauth.StatusActive, Metadata: map[string]any{"email": "test2@example.com"}}
	if _, err := manager.Register(context.Background(), auth2); err != nil {
		t.Fatalf("manager.Register(auth2): %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	registry.GetGlobalRegistry().RegisterClient(auth2.ID, auth2.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
		registry.GetGlobalRegistry().UnregisterClient(auth2.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{PassthroughHeaders: true, Streaming: sdkconfig.StreamingConfig{BootstrapRetries: 1}}, manager)
	type streamResult struct {
		dataChan        <-chan []byte
		upstreamHeaders http.Header
		errChan         <-chan *interfaces.ErrorMessage
	}
	resultChan := make(chan streamResult, 1)
	go func() {
		dataChan, upstreamHeaders, errChan := handler.ExecuteStreamWithAuthManager(context.Background(), "openai", "test-model", []byte(`{"model":"test-model"}`), "")
		resultChan <- streamResult{dataChan: dataChan, upstreamHeaders: upstreamHeaders, errChan: errChan}
	}()

	select {
	case result := <-resultChan:
		t.Fatalf("ExecuteStreamWithAuthManager returned before bootstrap retry completed: %#v", result.upstreamHeaders)
	case <-executor.retryStarted:
	}
	select {
	case result := <-resultChan:
		t.Fatalf("ExecuteStreamWithAuthManager returned while bootstrap retry was blocked: %#v", result.upstreamHeaders)
	default:
	}
	close(executor.allowRetry)

	result := <-resultChan
	if result.upstreamHeaders.Get("X-Upstream-Attempt") != "2" {
		t.Fatalf("upstream headers = %#v, want retry attempt headers", result.upstreamHeaders)
	}
	for range result.dataChan {
	}
	for msg := range result.errChan {
		if msg != nil {
			t.Fatalf("unexpected stream error: %+v", msg)
		}
	}
}

type bootstrapStreamExecutor struct {
	mu     sync.Mutex
	calls  int
	stream func(context.Context, int) (*coreexecutor.StreamResult, error)
}

func (*bootstrapStreamExecutor) Identifier() string { return "bootstrap-test" }

func (e *bootstrapStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "Execute not implemented"}
}

func (e *bootstrapStreamExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()
	return e.stream(ctx, call)
}

func (e *bootstrapStreamExecutor) Refresh(context.Context, *coreauth.Auth) (*coreauth.Auth, error) {
	return nil, nil
}

func (e *bootstrapStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreauth.Error{Code: "not_implemented", Message: "CountTokens not implemented"}
}

func (e *bootstrapStreamExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{Code: "not_implemented", Message: "HttpRequest not implemented", HTTPStatus: http.StatusNotImplemented}
}

func (e *bootstrapStreamExecutor) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

func registerBootstrapExecutor(t *testing.T, executor *bootstrapStreamExecutor) (*BaseAPIHandler, *coreauth.Manager) {
	t.Helper()
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "bootstrap-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive, Metadata: map[string]any{"email": "bootstrap@example.com"}}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("manager.Register(): %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "bootstrap-model"}})
	authRetry := &coreauth.Auth{ID: "bootstrap-auth-retry", Provider: executor.Identifier(), Status: coreauth.StatusActive, Metadata: map[string]any{"email": "bootstrap-retry@example.com"}}
	if _, errRegister := manager.Register(context.Background(), authRetry); errRegister != nil {
		t.Fatalf("manager.Register(retry): %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authRetry.ID, authRetry.Provider, []*registry.ModelInfo{{ID: "bootstrap-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
		registry.GetGlobalRegistry().UnregisterClient(authRetry.ID)
	})
	return NewBaseAPIHandlers(&sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{BootstrapRetries: 1}}, manager), manager
}

func TestExecuteStreamWithAuthManager_RetriesAfterDroppedBootstrapPayload(t *testing.T) {
	executor := &bootstrapStreamExecutor{stream: func(_ context.Context, call int) (*coreexecutor.StreamResult, error) {
		chunks := make(chan coreexecutor.StreamChunk, 2)
		if call == 1 {
			chunks <- coreexecutor.StreamChunk{Payload: []byte("drop")}
			chunks <- coreexecutor.StreamChunk{Err: &coreauth.Error{HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"}}
		} else {
			chunks <- coreexecutor.StreamChunk{Payload: []byte("ok")}
		}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}}
	handler, _ := registerBootstrapExecutor(t, executor)
	var intercepted []string
	handler.SetPluginHost(&handlerInterceptorTestHost{interceptStreamChunk: func(_ context.Context, req pluginapi.StreamChunkInterceptRequest) pluginapi.StreamChunkInterceptResponse {
		if req.ChunkIndex >= 0 {
			intercepted = append(intercepted, string(req.Body))
		}
		return pluginapi.StreamChunkInterceptResponse{Body: cloneBytes(req.Body), DropChunk: string(req.Body) == "drop"}
	}})

	dataChan, _, errChan := handler.ExecuteStreamWithAuthManager(context.Background(), "openai", "bootstrap-model", []byte(`{"model":"bootstrap-model"}`), "")
	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}
	for msg := range errChan {
		if msg != nil {
			t.Fatalf("unexpected stream error: %+v", msg)
		}
	}
	if string(got) != "ok" {
		t.Fatalf("stream payload = %q, want ok", got)
	}
	if executor.Calls() != 2 {
		t.Fatalf("stream attempts = %d, want 2", executor.Calls())
	}
	if strings.Join(intercepted, ",") != "drop,ok" {
		t.Fatalf("intercepted payloads = %v, want [drop ok] without double interception", intercepted)
	}
}

func TestExecuteStreamWithAuthManager_CancelDuringSynchronousBootstrap(t *testing.T) {
	started := make(chan struct{})
	executor := &bootstrapStreamExecutor{stream: func(_ context.Context, _ int) (*coreexecutor.StreamResult, error) {
		close(started)
		return &coreexecutor.StreamResult{Chunks: make(chan coreexecutor.StreamChunk)}, nil
	}}
	handler, _ := registerBootstrapExecutor(t, executor)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		data <-chan []byte
		errs <-chan *interfaces.ErrorMessage
	}
	results := make(chan result, 1)
	go func() {
		dataChan, _, errChan := handler.ExecuteStreamWithAuthManager(ctx, "openai", "bootstrap-model", []byte(`{"model":"bootstrap-model"}`), "")
		results <- result{data: dataChan, errs: errChan}
	}()
	<-started
	cancel()
	select {
	case got := <-results:
		if got.data != nil {
			if _, ok := <-got.data; ok {
				t.Fatal("data channel remains open after bootstrap cancellation")
			}
		}
		if got.errs != nil {
			for range got.errs {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("bootstrap cancellation did not return")
	}
}

func TestExecuteStreamWithAuthManager_EmptyClosedStream(t *testing.T) {
	executor := &bootstrapStreamExecutor{stream: func(_ context.Context, _ int) (*coreexecutor.StreamResult, error) {
		chunks := make(chan coreexecutor.StreamChunk)
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}}
	handler, _ := registerBootstrapExecutor(t, executor)
	dataChan, _, errChan := handler.ExecuteStreamWithAuthManager(context.Background(), "openai", "bootstrap-model", []byte(`{"model":"bootstrap-model"}`), "")
	if _, ok := <-dataChan; ok {
		t.Fatal("empty stream produced data")
	}
	var streamErr *interfaces.ErrorMessage
	for msg := range errChan {
		if msg != nil {
			streamErr = msg
		}
	}
	if streamErr == nil || streamErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("empty stream error = %+v, want terminal internal-server error", streamErr)
	}
}

type handlerReleaseNotification struct {
	group    executionregistry.ReleaseGroup
	sequence int64
}

type handlerReleaseSink struct {
	mu            sync.Mutex
	notifications []handlerReleaseNotification
	notified      chan struct{}
}

func newHandlerReleaseSink() *handlerReleaseSink {
	return &handlerReleaseSink{notified: make(chan struct{}, 1)}
}

func (s *handlerReleaseSink) MarkDirty(group executionregistry.ReleaseGroup, sequence int64) {
	s.mu.Lock()
	s.notifications = append(s.notifications, handlerReleaseNotification{group: group, sequence: sequence})
	s.mu.Unlock()
	select {
	case s.notified <- struct{}{}:
	default:
	}
}

func (s *handlerReleaseSink) Notifications() []handlerReleaseNotification {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]handlerReleaseNotification(nil), s.notifications...)
}

type handlerAccountedHomeDispatcher struct {
	calls atomic.Int32
}

func (*handlerAccountedHomeDispatcher) HeartbeatOK() bool { return true }
func (d *handlerAccountedHomeDispatcher) RPopAuth(_ context.Context, model string, _ string, _ http.Header, _ int) ([]byte, error) {
	d.calls.Add(1)
	return json.Marshal(map[string]any{
		"concurrency": map[string]any{"accounted": true, "credential_id": "handler-cred", "model": model},
		"model":       model,
		"auth_index":  "handler-cred",
		"auth":        map[string]any{"id": "handler-cred", "provider": "bootstrap-test", "status": coreauth.StatusActive},
	})
}
func (*handlerAccountedHomeDispatcher) AbortAmbiguousDispatch() {}

func TestExecuteStreamWithAuthManager_HomeBootstrapFailureDoesNotRedispatch(t *testing.T) {
	executor := &bootstrapStreamExecutor{stream: func(_ context.Context, _ int) (*coreexecutor.StreamResult, error) {
		chunks := make(chan coreexecutor.StreamChunk, 2)
		chunks <- coreexecutor.StreamChunk{Payload: []byte("drop")}
		chunks <- coreexecutor.StreamChunk{Err: &coreauth.Error{HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"}}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{Home: internalconfig.HomeConfig{Enabled: true}})
	manager.RegisterExecutor(executor)
	registry := executionregistry.New()
	releaseSink := newHandlerReleaseSink()
	registry.SetReleaseSink(releaseSink.MarkDirty)
	dispatcher := &handlerAccountedHomeDispatcher{}
	manager.PublishHomeDispatch(dispatcher, registry, 1)
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{BootstrapRetries: 1}}, manager)
	handler.SetPluginHost(&handlerInterceptorTestHost{interceptStreamChunk: func(_ context.Context, req pluginapi.StreamChunkInterceptRequest) pluginapi.StreamChunkInterceptResponse {
		return pluginapi.StreamChunkInterceptResponse{Body: cloneBytes(req.Body), DropChunk: string(req.Body) == "drop"}
	}})

	dataChan, _, errChan := handler.ExecuteStreamWithAuthManager(context.Background(), "openai", "home-model", []byte(`{"model":"home-model"}`), "")
	for range dataChan {
		t.Fatal("Home bootstrap failure produced data")
	}
	var streamErr *interfaces.ErrorMessage
	for msg := range errChan {
		if msg != nil {
			streamErr = msg
		}
	}
	if streamErr == nil || streamErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("stream error = %+v, want unauthorized terminal error", streamErr)
	}
	if got := dispatcher.calls.Load(); got != 1 {
		t.Fatalf("Home RPOP calls = %d, want 1", got)
	}
	select {
	case <-releaseSink.notified:
	case <-time.After(time.Second):
		t.Fatal("accounted Home selection was not released")
	}
	wantRelease := handlerReleaseNotification{
		group:    executionregistry.ReleaseGroup{CredentialID: "handler-cred", Model: "home-model"},
		sequence: 1,
	}
	if got := releaseSink.Notifications(); len(got) != 1 || got[0] != wantRelease {
		t.Fatalf("release notifications = %#v, want [%#v]", got, wantRelease)
	}
	if errDrain := registry.Drain(context.Background()); errDrain != nil {
		t.Fatalf("registry.Drain(): %v", errDrain)
	}
	if got := releaseSink.Notifications(); len(got) != 1 || got[0] != wantRelease {
		t.Fatalf("release notifications after drain = %#v, want [%#v]", got, wantRelease)
	}
}

func TestExecuteStreamWithAuthManager_HeaderPassthroughDisabledByDefault(t *testing.T) {
	executor := &failOnceStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{
		ID:       "auth1",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test1@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}

	auth2 := &coreauth.Auth{
		ID:       "auth2",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test2@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth2); err != nil {
		t.Fatalf("manager.Register(auth2): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	registry.GetGlobalRegistry().RegisterClient(auth2.ID, auth2.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
		registry.GetGlobalRegistry().UnregisterClient(auth2.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Streaming: sdkconfig.StreamingConfig{
			BootstrapRetries: 1,
		},
	}, manager)
	dataChan, upstreamHeaders, errChan := handler.ExecuteStreamWithAuthManager(context.Background(), "openai", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}
	for msg := range errChan {
		if msg != nil {
			t.Fatalf("unexpected error: %+v", msg)
		}
	}

	if string(got) != "ok" {
		t.Fatalf("expected payload ok, got %q", string(got))
	}
	if got := upstreamHeaders.Get("X-Codex-Turn-State"); got != "state-2" {
		t.Fatalf("X-Codex-Turn-State = %q, want state-2", got)
	}
	if got := upstreamHeaders.Get("X-Upstream-Attempt"); got != "" {
		t.Fatalf("X-Upstream-Attempt = %q, want empty with passthrough disabled", got)
	}
}

func TestExecuteStreamWithAuthManager_DoesNotRetryAfterFirstByte(t *testing.T) {
	executor := &payloadThenErrorStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{
		ID:       "auth1",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test1@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}

	auth2 := &coreauth.Auth{
		ID:       "auth2",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test2@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth2); err != nil {
		t.Fatalf("manager.Register(auth2): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	registry.GetGlobalRegistry().RegisterClient(auth2.ID, auth2.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
		registry.GetGlobalRegistry().UnregisterClient(auth2.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Streaming: sdkconfig.StreamingConfig{
			BootstrapRetries: 1,
		},
	}, manager)
	dataChan, _, errChan := handler.ExecuteStreamWithAuthManager(context.Background(), "openai", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}

	var gotErr error
	var gotStatus int
	for msg := range errChan {
		if msg != nil && msg.Error != nil {
			gotErr = msg.Error
			gotStatus = msg.StatusCode
		}
	}

	if string(got) != "partial" {
		t.Fatalf("expected payload partial, got %q", string(got))
	}
	if gotErr == nil {
		t.Fatalf("expected terminal error, got nil")
	}
	if gotStatus != http.StatusBadGateway {
		t.Fatalf("expected status %d, got %d", http.StatusBadGateway, gotStatus)
	}
	if executor.Calls() != 1 {
		t.Fatalf("expected 1 stream attempt, got %d", executor.Calls())
	}

	var success, failed int64
	for _, authID := range []string{auth1.ID, auth2.ID} {
		updated, ok := manager.GetByID(authID)
		if !ok || updated == nil {
			t.Fatalf("manager.GetByID(%q) returned ok=%t auth=%v", authID, ok, updated)
		}
		success += updated.Success
		failed += updated.Failed
	}
	if success != 0 || failed != 1 {
		t.Fatalf("auth totals = success=%d failed=%d, want 0/1", success, failed)
	}
}

func TestAuthManagerStreamResultErrorMarksFailureAndPreservesRetryAfter(t *testing.T) {
	const internalError = "confused upstream identity"
	retryAfter := 15 * time.Minute
	executor := &payloadThenErrorStreamExecutor{
		resultErrOnly:  true,
		preludePayload: []byte("partial"),
		resultPayload:  []byte("failed"),
		resultErr: &streamResultError{
			message:    internalError,
			status:     http.StatusTooManyRequests,
			retryAfter: retryAfter,
		},
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth := &coreauth.Auth{ID: "auth-result-error", Provider: "codex", Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("manager.Register(): %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	startedAt := time.Now()
	result, err := manager.ExecuteStream(
		context.Background(),
		[]string{"codex"},
		coreexecutor.Request{Model: "test-model"},
		coreexecutor.Options{},
	)
	if err != nil {
		t.Fatalf("manager.ExecuteStream(): %v", err)
	}
	chunk, ok := <-result.Chunks
	if !ok {
		t.Fatal("stream closed before payload")
	}
	if string(chunk.Payload) != "partial" || chunk.Err != nil || chunk.ResultErr != nil {
		t.Fatalf("forwarded chunk = payload %q err=%v result_err=%v", chunk.Payload, chunk.Err, chunk.ResultErr)
	}
	chunk, ok = <-result.Chunks
	if !ok {
		t.Fatal("stream closed before result error payload")
	}
	if string(chunk.Payload) != "failed" || chunk.Err != nil || chunk.ResultErr != nil {
		t.Fatalf("result error chunk = payload %q err=%v result_err=%v", chunk.Payload, chunk.Err, chunk.ResultErr)
	}

	updated, ok := manager.GetByID(auth.ID)
	if !ok || updated == nil {
		t.Fatalf("manager.GetByID(%q) returned ok=%t auth=%v", auth.ID, ok, updated)
	}
	if updated.Success != 0 || updated.Failed != 1 {
		t.Fatalf("auth totals = success=%d failed=%d, want 0/1", updated.Success, updated.Failed)
	}
	state := updated.ModelStates["test-model"]
	if state == nil {
		t.Fatal("test-model state is nil")
	}
	if state.StatusMessage != internalError {
		t.Fatalf("model status message = %q, want %q", state.StatusMessage, internalError)
	}
	finishedAt := time.Now()
	if state.NextRetryAfter.Before(startedAt.Add(retryAfter)) || state.NextRetryAfter.After(finishedAt.Add(retryAfter)) {
		t.Fatalf("model retry time = %v, want provider delay %v", state.NextRetryAfter, retryAfter)
	}
	if _, ok = <-result.Chunks; ok {
		t.Fatal("unexpected chunk after payload")
	}
}

func TestAuthManagerBootstrapResultErrorSeparatesClientAndAccounting(t *testing.T) {
	const clientPayload = `{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"limited original client identity"}}`
	const internalError = `{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"limited confused upstream identity"}}`
	retryAfter := 15 * time.Minute
	executor := &payloadThenErrorStreamExecutor{
		resultErrOnly: true,
		resultPayload: []byte(clientPayload),
		resultErr: &streamResultError{
			message:    internalError,
			status:     http.StatusTooManyRequests,
			retryAfter: retryAfter,
		},
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	manager.SetRetryConfig(0, 0, 1)

	auth := &coreauth.Auth{ID: "auth-bootstrap-result-error", Provider: "codex", Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("manager.Register(): %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	startedAt := time.Now()
	result, err := manager.ExecuteStream(
		context.Background(),
		[]string{"codex"},
		coreexecutor.Request{Model: "test-model"},
		coreexecutor.Options{},
	)
	if err != nil {
		t.Fatalf("manager.ExecuteStream(): %v", err)
	}

	chunk, ok := <-result.Chunks
	if !ok {
		t.Fatal("stream closed before client error payload")
	}
	if string(chunk.Payload) != clientPayload || chunk.Err != nil || chunk.ResultErr != nil {
		t.Fatalf("client chunk = payload %q err=%v result_err=%v", chunk.Payload, chunk.Err, chunk.ResultErr)
	}
	if _, ok = <-result.Chunks; ok {
		t.Fatal("unexpected chunk after client error payload")
	}

	updated, ok := manager.GetByID(auth.ID)
	if !ok || updated == nil {
		t.Fatalf("manager.GetByID(%q) returned ok=%t auth=%v", auth.ID, ok, updated)
	}
	state := updated.ModelStates["test-model"]
	if state == nil || state.StatusMessage != internalError {
		t.Fatalf("model status = %#v, want internal error %q", state, internalError)
	}
	finishedAt := time.Now()
	if state.NextRetryAfter.Before(startedAt.Add(retryAfter)) || state.NextRetryAfter.After(finishedAt.Add(retryAfter)) {
		t.Fatalf("model retry time = %v, want provider delay %v", state.NextRetryAfter, retryAfter)
	}
	if !state.Quota.NextRecoverAt.Equal(state.NextRetryAfter) {
		t.Fatalf("quota recovery time = %v, want %v", state.Quota.NextRecoverAt, state.NextRetryAfter)
	}
}

func TestExecuteStreamWithAuthManager_RetriesBootstrapResultErrorWithCoolingDisabled(t *testing.T) {
	const clientErrorPayload = `{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"limited original client identity"}}`
	const successPayload = `{"type":"response.completed","response":{"id":"resp-1"}}`
	executor := &payloadThenErrorStreamExecutor{
		resultErrOnly:         true,
		resultPayload:         []byte(clientErrorPayload),
		succeedAfterResultErr: true,
		successPayload:        []byte(successPayload),
		resultErr: &streamResultError{
			message:    `{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"limited confused upstream identity"}}`,
			status:     http.StatusTooManyRequests,
			retryAfter: time.Millisecond,
		},
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	manager.SetRetryConfig(1, 100*time.Millisecond, 0)

	auth := &coreauth.Auth{
		ID:       "auth-bootstrap-retry-after",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"disable_cooling": true},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("manager.Register(): %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	result, err := manager.ExecuteStream(
		context.Background(),
		[]string{"codex"},
		coreexecutor.Request{Model: "test-model"},
		coreexecutor.Options{},
	)
	if err != nil {
		t.Fatalf("manager.ExecuteStream(): %v", err)
	}
	if executor.Calls() != 2 {
		t.Fatalf("stream calls = %d, want 2", executor.Calls())
	}

	chunk, ok := <-result.Chunks
	if !ok {
		t.Fatal("stream closed before response payload")
	}
	if string(chunk.Payload) != successPayload || chunk.Err != nil || chunk.ResultErr != nil {
		t.Fatalf("client chunk = payload %q err=%v result_err=%v", chunk.Payload, chunk.Err, chunk.ResultErr)
	}
	if _, ok = <-result.Chunks; ok {
		t.Fatal("unexpected chunk after response payload")
	}
}

func TestExecuteStreamWithAuthManager_EnrichesBootstrapRetryAuthUnavailableError(t *testing.T) {
	executor := &failOnceStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{
		ID:       "auth1",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test1@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Streaming: sdkconfig.StreamingConfig{
			BootstrapRetries: 1,
		},
	}, manager)
	dataChan, _, errChan := handler.ExecuteStreamWithAuthManager(context.Background(), "openai", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty payload, got %q", string(got))
	}

	var gotErr *interfaces.ErrorMessage
	for msg := range errChan {
		if msg != nil {
			gotErr = msg
		}
	}
	if gotErr == nil {
		t.Fatalf("expected terminal error")
	}
	if gotErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", gotErr.StatusCode, http.StatusServiceUnavailable)
	}

	var authErr *coreauth.Error
	if !errors.As(gotErr.Error, &authErr) || authErr == nil {
		t.Fatalf("expected coreauth.Error, got %T", gotErr.Error)
	}
	if authErr.Code != "auth_unavailable" {
		t.Fatalf("code = %q, want %q", authErr.Code, "auth_unavailable")
	}
	if !strings.Contains(authErr.Message, "providers=codex") {
		t.Fatalf("message missing provider context: %q", authErr.Message)
	}
	if !strings.Contains(authErr.Message, "model=test-model") {
		t.Fatalf("message missing model context: %q", authErr.Message)
	}

	if executor.Calls() != 1 {
		t.Fatalf("expected exactly one upstream call before retry path selection failure, got %d", executor.Calls())
	}
}

func TestExecuteStreamWithAuthManager_PinnedAuthKeepsSameUpstream(t *testing.T) {
	executor := &authAwareStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{
		ID:       "auth1",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test1@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}

	auth2 := &coreauth.Auth{
		ID:       "auth2",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test2@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth2); err != nil {
		t.Fatalf("manager.Register(auth2): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	registry.GetGlobalRegistry().RegisterClient(auth2.ID, auth2.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
		registry.GetGlobalRegistry().UnregisterClient(auth2.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Streaming: sdkconfig.StreamingConfig{
			BootstrapRetries: 1,
		},
	}, manager)
	ctx := WithPinnedAuthID(context.Background(), "auth1")
	dataChan, _, errChan := handler.ExecuteStreamWithAuthManager(ctx, "openai", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}

	var gotErr error
	for msg := range errChan {
		if msg != nil && msg.Error != nil {
			gotErr = msg.Error
		}
	}

	if len(got) != 0 {
		t.Fatalf("expected empty payload, got %q", string(got))
	}
	if gotErr == nil {
		t.Fatalf("expected terminal error, got nil")
	}
	authIDs := executor.AuthIDs()
	if len(authIDs) == 0 {
		t.Fatalf("expected at least one upstream attempt")
	}
	for _, authID := range authIDs {
		if authID != "auth1" {
			t.Fatalf("expected all attempts on auth1, got sequence %v", authIDs)
		}
	}
}

func TestExecuteStreamWithAuthManager_SelectedAuthCallbackReceivesAuthID(t *testing.T) {
	executor := &authAwareStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth2 := &coreauth.Auth{
		ID:       "auth2",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test2@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth2); err != nil {
		t.Fatalf("manager.Register(auth2): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth2.ID, auth2.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth2.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Streaming: sdkconfig.StreamingConfig{
			BootstrapRetries: 0,
		},
	}, manager)

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	logging.SetGinRequestID(ginCtx, "1234abcd")

	selectedAuthID := ""
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	ctx = WithSelectedAuthIDCallback(ctx, func(authID string) {
		selectedAuthID = authID
	})
	dataChan, _, errChan := handler.ExecuteStreamWithAuthManager(ctx, "openai", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}
	for msg := range errChan {
		if msg != nil {
			t.Fatalf("unexpected error: %+v", msg)
		}
	}

	if string(got) != "ok" {
		t.Fatalf("expected payload ok, got %q", string(got))
	}
	if selectedAuthID != "auth2" {
		t.Fatalf("selectedAuthID = %q, want %q", selectedAuthID, "auth2")
	}
	traceID := logging.GetGinCPATraceID(ginCtx)
	parts := strings.Split(traceID, "-")
	if len(parts) != 3 || parts[1] != auth2.Index || parts[2] != "1234abcd" {
		t.Fatalf("trace ID = %q, want timestamp-%s-1234abcd", traceID, auth2.Index)
	}
	if _, errParse := time.Parse("20060102150405", parts[0]); errParse != nil {
		t.Fatalf("trace timestamp = %q: %v", parts[0], errParse)
	}
}

func TestExecuteStreamWithAuthManager_ValidatesOpenAIResponsesStreamDataJSON(t *testing.T) {
	executor := &invalidJSONStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{
		ID:       "auth1",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test1@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	dataChan, _, errChan := handler.ExecuteStreamWithAuthManager(context.Background(), "openai-response", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []byte
	for chunk := range dataChan {
		got = append(got, chunk...)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty payload, got %q", string(got))
	}

	gotErr := false
	for msg := range errChan {
		if msg == nil {
			continue
		}
		if msg.StatusCode != http.StatusBadGateway {
			t.Fatalf("expected status %d, got %d", http.StatusBadGateway, msg.StatusCode)
		}
		if msg.Error == nil {
			t.Fatalf("expected error")
		}
		gotErr = true
	}
	if !gotErr {
		t.Fatalf("expected terminal error")
	}
}

func TestExecuteStreamWithAuthManager_AllowsSplitOpenAIResponsesSSEEventLines(t *testing.T) {
	executor := &splitResponsesEventStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)

	auth1 := &coreauth.Auth{
		ID:       "auth1",
		Provider: "split-sse",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"email": "test1@example.com"},
	}
	if _, err := manager.Register(context.Background(), auth1); err != nil {
		t.Fatalf("manager.Register(auth1): %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(auth1.ID, auth1.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth1.ID)
	})

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	dataChan, _, errChan := handler.ExecuteStreamWithAuthManager(context.Background(), "openai-response", "test-model", []byte(`{"model":"test-model"}`), "")
	if dataChan == nil || errChan == nil {
		t.Fatalf("expected non-nil channels")
	}

	var got []string
	for chunk := range dataChan {
		got = append(got, string(chunk))
	}

	for msg := range errChan {
		if msg != nil {
			t.Fatalf("unexpected error: %+v", msg)
		}
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 forwarded chunks, got %d: %#v", len(got), got)
	}
	if got[0] != "event: response.completed" {
		t.Fatalf("unexpected first chunk: %q", got[0])
	}
	expectedData := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[]}}"
	if got[1] != expectedData {
		t.Fatalf("unexpected second chunk.\nGot:  %q\nWant: %q", got[1], expectedData)
	}
}
