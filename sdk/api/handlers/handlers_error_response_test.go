package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

func TestWriteErrorResponse_AddonHeadersDisabledByDefault(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)

	handler := NewBaseAPIHandlers(nil, nil)
	handler.WriteErrorResponse(c, &interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      errors.New("rate limit"),
		Addon: http.Header{
			"Retry-After":  {"30"},
			"X-Request-Id": {"req-1"},
		},
	})

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTooManyRequests)
	}
	if got := recorder.Header().Get("Retry-After"); got != "" {
		t.Fatalf("Retry-After should be empty when passthrough is disabled, got %q", got)
	}
	if got := recorder.Header().Get("X-Request-Id"); got != "" {
		t.Fatalf("X-Request-Id should be empty when passthrough is disabled, got %q", got)
	}
}

func TestWriteErrorResponsePreservesCapturedUpstreamResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	upstreamResponse := []byte(`{"error":{"message":"confused upstream identity"}}`)
	c.Set("API_RESPONSE", upstreamResponse)

	handler := NewBaseAPIHandlers(nil, nil)
	handler.WriteErrorResponse(c, &interfaces.ErrorMessage{
		StatusCode: http.StatusBadRequest,
		Error:      errors.New("original client identity"),
	})

	got, exists := c.Get("API_RESPONSE")
	if !exists {
		t.Fatal("API_RESPONSE was removed")
	}
	gotResponse, ok := got.([]byte)
	if !ok || string(gotResponse) != string(upstreamResponse) {
		t.Fatalf("API_RESPONSE = %q, want %q", gotResponse, upstreamResponse)
	}
	if strings.Contains(string(gotResponse), "original client identity") {
		t.Fatalf("API_RESPONSE mixed in downstream error: %s", gotResponse)
	}
	if !strings.Contains(recorder.Body.String(), "original client identity") {
		t.Fatalf("client response = %s, want original client identity", recorder.Body.String())
	}
}

func TestWriteErrorResponsePreservesFileBackedUpstreamResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses/compact", nil)
	c.Set(logging.APIResponseCapturedContextKey, true)

	handler := NewBaseAPIHandlers(nil, nil)
	handler.WriteErrorResponse(c, &interfaces.ErrorMessage{
		StatusCode: http.StatusBadRequest,
		Error:      errors.New("original client identity"),
	})

	if got, exists := c.Get("API_RESPONSE"); exists {
		t.Fatalf("API_RESPONSE = %q, want file-backed upstream response only", got)
	}
	if !strings.Contains(recorder.Body.String(), "original client identity") {
		t.Fatalf("client response = %s, want original client identity", recorder.Body.String())
	}
}

func TestLoggingAPIResponseErrorUsesInternalErrorView(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, nil)
	errInternal := modelExecutionClientError{
		statusCode: http.StatusBadRequest,
		internal:   "rejected confused upstream identity",
		client:     "rejected original client identity",
	}
	errMsg := executionErrorMessage(errInternal)

	handler.LoggingAPIResponseError(context.WithValue(context.Background(), "gin", c), errMsg)

	value, exists := c.Get("API_RESPONSE_ERROR")
	if !exists {
		t.Fatal("API_RESPONSE_ERROR was not recorded")
	}
	logged, ok := value.([]*interfaces.ErrorMessage)
	if !ok || len(logged) != 1 || logged[0] == nil || logged[0].Error == nil {
		t.Fatalf("API_RESPONSE_ERROR = %#v", value)
	}
	if got := logged[0].Error.Error(); got != errInternal.internal {
		t.Fatalf("logged error = %q, want %q", got, errInternal.internal)
	}
	if got := errMsg.Error.Error(); got != errInternal.client {
		t.Fatalf("client error = %q, want %q", got, errInternal.client)
	}
}

func TestWriteErrorResponseDirectResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Writer.Header().Set("X-Cpa-Trace-Id", "local-trace")
	c.Writer.Header().Set("Access-Control-Allow-Origin", "https://trusted.example")

	handler := NewBaseAPIHandlers(nil, nil)
	handler.WriteErrorResponse(c, &interfaces.ErrorMessage{
		StatusCode:     http.StatusForbidden,
		DirectResponse: true,
		Body:           []byte(`{"error":"blocked"}`),
		Headers: http.Header{
			"Content-Type":                {"application/problem+json"},
			"X-Plugin-Policy":             {"blocked"},
			"X-Cpa-Trace-Id":              {"plugin-trace"},
			"Access-Control-Allow-Origin": {"https://untrusted.example"},
		},
	})

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
	if got := recorder.Body.String(); got != `{"error":"blocked"}` {
		t.Fatalf("body = %q", got)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/problem+json" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := recorder.Header().Get("X-Plugin-Policy"); got != "blocked" {
		t.Fatalf("X-Plugin-Policy = %q", got)
	}
	if got := recorder.Header().Get("X-Cpa-Trace-Id"); got != "local-trace" {
		t.Fatalf("X-Cpa-Trace-Id = %q, want local value", got)
	}
	if got := recorder.Header().Get("Access-Control-Allow-Origin"); got != "https://trusted.example" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want trusted origin", got)
	}
}

func TestInternalConcurrencyBusyWritesRetryAfterWithoutPassthrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)

	handler := NewBaseAPIHandlers(nil, nil)
	handler.WriteErrorResponse(c, &interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      coreauth.NewHomeConcurrencyBusyError("busy", 750*time.Millisecond),
	})

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTooManyRequests)
	}
	if got := recorder.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
}

func TestWriteErrorResponseHomeBusyNormalAndStreamHeaders(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "stream"}[stream], func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			if stream {
				c.Request.Header.Set("Accept", "text/event-stream")
			}

			handler := NewBaseAPIHandlers(nil, nil)
			handler.WriteErrorResponse(c, &interfaces.ErrorMessage{
				StatusCode: http.StatusTooManyRequests,
				Error:      coreauth.NewHomeConcurrencyBusyError("busy", 750*time.Millisecond),
			})
			if recorder.Code != http.StatusTooManyRequests {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTooManyRequests)
			}
			if got := recorder.Header().Get("Retry-After"); got != "1" {
				t.Fatalf("Retry-After = %q, want 1", got)
			}
		})
	}
}

func TestWriteErrorResponse_AddonHeadersEnabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	c.Writer.Header().Set("X-Request-Id", "old-value")
	c.Writer.Header().Set("x-cpa-trace-id", "local-trace")
	c.Writer.Header().Set("Access-Control-Expose-Headers", "x-cpa-trace-id")

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{PassthroughHeaders: true}, nil)
	handler.WriteErrorResponse(c, &interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      errors.New("rate limit"),
		Addon: http.Header{
			"Retry-After":                   {"30"},
			"X-Request-Id":                  {"new-1", "new-2"},
			"x-cpa-trace-id":                {"upstream-trace"},
			"Access-Control-Expose-Headers": {"upstream-header"},
		},
	})

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTooManyRequests)
	}
	if got := recorder.Header().Get("Retry-After"); got != "30" {
		t.Fatalf("Retry-After = %q, want %q", got, "30")
	}
	if got := recorder.Header().Values("X-Request-Id"); !reflect.DeepEqual(got, []string{"new-1", "new-2"}) {
		t.Fatalf("X-Request-Id = %#v, want %#v", got, []string{"new-1", "new-2"})
	}
	if got := recorder.Header().Get("x-cpa-trace-id"); got != "local-trace" {
		t.Fatalf("x-cpa-trace-id = %q, want local trace", got)
	}
	if got := recorder.Header().Get("Access-Control-Expose-Headers"); got != "x-cpa-trace-id" {
		t.Fatalf("Access-Control-Expose-Headers = %q, want CPA value", got)
	}
}

func TestEnrichAuthSelectionError_DefaultsTo503WithContext(t *testing.T) {
	in := &coreauth.Error{Code: "auth_not_found", Message: "no auth available"}
	out := enrichAuthSelectionError(in, []string{"claude"}, "claude-sonnet-4-6")

	var got *coreauth.Error
	if !errors.As(out, &got) || got == nil {
		t.Fatalf("expected coreauth.Error, got %T", out)
	}
	if got.StatusCode() != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", got.StatusCode(), http.StatusServiceUnavailable)
	}
	if !strings.Contains(got.Message, "providers=claude") {
		t.Fatalf("message missing provider context: %q", got.Message)
	}
	if !strings.Contains(got.Message, "model=claude-sonnet-4-6") {
		t.Fatalf("message missing model context: %q", got.Message)
	}
	if !strings.Contains(got.Message, "/v0/management/auth-files") {
		t.Fatalf("message missing management hint: %q", got.Message)
	}
}

func TestEnrichAuthSelectionError_PreservesExplicitStatus(t *testing.T) {
	in := &coreauth.Error{Code: "auth_unavailable", Message: "no auth available", HTTPStatus: http.StatusTooManyRequests}
	out := enrichAuthSelectionError(in, []string{"gemini"}, "gemini-2.5-pro")

	var got *coreauth.Error
	if !errors.As(out, &got) || got == nil {
		t.Fatalf("expected coreauth.Error, got %T", out)
	}
	if got.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", got.StatusCode(), http.StatusTooManyRequests)
	}
}

func TestEnrichAuthSelectionError_IgnoresOtherErrors(t *testing.T) {
	in := errors.New("boom")
	out := enrichAuthSelectionError(in, []string{"claude"}, "claude-sonnet-4-6")
	if out != in {
		t.Fatalf("expected original error to be returned unchanged")
	}
}
