package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	requestlogging "github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	codexexecutor "github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executionregistry"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/tidwall/gjson"
)

type homeResponsesWebsocketDispatcher struct {
	calls atomic.Int32
}

func (*homeResponsesWebsocketDispatcher) HeartbeatOK() bool { return true }

func (d *homeResponsesWebsocketDispatcher) RPopAuth(context.Context, string, string, http.Header, int) ([]byte, error) {
	d.calls.Add(1)
	return json.Marshal(coreauth.Auth{
		ID:       "home-responses-websocket-auth",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"websockets": "true",
		},
	})
}

func (*homeResponsesWebsocketDispatcher) AbortAmbiguousDispatch() {}

type homeResponsesWebsocketExecutor struct {
	calls    atomic.Int32
	metadata []map[string]any
	mu       sync.Mutex
}

func (*homeResponsesWebsocketExecutor) Identifier() string { return "codex" }

func (*homeResponsesWebsocketExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *homeResponsesWebsocketExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, _ coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.calls.Add(1)
	e.mu.Lock()
	e.metadata = append(e.metadata, maps.Clone(opts.Metadata))
	e.mu.Unlock()
	if lifecycle, ok := opts.ExecutionLifecycle.(interface{ Retain() }); ok {
		lifecycle.Retain()
	}
	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.completed","response":{"id":"home-response","output":[]}}`)}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (*homeResponsesWebsocketExecutor) Refresh(context.Context, *coreauth.Auth) (*coreauth.Auth, error) {
	return nil, errors.New("not implemented")
}

func (*homeResponsesWebsocketExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (*homeResponsesWebsocketExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func TestResponsesWebsocketHomeSelectedAuthCallbackPinsAndReusesFirstSelection(t *testing.T) {
	gin.SetMode(gin.TestMode)

	dispatcher := &homeResponsesWebsocketDispatcher{}
	executor := &homeResponsesWebsocketExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{Home: internalconfig.HomeConfig{Enabled: true}})
	manager.PublishHomeDispatch(dispatcher, executionregistry.New(), 1)
	manager.RegisterExecutor(executor)
	registry.GetGlobalRegistry().RegisterClient("home-responses-websocket-auth", "codex", []*registry.ModelInfo{{ID: "gpt-5.4"}})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
	if errDial != nil {
		t.Fatalf("dial websocket: %v", errDial)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Errorf("close websocket: %v", errClose)
		}
	}()

	requests := []string{
		`{"type":"response.create","model":"gpt-5.4","input":[]}`,
		`{"type":"response.create","model":"gpt-5.4","input":[]}`,
	}
	for index, request := range requests {
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(request)); errWrite != nil {
			t.Fatalf("write websocket request %d: %v", index+1, errWrite)
		}
		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Fatalf("read websocket response %d: %v", index+1, errRead)
		}
		if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
			t.Fatalf("response %d type = %q, want %q: %s", index+1, got, wsEventTypeCompleted, payload)
		}
		if index == 0 {
			executor.mu.Lock()
			firstMetadata := maps.Clone(executor.metadata[0])
			executor.mu.Unlock()
			sessionID, _ := firstMetadata[coreexecutor.ExecutionSessionMetadataKey].(string)
			if _, ok := manager.GetExecutionSessionAuthByID(sessionID, "home-responses-websocket-auth"); !ok {
				t.Fatal("first selected-auth callback did not stage the session runtime auth")
			}
		}
	}

	executor.mu.Lock()
	metadata := append([]map[string]any(nil), executor.metadata...)
	executor.mu.Unlock()
	if len(metadata) != 2 {
		t.Fatalf("executor metadata calls = %d, want 2", len(metadata))
	}
	if got := metadata[1][coreexecutor.PinnedAuthMetadataKey]; got != "home-responses-websocket-auth" {
		t.Fatalf("second turn pinned auth metadata = %#v, want home selected auth (first metadata: %#v, second metadata: %#v)", got, metadata[0], metadata[1])
	}
	if got := dispatcher.calls.Load(); got != 1 {
		t.Fatalf("Home RPOP calls = %d, want 1 after selected-auth callback pin", got)
	}
	if got := executor.calls.Load(); got != 2 {
		t.Fatalf("executor calls = %d, want 2", got)
	}
}

func TestWebsocketReplayCloseRequiresTypedSignal(t *testing.T) {
	matched, payload := websocketClosePayloadForUpstreamError(responsesWebsocketHTTPReplayRequiredError())
	if !matched || len(payload) == 0 {
		t.Fatalf("typed replay signal matched=%t payload_len=%d, want close payload", matched, len(payload))
	}
	spoofed := websocketPinnedFailoverStatusError{
		status: http.StatusUpgradeRequired,
		msg:    `{"error":{"code":"upstream_http_replay_required"}}`,
	}
	if matched, _ := websocketClosePayloadForUpstreamError(spoofed); matched {
		t.Fatal("untyped upstream error spoofed replay close")
	}
}

func TestResponsesWebsocketRequestRequiresCurrentUpstream(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    bool
	}{
		{name: "incremental create", payload: `{"type":"response.create","previous_response_id":"resp-1","input":[]}`, want: true},
		{name: "append", payload: `{"type":"response.append","input":[]}`, want: true},
		{name: "full create", payload: `{"type":"response.create","input":[]}`, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := responsesWebsocketRequestRequiresCurrentUpstream([]byte(tc.payload)); got != tc.want {
				t.Fatalf("responsesWebsocketRequestRequiresCurrentUpstream() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestResponsesWebsocketNativePassthroughRequiresImmediatelyPreviousAuth(t *testing.T) {
	if !responsesWebsocketNativePassthroughAllowed(responsesWebsocketUpstreamModeWS, true, "auth-a", "auth-a") {
		t.Fatal("matching immediate websocket auth did not allow native passthrough")
	}
	if responsesWebsocketNativePassthroughAllowed(responsesWebsocketUpstreamModeWS, true, "auth-a", "auth-b") {
		t.Fatal("restored auth from an older provider session allowed native passthrough")
	}
	if responsesWebsocketNativePassthroughAllowed(responsesWebsocketUpstreamModeHTTP, true, "auth-a", "auth-a") {
		t.Fatal("HTTP mode allowed native websocket passthrough")
	}
}

func TestWriteWebsocketCloseForUpstreamErrorMirrorsMessageTooBig(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		reason string
	}{
		{
			name: "raw close error",
			err: &websocket.CloseError{
				Code: websocket.CloseMessageTooBig,
				Text: "message too big",
			},
			reason: "message too big",
		},
		{
			name: "mapped stream error",
			err: websocketPinnedFailoverStatusError{
				status: http.StatusRequestEntityTooLarge,
				msg:    `{"error":{"message":"upstream websocket message too big","code":"message_too_big"}}`,
			},
			reason: "upstream websocket message too big",
		},
		{
			name: "multibyte reason stays valid",
			err: &websocket.CloseError{
				Code: websocket.CloseMessageTooBig,
				Text: strings.Repeat("🙂", 31),
			},
			reason: strings.Repeat("🙂", 30),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverErr := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
				if err != nil {
					serverErr <- err
					return
				}
				matched, errWrite := writeWebsocketCloseForUpstreamError(conn, tt.err)
				if !matched && errWrite == nil {
					errWrite = errors.New("message-too-big error did not match")
				}
				if errClose := conn.Close(); errWrite == nil {
					errWrite = errClose
				}
				serverErr <- errWrite
			}))
			defer server.Close()

			wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if err != nil {
				t.Fatalf("dial websocket: %v", err)
			}
			defer func() { _ = conn.Close() }()

			if err = conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatalf("set read deadline: %v", err)
			}
			_, _, err = conn.ReadMessage()
			var closeErr *websocket.CloseError
			if !errors.As(err, &closeErr) {
				t.Fatalf("expected websocket close error, got %v", err)
			}
			if closeErr.Code != websocket.CloseMessageTooBig {
				t.Fatalf("expected close code 1009, got %d", closeErr.Code)
			}
			if closeErr.Text != tt.reason {
				t.Fatalf("expected close reason %q, got %q", tt.reason, closeErr.Text)
			}
			if err = <-serverErr; err != nil {
				t.Fatalf("close server websocket: %v", err)
			}
		})
	}
}

func TestResponsesWebsocketWriterCloseDoesNotWaitForActiveDataWriter(t *testing.T) {
	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		writer := newResponsesWebsocketWriter(conn)

		// Holding writeMu models a data writer blocked inside WriteMessage. The
		// upstream-close path must hard-close the socket instead of waiting for it.
		writer.writeMu.Lock()
		closeDone := make(chan error, 1)
		go func() {
			matched, errClose := writer.closeForUpstreamError(&websocket.CloseError{
				Code: websocket.CloseMessageTooBig,
				Text: "message too big",
			})
			if !matched && errClose == nil {
				errClose = errors.New("message-too-big error did not match")
			}
			closeDone <- errClose
		}()

		select {
		case errClose := <-closeDone:
			writer.writeMu.Unlock()
			serverErrCh <- errClose
		case <-time.After(time.Second):
			writer.writeMu.Unlock()
			serverErrCh <- errors.New("close waited behind active data writer")
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err = conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, _, err = conn.ReadMessage(); err == nil {
		t.Fatal("client read succeeded, want connection closure")
	}
	if errServer := <-serverErrCh; errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
}

func TestTruncateWebsocketCloseReason(t *testing.T) {
	tests := []struct {
		name     string
		reason   string
		maxBytes int
		want     string
	}{
		{
			name:     "non-positive limit",
			reason:   "message too big",
			maxBytes: 0,
			want:     "",
		},
		{
			name:     "short valid reason unchanged",
			reason:   "message too big",
			maxBytes: wsCloseReasonMaxBytes,
			want:     "message too big",
		},
		{
			name:     "long ascii reason",
			reason:   strings.Repeat("x", 1<<20),
			maxBytes: wsCloseReasonMaxBytes,
			want:     strings.Repeat("x", wsCloseReasonMaxBytes),
		},
		{
			name:     "long invalid reason",
			reason:   strings.Repeat("\xff", 1<<20),
			maxBytes: wsCloseReasonMaxBytes,
			want:     strings.Repeat("�", wsCloseReasonMaxBytes/utf8.RuneLen(utf8.RuneError)),
		},
		{
			name:     "multibyte rune does not fit",
			reason:   "ab🙂cd",
			maxBytes: 5,
			want:     "ab",
		},
		{
			name:     "invalid bytes become replacement runes",
			reason:   string([]byte{'a', 0xff, 0xfe, 'b'}),
			maxBytes: 8,
			want:     "a��b",
		},
		{
			name:     "invalid replacement does not cross limit",
			reason:   string([]byte{'a', 'b', 0xff, 'c'}),
			maxBytes: 4,
			want:     "ab",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateWebsocketCloseReason(tt.reason, tt.maxBytes)
			if got != tt.want {
				t.Fatalf("truncateWebsocketCloseReason() = %q, want %q", got, tt.want)
			}
			if !utf8.ValidString(got) {
				t.Fatalf("truncateWebsocketCloseReason() returned invalid UTF-8: %q", got)
			}
			if tt.maxBytes > 0 && len(got) > tt.maxBytes {
				t.Fatalf("truncateWebsocketCloseReason() returned %d bytes, limit %d", len(got), tt.maxBytes)
			}
		})
	}
}

func TestForwardResponsesWebsocketMirrorsMappedMessageTooBig(t *testing.T) {
	gin.SetMode(gin.TestMode)

	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.Close() }()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r
		data := make(chan []byte)
		errCh := make(chan *interfaces.ErrorMessage, 1)
		errCh <- &interfaces.ErrorMessage{
			StatusCode: http.StatusRequestEntityTooLarge,
			Error: websocketPinnedFailoverStatusError{
				status: http.StatusRequestEntityTooLarge,
				msg:    `{"error":{"message":"upstream websocket message too big","code":"message_too_big"}}`,
			},
		}

		h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
		_, _, _, errMsg, _, errForward := h.forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			newInMemoryWebsocketTimelineLog(),
			"session-1",
		)
		if errMsg == nil || errMsg.StatusCode != http.StatusRequestEntityTooLarge {
			serverErrCh <- fmt.Errorf("forward error message = %#v, want status %d", errMsg, http.StatusRequestEntityTooLarge)
			return
		}
		if !errors.Is(errForward, websocket.ErrCloseSent) {
			serverErrCh <- fmt.Errorf("forward error = %v, want %v", errForward, websocket.ErrCloseSent)
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err = conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, _, err = conn.ReadMessage()
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) {
		t.Fatalf("expected websocket close error, got %v", err)
	}
	if closeErr.Code != websocket.CloseMessageTooBig {
		t.Fatalf("close code = %d, want %d", closeErr.Code, websocket.CloseMessageTooBig)
	}
	if err = <-serverErrCh; err != nil {
		t.Fatalf("server error: %v", err)
	}
}

type websocketCaptureExecutor struct {
	streamCalls int
	payloads    [][]byte
}

type websocketProviderCaptureExecutor struct {
	provider string
	websocketCaptureExecutor
}

type websocketProviderRouteHost struct{}

func (*websocketProviderRouteHost) HasModelRouters() bool { return true }

func (*websocketProviderRouteHost) RouteModel(_ context.Context, req pluginapi.ModelRouteRequest) (pluginapi.ModelRouteResponse, bool) {
	if !gjson.GetBytes(req.Body, "route_to_claude").Bool() {
		return pluginapi.ModelRouteResponse{}, false
	}
	return pluginapi.ModelRouteResponse{
		Handled:     true,
		TargetKind:  pluginapi.ModelRouteTargetProvider,
		Target:      "claude",
		TargetModel: "claude-provider-route-target",
	}, true
}

func readResponsesWebsocketTestMessage(t *testing.T, conn *websocket.Conn) (int, []byte, error) {
	t.Helper()
	if conn == nil {
		t.Fatal("websocket connection is nil")
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set websocket read deadline: %v", err)
	}
	msgType, payload, err := conn.ReadMessage()
	_ = conn.SetReadDeadline(time.Time{})
	return msgType, payload, err
}

func receiveResponsesWebsocketTestError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server result")
		return nil
	}
}

func receiveResponsesWebsocketTestBytes(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()
	select {
	case payload := <-ch:
		return payload
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for websocket output")
		return nil
	}
}

type websocketCompactionCaptureExecutor struct {
	provider        string
	mu              sync.Mutex
	streamPayloads  [][]byte
	compactPayload  []byte
	dropCalls       int
	dropReasons     []string
	processedIDs    []string
	processedCh     chan string
	failStreamCalls map[int]error
}

type orderedWebsocketSelector struct {
	mu     sync.Mutex
	order  []string
	cursor int
}

func (s *orderedWebsocketSelector) Pick(_ context.Context, _ string, _ string, _ coreexecutor.Options, auths []*coreauth.Auth) (*coreauth.Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(auths) == 0 {
		return nil, errors.New("no auth available")
	}
	for len(s.order) > 0 && s.cursor < len(s.order) {
		authID := strings.TrimSpace(s.order[s.cursor])
		s.cursor++
		for _, auth := range auths {
			if auth != nil && auth.ID == authID {
				return auth, nil
			}
		}
	}
	for _, auth := range auths {
		if auth != nil {
			return auth, nil
		}
	}
	return nil, errors.New("no auth available")
}

type websocketAuthCaptureExecutor struct {
	mu      sync.Mutex
	authIDs []string
}

type websocketPinnedFailoverExecutor struct {
	mu         sync.Mutex
	failStatus int
	authIDs    []string
	calls      map[string]int
	payloads   map[string][][]byte
}

type websocketBootstrapFallbackExecutor struct {
	mu       sync.Mutex
	authIDs  []string
	payloads map[string][][]byte
}

type websocketDirectCaptureExecutor struct {
	mu                        sync.Mutex
	provider                  string
	failStatus                int
	authIDs                   []string
	models                    []string
	payloads                  [][]byte
	requiredUpstreamWebsocket []bool
	done                      chan struct{}
	doneOnce                  sync.Once
}

type websocketCanonicalRollbackExecutor struct {
	mu       sync.Mutex
	payloads [][]byte
	calls    int
}

type websocketGenerationReplayExecutor struct {
	mu                             sync.Mutex
	generation                     uint64
	streamCalls                    int
	failPreviousResponseOnCall     int
	failPreviousAfterPayloadOnCall int
	requireReplayOnCall            int
	resetAfterPayloadOnCall        int
	requireReplayCalls             map[int]bool
	turnStateByCall                map[int]string
	streamCallCh                   chan int
	payloads                       [][]byte
}

type websocketPinnedFailoverStatusError struct {
	status int
	msg    string
}

func (e websocketPinnedFailoverStatusError) Error() string { return e.msg }

func (e websocketPinnedFailoverStatusError) StatusCode() int { return e.status }

type websocketReplayRequiredStatusError struct {
	msg string
}

func (e websocketReplayRequiredStatusError) Error() string { return e.msg }

func (e websocketReplayRequiredStatusError) StatusCode() int { return http.StatusBadRequest }

func (e websocketReplayRequiredStatusError) CodexWebsocketReplayRequired() bool { return true }

func (e *websocketGenerationReplayExecutor) Identifier() string { return "codex" }

func (e *websocketGenerationReplayExecutor) UpstreamGeneration(string) uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.generation
}

func (e *websocketGenerationReplayExecutor) BumpGeneration() {
	e.mu.Lock()
	e.generation++
	e.mu.Unlock()
}

func (e *websocketGenerationReplayExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketGenerationReplayExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.streamCalls++
	call := e.streamCalls
	e.payloads = append(e.payloads, bytes.Clone(req.Payload))
	failPreviousResponse := e.failPreviousResponseOnCall == call
	failPreviousAfterPayload := e.failPreviousAfterPayloadOnCall == call
	requireReplay := e.requireReplayOnCall == call || e.requireReplayCalls[call]
	resetAfterPayload := e.resetAfterPayloadOnCall == call
	turnState := e.turnStateByCall[call]
	e.mu.Unlock()
	if e.streamCallCh != nil {
		select {
		case e.streamCallCh <- call:
		default:
		}
	}

	if requireReplay {
		return nil, websocketReplayRequiredStatusError{
			msg: "codex websocket upstream reset requires transcript replay: invalid_request_error: send_error",
		}
	}

	chunks := make(chan coreexecutor.StreamChunk, 3)
	if turnState != "" {
		chunks <- coreexecutor.StreamChunk{Payload: []byte(fmt.Sprintf(`{"type":"response.metadata","headers":{"x-codex-turn-state":%q}}`, turnState))}
		chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"codex.rate_limits","rate_limits":[]}`)}
	}
	if resetAfterPayload {
		chunks <- coreexecutor.StreamChunk{Err: websocketReplayRequiredStatusError{
			msg: "codex websocket upstream reset requires transcript replay: invalid_request_error: read_error",
		}}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}
	if failPreviousResponse {
		chunks <- coreexecutor.StreamChunk{Err: websocketPinnedFailoverStatusError{
			status: http.StatusBadRequest,
			msg:    `{"error":{"message":"previous response with id 'resp-1' not found","type":"invalid_request_error","code":"previous_response_not_found"}}`,
		}}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}
	if failPreviousAfterPayload {
		chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.output_item.done","item":{"id":"out-partial","type":"message","role":"assistant","content":[{"type":"output_text","text":"partial"}]},"output_index":0}`)}
		chunks <- coreexecutor.StreamChunk{Err: websocketPinnedFailoverStatusError{
			status: http.StatusBadRequest,
			msg:    `{"error":{"message":"previous response with id 'resp-1' not found","type":"invalid_request_error","code":"previous_response_not_found"}}`,
		}}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}

	chunks <- coreexecutor.StreamChunk{Payload: []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp-%d","output":[{"type":"message","id":"out-%d"}]}}`, call, call))}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *websocketGenerationReplayExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *websocketGenerationReplayExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketGenerationReplayExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *websocketGenerationReplayExecutor) Payloads() [][]byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([][]byte, len(e.payloads))
	for i := range e.payloads {
		out[i] = bytes.Clone(e.payloads[i])
	}
	return out
}

func (e *websocketBootstrapFallbackExecutor) Identifier() string { return "test-provider" }

func (e *websocketBootstrapFallbackExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketBootstrapFallbackExecutor) ExecuteStream(_ context.Context, auth *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	authID := ""
	if auth != nil {
		authID = auth.ID
	}

	e.mu.Lock()
	if e.payloads == nil {
		e.payloads = make(map[string][][]byte)
	}
	e.authIDs = append(e.authIDs, authID)
	e.payloads[authID] = append(e.payloads[authID], bytes.Clone(req.Payload))
	e.mu.Unlock()

	chunks := make(chan coreexecutor.StreamChunk, 1)
	if authID == "auth-ws" {
		chunks <- coreexecutor.StreamChunk{Err: websocketPinnedFailoverStatusError{
			status: http.StatusUpgradeRequired,
			msg:    `{"error":{"message":"websocket bootstrap failed","type":"server_error","code":"ws_failed"}}`,
		}}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}

	chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.completed","response":{"id":"resp-http","output":[{"type":"message","id":"out-http"}]}}`)}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *websocketBootstrapFallbackExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *websocketBootstrapFallbackExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketBootstrapFallbackExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *websocketBootstrapFallbackExecutor) AuthIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.authIDs...)
}

func (e *websocketBootstrapFallbackExecutor) Payloads(authID string) [][]byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	src := e.payloads[authID]
	out := make([][]byte, len(src))
	for i := range src {
		out[i] = bytes.Clone(src[i])
	}
	return out
}

func (e *websocketDirectCaptureExecutor) Identifier() string {
	if e != nil && strings.TrimSpace(e.provider) != "" {
		return strings.TrimSpace(e.provider)
	}
	return "codex"
}

func (e *websocketDirectCaptureExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketDirectCaptureExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	authID := ""
	if auth != nil {
		authID = auth.ID
	}
	e.mu.Lock()
	e.authIDs = append(e.authIDs, authID)
	e.models = append(e.models, req.Model)
	e.payloads = append(e.payloads, bytes.Clone(req.Payload))
	e.requiredUpstreamWebsocket = append(e.requiredUpstreamWebsocket, coreexecutor.RequiredUpstreamWebsocket(ctx))
	count := len(e.payloads)
	failStatus := e.failStatus
	e.mu.Unlock()

	chunks := make(chan coreexecutor.StreamChunk, 1)
	if failStatus > 0 {
		chunks <- coreexecutor.StreamChunk{Err: websocketPinnedFailoverStatusError{
			status: failStatus,
			msg:    `{"error":{"message":"routed provider failed","type":"authentication_error","code":"invalid_api_key"}}`,
		}}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}
	responseID := fmt.Sprintf("resp-%d", count)
	chunks <- coreexecutor.StreamChunk{Payload: []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"output":[{"type":"message","id":"out-%d"}]}}`, responseID, count))}
	close(chunks)
	if count >= 2 && e.done != nil {
		e.doneOnce.Do(func() {
			close(e.done)
		})
	}
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *websocketDirectCaptureExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *websocketDirectCaptureExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketDirectCaptureExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *websocketDirectCaptureExecutor) Payloads() [][]byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([][]byte, len(e.payloads))
	for i := range e.payloads {
		out[i] = bytes.Clone(e.payloads[i])
	}
	return out
}

func (e *websocketDirectCaptureExecutor) AuthIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.authIDs...)
}

func (e *websocketDirectCaptureExecutor) Models() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.models...)
}

func (e *websocketDirectCaptureExecutor) RequiredUpstreamWebsocketFlags() []bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]bool(nil), e.requiredUpstreamWebsocket...)
}

func (e *websocketCanonicalRollbackExecutor) Identifier() string { return "xai" }

func (e *websocketCanonicalRollbackExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketCanonicalRollbackExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.payloads = append(e.payloads, bytes.Clone(req.Payload))
	e.mu.Unlock()

	chunks := make(chan coreexecutor.StreamChunk, 1)
	if call == 2 {
		chunks <- coreexecutor.StreamChunk{Err: websocketPinnedFailoverStatusError{
			status: http.StatusBadRequest,
			msg:    `{"error":{"message":"bad turn","type":"invalid_request_error","code":"invalid_request"}}`,
		}}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}
	chunks <- coreexecutor.StreamChunk{Payload: []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp-%d","output":[{"type":"message","id":"out-%d"}]}}`, call, call))}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *websocketCanonicalRollbackExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *websocketCanonicalRollbackExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketCanonicalRollbackExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *websocketCanonicalRollbackExecutor) Payloads() [][]byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([][]byte, len(e.payloads))
	for i := range e.payloads {
		out[i] = bytes.Clone(e.payloads[i])
	}
	return out
}

type websocketUpstreamDisconnectExecutor struct {
	mu         sync.Mutex
	provider   string
	subscribed chan string
	sessions   map[string]chan error
}

func (e *websocketUpstreamDisconnectExecutor) Identifier() string {
	if provider := strings.TrimSpace(e.provider); provider != "" {
		return provider
	}
	return "codex"
}

func (e *websocketUpstreamDisconnectExecutor) UpstreamDisconnectChan(sessionID string) <-chan error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	e.mu.Lock()
	if e.sessions == nil {
		e.sessions = make(map[string]chan error)
	}
	ch, ok := e.sessions[sessionID]
	if !ok {
		ch = make(chan error, 1)
		e.sessions[sessionID] = ch
	}
	subscribed := e.subscribed
	e.mu.Unlock()

	if subscribed != nil {
		select {
		case subscribed <- sessionID:
		default:
		}
	}
	return ch
}

func (e *websocketUpstreamDisconnectExecutor) TriggerDisconnect(sessionID string, err error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}
	e.mu.Lock()
	ch := e.sessions[sessionID]
	delete(e.sessions, sessionID)
	e.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- err:
	default:
	}
	close(ch)
}

func (e *websocketUpstreamDisconnectExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketUpstreamDisconnectExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, errors.New("not implemented")
}

func (e *websocketUpstreamDisconnectExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *websocketUpstreamDisconnectExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketUpstreamDisconnectExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *websocketAuthCaptureExecutor) Identifier() string { return "test-provider" }

func (e *websocketAuthCaptureExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketAuthCaptureExecutor) ExecuteStream(_ context.Context, auth *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	if auth != nil {
		e.authIDs = append(e.authIDs, auth.ID)
	}
	e.mu.Unlock()

	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.completed","response":{"id":"resp-upstream","output":[{"type":"message","id":"out-1"}]}}`)}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *websocketAuthCaptureExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *websocketAuthCaptureExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketAuthCaptureExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *websocketAuthCaptureExecutor) AuthIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.authIDs...)
}

func (e *websocketPinnedFailoverExecutor) Identifier() string { return "xai" }

func (e *websocketPinnedFailoverExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketPinnedFailoverExecutor) ExecuteStream(_ context.Context, auth *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	authID := ""
	if auth != nil {
		authID = auth.ID
	}

	e.mu.Lock()
	if e.calls == nil {
		e.calls = make(map[string]int)
	}
	if e.payloads == nil {
		e.payloads = make(map[string][][]byte)
	}
	e.authIDs = append(e.authIDs, authID)
	e.calls[authID]++
	call := e.calls[authID]
	e.payloads[authID] = append(e.payloads[authID], bytes.Clone(req.Payload))
	e.mu.Unlock()

	if authID == "auth-a" && call == 2 {
		chunks := make(chan coreexecutor.StreamChunk, 1)
		chunks <- coreexecutor.StreamChunk{Err: websocketPinnedFailoverStatusError{
			status: e.failStatus,
			msg:    fmt.Sprintf(`{"error":{"message":"credential failed","status":%d}}`, e.failStatus),
		}}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}

	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Payload: []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp-%s-%d","output":[{"type":"message","id":"out-%s-%d"}]}}`, authID, call, authID, call))}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *websocketPinnedFailoverExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *websocketPinnedFailoverExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketPinnedFailoverExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *websocketPinnedFailoverExecutor) AuthIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.authIDs...)
}

func (e *websocketPinnedFailoverExecutor) Payloads(authID string) [][]byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	src := e.payloads[authID]
	out := make([][]byte, len(src))
	for i := range src {
		out[i] = bytes.Clone(src[i])
	}
	return out
}

func (e *websocketCaptureExecutor) Identifier() string { return "test-provider" }

func (e *websocketProviderCaptureExecutor) Identifier() string {
	if e != nil && strings.TrimSpace(e.provider) != "" {
		return strings.TrimSpace(e.provider)
	}
	return "test-provider"
}

func (e *websocketCaptureExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketCaptureExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.streamCalls++
	e.payloads = append(e.payloads, bytes.Clone(req.Payload))
	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.completed","response":{"id":"resp-upstream","output":[{"type":"message","id":"out-1"}]}}`)}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *websocketCaptureExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *websocketCaptureExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketCaptureExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *websocketCompactionCaptureExecutor) Identifier() string {
	if e != nil && strings.TrimSpace(e.provider) != "" {
		return strings.TrimSpace(e.provider)
	}
	return "codex"
}

func (e *websocketCompactionCaptureExecutor) Execute(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (coreexecutor.Response, error) {
	e.mu.Lock()
	e.compactPayload = bytes.Clone(req.Payload)
	e.mu.Unlock()
	if opts.Alt != "responses/compact" {
		return coreexecutor.Response{}, fmt.Errorf("unexpected non-compact execute alt: %q", opts.Alt)
	}
	return coreexecutor.Response{Payload: []byte(`{"id":"cmp-1","object":"response.compaction"}`)}, nil
}

func (e *websocketCompactionCaptureExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	callIndex := len(e.streamPayloads)
	e.streamPayloads = append(e.streamPayloads, bytes.Clone(req.Payload))
	errFail := e.failStreamCalls[callIndex]
	e.mu.Unlock()

	if errFail != nil {
		chunks := make(chan coreexecutor.StreamChunk, 1)
		chunks <- coreexecutor.StreamChunk{Err: errFail}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}

	var payload []byte
	switch callIndex {
	case 0:
		payload = []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[{"type":"function_call","id":"fc-1","call_id":"call-1","name":"tool"}]}}`)
	case 1:
		payload = []byte(`{"type":"response.completed","response":{"id":"resp-2","output":[{"type":"message","id":"assistant-1"}]}}`)
	default:
		payload = []byte(`{"type":"response.completed","response":{"id":"resp-3","output":[{"type":"message","id":"assistant-2"}]}}`)
	}

	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Payload: payload}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *websocketCompactionCaptureExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *websocketCompactionCaptureExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketCompactionCaptureExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *websocketCompactionCaptureExecutor) DropUpstreamSession(_ string, reason string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dropCalls++
	e.dropReasons = append(e.dropReasons, strings.TrimSpace(reason))
}

func (e *websocketCompactionCaptureExecutor) SendResponseProcessed(_ string, responseID string) error {
	e.mu.Lock()
	e.processedIDs = append(e.processedIDs, responseID)
	processedCh := e.processedCh
	e.mu.Unlock()
	if processedCh != nil {
		processedCh <- responseID
	}
	return nil
}

func TestNormalizeResponsesWebsocketRequestCreate(t *testing.T) {
	raw := []byte(`{"type":"response.create","model":"test-model","stream":false,"input":[{"type":"message","id":"msg-1"}]}`)

	normalized, last, errMsg := normalizeResponsesWebsocketRequest(raw, nil, nil)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "type").Exists() {
		t.Fatalf("normalized create request must not include type field")
	}
	if !gjson.GetBytes(normalized, "stream").Bool() {
		t.Fatalf("normalized create request must force stream=true")
	}
	if gjson.GetBytes(normalized, "model").String() != "test-model" {
		t.Fatalf("unexpected model: %s", gjson.GetBytes(normalized, "model").String())
	}
	if !bytes.Equal(last, normalized) {
		t.Fatalf("last request snapshot should match normalized request")
	}
}

func TestNormalizeResponsesWebsocketRequestCreateWithHistory(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"input":[{"type":"message","id":"msg-1"}]}`)
	lastResponseOutput := []byte(`[
		{"type":"function_call","id":"fc-1","call_id":"call-1"},
		{"type":"message","id":"assistant-1"}
	]`)
	raw := []byte(`{"type":"response.create","input":[{"type":"function_call_output","call_id":"call-1","id":"tool-out-1"}]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequest(raw, lastRequest, lastResponseOutput)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "type").Exists() {
		t.Fatalf("normalized subsequent create request must not include type field")
	}
	if gjson.GetBytes(normalized, "model").String() != "test-model" {
		t.Fatalf("unexpected model: %s", gjson.GetBytes(normalized, "model").String())
	}

	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 4 {
		t.Fatalf("merged input len = %d, want 4", len(input))
	}
	if input[0].Get("id").String() != "msg-1" ||
		input[1].Get("id").String() != "fc-1" ||
		input[2].Get("id").String() != "assistant-1" ||
		input[3].Get("id").String() != "tool-out-1" {
		t.Fatalf("unexpected merged input order")
	}
	if !bytes.Equal(next, normalized) {
		t.Fatalf("next request snapshot should match normalized request")
	}
}

func TestNormalizeResponsesWebsocketRequestWithPreviousResponseIDIncremental(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"instructions":"be helpful","input":[{"type":"message","id":"msg-1"}]}`)
	lastResponseOutput := []byte(`[
		{"type":"function_call","id":"fc-1","call_id":"call-1"},
		{"type":"message","id":"assistant-1"}
	]`)
	raw := []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"function_call_output","call_id":"call-1","id":"tool-out-1"}]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, lastRequest, lastResponseOutput, true, false, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "type").Exists() {
		t.Fatalf("normalized request must not include type field")
	}
	if gjson.GetBytes(normalized, "previous_response_id").String() != "resp-1" {
		t.Fatalf("previous_response_id must be preserved in incremental mode")
	}
	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 1 {
		t.Fatalf("incremental input len = %d, want 1", len(input))
	}
	if input[0].Get("id").String() != "tool-out-1" {
		t.Fatalf("unexpected incremental input item id: %s", input[0].Get("id").String())
	}
	if gjson.GetBytes(normalized, "model").String() != "test-model" {
		t.Fatalf("unexpected model: %s", gjson.GetBytes(normalized, "model").String())
	}
	if gjson.GetBytes(normalized, "instructions").String() != "be helpful" {
		t.Fatalf("unexpected instructions: %s", gjson.GetBytes(normalized, "instructions").String())
	}
	if gjson.GetBytes(next, "previous_response_id").Exists() {
		t.Fatalf("next request snapshot must not include previous_response_id: %s", next)
	}
	nextInput := gjson.GetBytes(next, "input").Array()
	if len(nextInput) != 4 {
		t.Fatalf("next request snapshot input len = %d, want 4: %s", len(nextInput), next)
	}
	for _, id := range []string{"msg-1", "fc-1", "assistant-1", "tool-out-1"} {
		if !strings.Contains(gjson.GetBytes(next, "input").Raw, fmt.Sprintf(`"id":"%s"`, id)) {
			t.Fatalf("next request snapshot missing %s: %s", id, next)
		}
	}
	if gjson.GetBytes(next, "model").String() != "test-model" {
		t.Fatalf("next request snapshot model = %s, want test-model", gjson.GetBytes(next, "model").String())
	}
	if gjson.GetBytes(next, "instructions").String() != "be helpful" {
		t.Fatalf("next request snapshot instructions = %s, want be helpful", gjson.GetBytes(next, "instructions").String())
	}
}

func TestNormalizeResponsesWebsocketInitialForcedReplayPreservesPreviousResponseID(t *testing.T) {
	raw := []byte(`{"type":"response.create","model":"test-model","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-1"}]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequestWithReplayMode(raw, nil, nil, "", nil, false, true, false, true)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if got := gjson.GetBytes(normalized, "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("previous_response_id = %q, want resp-1", got)
	}
	if gjson.GetBytes(normalized, "type").Exists() {
		t.Fatalf("normalized request must not include type field: %s", normalized)
	}
	if !bytes.Equal(next, normalized) {
		t.Fatalf("next request snapshot should match normalized request")
	}
}

func TestNormalizeResponsesWebsocketRequestInjectsPreviousResponseIDForIncremental(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"instructions":"be helpful","input":[{"type":"message","id":"msg-1"}]}`)
	lastResponseOutput := []byte(`[
		{"type":"function_call","id":"fc-1","call_id":"call-1"},
		{"type":"message","id":"assistant-1"}
	]`)
	raw := []byte(`{"type":"response.create","input":[{"type":"function_call_output","call_id":"call-1","id":"tool-out-1"}]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequestWithLastResponseID(raw, lastRequest, lastResponseOutput, "resp-1", true, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if got := gjson.GetBytes(normalized, "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("previous_response_id = %q, want resp-1", got)
	}
	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 1 {
		t.Fatalf("incremental input len = %d, want 1: %s", len(input), normalized)
	}
	if input[0].Get("id").String() != "tool-out-1" {
		t.Fatalf("unexpected incremental input item id: %s", input[0].Get("id").String())
	}
	if gjson.GetBytes(normalized, "model").String() != "test-model" {
		t.Fatalf("unexpected model: %s", gjson.GetBytes(normalized, "model").String())
	}
	if gjson.GetBytes(normalized, "instructions").String() != "be helpful" {
		t.Fatalf("unexpected instructions: %s", gjson.GetBytes(normalized, "instructions").String())
	}
	if gjson.GetBytes(next, "previous_response_id").Exists() {
		t.Fatalf("next request snapshot must not include previous_response_id: %s", next)
	}
	nextInput := gjson.GetBytes(next, "input").Array()
	if len(nextInput) != 4 {
		t.Fatalf("next request snapshot input len = %d, want 4: %s", len(nextInput), next)
	}
	for _, id := range []string{"msg-1", "fc-1", "assistant-1", "tool-out-1"} {
		if !strings.Contains(gjson.GetBytes(next, "input").Raw, fmt.Sprintf(`"id":"%s"`, id)) {
			t.Fatalf("next request snapshot missing %s: %s", id, next)
		}
	}
}

func TestNormalizeResponsesWebsocketRequestTreatsCodexFullCreateAsReplacement(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"input":[{"type":"message","role":"user","id":"msg-1","content":"original prompt"}]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"assistant-1","content":"original answer"}
	]`)
	raw := []byte(`{
		"type":"response.create",
		"model":"test-model",
		"input":[{"type":"message","role":"user","id":"msg-edited","content":"edited prompt"}],
		"tools":[],
		"tool_choice":"auto",
		"parallel_tool_calls":true,
		"store":false,
		"stream":true,
		"include":[],
		"client_metadata":{
			"x-codex-installation-id":"install-1",
			"x-codex-window-id":"thread-1:0"
		}
	}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequestWithLastResponseID(raw, lastRequest, lastResponseOutput, "resp-1", true, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "previous_response_id").Exists() {
		t.Fatalf("full Codex create must not reuse previous_response_id: %s", normalized)
	}
	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 1 || input[0].Get("id").String() != "msg-edited" {
		t.Fatalf("full Codex create must replace stale transcript: %s", normalized)
	}
	if !bytes.Equal(next, normalized) {
		t.Fatalf("next request snapshot should match replacement request")
	}

	replayed, _, errMsg := normalizeResponsesWebsocketRequestWithReplayMode(raw, lastRequest, lastResponseOutput, "resp-1", nil, true, false, false, true)
	if errMsg != nil {
		t.Fatalf("unexpected forced replay error: %v", errMsg.Error)
	}
	if gjson.GetBytes(replayed, "previous_response_id").Exists() {
		t.Fatalf("forced replay full Codex create must not reuse previous_response_id: %s", replayed)
	}
	replayedInput := gjson.GetBytes(replayed, "input").Array()
	if len(replayedInput) != 1 || replayedInput[0].Get("id").String() != "msg-edited" {
		t.Fatalf("forced replay full Codex create must still replace stale transcript: %s", replayed)
	}
}

func TestNormalizeResponsesWebsocketRequestInjectsPreviousResponseIDWhenPendingOutputIsPresent(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"instructions":"be helpful","input":[{"type":"message","id":"msg-1"}]}`)
	lastResponseOutput := []byte(`[]`)
	raw := []byte(`{"type":"response.create","input":[{"type":"function_call_output","call_id":"call-1","id":"tool-out-1"}]}`)

	normalized, _, errMsg := normalizeResponsesWebsocketRequestWithIncrementalState(raw, lastRequest, lastResponseOutput, "resp-1", []string{"call-1"}, true, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if got := gjson.GetBytes(normalized, "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("previous_response_id = %q, want resp-1", got)
	}
	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 1 || input[0].Get("id").String() != "tool-out-1" {
		t.Fatalf("unexpected incremental input: %s", normalized)
	}
}

func TestNormalizeResponsesWebsocketRequestSkipsPreviousResponseIDWhenPendingOutputIsMissing(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"instructions":"be helpful","input":[{"type":"message","id":"msg-1"}]}`)
	lastResponseOutput := []byte(`[
		{"type":"function_call","id":"fc-1","call_id":"call-1"}
	]`)
	raw := []byte(`{"type":"response.create","input":[{"type":"message","role":"user","id":"summary-1","content":"compacted summary"}]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequestWithIncrementalState(raw, lastRequest, lastResponseOutput, "resp-1", []string{"call-1"}, true, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id must not be injected when pending tool output is missing: %s", normalized)
	}
	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 1 {
		t.Fatalf("replacement input len = %d, want 1: %s", len(input), normalized)
	}
	if input[0].Get("id").String() != "summary-1" {
		t.Fatalf("unexpected replacement input: %s", normalized)
	}
	if !bytes.Equal(next, normalized) {
		t.Fatalf("next request snapshot should match normalized request")
	}
}

func TestNormalizeResponsesWebsocketRequestReplacesCodexLocalCompactionTranscript(t *testing.T) {
	lastRequest := []byte(`{"model":"gpt-5.6-sol","stream":true,"instructions":"be helpful","input":[
		{"type":"message","role":"user","id":"old-user","content":[{"type":"input_text","text":"old prompt"}]},
		{"type":"function_call_output","id":"old-tool-output","call_id":"old-call","output":"old result"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"function_call","id":"old-tool-call","call_id":"old-call","name":"lookup","arguments":"{}"},
		{"type":"message","role":"assistant","id":"old-assistant","content":[{"type":"output_text","text":"old answer"}]}
	]`)
	raw := []byte(fmt.Sprintf(`{"type":"response.create","input":[
		{"type":"additional_tools","role":"developer","tools":[]},
		{"role":"developer","id":"initial-context","content":"workspace context"},
		{"type":"message","role":"user","id":"compacted-user","content":[{"type":"input_text","text":"retained context"}]},
		{"role":"user","id":"local-summary","content":%q},
		{"type":"message","role":"developer","id":"turn-context","content":[{"type":"input_text","text":"current workspace context"}]},
		{"role":"user","id":"incoming-user","content":"continue the task"}
	],"parallel_tool_calls":true,"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"true"}}`, codexLocalCompactionSummaryPrefix+"\nThe compacted summary."))

	normalized, next, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, lastRequest, lastResponseOutput, false, false, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "previous_response_id").Exists() {
		t.Fatalf("replacement request must not include previous_response_id: %s", normalized)
	}
	if got, want := gjson.GetBytes(normalized, "input").Raw, gjson.GetBytes(raw, "input").Raw; got != want {
		t.Fatalf("replacement input did not preserve the complete new transcript:\n got: %s\nwant: %s", got, want)
	}
	input := gjson.GetBytes(normalized, "input").Array()
	wantIDs := []string{"", "initial-context", "compacted-user", "local-summary", "turn-context", "incoming-user"}
	if len(input) != len(wantIDs) {
		t.Fatalf("replacement input len = %d, want %d: %s", len(input), len(wantIDs), normalized)
	}
	for index, wantID := range wantIDs {
		if got := input[index].Get("id").String(); got != wantID {
			t.Fatalf("replacement input[%d].id = %q, want %q: %s", index, got, wantID, normalized)
		}
	}
	if got := input[0].Get("type").String(); got != "additional_tools" {
		t.Fatalf("input[0].type = %q, want additional_tools: %s", got, normalized)
	}
	if got := input[0].Get("role").String(); got != "developer" {
		t.Fatalf("input[0].role = %q, want developer: %s", got, normalized)
	}
	if tools := input[0].Get("tools"); !tools.IsArray() || len(tools.Array()) != 0 {
		t.Fatalf("input[0] empty tools array was not preserved: %s", normalized)
	}
	for _, staleID := range []string{"old-user", "old-tool-output", "old-tool-call", "old-assistant"} {
		if bytes.Contains(normalized, []byte(staleID)) {
			t.Fatalf("replacement input contains stale item %q: %s", staleID, normalized)
		}
	}
	if got := gjson.GetBytes(normalized, "model").String(); got != "gpt-5.6-sol" {
		t.Fatalf("model = %q, want gpt-5.6-sol", got)
	}
	if got := gjson.GetBytes(normalized, "instructions").String(); got != "be helpful" {
		t.Fatalf("instructions = %q, want be helpful", got)
	}
	if !gjson.GetBytes(normalized, "stream").Bool() {
		t.Fatalf("stream must be enabled: %s", normalized)
	}
	if !gjson.GetBytes(normalized, "parallel_tool_calls").Bool() {
		t.Fatalf("parallel_tool_calls was not preserved: %s", normalized)
	}
	if got := gjson.GetBytes(normalized, "client_metadata.ws_request_header_x_openai_internal_codex_responses_lite").String(); got != "true" {
		t.Fatalf("Responses Lite client metadata = %q, want true: %s", got, normalized)
	}
	if !bytes.Equal(next, normalized) {
		t.Fatalf("next request snapshot should match normalized request")
	}
}

func TestShouldReplaceWebsocketTranscriptCodexLocalCompactionSemantics(t *testing.T) {
	compactedInput := gjson.Parse(fmt.Sprintf(`[
		{"type":"message","role":"developer","content":[{"type":"input_text","text":"initial context"}]},
		{"type":"message","role":"user","content":[{"type":"input_text","text":"retained context"}]},
		{"type":"message","role":"user","content":[{"type":"input_text","text":%q}]}
	]`, codexLocalCompactionSummaryPrefix+"\nSummary body."))
	if !shouldReplaceWebsocketTranscript([]byte(`{"type":"response.create"}`), compactedInput, nil) {
		t.Fatal("Codex local compaction input must replace the websocket transcript")
	}
	for _, request := range []string{
		`{"type":"response.create","previous_response_id":"resp-1"}`,
		`{"type":"response.create","previous_response_id":""}`,
		`{"type":"response.create","previous_response_id":null}`,
	} {
		if shouldReplaceWebsocketTranscript([]byte(request), compactedInput, nil) {
			t.Fatalf("request carrying previous_response_id must not use the local compaction rule: %s", request)
		}
	}
	if shouldReplaceWebsocketTranscript([]byte(`{"type":"response.append"}`), compactedInput, nil) {
		t.Fatal("response.append must not be treated as a full local compaction reset")
	}

	ordinaryInput := gjson.Parse(`[
		{"type":"message","role":"developer","content":"Please summarize future messages."},
		{"type":"message","role":"user","content":[{"type":"input_text","text":"Please create a compacted summary of this text."}]}
	]`)
	if shouldReplaceWebsocketTranscript([]byte(`{"type":"response.create"}`), ordinaryInput, nil) {
		t.Fatal("ordinary user/developer input must not replace the transcript")
	}
}

func TestCodexLocalCompactionSummaryContentShapes(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    bool
	}{
		{name: "string content", content: fmt.Sprintf(`%q`, codexLocalCompactionSummaryPrefix+"\nSummary body."), want: true},
		{name: "multiple input text parts", content: fmt.Sprintf(`[{"type":"input_text","text":%q},{"type":"input_text","text":"\nSummary body."}]`, codexLocalCompactionSummaryPrefix), want: true},
		{name: "non-text part before summary", content: fmt.Sprintf(`[{"type":"input_image","image_url":"data:image/png;base64,AA=="},{"type":"input_text","text":%q}]`, codexLocalCompactionSummaryPrefix+"\nSummary body."), want: true},
		{name: "bare prefix", content: fmt.Sprintf(`%q`, codexLocalCompactionSummaryPrefix), want: false},
		{name: "prefix followed by space", content: fmt.Sprintf(`%q`, codexLocalCompactionSummaryPrefix+" Summary body."), want: false},
		{name: "summary after ordinary text", content: fmt.Sprintf(`[{"type":"input_text","text":"ordinary text"},{"type":"input_text","text":%q}]`, codexLocalCompactionSummaryPrefix+"\nSummary body."), want: false},
		{name: "developer summary", content: fmt.Sprintf(`%q`, codexLocalCompactionSummaryPrefix+"\nSummary body."), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			role := "user"
			if test.name == "developer summary" {
				role = "developer"
			}
			input := gjson.Parse(fmt.Sprintf(`[{"type":"message","role":%q,"content":%s}]`, role, test.content))
			if got := inputHasCodexLocalCompactionSummary(input); got != test.want {
				t.Fatalf("inputHasCodexLocalCompactionSummary() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestCodexLocalCompactionSummaryAdditionalToolsConstraints(t *testing.T) {
	summary := fmt.Sprintf(`{"role":"user","content":%q}`, codexLocalCompactionSummaryPrefix+"\nSummary body.")
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{name: "Responses Lite tools first", input: fmt.Sprintf(`[{"type":"additional_tools","role":"developer","tools":[{"type":"custom","name":"exec"}]},%s]`, summary), want: true},
		{name: "tools after message", input: fmt.Sprintf(`[%s,{"type":"additional_tools","role":"developer","tools":[{"type":"custom","name":"exec"}]}]`, summary)},
		{name: "tools with user role", input: fmt.Sprintf(`[{"type":"additional_tools","role":"user","tools":[{"type":"custom","name":"exec"}]},%s]`, summary)},
		{name: "tools missing array", input: fmt.Sprintf(`[{"type":"additional_tools","role":"developer"},%s]`, summary)},
		{name: "tools not array", input: fmt.Sprintf(`[{"type":"additional_tools","role":"developer","tools":{}},%s]`, summary)},
		{name: "tools empty", input: fmt.Sprintf(`[{"type":"additional_tools","role":"developer","tools":[]},%s]`, summary), want: true},
		{name: "malformed tool", input: fmt.Sprintf(`[{"type":"additional_tools","role":"developer","tools":[null]},%s]`, summary)},
		{name: "arbitrary input item", input: fmt.Sprintf(`[{"type":"unknown","role":"developer"},%s]`, summary)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := inputHasCodexLocalCompactionSummary(gjson.Parse(test.input)); got != test.want {
				t.Fatalf("inputHasCodexLocalCompactionSummary() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestCodexLocalCompactionSummaryRejectsOrdinaryHistoryItems(t *testing.T) {
	tests := []struct {
		name        string
		historyItem string
		wantReplace bool
	}{
		{name: "reasoning", historyItem: `{"type":"reasoning","id":"reasoning-1"}`},
		{name: "assistant", historyItem: `{"type":"message","role":"assistant","id":"assistant-1"}`, wantReplace: true},
		{name: "function call", historyItem: `{"type":"function_call","call_id":"call-1"}`, wantReplace: true},
		{name: "function call output", historyItem: `{"type":"function_call_output","call_id":"call-1"}`},
		{name: "custom tool call", historyItem: `{"type":"custom_tool_call","call_id":"call-1"}`, wantReplace: true},
		{name: "custom tool call output", historyItem: `{"type":"custom_tool_call_output","call_id":"call-1"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := gjson.Parse(fmt.Sprintf(`[%s,{"type":"message","role":"user","content":[{"type":"input_text","text":%q}]}]`, test.historyItem, codexLocalCompactionSummaryPrefix+"\nSummary body."))
			if inputHasCodexLocalCompactionSummary(input) {
				t.Fatal("ordinary transcript history must not match the local user-summary shape")
			}
			if got := shouldReplaceWebsocketTranscript([]byte(`{"type":"response.create"}`), input, nil); got != test.wantReplace {
				t.Fatalf("shouldReplaceWebsocketTranscript() = %t, want %t", got, test.wantReplace)
			}
		})
	}
}

func TestNormalizeResponsesWebsocketRequestWithPreviousResponseIDMergedWhenIncrementalDisabled(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"input":[{"type":"message","id":"msg-1"}]}`)
	lastResponseOutput := []byte(`[
		{"type":"function_call","id":"fc-1","call_id":"call-1"},
		{"type":"message","id":"assistant-1"}
	]`)
	raw := []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"function_call_output","call_id":"call-1","id":"tool-out-1"}]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, lastRequest, lastResponseOutput, false, false, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id must be removed when incremental mode is disabled")
	}
	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 4 {
		t.Fatalf("merged input len = %d, want 4", len(input))
	}
	if input[0].Get("id").String() != "msg-1" ||
		input[1].Get("id").String() != "fc-1" ||
		input[2].Get("id").String() != "assistant-1" ||
		input[3].Get("id").String() != "tool-out-1" {
		t.Fatalf("unexpected merged input order")
	}
	if !bytes.Equal(next, normalized) {
		t.Fatalf("next request snapshot should match normalized request")
	}
}

func TestNormalizeInitialResponseCreateDropsPreviousResponseIDForTranscriptReplacement(t *testing.T) {
	tests := []struct {
		name                        string
		raw                         []byte
		allowCompactionReplayBypass bool
		forceTranscriptReplacement  bool
		wantInputLen                int
		wantNoCompactionItems       bool
	}{
		{
			name:                        "compact marker",
			allowCompactionReplayBypass: true,
			wantInputLen:                2,
			raw: []byte(`{"type":"response.create","model":"test-model","previous_response_id":"resp-old","input":[
				{"type":"message","role":"user","id":"msg-1","content":"compacted question"},
				{"type":"context_compaction","encrypted_content":"summary"}
			]}`),
		},
		{
			name:                        "forced compact replay",
			allowCompactionReplayBypass: true,
			forceTranscriptReplacement:  true,
			wantInputLen:                2,
			raw: []byte(`{"type":"response.create","model":"test-model","previous_response_id":"resp-old","input":[
				{"type":"message","role":"user","id":"msg-1","content":"compacted summary"},
				{"type":"message","role":"user","id":"msg-2","content":"new question"}
			]}`),
		},
		{
			name:                  "unsupported compact marker",
			wantInputLen:          1,
			wantNoCompactionItems: true,
			raw: []byte(`{"type":"response.create","model":"test-model","previous_response_id":"resp-old","input":[
				{"type":"message","role":"user","id":"msg-1","content":"compacted question"},
				{"type":"context_compaction","encrypted_content":"summary"}
			]}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			normalized, next, errMsg := normalizeResponsesWebsocketRequestWithMode(tt.raw, nil, nil, true, tt.allowCompactionReplayBypass, tt.forceTranscriptReplacement)
			if errMsg != nil {
				t.Fatalf("unexpected error: %v", errMsg.Error)
			}
			if gjson.GetBytes(normalized, "previous_response_id").Exists() {
				t.Fatalf("previous_response_id must be removed for initial transcript replacement: %s", normalized)
			}
			if !bytes.Equal(next, normalized) {
				t.Fatalf("next request snapshot should match normalized request")
			}
			if gjson.GetBytes(normalized, "type").Exists() {
				t.Fatalf("normalized request must not include type field")
			}
			if !gjson.GetBytes(normalized, "stream").Bool() {
				t.Fatalf("normalized request must enable stream: %s", normalized)
			}
			input := gjson.GetBytes(normalized, "input").Array()
			if len(input) != tt.wantInputLen {
				t.Fatalf("normalized input len = %d, want %d: %s", len(input), tt.wantInputLen, normalized)
			}
			if tt.wantNoCompactionItems {
				for _, item := range input {
					if isResponsesWebsocketCompactionItemType(item.Get("type").String()) {
						t.Fatalf("compaction item must be stripped for unsupported initial replacement: %s", normalized)
					}
				}
			}
		})
	}
}

func TestNormalizeSubsequentRequestPlainFullCreateSkipsStaleMerge(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"input":[
		{"type":"message","role":"user","id":"msg-1","content":"first question"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"out-1","content":"first answer"}
	]`)
	raw := []byte(`{"type":"response.create","model":"test-model","input":[
		{"type":"message","role":"user","id":"msg-1","content":"first question"},
		{"type":"message","role":"user","id":"msg-2","content":"second question"}
	]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, lastRequest, lastResponseOutput, true, true, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "previous_response_id").Exists() {
		t.Fatalf("full create replacement must not include previous_response_id: %s", normalized)
	}
	if !bytes.Equal(next, normalized) {
		t.Fatalf("next request snapshot should match normalized replacement")
	}
	inputRaw := gjson.GetBytes(normalized, "input").Raw
	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 2 {
		t.Fatalf("input len = %d, want 2 full-create replacement items: %s", len(input), normalized)
	}
	for _, id := range []string{"msg-1", "msg-2"} {
		if !strings.Contains(inputRaw, fmt.Sprintf(`"id":"%s"`, id)) {
			t.Fatalf("normalized full create missing %s: %s", id, normalized)
		}
	}
	if strings.Contains(inputRaw, `"id":"out-1"`) {
		t.Fatalf("stale last response output was merged into full create replacement: %s", normalized)
	}
}

func TestNormalizeSubsequentRequestCompactionTriggerCleansSnapshot(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"input":[
		{"type":"message","role":"user","id":"msg-old","content":"old prompt"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"out-old","content":"old answer"}
	]`)
	raw := []byte(`{"type":"response.create","input":[
		{"type":"message","role":"user","id":"msg-1","content":"compacted prompt"},
		{"type":"compaction_trigger"},
		{"type":"context_compaction","encrypted_content":"summary"}
	]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, lastRequest, lastResponseOutput, false, false, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	normalizedInput := gjson.GetBytes(normalized, "input").Array()
	if len(normalizedInput) != 2 {
		t.Fatalf("normalized input len = %d, want message + compaction_trigger: %s", len(normalizedInput), normalized)
	}
	if normalizedInput[1].Get("type").String() != "compaction_trigger" {
		t.Fatalf("normalized request must preserve compaction_trigger for executor routing: %s", normalized)
	}
	if strings.Contains(gjson.GetBytes(next, "input").Raw, "compaction_trigger") {
		t.Fatalf("next request snapshot must not retain compaction_trigger: %s", next)
	}
	nextInput := gjson.GetBytes(next, "input").Array()
	if len(nextInput) != 1 || nextInput[0].Get("id").String() != "msg-1" {
		t.Fatalf("next request snapshot should retain only compacted transcript items: %s", next)
	}
}

func TestNormalizeSubsequentRequestCompactionTriggerOnlyMergesHistory(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"input":[
		{"type":"message","role":"user","id":"msg-old","content":"old prompt"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"out-old","content":"old answer"}
	]`)
	raw := []byte(`{"type":"response.create","previous_response_id":"resp-old","input":[
		{"type":"compaction_trigger"}
	]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, lastRequest, lastResponseOutput, true, true, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id must be removed for trigger-only compact merge: %s", normalized)
	}
	normalizedInput := gjson.GetBytes(normalized, "input").Array()
	if len(normalizedInput) != 3 {
		t.Fatalf("normalized input len = %d, want history + output + trigger: %s", len(normalizedInput), normalized)
	}
	if normalizedInput[0].Get("id").String() != "msg-old" ||
		normalizedInput[1].Get("id").String() != "out-old" ||
		normalizedInput[2].Get("type").String() != "compaction_trigger" {
		t.Fatalf("unexpected normalized input order: %s", normalized)
	}
	nextInput := gjson.GetBytes(next, "input").Array()
	if len(nextInput) != 2 {
		t.Fatalf("next input len = %d, want snapshot without trigger: %s", len(nextInput), next)
	}
	if strings.Contains(gjson.GetBytes(next, "input").Raw, "compaction_trigger") {
		t.Fatalf("next request snapshot must not retain compaction_trigger: %s", next)
	}
}

func TestNormalizeSubsequentRequestCompactionTriggerFullFallbackDoesNotMergeStaleHistory(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"input":[
		{"type":"message","role":"user","id":"msg-old","content":"old prompt"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"out-old","content":"old answer"}
	]`)
	raw := []byte(`{"type":"response.create","input":[
		{"type":"message","role":"user","id":"msg-old","content":"old prompt"},
		{"type":"message","role":"assistant","id":"out-old","content":"old answer"},
		{"type":"compaction_trigger"}
	]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, lastRequest, lastResponseOutput, true, true, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	normalizedInput := gjson.GetBytes(normalized, "input").Array()
	if len(normalizedInput) != 3 {
		t.Fatalf("normalized input len = %d, want fallback transcript + trigger without stale merge: %s", len(normalizedInput), normalized)
	}
	if normalizedInput[0].Get("id").String() != "msg-old" ||
		normalizedInput[1].Get("id").String() != "out-old" ||
		normalizedInput[2].Get("type").String() != "compaction_trigger" {
		t.Fatalf("unexpected normalized input order: %s", normalized)
	}
	nextInput := gjson.GetBytes(next, "input").Array()
	if len(nextInput) != 2 {
		t.Fatalf("next input len = %d, want fallback transcript without trigger: %s", len(nextInput), next)
	}
	if strings.Contains(gjson.GetBytes(next, "input").Raw, "compaction_trigger") {
		t.Fatalf("next request snapshot must not retain compaction_trigger: %s", next)
	}
}

func TestNormalizeResponsesWebsocketRequestAppend(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"input":[{"type":"message","id":"msg-1"}]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","id":"assistant-1"},
		{"type":"function_call_output","id":"tool-out-1"}
	]`)
	raw := []byte(`{"type":"response.append","input":[{"type":"message","id":"msg-2"},{"type":"message","id":"msg-3"}]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequest(raw, lastRequest, lastResponseOutput)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 5 {
		t.Fatalf("merged input len = %d, want 5", len(input))
	}
	if input[0].Get("id").String() != "msg-1" ||
		input[1].Get("id").String() != "assistant-1" ||
		input[2].Get("id").String() != "tool-out-1" ||
		input[3].Get("id").String() != "msg-2" ||
		input[4].Get("id").String() != "msg-3" {
		t.Fatalf("unexpected merged input order")
	}
	if !bytes.Equal(next, normalized) {
		t.Fatalf("next request snapshot should match normalized append request")
	}
}

func TestNormalizeResponsesWebsocketRequestAppendWithoutCreate(t *testing.T) {
	raw := []byte(`{"type":"response.append","input":[]}`)

	_, _, errMsg := normalizeResponsesWebsocketRequest(raw, nil, nil)
	if errMsg == nil {
		t.Fatalf("expected error for append without previous request")
	}
	if errMsg.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", errMsg.StatusCode, http.StatusBadRequest)
	}
}

func TestWebsocketJSONPayloadsFromChunk(t *testing.T) {
	chunk := []byte("event: response.created\n\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\"}}\n\ndata: [DONE]\n")

	payloads := websocketJSONPayloadsFromChunk(chunk)
	if len(payloads) != 1 {
		t.Fatalf("payloads len = %d, want 1", len(payloads))
	}
	if gjson.GetBytes(payloads[0], "type").String() != "response.created" {
		t.Fatalf("unexpected payload type: %s", gjson.GetBytes(payloads[0], "type").String())
	}
}

func TestWebsocketJSONPayloadsFromPlainJSONChunk(t *testing.T) {
	chunk := []byte(`{"type":"response.completed","response":{"id":"resp-1"}}`)

	payloads := websocketJSONPayloadsFromChunk(chunk)
	if len(payloads) != 1 {
		t.Fatalf("payloads len = %d, want 1", len(payloads))
	}
	if gjson.GetBytes(payloads[0], "type").String() != "response.completed" {
		t.Fatalf("unexpected payload type: %s", gjson.GetBytes(payloads[0], "type").String())
	}
}

func TestResponseCompletedOutputFromPayload(t *testing.T) {
	payload := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[{"type":"message","id":"out-1"}]}}`)

	output := responseCompletedOutputFromPayload(payload, nil, nil)
	items := gjson.ParseBytes(output).Array()
	if len(items) != 1 {
		t.Fatalf("output len = %d, want 1", len(items))
	}
	if items[0].Get("id").String() != "out-1" {
		t.Fatalf("unexpected output id: %s", items[0].Get("id").String())
	}
}

func TestResponseCompletedOutputFromPayloadDropsIncompleteCollectedToolCalls(t *testing.T) {
	payload := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[]}}`)
	collector := map[int64][]byte{
		0: []byte(`{"type":"message","id":"msg-1"}`),
		1: []byte(`{"type":"function_call","call_id":"call-1","name":"exec"}`),
		2: []byte(`{"type":"custom_tool_call","call_id":"call-2","name":"exec","input":"pwd"}`),
	}

	output := responseCompletedOutputFromPayload(payload, collector, nil)
	items := gjson.ParseBytes(output).Array()
	if len(items) != 2 {
		t.Fatalf("output len = %d, want 2: %s", len(items), output)
	}
	if items[0].Get("type").String() != "message" || items[0].Get("id").String() != "msg-1" {
		t.Fatalf("unexpected first output item: %s", items[0].Raw)
	}
	if items[1].Get("type").String() != "custom_tool_call" || items[1].Get("call_id").String() != "call-2" {
		t.Fatalf("unexpected second output item: %s", items[1].Raw)
	}
}

func TestRestoreResponsesWebsocketCompletionOutputPreservesNonEmptyOutput(t *testing.T) {
	payload := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[{"type":"message","id":"out-1"}]}}`)
	collector := map[int64][]byte{0: []byte(`{"type":"function_call","id":"call-1","call_id":"call-1"}`)}

	restored := restoreResponsesWebsocketCompletionOutput(payload, collector, nil)
	if string(restored) != string(payload) {
		t.Fatalf("non-empty completion output was overwritten: %s", restored)
	}
}

func TestRestoreResponsesWebsocketCompletionOutputReconcilesConflictingToolCall(t *testing.T) {
	payload := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[{"type":"message","id":"msg-1"},{"type":"function_call","call_id":"call-1","name":"exec"}]}}`)
	collector := map[int64][]byte{0: []byte(`{"type":"custom_tool_call","id":"ctc-1","call_id":"call-1","name":"exec","input":"pwd","status":"completed"}`)}

	restored := restoreResponsesWebsocketCompletionOutput(payload, collector, nil)
	output := gjson.GetBytes(restored, "response.output").Array()
	if len(output) != 2 {
		t.Fatalf("restored output len = %d, want 2: %s", len(output), restored)
	}
	if output[0].Get("type").String() != "message" || output[0].Get("id").String() != "msg-1" {
		t.Fatalf("unrelated completion item changed: %s", output[0].Raw)
	}
	if output[1].Get("type").String() != "custom_tool_call" || output[1].Get("call_id").String() != "call-1" {
		t.Fatalf("conflicting tool call was not reconciled: %s", output[1].Raw)
	}
	if input := output[1].Get("input"); input.Type != gjson.String || input.String() != "pwd" {
		t.Fatalf("reconciled custom tool input = %s, want string pwd", input.Raw)
	}

	lastRequest := []byte(`{"model":"gpt-test","stream":true,"input":[{"type":"message","id":"user-1","role":"user","content":"run pwd"}]}`)
	nextRequest := []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"custom_tool_call_output","call_id":"call-1","output":"ok"}]}`)
	completedOutput := []byte(gjson.GetBytes(restored, "response.output").Raw)
	normalized, _, errMsg := normalizeResponsesWebsocketRequestWithIncrementalState(
		nextRequest,
		lastRequest,
		completedOutput,
		"resp-1",
		[]string{"call-1"},
		false,
		false,
	)
	if errMsg != nil {
		t.Fatalf("normalize next request: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id must not be forwarded to HTTP/SSE upstream: %s", normalized)
	}
	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 4 {
		t.Fatalf("replayed input len = %d, want 4: %s", len(input), normalized)
	}
	if input[2].Get("type").String() != "custom_tool_call" || input[2].Get("input").String() != "pwd" {
		t.Fatalf("replayed tool call is invalid: %s", input[2].Raw)
	}
	if input[3].Get("type").String() != "custom_tool_call_output" || input[3].Get("call_id").String() != "call-1" {
		t.Fatalf("replayed tool output is invalid: %s", input[3].Raw)
	}

	cache := newWebsocketToolOutputCache(time.Minute, 10)
	donePayload := []byte(`{"type":"response.output_item.done","item":{"type":"custom_tool_call","id":"ctc-1","call_id":"call-1","name":"exec","input":"pwd","status":"completed"}}`)
	recordResponsesWebsocketToolCallsFromPayloadWithCache(cache, "session-1", donePayload)
	recordResponsesWebsocketToolCallsFromPayloadWithCache(cache, "session-1", restored)
	cached, ok := cache.get("session-1", "call-1")
	if !ok {
		t.Fatalf("reconciled custom tool call was not cached")
	}
	if gjson.GetBytes(cached, "type").String() != "custom_tool_call" || gjson.GetBytes(cached, "input").String() != "pwd" {
		t.Fatalf("cached tool call is invalid: %s", cached)
	}
}

func TestRestoreResponsesWebsocketCompletionOutputIgnoresIncompleteCollectedToolCall(t *testing.T) {
	payload := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[{"type":"function_call","call_id":"call-1","name":"exec"}]}}`)
	collector := map[int64][]byte{0: []byte(`{"type":"custom_tool_call","call_id":"call-1","name":"exec"}`)}

	restored := restoreResponsesWebsocketCompletionOutput(payload, collector, nil)
	if string(restored) != string(payload) {
		t.Fatalf("incomplete collected tool call overwrote completion output: %s", restored)
	}
}

func TestIsCompleteResponsesWebsocketToolCallRequiresStringFields(t *testing.T) {
	tests := []struct {
		name string
		item string
		want bool
	}{
		{name: "numeric call id", item: `{"type":"function_call","call_id":123,"name":"exec","arguments":"{}"}`},
		{name: "boolean name", item: `{"type":"function_call","call_id":"call-1","name":true,"arguments":"{}"}`},
		{name: "numeric arguments", item: `{"type":"function_call","call_id":"call-1","name":"exec","arguments":123}`},
		{name: "object custom input", item: `{"type":"custom_tool_call","call_id":"call-1","name":"exec","input":{}}`},
		{name: "valid function call", item: `{"type":"function_call","call_id":"call-1","name":"exec","arguments":""}`, want: true},
		{name: "valid custom tool call", item: `{"type":"custom_tool_call","call_id":"call-1","name":"exec","input":""}`, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isCompleteResponsesWebsocketToolCall(gjson.Parse(tt.item)); got != tt.want {
				t.Fatalf("isCompleteResponsesWebsocketToolCall() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestAppendWebsocketEvent(t *testing.T) {
	var builder strings.Builder

	appendWebsocketEvent(&builder, "request", []byte("  {\"type\":\"response.create\"}\n"))
	appendWebsocketEvent(&builder, "response", []byte("{\"type\":\"response.created\"}"))

	got := builder.String()
	if !strings.Contains(got, "websocket.request\n{\"type\":\"response.create\"}\n") {
		t.Fatalf("request event not found in body: %s", got)
	}
	if !strings.Contains(got, "websocket.response\n{\"type\":\"response.created\"}\n") {
		t.Fatalf("response event not found in body: %s", got)
	}
}

func TestAppendWebsocketTimelineEvent(t *testing.T) {
	var builder strings.Builder
	ts := time.Date(2026, time.April, 1, 12, 34, 56, 789000000, time.UTC)

	appendWebsocketTimelineEvent(&builder, "request", []byte("  {\"type\":\"response.create\"}\n"), ts)

	got := builder.String()
	if !strings.Contains(got, "Timestamp: 2026-04-01T12:34:56.789Z") {
		t.Fatalf("timeline timestamp not found: %s", got)
	}
	if !strings.Contains(got, "Event: websocket.request") {
		t.Fatalf("timeline event not found: %s", got)
	}
	if !strings.Contains(got, "{\"type\":\"response.create\"}") {
		t.Fatalf("timeline payload not found: %s", got)
	}
}

func TestLogResponsesWebsocketDownstreamErrorOmitsPayload(t *testing.T) {
	previousLevel := log.GetLevel()
	log.SetLevel(log.InfoLevel)
	hook := logtest.NewLocal(log.StandardLogger())
	t.Cleanup(func() {
		hook.Reset()
		log.SetLevel(previousLevel)
	})

	const sentinel = "client-prompt-cache-key"
	logResponsesWebsocketDownstreamError("session-1", []byte(`{"type":"error","status":500,"error":{"message":"`+sentinel+`"}}`))

	entries := hook.AllEntries()
	if len(entries) != 1 {
		t.Fatalf("log entry count = %d, want 1", len(entries))
	}
	entry := entries[0]
	if strings.Contains(entry.Message, sentinel) {
		t.Fatalf("log message leaked error payload: %q", entry.Message)
	}
	for key, value := range entry.Data {
		if strings.Contains(fmt.Sprint(value), sentinel) {
			t.Fatalf("log field %q leaked error payload: %v", key, value)
		}
	}
	if got := entry.Data["event"]; got != wsEventTypeError {
		t.Fatalf("event field = %v, want %s", got, wsEventTypeError)
	}
	if got := entry.Data["status"]; got != http.StatusInternalServerError {
		t.Fatalf("status field = %v, want %d", got, http.StatusInternalServerError)
	}
}

func TestSetWebsocketTimelineBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)

	setWebsocketTimelineBody(c, " \n ")
	if _, exists := c.Get(wsTimelineBodyKey); exists {
		t.Fatalf("timeline body key should not be set for empty body")
	}

	setWebsocketTimelineBody(c, "timeline body")
	value, exists := c.Get(wsTimelineBodyKey)
	if !exists {
		t.Fatalf("timeline body key not set")
	}
	bodyBytes, ok := value.([]byte)
	if !ok {
		t.Fatalf("timeline body key type mismatch")
	}
	if string(bodyBytes) != "timeline body" {
		t.Fatalf("timeline body = %q, want %q", string(bodyBytes), "timeline body")
	}
}

func TestWebsocketTimelineLogFallsBackToMemoryWithoutSource(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	ts := time.Date(2026, time.April, 1, 12, 34, 56, 789000000, time.UTC)

	timelineLog := newWebsocketTimelineLog(true, nil)
	timelineLog.BeginRequest()
	timelineLog.Append("request", []byte(`{"type":"response.create"}`), ts)
	timelineLog.SetContext(c)

	value, exists := c.Get(wsTimelineBodyKey)
	if !exists {
		t.Fatalf("timeline body key not set")
	}
	bodyBytes, ok := value.([]byte)
	if !ok {
		t.Fatalf("timeline body key type mismatch")
	}
	got := string(bodyBytes)
	if !strings.Contains(got, "Event: websocket.request") {
		t.Fatalf("timeline event not found: %s", got)
	}
	if !strings.Contains(got, `{"type":"response.create"}`) {
		t.Fatalf("timeline payload not found: %s", got)
	}
}

func TestRepairResponsesWebsocketToolCallsInsertsCachedOutput(t *testing.T) {
	cache := newWebsocketToolOutputCache(time.Minute, 10)
	sessionKey := "session-1"

	cacheWarm := []byte(`{"previous_response_id":"resp-1","input":[{"type":"function_call_output","call_id":"call-1","output":"ok"}]}`)
	warmed := repairResponsesWebsocketToolCallsWithCache(cache, sessionKey, cacheWarm)
	if gjson.GetBytes(warmed, "input.0.call_id").String() != "call-1" {
		t.Fatalf("expected warmup output to remain")
	}

	raw := []byte(`{"input":[{"type":"function_call","call_id":"call-1","name":"tool"},{"type":"message","id":"msg-1"}]}`)
	repaired := repairResponsesWebsocketToolCallsWithCache(cache, sessionKey, raw)

	input := gjson.GetBytes(repaired, "input").Array()
	if len(input) != 3 {
		t.Fatalf("repaired input len = %d, want 3", len(input))
	}
	if input[0].Get("type").String() != "function_call" || input[0].Get("call_id").String() != "call-1" {
		t.Fatalf("unexpected first item: %s", input[0].Raw)
	}
	if input[1].Get("type").String() != "function_call_output" || input[1].Get("call_id").String() != "call-1" {
		t.Fatalf("missing inserted output: %s", input[1].Raw)
	}
	if input[2].Get("type").String() != "message" || input[2].Get("id").String() != "msg-1" {
		t.Fatalf("unexpected trailing item: %s", input[2].Raw)
	}
}

func TestResponsesWebsocketToolCacheTurnCommitsOnlyOnSuccess(t *testing.T) {
	const sessionKey = "tool-cache-turn-commit-session"
	defer defaultWebsocketToolOutputCache.deleteSession(sessionKey)
	defer defaultWebsocketToolCallCache.deleteSession(sessionKey)

	turn := newResponsesWebsocketToolCacheTurn(sessionKey)
	turn.recordRequest([]byte(`{"input":[{"type":"function_call_output","id":"fco-1","call_id":"call-1","output":"cached result"}]}`))
	beforeCommit := repairResponsesWebsocketToolCallsWithoutRecording(sessionKey, []byte(`{"input":[{"type":"function_call","id":"fc-next","call_id":"call-1","name":"lookup","arguments":"{}"}]}`))
	if gjson.GetBytes(beforeCommit, "input.#").Int() != 0 {
		t.Fatalf("uncommitted turn populated global cache: %s", beforeCommit)
	}

	turn.commit()
	afterCommit := repairResponsesWebsocketToolCallsWithoutRecording(sessionKey, []byte(`{"input":[{"type":"function_call","id":"fc-next","call_id":"call-1","name":"lookup","arguments":"{}"}]}`))
	input := gjson.GetBytes(afterCommit, "input").Array()
	if len(input) != 2 || input[1].Get("output").String() != "cached result" {
		t.Fatalf("committed turn was not available to tool repair: %s", afterCommit)
	}
}

func TestResponsesWebsocketToolCacheRetainPreventsOverlappingReleaseDeletion(t *testing.T) {
	const sessionKey = "tool-cache-overlapping-retain-session"
	retainResponsesWebsocketToolCaches(sessionKey)
	retainResponsesWebsocketToolCaches(sessionKey)
	turn := newResponsesWebsocketToolCacheTurn(sessionKey)
	turn.recordRequest([]byte(`{"input":[{"type":"function_call_output","id":"fco-1","call_id":"call-1","output":"kept"}]}`))
	turn.commit()

	releaseResponsesWebsocketToolCaches(sessionKey)
	if _, ok := defaultWebsocketToolOutputCache.get(sessionKey, "call-1"); !ok {
		t.Fatal("first overlapping release deleted active session cache")
	}
	releaseResponsesWebsocketToolCaches(sessionKey)
	if _, ok := defaultWebsocketToolOutputCache.get(sessionKey, "call-1"); ok {
		t.Fatal("final release did not delete session cache")
	}
}

func TestRepairResponsesWebsocketToolCallsDropsOrphanFunctionCall(t *testing.T) {
	cache := newWebsocketToolOutputCache(time.Minute, 10)
	sessionKey := "session-1"

	raw := []byte(`{"input":[{"type":"function_call","call_id":"call-1","name":"tool"},{"type":"message","id":"msg-1"}]}`)
	repaired := repairResponsesWebsocketToolCallsWithCache(cache, sessionKey, raw)

	input := gjson.GetBytes(repaired, "input").Array()
	if len(input) != 1 {
		t.Fatalf("repaired input len = %d, want 1", len(input))
	}
	if input[0].Get("type").String() != "message" || input[0].Get("id").String() != "msg-1" {
		t.Fatalf("unexpected remaining item: %s", input[0].Raw)
	}
}

func TestRepairResponsesWebsocketToolCallsInsertsCachedCallForOrphanOutput(t *testing.T) {
	outputCache := newWebsocketToolOutputCache(time.Minute, 10)
	callCache := newWebsocketToolOutputCache(time.Minute, 10)
	sessionKey := "session-1"

	callCache.record(sessionKey, "call-1", []byte(`{"type":"function_call","call_id":"call-1","name":"tool"}`))

	raw := []byte(`{"input":[{"type":"function_call_output","call_id":"call-1","output":"ok"},{"type":"message","id":"msg-1"}]}`)
	repaired := repairResponsesWebsocketToolCallsWithCaches(outputCache, callCache, sessionKey, raw)

	input := gjson.GetBytes(repaired, "input").Array()
	if len(input) != 3 {
		t.Fatalf("repaired input len = %d, want 3", len(input))
	}
	if input[0].Get("type").String() != "function_call" || input[0].Get("call_id").String() != "call-1" {
		t.Fatalf("missing inserted call: %s", input[0].Raw)
	}
	if input[1].Get("type").String() != "function_call_output" || input[1].Get("call_id").String() != "call-1" {
		t.Fatalf("unexpected output item: %s", input[1].Raw)
	}
	if input[2].Get("type").String() != "message" || input[2].Get("id").String() != "msg-1" {
		t.Fatalf("unexpected trailing item: %s", input[2].Raw)
	}
}

func TestRepairResponsesWebsocketToolCallsKeepsPreviousResponseOutputIncremental(t *testing.T) {
	outputCache := newWebsocketToolOutputCache(time.Minute, 10)
	callCache := newWebsocketToolOutputCache(time.Minute, 10)
	sessionKey := "session-1"

	callCache.record(sessionKey, "call-1", []byte(`{"type":"function_call","id":"fc-1","call_id":"call-1","name":"tool"}`))

	raw := []byte(`{"previous_response_id":"resp-latest","input":[{"type":"function_call_output","call_id":"call-1","id":"tool-out-1","output":"ok"},{"type":"message","id":"msg-1"}]}`)
	repaired := repairResponsesWebsocketToolCallsWithCaches(outputCache, callCache, sessionKey, raw)

	if got := gjson.GetBytes(repaired, "previous_response_id").String(); got != "resp-latest" {
		t.Fatalf("previous_response_id = %q, want resp-latest", got)
	}
	input := gjson.GetBytes(repaired, "input").Array()
	if len(input) != 2 {
		t.Fatalf("repaired input len = %d, want 2: %s", len(input), repaired)
	}
	if input[0].Get("type").String() != "function_call_output" || input[0].Get("call_id").String() != "call-1" {
		t.Fatalf("unexpected output item: %s", input[0].Raw)
	}
	if input[1].Get("type").String() != "message" || input[1].Get("id").String() != "msg-1" {
		t.Fatalf("unexpected trailing item: %s", input[1].Raw)
	}
}

func TestRepairResponsesWebsocketToolCallsKeepsPreviousResponseCallIncremental(t *testing.T) {
	outputCache := newWebsocketToolOutputCache(time.Minute, 10)
	callCache := newWebsocketToolOutputCache(time.Minute, 10)
	sessionKey := "session-1"

	outputCache.record(sessionKey, "call-1", []byte(`{"type":"function_call_output","call_id":"call-1","id":"tool-out-1","output":"ok"}`))

	raw := []byte(`{"previous_response_id":"resp-latest","input":[{"type":"function_call","id":"fc-1","call_id":"call-1","name":"tool"},{"type":"message","id":"msg-1"}]}`)
	repaired := repairResponsesWebsocketToolCallsWithCaches(outputCache, callCache, sessionKey, raw)

	if got := gjson.GetBytes(repaired, "previous_response_id").String(); got != "resp-latest" {
		t.Fatalf("previous_response_id = %q, want resp-latest", got)
	}
	input := gjson.GetBytes(repaired, "input").Array()
	if len(input) != 2 {
		t.Fatalf("repaired input len = %d, want 2: %s", len(input), repaired)
	}
	if input[0].Get("type").String() != "function_call" || input[0].Get("call_id").String() != "call-1" {
		t.Fatalf("unexpected call item: %s", input[0].Raw)
	}
	if input[1].Get("type").String() != "message" || input[1].Get("id").String() != "msg-1" {
		t.Fatalf("unexpected trailing item: %s", input[1].Raw)
	}
}

func TestRepairResponsesWebsocketToolCallsDropsOrphanOutputWhenCallMissing(t *testing.T) {
	outputCache := newWebsocketToolOutputCache(time.Minute, 10)
	callCache := newWebsocketToolOutputCache(time.Minute, 10)
	sessionKey := "session-1"

	raw := []byte(`{"input":[{"type":"function_call_output","call_id":"call-1","output":"ok"},{"type":"message","id":"msg-1"}]}`)
	repaired := repairResponsesWebsocketToolCallsWithCaches(outputCache, callCache, sessionKey, raw)

	input := gjson.GetBytes(repaired, "input").Array()
	if len(input) != 1 {
		t.Fatalf("repaired input len = %d, want 1", len(input))
	}
	if input[0].Get("type").String() != "message" || input[0].Get("id").String() != "msg-1" {
		t.Fatalf("unexpected remaining item: %s", input[0].Raw)
	}
}

func TestRepairResponsesWebsocketToolCallsInsertsCachedCustomToolOutput(t *testing.T) {
	cache := newWebsocketToolOutputCache(time.Minute, 10)
	sessionKey := "session-1"

	cacheWarm := []byte(`{"previous_response_id":"resp-1","input":[{"type":"custom_tool_call_output","call_id":"call-1","output":"ok"}]}`)
	warmed := repairResponsesWebsocketToolCallsWithCache(cache, sessionKey, cacheWarm)
	if gjson.GetBytes(warmed, "input.0.call_id").String() != "call-1" {
		t.Fatalf("expected warmup output to remain")
	}

	raw := []byte(`{"input":[{"type":"custom_tool_call","call_id":"call-1","name":"apply_patch"},{"type":"message","id":"msg-1"}]}`)
	repaired := repairResponsesWebsocketToolCallsWithCache(cache, sessionKey, raw)

	input := gjson.GetBytes(repaired, "input").Array()
	if len(input) != 3 {
		t.Fatalf("repaired input len = %d, want 3", len(input))
	}
	if input[0].Get("type").String() != "custom_tool_call" || input[0].Get("call_id").String() != "call-1" {
		t.Fatalf("unexpected first item: %s", input[0].Raw)
	}
	if input[1].Get("type").String() != "custom_tool_call_output" || input[1].Get("call_id").String() != "call-1" {
		t.Fatalf("missing inserted output: %s", input[1].Raw)
	}
	if input[2].Get("type").String() != "message" || input[2].Get("id").String() != "msg-1" {
		t.Fatalf("unexpected trailing item: %s", input[2].Raw)
	}
}

func TestRepairResponsesWebsocketToolCallsDropsOrphanCustomToolCall(t *testing.T) {
	cache := newWebsocketToolOutputCache(time.Minute, 10)
	sessionKey := "session-1"

	raw := []byte(`{"input":[{"type":"custom_tool_call","call_id":"call-1","name":"apply_patch"},{"type":"message","id":"msg-1"}]}`)
	repaired := repairResponsesWebsocketToolCallsWithCache(cache, sessionKey, raw)

	input := gjson.GetBytes(repaired, "input").Array()
	if len(input) != 1 {
		t.Fatalf("repaired input len = %d, want 1", len(input))
	}
	if input[0].Get("type").String() != "message" || input[0].Get("id").String() != "msg-1" {
		t.Fatalf("unexpected remaining item: %s", input[0].Raw)
	}
}

func TestRepairResponsesWebsocketToolCallsInsertsCachedCustomToolCallForOrphanOutput(t *testing.T) {
	outputCache := newWebsocketToolOutputCache(time.Minute, 10)
	callCache := newWebsocketToolOutputCache(time.Minute, 10)
	sessionKey := "session-1"

	callCache.record(sessionKey, "call-1", []byte(`{"type":"custom_tool_call","call_id":"call-1","name":"apply_patch"}`))

	raw := []byte(`{"input":[{"type":"custom_tool_call_output","call_id":"call-1","output":"ok"},{"type":"message","id":"msg-1"}]}`)
	repaired := repairResponsesWebsocketToolCallsWithCaches(outputCache, callCache, sessionKey, raw)

	input := gjson.GetBytes(repaired, "input").Array()
	if len(input) != 3 {
		t.Fatalf("repaired input len = %d, want 3", len(input))
	}
	if input[0].Get("type").String() != "custom_tool_call" || input[0].Get("call_id").String() != "call-1" {
		t.Fatalf("missing inserted call: %s", input[0].Raw)
	}
	if input[1].Get("type").String() != "custom_tool_call_output" || input[1].Get("call_id").String() != "call-1" {
		t.Fatalf("unexpected output item: %s", input[1].Raw)
	}
	if input[2].Get("type").String() != "message" || input[2].Get("id").String() != "msg-1" {
		t.Fatalf("unexpected trailing item: %s", input[2].Raw)
	}
}

func TestRepairResponsesWebsocketToolCallsKeepsPreviousResponseCustomToolOutputIncremental(t *testing.T) {
	outputCache := newWebsocketToolOutputCache(time.Minute, 10)
	callCache := newWebsocketToolOutputCache(time.Minute, 10)
	sessionKey := "session-1"

	callCache.record(sessionKey, "call-1", []byte(`{"type":"custom_tool_call","call_id":"call-1","name":"apply_patch"}`))

	raw := []byte(`{"previous_response_id":"resp-latest","input":[{"type":"custom_tool_call_output","call_id":"call-1","output":"ok"},{"type":"message","id":"msg-1"}]}`)
	repaired := repairResponsesWebsocketToolCallsWithCaches(outputCache, callCache, sessionKey, raw)

	if got := gjson.GetBytes(repaired, "previous_response_id").String(); got != "resp-latest" {
		t.Fatalf("previous_response_id = %q, want resp-latest", got)
	}
	input := gjson.GetBytes(repaired, "input").Array()
	if len(input) != 2 {
		t.Fatalf("repaired input len = %d, want 2: %s", len(input), repaired)
	}
	if input[0].Get("type").String() != "custom_tool_call_output" || input[0].Get("call_id").String() != "call-1" {
		t.Fatalf("unexpected output item: %s", input[0].Raw)
	}
	if input[1].Get("type").String() != "message" || input[1].Get("id").String() != "msg-1" {
		t.Fatalf("unexpected trailing item: %s", input[1].Raw)
	}
}

func TestRepairResponsesWebsocketToolCallsDropsOrphanCustomToolOutputWhenCallMissing(t *testing.T) {
	outputCache := newWebsocketToolOutputCache(time.Minute, 10)
	callCache := newWebsocketToolOutputCache(time.Minute, 10)
	sessionKey := "session-1"

	raw := []byte(`{"input":[{"type":"custom_tool_call_output","call_id":"call-1","output":"ok"},{"type":"message","id":"msg-1"}]}`)
	repaired := repairResponsesWebsocketToolCallsWithCaches(outputCache, callCache, sessionKey, raw)

	input := gjson.GetBytes(repaired, "input").Array()
	if len(input) != 1 {
		t.Fatalf("repaired input len = %d, want 1", len(input))
	}
	if input[0].Get("type").String() != "message" || input[0].Get("id").String() != "msg-1" {
		t.Fatalf("unexpected remaining item: %s", input[0].Raw)
	}
}

func TestRecordResponsesWebsocketToolCallsIgnoresIncompleteCall(t *testing.T) {
	cache := newWebsocketToolOutputCache(time.Minute, 10)
	pending := make(map[string]struct{})
	payload := []byte(`{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"exec"}}`)

	recordResponsesWebsocketToolCallsFromPayloadWithCache(cache, "session-1", payload)
	recordPendingToolCallIDsFromPayload(pending, payload)

	if cached, ok := cache.get("session-1", "call-1"); ok {
		t.Fatalf("incomplete tool call was cached: %s", cached)
	}
	if len(pending) != 0 {
		t.Fatalf("incomplete tool call was recorded as pending: %v", pending)
	}
}

func TestRecordResponsesWebsocketToolCallsFromPayloadWithCache(t *testing.T) {
	cache := newWebsocketToolOutputCache(time.Minute, 10)
	sessionKey := "session-1"

	payload := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[{"type":"function_call","id":"fc-1","call_id":"call-1","name":"tool","arguments":"{}"}]}}`)
	recordResponsesWebsocketToolCallsFromPayloadWithCache(cache, sessionKey, payload)

	cached, ok := cache.get(sessionKey, "call-1")
	if !ok {
		t.Fatalf("expected cached tool call")
	}
	if gjson.GetBytes(cached, "type").String() != "function_call" || gjson.GetBytes(cached, "call_id").String() != "call-1" {
		t.Fatalf("unexpected cached tool call: %s", cached)
	}
}

func TestRecordResponsesWebsocketCustomToolCallsFromCompletedPayloadWithCache(t *testing.T) {
	cache := newWebsocketToolOutputCache(time.Minute, 10)
	sessionKey := "session-1"

	payload := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[{"type":"custom_tool_call","id":"ctc-1","call_id":"call-1","name":"apply_patch","input":"*** Begin Patch"}]}}`)
	recordResponsesWebsocketToolCallsFromPayloadWithCache(cache, sessionKey, payload)

	cached, ok := cache.get(sessionKey, "call-1")
	if !ok {
		t.Fatalf("expected cached custom tool call")
	}
	if gjson.GetBytes(cached, "type").String() != "custom_tool_call" || gjson.GetBytes(cached, "call_id").String() != "call-1" {
		t.Fatalf("unexpected cached custom tool call: %s", cached)
	}
}

func TestRecordResponsesWebsocketCustomToolCallsFromOutputItemDoneWithCache(t *testing.T) {
	cache := newWebsocketToolOutputCache(time.Minute, 10)
	sessionKey := "session-1"

	payload := []byte(`{"type":"response.output_item.done","item":{"type":"custom_tool_call","id":"ctc-1","call_id":"call-1","name":"apply_patch","input":"*** Begin Patch"}}`)
	recordResponsesWebsocketToolCallsFromPayloadWithCache(cache, sessionKey, payload)

	cached, ok := cache.get(sessionKey, "call-1")
	if !ok {
		t.Fatalf("expected cached custom tool call")
	}
	if gjson.GetBytes(cached, "type").String() != "custom_tool_call" || gjson.GetBytes(cached, "call_id").String() != "call-1" {
		t.Fatalf("unexpected cached custom tool call: %s", cached)
	}
}

func TestForwardResponsesWebsocketRestoresAndForwardsCompletedOutput(t *testing.T) {
	gin.SetMode(gin.TestMode)

	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() {
			errClose := conn.Close()
			if errClose != nil {
				serverErrCh <- errClose
			}
		}()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte, 2)
		errCh := make(chan *interfaces.ErrorMessage)
		data <- []byte(`{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"call-1","call_id":"call-1","name":"lookup","arguments":"{}"}}`)
		data <- []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[]}}\n\n")
		close(data)
		close(errCh)

		timelineLog := newInMemoryWebsocketTimelineLog()
		completedOutput, completedResponseID, pendingToolCallIDs, errMsg, _, err := (*OpenAIResponsesAPIHandler)(nil).forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			timelineLog,
			"session-1",
		)
		if err != nil {
			serverErrCh <- err
			return
		}
		if errMsg != nil {
			serverErrCh <- fmt.Errorf("unexpected websocket error message: %v", errMsg.Error)
			return
		}
		if gjson.GetBytes(completedOutput, "0.id").String() != "call-1" {
			serverErrCh <- errors.New("completed output not restored")
			return
		}
		if completedResponseID != "resp-1" {
			serverErrCh <- fmt.Errorf("completed response id = %q, want resp-1", completedResponseID)
			return
		}
		if len(pendingToolCallIDs) != 1 || pendingToolCallIDs[0] != "call-1" {
			serverErrCh <- fmt.Errorf("pending tool call ids = %v, want [call-1]", pendingToolCallIDs)
			return
		}
		if !strings.Contains(timelineLog.String(), "Event: websocket.response") {
			serverErrCh <- errors.New("websocket timeline did not capture downstream response")
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		errClose := conn.Close()
		if errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	_, outputItemPayload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read output item websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(outputItemPayload, "type").String(); got != "response.output_item.done" {
		t.Fatalf("output item payload type = %s, want response.output_item.done", got)
	}

	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read completion websocket message: %v", errReadMessage)
	}
	if gjson.GetBytes(payload, "type").String() != wsEventTypeCompleted {
		t.Fatalf("payload type = %s, want %s", gjson.GetBytes(payload, "type").String(), wsEventTypeCompleted)
	}
	if strings.Contains(string(payload), "response.done") {
		t.Fatalf("payload unexpectedly rewrote completed event: %s", payload)
	}
	if got := gjson.GetBytes(payload, "response.output.0.id").String(); got != "call-1" {
		t.Fatalf("downstream completion output id = %q, want call-1; payload=%s", got, payload)
	}

	if errServer := receiveResponsesWebsocketTestError(t, serverErrCh); errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
}

func TestForwardResponsesWebsocketTreatsResponseDoneAsTerminalWithoutRewriting(t *testing.T) {
	gin.SetMode(gin.TestMode)

	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() {
			errClose := conn.Close()
			if errClose != nil {
				serverErrCh <- errClose
			}
		}()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte, 1)
		errCh := make(chan *interfaces.ErrorMessage)
		data <- []byte(`{"type":"response.done","response":{"id":"resp-1","output":[{"type":"message","id":"out-1"}]}}`)
		close(data)
		close(errCh)

		timelineLog := newInMemoryWebsocketTimelineLog()
		completedOutput, completedResponseID, pendingToolCallIDs, errMsg, _, err := (*OpenAIResponsesAPIHandler)(nil).forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			timelineLog,
			"session-1",
		)
		if err != nil {
			serverErrCh <- err
			return
		}
		if errMsg != nil {
			serverErrCh <- fmt.Errorf("unexpected websocket error message: %v", errMsg.Error)
			return
		}
		if gjson.GetBytes(completedOutput, "0.id").String() != "out-1" {
			serverErrCh <- errors.New("done output not captured")
			return
		}
		if completedResponseID != "resp-1" {
			serverErrCh <- fmt.Errorf("completed response id = %q, want resp-1", completedResponseID)
			return
		}
		if len(pendingToolCallIDs) != 0 {
			serverErrCh <- fmt.Errorf("pending tool call ids = %v, want empty", pendingToolCallIDs)
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		errClose := conn.Close()
		if errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	_, payload, errReadMessage := conn.ReadMessage()
	if errReadMessage != nil {
		t.Fatalf("read websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != "response.done" {
		t.Fatalf("payload type = %s, want response.done; payload=%s", got, payload)
	}

	if errServer := <-serverErrCh; errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
}

func TestForwardResponsesWebsocketTreatsErrorPayloadAsTerminal(t *testing.T) {
	gin.SetMode(gin.TestMode)

	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() {
			errClose := conn.Close()
			if errClose != nil {
				serverErrCh <- errClose
			}
		}()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte, 1)
		errCh := make(chan *interfaces.ErrorMessage)
		data <- []byte(`{"type":"error","status":429,"error":{"message":"upstream failed"}}`)
		close(data)
		close(errCh)

		_, _, _, errMsg, _, err := (*OpenAIResponsesAPIHandler)(nil).forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			newInMemoryWebsocketTimelineLog(),
			"session-1",
		)
		if err != nil {
			serverErrCh <- err
			return
		}
		if errMsg == nil {
			serverErrCh <- errors.New("expected websocket error message")
			return
		}
		if errMsg.StatusCode != http.StatusTooManyRequests {
			serverErrCh <- fmt.Errorf("websocket error status = %d, want %d", errMsg.StatusCode, http.StatusTooManyRequests)
			return
		}
		if errMsg.Error == nil || !strings.Contains(errMsg.Error.Error(), "upstream failed") {
			serverErrCh <- fmt.Errorf("websocket error = %v, want upstream failed", errMsg.Error)
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		errClose := conn.Close()
		if errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	_, payload, errReadMessage := conn.ReadMessage()
	if errReadMessage != nil {
		t.Fatalf("read websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeError {
		t.Fatalf("payload type = %s, want %s; payload=%s", got, wsEventTypeError, payload)
	}

	if errServer := <-serverErrCh; errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
}

func TestRecordPendingToolCallIDsFromPayloadDropsSatisfiedCalls(t *testing.T) {
	pending := map[string]struct{}{}
	payload := []byte(`{"type":"response.completed","response":{"output":[{"type":"function_call","call_id":"call-1","id":"fc-1"},{"type":"function_call_output","call_id":"call-1","id":"out-1"},{"type":"custom_tool_call","call_id":"call-2","id":"ctc-1"},{"type":"custom_tool_call_output","call_id":"call-2","id":"custom-out-1"}]}}`)

	recordPendingToolCallIDsFromPayload(pending, payload)

	if len(pending) != 0 {
		t.Fatalf("pending tool call ids = %v, want empty", sortedStringSet(pending))
	}
}

func TestForwardResponsesWebsocketLogsAttemptedResponseOnWriteFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte, 1)
		errCh := make(chan *interfaces.ErrorMessage)
		data <- []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[{\"type\":\"message\",\"id\":\"out-1\"}]}}\n\n")
		close(data)
		close(errCh)

		timelineLog := newInMemoryWebsocketTimelineLog()
		if errClose := conn.Close(); errClose != nil {
			serverErrCh <- errClose
			return
		}

		_, _, _, _, _, err = (*OpenAIResponsesAPIHandler)(nil).forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			timelineLog,
			"session-1",
		)
		if err == nil {
			serverErrCh <- errors.New("expected websocket write failure")
			return
		}
		if !strings.Contains(timelineLog.String(), "Event: websocket.response") {
			serverErrCh <- errors.New("websocket timeline did not capture attempted downstream response")
			return
		}
		if !strings.Contains(timelineLog.String(), "\"type\":\"response.completed\"") {
			serverErrCh <- errors.New("websocket timeline did not retain attempted payload")
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	if errServer := receiveResponsesWebsocketTestError(t, serverErrCh); errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
}

func TestForwardResponsesWebsocketDoesNotSuppressPreviousResponseErrorAfterPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() {
			errClose := conn.Close()
			if errClose != nil {
				serverErrCh <- errClose
			}
		}()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte)
		errCh := make(chan *interfaces.ErrorMessage, 1)
		go func() {
			data <- []byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"id\":\"out-partial\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"partial\"}]},\"output_index\":0}\n\n")
			errCh <- &interfaces.ErrorMessage{
				StatusCode: http.StatusBadRequest,
				Error: websocketPinnedFailoverStatusError{
					status: http.StatusBadRequest,
					msg:    `{"error":{"message":"previous response with id 'resp-1' not found","type":"invalid_request_error","code":"previous_response_not_found"}}`,
				},
				Addon: http.Header{"x-request-id": []string{"req-previous-response"}},
			}
		}()

		_, _, _, errMsg, _, err := h.forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			newInMemoryWebsocketTimelineLog(),
			"session-1",
			responsesWebsocketForwardOptions{allowTranscriptReplayBeforeOutput: true},
		)
		if err != nil {
			serverErrCh <- err
			return
		}
		if errMsg == nil {
			serverErrCh <- errors.New("expected previous response error after forwarded payload")
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read output item websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != "response.output_item.done" {
		t.Fatalf("first payload type = %s, want response.output_item.done: %s", got, payload)
	}

	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read error websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeError {
		t.Fatalf("second payload type = %s, want %s: %s", got, wsEventTypeError, payload)
	}
	if got := gjson.GetBytes(payload, "status").Int(); got != http.StatusBadRequest {
		t.Fatalf("error status = %d, want %d: %s", got, http.StatusBadRequest, payload)
	}
	if got := gjson.GetBytes(payload, "error.code").String(); got != "previous_response_not_found" {
		t.Fatalf("error code = %s, want previous_response_not_found: %s", got, payload)
	}
	if got := gjson.GetBytes(payload, "headers.x-request-id").String(); got != "req-previous-response" {
		t.Fatalf("error header x-request-id = %s, want req-previous-response: %s", got, payload)
	}
	if !strings.Contains(string(payload), "previous_response_not_found") {
		t.Fatalf("error payload missing previous_response_not_found: %s", payload)
	}

	if errServer := receiveResponsesWebsocketTestError(t, serverErrCh); errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
}

func TestForwardResponsesWebsocketPrioritizesPendingErrorWhenDataCloses(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.Close() }()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte)
		errCh := make(chan *interfaces.ErrorMessage, 1)
		errCh <- &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error: websocketPinnedFailoverStatusError{
				status: http.StatusBadRequest,
				msg:    `{"error":{"message":"previous response with id 'resp-1' not found","type":"invalid_request_error","code":"previous_response_not_found"}}`,
			},
		}
		close(errCh)
		close(data)

		_, _, _, errMsg, replayAllowed, err := h.forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			newInMemoryWebsocketTimelineLog(),
			"session-1",
			responsesWebsocketForwardOptions{allowTranscriptReplayBeforeOutput: true},
		)
		if err != nil {
			serverErrCh <- err
			return
		}
		if errMsg == nil {
			serverErrCh <- errors.New("expected pending previous_response_not_found error")
			return
		}
		if !replayAllowed {
			serverErrCh <- errors.New("expected pending error to allow transcript replay")
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if errServer := receiveResponsesWebsocketTestError(t, serverErrCh); errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
}

func TestForwardResponsesWebsocketReplaysConnectionLimitBeforeOutput(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.Close() }()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte)
		errCh := make(chan *interfaces.ErrorMessage, 1)
		errCh <- &interfaces.ErrorMessage{
			StatusCode: http.StatusTooManyRequests,
			Error:      errors.New(`{"error":{"code":"websocket_connection_limit_reached","message":"too many websockets"}}`),
		}
		close(errCh)
		close(data)

		_, _, _, errMsg, replayAllowed, err := h.forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			newInMemoryWebsocketTimelineLog(),
			"session-1",
			responsesWebsocketForwardOptions{allowTranscriptReplayBeforeOutput: true},
		)
		if err != nil {
			serverErrCh <- err
			return
		}
		if errMsg == nil {
			serverErrCh <- errors.New("expected websocket connection limit error")
			return
		}
		if !replayAllowed {
			serverErrCh <- errors.New("expected websocket connection limit to allow transcript replay")
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if errServer := receiveResponsesWebsocketTestError(t, serverErrCh); errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
}

func TestForwardResponsesWebsocketDoesNotWaitForFinalErrorAfterDataCloses(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.Close() }()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte)
		errCh := make(chan *interfaces.ErrorMessage)
		close(data)

		_, _, _, errMsg, replayAllowed, err := h.forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			newInMemoryWebsocketTimelineLog(),
			"session-1",
			responsesWebsocketForwardOptions{allowTranscriptReplayBeforeOutput: true},
		)
		if err != nil {
			serverErrCh <- err
			return
		}
		if errMsg == nil || errMsg.Error == nil || !strings.Contains(errMsg.Error.Error(), "stream closed before response.completed") {
			serverErrCh <- fmt.Errorf("expected stream closed error, got %v", errMsg)
			return
		}
		if replayAllowed {
			serverErrCh <- errors.New("stream closed error must not allow transcript replay")
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read stream-closed websocket error: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeError {
		t.Fatalf("payload type = %s, want %s: %s", got, wsEventTypeError, payload)
	}
	if !strings.Contains(string(payload), "stream closed before response.completed") {
		t.Fatalf("payload missing stream closed error: %s", payload)
	}

	if errServer := receiveResponsesWebsocketTestError(t, serverErrCh); errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
}

func TestForwardResponsesWebsocketIgnoresErrorAfterCompleted(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.Close() }()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte)
		errCh := make(chan *interfaces.ErrorMessage, 1)
		go func() {
			data <- []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[{\"type\":\"message\",\"id\":\"out-1\"}]}}\n\n")
			errCh <- &interfaces.ErrorMessage{
				StatusCode: http.StatusBadRequest,
				Error:      errors.New("late upstream error"),
			}
			close(data)
			close(errCh)
		}()

		completedOutput, _, _, errMsg, replayAllowed, err := h.forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			newInMemoryWebsocketTimelineLog(),
			"session-1",
			responsesWebsocketForwardOptions{allowTranscriptReplayBeforeOutput: true},
		)
		if err != nil {
			serverErrCh <- err
			return
		}
		if errMsg != nil {
			serverErrCh <- fmt.Errorf("late error after completed was not ignored: %v", errMsg)
			return
		}
		if replayAllowed {
			serverErrCh <- errors.New("completed response must not allow transcript replay")
			return
		}
		if got := gjson.GetBytes(completedOutput, "0.id").String(); got != "out-1" {
			serverErrCh <- fmt.Errorf("completed output id = %s, want out-1", got)
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read completed websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}
	_, _, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage == nil {
		t.Fatal("expected websocket close after completed, got extra message")
	}

	if errServer := receiveResponsesWebsocketTestError(t, serverErrCh); errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
}

func TestForwardResponsesWebsocketDoesNotReplayAfterCreatedEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.Close() }()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte)
		errCh := make(chan *interfaces.ErrorMessage, 1)
		go func() {
			data <- []byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\"}}\n\n")
			errCh <- &interfaces.ErrorMessage{
				StatusCode: http.StatusBadRequest,
				Error: websocketPinnedFailoverStatusError{
					status: http.StatusBadRequest,
					msg:    `{"error":{"message":"previous response with id 'resp-1' not found","type":"invalid_request_error","code":"previous_response_not_found"}}`,
				},
			}
			close(data)
			close(errCh)
		}()

		_, _, _, errMsg, replayAllowed, err := h.forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			newInMemoryWebsocketTimelineLog(),
			"session-1",
			responsesWebsocketForwardOptions{allowTranscriptReplayBeforeOutput: true},
		)
		if err != nil {
			serverErrCh <- err
			return
		}
		if errMsg == nil || !shouldRetryResponsesWebsocketTranscriptReplay(errMsg) {
			serverErrCh <- fmt.Errorf("expected previous_response_not_found replay error, got %v", errMsg)
			return
		}
		if replayAllowed {
			serverErrCh <- errors.New("forwarded response.created must preclude transcript replay")
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read created websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != "response.created" {
		t.Fatalf("payload type = %s, want response.created: %s", got, payload)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read terminal websocket error: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeError {
		t.Fatalf("payload type = %s, want %s: %s", got, wsEventTypeError, payload)
	}

	if errServer := receiveResponsesWebsocketTestError(t, serverErrCh); errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
}

func TestForwardResponsesWebsocketTreatsResponseFailedAsTerminal(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.Close() }()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte, 1)
		errCh := make(chan *interfaces.ErrorMessage)
		data <- []byte(`data: {"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"code":"context_length_exceeded","message":"context too large"}}}` + "\n\n")
		close(data)
		close(errCh)

		_, _, _, errMsg, replayAllowed, errForward := h.forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			newInMemoryWebsocketTimelineLog(),
			"session-1",
			responsesWebsocketForwardOptions{allowTranscriptReplayBeforeOutput: true},
		)
		if errForward != nil {
			serverErrCh <- errForward
			return
		}
		if replayAllowed {
			serverErrCh <- errors.New("non-replayable response.failed must not allow transcript replay")
			return
		}
		if errMsg == nil || errMsg.Error == nil || !strings.Contains(errMsg.Error.Error(), "context too large") {
			serverErrCh <- fmt.Errorf("response.failed error = %v, want context too large", errMsg)
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read response.failed websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeFailed {
		t.Fatalf("payload type = %s, want %s: %s", got, wsEventTypeFailed, payload)
	}

	if errServer := receiveResponsesWebsocketTestError(t, serverErrCh); errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
}

func TestForwardResponsesWebsocketTreatsResponseIncompleteAsTerminalAfterPartialOutput(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
	serverErrCh := make(chan error, 1)
	incompletePayload := []byte(`{"type":"response.incomplete","sequence_number":3,"response":{"id":"resp-1","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.Close() }()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte, 3)
		errCh := make(chan *interfaces.ErrorMessage)
		data <- []byte(`{"type":"response.created","response":{"id":"resp-1"}}`)
		data <- []byte(`{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"partial"}`)
		data <- incompletePayload

		_, _, _, errMsg, replayAllowed, errForward := h.forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			newInMemoryWebsocketTimelineLog(),
			"session-1",
			responsesWebsocketForwardOptions{allowTranscriptReplayBeforeOutput: true},
		)
		close(data)
		close(errCh)
		if errForward != nil {
			serverErrCh <- errForward
			return
		}
		if replayAllowed {
			serverErrCh <- errors.New("response.incomplete after partial output must not allow transcript replay")
			return
		}
		if errMsg == nil || errMsg.StatusCode != http.StatusBadRequest || errMsg.Error == nil ||
			errMsg.Error.Error() != "Incomplete response returned, reason: max_output_tokens" {
			serverErrCh <- fmt.Errorf("response.incomplete error = %#v", errMsg)
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, wantType := range []string{"response.created", "response.output_text.delta", "response.incomplete"} {
		_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
		if errReadMessage != nil {
			t.Fatalf("read %s websocket message: %v", wantType, errReadMessage)
		}
		if got := gjson.GetBytes(payload, "type").String(); got != wantType {
			t.Fatalf("payload type = %s, want %s: %s", got, wantType, payload)
		}
		if wantType == "response.incomplete" && !bytes.Equal(payload, incompletePayload) {
			t.Fatalf("response.incomplete payload changed: got %s want %s", payload, incompletePayload)
		}
	}

	if errServer := receiveResponsesWebsocketTestError(t, serverErrCh); errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
	if _, _, errReadMessage := readResponsesWebsocketTestMessage(t, conn); errReadMessage == nil {
		t.Fatal("unexpected websocket message after response.incomplete")
	}
}

func TestForwardResponsesWebsocketReplaysResponseFailedBeforeOutput(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.Close() }()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte, 1)
		errCh := make(chan *interfaces.ErrorMessage)
		data <- []byte(`data: {"type":"response.failed","response":{"id":"resp-1","status":"failed","error":{"message":"No response found for previous_response_id resp-old"}}}` + "\n\n")
		close(data)
		close(errCh)

		_, _, _, errMsg, replayAllowed, errForward := h.forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			newInMemoryWebsocketTimelineLog(),
			"session-1",
			responsesWebsocketForwardOptions{allowTranscriptReplayBeforeOutput: true},
		)
		if errForward != nil {
			serverErrCh <- errForward
			return
		}
		if errMsg == nil || !replayAllowed {
			serverErrCh <- fmt.Errorf("response.failed previous_response_id error should allow replay, err=%v replay=%t", errMsg, replayAllowed)
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if errServer := receiveResponsesWebsocketTestError(t, serverErrCh); errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
}

func TestForwardResponsesWebsocketDoesNotBridgeTurnStateBeforeReplayBoundary(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			serverErrCh <- errUpgrade
			return
		}
		defer func() { _ = conn.Close() }()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte, 1)
		errCh := make(chan *interfaces.ErrorMessage)
		data <- []byte("data: {\"type\":\"codex.rate_limits\",\"rate_limits\":[]}\n\n" +
			"data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp-1\",\"status\":\"failed\",\"error\":{\"message\":\"No response found for previous_response_id resp-old\"}}}\n\n")
		close(data)
		close(errCh)
		upstreamHeaders := http.Header{}
		upstreamHeaders.Set(wsTurnStateHeader, "state-1")

		_, _, _, errMsg, replayAllowed, errForward := h.forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			upstreamHeaders,
			newInMemoryWebsocketTimelineLog(),
			"session-1",
			responsesWebsocketForwardOptions{allowTranscriptReplayBeforeOutput: true},
		)
		if errForward != nil {
			serverErrCh <- errForward
			return
		}
		if errMsg == nil || !replayAllowed {
			serverErrCh <- fmt.Errorf("response.failed should allow replay after rate limits, err=%v replay=%t", errMsg, replayAllowed)
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
	if errDial != nil {
		t.Fatalf("dial websocket: %v", errDial)
	}
	defer func() { _ = conn.Close() }()

	_, payload, errRead := readResponsesWebsocketTestMessage(t, conn)
	if errRead != nil {
		t.Fatalf("read rate limits payload: %v", errRead)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != "codex.rate_limits" {
		t.Fatalf("payload type = %s, want codex.rate_limits: %s", got, payload)
	}
	if errServer := receiveResponsesWebsocketTestError(t, serverErrCh); errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
	_, payload, errRead = readResponsesWebsocketTestMessage(t, conn)
	if errRead == nil {
		t.Fatalf("unexpected payload before replay: %s", payload)
	}
}

func TestForwardResponsesWebsocketUsesOutputItemDoneWhenCompletedOutputEmpty(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
	outputCh := make(chan []byte, 1)
	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.Close() }()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = r

		data := make(chan []byte, 4)
		errCh := make(chan *interfaces.ErrorMessage)
		data <- []byte(`data: {"type":"response.output_item.done","item":{"id":"rs-1","type":"reasoning","summary":[],"encrypted_content":"encrypted-reasoning"},"output_index":0}` + "\n\n")
		data <- []byte(`data: {"type":"response.output_item.done","item":{"id":"fc-1","type":"function_call","call_id":"call-1","name":"lookup","arguments":"{\"q\":\"weather\"}","status":"completed"},"output_index":1}` + "\n\n")
		data <- []byte(`data: {"type":"response.output_item.done","item":{"id":"msg-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]},"output_index":2}` + "\n\n")
		data <- []byte(`data: {"type":"response.completed","response":{"id":"resp-1","output":[]}}` + "\n\n")
		close(data)
		close(errCh)

		completedOutput, _, _, _, _, err := h.forwardResponsesWebsocket(
			ctx,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errCh,
			nil,
			newInMemoryWebsocketTimelineLog(),
			"session-1",
		)
		if err != nil {
			serverErrCh <- err
			return
		}
		outputCh <- completedOutput
		serverErrCh <- nil
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for i := 0; i < 4; i++ {
		if _, _, errRead := readResponsesWebsocketTestMessage(t, conn); errRead != nil {
			t.Fatalf("read websocket message %d: %v", i, errRead)
		}
	}

	if errServer := receiveResponsesWebsocketTestError(t, serverErrCh); errServer != nil {
		t.Fatalf("server error: %v", errServer)
	}
	completedOutput := receiveResponsesWebsocketTestBytes(t, outputCh)
	if got := gjson.GetBytes(completedOutput, "0.encrypted_content").String(); got != "encrypted-reasoning" {
		t.Fatalf("completed reasoning encrypted_content = %q, want encrypted-reasoning; output=%s", got, completedOutput)
	}
	if got := gjson.GetBytes(completedOutput, "1.call_id").String(); got != "call-1" {
		t.Fatalf("completed function call_id = %q, want call-1; output=%s", got, completedOutput)
	}
	if got := gjson.GetBytes(completedOutput, "2.content.0.text").String(); got != "ok" {
		t.Fatalf("completed output text = %q, want ok; output=%s", got, completedOutput)
	}
}

func TestResponsesWebsocketTimelineRecordsDisconnectEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)

	manager := coreauth.NewManager(nil, nil, nil)
	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	logsDir := t.TempDir()

	timelineCh := make(chan string, 1)
	router := gin.New()
	router.GET("/v1/responses/ws", func(c *gin.Context) {
		source, errSource := requestlogging.NewFileBodySourceInDir(logsDir, "websocket-timeline-test")
		if errSource != nil {
			timelineCh <- ""
			return
		}
		c.Set(requestlogging.WebsocketTimelineSourceContextKey, source)
		h.ResponsesWebsocket(c)
		timeline := ""
		if value, exists := c.Get(wsTimelineBodyKey); exists {
			if body, ok := value.([]byte); ok {
				timeline = string(body)
			}
		} else if value, exists := c.Get(requestlogging.WebsocketTimelineSourceContextKey); exists {
			if source, ok := value.(*requestlogging.FileBodySource); ok {
				body, _ := source.Bytes()
				timeline = string(body)
				_ = source.Cleanup()
			}
		}
		if value, exists := c.Get(requestlogging.APIWebsocketTimelineSourceContextKey); exists {
			if source, ok := value.(*requestlogging.FileBodySource); ok {
				_ = source.Cleanup()
			}
		}
		timelineCh <- timeline
	})

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}

	closePayload := websocket.FormatCloseMessage(websocket.CloseGoingAway, "client closing")
	if err = conn.WriteControl(websocket.CloseMessage, closePayload, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("write close control: %v", err)
	}
	_ = conn.Close()

	select {
	case timeline := <-timelineCh:
		if !strings.Contains(timeline, "Event: websocket.disconnect") {
			t.Fatalf("websocket timeline missing disconnect event: %s", timeline)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for websocket timeline")
	}
}

func TestResponsesWebsocketMirrorsUpstreamMessageTooBigDisconnect(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for _, provider := range []string{"codex", "xai"} {
		t.Run(provider, func(t *testing.T) {
			executor := &websocketUpstreamDisconnectExecutor{provider: provider, subscribed: make(chan string, 1)}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(executor)
			base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
			h := NewOpenAIResponsesAPIHandler(base)

			router := gin.New()
			router.GET("/v1/responses/ws", h.ResponsesWebsocket)
			server := httptest.NewServer(router)
			defer server.Close()

			wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if err != nil {
				t.Fatalf("dial websocket: %v", err)
			}
			defer func() { _ = conn.Close() }()

			var sessionID string
			select {
			case sessionID = <-executor.subscribed:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for upstream disconnect subscription")
			}

			executor.TriggerDisconnect(sessionID, &websocket.CloseError{
				Code: websocket.CloseMessageTooBig,
				Text: "message too big",
			})

			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, _, err = conn.ReadMessage()
			var closeErr *websocket.CloseError
			if !errors.As(err, &closeErr) {
				t.Fatalf("expected downstream websocket close error, got %v", err)
			}
			if closeErr.Code != websocket.CloseMessageTooBig {
				t.Fatalf("downstream close code = %d, want %d", closeErr.Code, websocket.CloseMessageTooBig)
			}
			if closeErr.Text != "message too big" {
				t.Fatalf("downstream close reason = %q, want message too big", closeErr.Text)
			}
		})
	}
}

func TestResponsesWebsocketHeartbeatSendsPing(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		startResponsesWebsocketHeartbeatWithInterval(conn, done, "test-heartbeat", 10*time.Millisecond)
		<-done
	}))
	defer server.Close()
	defer close(done)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pingCh := make(chan struct{}, 1)
	conn.SetPingHandler(func(appData string) error {
		select {
		case pingCh <- struct{}{}:
		default:
		}
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
	})

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}()

	select {
	case <-pingCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for heartbeat ping")
	}
	_ = conn.Close()
	<-readDone
}

func TestResponsesWebsocketHeartbeatDoesNotCloseWithoutPong(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		startResponsesWebsocketHeartbeatWithInterval(conn, done, "test-heartbeat-no-pong", 10*time.Millisecond)
		<-done
	}))
	defer server.Close()
	defer close(done)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pingCh := make(chan struct{}, 1)
	conn.SetPingHandler(func(string) error {
		select {
		case pingCh <- struct{}{}:
		default:
		}
		return nil
	})

	readErrCh := make(chan error, 1)
	go func() {
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				readErrCh <- errRead
				return
			}
		}
	}()

	select {
	case <-pingCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for heartbeat ping")
	}

	select {
	case errRead := <-readErrCh:
		t.Fatalf("downstream websocket closed without pong: %v", errRead)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestResponsesWebsocketCodexWebsocketPassthroughPassesCompactedRequestWithoutTranscriptMerge(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketDirectCaptureExecutor{done: make(chan struct{})}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-ws",
		Provider:   "codex",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	firstRequest := []byte(`{"type":"response.create","model":"test-model","input":[{"type":"message","role":"user","content":"first"}]}`)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if errWrite := conn.WriteMessage(websocket.TextMessage, firstRequest); errWrite != nil {
		t.Fatalf("write first websocket message: %v", errWrite)
	}
	if _, _, errRead := conn.ReadMessage(); errRead != nil {
		t.Fatalf("read first websocket response: %v", errRead)
	}

	compactedRequest := []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"compaction_summary","summary":"compressed history"},{"type":"message","role":"user","content":"after compaction"}]}`)
	if errWrite := conn.WriteMessage(websocket.TextMessage, compactedRequest); errWrite != nil {
		t.Fatalf("write compacted websocket message: %v", errWrite)
	}
	if _, _, errRead := conn.ReadMessage(); errRead != nil {
		t.Fatalf("read compacted websocket response: %v", errRead)
	}

	select {
	case <-executor.done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for websocket passthrough")
	}

	payloads := executor.Payloads()
	if len(payloads) != 2 {
		t.Fatalf("passthrough payload count = %d, want 2", len(payloads))
	}
	if got := gjson.GetBytes(payloads[0], "input").Raw; got != gjson.GetBytes(firstRequest, "input").Raw {
		t.Fatalf("first passthrough input = %s, want %s", got, gjson.GetBytes(firstRequest, "input").Raw)
	}
	if got := gjson.GetBytes(payloads[1], "input").Raw; got != gjson.GetBytes(compactedRequest, "input").Raw {
		t.Fatalf("compacted passthrough input = %s, want %s", got, gjson.GetBytes(compactedRequest, "input").Raw)
	}
	if gjson.GetBytes(payloads[1], "previous_response_id").Exists() {
		t.Fatalf("compacted passthrough previous_response_id leaked: %s", payloads[1])
	}
	if got := gjson.GetBytes(payloads[1], "model").String(); got != "test-model" {
		t.Fatalf("compacted passthrough model = %s, want test-model", got)
	}
	if bytes.Contains(payloads[1], []byte(`"content":"first"`)) || bytes.Contains(payloads[1], []byte(`"id":"out-1"`)) {
		t.Fatalf("compacted passthrough payload contains stale transcript state: %s", payloads[1])
	}
	authIDs := executor.AuthIDs()
	if len(authIDs) != 2 || authIDs[0] != "auth-ws" || authIDs[1] != "auth-ws" {
		t.Fatalf("passthrough auth IDs = %v, want [auth-ws auth-ws]", authIDs)
	}
}

func TestResponsesWebsocketCodexWebsocketPassthroughDoesNotRepairRawRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketDirectCaptureExecutor{done: make(chan struct{})}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-ws-raw",
		Provider:   "codex",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	firstRequest := []byte(`{"type":"response.create","model":"test-model","input":[{"type":"message","role":"user","content":"first"}]}`)
	if errWrite := conn.WriteMessage(websocket.TextMessage, firstRequest); errWrite != nil {
		t.Fatalf("write first websocket message: %v", errWrite)
	}
	if _, _, errRead := conn.ReadMessage(); errRead != nil {
		t.Fatalf("read first websocket response: %v", errRead)
	}

	rawRequest := []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"function_call_output","call_id":"missing-call","id":"orphan-output","output":"raw output"}]}`)
	if errWrite := conn.WriteMessage(websocket.TextMessage, rawRequest); errWrite != nil {
		t.Fatalf("write raw websocket message: %v", errWrite)
	}
	if _, _, errRead := conn.ReadMessage(); errRead != nil {
		t.Fatalf("read raw websocket response: %v", errRead)
	}

	select {
	case <-executor.done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for websocket passthrough")
	}

	payloads := executor.Payloads()
	if len(payloads) != 2 {
		t.Fatalf("passthrough payload count = %d, want 2", len(payloads))
	}
	secondPayload := payloads[1]
	if got := gjson.GetBytes(secondPayload, "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("raw passthrough previous_response_id = %s, want resp-1: %s", got, secondPayload)
	}
	input := gjson.GetBytes(secondPayload, "input").Array()
	if len(input) != 1 {
		t.Fatalf("raw passthrough input len = %d, want 1: %s", len(input), secondPayload)
	}
	if got := input[0].Get("id").String(); got != "orphan-output" {
		t.Fatalf("raw passthrough input id = %s, want orphan-output: %s", got, secondPayload)
	}
	authIDs := executor.AuthIDs()
	if len(authIDs) != 2 || authIDs[0] != "auth-ws-raw" || authIDs[1] != "auth-ws-raw" {
		t.Fatalf("passthrough auth IDs = %v, want [auth-ws-raw auth-ws-raw]", authIDs)
	}
}

func TestResponsesWebsocketXAIWebsocketPassthroughKeepsNativeIncrementalRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	modelName := "xai-websocket-passthrough-model"
	executor := &websocketDirectCaptureExecutor{provider: "xai", done: make(chan struct{})}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-xai-ws",
		Provider:   "xai",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: modelName}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	firstRequest := []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-1","role":"user","content":"first"}]}`, modelName))
	if errWrite := conn.WriteMessage(websocket.TextMessage, firstRequest); errWrite != nil {
		t.Fatalf("write first websocket message: %v", errWrite)
	}
	if _, _, errRead := conn.ReadMessage(); errRead != nil {
		t.Fatalf("read first websocket response: %v", errRead)
	}

	secondRequest := []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2","role":"user","content":"second"}]}`)
	if errWrite := conn.WriteMessage(websocket.TextMessage, secondRequest); errWrite != nil {
		t.Fatalf("write second websocket message: %v", errWrite)
	}
	if _, _, errRead := conn.ReadMessage(); errRead != nil {
		t.Fatalf("read second websocket response: %v", errRead)
	}

	select {
	case <-executor.done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for websocket passthrough")
	}

	payloads := executor.Payloads()
	if len(payloads) != 2 {
		t.Fatalf("xai websocket payload count = %d, want 2", len(payloads))
	}
	secondPayload := payloads[1]
	if got := gjson.GetBytes(secondPayload, "type").String(); got != wsRequestTypeCreate {
		t.Fatalf("incremental xai payload type = %q, want %q: %s", got, wsRequestTypeCreate, secondPayload)
	}
	if got := gjson.GetBytes(secondPayload, "model").String(); got != modelName {
		t.Fatalf("second xai payload model = %s, want %s", got, modelName)
	}
	if got := gjson.GetBytes(secondPayload, "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("second xai previous_response_id = %q, want resp-1: %s", got, secondPayload)
	}
	input := gjson.GetBytes(secondPayload, "input").Array()
	if len(input) != 1 || input[0].Get("id").String() != "msg-2" {
		t.Fatalf("second xai incremental input is not the client delta: %s", secondPayload)
	}
	authIDs := executor.AuthIDs()
	if len(authIDs) != 2 || authIDs[0] != "auth-xai-ws" || authIDs[1] != "auth-xai-ws" {
		t.Fatalf("xai websocket auth IDs = %v, want [auth-xai-ws auth-xai-ws]", authIDs)
	}
	if got := executor.RequiredUpstreamWebsocketFlags(); len(got) != 2 || got[0] || !got[1] {
		t.Fatalf("required upstream websocket flags = %v, want [false true]", got)
	}
}

func TestResponsesWebsocketFullRequestCanRouteFromNativeWebsocketToBuiltInProvider(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const sourceModel = "codex-provider-route-source"
	const targetModel = "claude-provider-route-target"
	codexExecutor := &websocketDirectCaptureExecutor{provider: "codex"}
	claudeExecutor := &websocketDirectCaptureExecutor{provider: "claude"}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(codexExecutor)
	manager.RegisterExecutor(claudeExecutor)
	codexAuth := &coreauth.Auth{
		ID:         "auth-codex-provider-route",
		Provider:   "codex",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	claudeAuth := &coreauth.Auth{
		ID:       "auth-claude-provider-route",
		Provider: "claude",
		Status:   coreauth.StatusActive,
	}
	for _, auth := range []*coreauth.Auth{codexAuth, claudeAuth} {
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("Register auth %s: %v", auth.ID, err)
		}
	}
	registry.GetGlobalRegistry().RegisterClient(codexAuth.ID, codexAuth.Provider, []*registry.ModelInfo{{ID: sourceModel}})
	registry.GetGlobalRegistry().RegisterClient(claudeAuth.ID, claudeAuth.Provider, []*registry.ModelInfo{{ID: targetModel}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(codexAuth.ID)
		registry.GetGlobalRegistry().UnregisterClient(claudeAuth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	base.SetModelRouterHost(&websocketProviderRouteHost{})
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	firstRequest := []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-1"}]}`, sourceModel))
	if errWrite := conn.WriteMessage(websocket.TextMessage, firstRequest); errWrite != nil {
		t.Fatalf("write first websocket message: %v", errWrite)
	}
	if _, _, errRead := conn.ReadMessage(); errRead != nil {
		t.Fatalf("read first websocket response: %v", errRead)
	}

	routedRequest := []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"route_to_claude":true,"input":[{"type":"message","id":"msg-routed"}]}`, sourceModel))
	if errWrite := conn.WriteMessage(websocket.TextMessage, routedRequest); errWrite != nil {
		t.Fatalf("write routed websocket message: %v", errWrite)
	}
	_, response, errRead := conn.ReadMessage()
	if errRead != nil {
		t.Fatalf("read routed websocket response: %v", errRead)
	}
	if got := gjson.GetBytes(response, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("routed response type = %q, want %q: %s", got, wsEventTypeCompleted, response)
	}
	if got := len(codexExecutor.Payloads()); got != 1 {
		t.Fatalf("codex payload count = %d, want 1", got)
	}
	claudePayloads := claudeExecutor.Payloads()
	if len(claudePayloads) != 1 {
		t.Fatalf("claude payload count = %d, want 1", len(claudePayloads))
	}
	if got := claudeExecutor.Models(); len(got) != 1 || got[0] != targetModel {
		t.Fatalf("routed models = %v, want [%s]", got, targetModel)
	}
}

func TestResponsesWebsocketFailedProviderRoutePreservesNativeWebsocketPin(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const sourceModel = "codex-provider-route-failure-source"
	const targetModel = "claude-provider-route-target"
	codexExecutor := &websocketDirectCaptureExecutor{provider: "codex"}
	claudeExecutor := &websocketDirectCaptureExecutor{provider: "claude", failStatus: http.StatusUnauthorized}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(codexExecutor)
	manager.RegisterExecutor(claudeExecutor)
	codexAuth := &coreauth.Auth{ID: "auth-codex-provider-route-failure", Provider: "codex", Status: coreauth.StatusActive, Attributes: map[string]string{"websockets": "true"}}
	claudeAuth := &coreauth.Auth{ID: "auth-claude-provider-route-failure", Provider: "claude", Status: coreauth.StatusActive}
	for _, auth := range []*coreauth.Auth{codexAuth, claudeAuth} {
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("Register auth %s: %v", auth.ID, err)
		}
	}
	registry.GetGlobalRegistry().RegisterClient(codexAuth.ID, codexAuth.Provider, []*registry.ModelInfo{{ID: sourceModel}})
	registry.GetGlobalRegistry().RegisterClient(claudeAuth.ID, claudeAuth.Provider, []*registry.ModelInfo{{ID: targetModel}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(codexAuth.ID)
		registry.GetGlobalRegistry().UnregisterClient(claudeAuth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	base.SetModelRouterHost(&websocketProviderRouteHost{})
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	requests := []string{
		fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-1"}]}`, sourceModel),
		fmt.Sprintf(`{"type":"response.create","model":%q,"route_to_claude":true,"input":[{"type":"message","id":"msg-routed"}]}`, sourceModel),
		`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`,
	}
	wantTypes := []string{wsEventTypeCompleted, wsEventTypeError, wsEventTypeCompleted}
	for i, request := range requests {
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(request)); errWrite != nil {
			t.Fatalf("write request %d: %v", i+1, errWrite)
		}
		_, response, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Fatalf("read response %d: %v", i+1, errRead)
		}
		if got := gjson.GetBytes(response, "type").String(); got != wantTypes[i] {
			t.Fatalf("response %d type = %q, want %q: %s", i+1, got, wantTypes[i], response)
		}
	}

	codexPayloads := codexExecutor.Payloads()
	if len(codexPayloads) != 2 {
		t.Fatalf("codex payload count = %d, want 2", len(codexPayloads))
	}
	if got := gjson.GetBytes(codexPayloads[1], "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("resumed codex previous_response_id = %q, want resp-1: %s", got, codexPayloads[1])
	}
	if got := len(claudeExecutor.Payloads()); got != 1 {
		t.Fatalf("claude payload count = %d, want 1", got)
	}
}

func TestResponsesWebsocketDeltaRouteToBuiltInProviderRequiresFullReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const sourceModel = "codex-provider-route-delta-source"
	const targetModel = "claude-provider-route-target"
	codexExecutor := &websocketDirectCaptureExecutor{provider: "codex"}
	claudeExecutor := &websocketDirectCaptureExecutor{provider: "claude"}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(codexExecutor)
	manager.RegisterExecutor(claudeExecutor)
	codexAuth := &coreauth.Auth{ID: "auth-codex-provider-route-delta", Provider: "codex", Status: coreauth.StatusActive, Attributes: map[string]string{"websockets": "true"}}
	claudeAuth := &coreauth.Auth{ID: "auth-claude-provider-route-delta", Provider: "claude", Status: coreauth.StatusActive}
	for _, auth := range []*coreauth.Auth{codexAuth, claudeAuth} {
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("Register auth %s: %v", auth.ID, err)
		}
	}
	registry.GetGlobalRegistry().RegisterClient(codexAuth.ID, codexAuth.Provider, []*registry.ModelInfo{{ID: sourceModel}})
	registry.GetGlobalRegistry().RegisterClient(claudeAuth.ID, claudeAuth.Provider, []*registry.ModelInfo{{ID: targetModel}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(codexAuth.ID)
		registry.GetGlobalRegistry().UnregisterClient(claudeAuth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	base.SetModelRouterHost(&websocketProviderRouteHost{})
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	firstRequest := []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-1"}]}`, sourceModel))
	if errWrite := conn.WriteMessage(websocket.TextMessage, firstRequest); errWrite != nil {
		t.Fatalf("write first websocket message: %v", errWrite)
	}
	if _, _, errRead := conn.ReadMessage(); errRead != nil {
		t.Fatalf("read first websocket response: %v", errRead)
	}

	routedDelta := []byte(`{"type":"response.create","route_to_claude":true,"previous_response_id":"resp-1","input":[{"type":"message","id":"msg-routed"}]}`)
	if errWrite := conn.WriteMessage(websocket.TextMessage, routedDelta); errWrite != nil {
		t.Fatalf("write routed delta: %v", errWrite)
	}
	_, _, errRead := conn.ReadMessage()
	var closeErr *websocket.CloseError
	if !errors.As(errRead, &closeErr) {
		t.Fatalf("routed delta error = %v, want websocket close", errRead)
	}
	if closeErr.Code != websocket.CloseServiceRestart || closeErr.Text != wsHTTPReplayRequiredCloseReason {
		t.Fatalf("routed delta close = %d %q, want %d %q", closeErr.Code, closeErr.Text, websocket.CloseServiceRestart, wsHTTPReplayRequiredCloseReason)
	}
	if got := len(codexExecutor.Payloads()); got != 1 {
		t.Fatalf("codex payload count = %d, want 1", got)
	}
	if got := len(claudeExecutor.Payloads()); got != 0 {
		t.Fatalf("claude payload count = %d, want 0 before full replay", got)
	}
}

func TestResponsesWebsocketClosesForHTTPReplayWhenWebsocketEligibilityChanges(t *testing.T) {
	gin.SetMode(gin.TestMode)

	modelName := "xai-websocket-mode-change-model"
	executor := &websocketDirectCaptureExecutor{provider: "xai", done: make(chan struct{})}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-xai-mode-change",
		Provider:   "xai",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: modelName}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	firstRequest := []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-1"}]}`, modelName))
	if errWrite := conn.WriteMessage(websocket.TextMessage, firstRequest); errWrite != nil {
		t.Fatalf("write first websocket message: %v", errWrite)
	}
	if _, _, errRead := conn.ReadMessage(); errRead != nil {
		t.Fatalf("read first websocket response: %v", errRead)
	}

	secondRequest := []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`)
	if errWrite := conn.WriteMessage(websocket.TextMessage, secondRequest); errWrite != nil {
		t.Fatalf("write second websocket message: %v", errWrite)
	}
	if _, _, errRead := conn.ReadMessage(); errRead != nil {
		t.Fatalf("read second websocket response: %v", errRead)
	}

	updatedAuth := &coreauth.Auth{
		ID:       auth.ID,
		Provider: auth.Provider,
		Status:   coreauth.StatusActive,
	}
	if _, errUpdate := manager.Update(context.Background(), updatedAuth); errUpdate != nil {
		t.Fatalf("Update auth: %v", errUpdate)
	}

	thirdRequest := []byte(`{"type":"response.create","previous_response_id":"resp-2","input":[{"type":"message","id":"msg-3"}]}`)
	if errWrite := conn.WriteMessage(websocket.TextMessage, thirdRequest); errWrite != nil {
		t.Fatalf("write third websocket message: %v", errWrite)
	}
	_, _, errRead := conn.ReadMessage()
	var closeErr *websocket.CloseError
	if !errors.As(errRead, &closeErr) {
		t.Fatalf("third response error = %v, want websocket close", errRead)
	}
	if closeErr.Code != websocket.CloseServiceRestart || closeErr.Text != wsHTTPReplayRequiredCloseReason {
		t.Fatalf("third response close = %d %q, want %d %q", closeErr.Code, closeErr.Text, websocket.CloseServiceRestart, wsHTTPReplayRequiredCloseReason)
	}

	payloads := executor.Payloads()
	if len(payloads) != 2 {
		t.Fatalf("executor payload count = %d, want 2; transport switch must not call HTTP upstream", len(payloads))
	}
	second := payloads[1]
	if got := gjson.GetBytes(second, "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("stable websocket previous_response_id = %q, want resp-1: %s", got, second)
	}
	if input := gjson.GetBytes(second, "input").Array(); len(input) != 1 || input[0].Get("id").String() != "msg-2" {
		t.Fatalf("stable websocket payload is not incremental: %s", second)
	}

	replayConn, _, errDialReplay := websocket.DefaultDialer.Dial(wsURL, nil)
	if errDialReplay != nil {
		t.Fatalf("dial replay websocket: %v", errDialReplay)
	}
	defer func() { _ = replayConn.Close() }()
	fullReplay := []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-1"},{"type":"message","id":"out-1"},{"type":"message","id":"msg-2"},{"type":"message","id":"out-2"},{"type":"message","id":"msg-3"}]}`, modelName))
	if errWrite := replayConn.WriteMessage(websocket.TextMessage, fullReplay); errWrite != nil {
		t.Fatalf("write full replay: %v", errWrite)
	}
	if _, _, errReadReplay := replayConn.ReadMessage(); errReadReplay != nil {
		t.Fatalf("read full replay response: %v", errReadReplay)
	}
	deltaAfterReplay := []byte(`{"type":"response.create","previous_response_id":"resp-3","input":[{"type":"message","id":"msg-4"}]}`)
	if errWrite := replayConn.WriteMessage(websocket.TextMessage, deltaAfterReplay); errWrite != nil {
		t.Fatalf("write delta after replay: %v", errWrite)
	}
	if _, _, errReadReplay := replayConn.ReadMessage(); errReadReplay != nil {
		t.Fatalf("read delta after replay response: %v", errReadReplay)
	}

	payloads = executor.Payloads()
	if len(payloads) != 4 {
		t.Fatalf("executor payload count after replay = %d, want 4", len(payloads))
	}
	httpDelta := payloads[3]
	if gjson.GetBytes(httpDelta, "previous_response_id").Exists() {
		t.Fatalf("HTTP-mode delta retained previous_response_id: %s", httpDelta)
	}
	input := gjson.GetBytes(httpDelta, "input").Array()
	wantIDs := []string{"msg-1", "out-1", "msg-2", "out-2", "msg-3", "out-3", "msg-4"}
	if len(input) != len(wantIDs) {
		t.Fatalf("HTTP-mode canonical input len = %d, want %d: %s", len(input), len(wantIDs), httpDelta)
	}
	for i, wantID := range wantIDs {
		if got := input[i].Get("id").String(); got != wantID {
			t.Fatalf("HTTP-mode canonical input[%d].id = %q, want %q: %s", i, got, wantID, httpDelta)
		}
	}
}

func TestResponsesWebsocketRejectsUnknownPreviousResponseOnNewSocket(t *testing.T) {
	gin.SetMode(gin.TestMode)

	modelName := "xai-websocket-reconnect-model"
	executor := &websocketDirectCaptureExecutor{provider: "xai"}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-xai-reconnect",
		Provider:   "xai",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: modelName}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	request := []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"previous_response_id":"resp-old","input":[{"type":"message","id":"msg-2","role":"user","content":"second"}]}`, modelName))
	if errWrite := conn.WriteMessage(websocket.TextMessage, request); errWrite != nil {
		t.Fatalf("write websocket message: %v", errWrite)
	}
	_, payload, errRead := conn.ReadMessage()
	if errRead != nil {
		t.Fatalf("read websocket response: %v", errRead)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeError {
		t.Fatalf("response type = %q, want %q: %s", got, wsEventTypeError, payload)
	}
	if got := int(gjson.GetBytes(payload, "status").Int()); got != http.StatusConflict {
		t.Fatalf("response status = %d, want %d: %s", got, http.StatusConflict, payload)
	}
	if got := gjson.GetBytes(payload, "error.code").String(); got != "previous_response_not_found" {
		t.Fatalf("response error code = %q, want previous_response_not_found: %s", got, payload)
	}
	if got := len(executor.Payloads()); got != 0 {
		t.Fatalf("executor payload count = %d, want 0", got)
	}

	recoveryRequest := []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-1"},{"type":"message","id":"out-1","role":"assistant"},{"type":"message","id":"msg-2"}]}`, modelName))
	if errWrite := conn.WriteMessage(websocket.TextMessage, recoveryRequest); errWrite != nil {
		t.Fatalf("write full recovery message: %v", errWrite)
	}
	_, recoveryPayload, errRead := conn.ReadMessage()
	if errRead != nil {
		t.Fatalf("read full recovery response: %v", errRead)
	}
	if got := gjson.GetBytes(recoveryPayload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("recovery response type = %q, want %q: %s", got, wsEventTypeCompleted, recoveryPayload)
	}
	payloads := executor.Payloads()
	if len(payloads) != 1 {
		t.Fatalf("executor payload count after recovery = %d, want 1", len(payloads))
	}
	if got := len(gjson.GetBytes(payloads[0], "input").Array()); got != 3 {
		t.Fatalf("full recovery input len = %d, want 3: %s", got, payloads[0])
	}
}

func TestResponsesWebsocketRollsBackCanonicalTranscriptAfterNonRetryableError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	modelName := "xai-websocket-rollback-model"
	executor := &websocketCanonicalRollbackExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:       "auth-xai-rollback",
		Provider: "xai",
		Status:   coreauth.StatusActive,
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: modelName}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Session-Id": []string{"rollback-tool-cache-session"}})
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	requests := []string{
		fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-1"}]}`, modelName),
		`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"function_call","id":"fc-failed","call_id":"failed-call","name":"failed_tool","arguments":"{}"},{"type":"function_call_output","id":"fco-failed","call_id":"failed-call","output":"must-not-survive"}]}`,
		`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-3"},{"type":"function_call","id":"fc-retry","call_id":"failed-call","name":"failed_tool","arguments":"{}"}]}`,
	}
	wantTypes := []string{wsEventTypeCompleted, wsEventTypeError, wsEventTypeCompleted}
	for i := range requests {
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(requests[i])); errWrite != nil {
			t.Fatalf("write websocket message %d: %v", i+1, errWrite)
		}
		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Fatalf("read websocket response %d: %v", i+1, errRead)
		}
		if got := gjson.GetBytes(payload, "type").String(); got != wantTypes[i] {
			t.Fatalf("response %d type = %q, want %q: %s", i+1, got, wantTypes[i], payload)
		}
	}

	payloads := executor.Payloads()
	if len(payloads) != 3 {
		t.Fatalf("executor payload count = %d, want 3", len(payloads))
	}
	third := payloads[2]
	if gjson.GetBytes(third, "previous_response_id").Exists() {
		t.Fatalf("retry payload must not depend on previous_response_id: %s", third)
	}
	input := gjson.GetBytes(third, "input").Array()
	if len(input) != 3 {
		t.Fatalf("retry canonical input len = %d, want 3: %s", len(input), third)
	}
	wantIDs := []string{"msg-1", "out-1", "msg-3"}
	for i, wantID := range wantIDs {
		if got := input[i].Get("id").String(); got != wantID {
			t.Fatalf("retry canonical input[%d].id = %q, want %q: %s", i, got, wantID, third)
		}
	}
	if bytes.Contains(third, []byte(`"id":"fc-failed"`)) || bytes.Contains(third, []byte(`"id":"fco-failed"`)) {
		t.Fatalf("failed turn leaked into retry transcript: %s", third)
	}
	if bytes.Contains(third, []byte(`"call_id":"failed-call"`)) || bytes.Contains(third, []byte("must-not-survive")) {
		t.Fatalf("failed turn contaminated tool repair cache: %s", third)
	}
}

func TestResponsesWebsocketSwitchesPinnedAuthAcrossProviders(t *testing.T) {
	for _, testCase := range []struct {
		name                      string
		xaiWebsockets             bool
		returnToDifferentXAIModel bool
	}{
		{name: "xai SSE", xaiWebsockets: false},
		{name: "xai websocket", xaiWebsockets: true},
		{name: "xai websocket different model", xaiWebsockets: true, returnToDifferentXAIModel: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)

			xaiModel := "xai-provider-switch-" + strings.ReplaceAll(testCase.name, " ", "-")
			returnXAIModel := xaiModel
			if testCase.returnToDifferentXAIModel {
				returnXAIModel += "-return"
			}
			codexModel := "codex-provider-switch-" + strings.ReplaceAll(testCase.name, " ", "-")
			xaiExecutor := &websocketDirectCaptureExecutor{provider: "xai"}
			codexExecutor := &websocketDirectCaptureExecutor{provider: "codex"}

			xaiAuth := &coreauth.Auth{
				ID:       "auth-" + xaiModel,
				Provider: "xai",
				Status:   coreauth.StatusActive,
			}
			if testCase.xaiWebsockets {
				xaiAuth.Attributes = map[string]string{"websockets": "true"}
			}
			codexAuth := &coreauth.Auth{
				ID:         "auth-" + codexModel,
				Provider:   "codex",
				Status:     coreauth.StatusActive,
				Attributes: map[string]string{"websockets": "true"},
			}
			selector := &orderedWebsocketSelector{order: []string{xaiAuth.ID, codexAuth.ID}}
			manager := coreauth.NewManager(nil, selector, nil)
			manager.RegisterExecutor(xaiExecutor)
			manager.RegisterExecutor(codexExecutor)
			if _, errRegister := manager.Register(context.Background(), xaiAuth); errRegister != nil {
				t.Fatalf("Register xAI auth: %v", errRegister)
			}
			if _, errRegister := manager.Register(context.Background(), codexAuth); errRegister != nil {
				t.Fatalf("Register Codex auth: %v", errRegister)
			}

			registry.GetGlobalRegistry().RegisterClient(xaiAuth.ID, xaiAuth.Provider, []*registry.ModelInfo{{ID: xaiModel}})
			registry.GetGlobalRegistry().RegisterClient(codexAuth.ID, codexAuth.Provider, []*registry.ModelInfo{{ID: codexModel}})
			registeredAuthIDs := []string{xaiAuth.ID, codexAuth.ID}
			if testCase.xaiWebsockets {
				xaiAlternateAuth := &coreauth.Auth{
					ID:         "auth-alternate-" + xaiModel,
					Provider:   "xai",
					Status:     coreauth.StatusActive,
					Attributes: map[string]string{"websockets": "true"},
				}
				selector.order = append(selector.order, xaiAlternateAuth.ID)
				if _, errRegister := manager.Register(context.Background(), xaiAlternateAuth); errRegister != nil {
					t.Fatalf("Register alternate xAI auth: %v", errRegister)
				}
				alternateModels := []*registry.ModelInfo{{ID: xaiModel}}
				if testCase.returnToDifferentXAIModel {
					alternateModels = []*registry.ModelInfo{{ID: returnXAIModel}}
				}
				registry.GetGlobalRegistry().RegisterClient(xaiAlternateAuth.ID, xaiAlternateAuth.Provider, alternateModels)
				registeredAuthIDs = append(registeredAuthIDs, xaiAlternateAuth.ID)
			}
			t.Cleanup(func() {
				for _, authID := range registeredAuthIDs {
					registry.GetGlobalRegistry().UnregisterClient(authID)
				}
			})

			base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
			h := NewOpenAIResponsesAPIHandler(base)
			router := gin.New()
			router.GET("/v1/responses/ws", h.ResponsesWebsocket)

			server := httptest.NewServer(router)
			defer server.Close()

			wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
			conn, _, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
			if errDial != nil {
				t.Fatalf("dial websocket: %v", errDial)
			}
			defer func() {
				if errClose := conn.Close(); errClose != nil {
					t.Errorf("close websocket: %v", errClose)
				}
			}()

			requests := []string{
				fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-xai-1"}]}`, xaiModel),
				fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-codex-1"}]}`, codexModel),
				fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-xai-2"}]}`, returnXAIModel),
				`{"type":"response.create","input":[{"type":"message","id":"msg-xai-3"}]}`,
			}
			for index, request := range requests {
				turn := index + 1
				if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(request)); errWrite != nil {
					t.Fatalf("write websocket message %d: %v", turn, errWrite)
				}
				_, payload, errRead := conn.ReadMessage()
				if errRead != nil {
					t.Fatalf("read websocket response %d: %v", turn, errRead)
				}
				if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
					t.Fatalf("response %d type = %s, want %s: %s", turn, got, wsEventTypeCompleted, payload)
				}
			}

			wantReturnAuthID := xaiAuth.ID
			if testCase.returnToDifferentXAIModel {
				wantReturnAuthID = "auth-alternate-" + xaiModel
			}
			if got := xaiExecutor.AuthIDs(); len(got) != 3 || got[0] != xaiAuth.ID || got[1] != wantReturnAuthID || got[2] != wantReturnAuthID {
				t.Fatalf("xAI auth IDs = %v, want [%s %s %s]", got, xaiAuth.ID, wantReturnAuthID, wantReturnAuthID)
			}
			if got := codexExecutor.AuthIDs(); len(got) != 1 || got[0] != codexAuth.ID {
				t.Fatalf("Codex auth IDs = %v, want [%s]", got, codexAuth.ID)
			}
		})
	}
}

func TestResponsesWebsocketPinnedAuthMatchesModel(t *testing.T) {
	modelA := "xai-pinned-auth-model-a"
	modelB := "xai-pinned-auth-model-b"
	auth := &coreauth.Auth{ID: "xai-pinned-auth", Provider: "xai", Status: coreauth.StatusActive}
	otherAuthID := "xai-pinned-auth-other"
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: modelA}})
	registry.GetGlobalRegistry().RegisterClient(otherAuthID, auth.Provider, []*registry.ModelInfo{{ID: modelB}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
		registry.GetGlobalRegistry().UnregisterClient(otherAuthID)
	})

	if !responsesWebsocketPinnedAuthMatchesModel(auth, modelA, modelA, false) {
		t.Fatal("expected registered auth to match its supported model")
	}
	if responsesWebsocketPinnedAuthMatchesModel(auth, modelB, modelA, false) {
		t.Fatal("registered auth matched an unsupported model from the same provider")
	}

	disabledAuth := auth.Clone()
	disabledAuth.Disabled = true
	if responsesWebsocketPinnedAuthMatchesModel(disabledAuth, modelA, modelA, false) {
		t.Fatal("disabled auth matched a model")
	}

	cooldownAuth := auth.Clone()
	cooldownAuth.ModelStates = map[string]*coreauth.ModelState{
		modelA: {Unavailable: true, NextRetryAfter: time.Now().Add(time.Minute)},
	}
	if responsesWebsocketPinnedAuthMatchesModel(cooldownAuth, modelA, modelA, false) {
		t.Fatal("auth in model cooldown matched a model")
	}

	unregisteredAuth := &coreauth.Auth{ID: "unregistered-auth", Provider: "xai", Status: coreauth.StatusActive}
	if responsesWebsocketPinnedAuthMatchesModel(unregisteredAuth, modelA, modelA, false) {
		t.Fatal("unregistered ordinary auth matched a model")
	}
	if !responsesWebsocketPinnedAuthMatchesModel(unregisteredAuth, modelA, modelA, true) {
		t.Fatal("expected Home runtime auth to match its pinned model")
	}
	if responsesWebsocketPinnedAuthMatchesModel(unregisteredAuth, modelB, modelA, true) {
		t.Fatal("Home runtime auth matched a different model")
	}
}

func TestWebsocketUpstreamSupportsIncrementalInputForModel(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{
		ID:         "auth-ws",
		Provider:   "test-provider",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	if !h.websocketUpstreamSupportsIncrementalInputForModel("test-model") {
		t.Fatalf("expected websocket-capable upstream for test-model")
	}
}

func TestWebsocketUpstreamSupportsIncrementalInputForXAI(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{
		ID:         "auth-xai-ws",
		Provider:   "xai",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "xai-test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	if !h.websocketUpstreamSupportsIncrementalInputForModel("xai-test-model") {
		t.Fatalf("expected xai websocket upstream to support previous_response_id incremental input")
	}
}

func TestResponsesWebsocketUsesUpstreamWebsocketPassthroughForXAI(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &websocketProviderCaptureExecutor{provider: "xai"}
	manager.RegisterExecutor(executor)

	modelName := "xai-passthrough-model"
	auth := &coreauth.Auth{
		ID:         "auth-xai-ws",
		Provider:   "xai",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: modelName}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	if !h.responsesWebsocketUsesUpstreamWebsocketPassthrough(modelName) {
		t.Fatalf("expected xai websocket upstream passthrough for %s", modelName)
	}
}

func TestWebsocketUpstreamSupportsCompactionReplayForModel(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{
		ID:       "auth-codex",
		Provider: "codex",
		Status:   coreauth.StatusActive,
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	if !h.websocketUpstreamSupportsCompactionReplayForModel("test-model") {
		t.Fatalf("expected codex upstream to support compaction replay")
	}
}

func TestWebsocketUpstreamSupportsCompactionReplayForModelFalseWhenMixedBackends(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	auths := []*coreauth.Auth{
		{ID: "auth-codex", Provider: "codex", Status: coreauth.StatusActive},
		{ID: "auth-claude", Provider: "claude", Status: coreauth.StatusActive},
	}
	for _, auth := range auths {
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("Register auth %s: %v", auth.ID, err)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	}
	t.Cleanup(func() {
		for _, auth := range auths {
			registry.GetGlobalRegistry().UnregisterClient(auth.ID)
		}
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	if h.websocketUpstreamSupportsCompactionReplayForModel("test-model") {
		t.Fatalf("expected mixed backend model to disable compaction replay bypass")
	}
}

func TestResponsesWebsocketPrewarmHandledLocallyForSSEUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketCaptureExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "auth-sse", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		errClose := conn.Close()
		if errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"test-model","generate":false}`))
	if errWrite != nil {
		t.Fatalf("write prewarm websocket message: %v", errWrite)
	}

	_, createdPayload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read prewarm created message: %v", errReadMessage)
	}
	if gjson.GetBytes(createdPayload, "type").String() != "response.created" {
		t.Fatalf("created payload type = %s, want response.created", gjson.GetBytes(createdPayload, "type").String())
	}
	prewarmResponseID := gjson.GetBytes(createdPayload, "response.id").String()
	if prewarmResponseID == "" {
		t.Fatalf("prewarm response id is empty")
	}
	if executor.streamCalls != 0 {
		t.Fatalf("stream calls after prewarm = %d, want 0", executor.streamCalls)
	}

	_, completedPayload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read prewarm completed message: %v", errReadMessage)
	}
	if gjson.GetBytes(completedPayload, "type").String() != wsEventTypeCompleted {
		t.Fatalf("completed payload type = %s, want %s", gjson.GetBytes(completedPayload, "type").String(), wsEventTypeCompleted)
	}
	if gjson.GetBytes(completedPayload, "response.id").String() != prewarmResponseID {
		t.Fatalf("completed response id = %s, want %s", gjson.GetBytes(completedPayload, "response.id").String(), prewarmResponseID)
	}
	if gjson.GetBytes(completedPayload, "response.usage.total_tokens").Int() != 0 {
		t.Fatalf("prewarm total tokens = %d, want 0", gjson.GetBytes(completedPayload, "response.usage.total_tokens").Int())
	}

	secondRequest := fmt.Sprintf(`{"type":"response.create","previous_response_id":%q,"input":[{"type":"message","id":"msg-1"}]}`, prewarmResponseID)
	errWrite = conn.WriteMessage(websocket.TextMessage, []byte(secondRequest))
	if errWrite != nil {
		t.Fatalf("write follow-up websocket message: %v", errWrite)
	}

	_, upstreamPayload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read upstream completed message: %v", errReadMessage)
	}
	if gjson.GetBytes(upstreamPayload, "type").String() != wsEventTypeCompleted {
		t.Fatalf("upstream payload type = %s, want %s", gjson.GetBytes(upstreamPayload, "type").String(), wsEventTypeCompleted)
	}
	if executor.streamCalls != 1 {
		t.Fatalf("stream calls after follow-up = %d, want 1", executor.streamCalls)
	}
	if len(executor.payloads) != 1 {
		t.Fatalf("captured upstream payloads = %d, want 1", len(executor.payloads))
	}
	forwarded := executor.payloads[0]
	if gjson.GetBytes(forwarded, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id leaked upstream: %s", forwarded)
	}
	if gjson.GetBytes(forwarded, "generate").Exists() {
		t.Fatalf("generate leaked upstream: %s", forwarded)
	}
	if gjson.GetBytes(forwarded, "model").String() != "test-model" {
		t.Fatalf("forwarded model = %s, want test-model", gjson.GetBytes(forwarded, "model").String())
	}
	input := gjson.GetBytes(forwarded, "input").Array()
	if len(input) != 1 || input[0].Get("id").String() != "msg-1" {
		t.Fatalf("unexpected forwarded input: %s", forwarded)
	}
}

func TestResponsesWebsocketMergesTranscriptForNonPassthroughUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketCaptureExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-ws",
		Provider:   executor.Identifier(),
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	requests := []string{
		`{"type":"response.create","model":"test-model","input":[{"type":"message","id":"msg-1"}]}`,
		`{"type":"response.create","input":[{"type":"message","id":"msg-2"}]}`,
	}
	for i := range requests {
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(requests[i])); errWrite != nil {
			t.Fatalf("write websocket message %d: %v", i+1, errWrite)
		}
		_, payload, errReadMessage := conn.ReadMessage()
		if errReadMessage != nil {
			t.Fatalf("read websocket message %d: %v", i+1, errReadMessage)
		}
		if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
			t.Fatalf("message %d payload type = %s, want %s", i+1, got, wsEventTypeCompleted)
		}
	}

	if len(executor.payloads) != 2 {
		t.Fatalf("upstream payload count = %d, want 2", len(executor.payloads))
	}
	secondPayload := executor.payloads[1]
	if gjson.GetBytes(secondPayload, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id must not be sent on non-passthrough upstream: %s", secondPayload)
	}
	input := gjson.GetBytes(secondPayload, "input").Array()
	if len(input) != 3 {
		t.Fatalf("second upstream input len = %d, want 3: %s", len(input), secondPayload)
	}
	if input[0].Get("id").String() != "msg-1" || input[1].Get("id").String() != "out-1" || input[2].Get("id").String() != "msg-2" {
		t.Fatalf("unexpected merged upstream input: %s", secondPayload)
	}
}

func TestResponsesWebsocketFullCreateReplacementSkipsToolRepairCache(t *testing.T) {
	gin.SetMode(gin.TestMode)

	sessionKey := "session-full-create-replacement-repair"
	resetResponsesWebsocketToolCaches(sessionKey)
	t.Cleanup(func() {
		resetResponsesWebsocketToolCaches(sessionKey)
	})

	executor := &websocketCaptureExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-ws",
		Provider:   executor.Identifier(),
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsHeaders := http.Header{}
	wsHeaders.Set("X-Client-Request-Id", sessionKey)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, wsHeaders)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"test-model","input":[{"type":"message","id":"msg-1"}]}`)); errWrite != nil {
		t.Fatalf("write initial websocket message: %v", errWrite)
	}
	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read initial websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("initial payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	defaultWebsocketToolOutputCache.record(sessionKey, "call-1", json.RawMessage(`{"type":"function_call_output","call_id":"call-1","id":"old-output","output":"stale"}`))
	replacement := []byte(`{
		"type":"response.create",
		"model":"test-model",
		"input":[
			{"type":"function_call","id":"fc-replacement","call_id":"call-1","name":"tool"},
			{"type":"message","id":"msg-2","content":"new transcript"}
		],
		"tools":[],
		"tool_choice":"auto",
		"parallel_tool_calls":true,
		"store":false,
		"stream":true,
		"include":[],
		"client_metadata":{
			"x-codex-installation-id":"install-1",
			"x-codex-window-id":"thread-1:0"
		}
	}`)
	if errWrite := conn.WriteMessage(websocket.TextMessage, replacement); errWrite != nil {
		t.Fatalf("write replacement websocket message: %v", errWrite)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read replacement websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("replacement payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	if len(executor.payloads) != 2 {
		t.Fatalf("upstream payload count = %d, want 2", len(executor.payloads))
	}
	forwarded := executor.payloads[1]
	input := gjson.GetBytes(forwarded, "input").Array()
	if len(input) != 2 {
		t.Fatalf("replacement input len = %d, want 2 without cached tool output: %s", len(input), forwarded)
	}
	if strings.Contains(gjson.GetBytes(forwarded, "input").Raw, "old-output") {
		t.Fatalf("replacement must not reuse stale tool repair cache: %s", forwarded)
	}
}

func TestResponsesWebsocketDoesNotInjectPreviousResponseIDWhenPendingToolOutputMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketCompactionCaptureExecutor{provider: "test-provider"}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-ws",
		Provider:   executor.Identifier(),
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	requests := []string{
		`{"type":"response.create","model":"test-model","input":[{"type":"message","id":"msg-1"}]}`,
		`{"type":"response.create","input":[{"type":"message","role":"user","id":"summary-1","content":"compacted summary"}]}`,
	}
	for i := range requests {
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(requests[i])); errWrite != nil {
			t.Fatalf("write websocket message %d: %v", i+1, errWrite)
		}
		_, payload, errReadMessage := conn.ReadMessage()
		if errReadMessage != nil {
			t.Fatalf("read websocket message %d: %v", i+1, errReadMessage)
		}
		if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
			t.Fatalf("message %d payload type = %s, want %s", i+1, got, wsEventTypeCompleted)
		}
	}

	executor.mu.Lock()
	payloads := append([][]byte(nil), executor.streamPayloads...)
	executor.mu.Unlock()

	if len(payloads) != 2 {
		t.Fatalf("upstream payload count = %d, want 2", len(payloads))
	}
	secondPayload := payloads[1]
	if gjson.GetBytes(secondPayload, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id must not be injected when pending tool output is missing: %s", secondPayload)
	}
	input := gjson.GetBytes(secondPayload, "input").Array()
	if len(input) != 3 {
		t.Fatalf("second upstream input len = %d, want 3: %s", len(input), secondPayload)
	}
	if input[0].Get("id").String() != "msg-1" || input[1].Get("id").String() != "fc-1" || input[2].Get("id").String() != "summary-1" {
		t.Fatalf("unexpected merged upstream input when pending tool output is missing: %s", secondPayload)
	}
}

func TestResponsesWebsocketReplaysTranscriptAfterUpstreamGenerationChange(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketGenerationReplayExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-codex",
		Provider:   executor.Identifier(),
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"test-model","input":[{"type":"message","id":"msg-1"}]}`)); errWrite != nil {
		t.Fatalf("write initial websocket message: %v", errWrite)
	}
	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read initial websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("initial payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	executor.BumpGeneration()
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`)); errWrite != nil {
		t.Fatalf("write follow-up websocket message: %v", errWrite)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read follow-up websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("follow-up payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	payloads := executor.Payloads()
	if len(payloads) != 2 {
		t.Fatalf("captured upstream payloads = %d, want 2", len(payloads))
	}
	replayed := payloads[1]
	if gjson.GetBytes(replayed, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id leaked after upstream generation changed: %s", replayed)
	}
	input := gjson.GetBytes(replayed, "input").Array()
	if len(input) != 3 {
		t.Fatalf("replayed input len = %d, want 3: %s", len(input), replayed)
	}
	for _, id := range []string{"msg-1", "out-1", "msg-2"} {
		if !strings.Contains(gjson.GetBytes(replayed, "input").Raw, fmt.Sprintf(`"id":"%s"`, id)) {
			t.Fatalf("replayed input missing %s: %s", id, replayed)
		}
	}
}

func TestResponsesWebsocketReplaysFullTranscriptAfterIncrementalSuccessAndUpstreamGenerationChange(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketGenerationReplayExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-codex",
		Provider:   executor.Identifier(),
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"test-model","input":[{"type":"message","id":"msg-1"}]}`)); errWrite != nil {
		t.Fatalf("write initial websocket message: %v", errWrite)
	}
	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read initial websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("initial payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`)); errWrite != nil {
		t.Fatalf("write incremental websocket message: %v", errWrite)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read incremental websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("incremental payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	executor.BumpGeneration()
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","previous_response_id":"resp-2","input":[{"type":"message","id":"msg-3"}]}`)); errWrite != nil {
		t.Fatalf("write replay websocket message: %v", errWrite)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read replay websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("replay payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	payloads := executor.Payloads()
	if len(payloads) != 3 {
		t.Fatalf("captured upstream payloads = %d, want 3", len(payloads))
	}
	incremental := payloads[1]
	if got := gjson.GetBytes(incremental, "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("second upstream previous_response_id = %s, want resp-1: %s", got, incremental)
	}
	if input := gjson.GetBytes(incremental, "input").Array(); len(input) != 1 || input[0].Get("id").String() != "msg-2" {
		t.Fatalf("second upstream should stay incremental: %s", incremental)
	}

	replayed := payloads[2]
	if gjson.GetBytes(replayed, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id leaked after upstream generation changed: %s", replayed)
	}
	input := gjson.GetBytes(replayed, "input").Array()
	if len(input) != 5 {
		t.Fatalf("replayed input len = %d, want 5: %s", len(input), replayed)
	}
	inputRaw := gjson.GetBytes(replayed, "input").Raw
	for _, id := range []string{"msg-1", "out-1", "msg-2", "out-2", "msg-3"} {
		if !strings.Contains(inputRaw, fmt.Sprintf(`"id":"%s"`, id)) {
			t.Fatalf("replayed input missing %s: %s", id, replayed)
		}
	}
}

func TestResponsesWebsocketRetriesPreviousResponseNotFoundWithTranscriptReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketGenerationReplayExecutor{failPreviousResponseOnCall: 2}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-codex",
		Provider:   executor.Identifier(),
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"test-model","input":[{"type":"message","id":"msg-1"}]}`)); errWrite != nil {
		t.Fatalf("write initial websocket message: %v", errWrite)
	}
	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read initial websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("initial payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`)); errWrite != nil {
		t.Fatalf("write follow-up websocket message: %v", errWrite)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read retried websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("retried payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	payloads := executor.Payloads()
	if len(payloads) != 3 {
		t.Fatalf("captured upstream payloads = %d, want 3", len(payloads))
	}
	if got := gjson.GetBytes(payloads[1], "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("second upstream previous_response_id = %s, want resp-1: %s", got, payloads[1])
	}
	replayed := payloads[2]
	if gjson.GetBytes(replayed, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id leaked after retry replay: %s", replayed)
	}
	inputRaw := gjson.GetBytes(replayed, "input").Raw
	for _, id := range []string{"msg-1", "out-1", "msg-2"} {
		if !strings.Contains(inputRaw, fmt.Sprintf(`"id":"%s"`, id)) {
			t.Fatalf("retry replay input missing %s: %s", id, replayed)
		}
	}
}

func TestResponsesWebsocketDoesNotReplayPreviousResponseNotFoundAfterPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)

	streamCallCh := make(chan int, 4)
	executor := &websocketGenerationReplayExecutor{failPreviousAfterPayloadOnCall: 2, streamCallCh: streamCallCh}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-codex",
		Provider:   executor.Identifier(),
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"test-model","input":[{"type":"message","id":"msg-1"}]}`)); errWrite != nil {
		t.Fatalf("write initial websocket message: %v", errWrite)
	}
	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read initial websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("initial payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`)); errWrite != nil {
		t.Fatalf("write follow-up websocket message: %v", errWrite)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read output item websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != "response.output_item.done" {
		t.Fatalf("follow-up first payload type = %s, want response.output_item.done: %s", got, payload)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read previous-response error websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeError {
		t.Fatalf("follow-up second payload type = %s, want %s: %s", got, wsEventTypeError, payload)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-streamCallCh:
		default:
			t.Fatalf("missing expected upstream call %d", i+1)
		}
	}

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","input":[{"type":"message","id":"msg-3"}]}`)); errWrite != nil {
		t.Fatalf("write third websocket message: %v", errWrite)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read third websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("third payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	payloads := executor.Payloads()
	if len(payloads) != 3 {
		t.Fatalf("captured upstream payloads = %d, want 3 without replay after forwarded payload", len(payloads))
	}
	if got := gjson.GetBytes(payloads[1], "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("second upstream previous_response_id = %s, want resp-1: %s", got, payloads[1])
	}
	if gjson.GetBytes(payloads[2], "previous_response_id").Exists() {
		t.Fatalf("third upstream previous_response_id leaked: %s", payloads[2])
	}
	thirdInput := gjson.GetBytes(payloads[2], "input").Array()
	if len(thirdInput) != 3 {
		t.Fatalf("third upstream input len = %d, want 3: %s", len(thirdInput), payloads[2])
	}
	thirdInputRaw := gjson.GetBytes(payloads[2], "input").Raw
	for _, id := range []string{"msg-1", "out-1", "msg-3"} {
		if !strings.Contains(thirdInputRaw, fmt.Sprintf(`"id":"%s"`, id)) {
			t.Fatalf("third upstream input missing %s: %s", id, payloads[2])
		}
	}
	if strings.Contains(thirdInputRaw, `"id":"msg-2"`) {
		t.Fatalf("failed previous-response turn was committed into third upstream input: %s", payloads[2])
	}
}

func TestResponsesWebsocketRetriesUpstreamResetAfterTurnStateMetadata(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketGenerationReplayExecutor{
		resetAfterPayloadOnCall: 2,
		turnStateByCall: map[int]string{
			2: "stale-state",
			3: "fresh-state",
		},
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-codex",
		Provider:   executor.Identifier(),
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"test-model","input":[{"type":"message","id":"msg-1"}]}`)); errWrite != nil {
		t.Fatalf("write initial websocket message: %v", errWrite)
	}
	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read initial websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("initial payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`)); errWrite != nil {
		t.Fatalf("write follow-up websocket message: %v", errWrite)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read replayed turn-state metadata: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != "response.metadata" {
		t.Fatalf("replayed first payload type = %s, want response.metadata: %s", got, payload)
	}
	if got := gjson.GetBytes(payload, "headers").Get(wsTurnStateHeader).String(); got != "fresh-state" {
		t.Fatalf("replayed turn state = %q, want fresh-state: %s", got, payload)
	}

	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read replayed rate limits: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != "codex.rate_limits" {
		t.Fatalf("replayed second payload type = %s, want codex.rate_limits: %s", got, payload)
	}

	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read replayed completed response: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("replayed third payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}
	if got := gjson.GetBytes(payload, "response.id").String(); got != "resp-3" {
		t.Fatalf("replayed response id = %q, want resp-3: %s", got, payload)
	}

	payloads := executor.Payloads()
	if len(payloads) != 3 {
		t.Fatalf("captured upstream payloads = %d, want 3", len(payloads))
	}
	if got := gjson.GetBytes(payloads[1], "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("second upstream previous_response_id = %s, want resp-1: %s", got, payloads[1])
	}
	replayed := payloads[2]
	if gjson.GetBytes(replayed, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id leaked after metadata-first reset: %s", replayed)
	}
	inputRaw := gjson.GetBytes(replayed, "input").Raw
	for _, id := range []string{"msg-1", "out-1", "msg-2"} {
		if !strings.Contains(inputRaw, fmt.Sprintf(`"id":"%s"`, id)) {
			t.Fatalf("metadata-first reset replay input missing %s: %s", id, replayed)
		}
	}
}

func TestResponsesWebsocketRetriesUpstreamResetDuringForcedTranscriptReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketGenerationReplayExecutor{requireReplayOnCall: 2}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-codex",
		Provider:   executor.Identifier(),
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"test-model","input":[{"type":"message","id":"msg-1"}]}`)); errWrite != nil {
		t.Fatalf("write initial websocket message: %v", errWrite)
	}
	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read initial websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("initial payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	executor.BumpGeneration()
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`)); errWrite != nil {
		t.Fatalf("write follow-up websocket message: %v", errWrite)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read forced-replay websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("forced-replay payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	payloads := executor.Payloads()
	if len(payloads) != 3 {
		t.Fatalf("captured upstream payloads = %d, want 3", len(payloads))
	}
	for _, replayed := range payloads[1:] {
		if gjson.GetBytes(replayed, "previous_response_id").Exists() {
			t.Fatalf("previous_response_id leaked during forced replay retry: %s", replayed)
		}
		inputRaw := gjson.GetBytes(replayed, "input").Raw
		for _, id := range []string{"msg-1", "out-1", "msg-2"} {
			if !strings.Contains(inputRaw, fmt.Sprintf(`"id":"%s"`, id)) {
				t.Fatalf("forced replay input missing %s: %s", id, replayed)
			}
		}
	}
}

func TestResponsesWebsocketDoesNotCommitFailedTranscriptReplayAfterRetryExhaustion(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketGenerationReplayExecutor{
		requireReplayCalls: map[int]bool{
			2: true,
			3: true,
			4: true,
		},
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "auth-codex",
		Provider:   executor.Identifier(),
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"test-model","input":[{"type":"message","id":"msg-1"}]}`)); errWrite != nil {
		t.Fatalf("write initial websocket message: %v", errWrite)
	}
	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read initial websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("initial payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	executor.BumpGeneration()
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`)); errWrite != nil {
		t.Fatalf("write retry-exhausted websocket message: %v", errWrite)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read retry-exhausted websocket error: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeError {
		t.Fatalf("retry-exhausted payload type = %s, want %s: %s", got, wsEventTypeError, payload)
	}
	if got := int(gjson.GetBytes(payload, "status").Int()); got != http.StatusBadGateway {
		t.Fatalf("retry-exhausted status = %d, want %d: %s", got, http.StatusBadGateway, payload)
	}
	if strings.Contains(string(payload), "requires transcript replay") {
		t.Fatalf("internal replay sentinel leaked to downstream: %s", payload)
	}

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-3"}]}`)); errWrite != nil {
		t.Fatalf("write recovery websocket message: %v", errWrite)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read recovery websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("recovery payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	payloads := executor.Payloads()
	if len(payloads) != 5 {
		t.Fatalf("captured upstream payloads = %d, want 5", len(payloads))
	}
	replayed := payloads[4]
	if gjson.GetBytes(replayed, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id leaked after retry exhaustion recovery: %s", replayed)
	}
	inputRaw := gjson.GetBytes(replayed, "input").Raw
	for _, id := range []string{"msg-1", "out-1", "msg-3"} {
		if !strings.Contains(inputRaw, fmt.Sprintf(`"id":"%s"`, id)) {
			t.Fatalf("recovery replay input missing %s: %s", id, replayed)
		}
	}
	if strings.Contains(inputRaw, `"id":"msg-2"`) {
		t.Fatalf("failed retry-exhausted turn was committed into recovery replay: %s", replayed)
	}
}

func TestResponsesWebsocketRetriesRealCodexUpstreamReadResetWithTranscriptReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var upstreamMu sync.Mutex
	upstreamPayloads := make([][]byte, 0, 3)
	upstreamRequests := make(chan int, 3)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade upstream websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				return
			}
			upstreamMu.Lock()
			upstreamPayloads = append(upstreamPayloads, bytes.Clone(payload))
			call := len(upstreamPayloads)
			upstreamMu.Unlock()
			upstreamRequests <- call

			switch call {
			case 1:
				completed := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[{"type":"message","id":"out-1","role":"assistant","content":[{"type":"output_text","text":"first"}]}]}}`)
				if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
					t.Errorf("write initial upstream response: %v", errWrite)
					return
				}
			case 2:
				return
			default:
				completed := []byte(`{"type":"response.completed","response":{"id":"resp-replayed","output":[{"type":"message","id":"out-replayed","role":"assistant","content":[{"type":"output_text","text":"replayed"}]}]}}`)
				if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
					t.Errorf("write replayed upstream response: %v", errWrite)
				}
				return
			}
		}
	}))
	defer upstream.Close()

	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(codexexecutor.NewCodexAutoExecutor(&internalconfig.Config{
		SDKConfig: internalconfig.SDKConfig{DisableImageGeneration: internalconfig.DisableImageGenerationAll},
	}))
	auth := &coreauth.Auth{
		ID:       "auth-real-codex-ws-reset",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"api_key":    "sk-test",
			"base_url":   upstream.URL,
			"websockets": "true",
		},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "gpt-5-codex"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	defer downstream.Close()

	wsURL := "ws" + strings.TrimPrefix(downstream.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial downstream websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5-codex","input":[{"type":"message","id":"msg-1"}]}`)); errWrite != nil {
		t.Fatalf("write initial downstream request: %v", errWrite)
	}
	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read initial downstream response: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("initial downstream payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`)); errWrite != nil {
		t.Fatalf("write follow-up downstream request: %v", errWrite)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read replayed downstream response: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("replayed downstream payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	for i := 0; i < 3; i++ {
		select {
		case <-upstreamRequests:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for upstream request %d", i+1)
		}
	}
	upstreamMu.Lock()
	payloads := make([][]byte, len(upstreamPayloads))
	for i := range upstreamPayloads {
		payloads[i] = bytes.Clone(upstreamPayloads[i])
	}
	upstreamMu.Unlock()

	if len(payloads) != 3 {
		t.Fatalf("captured upstream payloads = %d, want 3", len(payloads))
	}
	if got := gjson.GetBytes(payloads[1], "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("second upstream previous_response_id = %s, want resp-1: %s", got, payloads[1])
	}
	replayed := payloads[2]
	if gjson.GetBytes(replayed, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id leaked after real upstream read reset: %s", replayed)
	}
	inputRaw := gjson.GetBytes(replayed, "input").Raw
	for _, id := range []string{"msg-1", "out-1", "msg-2"} {
		if !strings.Contains(inputRaw, fmt.Sprintf(`"id":"%s"`, id)) {
			t.Fatalf("real upstream read reset replay input missing %s: %s", id, replayed)
		}
	}
}

func TestResponsesWebsocketMediatesCodex426Fallback(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var upstreamMu sync.Mutex
	var websocketUpgrades int
	var httpPayloads [][]byte
	var httpHeaders []http.Header
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if websocket.IsWebSocketUpgrade(r) {
			upstreamMu.Lock()
			websocketUpgrades++
			upstreamMu.Unlock()
			w.WriteHeader(http.StatusUpgradeRequired)
			return
		}

		body, errRead := io.ReadAll(r.Body)
		if errRead != nil {
			t.Errorf("read upstream HTTP body: %v", errRead)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		upstreamMu.Lock()
		httpPayloads = append(httpPayloads, bytes.Clone(body))
		httpHeaders = append(httpHeaders, r.Header.Clone())
		call := len(httpPayloads)
		upstreamMu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set(wsTurnStateHeader, "state-1")
		switch call {
		case 1:
			_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[{\"type\":\"message\",\"id\":\"out-1\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"first\"}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"))
		case 2:
			_, _ = w.Write([]byte("data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"compact-summary\"}}\n\n"))
			_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-compact\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"))
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer upstream.Close()

	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(codexexecutor.NewCodexAutoExecutor(&internalconfig.Config{
		SDKConfig: internalconfig.SDKConfig{DisableImageGeneration: internalconfig.DisableImageGenerationAll},
	}))
	auth := &coreauth.Auth{
		ID:       "auth-real-codex-426",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"api_key":    "sk-test",
			"base_url":   upstream.URL,
			"websockets": "true",
		},
	}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("Register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "gpt-5-codex"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	defer downstream.Close()

	wsURL := "ws" + strings.TrimPrefix(downstream.URL, "http") + "/v1/responses/ws"
	conn, _, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
	if errDial != nil {
		t.Fatalf("dial downstream websocket: %v", errDial)
	}
	defer func() { _ = conn.Close() }()

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5-codex","generate":false,"input":[]}`)); errWrite != nil {
		t.Fatalf("write downstream prewarm: %v", errWrite)
	}
	_, prewarmCreated, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read prewarm created: %v", errReadMessage)
	}
	if got := gjson.GetBytes(prewarmCreated, "type").String(); got != "response.created" {
		t.Fatalf("prewarm first payload type = %s, want response.created: %s", got, prewarmCreated)
	}
	prewarmResponseID := gjson.GetBytes(prewarmCreated, "response.id").String()
	if prewarmResponseID == "" {
		t.Fatal("prewarm response id is empty")
	}
	_, prewarmCompleted, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read prewarm completed: %v", errReadMessage)
	}
	if got := gjson.GetBytes(prewarmCompleted, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("prewarm second payload type = %s, want %s: %s", got, wsEventTypeCompleted, prewarmCompleted)
	}
	upstreamMu.Lock()
	prewarmHTTPCalls := len(httpPayloads)
	upstreamMu.Unlock()
	if prewarmHTTPCalls != 0 {
		t.Fatalf("upstream HTTP calls after prewarm = %d, want 0", prewarmHTTPCalls)
	}

	firstRequest := fmt.Sprintf(`{"type":"response.create","model":"gpt-5-codex","previous_response_id":%q,"input":[{"type":"message","id":"msg-1","role":"user","content":[{"type":"input_text","text":"hello"}]}]}`, prewarmResponseID)
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(firstRequest)); errWrite != nil {
		t.Fatalf("write first downstream request: %v", errWrite)
	}
	_, metadataPayload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read first turn-state metadata: %v", errReadMessage)
	}
	if got := gjson.GetBytes(metadataPayload, "type").String(); got != "response.metadata" {
		t.Fatalf("first fallback payload type = %s, want response.metadata: %s", got, metadataPayload)
	}
	if got := gjson.GetBytes(metadataPayload, "headers").Get(wsTurnStateHeader).String(); got != "state-1" {
		t.Fatalf("turn-state metadata = %q, want state-1: %s", got, metadataPayload)
	}
	if got := len(gjson.GetBytes(metadataPayload, "headers").Map()); got != 1 {
		t.Fatalf("metadata header count = %d, want 1: %s", got, metadataPayload)
	}
	_, firstCompleted, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read first completed response: %v", errReadMessage)
	}
	if got := gjson.GetBytes(firstCompleted, "response.id").String(); got != "resp-1" {
		t.Fatalf("first response id = %q, want resp-1: %s", got, firstCompleted)
	}

	compactRequest := []byte(`{"type":"response.create","model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"compaction_trigger"}],"client_metadata":{"x-codex-turn-state":"state-1"}}`)
	if errWrite := conn.WriteMessage(websocket.TextMessage, compactRequest); errWrite != nil {
		t.Fatalf("write compact downstream request: %v", errWrite)
	}
	sawCompactMetadata := false
	sawCompactionItem := false
compactResponse:
	for i := 0; i < 4; i++ {
		_, payload, errRead := readResponsesWebsocketTestMessage(t, conn)
		if errRead != nil {
			t.Fatalf("read compact downstream response: %v", errRead)
		}
		switch gjson.GetBytes(payload, "type").String() {
		case "response.metadata":
			sawCompactMetadata = gjson.GetBytes(payload, "headers").Get(wsTurnStateHeader).String() == "state-1"
		case "response.output_item.done":
			sawCompactionItem = gjson.GetBytes(payload, "item.type").String() == "compaction"
		case wsEventTypeCompleted:
			break compactResponse
		}
	}
	if !sawCompactMetadata {
		t.Fatal("compact response did not bridge turn-state metadata")
	}
	if !sawCompactionItem {
		t.Fatal("compact response did not forward compaction output item")
	}

	upstreamMu.Lock()
	payloads := make([][]byte, len(httpPayloads))
	for i := range httpPayloads {
		payloads[i] = bytes.Clone(httpPayloads[i])
	}
	headers := make([]http.Header, len(httpHeaders))
	for i := range httpHeaders {
		headers[i] = httpHeaders[i].Clone()
	}
	upgradeCount := websocketUpgrades
	upstreamMu.Unlock()
	if upgradeCount != 1 {
		t.Fatalf("upstream websocket upgrade count = %d, want 1", upgradeCount)
	}
	if len(payloads) != 2 {
		t.Fatalf("upstream HTTP payload count = %d, want 2", len(payloads))
	}
	for i, payload := range payloads {
		if gjson.GetBytes(payload, "type").Exists() {
			t.Fatalf("HTTP payload %d contains websocket type: %s", i, payload)
		}
		if gjson.GetBytes(payload, "previous_response_id").Exists() {
			t.Fatalf("HTTP payload %d contains previous_response_id: %s", i, payload)
		}
		if gjson.GetBytes(payload, "generate").Exists() {
			t.Fatalf("HTTP payload %d contains generate: %s", i, payload)
		}
	}
	firstInput := gjson.GetBytes(payloads[0], "input").Array()
	if len(firstInput) != 1 || firstInput[0].Get("id").String() != "msg-1" {
		t.Fatalf("first HTTP input is not the mediated user transcript: %s", payloads[0])
	}
	compactInput := gjson.GetBytes(payloads[1], "input").Array()
	if len(compactInput) != 3 ||
		compactInput[0].Get("id").String() != "msg-1" ||
		compactInput[1].Get("id").String() != "out-1" ||
		compactInput[2].Get("type").String() != "compaction_trigger" {
		t.Fatalf("compact HTTP input is not the full transcript plus trigger: %s", payloads[1])
	}
	triggerCount := 0
	for _, item := range compactInput {
		if item.Get("type").String() == "compaction_trigger" {
			triggerCount++
		}
	}
	if triggerCount != 1 {
		t.Fatalf("compact trigger count = %d, want 1: %s", triggerCount, payloads[1])
	}
	if got := headers[0].Get(wsTurnStateHeader); got != "" {
		t.Fatalf("first HTTP request turn-state = %q, want empty", got)
	}
	if got := headers[1].Get(wsTurnStateHeader); got != "state-1" {
		t.Fatalf("compact HTTP request turn-state = %q, want state-1", got)
	}
}

func TestResponsesWebsocketStripsGenerateWhenWebsocketAttemptFallsBackToHTTP(t *testing.T) {
	gin.SetMode(gin.TestMode)

	selector := &orderedWebsocketSelector{order: []string{"auth-ws", "auth-http", "auth-http"}}
	executor := &websocketBootstrapFallbackExecutor{}
	manager := coreauth.NewManager(nil, selector, nil)
	manager.RegisterExecutor(executor)

	authWS := &coreauth.Auth{
		ID:         "auth-ws",
		Provider:   executor.Identifier(),
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), authWS); err != nil {
		t.Fatalf("Register websocket auth: %v", err)
	}
	authHTTP := &coreauth.Auth{ID: "auth-http", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), authHTTP); err != nil {
		t.Fatalf("Register HTTP auth: %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(authWS.ID, authWS.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	registry.GetGlobalRegistry().RegisterClient(authHTTP.ID, authHTTP.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(authWS.ID)
		registry.GetGlobalRegistry().UnregisterClient(authHTTP.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	request := `{"type":"response.create","model":"test-model","generate":true,"input":[{"type":"message","id":"msg-1"}]}`
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(request)); errWrite != nil {
		t.Fatalf("write websocket message: %v", errWrite)
	}
	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("payload type = %s, want %s: %s", got, wsEventTypeCompleted, payload)
	}

	if got := executor.AuthIDs(); len(got) != 2 || got[0] != "auth-ws" || got[1] != "auth-http" {
		t.Fatalf("selected auth IDs = %v, want [auth-ws auth-http]", got)
	}

	wsPayloads := executor.Payloads("auth-ws")
	if len(wsPayloads) != 1 {
		t.Fatalf("auth-ws payload count = %d, want 1", len(wsPayloads))
	}
	if !gjson.GetBytes(wsPayloads[0], "generate").Exists() {
		t.Fatalf("websocket attempt payload unexpectedly stripped generate: %s", wsPayloads[0])
	}

	httpPayloads := executor.Payloads("auth-http")
	if len(httpPayloads) != 1 {
		t.Fatalf("auth-http payload count = %d, want 1", len(httpPayloads))
	}
	if gjson.GetBytes(httpPayloads[0], "generate").Exists() {
		t.Fatalf("generate leaked after HTTP fallback: %s", httpPayloads[0])
	}

	secondRequest := `{"type":"response.create","previous_response_id":"resp-http","input":[{"type":"message","id":"msg-2"}]}`
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(secondRequest)); errWrite != nil {
		t.Fatalf("write second websocket message: %v", errWrite)
	}
	_, secondPayload, errReadSecond := conn.ReadMessage()
	if errReadSecond != nil {
		t.Fatalf("read second websocket message: %v", errReadSecond)
	}
	if got := gjson.GetBytes(secondPayload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("second payload type = %s, want %s: %s", got, wsEventTypeCompleted, secondPayload)
	}
	if got := executor.AuthIDs(); len(got) != 3 || got[2] != "auth-http" {
		t.Fatalf("selected auth IDs after HTTP retry = %v, want [auth-ws auth-http auth-http]", got)
	}
}

func TestWebsocketClientAddressUsesGinClientIP(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	c, engine := gin.CreateTestContext(recorder)
	if err := engine.SetTrustedProxies([]string{"0.0.0.0/0", "::/0"}); err != nil {
		t.Fatalf("SetTrustedProxies: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/responses/ws", nil)
	req.RemoteAddr = "172.18.0.1:34282"
	req.Header.Set("X-Forwarded-For", "203.0.113.7")
	c.Request = req

	if got := websocketClientAddress(c); got != strings.TrimSpace(c.ClientIP()) {
		t.Fatalf("websocketClientAddress = %q, ClientIP = %q", got, c.ClientIP())
	}
}

func TestWebsocketClientAddressReturnsEmptyForNilContext(t *testing.T) {
	if got := websocketClientAddress(nil); got != "" {
		t.Fatalf("websocketClientAddress(nil) = %q, want empty", got)
	}
}

func TestResponsesWebsocketPinsOnlyWebsocketCapableAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)

	selector := &orderedWebsocketSelector{order: []string{"auth-sse", "auth-ws"}}
	executor := &websocketAuthCaptureExecutor{}
	manager := coreauth.NewManager(nil, selector, nil)
	manager.RegisterExecutor(executor)

	authSSE := &coreauth.Auth{ID: "auth-sse", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), authSSE); err != nil {
		t.Fatalf("Register SSE auth: %v", err)
	}
	authWS := &coreauth.Auth{
		ID:         "auth-ws",
		Provider:   executor.Identifier(),
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), authWS); err != nil {
		t.Fatalf("Register websocket auth: %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(authSSE.ID, authSSE.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	registry.GetGlobalRegistry().RegisterClient(authWS.ID, authWS.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(authSSE.ID)
		registry.GetGlobalRegistry().UnregisterClient(authWS.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	requests := []string{
		`{"type":"response.create","model":"test-model","input":[{"type":"message","id":"msg-1"}]}`,
		`{"type":"response.create","input":[{"type":"message","id":"msg-2"}]}`,
	}
	for i := range requests {
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(requests[i])); errWrite != nil {
			t.Fatalf("write websocket message %d: %v", i+1, errWrite)
		}
		_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
		if errReadMessage != nil {
			t.Fatalf("read websocket message %d: %v", i+1, errReadMessage)
		}
		if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
			t.Fatalf("message %d payload type = %s, want %s", i+1, got, wsEventTypeCompleted)
		}
	}

	if got := executor.AuthIDs(); len(got) != 2 || got[0] != "auth-sse" || got[1] != "auth-ws" {
		t.Fatalf("selected auth IDs = %v, want [auth-sse auth-ws]", got)
	}
}

func TestResponsesWebsocketUsesNativeIncrementalAfterPinningWebsocketAuthFromMixedPool(t *testing.T) {
	gin.SetMode(gin.TestMode)

	modelName := "xai-mixed-pool-model"
	selector := &orderedWebsocketSelector{order: []string{"auth-http", "auth-ws"}}
	executor := &websocketDirectCaptureExecutor{provider: "xai"}
	manager := coreauth.NewManager(nil, selector, nil)
	manager.RegisterExecutor(executor)
	authHTTP := &coreauth.Auth{ID: "auth-http", Provider: "xai", Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), authHTTP); err != nil {
		t.Fatalf("Register HTTP auth: %v", err)
	}
	authWS := &coreauth.Auth{
		ID:         "auth-ws",
		Provider:   "xai",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), authWS); err != nil {
		t.Fatalf("Register websocket auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(authHTTP.ID, authHTTP.Provider, []*registry.ModelInfo{{ID: modelName}})
	registry.GetGlobalRegistry().RegisterClient(authWS.ID, authWS.Provider, []*registry.ModelInfo{{ID: modelName}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(authHTTP.ID)
		registry.GetGlobalRegistry().UnregisterClient(authWS.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	requests := []string{
		fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-1"}]}`, modelName),
		`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-2"}]}`,
		`{"type":"response.create","previous_response_id":"resp-2","input":[{"type":"message","id":"msg-3"}]}`,
	}
	for i := range requests {
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(requests[i])); errWrite != nil {
			t.Fatalf("write websocket message %d: %v", i+1, errWrite)
		}
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Fatalf("read websocket response %d: %v", i+1, errRead)
		}
	}

	if got := executor.AuthIDs(); len(got) != 3 || got[0] != "auth-http" || got[1] != "auth-ws" || got[2] != "auth-ws" {
		t.Fatalf("selected auth IDs = %v, want [auth-http auth-ws auth-ws]", got)
	}
	payloads := executor.Payloads()
	if len(payloads) != 3 {
		t.Fatalf("payload count = %d, want 3", len(payloads))
	}
	if gjson.GetBytes(payloads[1], "previous_response_id").Exists() || len(gjson.GetBytes(payloads[1], "input").Array()) != 3 {
		t.Fatalf("first request on newly selected websocket auth must be canonical: %s", payloads[1])
	}
	if got := gjson.GetBytes(payloads[2], "previous_response_id").String(); got != "resp-2" {
		t.Fatalf("stable pinned websocket previous_response_id = %q, want resp-2: %s", got, payloads[2])
	}
	input := gjson.GetBytes(payloads[2], "input").Array()
	if len(input) != 1 || input[0].Get("id").String() != "msg-3" {
		t.Fatalf("stable pinned websocket request is not incremental: %s", payloads[2])
	}
}

func TestResponsesWebsocketReplaysImmediatelyAfterPinnedAuthFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name            string
		status          int
		backupWebsocket bool
	}{
		{name: "unauthorized to websocket", status: http.StatusUnauthorized, backupWebsocket: true},
		{name: "unauthorized to http", status: http.StatusUnauthorized, backupWebsocket: false},
		{name: "rate limit to websocket", status: http.StatusTooManyRequests, backupWebsocket: true},
		{name: "rate limit to http", status: http.StatusTooManyRequests, backupWebsocket: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			modelName := fmt.Sprintf("credential-failure-%d-%t-model", tc.status, tc.backupWebsocket)
			selector := &orderedWebsocketSelector{order: []string{"auth-a", "auth-b"}}
			executor := &websocketPinnedFailoverExecutor{failStatus: tc.status}
			manager := coreauth.NewManager(nil, selector, nil)
			manager.RegisterExecutor(executor)

			authA := &coreauth.Auth{
				ID:         "auth-a",
				Provider:   executor.Identifier(),
				Status:     coreauth.StatusActive,
				Attributes: map[string]string{"websockets": "true"},
			}
			if _, err := manager.Register(context.Background(), authA); err != nil {
				t.Fatalf("Register auth A: %v", err)
			}
			authB := &coreauth.Auth{
				ID:         "auth-b",
				Provider:   executor.Identifier(),
				Status:     coreauth.StatusActive,
				Attributes: map[string]string{"websockets": strconv.FormatBool(tc.backupWebsocket)},
			}
			if _, err := manager.Register(context.Background(), authB); err != nil {
				t.Fatalf("Register auth B: %v", err)
			}

			registry.GetGlobalRegistry().RegisterClient(authA.ID, authA.Provider, []*registry.ModelInfo{{ID: modelName}})
			registry.GetGlobalRegistry().RegisterClient(authB.ID, authB.Provider, []*registry.ModelInfo{{ID: modelName}})
			t.Cleanup(func() {
				registry.GetGlobalRegistry().UnregisterClient(authA.ID)
				registry.GetGlobalRegistry().UnregisterClient(authB.ID)
			})

			base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
			h := NewOpenAIResponsesAPIHandler(base)
			router := gin.New()
			router.GET("/v1/responses/ws", h.ResponsesWebsocket)

			server := httptest.NewServer(router)
			defer server.Close()

			wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if err != nil {
				t.Fatalf("dial websocket: %v", err)
			}
			defer func() { _ = conn.Close() }()

			firstRequest := fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-1"}]}`, modelName)
			if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(firstRequest)); errWrite != nil {
				t.Fatalf("write first websocket message: %v", errWrite)
			}
			if _, payload, errRead := conn.ReadMessage(); errRead != nil || gjson.GetBytes(payload, "type").String() != wsEventTypeCompleted {
				t.Fatalf("first websocket response = %s, err=%v", payload, errRead)
			}

			secondRequest := `{"type":"response.create","previous_response_id":"resp-auth-a-1","input":[{"type":"message","id":"msg-2"}]}`
			if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(secondRequest)); errWrite != nil {
				t.Fatalf("write second websocket message: %v", errWrite)
			}
			_, _, errReadClose := conn.ReadMessage()
			var replayClose *websocket.CloseError
			if !errors.As(errReadClose, &replayClose) || replayClose.Code != websocket.CloseServiceRestart || replayClose.Text != wsHTTPReplayRequiredCloseReason {
				t.Fatalf("credential failure response = %v, want replay close %d %q", errReadClose, websocket.CloseServiceRestart, wsHTTPReplayRequiredCloseReason)
			}
			if got := executor.AuthIDs(); len(got) != 2 || got[0] != "auth-a" || got[1] != "auth-a" {
				t.Fatalf("selected auth IDs before replay = %v, want [auth-a auth-a]", got)
			}

			replayConn, _, errDialReplay := websocket.DefaultDialer.Dial(wsURL, nil)
			if errDialReplay != nil {
				t.Fatalf("dial replay websocket: %v", errDialReplay)
			}
			defer func() { _ = replayConn.Close() }()
			fullReplay := fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","id":"msg-1"},{"type":"message","id":"out-auth-a-1"},{"type":"message","id":"msg-2"}]}`, modelName)
			if errWrite := replayConn.WriteMessage(websocket.TextMessage, []byte(fullReplay)); errWrite != nil {
				t.Fatalf("write full replay: %v", errWrite)
			}
			if _, replayPayload, errReadReplay := replayConn.ReadMessage(); errReadReplay != nil || gjson.GetBytes(replayPayload, "type").String() != wsEventTypeCompleted {
				t.Fatalf("full replay response = %s, err=%v", replayPayload, errReadReplay)
			}
			if got := executor.AuthIDs(); len(got) != 3 || got[2] != "auth-b" {
				t.Fatalf("selected auth IDs after replay = %v, want [auth-a auth-a auth-b]", got)
			}
			authBPayloads := executor.Payloads("auth-b")
			if len(authBPayloads) != 1 {
				t.Fatalf("auth-b payloads = %d, want 1", len(authBPayloads))
			}
			authBPayload := authBPayloads[0]
			if gjson.GetBytes(authBPayload, "previous_response_id").Exists() || len(gjson.GetBytes(authBPayload, "input").Array()) != 3 {
				t.Fatalf("auth-b did not receive full replay: %s", authBPayload)
			}
		})
	}
}

func TestShouldReplayResponsesWebsocketPinnedAuthFailure(t *testing.T) {
	cases := []struct {
		name string
		err  *interfaces.ErrorMessage
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "unauthorized", err: &interfaces.ErrorMessage{StatusCode: http.StatusUnauthorized}, want: true},
		{name: "rate limit", err: &interfaces.ErrorMessage{StatusCode: http.StatusTooManyRequests}, want: true},
		{name: "forbidden", err: &interfaces.ErrorMessage{StatusCode: http.StatusForbidden}, want: false},
		{name: "service unavailable", err: &interfaces.ErrorMessage{StatusCode: http.StatusServiceUnavailable}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldReplayResponsesWebsocketPinnedAuthFailure(tc.err); got != tc.want {
				t.Fatalf("shouldReplayResponsesWebsocketPinnedAuthFailure() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestShouldReleaseResponsesWebsocketPinnedAuth(t *testing.T) {
	cases := []struct {
		name string
		err  *interfaces.ErrorMessage
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "request timeout", err: &interfaces.ErrorMessage{StatusCode: http.StatusRequestTimeout, Error: fmt.Errorf("stream closed before response.completed")}, want: true},
		{name: "service unavailable", err: &interfaces.ErrorMessage{StatusCode: http.StatusServiceUnavailable, Error: fmt.Errorf("websocket bootstrap failed")}, want: true},
		{name: "bad request", err: &interfaces.ErrorMessage{StatusCode: http.StatusBadRequest, Error: fmt.Errorf("invalid request")}, want: false},
		{name: "previous response missing", err: &interfaces.ErrorMessage{StatusCode: http.StatusBadRequest, Error: fmt.Errorf("previous_response_not_found")}, want: true},
		{name: "empty stream", err: &interfaces.ErrorMessage{StatusCode: http.StatusInternalServerError, Error: fmt.Errorf("empty_stream: upstream stream closed before first payload")}, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldReleaseResponsesWebsocketPinnedAuth(tc.err); got != tc.want {
				t.Fatalf("shouldReleaseResponsesWebsocketPinnedAuth() = %v, want %v", got, tc.want)
			}
		})
	}
}

type websocketPinnedPrematureCloseExecutor struct {
	mu       sync.Mutex
	authIDs  []string
	calls    map[string]int
	payloads map[string][][]byte
}

func (e *websocketPinnedPrematureCloseExecutor) Identifier() string { return "xai" }

func (e *websocketPinnedPrematureCloseExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketPinnedPrematureCloseExecutor) ExecuteStream(_ context.Context, auth *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	authID := ""
	if auth != nil {
		authID = auth.ID
	}

	e.mu.Lock()
	if e.calls == nil {
		e.calls = make(map[string]int)
	}
	if e.payloads == nil {
		e.payloads = make(map[string][][]byte)
	}
	e.authIDs = append(e.authIDs, authID)
	e.calls[authID]++
	call := e.calls[authID]
	e.payloads[authID] = append(e.payloads[authID], bytes.Clone(req.Payload))
	e.mu.Unlock()

	if authID == "auth-a" && call == 2 {
		chunks := make(chan coreexecutor.StreamChunk, 1)
		chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.output_item.added","item":{"id":"partial-1","type":"message"}}`)}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}

	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Payload: []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp-%s-%d","output":[{"type":"message","id":"out-%s-%d"}]}}`, authID, call, authID, call))}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *websocketPinnedPrematureCloseExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *websocketPinnedPrematureCloseExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *websocketPinnedPrematureCloseExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *websocketPinnedPrematureCloseExecutor) AuthIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.authIDs...)
}

func (e *websocketPinnedPrematureCloseExecutor) Payloads(authID string) [][]byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	src := e.payloads[authID]
	out := make([][]byte, len(src))
	for i := range src {
		out[i] = bytes.Clone(src[i])
	}
	return out
}

func TestResponsesWebsocketReleasesPinnedAuthAfterStreamClosed408(t *testing.T) {
	gin.SetMode(gin.TestMode)

	selector := &orderedWebsocketSelector{order: []string{"auth-a", "auth-b"}}
	executor := &websocketPinnedPrematureCloseExecutor{}
	manager := coreauth.NewManager(nil, selector, nil)
	manager.RegisterExecutor(executor)

	authA := &coreauth.Auth{
		ID:         "auth-a",
		Provider:   executor.Identifier(),
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), authA); err != nil {
		t.Fatalf("Register auth A: %v", err)
	}
	authB := &coreauth.Auth{
		ID:         "auth-b",
		Provider:   executor.Identifier(),
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"websockets": "true"},
	}
	if _, err := manager.Register(context.Background(), authB); err != nil {
		t.Fatalf("Register auth B: %v", err)
	}

	registry.GetGlobalRegistry().RegisterClient(authA.ID, authA.Provider, []*registry.ModelInfo{{ID: "stream-model"}})
	registry.GetGlobalRegistry().RegisterClient(authB.ID, authB.Provider, []*registry.ModelInfo{{ID: "stream-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(authA.ID)
		registry.GetGlobalRegistry().UnregisterClient(authB.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	firstRequest := `{"type":"response.create","model":"stream-model","input":[{"type":"message","id":"msg-1"}]}`
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(firstRequest)); errWrite != nil {
		t.Fatalf("write first websocket message: %v", errWrite)
	}
	if _, payload, errRead := conn.ReadMessage(); errRead != nil || gjson.GetBytes(payload, "type").String() != wsEventTypeCompleted {
		t.Fatalf("first websocket response = %s, err=%v", payload, errRead)
	}

	secondRequest := `{"type":"response.create","previous_response_id":"resp-auth-a-1","input":[{"type":"message","id":"msg-2"}]}`
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(secondRequest)); errWrite != nil {
		t.Fatalf("write second websocket message: %v", errWrite)
	}
	for {
		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Fatalf("read stream-closed response: %v", errRead)
		}
		eventType := gjson.GetBytes(payload, "type").String()
		if eventType == wsEventTypeError {
			if got := int(gjson.GetBytes(payload, "status").Int()); got != http.StatusRequestTimeout {
				t.Fatalf("stream-closed status = %d, want %d: %s", got, http.StatusRequestTimeout, payload)
			}
			break
		}
		if eventType == wsEventTypeCompleted {
			t.Fatalf("stream-closed turn unexpectedly completed: %s", payload)
		}
	}

	thirdDelta := `{"type":"response.create","previous_response_id":"resp-auth-a-1","input":[{"type":"message","id":"msg-3"}]}`
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(thirdDelta)); errWrite != nil {
		t.Fatalf("write third websocket message: %v", errWrite)
	}
	_, _, errReadClose := conn.ReadMessage()
	var replayClose *websocket.CloseError
	if !errors.As(errReadClose, &replayClose) || replayClose.Code != websocket.CloseServiceRestart {
		t.Fatalf("third websocket response error = %v, want replay close", errReadClose)
	}
	if got := executor.AuthIDs(); len(got) != 2 || got[0] != "auth-a" || got[1] != "auth-a" {
		t.Fatalf("selected auth IDs before replay = %v, want [auth-a auth-a]", got)
	}

	replayConn, _, errDialReplay := websocket.DefaultDialer.Dial(wsURL, nil)
	if errDialReplay != nil {
		t.Fatalf("dial replay websocket: %v", errDialReplay)
	}
	defer func() { _ = replayConn.Close() }()
	fullReplay := `{"type":"response.create","model":"stream-model","input":[{"type":"message","id":"msg-1"},{"type":"message","id":"out-auth-a-1"},{"type":"message","id":"msg-3"}]}`
	if errWrite := replayConn.WriteMessage(websocket.TextMessage, []byte(fullReplay)); errWrite != nil {
		t.Fatalf("write full replay: %v", errWrite)
	}
	if _, replayResponse, errReadReplay := replayConn.ReadMessage(); errReadReplay != nil || gjson.GetBytes(replayResponse, "type").String() != wsEventTypeCompleted {
		t.Fatalf("full replay response = %s, err=%v", replayResponse, errReadReplay)
	}
	authIDs := executor.AuthIDs()
	if len(authIDs) != 3 || authIDs[0] != "auth-a" || authIDs[1] != "auth-a" {
		t.Fatalf("selected auth IDs after replay = %v, want auth-a for the first two turns", authIDs)
	}
	replayAuthID := authIDs[2]
	replayPayloads := executor.Payloads(replayAuthID)
	replayPayload := replayPayloads[len(replayPayloads)-1]
	if gjson.GetBytes(replayPayload, "previous_response_id").Exists() || len(gjson.GetBytes(replayPayload, "input").Array()) != 3 {
		t.Fatalf("replay auth %s did not receive full replay: %s", replayAuthID, replayPayload)
	}
}

func TestNormalizeResponsesWebsocketRequestTreatsTranscriptReplacementAsReset(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"input":[{"type":"message","id":"msg-1"},{"type":"function_call","id":"fc-1","call_id":"call-1"},{"type":"function_call_output","id":"tool-out-1","call_id":"call-1"},{"type":"message","id":"assistant-1","role":"assistant"}]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","id":"assistant-1","role":"assistant"}
	]`)
	raw := []byte(`{"type":"response.create","input":[{"type":"function_call","id":"fc-compact","call_id":"call-1","name":"tool"},{"type":"message","id":"msg-2"}]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequest(raw, lastRequest, lastResponseOutput)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id must not exist in transcript replacement mode")
	}
	items := gjson.GetBytes(normalized, "input").Array()
	if len(items) != 2 {
		t.Fatalf("replacement input len = %d, want 2: %s", len(items), normalized)
	}
	if items[0].Get("id").String() != "fc-compact" || items[1].Get("id").String() != "msg-2" {
		t.Fatalf("replacement transcript was not preserved as-is: %s", normalized)
	}
	if !bytes.Equal(next, normalized) {
		t.Fatalf("next request snapshot should match replacement request")
	}
}

func TestNormalizeResponsesWebsocketRequestDoesNotTreatDeveloperMessageAsReplacement(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"input":[{"type":"message","id":"msg-1"}]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","id":"assistant-1","role":"assistant"}
	]`)
	raw := []byte(`{"type":"response.create","input":[{"type":"message","id":"dev-1","role":"developer"},{"type":"message","id":"msg-2"}]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequest(raw, lastRequest, lastResponseOutput)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	items := gjson.GetBytes(normalized, "input").Array()
	if len(items) != 4 {
		t.Fatalf("merged input len = %d, want 4: %s", len(items), normalized)
	}
	if items[0].Get("id").String() != "msg-1" ||
		items[1].Get("id").String() != "assistant-1" ||
		items[2].Get("id").String() != "dev-1" ||
		items[3].Get("id").String() != "msg-2" {
		t.Fatalf("developer follow-up should preserve merge behavior: %s", normalized)
	}
	if !bytes.Equal(next, normalized) {
		t.Fatalf("next request snapshot should match merged request")
	}
}

func TestNormalizeResponsesWebsocketRequestDropsDuplicateFunctionCallsByCallID(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"input":[{"type":"function_call","id":"fc-1","call_id":"call-1"},{"type":"function_call_output","id":"tool-out-1","call_id":"call-1"}]}`)
	lastResponseOutput := []byte(`[
		{"type":"function_call","id":"fc-1","call_id":"call-1","name":"tool"}
	]`)
	raw := []byte(`{"type":"response.create","input":[{"type":"message","id":"msg-2"}]}`)

	normalized, _, errMsg := normalizeResponsesWebsocketRequest(raw, lastRequest, lastResponseOutput)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}

	items := gjson.GetBytes(normalized, "input").Array()
	if len(items) != 3 {
		t.Fatalf("merged input len = %d, want 3: %s", len(items), normalized)
	}
	if items[0].Get("id").String() != "fc-1" ||
		items[1].Get("id").String() != "tool-out-1" ||
		items[2].Get("id").String() != "msg-2" {
		t.Fatalf("unexpected merged input order: %s", normalized)
	}
}

func TestNormalizeResponsesWebsocketRequestDropsDuplicateInputItemsByID(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"input":[{"type":"message","id":"msg-1","role":"user"}]}`)
	lastResponseOutput := []byte(`[
		{"type":"function_call","id":"fc-1","call_id":"call-1","name":"tool"}
	]`)
	raw := []byte(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"function_call","id":"fc-1","call_id":"call-2","name":"tool"},{"type":"function_call_output","id":"tool-out-1","call_id":"call-2"}]}`)

	normalized, _, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, lastRequest, lastResponseOutput, false, true, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}

	items := gjson.GetBytes(normalized, "input").Array()
	if len(items) != 3 {
		t.Fatalf("merged input len = %d, want 3: %s", len(items), normalized)
	}
	if items[0].Get("id").String() != "msg-1" ||
		items[1].Get("id").String() != "fc-1" ||
		items[1].Get("call_id").String() != "call-2" ||
		items[2].Get("id").String() != "tool-out-1" {
		t.Fatalf("unexpected merged input order: %s", normalized)
	}
}

func TestNormalizeResponsesWebsocketRequestTreatsCustomToolTranscriptReplacementAsReset(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"input":[{"type":"message","id":"msg-1"},{"type":"custom_tool_call","id":"ctc-1","call_id":"call-1","name":"apply_patch"},{"type":"custom_tool_call_output","id":"tool-out-1","call_id":"call-1"},{"type":"message","id":"assistant-1","role":"assistant"}]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","id":"assistant-1","role":"assistant"}
	]`)
	raw := []byte(`{"type":"response.create","input":[{"type":"custom_tool_call","id":"ctc-compact","call_id":"call-1","name":"apply_patch"},{"type":"custom_tool_call_output","id":"tool-out-compact","call_id":"call-1"},{"type":"message","id":"msg-2"}]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequest(raw, lastRequest, lastResponseOutput)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id must not exist in transcript replacement mode")
	}
	items := gjson.GetBytes(normalized, "input").Array()
	if len(items) != 3 {
		t.Fatalf("replacement input len = %d, want 3: %s", len(items), normalized)
	}
	if items[0].Get("id").String() != "ctc-compact" ||
		items[1].Get("id").String() != "tool-out-compact" ||
		items[2].Get("id").String() != "msg-2" {
		t.Fatalf("replacement transcript was not preserved as-is: %s", normalized)
	}
	if !bytes.Equal(next, normalized) {
		t.Fatalf("next request snapshot should match replacement request")
	}
}

func TestNormalizeResponsesWebsocketRequestDropsDuplicateCustomToolCallsByCallID(t *testing.T) {
	lastRequest := []byte(`{"model":"test-model","stream":true,"input":[{"type":"custom_tool_call","id":"ctc-1","call_id":"call-1","name":"apply_patch"},{"type":"custom_tool_call_output","id":"tool-out-1","call_id":"call-1"}]}`)
	lastResponseOutput := []byte(`[
		{"type":"custom_tool_call","id":"ctc-1","call_id":"call-1","name":"apply_patch"}
	]`)
	raw := []byte(`{"type":"response.create","input":[{"type":"message","id":"msg-2"}]}`)

	normalized, _, errMsg := normalizeResponsesWebsocketRequest(raw, lastRequest, lastResponseOutput)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}

	items := gjson.GetBytes(normalized, "input").Array()
	if len(items) != 3 {
		t.Fatalf("merged input len = %d, want 3: %s", len(items), normalized)
	}
	if items[0].Get("id").String() != "ctc-1" ||
		items[1].Get("id").String() != "tool-out-1" ||
		items[2].Get("id").String() != "msg-2" {
		t.Fatalf("unexpected merged input order: %s", normalized)
	}
}

func TestDedupeResponsesWebsocketInputItemsByIDAfterRepair(t *testing.T) {
	payload := []byte(`{"input":[{"type":"custom_tool_call","id":"ctc-1","call_id":"call-1","name":"tool"},{"type":"custom_tool_call","id":"ctc-1","call_id":"call-2","name":"tool"},{"type":"custom_tool_call_output","id":"tool-out-1","call_id":"call-2"}]}`)

	deduped := dedupeResponsesWebsocketInputItemsByID(payload)

	items := gjson.GetBytes(deduped, "input").Array()
	if len(items) != 2 {
		t.Fatalf("deduped input len = %d, want 2: %s", len(items), deduped)
	}
	if items[0].Get("id").String() != "ctc-1" ||
		items[0].Get("call_id").String() != "call-2" ||
		items[1].Get("id").String() != "tool-out-1" {
		t.Fatalf("unexpected deduped input: %s", deduped)
	}
}

func TestDedupeResponsesWebsocketInputItemsByIDKeepsReferencedToolCall(t *testing.T) {
	// Two function_call items share the same id but carry different call_ids
	// (e.g. the upstream reused the item id across a re-sent/repaired call).
	// Only the first call_id has a matching function_call_output. Deduping by
	// id must keep the referenced call so the output is not orphaned, which
	// previously triggered an upstream 400 "No tool call found for function
	// call output with call_id ...".
	payload := []byte(`{"input":[{"type":"function_call","id":"fc-1","call_id":"call-1","name":"exec_command"},{"type":"function_call","id":"fc-1","call_id":"call-2","name":"exec_command"},{"type":"function_call_output","id":"fco-1","call_id":"call-1"}]}`)

	deduped := dedupeResponsesWebsocketInputItemsByID(payload)

	items := gjson.GetBytes(deduped, "input").Array()
	if len(items) != 2 {
		t.Fatalf("deduped input len = %d, want 2: %s", len(items), deduped)
	}
	if items[0].Get("id").String() != "fc-1" ||
		items[0].Get("call_id").String() != "call-1" ||
		items[1].Get("id").String() != "fco-1" ||
		items[1].Get("call_id").String() != "call-1" {
		t.Fatalf("unexpected deduped input: %s", deduped)
	}
}

func TestDedupeResponsesWebsocketInputItemsByIDKeepsReferencedFunctionCallPairs(t *testing.T) {
	payload := []byte(`{"input":[{"type":"function_call","id":"fc-1","call_id":"call-1","name":"exec_command"},{"type":"function_call_output","id":"fco-1","call_id":"call-1"},{"type":"function_call","id":"fc-1","call_id":"call-2","name":"exec_command"},{"type":"function_call_output","id":"fco-2","call_id":"call-2"}]}`)

	deduped := dedupeResponsesWebsocketInputItemsByID(payload)

	items := gjson.GetBytes(deduped, "input").Array()
	if len(items) != 4 {
		t.Fatalf("deduped input len = %d, want 4: %s", len(items), deduped)
	}
	if items[0].Get("type").String() != "function_call" ||
		items[0].Get("id").Exists() ||
		items[0].Get("call_id").String() != "call-1" ||
		items[1].Get("type").String() != "function_call_output" ||
		items[1].Get("id").String() != "fco-1" ||
		items[1].Get("call_id").String() != "call-1" ||
		items[2].Get("type").String() != "function_call" ||
		items[2].Get("id").String() != "fc-1" ||
		items[2].Get("call_id").String() != "call-2" ||
		items[3].Get("type").String() != "function_call_output" ||
		items[3].Get("id").String() != "fco-2" ||
		items[3].Get("call_id").String() != "call-2" {
		t.Fatalf("unexpected deduped input: %s", deduped)
	}
}

func TestDedupeResponsesWebsocketInputItemsByIDKeepsReferencedCustomToolCallPairs(t *testing.T) {
	payload := []byte(`{"input":[{"type":"custom_tool_call","id":"ctc-1","call_id":"call-1","name":"apply_patch"},{"type":"custom_tool_call_output","id":"ctco-1","call_id":"call-1"},{"type":"custom_tool_call","id":"ctc-1","call_id":"call-2","name":"apply_patch"},{"type":"custom_tool_call_output","id":"ctco-2","call_id":"call-2"}]}`)

	deduped := dedupeResponsesWebsocketInputItemsByID(payload)

	items := gjson.GetBytes(deduped, "input").Array()
	if len(items) != 4 {
		t.Fatalf("deduped input len = %d, want 4: %s", len(items), deduped)
	}
	if items[0].Get("type").String() != "custom_tool_call" ||
		items[0].Get("id").Exists() ||
		items[0].Get("call_id").String() != "call-1" ||
		items[1].Get("type").String() != "custom_tool_call_output" ||
		items[1].Get("id").String() != "ctco-1" ||
		items[1].Get("call_id").String() != "call-1" ||
		items[2].Get("type").String() != "custom_tool_call" ||
		items[2].Get("id").String() != "ctc-1" ||
		items[2].Get("call_id").String() != "call-2" ||
		items[3].Get("type").String() != "custom_tool_call_output" ||
		items[3].Get("id").String() != "ctco-2" ||
		items[3].Get("call_id").String() != "call-2" {
		t.Fatalf("unexpected deduped input: %s", deduped)
	}
}

func TestResponsesWebsocketCompactionResetsTurnStateOnCustomToolTranscriptReplacement(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketCompactionCaptureExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "auth-sse", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)
	router.POST("/v1/responses/compact", h.Compact)

	server := httptest.NewServer(router)
	defer server.Close()

	sessionID := "session-custom-tool-compact"
	threadID := "thread-custom-tool-compact"
	wsHeaders := http.Header{}
	wsHeaders.Set("X-Client-Request-Id", threadID)
	wsHeaders.Set("Session-Id", sessionID)
	wsHeaders.Set("Thread-Id", threadID)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, wsHeaders)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	requests := []string{
		`{"type":"response.create","model":"test-model","input":[{"type":"message","id":"msg-1"}]}`,
		`{"type":"response.create","input":[{"type":"custom_tool_call_output","call_id":"call-1","id":"tool-out-1"}]}`,
	}
	for i := range requests {
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(requests[i])); errWrite != nil {
			t.Fatalf("write websocket message %d: %v", i+1, errWrite)
		}
		_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
		if errReadMessage != nil {
			t.Fatalf("read websocket message %d: %v", i+1, errReadMessage)
		}
		if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
			t.Fatalf("message %d payload type = %s, want %s", i+1, got, wsEventTypeCompleted)
		}
	}

	compactReq, errReq := http.NewRequest(
		http.MethodPost,
		server.URL+"/v1/responses/compact",
		strings.NewReader(`{"model":"test-model","input":[{"type":"message","id":"summary-1"}]}`),
	)
	if errReq != nil {
		t.Fatalf("create compact request: %v", errReq)
	}
	compactReq.Header.Set("Content-Type", "application/json")
	compactReq.Header.Set("Session-Id", sessionID)
	compactReq.Header.Set("Thread-Id", threadID)
	compactResp, errPost := server.Client().Do(compactReq)
	if errPost != nil {
		t.Fatalf("compact request failed: %v", errPost)
	}
	if errClose := compactResp.Body.Close(); errClose != nil {
		t.Fatalf("close compact response body: %v", errClose)
	}
	if compactResp.StatusCode != http.StatusOK {
		t.Fatalf("compact status = %d, want %d", compactResp.StatusCode, http.StatusOK)
	}

	postCompact := `{"type":"response.create","input":[{"type":"custom_tool_call","id":"ctc-compact","call_id":"call-1","name":"apply_patch"},{"type":"custom_tool_call_output","id":"tool-out-compact","call_id":"call-1"},{"type":"message","id":"msg-2"}]}`
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(postCompact)); errWrite != nil {
		t.Fatalf("write post-compact websocket message: %v", errWrite)
	}
	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read post-compact websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("post-compact payload type = %s, want %s", got, wsEventTypeCompleted)
	}

	executor.mu.Lock()
	defer executor.mu.Unlock()

	if executor.compactPayload == nil {
		t.Fatalf("compact payload was not captured")
	}
	if len(executor.streamPayloads) != 3 {
		t.Fatalf("stream payload count = %d, want 3", len(executor.streamPayloads))
	}
	if executor.dropCalls != 1 || len(executor.dropReasons) != 1 || executor.dropReasons[0] != "compact_replay" {
		t.Fatalf("drop upstream calls = %d reasons=%v, want one compact_replay", executor.dropCalls, executor.dropReasons)
	}

	merged := executor.streamPayloads[2]
	items := gjson.GetBytes(merged, "input").Array()
	if len(items) != 3 {
		t.Fatalf("merged input len = %d, want 3: %s", len(items), merged)
	}
	if items[0].Get("id").String() != "ctc-compact" ||
		items[1].Get("id").String() != "tool-out-compact" ||
		items[2].Get("id").String() != "msg-2" {
		t.Fatalf("unexpected post-compact input order: %s", merged)
	}
	if items[0].Get("call_id").String() != "call-1" {
		t.Fatalf("post-compact custom tool call id = %s, want call-1", items[0].Get("call_id").String())
	}
}

func TestResponsesWebsocketCompactionResetsTurnStateOnTranscriptReplacement(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketCompactionCaptureExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "auth-sse", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)
	router.POST("/v1/responses/compact", h.Compact)

	server := httptest.NewServer(router)
	defer server.Close()

	sessionID := "session-tool-compact"
	threadID := "thread-tool-compact"
	wsHeaders := http.Header{}
	wsHeaders.Set("X-Client-Request-Id", threadID)
	wsHeaders.Set("Session-Id", sessionID)
	wsHeaders.Set("Thread-Id", threadID)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, wsHeaders)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	requests := []string{
		`{"type":"response.create","model":"test-model","input":[{"type":"message","id":"msg-1"}]}`,
		`{"type":"response.create","input":[{"type":"function_call_output","call_id":"call-1","id":"tool-out-1"}]}`,
	}
	for i := range requests {
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(requests[i])); errWrite != nil {
			t.Fatalf("write websocket message %d: %v", i+1, errWrite)
		}
		_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
		if errReadMessage != nil {
			t.Fatalf("read websocket message %d: %v", i+1, errReadMessage)
		}
		if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
			t.Fatalf("message %d payload type = %s, want %s", i+1, got, wsEventTypeCompleted)
		}
	}

	compactReq, errReq := http.NewRequest(
		http.MethodPost,
		server.URL+"/v1/responses/compact",
		strings.NewReader(`{"model":"test-model","input":[{"type":"message","id":"summary-1"}]}`),
	)
	if errReq != nil {
		t.Fatalf("create compact request: %v", errReq)
	}
	compactReq.Header.Set("Content-Type", "application/json")
	compactReq.Header.Set("Session-Id", sessionID)
	compactReq.Header.Set("Thread-Id", threadID)
	compactResp, errPost := server.Client().Do(compactReq)
	if errPost != nil {
		t.Fatalf("compact request failed: %v", errPost)
	}
	if errClose := compactResp.Body.Close(); errClose != nil {
		t.Fatalf("close compact response body: %v", errClose)
	}
	if compactResp.StatusCode != http.StatusOK {
		t.Fatalf("compact status = %d, want %d", compactResp.StatusCode, http.StatusOK)
	}

	// Simulate a post-compaction client turn that replaces local history with a compacted transcript.
	// The websocket handler must treat this as a state reset, not append it to stale pre-compaction state.
	postCompact := `{"type":"response.create","input":[{"type":"function_call","id":"fc-compact","call_id":"call-1","name":"tool"},{"type":"message","id":"msg-2"}]}`
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(postCompact)); errWrite != nil {
		t.Fatalf("write post-compact websocket message: %v", errWrite)
	}
	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read post-compact websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("post-compact payload type = %s, want %s", got, wsEventTypeCompleted)
	}

	executor.mu.Lock()
	defer executor.mu.Unlock()

	if executor.compactPayload == nil {
		t.Fatalf("compact payload was not captured")
	}
	if len(executor.streamPayloads) != 3 {
		t.Fatalf("stream payload count = %d, want 3", len(executor.streamPayloads))
	}
	if executor.dropCalls != 1 || len(executor.dropReasons) != 1 || executor.dropReasons[0] != "compact_replay" {
		t.Fatalf("drop upstream calls = %d reasons=%v, want one compact_replay", executor.dropCalls, executor.dropReasons)
	}

	merged := executor.streamPayloads[2]
	items := gjson.GetBytes(merged, "input").Array()
	if len(items) != 2 {
		t.Fatalf("merged input len = %d, want 2: %s", len(items), merged)
	}
	if items[0].Get("id").String() != "fc-compact" ||
		items[1].Get("id").String() != "msg-2" {
		t.Fatalf("unexpected post-compact input order: %s", merged)
	}
	if items[0].Get("call_id").String() != "call-1" {
		t.Fatalf("post-compact function call id = %s, want call-1", items[0].Get("call_id").String())
	}
	for _, item := range items {
		if item.Get("id").String() == "tool-out-1" {
			t.Fatalf("post-compact replacement must not be repaired with stale pre-compact tool output: %s", merged)
		}
	}
}

func TestResponsesWebsocketMarkerlessFullTranscriptDropsUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketCompactionCaptureExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "auth-markerless-compact", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"test-model","input":[{"type":"message","role":"user","id":"msg-1","content":"old prompt"}]}`)); errWrite != nil {
		t.Fatalf("write initial websocket message: %v", errWrite)
	}
	_, payload, errReadMessage := readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read initial websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("initial payload type = %s, want %s", got, wsEventTypeCompleted)
	}

	markerlessCompact := fmt.Sprintf(
		`{"type":"response.create","input":[{"type":"message","role":"user","id":"summary-msg","content":[{"type":"input_text","text":%q}]},{"type":"message","role":"user","id":"msg-2","content":"question after compact"}]}`,
		codexLocalCompactionSummaryPrefix+"\ncompacted summary",
	)
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(markerlessCompact)); errWrite != nil {
		t.Fatalf("write markerless compact websocket message: %v", errWrite)
	}
	_, payload, errReadMessage = readResponsesWebsocketTestMessage(t, conn)
	if errReadMessage != nil {
		t.Fatalf("read markerless compact websocket message: %v", errReadMessage)
	}
	if got := gjson.GetBytes(payload, "type").String(); got != wsEventTypeCompleted {
		t.Fatalf("markerless compact payload type = %s, want %s", got, wsEventTypeCompleted)
	}

	executor.mu.Lock()
	defer executor.mu.Unlock()

	if len(executor.streamPayloads) != 2 {
		t.Fatalf("stream payload count = %d, want 2", len(executor.streamPayloads))
	}
	if executor.dropCalls != 1 || len(executor.dropReasons) != 1 || executor.dropReasons[0] != "compact_replay" {
		t.Fatalf("drop upstream calls = %d reasons=%v, want one compact_replay", executor.dropCalls, executor.dropReasons)
	}

	merged := executor.streamPayloads[1]
	items := gjson.GetBytes(merged, "input").Array()
	if len(items) != 2 {
		t.Fatalf("markerless compact input len = %d, want compact replacement items without stale merge: %s", len(items), merged)
	}
	if items[0].Get("id").String() != "summary-msg" || items[1].Get("id").String() != "msg-2" {
		t.Fatalf("unexpected markerless compact input: %s", merged)
	}
}

func TestResponsesWebsocketResponseProcessedControlFrameDoesNotExecuteStream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &websocketCompactionCaptureExecutor{processedCh: make(chan string, 1)}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "auth-response-processed", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register auth: %v", err)
	}

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses/ws", h.ResponsesWebsocket)

	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Fatalf("close websocket: %v", errClose)
		}
	}()

	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.processed","response_id":"resp-processed-1"}`)); errWrite != nil {
		t.Fatalf("write response.processed websocket message: %v", errWrite)
	}

	select {
	case got := <-executor.processedCh:
		if got != "resp-processed-1" {
			t.Fatalf("processed response id = %q, want resp-processed-1", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for response.processed control frame")
	}

	executor.mu.Lock()
	defer executor.mu.Unlock()
	if len(executor.streamPayloads) != 0 {
		t.Fatalf("response.processed must not execute stream, got %d calls", len(executor.streamPayloads))
	}
}

func TestInputContainsFullTranscriptFalseForAssistantMessageOnly(t *testing.T) {
	input := gjson.Parse(`[
		{"type":"message","role":"user","content":"hello"},
		{"type":"message","role":"assistant","content":"hi there"}
	]`)
	if inputContainsFullTranscript(input) {
		t.Fatal("assistant message alone must not be treated as full transcript")
	}
}

func TestInputContainsFullTranscriptDetectsCompactionReplayItem(t *testing.T) {
	for _, typ := range []string{"compaction", "compaction_summary", "context_compaction"} {
		input := gjson.Parse(`[{"type":"message","role":"user","content":"hello"},{"type":"` + typ + `","encrypted_content":"summary"}]`)
		if !inputContainsFullTranscript(input) {
			t.Fatalf("expected full transcript for type=%s", typ)
		}
	}
}

func TestInputContainsFullTranscriptFalseForCompactionTriggerOnly(t *testing.T) {
	input := gjson.Parse(`[{"type":"compaction_trigger"}]`)
	if inputContainsFullTranscript(input) {
		t.Fatal("compaction_trigger alone must not be treated as full transcript")
	}
}

func TestInputWithoutCompactionItemsPreservesCompactionTrigger(t *testing.T) {
	input := gjson.Parse(`[
		{"type":"message","role":"user","id":"msg-1","content":"hello"},
		{"type":"compaction_trigger","trigger":"auto"},
		{"type":"context_compaction","encrypted_content":"summary"}
	]`)

	filtered := inputWithoutCompactionItems(input)
	items := gjson.Parse(filtered).Array()
	if len(items) != 2 {
		t.Fatalf("filtered input len = %d, want 2: %s", len(items), filtered)
	}
	if items[1].Get("type").String() != "compaction_trigger" {
		t.Fatalf("compaction_trigger must be preserved for executor routing: %s", filtered)
	}
	if strings.Contains(filtered, "context_compaction") {
		t.Fatalf("context_compaction replay item must still be stripped: %s", filtered)
	}
}

func TestInputContainsFullTranscriptFalseForIncremental(t *testing.T) {
	// Normal incremental turns: user messages or function_call_output only.
	for _, raw := range []string{
		`[{"type":"function_call_output","call_id":"call-1","output":"result"}]`,
		`[{"type":"message","role":"user","content":"next question"}]`,
		`[]`,
	} {
		if inputContainsFullTranscript(gjson.Parse(raw)) {
			t.Fatalf("incremental input must not be detected as full transcript: %s", raw)
		}
	}
}

func TestResponsesWebsocketReplayBoundaryIgnoresNonSemanticControlEvents(t *testing.T) {
	if !responsesWebsocketPayloadPrecludesTranscriptReplay([]byte(`{"type":"response.created","response":{"id":"resp-1"}}`)) {
		t.Fatal("response.created must preclude transcript replay once forwarded")
	}
	if !responsesWebsocketPayloadPrecludesTranscriptReplay([]byte(`{"type":"response.in_progress","response":{"id":"resp-1"}}`)) {
		t.Fatal("response.in_progress must preclude transcript replay once forwarded")
	}
	if responsesWebsocketPayloadPrecludesTranscriptReplay([]byte(`{"type":"codex.rate_limits","rate_limits":[]}`)) {
		t.Fatal("codex.rate_limits must not preclude transcript replay")
	}
	if !responsesWebsocketPayloadPrecludesTranscriptReplay([]byte(`{"type":"response.output_item.done","item":{"id":"msg-1"}}`)) {
		t.Fatal("semantic output must preclude transcript replay")
	}
}

func TestResponsesWebsocketTurnStateOnlyMetadata(t *testing.T) {
	if !responsesWebsocketTurnStateOnlyMetadata([]byte(`{"type":"response.metadata","headers":{"x-codex-turn-state":"state-1"}}`)) {
		t.Fatal("pure turn-state metadata must be safe to buffer before the replay boundary")
	}
	richMetadata := []byte(`{"type":"response.metadata","headers":{"x-codex-turn-state":"state-1"},"metadata":{"openai_verification_recommendation":["verify"]}}`)
	if responsesWebsocketTurnStateOnlyMetadata(richMetadata) {
		t.Fatal("metadata with semantic fields must preclude transcript replay")
	}
}

func TestResponsesWebsocketTurnStateMetadataPayloadIsScopedAndDeduplicated(t *testing.T) {
	headers := http.Header{}
	headers.Set(wsTurnStateHeader, "state-1")
	headers.Set("Set-Cookie", "secret=value")
	payload := responsesWebsocketTurnStateMetadataPayload(headers, []byte(`{"type":"response.created"}`))
	if got := gjson.GetBytes(payload, "type").String(); got != "response.metadata" {
		t.Fatalf("metadata type = %q, want response.metadata: %s", got, payload)
	}
	metadataHeaders := gjson.GetBytes(payload, "headers").Map()
	if len(metadataHeaders) != 1 || metadataHeaders[wsTurnStateHeader].String() != "state-1" {
		t.Fatalf("metadata headers = %v, want only turn-state: %s", metadataHeaders, payload)
	}
	matching := []byte(`{"type":"response.metadata","headers":{"x-codex-turn-state":"state-1"}}`)
	if duplicate := responsesWebsocketTurnStateMetadataPayload(headers, matching); len(duplicate) != 0 {
		t.Fatalf("matching upstream metadata was duplicated: %s", duplicate)
	}
}

func TestResponsesWebsocketPreviousResponseReplayUsesStructuredErrorCode(t *testing.T) {
	errMsg := &interfaces.ErrorMessage{
		StatusCode: http.StatusBadRequest,
		Error:      errors.New(`{"error":{"code":"invalid_prompt","message":"previous response with id 'resp-1' not found"}}`),
	}
	if shouldRetryResponsesWebsocketTranscriptReplay(errMsg) {
		t.Fatal("structured non-previous-response error must not trigger transcript replay")
	}

	errMsg = &interfaces.ErrorMessage{
		StatusCode: http.StatusBadRequest,
		Error:      errors.New(`{"error":{"code":"previous_response_not_found","message":"previous response missing"}}`),
	}
	if !shouldRetryResponsesWebsocketTranscriptReplay(errMsg) {
		t.Fatal("structured previous_response_not_found error must trigger transcript replay")
	}

	errMsg = &interfaces.ErrorMessage{
		StatusCode: http.StatusBadRequest,
		Error:      errors.New(`{"error":{"type":"invalid_request_error","message":"No response found for previous_response_id resp_123"}}`),
	}
	if !shouldRetryResponsesWebsocketTranscriptReplay(errMsg) {
		t.Fatal("structured previous_response_id not found message must trigger transcript replay")
	}
}

func TestResponsesWebsocketConnectionLimitTriggersTranscriptReplay(t *testing.T) {
	errMsg := &interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      errors.New(`{"error":{"code":"websocket_connection_limit_reached","message":"too many websockets"}}`),
	}
	if !shouldRetryResponsesWebsocketTranscriptReplay(errMsg) {
		t.Fatal("websocket connection limit must trigger transcript replay")
	}

	errMsg = &interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      errors.New(`{"error":{"code":"rate_limit_exceeded","message":"too many requests"}}`),
	}
	if shouldRetryResponsesWebsocketTranscriptReplay(errMsg) {
		t.Fatal("ordinary rate limit must not trigger transcript replay")
	}
}

func TestResponsesWebsocketErrorPayloadPreservesStructuredCodeForReplay(t *testing.T) {
	payload := []byte(`{"type":"response.failed","response":{"error":{"code":"invalid_prompt","message":"previous response with id 'resp-1' not found"}}}`)
	errMsg := responsesWebsocketErrorMessageFromPayload(payload)
	if shouldRetryResponsesWebsocketTranscriptReplay(errMsg) {
		t.Fatal("response.failed with non-previous-response code must not trigger transcript replay")
	}

	payload = []byte(`{"type":"response.failed","response":{"error":{"code":"previous_response_not_found","message":"previous response missing"}}}`)
	errMsg = responsesWebsocketErrorMessageFromPayload(payload)
	if !shouldRetryResponsesWebsocketTranscriptReplay(errMsg) {
		t.Fatal("response.failed with previous_response_not_found code must trigger transcript replay")
	}

	payload = []byte(`{"type":"error","status":400,"code":"invalid_prompt","message":"previous response with id 'resp-1' not found"}`)
	errMsg = responsesWebsocketErrorMessageFromPayload(payload)
	if shouldRetryResponsesWebsocketTranscriptReplay(errMsg) {
		t.Fatal("top-level non-previous-response code must not trigger transcript replay")
	}
}

func TestResponsesWebsocketResponseFailedInfersAuthStatuses(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		status int
	}{
		{
			name:   "rate limit",
			raw:    `{"type":"response.failed","response":{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"too many requests"}}}`,
			status: http.StatusTooManyRequests,
		},
		{
			name:   "insufficient quota",
			raw:    `{"type":"response.failed","response":{"error":{"code":"insufficient_quota","message":"quota exceeded"}}}`,
			status: http.StatusTooManyRequests,
		},
		{
			name:   "authentication",
			raw:    `{"type":"response.failed","response":{"error":{"type":"authentication_error","message":"expired token"}}}`,
			status: http.StatusUnauthorized,
		},
		{
			name:   "permission",
			raw:    `{"type":"response.failed","response":{"error":{"type":"permission_error","message":"forbidden"}}}`,
			status: http.StatusForbidden,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errMsg := responsesWebsocketErrorMessageFromPayload([]byte(tt.raw))
			if errMsg.StatusCode != tt.status {
				t.Fatalf("status = %d, want %d", errMsg.StatusCode, tt.status)
			}
			if !shouldReleaseResponsesWebsocketPinnedAuth(errMsg) {
				t.Fatalf("status %d must release pinned auth", tt.status)
			}
		})
	}
}

func TestResponsesWebsocketResponseFailedInfersPolicyStatus(t *testing.T) {
	for _, code := range []string{"invalid_prompt", "bio_policy", "cyber_policy"} {
		t.Run(code, func(t *testing.T) {
			payload := []byte(fmt.Sprintf(`{"type":"response.failed","response":{"error":{"code":%q,"message":"policy rejection"}}}`, code))
			errMsg := responsesWebsocketErrorMessageFromPayload(payload)
			if errMsg.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", errMsg.StatusCode, http.StatusBadRequest)
			}
			if shouldReleaseResponsesWebsocketPinnedAuth(errMsg) {
				t.Fatal("request-scoped policy error must not release pinned auth")
			}
			if shouldRetryResponsesWebsocketTranscriptReplay(errMsg) {
				t.Fatal("request-scoped policy error must not trigger transcript replay")
			}
		})
	}
}

func TestNormalizeSubsequentRequestCompactSkipsMerge(t *testing.T) {
	lastRequest := []byte(`{"model":"gpt-5.4","stream":true,"input":[
		{"type":"message","role":"user","id":"msg-1","content":"original long prompt"},
		{"type":"message","role":"assistant","id":"msg-2","content":"original long response"},
		{"type":"function_call","id":"fc-1","call_id":"call-old","name":"bash","arguments":"{}"},
		{"type":"function_call_output","id":"fco-1","call_id":"call-old","output":"old result"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"msg-3","content":"another assistant reply"},
		{"type":"function_call","id":"fc-2","call_id":"call-stale","name":"read","arguments":"{}"}
	]`)

	// Remote compact response: user messages + compaction item, NO assistant message.
	// This is the primary compact scenario from Codex CLI.
	raw := []byte(`{"type":"response.create","input":[
		{"type":"message","role":"user","id":"msg-1c","content":"compacted user msg"},
		{"type":"compaction","encrypted_content":"conversation summary"}
	]}`)

	normalized, _, errMsg := normalizeResponsesWebsocketRequest(raw, lastRequest, lastResponseOutput)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}

	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 2 {
		t.Fatalf("input len = %d, want 2 (compacted only); stale state was not skipped", len(input))
	}
	if input[0].Get("id").String() != "msg-1c" {
		t.Fatalf("input[0].id = %q, want %q", input[0].Get("id").String(), "msg-1c")
	}
	if input[1].Get("type").String() != "compaction" {
		t.Fatalf("input[1].type = %q, want %q", input[1].Get("type").String(), "compaction")
	}
}

func TestNormalizeSubsequentRequestReasoningContinuationWithPreviousResponseID(t *testing.T) {
	lastRequest := []byte(`{"model":"gpt-5.6-terra","stream":true,"input":[{"type":"message","role":"user","id":"old-user","content":"long history"}]}`)
	lastResponseOutput := []byte(`[{"type":"function_call","id":"old-call","call_id":"old-call","name":"lookup","arguments":"{}"}]`)

	for _, requestType := range []string{"response.create", "response.append"} {
		t.Run(requestType, func(t *testing.T) {
			raw := []byte(`{"type":"` + requestType + `","previous_response_id":"resp-1","input":[
				{"type":"reasoning","id":"reasoning-1","summary":[]},
				{"type":"function_call_output","id":"output-1","call_id":"old-call","output":"result"}
			]}`)

			normalized, _, errMsg := normalizeResponsesWebsocketRequest(raw, lastRequest, lastResponseOutput)
			if errMsg != nil {
				t.Fatalf("unexpected error: %v", errMsg.Error)
			}
			if got := gjson.GetBytes(normalized, "previous_response_id").String(); got != "resp-1" {
				t.Fatalf("previous_response_id = %q, want resp-1; payload=%s", got, normalized)
			}
			input := gjson.GetBytes(normalized, "input").Array()
			if len(input) != 2 || input[0].Get("id").String() != "reasoning-1" || input[1].Get("id").String() != "output-1" {
				t.Fatalf("incremental continuation was replaced or merged: %s", normalized)
			}
		})
	}
}

func TestResponsesWebsocketOutputCollectorRestoresCompletedOutput(t *testing.T) {
	outputItemsByIndex := make(map[int64][]byte)
	var outputItemsFallback [][]byte
	for _, payload := range [][]byte{
		[]byte(`{"type":"response.output_item.done","output_index":1,"item":{"type":"message","id":"reply-1","role":"assistant"}}`),
		[]byte(`{"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"summary-1","summary":[]}}`),
		[]byte(`{"type":"response.output_item.done","item":{"type":"function_call","id":"call-1","call_id":"call-1","name":"exec","arguments":"{}"}}`),
	} {
		collectResponsesWebsocketOutputItemDone(payload, outputItemsByIndex, &outputItemsFallback)
	}

	output := responseCompletedOutputFromPayload(
		[]byte(`{"type":"response.completed","response":{"id":"resp-1","output":[]}}`),
		outputItemsByIndex,
		outputItemsFallback,
	)
	items := gjson.ParseBytes(output).Array()
	if len(items) != 3 {
		t.Fatalf("collected output len = %d, want 3: %s", len(items), output)
	}
	wantIDs := []string{"summary-1", "reply-1", "call-1"}
	for i, wantID := range wantIDs {
		if got := items[i].Get("id").String(); got != wantID {
			t.Fatalf("output[%d].id = %q, want %q: %s", i, got, wantID, output)
		}
	}
}

func TestNormalizeSubsequentRequestContextCompactionSkipsMergeAndPreviousResponseID(t *testing.T) {
	lastRequest := []byte(`{"model":"gpt-5.4","stream":true,"input":[
		{"type":"message","role":"user","id":"msg-1","content":"original long prompt"},
		{"type":"message","role":"assistant","id":"msg-2","content":"original long response"},
		{"type":"function_call","id":"fc-1","call_id":"call-old","name":"bash","arguments":"{}"},
		{"type":"function_call_output","id":"fco-1","call_id":"call-old","output":"old result"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"msg-3","content":"another assistant reply"},
		{"type":"function_call","id":"fc-2","call_id":"call-stale","name":"read","arguments":"{}"}
	]`)

	raw := []byte(`{"type":"response.create","previous_response_id":"resp-old","input":[
		{"type":"message","role":"user","id":"msg-1c","content":"compacted user msg"},
		{"type":"context_compaction","encrypted_content":"conversation summary"}
	]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, lastRequest, lastResponseOutput, true, true, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id must be removed for compact transcript replacement: %s", normalized)
	}
	if !bytes.Equal(next, normalized) {
		t.Fatalf("next request snapshot should match compact replacement")
	}

	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 2 {
		t.Fatalf("input len = %d, want 2 (compacted only); stale state was not skipped: %s", len(input), normalized)
	}
	if input[0].Get("id").String() != "msg-1c" {
		t.Fatalf("input[0].id = %q, want %q", input[0].Get("id").String(), "msg-1c")
	}
	if input[1].Get("type").String() != "context_compaction" {
		t.Fatalf("input[1].type = %q, want %q", input[1].Get("type").String(), "context_compaction")
	}
}

func TestNormalizeSubsequentRequestForcedPlainUserTranscriptReplacementSkipsMerge(t *testing.T) {
	lastRequest := []byte(`{"model":"gpt-5.4","stream":true,"input":[
		{"type":"message","role":"user","id":"msg-1","content":"original long prompt"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"msg-2","content":"old assistant output"}
	]`)
	raw := []byte(`{"type":"response.create","previous_response_id":"resp-old","input":[
		{"type":"message","role":"user","id":"summary-msg","content":"compacted summary"},
		{"type":"message","role":"user","id":"msg-3","content":"new question"}
	]}`)

	normalized, next, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, lastRequest, lastResponseOutput, true, true, true)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id must be removed for forced compact replacement: %s", normalized)
	}
	if !bytes.Equal(next, normalized) {
		t.Fatalf("next request snapshot should match forced compact replacement")
	}
	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 2 {
		t.Fatalf("input len = %d, want 2 replacement items; stale state was merged: %s", len(input), normalized)
	}
	if input[0].Get("id").String() != "summary-msg" ||
		input[1].Get("id").String() != "msg-3" {
		t.Fatalf("unexpected replacement input order: %s", normalized)
	}
}

func TestNormalizeSubsequentRequestCompactUnsupportedSkipsStaleMerge(t *testing.T) {
	lastRequest := []byte(`{"model":"gpt-5.4","stream":true,"input":[
		{"type":"message","role":"user","id":"msg-1","content":"original long prompt"},
		{"type":"message","role":"assistant","id":"msg-2","content":"original long response"},
		{"type":"function_call","id":"fc-1","call_id":"call-old","name":"bash","arguments":"{}"},
		{"type":"function_call_output","id":"fco-1","call_id":"call-old","output":"old result"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"msg-3","content":"another assistant reply"},
		{"type":"function_call","id":"fc-2","call_id":"call-stale","name":"read","arguments":"{}"}
	]`)
	raw := []byte(`{"type":"response.create","input":[
		{"type":"message","role":"user","id":"msg-1c","content":"compacted user msg"},
		{"type":"compaction","encrypted_content":"conversation summary"}
	]}`)

	normalized, _, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, lastRequest, lastResponseOutput, false, false, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}

	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 1 {
		t.Fatalf("input len = %d, want 1 compact replacement item without stale merge", len(input))
	}
	if input[0].Get("id").String() != "msg-1c" {
		t.Fatalf("input[0].id = %q, want msg-1c", input[0].Get("id").String())
	}
	for _, item := range input {
		if item.Get("type").String() == "compaction" || item.Get("type").String() == "compaction_summary" {
			t.Fatalf("compaction items must be stripped for unsupported downstream fallback: %s", item.Raw)
		}
	}
}

func TestNormalizeSubsequentRequestCompactUnsupportedWithAssistantStripsCompaction(t *testing.T) {
	lastRequest := []byte(`{"model":"gpt-5.4","stream":true,"input":[
		{"type":"message","role":"user","id":"msg-1","content":"original long prompt"},
		{"type":"message","role":"assistant","id":"msg-2","content":"original long response"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"msg-3","content":"stale assistant reply"}
	]`)
	raw := []byte(`{"type":"response.create","input":[
		{"type":"message","role":"user","id":"msg-1c","content":"compacted user msg"},
		{"type":"message","role":"assistant","id":"msg-2c","content":"compacted assistant msg"},
		{"type":"context_compaction","encrypted_content":"conversation summary"},
		{"type":"message","role":"user","id":"msg-4","content":"question after compact"}
	]}`)

	normalized, _, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, lastRequest, lastResponseOutput, false, false, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}

	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 3 {
		t.Fatalf("input len = %d, want 3 compact replacement items without stale merge: %s", len(input), normalized)
	}
	if input[0].Get("id").String() != "msg-1c" ||
		input[1].Get("id").String() != "msg-2c" ||
		input[2].Get("id").String() != "msg-4" {
		t.Fatalf("unexpected compact replacement input order: %s", normalized)
	}
	for _, item := range input {
		if isResponsesWebsocketCompactionItemType(item.Get("type").String()) {
			t.Fatalf("compaction items must be stripped before replacement heuristic: %s", normalized)
		}
		if item.Get("id").String() == "msg-1" || item.Get("id").String() == "msg-2" || item.Get("id").String() == "msg-3" {
			t.Fatalf("stale pre-compact state must not be merged into compact transcript: %s", normalized)
		}
	}
}

func TestNormalizeSubsequentRequestCompactUnsupportedDoesNotUsePreviousResponseID(t *testing.T) {
	lastRequest := []byte(`{"model":"gpt-5.4","stream":true,"input":[
		{"type":"message","role":"user","id":"msg-1","content":"original long prompt"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"msg-2","content":"another assistant reply"}
	]`)
	raw := []byte(`{"type":"response.create","previous_response_id":"resp-old","input":[
		{"type":"message","role":"user","id":"msg-1c","content":"compacted user msg"},
		{"type":"context_compaction","encrypted_content":"conversation summary"}
	]}`)

	normalized, _, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, lastRequest, lastResponseOutput, true, false, false)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id must be removed for unsupported compact fallback: %s", normalized)
	}
	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 1 {
		t.Fatalf("input len = %d, want 1 compact replacement item without stale merge: %s", len(input), normalized)
	}
	if input[0].Get("id").String() != "msg-1c" {
		t.Fatalf("input[0].id = %q, want msg-1c", input[0].Get("id").String())
	}
	for _, item := range input {
		if item.Get("type").String() == "context_compaction" {
			t.Fatalf("context_compaction item must be stripped for unsupported compact fallback: %s", normalized)
		}
	}
}

func TestNormalizeSubsequentRequestForcedCompactUnsupportedStripsCompaction(t *testing.T) {
	lastRequest := []byte(`{"model":"gpt-5.4","stream":true,"input":[
		{"type":"message","role":"user","id":"msg-1","content":"original long prompt"},
		{"type":"message","role":"assistant","id":"msg-2","content":"old response"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"msg-3","content":"another assistant reply"}
	]`)
	raw := []byte(`{"type":"response.create","previous_response_id":"resp-old","input":[
		{"type":"message","role":"user","id":"msg-1c","content":"compacted user msg"},
		{"type":"compaction","encrypted_content":"conversation summary"}
	]}`)

	normalized, _, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, lastRequest, lastResponseOutput, true, false, true)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	if gjson.GetBytes(normalized, "previous_response_id").Exists() {
		t.Fatalf("previous_response_id must be removed for forced compact fallback: %s", normalized)
	}
	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 1 {
		t.Fatalf("input len = %d, want 1 compact replacement item without stale merge: %s", len(input), normalized)
	}
	if input[0].Get("id").String() != "msg-1c" {
		t.Fatalf("input[0].id = %q, want msg-1c", input[0].Get("id").String())
	}
	for _, item := range input {
		if item.Get("type").String() == "compaction" {
			t.Fatalf("compaction item must be stripped for forced unsupported compact fallback: %s", normalized)
		}
	}
}

func TestNormalizeSubsequentRequestResponsesLiteFullCreateReplacesTranscript(t *testing.T) {
	lastRequest := []byte(`{"model":"gpt-5.6-sol","stream":true,"input":[
		{"type":"message","role":"user","id":"msg-old","content":"old branch"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"out-old","content":"old response"}
	]`)
	raw := []byte(`{
		"type":"response.create",
		"model":"gpt-5.6-sol",
		"input":[
			{"type":"additional_tools","role":"developer","tools":[]},
			{"type":"message","role":"user","id":"msg-new","content":"edited branch"}
		],
		"tool_choice":"auto",
		"parallel_tool_calls":false,
		"store":false,
		"stream":true,
		"include":[],
		"client_metadata":{
			"x-codex-installation-id":"install-1",
			"x-codex-window-id":"thread-1:1",
			"ws_request_header_x_openai_internal_codex_responses_lite":"true"
		}
	}`)

	normalized, _, errMsg := normalizeResponsesWebsocketRequest(raw, lastRequest, lastResponseOutput)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}
	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 2 {
		t.Fatalf("input len = %d, want 2 responses-lite replacement items: %s", len(input), normalized)
	}
	if input[0].Get("type").String() != "additional_tools" || input[1].Get("id").String() != "msg-new" {
		t.Fatalf("unexpected responses-lite replacement input: %s", normalized)
	}
}

func TestNormalizeSubsequentRequestIncrementalInputStillMerges(t *testing.T) {
	// Normal incremental flow: user sends function_call_output (no assistant message).
	lastRequest := []byte(`{"model":"gpt-5.4","stream":true,"input":[
		{"type":"message","role":"user","id":"msg-1","content":"hello"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"msg-2","content":"let me check"},
		{"type":"function_call","id":"fc-1","call_id":"call-1","name":"bash","arguments":"{}"}
	]`)
	raw := []byte(`{"type":"response.create","input":[
		{"type":"function_call_output","call_id":"call-1","id":"fco-1","output":"done"}
	]}`)

	normalized, _, errMsg := normalizeResponsesWebsocketRequest(raw, lastRequest, lastResponseOutput)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}

	input := gjson.GetBytes(normalized, "input").Array()

	// Should be merged: msg-1 + msg-2 + fc-1 + fco-1 = 4 items
	if len(input) != 4 {
		t.Fatalf("input len = %d, want 4 (merged)", len(input))
	}
	wantIDs := []string{"msg-1", "msg-2", "fc-1", "fco-1"}
	for i, want := range wantIDs {
		got := input[i].Get("id").String()
		if got != want {
			t.Fatalf("input[%d].id = %q, want %q", i, got, want)
		}
	}
}

func TestNormalizeSubsequentRequestAssistantInputTriggersTranscriptReplacement(t *testing.T) {
	// After dev's shouldReplaceWebsocketTranscript, assistant messages in input
	// trigger transcript replacement (no merge with prior state).
	lastRequest := []byte(`{"model":"gpt-5.4","stream":true,"input":[
		{"type":"message","role":"user","id":"msg-1","content":"hello"}
	]}`)
	lastResponseOutput := []byte(`[
		{"type":"message","role":"assistant","id":"msg-2","content":"prior assistant"},
		{"type":"function_call","id":"fc-1","call_id":"call-1","name":"bash","arguments":"{}"}
	]`)
	raw := []byte(`{"type":"response.append","input":[
		{"type":"message","role":"assistant","id":"msg-3","content":"patched assistant turn"}
	]}`)

	normalized, _, errMsg := normalizeResponsesWebsocketRequest(raw, lastRequest, lastResponseOutput)
	if errMsg != nil {
		t.Fatalf("unexpected error: %v", errMsg.Error)
	}

	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 1 {
		t.Fatalf("input len = %d, want 1 (transcript replacement, not merge)", len(input))
	}
	if input[0].Get("id").String() != "msg-3" {
		t.Fatalf("input[0].id = %q, want %q", input[0].Get("id").String(), "msg-3")
	}
}
