package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestCodexWebsocketsExecutor_CloseAllReleasesSessions(t *testing.T) {
	sessionID := "test-session-store-survives-replace"

	globalCodexWebsocketSessionStore.mu.Lock()
	delete(globalCodexWebsocketSessionStore.sessions, sessionID)
	globalCodexWebsocketSessionStore.mu.Unlock()

	exec1 := NewCodexWebsocketsExecutor(nil)
	sess1 := exec1.getOrCreateSession(sessionID)
	if sess1 == nil {
		t.Fatalf("expected session to be created")
	}

	exec2 := NewCodexWebsocketsExecutor(nil)
	sess2 := exec2.getOrCreateSession(sessionID)
	if sess2 == nil {
		t.Fatalf("expected session to be available across executors")
	}
	if sess1 != sess2 {
		t.Fatalf("expected the same session instance across executors")
	}

	exec1.CloseExecutionSession(cliproxyauth.CloseAllExecutionSessionsID)

	globalCodexWebsocketSessionStore.mu.Lock()
	_, stillPresent := globalCodexWebsocketSessionStore.sessions[sessionID]
	globalCodexWebsocketSessionStore.mu.Unlock()
	if stillPresent {
		t.Fatalf("expected session to be removed after executor shutdown")
	}

	exec2.CloseExecutionSession(sessionID)
}

func TestWebsocketSessionCloseRejectsPendingHandshake(t *testing.T) {
	for _, provider := range []string{"codex", "xai"} {
		t.Run(provider, func(t *testing.T) {
			handshakeStarted := make(chan struct{})
			releaseHandshake := make(chan struct{})
			upstreamClosed := make(chan struct{})
			resumeHandshake := sync.OnceFunc(func() { close(releaseHandshake) })
			upgrader := websocket.Upgrader{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(upstreamClosed)
				close(handshakeStarted)
				<-releaseHandshake
				conn, errUpgrade := upgrader.Upgrade(w, r, nil)
				if errUpgrade != nil {
					return
				}
				defer func() { _ = conn.Close() }()
				_, _, _ = conn.ReadMessage()
			}))
			defer server.Close()
			defer resumeHandshake()

			sess := &codexWebsocketSession{sessionID: "pending-handshake"}
			store := &codexWebsocketSessionStore{sessions: map[string]*codexWebsocketSession{sess.sessionID: sess}}
			wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
			var dial func(context.Context) (*websocket.Conn, *websocketConnectionCloser, *http.Response, error)
			var closeSessions func(string)
			if provider == "codex" {
				executor := NewCodexWebsocketsExecutor(nil)
				executor.store = store
				closeSessions = executor.CloseExecutionSession
				dial = func(ctx context.Context) (*websocket.Conn, *websocketConnectionCloser, *http.Response, error) {
					conn, closer, response, _, errDial := executor.ensureUpstreamConn(ctx, nil, sess, "auth", wsURL, nil)
					return conn, closer, response, errDial
				}
			} else {
				executor := NewXAIWebsocketsExecutor(nil)
				executor.store = store
				closeSessions = executor.CloseExecutionSession
				dial = func(ctx context.Context) (*websocket.Conn, *websocketConnectionCloser, *http.Response, error) {
					return executor.ensureUpstreamConn(ctx, nil, sess, "auth", wsURL, nil)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			type dialResult struct {
				conn     *websocket.Conn
				closer   *websocketConnectionCloser
				response *http.Response
				err      error
			}
			results := make(chan dialResult, 1)
			go func() {
				conn, closer, response, errDial := dial(ctx)
				results <- dialResult{conn, closer, response, errDial}
			}()
			select {
			case <-handshakeStarted:
			case <-ctx.Done():
				t.Fatal("upstream handshake did not start")
			}

			// Executor replacement closes all sessions while their handshakes may be in flight.
			closeSessions(cliproxyauth.CloseAllExecutionSessionsID)
			resumeHandshake()
			var result dialResult
			select {
			case result = <-results:
			case <-ctx.Done():
				t.Fatal("handshake completion timed out")
			}
			defer func() { _ = result.closer.Close() }()
			closeHTTPResponseBody(result.response, "test handshake response")
			if result.err == nil || !strings.Contains(result.err.Error(), "execution session closed") {
				t.Errorf("handshake error = %v, want execution session closed", result.err)
			}
			if result.conn != nil {
				t.Error("closed session returned a usable upstream connection")
			}
			if result.response != nil {
				t.Error("session closure returned an upstream HTTP response")
			}
			sess.connMu.Lock()
			retained := sess.conn
			sess.connMu.Unlock()
			if retained != nil {
				t.Error("closed session retained the late upstream connection")
			}
			select {
			case <-upstreamClosed:
			case <-ctx.Done():
				t.Fatal("late upstream connection remained open")
			}
		})
	}
}
