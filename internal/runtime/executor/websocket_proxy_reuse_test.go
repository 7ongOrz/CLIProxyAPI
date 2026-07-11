package executor

import (
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestWebsocketSessionIsolatesReusableConnectionByProxy(t *testing.T) {
	conn := &websocket.Conn{}
	closer := newWebsocketConnectionCloser(conn)
	sess := &codexWebsocketSession{
		authID:     "auth-1",
		wsURL:      "wss://upstream.example/v1",
		proxyURL:   "http://proxy-a.example:8081",
		conn:       conn,
		connCloser: closer,
	}

	if got, _ := existingWebsocketSessionConn(sess, "auth-1", sess.wsURL, "http://proxy-b.example:8082"); got != nil {
		t.Fatal("reused websocket after proxy_url changed")
	}
	if got, _ := existingWebsocketSessionConn(sess, "auth-1", sess.wsURL, ""); got != nil {
		t.Fatal("reused proxied websocket after proxy override was removed")
	}
	if got, _ := existingWebsocketSessionConn(sess, "auth-1", sess.wsURL, sess.proxyURL); got == nil {
		t.Fatal("did not reuse websocket for the same proxy")
	}
	if !websocketSessionTargetChanged(sess, "auth-1", sess.wsURL, "http://proxy-b.example:8082") {
		t.Fatal("proxy change was not treated as a websocket target change")
	}

	detached, detachedCloser, _, _, _ := detachMismatchedWebsocketSessionConn(sess, "auth-1", sess.wsURL, "")
	if detached == nil || detachedCloser == nil {
		t.Fatal("removing the proxy override did not detach the websocket")
	}
	if sess.conn != nil {
		t.Fatal("detached websocket remained attached")
	}
}

func TestCodexWebsocketProxyChangeAdvancesGeneration(t *testing.T) {
	server, closed := newWebsocketTargetServer(t)
	defer server.Close()
	exec := NewCodexWebsocketsExecutor(&config.Config{})
	exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	sess := exec.getOrCreateSession(t.Name())
	defer exec.CloseExecutionSession(sess.sessionID)
	auth := &cliproxyauth.Auth{ID: "auth-a"}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	ensureConn := codexEnsureUpstreamConnAdapter(exec)
	original := ensureWebsocketTargetConn(t, ensureConn, auth, sess, auth.ID, wsURL)

	auth.ProxyURL = "direct"
	replacement := ensureWebsocketTargetConn(t, ensureConn, auth, sess, auth.ID, wsURL)
	if replacement == original {
		t.Fatal("proxy change reused the original connection")
	}
	if got := exec.UpstreamGeneration(sess.sessionID); got != 1 {
		t.Fatalf("generation after proxy change = %d, want 1", got)
	}
	if got := <-closed; got != auth.ID {
		t.Fatalf("closed connection auth = %q, want %q", got, auth.ID)
	}
	if got := ensureWebsocketTargetConn(t, ensureConn, auth, sess, auth.ID, wsURL); got != replacement {
		t.Fatal("matching proxy should reuse the replacement connection")
	}
	if got := exec.UpstreamGeneration(sess.sessionID); got != 1 {
		t.Fatalf("generation after reuse = %d, want 1", got)
	}
}
