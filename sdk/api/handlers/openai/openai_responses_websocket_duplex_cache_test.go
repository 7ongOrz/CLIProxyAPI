package openai

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

func TestForwardResponsesWebsocketToolCacheOwnership(t *testing.T) {
	for _, duplex := range []bool{false, true} {
		t.Run(fmt.Sprintf("duplex=%t", duplex), func(t *testing.T) {
			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
			result := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
				if err != nil {
					result <- err
					return
				}
				defer func() { _ = conn.Close() }()
				ctx, _ := gin.CreateTestContext(w)
				ctx.Request = r
				turn := newResponsesWebsocketToolCacheTurn(t.Name())
				data := make(chan []byte, 4)
				for i := 0; i < 2; i++ {
					data <- []byte(fmt.Sprintf(`{"type":"response.created","response":{"id":"r%d"}}`, i))
					data <- []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"r%d","output":[{"type":"function_call","call_id":"c%d","name":"shell","arguments":"{}"}]}}`, i, i))
				}
				close(data)
				errs := make(chan *interfaces.ErrorMessage)
				close(errs)
				_, _, _, _, _, err = h.forwardResponsesWebsocket(ctx, newResponsesWebsocketWriter(conn), func(...interface{}) {}, data, errs, nil, newInMemoryWebsocketTimelineLog(), t.Name(), responsesWebsocketForwardOptions{
					duplexStream: func() bool { return duplex }, toolCacheTurn: turn,
				})
				if err != nil && !errors.Is(err, websocket.ErrCloseSent) {
					result <- err
					return
				}
				want := 1
				if duplex {
					want = 0
				}
				if len(turn.calls) != want {
					result <- fmt.Errorf("repair cache retained %d calls, want %d", len(turn.calls), want)
					return
				}
				result <- nil
			}))
			defer server.Close()
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			for {
				if _, _, err = readResponsesWebsocketTestMessage(t, conn); err != nil {
					break
				}
			}
			if err = <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}
