package executor

import (
	"context"
	"errors"
)

// ErrNoActiveUpstreamWebsocket means response.interrupt has no live upstream socket.
var ErrNoActiveUpstreamWebsocket = errors.New("no active upstream websocket for response.interrupt")

// WebsocketInput is a frame from the single downstream reader. Err terminates
// the connection; Payload is owned by the receiver and must not be replayed.
type WebsocketInput struct {
	Payload []byte
	Err     error
}
type websocketInputKey struct{}

func WithWebsocketInput(ctx context.Context, input <-chan WebsocketInput) context.Context {
	return context.WithValue(ctx, websocketInputKey{}, input)
}
func WebsocketInputFromContext(ctx context.Context) <-chan WebsocketInput {
	if ctx == nil {
		return nil
	}
	input, _ := ctx.Value(websocketInputKey{}).(<-chan WebsocketInput)
	return input
}

type websocketInterruptAckKey struct{}

// WithWebsocketInterruptAckHandler delivers control acknowledgements for the
// downstream connection, including those received after a response has ended.
// The handler owns downstream write failures and connection cancellation.
func WithWebsocketInterruptAckHandler(ctx context.Context, handler func([]byte)) context.Context {
	return context.WithValue(ctx, websocketInterruptAckKey{}, handler)
}

func WebsocketInterruptAckHandler(ctx context.Context) func([]byte) {
	if ctx == nil {
		return nil
	}
	handler, _ := ctx.Value(websocketInterruptAckKey{}).(func([]byte))
	return handler
}

// WithWebsocketAuthCheck supplies the live account-state check for a bound
// connection. It may reject further frames but never select another account.
type websocketAuthCheckKey struct{}

func WithWebsocketAuthCheck(ctx context.Context, check func(string) bool) context.Context {
	return context.WithValue(ctx, websocketAuthCheckKey{}, check)
}
func WebsocketAuthEnabled(ctx context.Context, authID string) bool {
	if ctx == nil {
		return true
	}
	check, _ := ctx.Value(websocketAuthCheckKey{}).(func(string) bool)
	return check == nil || check(authID)
}
