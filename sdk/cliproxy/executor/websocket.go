package executor

import (
	"context"
	"errors"
	"net/http"
)

// UpstreamWebsocketReplayRequiredError indicates that an incremental request
// cannot safely continue because its upstream websocket is no longer reusable.
type UpstreamWebsocketReplayRequiredError struct{}

func (*UpstreamWebsocketReplayRequiredError) Error() string {
	return `{"error":{"message":"upstream transport requires full HTTP replay","type":"server_error","code":"upstream_http_replay_required","status":426}}`
}

func (*UpstreamWebsocketReplayRequiredError) StatusCode() int { return http.StatusUpgradeRequired }

func (*UpstreamWebsocketReplayRequiredError) IsRequestScoped() bool { return true }

// NewUpstreamWebsocketReplayRequiredError creates a request-scoped replay signal.
func NewUpstreamWebsocketReplayRequiredError() error {
	return &UpstreamWebsocketReplayRequiredError{}
}

// IsUpstreamWebsocketReplayRequired reports whether err is the internal replay signal.
func IsUpstreamWebsocketReplayRequired(err error) bool {
	var replayErr *UpstreamWebsocketReplayRequiredError
	return errors.As(err, &replayErr)
}

// UpstreamWebsocketTurnError ends one downstream websocket turn after the upstream
// websocket was lost. The downstream socket stays open and the turn is never replayed.
// Cause is kept for logging and is never sent to the client.
type UpstreamWebsocketTurnError struct {
	Cause error
	// FullContextRequired marks a continuation (previous_response_id, append) that refers to
	// the lost socket: nothing was sent, the client resends the full input.
	FullContextRequired bool
	// NotSent marks an operation that was never written upstream.
	NotSent bool
	EventID string
	// Idle marks a loss with no running turn: only Abandoned is reported, if anything.
	Idle bool
	// Abandoned lists the other operations that lose their outcome with this turn.
	Abandoned []*UpstreamWebsocketTurnError
}

func (e *UpstreamWebsocketTurnError) Error() string {
	if e.FullContextRequired {
		return `{"error":{"message":"Previous response is not available on this websocket; resend the full conversation input without previous_response_id","type":"invalid_request_error","code":"previous_response_not_found","param":"previous_response_id"}}`
	}
	if e.NotSent {
		return `{"error":{"message":"Operation was not sent because the upstream websocket was lost. Send a new response.create with full conversation input.","type":"invalid_request_error","code":"upstream_websocket_not_sent"}}`
	}
	return `{"error":{"message":"Upstream websocket disconnected. The turn was not replayed and its outcome may be unknown. Send a new response.create with full conversation input without previous_response_id.","type":"server_error","code":"upstream_websocket_lost"}}`
}

func (e *UpstreamWebsocketTurnError) Unwrap() error { return e.Cause }
func (e *UpstreamWebsocketTurnError) StatusCode() int {
	if e.FullContextRequired {
		return http.StatusConflict
	}
	return http.StatusBadGateway
}
func (*UpstreamWebsocketTurnError) IsRequestScoped() bool { return true }

// IsUpstreamWebsocketTurnError reports whether err is a recoverable upstream websocket loss.
func IsUpstreamWebsocketTurnError(err error) bool {
	var turnErr *UpstreamWebsocketTurnError
	return errors.As(err, &turnErr)
}

type inheritedWebsocketInstructionsKey struct{}

// WithInheritedWebsocketInstructions distinguishes legacy transcript inheritance
// from instructions supplied by the current frame. Only opt-in consumers use it.
func WithInheritedWebsocketInstructions(ctx context.Context, raw string) context.Context {
	return context.WithValue(ctx, inheritedWebsocketInstructionsKey{}, raw)
}

// InheritedWebsocketInstructions returns the JSON value added by transcript rebuilding.
func InheritedWebsocketInstructions(ctx context.Context) string {
	raw, _ := ctx.Value(inheritedWebsocketInstructionsKey{}).(string)
	return raw
}

type rebuiltWebsocketContinuationKey struct{}

// WithRebuiltWebsocketContinuation records that the handler reconstructed an incremental frame.
func WithRebuiltWebsocketContinuation(ctx context.Context, rebuilt bool) context.Context {
	return context.WithValue(ctx, rebuiltWebsocketContinuationKey{}, rebuilt)
}

func RebuiltWebsocketContinuation(ctx context.Context) bool {
	rebuilt, _ := ctx.Value(rebuiltWebsocketContinuationKey{}).(bool)
	return rebuilt
}
