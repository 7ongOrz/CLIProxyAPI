package auth

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func TestExecuteStreamDeliversBufferedTerminalAfterAttemptEnds(t *testing.T) {
	for _, resultError := range []bool{false, true} {
		t.Run(fmt.Sprintf("result_error=%t", resultError), func(t *testing.T) {
			deliveryCtx, cancelDelivery := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelDelivery()
			attemptCtx, cancelAttempt := context.WithCancel(deliveryCtx)
			defer cancelAttempt()
			terminalErr := customStatusError{code: http.StatusBadGateway, msg: "upstream failed"}
			created := `{"type":"response.created"}`
			failed := `{"type":"response.failed","response":{"error":{"message":"upstream failed"}}}`
			executor := &customStreamMockExecutor{
				identifier: "codex",
				streamFn: func(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
					chunks := make(chan cliproxyexecutor.StreamChunk, 2)
					chunks <- cliproxyexecutor.StreamChunk{Payload: []byte(created)}
					terminal := cliproxyexecutor.StreamChunk{Err: terminalErr}
					if resultError {
						terminal = cliproxyexecutor.StreamChunk{Payload: []byte(failed), ResultErr: terminalErr}
					}
					chunks <- terminal
					close(chunks)
					cancelAttempt()
					return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
				},
			}
			manager := NewManager(nil, nil, nil)
			auth := &Auth{ID: "ended-attempt", Provider: "codex"}
			result, errStream := manager.executeStreamWithModelPool(attemptCtx, deliveryCtx, executor, auth, "codex",
				cliproxyexecutor.Request{Model: "model-a"}, cliproxyexecutor.Options{Stream: true},
				"model-a", "", []string{"model-a"}, false, OAuthModelAliasResult{}, nil, false, false)
			if errStream != nil {
				t.Fatalf("ExecuteStream: %v", errStream)
			}
			var chunks []cliproxyexecutor.StreamChunk
			for chunk := range result.Chunks {
				chunks = append(chunks, chunk)
			}
			if len(chunks) != 2 || string(chunks[0].Payload) != created {
				t.Fatalf("chunks = %+v, want handshake followed by terminal", chunks)
			}
			terminal := chunks[1]
			if resultError {
				if string(terminal.Payload) != failed || terminal.ResultStatusCode != http.StatusBadGateway || terminal.ResultErr != nil {
					t.Fatalf("terminal = %+v, want original payload with accounted status 502", terminal)
				}
			} else if terminal.Err != terminalErr {
				t.Fatalf("terminal error = %v, want %v", terminal.Err, terminalErr)
			}
		})
	}
}

func TestExecuteStreamOlderSuccessPreservesNewerCooldown(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, resultError := range []bool{false, true} {
		t.Run(fmt.Sprintf("result_error=%t", resultError), func(t *testing.T) {
			deliveryCtx, cancelDelivery := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelDelivery()
			manager := NewManager(nil, nil, nil)
			auth, errRegister := manager.Register(WithSkipPersist(deliveryCtx), &Auth{
				ID: "stream-cooldown-order", Provider: "codex", Status: StatusActive,
			})
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			retryAfter := time.Hour
			quotaErr := customStatusError{
				code: http.StatusTooManyRequests, msg: "model rate limit exceeded", retryAfter: &retryAfter,
			}
			startStream := func(fail bool) *cliproxyexecutor.StreamResult {
				t.Helper()
				attemptCtx, cancelAttempt := context.WithCancel(deliveryCtx)
				t.Cleanup(cancelAttempt)
				executor := &customStreamMockExecutor{
					identifier: "codex",
					streamFn: func(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
						chunks := make(chan cliproxyexecutor.StreamChunk, 2)
						chunks <- cliproxyexecutor.StreamChunk{Payload: []byte(`{"type":"response.created"}`)}
						if fail {
							terminal := cliproxyexecutor.StreamChunk{Err: quotaErr}
							if resultError {
								terminal = cliproxyexecutor.StreamChunk{
									Payload: []byte(`{"type":"response.failed"}`), ResultErr: quotaErr,
								}
							}
							chunks <- terminal
						}
						close(chunks)
						cancelAttempt()
						return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
					},
				}
				result, errStream := manager.executeStreamWithModelPool(attemptCtx, deliveryCtx, executor, auth, "codex",
					cliproxyexecutor.Request{Model: "model-a"}, cliproxyexecutor.Options{Stream: true},
					"model-a", "", []string{"model-a"}, false, OAuthModelAliasResult{}, nil, false, false)
				if errStream != nil {
					t.Fatalf("ExecuteStream: %v", errStream)
				}
				return result
			}

			// Delivery of the first stream stays blocked until the newer failure is recorded.
			older := startStream(false)
			newer := startStream(true)
			for range newer.Chunks {
			}
			before, _ := manager.GetByID(auth.ID)
			state := before.ModelStates["model-a"]
			if state == nil || !state.Unavailable || !state.NextRetryAfter.After(time.Now()) {
				t.Fatalf("expected active cooldown after the newer failure, got %+v", state)
			}
			for range older.Chunks {
			}
			after, _ := manager.GetByID(auth.ID)
			retained := after.ModelStates["model-a"]
			if retained == nil || !retained.Unavailable || !retained.NextRetryAfter.Equal(state.NextRetryAfter) {
				t.Fatalf("expected the newer cooldown to survive the older success, got %+v", retained)
			}
			if after.Success != 1 || after.Failed != 1 {
				t.Fatalf("outcomes = %d successes, %d failures; want one each", after.Success, after.Failed)
			}
		})
	}
}
