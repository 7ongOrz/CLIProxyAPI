package auth

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
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
