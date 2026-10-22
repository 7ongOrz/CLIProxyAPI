package executor

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	internalcache "github.com/router-for-me/CLIProxyAPI/v8/internal/cache"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestAntigravityStreamMultilinePreservesUsageAndReplay(t *testing.T) {
	const payload = `{"response":{"candidates":[{"content":{"role":"model","parts":[{"thoughtSignature":"sig-first","functionCall":{"name":"Read","args":{"path":"/a"},"id":"call-1"}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":11,"candidatesTokenCount":22,"totalTokenCount":33},"modelVersion":"gemini-3.7-flash","responseId":"resp-multiline"}}`
	const splitStart = `{"response":{"candidates":[{"content":{"role":"model","parts":[{"thoughtSignature":"sig-first","functionCall":{"name":"Read","args":{"path":"/a"},"id":"call-1"}}]},"finishReason":"STOP"}],"modelVersion":"gemini-3.7-flash","responseId":"resp-multiline"},"traceId":"trace-multiline"}`
	const splitUsage = `{"response":{"candidates":[{"content":{"role":"model","parts":[{"text":""}]}}],"usageMetadata":{"promptTokenCount":11,"candidatesTokenCount":22,"totalTokenCount":33},"modelVersion":"gemini-3.7-flash","responseId":"resp-multiline"},"traceId":"trace-multiline"}`
	multilineEvent := func(payload, prefix string) string {
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, []byte(payload), "", "  "); err != nil {
			t.Fatal(err)
		}
		return prefix + strings.ReplaceAll(pretty.String(), "\n", "\n"+prefix) + "\n\n"
	}
	for _, tc := range []struct {
		name string
		body string
	}{
		{"single line", "data: " + payload + "\n\n"},
		{"multiline data", multilineEvent(payload, "data: ")},
		{"multiline raw JSON", multilineEvent(payload, "")},
		{"split usage single line", "data: " + splitStart + "\n\ndata: " + splitUsage + "\n\n"},
		{"split usage multiline data", multilineEvent(splitStart, "data: ") + multilineEvent(splitUsage, "data: ")},
		{"split usage multiline raw JSON", multilineEvent(splitStart, "") + multilineEvent(splitUsage, "")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			internalcache.ClearAntigravityReasoningReplayCache()
			t.Cleanup(internalcache.ClearAntigravityReasoningReplayCache)
			capture := &antigravityUsageCapture{authID: t.Name(), records: make(chan usage.Record, 4)}
			usage.RegisterNamedPlugin(t.Name(), capture)
			t.Cleanup(func() { usage.RegisterNamedPlugin(t.Name(), antigravityUsageNoop{}) })
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()

			executor := NewAntigravityExecutor(&config.Config{RequestRetry: 1})
			result, errExecute := executor.ExecuteStream(t.Context(), &cliproxyauth.Auth{
				ID: t.Name(),
				Metadata: map[string]any{
					"access_token": "test-token",
					"expired":      time.Now().Add(24 * time.Hour).Format(time.RFC3339),
					"project_id":   "test-project",
				},
				Attributes: map[string]string{"base_url": server.URL},
			}, cliproxyexecutor.Request{
				Model:   "gemini-3.7-flash",
				Payload: []byte(`{"model":"gemini-3.7-flash","messages":[{"role":"user","content":"hello"}],"stream":true}`),
			}, cliproxyexecutor.Options{
				SourceFormat:   sdktranslator.FormatOpenAI,
				ResponseFormat: sdktranslator.FormatOpenAI,
				Stream:         true,
				Headers:        http.Header{"Session-Id": []string{t.Name()}},
			})
			if errExecute != nil {
				t.Fatal(errExecute)
			}
			var terminal []byte
			var terminalCount int
			for chunk := range result.Chunks {
				if chunk.Err != nil {
					t.Fatal(chunk.Err)
				}
				if gjson.GetBytes(chunk.Payload, "choices.0.finish_reason").String() != "" {
					terminal = chunk.Payload
					terminalCount++
				}
			}
			if terminalCount != 1 {
				t.Errorf("terminal chunks = %d, want 1", terminalCount)
			}
			if gjson.GetBytes(terminal, "usage.total_tokens").Int() != 33 || gjson.GetBytes(terminal, "usage.prompt_tokens").Int() != 11 {
				t.Errorf("terminal chunk = %s, want input 11 and total 33", terminal)
			}

			select {
			case record := <-capture.records:
				if record.Failed || record.Detail.InputTokens != 11 || record.Detail.OutputTokens != 22 || record.Detail.TotalTokens != 33 {
					t.Errorf("usage failed=%v detail=%+v, want successful input 11 output 22 total 33", record.Failed, record.Detail)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for usage")
			}
			items, found := internalcache.GetAntigravityReasoningReplayItems("gemini-3.7-flash", "responses:"+t.Name())
			if !found || len(items) != 1 {
				t.Fatalf("replay items=%d found=%v, want one completed tool call", len(items), found)
			}
			if gjson.GetBytes(items[0], "call_id").String() != "call-1" || gjson.GetBytes(items[0], "thoughtSignature").String() != "sig-first" {
				t.Errorf("replay item = %s, want call-1 with sig-first", items[0])
			}
		})
	}
}
