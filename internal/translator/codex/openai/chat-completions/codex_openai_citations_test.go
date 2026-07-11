package chat_completions

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexChatCitationsUseContentPartOffsets(t *testing.T) {
	annotation := `{"type":"url_citation","url":"https://example.com","title":"Example","start_index":0,"end_index":2}`
	part := `{"type":"output_text","text":"引用","annotations":[` + annotation + `]}`
	for _, event := range []string{
		`{"type":"response.output_text.annotation.added","annotation":` + annotation + `}`,
		`{"type":"response.output_text.done","text":"引用","annotations":[` + annotation + `]}`,
		`{"type":"response.content_part.done","part":` + part + `}`,
		`{"type":"response.output_item.done","item":{"type":"message","content":[` + part + `]}}`,
	} {
		t.Run(gjson.Get(event, "type").String(), func(t *testing.T) {
			var param any
			for _, delta := range []string{"引", "用"} {
				ConvertCodexResponseToOpenAI(t.Context(), "gpt-5.5", nil, nil, []byte(`data: {"type":"response.output_text.delta","delta":"`+delta+`"}`), &param)
			}
			out := ConvertCodexResponseToOpenAI(t.Context(), "gpt-5.5", nil, nil, []byte("data: "+event), &param)
			if len(out) != 1 {
				t.Fatalf("citation chunks = %d, want 1", len(out))
			}
			got := gjson.GetBytes(out[0], "choices.0.delta.annotations.0").Raw
			want := `{"type":"url_citation","url_citation":{"url":"https://example.com","title":"Example","start_index":0,"end_index":2}}`
			assertCitationJSON(t, got, want)
		})
	}
}

func TestCodexChatCitationsPreserveRepeatedSourcesAcrossParts(t *testing.T) {
	annotation := map[string]any{"type": "url_citation", "url": "https://example.com", "title": "Example", "start_index": 0, "end_index": 2}
	parts := []any{
		map[string]any{"type": "output_text", "text": "前🙂", "annotations": []any{annotation}},
		map[string]any{"type": "output_text", "text": "引用", "annotations": []any{annotation}},
	}
	message := map[string]any{"type": "message", "content": parts}
	terminal := map[string]any{"type": "response.completed", "response": map[string]any{"status": "completed", "output": []any{message}}}
	nonStream := ConvertCodexResponseToOpenAINonStream(t.Context(), "gpt-5.5", nil, nil, mustJSONMarshal(t, terminal), nil)
	want := `[{"type":"url_citation","url_citation":{"url":"https://example.com","title":"Example","start_index":0,"end_index":2}},{"type":"url_citation","url_citation":{"url":"https://example.com","title":"Example","start_index":2,"end_index":4}}]`
	assertCitationJSON(t, gjson.GetBytes(nonStream, "choices.0.message.annotations").Raw, want)

	for _, acrossMessages := range []bool{false, true} {
		t.Run(map[bool]string{false: "content parts", true: "output messages"}[acrossMessages], func(t *testing.T) {
			var param any
			var citations []json.RawMessage
			for index, text := range []string{"前🙂", "引用"} {
				outputIndex, contentIndex := 0, index
				if acrossMessages {
					outputIndex, contentIndex = index, 0
				}
				for _, event := range []map[string]any{
					{"type": "response.output_text.delta", "delta": text},
					{"type": "response.output_text.annotation.added", "annotation": annotation},
					{"type": "response.content_part.done", "part": parts[index]},
				} {
					event["output_index"], event["content_index"] = outputIndex, contentIndex
					out := ConvertCodexResponseToOpenAI(t.Context(), "gpt-5.5", nil, nil, append([]byte("data: "), mustJSONMarshal(t, event)...), &param)
					for _, chunk := range out {
						for _, citation := range gjson.GetBytes(chunk, "choices.0.delta.annotations").Array() {
							citations = append(citations, json.RawMessage(citation.Raw))
						}
					}
				}
			}
			assertCitationJSON(t, string(mustJSONMarshal(t, citations)), want)
		})
	}
}

func TestCodexChatCitationsUseCompletedItemPartOffsets(t *testing.T) {
	var param any
	for index, text := range []string{"前🙂", "引用"} {
		event := map[string]any{"type": "response.output_text.delta", "output_index": 3, "content_index": index, "delta": text}
		ConvertCodexResponseToOpenAI(t.Context(), "gpt-5.5", nil, nil, append([]byte("data: "), mustJSONMarshal(t, event)...), &param)
	}
	event := []byte(`data: {"type":"response.output_item.done","output_index":3,"item":{"type":"message","content":[{"type":"output_text","text":"前🙂","annotations":[{"type":"url_citation","url":"https://example.com","title":"Example","start_index":0,"end_index":2}]},{"type":"output_text","text":"引用","annotations":[{"type":"url_citation","url":"https://example.com","title":"Example","start_index":0,"end_index":2}]}]}}`)
	out := ConvertCodexResponseToOpenAI(t.Context(), "gpt-5.5", nil, nil, event, &param)
	if len(out) != 1 {
		t.Fatalf("citation chunks = %d, want 1", len(out))
	}
	want := `[{"type":"url_citation","url_citation":{"url":"https://example.com","title":"Example","start_index":0,"end_index":2}},{"type":"url_citation","url_citation":{"url":"https://example.com","title":"Example","start_index":2,"end_index":4}}]`
	assertCitationJSON(t, gjson.GetBytes(out[0], "choices.0.delta.annotations").Raw, want)
	if out := ConvertCodexResponseToOpenAI(t.Context(), "gpt-5.5", nil, nil, event, &param); len(out) != 0 {
		t.Fatalf("repeated completion emitted %d chunks", len(out))
	}
}

func assertCitationJSON(t *testing.T, got, want string) {
	t.Helper()
	var gotValue, wantValue any
	if err := json.Unmarshal([]byte(got), &gotValue); err != nil {
		t.Fatalf("decode citation %q: %v", got, err)
	}
	if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Errorf("citation = %s, want %s", got, want)
	}
}
