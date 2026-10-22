package responses

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexIncludePreservesRequestedOutputReplay(t *testing.T) {
	for _, tt := range []struct {
		name    string
		include string
		want    string
	}{
		{"default", `null`, `["reasoning.encrypted_content"]`},
		{"empty array", `[]`, `["reasoning.encrypted_content"]`},
		{"string value", `"output.encrypted_content"`, `["reasoning.encrypted_content"]`},
		{"object value", `{"output.encrypted_content":true}`, `["reasoning.encrypted_content"]`},
		{"legacy", `["reasoning.encrypted_content"]`, `["reasoning.encrypted_content"]`},
		{"official replay", `["reasoning.encrypted_content","output.encrypted_content"]`, `["reasoning.encrypted_content","output.encrypted_content"]`},
		{"replay alone", `["output.encrypted_content"]`, `["reasoning.encrypted_content","output.encrypted_content"]`},
		{"sources", `["web_search_call.action.sources"]`, `["reasoning.encrypted_content","web_search_call.action.sources"]`},
		{"replay with sources", `["web_search_call.action.sources","output.encrypted_content"]`, `["reasoning.encrypted_content","output.encrypted_content","web_search_call.action.sources"]`},
		{"duplicate replay", `["output.encrypted_content","output.encrypted_content"]`, `["reasoning.encrypted_content","output.encrypted_content"]`},
		{"supported values", `[null,42,"file_search_call.results","output.encrypted_content"]`, `["reasoning.encrypted_content","output.encrypted_content"]`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			input := `{"include":` + tt.include + `,"input":[{"type":"message","role":"assistant","encrypted_content":"ciphertext","content":[]}]}`
			payload := []byte(input)
			got := setCodexRequiredInclude(payload)
			if include := gjson.GetBytes(got, "include").Raw; include != tt.want {
				t.Fatalf("include = %s, want %s", include, tt.want)
			}
			if gjson.GetBytes(got, "input").Raw != gjson.Get(input, "input").Raw {
				t.Fatalf("replayed input changed: %s", got)
			}
			if string(payload) != input {
				t.Fatalf("source payload changed: %s", payload)
			}
		})
	}
}

func TestCodexIncludeReusesNormalizedPayload(t *testing.T) {
	for _, include := range []string{
		`[ "reasoning.encrypted_content" ]`,
		`[ "reasoning.encrypted_content", "output.encrypted_content" ]`,
		`[ "reasoning.encrypted_content", "web_search_call.action.sources" ]`,
		`[ "reasoning.encrypted_content", "output.encrypted_content", "web_search_call.action.sources" ]`,
	} {
		payload := []byte(`{"include":` + include + `}`)
		got := setCodexRequiredInclude(payload)
		if &got[0] != &payload[0] {
			t.Fatalf("normalized payload was copied: %s", include)
		}
	}
}
