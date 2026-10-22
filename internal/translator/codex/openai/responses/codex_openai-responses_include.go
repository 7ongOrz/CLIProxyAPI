package responses

import (
	"slices"

	"github.com/MoeForge/moego/ojson"
)

func setCodexRequiredIncludeDoc(rawJSON []byte, ensureDoc func() *ojson.Doc) {
	var values []ojson.Result
	if current := ojson.GetBytes(rawJSON, "include"); current.IsArray() {
		values = current.Array()
	}
	// Reasoning replay is required. Output replay and search sources are opt-in.
	include := []string{"reasoning.encrypted_content"}
	for _, optional := range []string{"output.encrypted_content", "web_search_call.action.sources"} {
		for _, value := range values {
			if value.Type == ojson.String && value.String() == optional {
				include = append(include, optional)
				break
			}
		}
	}
	if slices.EqualFunc(values, include, func(value ojson.Result, expected string) bool {
		return value.Type == ojson.String && value.String() == expected
	}) {
		return
	}
	_ = ensureDoc().Set("include", include)
}
