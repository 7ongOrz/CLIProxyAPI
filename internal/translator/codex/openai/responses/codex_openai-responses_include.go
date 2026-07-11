package responses

import (
	"slices"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func setCodexRequiredInclude(rawJSON []byte) []byte {
	var values []gjson.Result
	if current := gjson.GetBytes(rawJSON, "include"); current.IsArray() {
		values = current.Array()
	}
	// Reasoning replay is required. Output replay and search sources are opt-in.
	include := []string{"reasoning.encrypted_content"}
	for _, optional := range []string{"output.encrypted_content", "web_search_call.action.sources"} {
		for _, value := range values {
			if value.Type == gjson.String && value.String() == optional {
				include = append(include, optional)
				break
			}
		}
	}
	if slices.EqualFunc(values, include, func(value gjson.Result, expected string) bool {
		return value.Type == gjson.String && value.String() == expected
	}) {
		return rawJSON
	}
	updated, errSet := sjson.SetBytes(rawJSON, "include", include)
	if errSet != nil {
		return rawJSON
	}
	return updated
}
