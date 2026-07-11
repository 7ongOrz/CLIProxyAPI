package openai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func normalizeResponsesWebsocketRequest(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte) ([]byte, []byte, *interfaces.ErrorMessage) {
	return normalizeResponsesWebsocketRequestWithMode(rawJSON, lastRequest, lastResponseOutput, true, true, false)
}

func normalizeResponsesWebsocketRequestWithMode(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte, allowIncrementalInputWithPreviousResponseID bool, allowCompactionReplayBypass bool, forceTranscriptReplacement bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	return normalizeResponsesWebsocketRequestWithReplayMode(rawJSON, lastRequest, lastResponseOutput, "", nil, allowIncrementalInputWithPreviousResponseID, allowCompactionReplayBypass, forceTranscriptReplacement, false)
}

func normalizeResponsesWebsocketRequestWithLastResponseID(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte, lastResponseID string, allowIncrementalInputWithPreviousResponseID bool, allowCompactionReplayBypass bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	return normalizeResponsesWebsocketRequestWithIncrementalState(rawJSON, lastRequest, lastResponseOutput, lastResponseID, nil, allowIncrementalInputWithPreviousResponseID, allowCompactionReplayBypass)
}

func normalizeResponsesWebsocketRequestWithIncrementalState(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte, lastResponseID string, lastResponsePendingToolCallIDs []string, allowIncrementalInputWithPreviousResponseID bool, allowCompactionReplayBypass bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	return normalizeResponsesWebsocketRequestWithReplayMode(rawJSON, lastRequest, lastResponseOutput, lastResponseID, lastResponsePendingToolCallIDs, allowIncrementalInputWithPreviousResponseID, allowCompactionReplayBypass, false, false)
}

func normalizeResponsesWebsocketRequestWithReplayMode(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte, lastResponseID string, lastResponsePendingToolCallIDs []string, allowIncrementalInputWithPreviousResponseID bool, allowCompactionReplayBypass bool, forceTranscriptReplacement bool, forceTranscriptReplay bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	requestType := strings.TrimSpace(gjson.GetBytes(rawJSON, "type").String())
	switch requestType {
	case wsRequestTypeCreate:
		// log.Infof("responses websocket: response.create request")
		if len(lastRequest) == 0 {
			dropPreviousResponseID := forceTranscriptReplacement || inputContainsFullTranscript(gjson.GetBytes(rawJSON, "input"))
			return normalizeResponseCreateRequest(rawJSON, dropPreviousResponseID, allowCompactionReplayBypass)
		}
		return normalizeResponseSubsequentRequest(rawJSON, lastRequest, lastResponseOutput, lastResponseID, lastResponsePendingToolCallIDs, allowIncrementalInputWithPreviousResponseID, allowCompactionReplayBypass, forceTranscriptReplacement, forceTranscriptReplay)
	case wsRequestTypeAppend:
		// log.Infof("responses websocket: response.append request")
		return normalizeResponseSubsequentRequest(rawJSON, lastRequest, lastResponseOutput, lastResponseID, lastResponsePendingToolCallIDs, allowIncrementalInputWithPreviousResponseID, allowCompactionReplayBypass, forceTranscriptReplacement, forceTranscriptReplay)
	default:
		return nil, lastRequest, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("unsupported websocket request type: %s", requestType),
		}
	}
}

func normalizeResponseCreateRequest(rawJSON []byte, dropPreviousResponseID bool, allowCompactionReplayBypass bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	normalized, errDelete := sjson.DeleteBytes(rawJSON, "type")
	if errDelete != nil {
		normalized = bytes.Clone(rawJSON)
	}
	if dropPreviousResponseID {
		normalized, _ = sjson.DeleteBytes(normalized, "previous_response_id")
	}
	normalized, _ = sjson.SetBytes(normalized, "stream", true)
	if !gjson.GetBytes(normalized, "input").Exists() {
		normalized, _ = sjson.SetRawBytes(normalized, "input", []byte("[]"))
	}
	input := gjson.GetBytes(normalized, "input")
	if inputContainsFullTranscript(input) && !allowCompactionReplayBypass {
		normalized, _ = sjson.SetRawBytes(normalized, "input", []byte(inputWithoutCompactionItems(input)))
	}

	modelName := strings.TrimSpace(gjson.GetBytes(normalized, "model").String())
	if modelName == "" {
		return nil, nil, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("missing model in response.create request"),
		}
	}
	return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
}

func normalizeResponseSubsequentRequest(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte, lastResponseID string, lastResponsePendingToolCallIDs []string, allowIncrementalInputWithPreviousResponseID bool, allowCompactionReplayBypass bool, forceTranscriptReplacement bool, forceTranscriptReplay bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	if len(lastRequest) == 0 {
		return nil, lastRequest, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("websocket request received before response.create"),
		}
	}

	nextInput := gjson.GetBytes(rawJSON, "input")
	if !nextInput.Exists() || !nextInput.IsArray() {
		return nil, lastRequest, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("websocket request requires array field: input"),
		}
	}

	if inputContainsFullTranscript(nextInput) {
		normalized, errMsg := buildResponsesWebsocketTranscriptState(rawJSON, lastRequest, lastResponseOutput, nextInput, allowCompactionReplayBypass)
		if errMsg != nil {
			return nil, lastRequest, errMsg
		}
		return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
	}

	// When the input already contains historical model output items but no compact
	// marker, treating it as an incremental append duplicates stale turn-state and
	// can leave late orphaned function_call items.
	if responsesWebsocketRequestReplacesTranscript(rawJSON, nextInput, lastRequest) {
		normalized := normalizeResponseTranscriptReplacement(rawJSON, lastRequest)
		return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
	}

	if forceTranscriptReplacement {
		normalized := normalizeResponseTranscriptReplacement(rawJSON, lastRequest)
		return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
	}

	if inputContainsCompactionTrigger(nextInput) {
		normalized, errMsg := buildResponsesWebsocketTranscriptState(rawJSON, lastRequest, lastResponseOutput, nextInput, allowCompactionReplayBypass)
		if errMsg != nil {
			return nil, lastRequest, errMsg
		}
		return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
	}

	if forceTranscriptReplay {
		normalized, errMsg := buildResponsesWebsocketTranscriptState(rawJSON, lastRequest, lastResponseOutput, nextInput, allowCompactionReplayBypass)
		if errMsg != nil {
			return nil, lastRequest, errMsg
		}
		return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
	}

	// Websocket v2 mode uses response.create with previous_response_id + incremental input.
	// Do not expand it into a full input transcript; upstream expects the incremental payload.
	if allowIncrementalInputWithPreviousResponseID {
		prev := strings.TrimSpace(gjson.GetBytes(rawJSON, "previous_response_id").String())
		if prev == "" {
			if !inputSatisfiesPendingToolCalls(nextInput, lastResponsePendingToolCallIDs) {
				normalized := normalizeResponseTranscriptReplacement(rawJSON, lastRequest)
				return normalized, bytes.Clone(normalized), nil
			}
			prev = strings.TrimSpace(lastResponseID)
		}
		if prev != "" {
			normalized, errDelete := sjson.DeleteBytes(rawJSON, "type")
			if errDelete != nil {
				normalized = bytes.Clone(rawJSON)
			}
			normalized, _ = sjson.SetBytes(normalized, "previous_response_id", prev)
			if !gjson.GetBytes(normalized, "model").Exists() {
				modelName := strings.TrimSpace(gjson.GetBytes(lastRequest, "model").String())
				if modelName != "" {
					normalized, _ = sjson.SetBytes(normalized, "model", modelName)
				}
			}
			if !gjson.GetBytes(normalized, "instructions").Exists() {
				instructions := gjson.GetBytes(lastRequest, "instructions")
				if instructions.Exists() {
					normalized, _ = sjson.SetRawBytes(normalized, "instructions", []byte(instructions.Raw))
				}
			}
			normalized, _ = sjson.SetBytes(normalized, "stream", true)
			updatedLastRequest, errMsg := buildResponsesWebsocketTranscriptState(rawJSON, lastRequest, lastResponseOutput, nextInput, allowCompactionReplayBypass)
			if errMsg != nil {
				return nil, lastRequest, errMsg
			}
			return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(updatedLastRequest), nil
		}
	}

	normalized, errMsg := buildResponsesWebsocketTranscriptState(rawJSON, lastRequest, lastResponseOutput, nextInput, allowCompactionReplayBypass)
	if errMsg != nil {
		return nil, lastRequest, errMsg
	}
	return normalized, responsesWebsocketSnapshotWithoutCompactionTriggers(normalized), nil
}

func buildResponsesWebsocketTranscriptState(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte, nextInput gjson.Result, allowCompactionReplayBypass bool) ([]byte, *interfaces.ErrorMessage) {
	// When the client sends a compact replay, the input already carries the
	// canonical post-compaction history. In that case, skip merging with stale
	// lastRequest/lastResponseOutput to avoid re-inflating compacted context or
	// breaking function_call / function_call_output pairings.
	// See: https://github.com/router-for-me/CLIProxyAPI/issues/2207
	var mergedInput string
	if inputContainsFullTranscript(nextInput) {
		if allowCompactionReplayBypass {
			log.Infof("responses websocket: full transcript detected, skipping stale merge (input items=%d)", len(nextInput.Array()))
			mergedInput = nextInput.Raw
		} else {
			log.Infof("responses websocket: full transcript detected, stripping compaction items for unsupported upstream (input items=%d)", len(nextInput.Array()))
			mergedInput = inputWithoutCompactionItems(nextInput)
		}
	} else {
		appendInputRaw := nextInput.Raw
		existingInput := gjson.GetBytes(lastRequest, "input")
		var errMerge error
		mergedInput, errMerge = mergeJSONArrayRaw(existingInput.Raw, normalizeJSONArrayRaw(lastResponseOutput))
		if errMerge != nil {
			return nil, &interfaces.ErrorMessage{
				StatusCode: http.StatusBadRequest,
				Error:      fmt.Errorf("invalid previous response output: %w", errMerge),
			}
		}

		mergedInput, errMerge = mergeJSONArrayRaw(mergedInput, appendInputRaw)
		if errMerge != nil {
			return nil, &interfaces.ErrorMessage{
				StatusCode: http.StatusBadRequest,
				Error:      fmt.Errorf("invalid request input: %w", errMerge),
			}
		}
	}
	dedupedInput, errDedupeFunctionCalls := dedupeFunctionCallsByCallID(mergedInput)
	if errDedupeFunctionCalls == nil {
		mergedInput = dedupedInput
	}
	dedupedInput, errDedupeItemIDs := dedupeInputItemsByID(mergedInput)
	if errDedupeItemIDs == nil {
		mergedInput = dedupedInput
	}

	normalized, errDelete := sjson.DeleteBytes(rawJSON, "type")
	if errDelete != nil {
		normalized = bytes.Clone(rawJSON)
	}
	normalized, _ = sjson.DeleteBytes(normalized, "previous_response_id")
	var errSet error
	normalized, errSet = sjson.SetRawBytes(normalized, "input", []byte(mergedInput))
	if errSet != nil {
		return nil, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("failed to merge websocket input: %w", errSet),
		}
	}
	if !gjson.GetBytes(normalized, "model").Exists() {
		modelName := strings.TrimSpace(gjson.GetBytes(lastRequest, "model").String())
		if modelName != "" {
			normalized, _ = sjson.SetBytes(normalized, "model", modelName)
		}
	}
	if !gjson.GetBytes(normalized, "instructions").Exists() {
		instructions := gjson.GetBytes(lastRequest, "instructions")
		if instructions.Exists() {
			normalized, _ = sjson.SetRawBytes(normalized, "instructions", []byte(instructions.Raw))
		}
	}
	normalized, _ = sjson.SetBytes(normalized, "stream", true)
	return normalized, nil
}

func responsesWebsocketSnapshotWithoutCompactionTriggers(payload []byte) []byte {
	input := gjson.GetBytes(payload, "input")
	if !input.IsArray() {
		return bytes.Clone(payload)
	}
	filtered := make([]string, 0, len(input.Array()))
	removed := false
	for _, item := range input.Array() {
		if strings.TrimSpace(item.Get("type").String()) == "compaction_trigger" {
			removed = true
			continue
		}
		filtered = append(filtered, item.Raw)
	}
	if !removed {
		return bytes.Clone(payload)
	}
	out, errSet := sjson.SetRawBytes(payload, "input", []byte("["+strings.Join(filtered, ",")+"]"))
	if errSet != nil {
		return bytes.Clone(payload)
	}
	return out
}

func shouldReplaceWebsocketTranscript(rawJSON []byte, nextInput gjson.Result, lastRequest []byte) bool {
	requestType := strings.TrimSpace(gjson.GetBytes(rawJSON, "type").String())
	if requestType != wsRequestTypeCreate && requestType != wsRequestTypeAppend {
		return false
	}
	previousResponseID := gjson.GetBytes(rawJSON, "previous_response_id")
	if strings.TrimSpace(previousResponseID.String()) != "" {
		return false
	}
	if !nextInput.Exists() || !nextInput.IsArray() {
		return false
	}
	if requestType == wsRequestTypeCreate && !previousResponseID.Exists() && inputHasCodexLocalCompactionSummary(nextInput) {
		return true
	}

	for _, item := range nextInput.Array() {
		switch strings.TrimSpace(item.Get("type").String()) {
		case "function_call", "custom_tool_call":
			return true
		case "message":
			if strings.TrimSpace(item.Get("role").String()) == "assistant" {
				return true
			}
		}
	}

	return inputStartsWithPreviousRequestInput(nextInput, lastRequest)
}

func responsesWebsocketRequestReplacesTranscript(rawJSON []byte, nextInput gjson.Result, lastRequest []byte) bool {
	return shouldReplaceWebsocketTranscript(rawJSON, nextInput, lastRequest) ||
		isCodexFullWebsocketCreateWithoutPreviousResponseID(rawJSON)
}

func isCodexFullWebsocketCreateWithoutPreviousResponseID(rawJSON []byte) bool {
	if strings.TrimSpace(gjson.GetBytes(rawJSON, "type").String()) != wsRequestTypeCreate {
		return false
	}
	if strings.TrimSpace(gjson.GetBytes(rawJSON, "previous_response_id").String()) != "" {
		return false
	}
	if strings.TrimSpace(gjson.GetBytes(rawJSON, "model").String()) == "" {
		return false
	}
	clientMetadata := gjson.GetBytes(rawJSON, "client_metadata")
	if !clientMetadata.IsObject() {
		return false
	}
	responsesLite := strings.EqualFold(strings.TrimSpace(clientMetadata.Get(wsResponsesLiteMetadataKey).String()), "true")
	if (!gjson.GetBytes(rawJSON, "tools").Exists() && !responsesLite) ||
		!gjson.GetBytes(rawJSON, "tool_choice").Exists() ||
		!gjson.GetBytes(rawJSON, "parallel_tool_calls").Exists() ||
		!gjson.GetBytes(rawJSON, "store").Exists() ||
		!gjson.GetBytes(rawJSON, "stream").Exists() ||
		!gjson.GetBytes(rawJSON, "include").Exists() {
		return false
	}
	return strings.TrimSpace(clientMetadata.Get("x-codex-installation-id").String()) != "" &&
		strings.TrimSpace(clientMetadata.Get("x-codex-window-id").String()) != ""
}

func inputStartsWithPreviousRequestInput(nextInput gjson.Result, lastRequest []byte) bool {
	if !nextInput.Exists() || !nextInput.IsArray() {
		return false
	}
	previousInput := gjson.GetBytes(lastRequest, "input")
	if !previousInput.Exists() || !previousInput.IsArray() {
		return false
	}
	previousItems := previousInput.Array()
	nextItems := nextInput.Array()
	if len(previousItems) == 0 || len(nextItems) < len(previousItems) {
		return false
	}
	for i := range previousItems {
		if !jsonRawValuesEqual(previousItems[i].Raw, nextItems[i].Raw) {
			return false
		}
	}
	return true
}

func jsonRawValuesEqual(a, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if a == b {
		return true
	}
	normalizedA, okA := normalizeJSONValueRaw(a)
	normalizedB, okB := normalizeJSONValueRaw(b)
	if okA && okB {
		return bytes.Equal(normalizedA, normalizedB)
	}
	return false
}

func inputHasCodexLocalCompactionSummary(input gjson.Result) bool {
	if !input.IsArray() {
		return false
	}

	hasSummary := false
	for index, item := range input.Array() {
		itemType := strings.TrimSpace(item.Get("type").String())
		if itemType == "additional_tools" {
			tools := item.Get("tools")
			if index != 0 || strings.TrimSpace(item.Get("role").String()) != "developer" || !tools.IsArray() {
				return false
			}
			for _, tool := range tools.Array() {
				if !tool.IsObject() || strings.TrimSpace(tool.Get("type").String()) == "" {
					return false
				}
			}
			continue
		}
		if itemType != "" && itemType != "message" {
			return false
		}

		role := strings.TrimSpace(item.Get("role").String())
		if role != "user" && role != "developer" {
			return false
		}
		if role == "user" && strings.HasPrefix(codexLocalCompactionMessageText(item), codexLocalCompactionSummaryPrefix+"\n") {
			hasSummary = true
		}
	}
	return hasSummary
}

func codexLocalCompactionMessageText(message gjson.Result) string {
	content := message.Get("content")
	if content.Type == gjson.String {
		return content.String()
	}
	if !content.IsArray() {
		return ""
	}

	var text strings.Builder
	for _, part := range content.Array() {
		if strings.TrimSpace(part.Get("type").String()) == "input_text" {
			text.WriteString(part.Get("text").String())
		}
	}
	return text.String()
}

func inputSatisfiesPendingToolCalls(input gjson.Result, pendingCallIDs []string) bool {
	if len(pendingCallIDs) == 0 {
		return true
	}
	if !input.IsArray() {
		return false
	}
	outputs := make(map[string]struct{}, len(pendingCallIDs))
	for _, item := range input.Array() {
		switch strings.TrimSpace(item.Get("type").String()) {
		case "function_call_output", "custom_tool_call_output":
			callID := strings.TrimSpace(item.Get("call_id").String())
			if callID != "" {
				outputs[callID] = struct{}{}
			}
		}
	}
	for _, callID := range pendingCallIDs {
		callID = strings.TrimSpace(callID)
		if callID == "" {
			continue
		}
		if _, ok := outputs[callID]; !ok {
			return false
		}
	}
	return true
}

func normalizeJSONValueRaw(raw string) ([]byte, bool) {
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, false
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}
	return normalized, true
}

func normalizeResponseTranscriptReplacement(rawJSON []byte, lastRequest []byte) []byte {
	normalized, errDelete := sjson.DeleteBytes(rawJSON, "type")
	if errDelete != nil {
		normalized = bytes.Clone(rawJSON)
	}
	normalized, _ = sjson.DeleteBytes(normalized, "previous_response_id")
	if !gjson.GetBytes(normalized, "model").Exists() {
		modelName := strings.TrimSpace(gjson.GetBytes(lastRequest, "model").String())
		if modelName != "" {
			normalized, _ = sjson.SetBytes(normalized, "model", modelName)
		}
	}
	if !gjson.GetBytes(normalized, "instructions").Exists() {
		instructions := gjson.GetBytes(lastRequest, "instructions")
		if instructions.Exists() {
			normalized, _ = sjson.SetRawBytes(normalized, "instructions", []byte(instructions.Raw))
		}
	}
	normalized, _ = sjson.SetBytes(normalized, "stream", true)
	return bytes.Clone(normalized)
}

func dedupeFunctionCallsByCallID(rawArray string) (string, error) {
	rawArray = strings.TrimSpace(rawArray)
	if rawArray == "" {
		return "[]", nil
	}
	var items []json.RawMessage
	if errUnmarshal := json.Unmarshal([]byte(rawArray), &items); errUnmarshal != nil {
		return "", errUnmarshal
	}

	seenCallIDs := make(map[string]struct{}, len(items))
	filtered := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		if len(item) == 0 {
			continue
		}
		itemType := strings.TrimSpace(gjson.GetBytes(item, "type").String())
		if isResponsesToolCallType(itemType) {
			callID := strings.TrimSpace(gjson.GetBytes(item, "call_id").String())
			if callID != "" {
				if _, ok := seenCallIDs[callID]; ok {
					continue
				}
				seenCallIDs[callID] = struct{}{}
			}
		}
		filtered = append(filtered, item)
	}

	out, errMarshal := json.Marshal(filtered)
	if errMarshal != nil {
		return "", errMarshal
	}
	return string(out), nil
}

func dedupeResponsesWebsocketInputItemsByID(payload []byte) []byte {
	input := gjson.GetBytes(payload, "input")
	if !input.Exists() || !input.IsArray() {
		return payload
	}
	dedupedInput, errDedupe := dedupeInputItemsByID(input.Raw)
	if errDedupe != nil || dedupedInput == input.Raw {
		return payload
	}
	updated, errSet := sjson.SetRawBytes(payload, "input", []byte(dedupedInput))
	if errSet != nil {
		return payload
	}
	return updated
}

func dedupeInputItemsByID(rawArray string) (string, error) {
	rawArray = strings.TrimSpace(rawArray)
	if rawArray == "" {
		return "[]", nil
	}
	var items []json.RawMessage
	if errUnmarshal := json.Unmarshal([]byte(rawArray), &items); errUnmarshal != nil {
		return "", errUnmarshal
	}

	// Parse each item's type, id and call_id once; gjson is a scan-based
	// parser, so reusing this metadata avoids rescanning every item in each of
	// the loops below as the conversation history grows.
	type itemMetadata struct {
		itemType string
		id       string
		callID   string
	}
	meta := make([]itemMetadata, len(items))
	for i, item := range items {
		if len(item) == 0 {
			continue
		}
		res := gjson.GetManyBytes(item, "type", "id", "call_id")
		meta[i] = itemMetadata{
			itemType: strings.TrimSpace(res[0].String()),
			id:       strings.TrimSpace(res[1].String()),
			callID:   strings.TrimSpace(res[2].String()),
		}
	}

	// Collect the call_ids that are still referenced by tool-call output
	// items. When several input items share the same id, the one we keep must
	// preserve any call_id that has a matching output; otherwise the upstream
	// rejects the request with "No tool call found for function call output".
	referencedCallIDs := make(map[string]struct{}, len(items))
	for i := range items {
		switch meta[i].itemType {
		case "function_call_output", "custom_tool_call_output":
			if meta[i].callID != "" {
				referencedCallIDs[meta[i].callID] = struct{}{}
			}
		}
	}

	// For each id, choose the index that keeps it. The default is the last
	// occurrence (matching the original dedupe behavior), but we never replace
	// an item whose call_id still has a matching output with one that does not.
	// Additional referenced calls sharing that id are retained without their
	// optional id below so their outputs remain paired.
	keepIndexByID := make(map[string]int, len(items))
	keepReferencedByID := make(map[string]bool, len(items))
	for i := range items {
		itemID := meta[i].id
		if itemID == "" {
			continue
		}
		_, referenced := referencedCallIDs[meta[i].callID]
		referenced = referenced && meta[i].callID != ""
		if _, seen := keepIndexByID[itemID]; !seen {
			keepIndexByID[itemID] = i
			keepReferencedByID[itemID] = referenced
			continue
		}
		if referenced || !keepReferencedByID[itemID] {
			keepIndexByID[itemID] = i
			keepReferencedByID[itemID] = referenced
		}
	}

	filtered := make([]json.RawMessage, 0, len(items))
	for i, item := range items {
		if len(item) == 0 {
			continue
		}
		itemID := meta[i].id
		if itemID != "" {
			keepIndex := keepIndexByID[itemID]
			if keepIndex != i {
				_, referenced := referencedCallIDs[meta[i].callID]
				if !isResponsesToolCallType(meta[i].itemType) ||
					meta[i].callID == "" ||
					meta[i].callID == meta[keepIndex].callID ||
					!referenced {
					continue
				}
				itemWithoutID, errDeleteID := sjson.DeleteBytes(item, "id")
				if errDeleteID != nil {
					return "", errDeleteID
				}
				item = itemWithoutID
			}
		}
		filtered = append(filtered, item)
	}

	out, errMarshal := json.Marshal(filtered)
	if errMarshal != nil {
		return "", errMarshal
	}
	return string(out), nil
}

func normalizeResponsesWebsocketPassthroughRequest(rawJSON []byte, modelName string) ([]byte, *interfaces.ErrorMessage) {
	if !json.Valid(rawJSON) {
		return nil, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("invalid websocket request JSON"),
		}
	}

	requestType := strings.TrimSpace(gjson.GetBytes(rawJSON, "type").String())
	switch requestType {
	case wsRequestTypeCreate, wsRequestTypeAppend:
	default:
		return nil, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("unsupported websocket request type: %s", requestType),
		}
	}

	normalized := bytes.Clone(rawJSON)
	if strings.TrimSpace(gjson.GetBytes(normalized, "model").String()) == "" {
		modelName = strings.TrimSpace(modelName)
		if modelName == "" {
			return nil, &interfaces.ErrorMessage{
				StatusCode: http.StatusBadRequest,
				Error:      fmt.Errorf("missing model in response.create request"),
			}
		}
		normalized, _ = sjson.SetBytes(normalized, "model", modelName)
	}
	normalized, _ = sjson.SetBytes(normalized, "stream", true)
	return normalized, nil
}
