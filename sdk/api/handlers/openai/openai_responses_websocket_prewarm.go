package openai

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func shouldHandleResponsesWebsocketPrewarmLocally(rawJSON []byte, lastRequest []byte, allowIncrementalInputWithPreviousResponseID bool) bool {
	if allowIncrementalInputWithPreviousResponseID || len(lastRequest) != 0 {
		return false
	}
	if strings.TrimSpace(gjson.GetBytes(rawJSON, "type").String()) != wsRequestTypeCreate {
		return false
	}
	generateResult := gjson.GetBytes(rawJSON, "generate")
	return generateResult.Exists() && !generateResult.Bool()
}

func writeResponsesWebsocketSyntheticPrewarm(
	c *gin.Context,
	writer *responsesWebsocketWriter,
	requestJSON []byte,
	wsTimelineLog websocketTimelineAppender,
	sessionID string,
) error {
	payloads, errPayloads := syntheticResponsesWebsocketPrewarmPayloads(requestJSON)
	if errPayloads != nil {
		return errPayloads
	}
	for i := 0; i < len(payloads); i++ {
		markAPIResponseTimestamp(c)
		// log.Infof(
		// 	"responses websocket: downstream_out id=%s type=%d event=%s payload=%s",
		// 	sessionID,
		// 	websocket.TextMessage,
		// 	websocketPayloadEventType(payloads[i]),
		// 	websocketPayloadPreview(payloads[i]),
		// )
		if errWrite := writeResponsesWebsocketPayload(writer, wsTimelineLog, payloads[i], time.Now()); errWrite != nil {
			log.Warnf(
				"responses websocket: downstream_out write failed id=%s event=%s error=%v",
				sessionID,
				websocketPayloadEventType(payloads[i]),
				errWrite,
			)
			return errWrite
		}
	}
	return nil
}

func syntheticResponsesWebsocketPrewarmPayloads(requestJSON []byte) ([][]byte, error) {
	responseID := "resp_prewarm_" + uuid.NewString()
	createdAt := time.Now().Unix()
	modelName := strings.TrimSpace(gjson.GetBytes(requestJSON, "model").String())

	createdPayload := []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"","object":"response","created_at":0,"status":"in_progress","background":false,"error":null,"output":[]}}`)
	var errSet error
	createdPayload, errSet = sjson.SetBytes(createdPayload, "response.id", responseID)
	if errSet != nil {
		return nil, errSet
	}
	createdPayload, errSet = sjson.SetBytes(createdPayload, "response.created_at", createdAt)
	if errSet != nil {
		return nil, errSet
	}
	if modelName != "" {
		createdPayload, errSet = sjson.SetBytes(createdPayload, "response.model", modelName)
		if errSet != nil {
			return nil, errSet
		}

	}

	completedPayload := []byte(`{"type":"response.completed","sequence_number":1,"response":{"id":"","object":"response","created_at":0,"status":"completed","background":false,"error":null,"output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
	completedPayload, errSet = sjson.SetBytes(completedPayload, "response.id", responseID)
	if errSet != nil {
		return nil, errSet
	}
	completedPayload, errSet = sjson.SetBytes(completedPayload, "response.created_at", createdAt)
	if errSet != nil {
		return nil, errSet
	}
	if modelName != "" {
		completedPayload, errSet = sjson.SetBytes(completedPayload, "response.model", modelName)
		if errSet != nil {
			return nil, errSet
		}
	}

	return [][]byte{createdPayload, completedPayload}, nil
}

func mergeJSONArrayRaw(existingRaw, appendRaw string) (string, error) {
	existingRaw = strings.TrimSpace(existingRaw)
	appendRaw = strings.TrimSpace(appendRaw)
	if existingRaw == "" {
		existingRaw = "[]"
	}
	if appendRaw == "" {
		appendRaw = "[]"
	}

	var existing []json.RawMessage
	if err := json.Unmarshal([]byte(existingRaw), &existing); err != nil {
		return "", err
	}
	var appendItems []json.RawMessage
	if err := json.Unmarshal([]byte(appendRaw), &appendItems); err != nil {
		return "", err
	}

	merged := append(existing, appendItems...)
	out, err := json.Marshal(merged)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func inputContainsFullTranscript(input gjson.Result) bool {
	if !input.IsArray() {
		return false
	}
	for _, item := range input.Array() {
		if isResponsesWebsocketCompactionReplayItemType(item.Get("type").String()) {
			return true
		}
	}
	return false
}

func inputContainsCompactionTrigger(input gjson.Result) bool {
	if !input.IsArray() {
		return false
	}
	for _, item := range input.Array() {
		if strings.TrimSpace(item.Get("type").String()) == "compaction_trigger" {
			return true
		}
	}
	return false
}

func isResponsesWebsocketCompactionItemType(t string) bool {
	switch strings.TrimSpace(t) {
	case "compaction", "compaction_summary", "compaction_trigger", "context_compaction":
		return true
	default:
		return false
	}
}

func isResponsesWebsocketCompactionReplayItemType(t string) bool {
	switch strings.TrimSpace(t) {
	case "compaction", "compaction_summary", "context_compaction":
		return true
	default:
		return false
	}
}

func inputWithoutCompactionItems(input gjson.Result) string {
	if !input.IsArray() {
		return normalizeJSONArrayRaw([]byte(input.Raw))
	}
	filtered := make([]string, 0, len(input.Array()))
	for _, item := range input.Array() {
		if isResponsesWebsocketCompactionReplayItemType(item.Get("type").String()) {
			continue
		}
		filtered = append(filtered, item.Raw)
	}
	return "[" + strings.Join(filtered, ",") + "]"
}

func normalizeJSONArrayRaw(raw []byte) string {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return "[]"
	}
	result := gjson.Parse(trimmed)
	if result.Type == gjson.JSON && result.IsArray() {
		return trimmed
	}
	return "[]"
}
