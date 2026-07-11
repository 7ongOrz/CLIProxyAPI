package openai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	requestlogging "github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

type websocketTimelineAppender interface {
	Append(eventType string, payload []byte, timestamp time.Time)
}

type responsesWebsocketPinnedAuthState struct {
	authID   string
	modelKey string
}

type websocketTimelineLog struct {
	enabled bool
	source  *requestlogging.FileBodySource
	builder *strings.Builder

	currentPart       io.WriteCloser
	currentPartHasLog bool
}

func newWebsocketTimelineLog(enabled bool, source *requestlogging.FileBodySource) *websocketTimelineLog {
	if !enabled {
		return &websocketTimelineLog{}
	}
	if source == nil {
		return newInMemoryWebsocketTimelineLog()
	}
	return &websocketTimelineLog{
		enabled: true,
		source:  source,
	}
}

func newInMemoryWebsocketTimelineLog() *websocketTimelineLog {
	return &websocketTimelineLog{
		enabled: true,
		builder: &strings.Builder{},
	}
}

func websocketTimelineSourceFromContext(c *gin.Context) *requestlogging.FileBodySource {
	if c == nil {
		return nil
	}
	value, exists := c.Get(requestlogging.WebsocketTimelineSourceContextKey)
	if !exists {
		return nil
	}
	source, ok := value.(*requestlogging.FileBodySource)
	if !ok {
		return nil
	}
	return source
}

func (l *websocketTimelineLog) BeginRequest() {
	if l == nil || !l.enabled || l.source == nil {
		return
	}
	l.closeCurrentPart()
	part, errCreate := l.source.CreatePart("request")
	if errCreate != nil {
		log.WithError(errCreate).Warn("failed to create websocket request detail log")
		return
	}
	l.currentPart = part
	l.currentPartHasLog = false
}

func (l *websocketTimelineLog) Append(eventType string, payload []byte, timestamp time.Time) {
	if l == nil || !l.enabled {
		return
	}
	data := formatWebsocketTimelineEvent(eventType, payload, timestamp)
	if len(data) == 0 {
		return
	}
	if l.source != nil {
		if l.currentPart == nil {
			l.BeginRequest()
		}
		if l.currentPart == nil {
			return
		}
		if errWrite := writeWebsocketTimelinePart(l.currentPart, data, l.currentPartHasLog); errWrite != nil {
			log.WithError(errWrite).Warn("failed to write websocket request detail log")
			return
		}
		l.currentPartHasLog = true
		return
	}
	if l.builder != nil {
		writeWebsocketTimelineBuilder(l.builder, data)
	}
}

func (l *websocketTimelineLog) SetContext(c *gin.Context) {
	if l == nil || !l.enabled {
		return
	}
	l.closeCurrentPart()
	if l.source != nil {
		if l.source.HasPayload() {
			c.Set(requestlogging.WebsocketTimelineSourceContextKey, l.source)
			return
		}
		if errCleanup := l.source.Cleanup(); errCleanup != nil {
			log.WithError(errCleanup).Warn("failed to clean up empty websocket timeline log parts")
		}
	}
	if l.builder != nil {
		setWebsocketTimelineBody(c, l.builder.String())
	}
}

func (l *websocketTimelineLog) String() string {
	if l == nil || !l.enabled {
		return ""
	}
	l.closeCurrentPart()
	if l.source != nil {
		data, errRead := l.source.Bytes()
		if errRead != nil {
			return ""
		}
		return string(data)
	}
	if l.builder == nil {
		return ""
	}
	return l.builder.String()
}

func (l *websocketTimelineLog) closeCurrentPart() {
	if l == nil || l.currentPart == nil {
		return
	}
	if errClose := l.currentPart.Close(); errClose != nil {
		log.WithError(errClose).Warn("failed to close websocket request detail log")
	}
	l.currentPart = nil
	l.currentPartHasLog = false
}

func writeWebsocketTimelinePart(w io.Writer, data []byte, prependNewline bool) error {
	if w == nil || len(data) == 0 {
		return nil
	}
	if prependNewline {
		if _, errWrite := io.WriteString(w, "\n"); errWrite != nil {
			return errWrite
		}
	}
	_, errWrite := w.Write(data)
	return errWrite
}

func writeWebsocketTimelineBuilder(builder *strings.Builder, data []byte) {
	if builder == nil || len(data) == 0 {
		return
	}
	if builder.Len() > 0 {
		builder.WriteString("\n")
	}
	builder.Write(data)
}

func appendWebsocketEvent(builder *strings.Builder, eventType string, payload []byte) {
	if builder == nil {
		return
	}
	trimmedPayload := bytes.TrimSpace(payload)
	if len(trimmedPayload) == 0 {
		return
	}
	if builder.Len() > 0 {
		builder.WriteString("\n")
	}
	builder.WriteString("websocket.")
	builder.WriteString(eventType)
	builder.WriteString("\n")
	builder.Write(trimmedPayload)
	builder.WriteString("\n")
}

func websocketPayloadEventType(payload []byte) string {
	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	if eventType == "" {
		return "-"
	}
	return eventType
}

func websocketPayloadPreview(payload []byte) string {
	trimmedPayload := bytes.TrimSpace(payload)
	if len(trimmedPayload) == 0 {
		return "<empty>"
	}
	previewText := strings.ReplaceAll(string(trimmedPayload), "\n", "\\n")
	previewText = strings.ReplaceAll(previewText, "\r", "\\r")
	return previewText
}

func logResponsesWebsocketDownstreamError(sessionID string, payload []byte) {
	log.WithFields(log.Fields{
		"id":     sessionID,
		"type":   websocket.TextMessage,
		"event":  websocketPayloadEventType(payload),
		"status": int(gjson.GetBytes(payload, "status").Int()),
	}).Info("responses websocket: downstream_out")
}

func isResponsesWebsocketCompletionEvent(eventType string) bool {
	return eventType == wsEventTypeCompleted || eventType == wsEventTypeDone
}

type responsesWebsocketPayloadError struct {
	status  int
	payload []byte
}

func (e *responsesWebsocketPayloadError) Error() string {
	if e == nil {
		return ""
	}
	return string(e.payload)
}

func (e *responsesWebsocketPayloadError) StatusCode() int {
	if e == nil {
		return 0
	}
	return e.status
}

func responsesWebsocketErrorMessageFromPayload(payload []byte) *interfaces.ErrorMessage {
	status, hasExplicitStatus := responsesWebsocketExplicitErrorStatus(payload)
	errText := strings.TrimSpace(gjson.GetBytes(payload, "error.message").String())
	if errText == "" {
		errText = strings.TrimSpace(gjson.GetBytes(payload, "response.error.message").String())
	}
	if errText == "" {
		errText = strings.TrimSpace(gjson.GetBytes(payload, "body.error.message").String())
	}
	if errText == "" {
		errText = strings.TrimSpace(gjson.GetBytes(payload, "message").String())
	}
	if errText == "" {
		errText = strings.TrimSpace(gjson.GetBytes(payload, "error.code").String())
	}
	if errText == "" {
		errText = strings.TrimSpace(gjson.GetBytes(payload, "response.error.code").String())
	}
	if errText == "" {
		errText = strings.TrimSpace(gjson.GetBytes(payload, "body.error.code").String())
	}
	errPayload := responsesWebsocketStructuredErrorPayload(payload)
	if !hasExplicitStatus {
		status = responsesWebsocketInferredErrorStatus(payload, errText, errPayload)
	}
	if status <= 0 {
		status = http.StatusInternalServerError
	}

	trimmedPayload := bytes.TrimSpace(payload)
	if len(trimmedPayload) > 0 {
		return &interfaces.ErrorMessage{
			StatusCode: status,
			Error: &responsesWebsocketPayloadError{
				status:  status,
				payload: bytes.Clone(trimmedPayload),
			},
		}
	}
	return &interfaces.ErrorMessage{StatusCode: status, Error: fmt.Errorf("%s", http.StatusText(status))}
}

func responsesWebsocketIncompleteErrorMessageFromPayload(payload []byte) *interfaces.ErrorMessage {
	reason := strings.TrimSpace(gjson.GetBytes(payload, "response.incomplete_details.reason").String())
	if reason == "" {
		reason = "unknown"
	}
	return &interfaces.ErrorMessage{
		StatusCode: http.StatusBadRequest,
		Error:      fmt.Errorf("Incomplete response returned, reason: %s", reason),
	}
}

func responsesWebsocketExplicitErrorStatus(payload []byte) (int, bool) {
	for _, path := range []string{
		"status",
		"status_code",
		"response.status_code",
		"response.error.status",
		"response.error.status_code",
		"error.status",
		"error.status_code",
		"body.error.status",
		"body.error.status_code",
	} {
		status := int(gjson.GetBytes(payload, path).Int())
		if status > 0 {
			return status, true
		}
	}
	return 0, false
}

func responsesWebsocketInferredErrorStatus(payload []byte, errText string, errPayload string) int {
	if responsesWebsocketErrorTextIndicatesPreviousResponseNotFound(errText) ||
		responsesWebsocketErrorIndicatesPreviousResponseNotFound(errPayload) {
		return http.StatusBadRequest
	}

	errType := responsesWebsocketLowerPayloadString(payload, "error.type", "response.error.type", "body.error.type")
	errCode := responsesWebsocketLowerPayloadString(payload, "error.code", "response.error.code", "body.error.code", "code")
	switch {
	case errType == "usage_limit_reached" ||
		errType == "insufficient_quota" ||
		errType == "rate_limit_error" ||
		errCode == "insufficient_quota" ||
		errCode == "rate_limit_exceeded" ||
		responsesWebsocketErrorTextIndicatesModelCapacity(errText):
		return http.StatusTooManyRequests
	case errType == "authentication_error":
		return http.StatusUnauthorized
	case errType == "permission_error":
		return http.StatusForbidden
	case errType == "invalid_request_error" ||
		errCode == "invalid_request_error" ||
		errCode == "previous_response_not_found" ||
		errCode == "context_length_exceeded" ||
		errCode == "context_too_large" ||
		errCode == "invalid_prompt" ||
		errCode == "bio_policy" ||
		errCode == "cyber_policy":
		return http.StatusBadRequest
	default:
		return 0
	}
}

func responsesWebsocketLowerPayloadString(payload []byte, paths ...string) string {
	for _, path := range paths {
		value := strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, path).String()))
		if value != "" {
			return value
		}
	}
	return ""
}

func responsesWebsocketErrorTextIndicatesModelCapacity(text string) bool {
	lower := strings.ToLower(strings.TrimSpace(text))
	return strings.Contains(lower, "selected model is at capacity") ||
		strings.Contains(lower, "model is at capacity. please try a different model")
}

func responsesWebsocketStructuredErrorPayload(payload []byte) string {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 || !json.Valid(payload) {
		return ""
	}
	for _, path := range []string{"error", "body.error", "response.error", "code"} {
		if gjson.GetBytes(payload, path).Exists() {
			return string(payload)
		}
	}
	return ""
}

func setWebsocketTimelineBody(c *gin.Context, body string) {
	setWebsocketBody(c, wsTimelineBodyKey, body)
}

func setWebsocketBody(c *gin.Context, key string, body string) {
	if c == nil {
		return
	}
	trimmedBody := strings.TrimSpace(body)
	if trimmedBody == "" {
		return
	}
	c.Set(key, []byte(trimmedBody))
}

func writeResponsesWebsocketPayload(writer *responsesWebsocketWriter, wsTimelineLog websocketTimelineAppender, payload []byte, timestamp time.Time) error {
	if wsTimelineLog != nil {
		wsTimelineLog.Append("response", payload, timestamp)
	}
	if writer == nil || writer.conn == nil {
		return fmt.Errorf("responses websocket: writer is nil")
	}
	writer.writeMu.Lock()
	defer writer.writeMu.Unlock()
	if writer.closing.Load() {
		return websocket.ErrCloseSent
	}
	return writer.conn.WriteMessage(websocket.TextMessage, payload)
}

func startResponsesWebsocketHeartbeat(conn *websocket.Conn, done <-chan struct{}, sessionID string) {
	startResponsesWebsocketHeartbeatWithInterval(conn, done, sessionID, wsHeartbeatInterval)
}

func startResponsesWebsocketHeartbeatWithInterval(conn *websocket.Conn, done <-chan struct{}, sessionID string, interval time.Duration) {
	if conn == nil || interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if errWrite := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Time{}); errWrite != nil {
					log.Debugf("responses websocket: heartbeat ping failed id=%s error=%v", strings.TrimSpace(sessionID), errWrite)
					_ = conn.Close()
					return
				}
			}
		}
	}()
}

func appendWebsocketTimelineDisconnect(timeline websocketTimelineAppender, err error, timestamp time.Time) {
	if err == nil {
		return
	}
	if timeline != nil {
		timeline.Append("disconnect", []byte(err.Error()), timestamp)
	}
}

func appendWebsocketTimelineEvent(builder *strings.Builder, eventType string, payload []byte, timestamp time.Time) {
	if builder == nil {
		return
	}
	writeWebsocketTimelineBuilder(builder, formatWebsocketTimelineEvent(eventType, payload, timestamp))
}

func formatWebsocketTimelineEvent(eventType string, payload []byte, timestamp time.Time) []byte {
	trimmedPayload := bytes.TrimSpace(payload)
	if len(trimmedPayload) == 0 {
		return nil
	}
	var builder strings.Builder
	builder.WriteString("Timestamp: ")
	builder.WriteString(timestamp.Format(time.RFC3339Nano))
	builder.WriteString("\n")
	builder.WriteString("Event: websocket.")
	builder.WriteString(eventType)
	builder.WriteString("\n")
	builder.Write(trimmedPayload)
	builder.WriteString("\n")
	return []byte(builder.String())
}

func markAPIResponseTimestamp(c *gin.Context) {
	if c == nil {
		return
	}
	if _, exists := c.Get("API_RESPONSE_TIMESTAMP"); exists {
		return
	}
	c.Set("API_RESPONSE_TIMESTAMP", time.Now())
}
