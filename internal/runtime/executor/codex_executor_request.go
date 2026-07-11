package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/misc"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	codexUserAgent                = "codex-tui/0.146.0 (Mac OS 26.5.0; arm64) iTerm.app/3.6.10 (codex-tui; 0.146.0)"
	codexOriginator               = "codex-tui"
	codexDefaultImageToolModel    = "gpt-image-2"
	codexTurnStateHeader          = "X-Codex-Turn-State"
	codexResponsesLiteHeader      = "X-OpenAI-Internal-Codex-Responses-Lite"
	codexResponsesLiteMetadataKey = "ws_request_header_x_openai_internal_codex_responses_lite"
	codexResponsesLiteMetadata    = "client_metadata." + codexResponsesLiteMetadataKey
)

var dataTag = []byte("data:")

func translateCodexRequestPair(from, to sdktranslator.Format, model string, originalPayload, payload []byte, stream bool, preserveEmptyThinkingBlocks ...bool) ([]byte, []byte) {
	isCompat := len(preserveEmptyThinkingBlocks) > 0 && preserveEmptyThinkingBlocks[0]
	translate := func(raw []byte) []byte {
		if isCompat && from == sdktranslator.FormatClaude && to == sdktranslator.FormatCodex {
			return helps.TranslateRequestWithAPIKeyModelCompatibility(context.Background(), nil, nil, from, to, model, raw, stream, true)
		}
		return sdktranslator.TranslateRequest(from, to, model, raw, stream)
	}
	if bytes.Equal(originalPayload, payload) {
		body := translate(payload)
		return body, body
	}
	originalTranslated := translate(originalPayload)
	body := translate(payload)
	return originalTranslated, body
}

// PrepareRequest injects Codex credentials into the outgoing HTTP request.
func (e *CodexExecutor) PrepareRequest(req *http.Request, auth *cliproxyauth.Auth) error {
	if req == nil {
		return nil
	}
	apiKey, _ := codexCreds(auth)
	if strings.TrimSpace(apiKey) != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(req, attrs)
	return nil
}

// HttpRequest injects Codex credentials into the request and executes it.
func (e *CodexExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("codex executor: request is nil")
	}
	if ctx == nil {
		ctx = req.Context()
	}
	httpReq := req.WithContext(ctx)
	if err := e.PrepareRequest(httpReq, auth); err != nil {
		return nil, err
	}
	httpClient := helps.NewUtlsHTTPClient(ctx, e.cfg, auth, 0)
	return httpClient.Do(httpReq)
}

type codexIdentityConfuseState struct {
	enabled                bool
	authID                 string
	originalPromptCacheKey string
	promptCacheKey         string
	installationIDs        []codexIdentityReplacement
	sessionIDs             []codexIdentityReplacement
	threadIDs              []codexIdentityReplacement
	turnIDs                []codexIdentityReplacement
	sessionID              string
	threadID               string
}

type codexIdentityReplacement struct {
	original string
	confused string
}

func (e *CodexExecutor) cacheHelper(ctx context.Context, from sdktranslator.Format, url string, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, userPayload []byte, rawJSON []byte, headerSets ...http.Header) (*http.Request, []byte, codexIdentityConfuseState, error) {
	var headers http.Header
	if len(headerSets) > 0 {
		headers = headerSets[0]
	}
	var cache helps.CodexCache
	if sourceFormatEqual(from, sdktranslator.FormatClaude) {
		modelName := strings.TrimSpace(gjson.GetBytes(rawJSON, "model").String())
		if modelName == "" {
			modelName = thinking.ParseSuffix(req.Model).ModelName
		}
		cached, ok, errCache := helps.ClaudeCodePromptCache(ctx, modelName, req.Payload, headers)
		if errCache != nil {
			return nil, nil, codexIdentityConfuseState{}, errCache
		}
		if ok {
			cache = cached
		}
	} else if sourceFormatEqual(from, sdktranslator.FormatOpenAIResponse) {
		promptCacheKey := gjson.GetBytes(req.Payload, "prompt_cache_key")
		if promptCacheKey.Exists() {
			cache.ID = promptCacheKey.String()
		}
	} else if sourceFormatEqual(from, sdktranslator.FormatOpenAI) {
		if promptCacheKey := gjson.GetBytes(req.Payload, "prompt_cache_key"); promptCacheKey.Exists() {
			cache.ID = strings.TrimSpace(promptCacheKey.String())
		}
		if cache.ID == "" {
			cache.ID = helps.ProviderSessionUUID("codex", req.Metadata)
		}
		if cache.ID == "" {
			if apiKey := strings.TrimSpace(helps.APIKeyFromContext(ctx)); apiKey != "" {
				cache.ID = uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-proxy-api:codex:prompt-cache:"+apiKey)).String()
			}
		}
	}
	if cache.ID == "" {
		cache.ID = helps.ProviderSessionUUID("codex", req.Metadata)
	}

	if cache.ID != "" {
		rawJSON = helps.SetStringIfDifferent(rawJSON, "prompt_cache_key", cache.ID)
	}
	rawJSON = helps.SanitizeCodexInputItemIDs(rawJSON)
	var identityState codexIdentityConfuseState
	rawJSON, identityState = applyCodexIdentityConfuseBody(e.cfg, auth, userPayload, rawJSON)
	if identityState.promptCacheKey != "" {
		cache.ID = identityState.promptCacheKey
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(rawJSON))
	if err != nil {
		return nil, nil, codexIdentityConfuseState{}, err
	}
	if cache.ID != "" {
		httpReq.Header.Set("Session_id", cache.ID)
	}
	return httpReq, rawJSON, identityState, nil
}

func applyCodexIdentityConfuseBody(cfg *config.Config, auth *cliproxyauth.Auth, userPayload []byte, rawJSON []byte) ([]byte, codexIdentityConfuseState) {
	if !codexIdentityConfuseEnabled(cfg) || auth == nil || strings.TrimSpace(auth.ID) == "" || len(rawJSON) == 0 {
		return rawJSON, codexIdentityConfuseState{}
	}

	state := codexIdentityConfuseState{enabled: true, authID: strings.TrimSpace(auth.ID)}
	if promptCacheKey := codexIdentityPromptCacheKeyFromPayload(userPayload, rawJSON); promptCacheKey != "" {
		state.setPromptCacheKey(promptCacheKey)
	}
	if state.promptCacheKey != "" && gjson.GetBytes(rawJSON, "prompt_cache_key").Exists() {
		rawJSON = helps.SetStringIfDifferent(rawJSON, "prompt_cache_key", state.promptCacheKey)
	}
	if installationID := codexIdentityInstallationIDFromPayload(userPayload, rawJSON); installationID != "" {
		rawJSON, _ = sjson.SetBytes(rawJSON, "client_metadata.x-codex-installation-id", state.confuseInstallationID(installationID))
	}
	if sessionID := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.session_id").String()); sessionID != "" {
		state.sessionID = state.confuseSessionID(sessionID)
		rawJSON, _ = sjson.SetBytes(rawJSON, "client_metadata.session_id", state.sessionID)
	}
	if threadID := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.thread_id").String()); threadID != "" {
		state.threadID = state.confuseThreadID(threadID)
		rawJSON, _ = sjson.SetBytes(rawJSON, "client_metadata.thread_id", state.threadID)
	}
	if turnID := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.turn_id").String()); turnID != "" {
		rawJSON, _ = sjson.SetBytes(rawJSON, "client_metadata.turn_id", state.confuseTurnID(turnID))
	}
	if parentTurnID := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.parent_turn_id").String()); parentTurnID != "" {
		rawJSON, _ = sjson.SetBytes(rawJSON, "client_metadata.parent_turn_id", state.confuseTurnID(parentTurnID))
	}
	rawJSON = applyCodexIdentityConfuseInputTurnIDs(rawJSON, &state)
	if parentThreadID := codexIdentityParentThreadIDFromPayload(userPayload, rawJSON); parentThreadID != "" {
		rawJSON, _ = sjson.SetBytes(rawJSON, "client_metadata.x-codex-parent-thread-id", state.confuseThreadID(parentThreadID))
	}
	if turnMetadata := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.x-codex-turn-metadata").String()); turnMetadata != "" {
		rawJSON, _ = sjson.SetBytes(rawJSON, "client_metadata.x-codex-turn-metadata", applyCodexTurnMetadataIdentityConfuse(turnMetadata, &state))
	}
	if windowID := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.x-codex-window-id").String()); windowID != "" {
		rawJSON, _ = sjson.SetBytes(rawJSON, "client_metadata.x-codex-window-id", confuseCodexWindowID(windowID, &state))
	}

	return rawJSON, state
}

func applyCodexIdentityConfuseInputTurnIDs(body []byte, state *codexIdentityConfuseState) []byte {
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return body
	}

	items := input.Array()
	rebuilt := make([]string, 0, len(items))
	changed := false
	for _, item := range items {
		raw := item.Raw
		turnID := strings.TrimSpace(item.Get("internal_chat_message_metadata_passthrough.turn_id").String())
		if turnID != "" {
			next, errSet := sjson.SetBytes([]byte(raw), "internal_chat_message_metadata_passthrough.turn_id", state.confuseTurnID(turnID))
			if errSet == nil {
				raw = string(next)
				changed = true
			}
		}
		rebuilt = append(rebuilt, raw)
	}
	if !changed {
		return body
	}

	updated, errSet := sjson.SetRawBytes(body, "input", []byte("["+strings.Join(rebuilt, ",")+"]"))
	if errSet != nil {
		return body
	}
	return updated
}

func applyCodexIdentityConfuseHeaders(headers http.Header, state *codexIdentityConfuseState) {
	if headers == nil {
		return
	}
	if state == nil || !state.enabled {
		return
	}

	if state.promptCacheKey == "" {
		state.setPromptCacheKey(codexIdentityPromptCacheKeyFromHeaders(headers))
	}

	if installationID := strings.TrimSpace(headerValueCaseInsensitive(headers, "X-Codex-Installation-Id")); installationID != "" {
		headers.Set("X-Codex-Installation-Id", state.confuseInstallationID(installationID))
	}
	updatedTurnMetadata := ""
	if rawTurnMetadata := strings.TrimSpace(headers.Get("X-Codex-Turn-Metadata")); rawTurnMetadata != "" {
		updatedTurnMetadata = applyCodexTurnMetadataIdentityConfuse(rawTurnMetadata, state)
		headers.Set("X-Codex-Turn-Metadata", updatedTurnMetadata)
	}
	if parentThreadID := strings.TrimSpace(headerValueCaseInsensitive(headers, "X-Codex-Parent-Thread-Id")); parentThreadID != "" {
		headers.Set("X-Codex-Parent-Thread-Id", state.confuseThreadID(parentThreadID))
	}
	sessionID := strings.TrimSpace(state.sessionID)
	if sessionID == "" {
		sessionID = state.confuseSessionID(codexSessionHeaderValue(headers))
		state.sessionID = sessionID
	}
	if sessionID == "" {
		sessionID = strings.TrimSpace(state.promptCacheKey)
	}
	if sessionID != "" {
		setCodexSessionHeaderCasePreserved(headers, "Session_id", sessionID)
	}
	threadID := strings.TrimSpace(state.threadID)
	if threadID == "" {
		threadID = strings.TrimSpace(headerValueCaseInsensitive(headers, "Thread-Id"))
		if threadID == "" {
			threadID = strings.TrimSpace(headerValueCaseInsensitive(headers, "X-Client-Request-Id"))
		}
		threadID = state.confuseThreadID(threadID)
		state.threadID = threadID
	}
	if threadID == "" {
		threadID = strings.TrimSpace(state.promptCacheKey)
	}
	if threadID != "" {
		headers.Set("X-Client-Request-Id", threadID)
		headers.Set("Thread-Id", threadID)
	}

	if state.promptCacheKey != "" && headerValueCaseInsensitive(headers, "Conversation_id") != "" {
		setHeaderCasePreserved(headers, "Conversation_id", state.promptCacheKey)
	}
	windowID := strings.TrimSpace(headerValueCaseInsensitive(headers, "X-Codex-Window-Id"))
	if windowID == "" && updatedTurnMetadata != "" {
		windowID = strings.TrimSpace(gjson.Get(updatedTurnMetadata, "window_id").String())
	}
	if confusedWindowID := confuseCodexWindowID(windowID, state); confusedWindowID != "" {
		headers.Set("X-Codex-Window-Id", confusedWindowID)
	}
}

func applyCodexTurnMetadataIdentityConfuse(rawTurnMetadata string, state *codexIdentityConfuseState) string {
	updatedTurnMetadata := rawTurnMetadata
	if state == nil || !state.enabled {
		return updatedTurnMetadata
	}
	if state.promptCacheKey != "" && gjson.Get(rawTurnMetadata, "prompt_cache_key").Exists() {
		updatedTurnMetadata, _ = sjson.Set(updatedTurnMetadata, "prompt_cache_key", state.promptCacheKey)
	}
	if installationID := strings.TrimSpace(gjson.Get(rawTurnMetadata, "installation_id").String()); installationID != "" {
		updatedTurnMetadata, _ = sjson.Set(updatedTurnMetadata, "installation_id", state.confuseInstallationID(installationID))
	}
	if sessionID := strings.TrimSpace(gjson.Get(rawTurnMetadata, "session_id").String()); sessionID != "" {
		state.sessionID = state.confuseSessionID(sessionID)
		updatedTurnMetadata, _ = sjson.Set(updatedTurnMetadata, "session_id", state.sessionID)
	}
	if threadID := strings.TrimSpace(gjson.Get(rawTurnMetadata, "thread_id").String()); threadID != "" {
		state.threadID = state.confuseThreadID(threadID)
		updatedTurnMetadata, _ = sjson.Set(updatedTurnMetadata, "thread_id", state.threadID)
	}
	if turnID := strings.TrimSpace(gjson.Get(rawTurnMetadata, "turn_id").String()); turnID != "" {
		updatedTurnMetadata, _ = sjson.Set(updatedTurnMetadata, "turn_id", state.confuseTurnID(turnID))
	}
	if parentTurnID := strings.TrimSpace(gjson.Get(rawTurnMetadata, "parent_turn_id").String()); parentTurnID != "" {
		updatedTurnMetadata, _ = sjson.Set(updatedTurnMetadata, "parent_turn_id", state.confuseTurnID(parentTurnID))
	}
	if forkedFromThreadID := strings.TrimSpace(gjson.Get(rawTurnMetadata, "forked_from_thread_id").String()); forkedFromThreadID != "" {
		updatedTurnMetadata, _ = sjson.Set(updatedTurnMetadata, "forked_from_thread_id", state.confuseThreadID(forkedFromThreadID))
	}
	if parentThreadID := strings.TrimSpace(gjson.Get(rawTurnMetadata, "parent_thread_id").String()); parentThreadID != "" {
		updatedTurnMetadata, _ = sjson.Set(updatedTurnMetadata, "parent_thread_id", state.confuseThreadID(parentThreadID))
	}
	if gjson.Get(rawTurnMetadata, "window_id").Exists() {
		windowID := strings.TrimSpace(gjson.Get(rawTurnMetadata, "window_id").String())
		updatedTurnMetadata, _ = sjson.Set(updatedTurnMetadata, "window_id", confuseCodexWindowID(windowID, state))
	}
	return updatedTurnMetadata
}

func codexIdentityPromptCacheKeyFromPayload(userPayload []byte, rawJSON []byte) string {
	if promptCacheKey := strings.TrimSpace(gjson.GetBytes(userPayload, "prompt_cache_key").String()); promptCacheKey != "" {
		return promptCacheKey
	}
	if promptCacheKey := strings.TrimSpace(gjson.GetBytes(rawJSON, "prompt_cache_key").String()); promptCacheKey != "" {
		return promptCacheKey
	}
	if turnMetadata := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.x-codex-turn-metadata").String()); turnMetadata != "" {
		if promptCacheKey := strings.TrimSpace(gjson.Get(turnMetadata, "prompt_cache_key").String()); promptCacheKey != "" {
			return promptCacheKey
		}
		if windowID := strings.TrimSpace(gjson.Get(turnMetadata, "window_id").String()); windowID != "" {
			if promptCacheKey := codexPromptCacheKeyFromWindowID(windowID); promptCacheKey != "" {
				return promptCacheKey
			}
		}
	}
	if windowID := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.x-codex-window-id").String()); windowID != "" {
		return codexPromptCacheKeyFromWindowID(windowID)
	}
	return ""
}

func codexIdentityInstallationIDFromPayload(userPayload []byte, rawJSON []byte) string {
	if installationID := strings.TrimSpace(gjson.GetBytes(userPayload, "client_metadata.x-codex-installation-id").String()); installationID != "" {
		return installationID
	}
	return strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.x-codex-installation-id").String())
}

func codexIdentityParentThreadIDFromPayload(userPayload []byte, rawJSON []byte) string {
	if parentThreadID := strings.TrimSpace(gjson.GetBytes(userPayload, "client_metadata.x-codex-parent-thread-id").String()); parentThreadID != "" {
		return parentThreadID
	}
	return strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.x-codex-parent-thread-id").String())
}

func codexIdentityPromptCacheKeyFromHeaders(headers http.Header) string {
	if headers == nil {
		return ""
	}
	if turnMetadata := strings.TrimSpace(headers.Get("X-Codex-Turn-Metadata")); turnMetadata != "" {
		if promptCacheKey := strings.TrimSpace(gjson.Get(turnMetadata, "prompt_cache_key").String()); promptCacheKey != "" {
			return promptCacheKey
		}
		if windowID := strings.TrimSpace(gjson.Get(turnMetadata, "window_id").String()); windowID != "" {
			if promptCacheKey := codexPromptCacheKeyFromWindowID(windowID); promptCacheKey != "" {
				return promptCacheKey
			}
		}
	}
	if windowID := strings.TrimSpace(headerValueCaseInsensitive(headers, "X-Codex-Window-Id")); windowID != "" {
		if promptCacheKey := codexPromptCacheKeyFromWindowID(windowID); promptCacheKey != "" {
			return promptCacheKey
		}
	}
	if threadID := strings.TrimSpace(headerValueCaseInsensitive(headers, "Thread-Id")); threadID != "" {
		return threadID
	}
	return ""
}

func codexPromptCacheKeyFromWindowID(windowID string) string {
	windowID = strings.TrimSpace(windowID)
	suffix, ok := codexWindowIDGenerationSuffix(windowID)
	if !ok {
		return windowID
	}
	return strings.TrimSpace(strings.TrimSuffix(windowID, suffix))
}

func confuseCodexWindowID(windowID string, state *codexIdentityConfuseState) string {
	if state == nil || !state.enabled {
		return strings.TrimSpace(windowID)
	}

	windowID = strings.TrimSpace(windowID)
	promptCacheKey := strings.TrimSpace(state.promptCacheKey)
	if windowID == "" {
		if promptCacheKey != "" {
			return promptCacheKey + ":0"
		}
		if threadID := strings.TrimSpace(state.threadID); threadID != "" {
			return threadID + ":0"
		}
		return ""
	}
	if promptCacheKey != "" && (windowID == promptCacheKey || strings.HasPrefix(windowID, promptCacheKey+":")) {
		if windowID == promptCacheKey {
			return promptCacheKey
		}
		if suffix, ok := codexWindowIDGenerationSuffix(windowID); ok {
			return promptCacheKey + suffix
		}
		return promptCacheKey + ":0"
	}

	originalPromptCacheKey := strings.TrimSpace(state.originalPromptCacheKey)
	if originalPromptCacheKey != "" {
		if windowID == originalPromptCacheKey {
			return promptCacheKey
		}
		if strings.HasPrefix(windowID, originalPromptCacheKey+":") {
			if suffix, ok := codexWindowIDGenerationSuffix(windowID); ok {
				return promptCacheKey + suffix
			}
			return promptCacheKey + ":0"
		}
	}

	if suffix, ok := codexWindowIDGenerationSuffix(windowID); ok {
		windowBase := strings.TrimSpace(strings.TrimSuffix(windowID, suffix))
		if windowBase != "" {
			return state.confuseThreadID(windowBase) + suffix
		}
	}
	if threadID := strings.TrimSpace(state.threadID); threadID != "" {
		return threadID + ":0"
	}
	if promptCacheKey != "" {
		return promptCacheKey + ":0"
	}
	return windowID
}

func codexWindowIDGenerationSuffix(windowID string) (string, bool) {
	idx := strings.LastIndex(windowID, ":")
	if idx < 0 || idx == len(windowID)-1 {
		return "", false
	}
	for _, ch := range windowID[idx+1:] {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}
	return windowID[idx:], true
}

func applyCodexIdentityConfuseResponseLog(payload []byte, state codexIdentityConfuseState) []byte {
	replacements := codexIdentityResponseReplacements(state)
	pairs := make([]string, 0, len(replacements)*2)
	for _, replacement := range replacements {
		pairs = append(pairs, replacement.original, replacement.confused)
	}
	if len(pairs) == 0 {
		return payload
	}
	return replaceCodexIdentityResponseLogStringValues(payload, strings.NewReplacer(pairs...))
}

func appendCodexAPIResponseLog(ctx context.Context, cfg *config.Config, payload []byte, state codexIdentityConfuseState) {
	if cfg == nil || !cfg.RequestLog || cfg.CommercialMode {
		return
	}
	helps.AppendAPIResponseChunk(ctx, cfg, applyCodexIdentityConfuseResponseLog(payload, state))
}

func appendCodexWebsocketResponseLog(ctx context.Context, cfg *config.Config, payload []byte, state codexIdentityConfuseState) {
	if cfg == nil || !cfg.RequestLog || cfg.CommercialMode {
		return
	}
	helps.AppendAPIWebsocketResponse(ctx, cfg, applyCodexIdentityConfuseResponseLog(payload, state))
}

func applyCodexIdentityExposeResponsePayload(payload []byte, state codexIdentityConfuseState) []byte {
	replacements := codexIdentityResponseReplacements(state)
	pairs := make([]string, 0, len(replacements)*2)
	for _, replacement := range replacements {
		pairs = append(pairs, replacement.confused, replacement.original)
	}
	if len(pairs) == 0 {
		return payload
	}
	return replaceCodexIdentityResponseStringValues(payload, strings.NewReplacer(pairs...))
}

func codexIdentityResponseReplacements(state codexIdentityConfuseState) []codexIdentityReplacement {
	replacements := make([]codexIdentityReplacement, 0, 1+len(state.installationIDs)+len(state.sessionIDs)+len(state.threadIDs)+len(state.turnIDs))
	if state.originalPromptCacheKey != "" && state.promptCacheKey != "" && state.originalPromptCacheKey != state.promptCacheKey {
		replacements = append(replacements, codexIdentityReplacement{
			original: state.originalPromptCacheKey,
			confused: state.promptCacheKey,
		})
	}
	replacements = append(replacements, state.installationIDs...)
	replacements = append(replacements, state.sessionIDs...)
	replacements = append(replacements, state.threadIDs...)
	replacements = append(replacements, state.turnIDs...)
	return replacements
}

func (state *codexIdentityConfuseState) confuseInstallationID(installationID string) string {
	return state.confuseIdentityID(installationID, "installation", &state.installationIDs)
}

func (state *codexIdentityConfuseState) confuseSessionID(sessionID string) string {
	if state != nil && (sessionID == state.originalPromptCacheKey || sessionID == state.promptCacheKey) {
		return state.promptCacheKey
	}
	return state.confuseIdentityID(sessionID, "session", &state.sessionIDs)
}

func (state *codexIdentityConfuseState) confuseThreadID(threadID string) string {
	if state != nil && (threadID == state.originalPromptCacheKey || threadID == state.promptCacheKey) {
		return state.promptCacheKey
	}
	return state.confuseIdentityID(threadID, "prompt-cache", &state.threadIDs)
}

func (state *codexIdentityConfuseState) confuseIdentityID(identityID string, kind string, replacements *[]codexIdentityReplacement) string {
	identityID = strings.TrimSpace(identityID)
	if state == nil || !state.enabled || strings.TrimSpace(state.authID) == "" || identityID == "" {
		return identityID
	}
	for _, replacement := range *replacements {
		if replacement.original == identityID || replacement.confused == identityID {
			return replacement.confused
		}
	}
	confusedID := codexIdentityConfuseUUID(state.authID, kind, identityID)
	*replacements = append(*replacements, codexIdentityReplacement{original: identityID, confused: confusedID})
	return confusedID
}

func (state *codexIdentityConfuseState) confuseTurnID(turnID string) string {
	turnID = strings.TrimSpace(turnID)
	if state == nil || !state.enabled || strings.TrimSpace(state.authID) == "" || turnID == "" {
		return turnID
	}
	for _, replacement := range state.turnIDs {
		if replacement.original == turnID || replacement.confused == turnID {
			return replacement.confused
		}
	}
	confusedTurnID := codexIdentityConfuseUUID(state.authID, "turn", turnID)
	state.turnIDs = append(state.turnIDs, codexIdentityReplacement{original: turnID, confused: confusedTurnID})
	return confusedTurnID
}

func (state *codexIdentityConfuseState) setPromptCacheKey(promptCacheKey string) {
	promptCacheKey = strings.TrimSpace(promptCacheKey)
	if state == nil || !state.enabled || strings.TrimSpace(state.authID) == "" || promptCacheKey == "" {
		return
	}
	state.originalPromptCacheKey = promptCacheKey
	state.promptCacheKey = codexIdentityConfuseUUID(state.authID, "prompt-cache", promptCacheKey)
}

func replaceCodexIdentityResponseStringValues(payload []byte, replacer *strings.Replacer) []byte {
	return replaceCodexIdentityResponseStringValuesIf(payload, replacer, nil)
}

func replaceCodexIdentityResponseLogStringValues(payload []byte, replacer *strings.Replacer) []byte {
	return replaceCodexIdentityResponseStringValuesIf(payload, replacer, codexIdentityResponseLogField)
}

func replaceCodexIdentityResponseStringValuesIf(payload []byte, replacer *strings.Replacer, replaceField func(string) bool) []byte {
	if len(payload) == 0 || replacer == nil {
		return payload
	}
	if !codexIdentityResponsePayloadIsStructured(payload) {
		if replaceField != nil {
			trimmed := bytes.TrimSpace(payload)
			if bytes.HasPrefix(trimmed, []byte("event:")) ||
				bytes.HasPrefix(trimmed, []byte("id:")) ||
				bytes.HasPrefix(trimmed, []byte("retry:")) {
				return payload
			}
		}
		return []byte(replacer.Replace(string(payload)))
	}

	var updated []byte
	last := 0
	field := ""
	for start := 0; start < len(payload); {
		if payload[start] != '"' {
			start++
			continue
		}

		end, ok := codexIdentityJSONStringEnd(payload, start)
		if !ok {
			break
		}
		if codexIdentityJSONStringIsObjectKey(payload, end) {
			if replaceField != nil {
				_ = json.Unmarshal(payload[start:end+1], &field)
			}
			start = end + 1
			continue
		}
		valueField := field
		field = ""
		if replaceField != nil && !replaceField(valueField) {
			start = end + 1
			continue
		}

		var value string
		if err := json.Unmarshal(payload[start:end+1], &value); err != nil {
			start = end + 1
			continue
		}
		replaced := replacer.Replace(value)
		if replaced == value {
			start = end + 1
			continue
		}
		replacement, _ := json.Marshal(replaced)
		if updated == nil {
			updated = make([]byte, 0, len(payload)+len(replacement)-(end-start+1))
		}
		updated = append(updated, payload[last:start]...)
		updated = append(updated, replacement...)
		last = end + 1
		start = end + 1
	}
	if updated != nil {
		return append(updated, payload[last:]...)
	}
	return payload
}

func codexIdentityResponseLogField(field string) bool {
	field = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(field), "-", "_"))
	switch field {
	case "error", "message",
		"prompt_cache_key",
		"installation_id", "x_codex_installation_id",
		"session_id",
		"thread_id", "parent_thread_id", "forked_from_thread_id", "x_client_request_id",
		"turn_id", "parent_turn_id",
		"window_id", "x_codex_window_id", "x_codex_parent_thread_id", "x_codex_turn_metadata":
		return true
	default:
		return false
	}
}

func codexIdentityJSONStringEnd(payload []byte, start int) (int, bool) {
	escaped := false
	for i := start + 1; i < len(payload); i++ {
		if escaped {
			escaped = false
			continue
		}
		switch payload[i] {
		case '\\':
			escaped = true
		case '"':
			return i, true
		}
	}
	return 0, false
}

func codexIdentityJSONStringIsObjectKey(payload []byte, end int) bool {
	for i := end + 1; i < len(payload); i++ {
		switch payload[i] {
		case ' ', '\t', '\r', '\n':
			continue
		default:
			return payload[i] == ':'
		}
	}
	return false
}

func codexIdentityResponsePayloadIsStructured(payload []byte) bool {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return false
	}
	switch trimmed[0] {
	case '{', '[', '"':
		return true
	}
	return bytes.HasPrefix(trimmed, dataTag) || bytes.Contains(payload, []byte("\ndata:"))
}

func codexIdentityConfuseEnabled(cfg *config.Config) bool {
	if cfg == nil || !cfg.Codex.IdentityConfuse {
		return false
	}
	strategy := strings.ToLower(strings.TrimSpace(cfg.Routing.Strategy))
	return cfg.Routing.SessionAffinity || strategy == "fill-first" || strategy == "fillfirst" || strategy == "ff"
}

func codexIdentityConfuseUUID(authID string, kind string, value string) string {
	name := strings.Join([]string{"cli-proxy-api", "codex", "identity-confuse", kind, strings.TrimSpace(authID), strings.TrimSpace(value)}, ":")
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(name)).String()
}

func applyCodexHeaders(r *http.Request, auth *cliproxyauth.Auth, token string, stream bool, cfg *config.Config) {
	applyCodexHeadersForRequest(r, auth, token, stream, cfg, nil)
}

func applyCodexHeadersForRequest(r *http.Request, auth *cliproxyauth.Auth, token string, stream bool, cfg *config.Config, requestHeaders http.Header) {
	if requestHeaders == nil {
		if ginCtx, ok := r.Context().Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
			requestHeaders = ginCtx.Request.Header
		}
	}
	applyCodexHeadersFromSources(r, auth, token, stream, cfg, requestHeaders)
}

// applyModelHeaderOverrides forces models.json config.override_header onto upstream headers.
func applyModelHeaderOverrides(headers http.Header, modelName string) {
	if headers == nil {
		return
	}
	overrides := registry.ModelOverrideHeaders(modelName)
	if len(overrides) == 0 {
		return
	}
	for key, value := range overrides {
		headers.Set(key, value)
	}
	if strings.Contains(headers.Get("User-Agent"), "Mac OS") && codexSessionHeaderValue(headers) == "" {
		headers.Set("Session_id", uuid.NewString())
	}
}

// applyCodexDirectImageHeaders sets Codex upstream headers for direct /images/* calls.
// Downstream client User-Agent values are not forwarded to reduce Cloudflare 1010 blocks.
func applyCodexDirectImageHeaders(r *http.Request, auth *cliproxyauth.Auth, token string, stream bool, cfg *config.Config) {
	var ginHeaders http.Header
	if ginCtx, ok := r.Context().Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
		ginHeaders = ginCtx.Request.Header.Clone()
		ginHeaders.Del("User-Agent")
	}
	applyCodexHeadersFromSources(r, auth, token, stream, cfg, ginHeaders)
}

func applyCodexHeadersFromSources(r *http.Request, auth *cliproxyauth.Auth, token string, stream bool, cfg *config.Config, ginHeaders http.Header) {
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+token)

	if ginHeaders != nil && ginHeaders.Get("X-Codex-Beta-Features") != "" {
		r.Header.Set("X-Codex-Beta-Features", ginHeaders.Get("X-Codex-Beta-Features"))
	}
	misc.EnsureHeader(r.Header, ginHeaders, codexTurnStateHeader, "")
	misc.EnsureHeader(r.Header, ginHeaders, codexResponsesLiteHeader, "")
	misc.EnsureHeader(r.Header, ginHeaders, "Version", "")
	misc.EnsureHeader(r.Header, ginHeaders, "X-Codex-Turn-Metadata", "")
	misc.EnsureHeader(r.Header, ginHeaders, "X-Codex-Window-Id", "")
	misc.EnsureHeader(r.Header, ginHeaders, "X-Codex-Parent-Thread-Id", "")
	misc.EnsureHeader(r.Header, ginHeaders, "X-Codex-Installation-Id", "")
	misc.EnsureHeader(r.Header, ginHeaders, "X-OpenAI-Subagent", "")
	misc.EnsureHeader(r.Header, ginHeaders, "X-Client-Request-Id", "")
	misc.EnsureHeader(r.Header, ginHeaders, "Thread-Id", "")
	cfgUserAgent, _ := codexHeaderDefaults(cfg, auth)
	ensureHeaderWithConfigPrecedence(r.Header, ginHeaders, "User-Agent", cfgUserAgent, codexUserAgent)

	if sessionID := codexSessionHeaderValue(ginHeaders); sessionID != "" {
		setCodexSessionHeader(r.Header, "Session-Id", sessionID)
	} else if strings.Contains(r.Header.Get("User-Agent"), "Mac OS") {
		misc.EnsureHeader(r.Header, ginHeaders, "Session_id", uuid.NewString())
	}

	if stream {
		r.Header.Set("Accept", "text/event-stream")
	} else {
		r.Header.Set("Accept", "application/json")
	}
	r.Header.Set("Connection", "Keep-Alive")

	isAPIKey := false
	if auth != nil && auth.Attributes != nil {
		if v := strings.TrimSpace(auth.Attributes["api_key"]); v != "" {
			isAPIKey = true
		}
	}
	if originator := strings.TrimSpace(ginHeaders.Get("Originator")); originator != "" {
		r.Header.Set("Originator", originator)
	} else if !isAPIKey {
		r.Header.Set("Originator", codexOriginator)
	}
	if !isAPIKey {
		if auth != nil && auth.Metadata != nil {
			if accountID, ok := auth.Metadata["account_id"].(string); ok {
				r.Header.Set("Chatgpt-Account-Id", accountID)
			}
		}
	}
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(r, attrs)
	applyCodexCloakingHeaders(r.Header, cfg)
}

func applyCodexCloakingHeaders(headers http.Header, cfg *config.Config) {
	if headers == nil || cfg == nil || cfg.Codex.DisableCodexCloaking {
		return
	}
	headers.Set("User-Agent", codexUserAgent)
	headers.Set("Originator", codexOriginator)
}

func applyCodexClientMetadataCompatibilityHeaders(headers http.Header, body []byte) {
	if headers == nil || len(body) == 0 {
		return
	}
	clientMetadata := gjson.GetBytes(body, "client_metadata")
	if !clientMetadata.IsObject() {
		return
	}
	setMetadataHeader := func(metadataKey string, headerName string) {
		if value := strings.TrimSpace(clientMetadata.Get(metadataKey).String()); value != "" {
			headers.Set(headerName, value)
		}
	}
	setMetadataHeader("x-codex-turn-metadata", "X-Codex-Turn-Metadata")
	setMetadataHeader("x-codex-window-id", "X-Codex-Window-Id")
	setMetadataHeader("x-codex-parent-thread-id", "X-Codex-Parent-Thread-Id")
	setMetadataHeader("x-openai-subagent", "X-OpenAI-Subagent")
	if sessionID := strings.TrimSpace(clientMetadata.Get("session_id").String()); sessionID != "" {
		setCodexSessionHeader(headers, "Session-Id", sessionID)
	}
	if threadID := strings.TrimSpace(clientMetadata.Get("thread_id").String()); threadID != "" {
		headers.Set("Thread-Id", threadID)
		headers.Set("X-Client-Request-Id", threadID)
	}
}

func applyCodexHTTPClientMetadataHeaders(headers http.Header, body []byte) {
	applyCodexClientMetadataCompatibilityHeaders(headers, body)
	clientMetadata := gjson.GetBytes(body, "client_metadata")
	if !clientMetadata.IsObject() {
		return
	}
	if turnState := strings.TrimSpace(clientMetadata.Get("x-codex-turn-state").String()); turnState != "" {
		headers.Set(codexTurnStateHeader, turnState)
	}
	if strings.EqualFold(strings.TrimSpace(clientMetadata.Get(codexResponsesLiteMetadataKey).String()), "true") {
		headers.Set(codexResponsesLiteHeader, "true")
	}
}

func finalizeCodexHTTPHeaders(headers http.Header, body []byte, modelName string, identityState *codexIdentityConfuseState) {
	applyCodexHTTPClientMetadataHeaders(headers, body)
	applyModelHeaderOverrides(headers, modelName)
	applyCodexIdentityConfuseHeaders(headers, identityState)
}

func normalizeCodexInstructions(body []byte) []byte {
	instructions := gjson.GetBytes(body, "instructions")
	if !instructions.Exists() || instructions.Type == gjson.Null {
		body, _ = sjson.SetBytes(body, "instructions", "")
	}
	return body
}

func normalizeCodexResponsesLiteRequest(body []byte, headers http.Header) ([]byte, bool) {
	responsesLite := isCodexResponsesLiteRequest(body, headers)
	if !responsesLite {
		return body, false
	}
	body, _ = sjson.DeleteBytes(body, "instructions")
	body, _ = sjson.SetBytes(body, "parallel_tool_calls", false)
	return body, true
}

var imageGenToolJSON = []byte(`{"type":"image_generation","output_format":"png"}`)
var imageGenToolArrayJSON = []byte(`[{"type":"image_generation","output_format":"png"}]`)

func isCodexFreePlanAuth(auth *cliproxyauth.Auth) bool {
	if auth == nil || auth.Attributes == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(auth.Attributes["plan_type"]), "free")
}

func isImageGenerationFunctionTool(tool gjson.Result) bool {
	switch tool.Get("type").String() {
	case "function":
		return tool.Get("name").String() == "image_gen.imagegen"
	case "namespace":
		if tool.Get("name").String() != "image_gen" {
			return false
		}
		tools := tool.Get("tools")
		if !tools.IsArray() {
			return false
		}
		for _, nestedTool := range tools.Array() {
			if nestedTool.Get("type").String() == "function" && nestedTool.Get("name").String() == "imagegen" {
				return true
			}
		}
	}
	return false
}

func isCodexResponsesLiteRequest(body []byte, headers http.Header) bool {
	if strings.EqualFold(strings.TrimSpace(headers.Get(codexResponsesLiteHeader)), "true") {
		return true
	}
	// Codex Desktop mirrors websocket-only request headers into client_metadata.
	value := gjson.GetBytes(body, codexResponsesLiteMetadata)
	if !value.Exists() {
		return false
	}
	return value.Type == gjson.True || value.Type == gjson.String && strings.EqualFold(strings.TrimSpace(value.String()), "true")
}

func ensureImageGenerationTool(body []byte, baseModel string, auth *cliproxyauth.Auth, headers http.Header) []byte {
	if isCodexResponsesLiteRequest(body, headers) {
		return body
	}
	if strings.HasSuffix(baseModel, "spark") {
		return body
	}
	if isCodexFreePlanAuth(auth) {
		return body
	}

	tools := gjson.GetBytes(body, "tools")
	if !tools.Exists() || !tools.IsArray() {
		body, _ = sjson.SetRawBytes(body, "tools", imageGenToolArrayJSON)
		return body
	}
	for _, t := range tools.Array() {
		if t.Get("type").String() == "image_generation" || isImageGenerationFunctionTool(t) {
			return body
		}
	}
	body, _ = sjson.SetRawBytes(body, "tools.-1", imageGenToolJSON)
	return body
}

func normalizeCodexParallelToolCalls(body []byte, headers http.Header) []byte {
	if isCodexResponsesLiteRequest(body, headers) {
		body = helps.SetBoolIfDifferent(body, "parallel_tool_calls", false)
		return body
	}
	return normalizeCodexParallelToolCallsForTools(body)
}

func normalizeCodexParallelToolCallsForTools(body []byte) []byte {
	if !gjson.GetBytes(body, "parallel_tool_calls").Exists() {
		return body
	}

	tools := gjson.GetBytes(body, "tools")
	hasTools := tools.Exists() && tools.IsArray() && len(tools.Array()) > 0
	if hasTools {
		return body
	}

	body, _ = sjson.DeleteBytes(body, "parallel_tool_calls")
	return body
}

func publishCodexImageToolUsage(ctx context.Context, reporter *helps.UsageReporter, body []byte, completedData []byte) {
	detail, ok := helps.ParseCodexImageToolUsage(completedData)
	if !ok {
		return
	}
	reporter.EnsurePublished(ctx)
	reporter.PublishAdditionalModel(ctx, codexImageGenerationToolModel(body), detail)
}

func codexImageGenerationToolModel(body []byte) string {
	tools := gjson.GetBytes(body, "tools")
	if tools.IsArray() {
		for _, tool := range tools.Array() {
			if tool.Get("type").String() != "image_generation" {
				continue
			}
			if model := strings.TrimSpace(tool.Get("model").String()); model != "" {
				return model
			}
			break
		}
	}
	return codexDefaultImageToolModel
}
