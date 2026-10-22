package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/MoeForge/moego/ojson"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/thinking"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	log "github.com/sirupsen/logrus"
)

// codexDuplexConnectionError keeps a connection failure from cooling the shared
// credential. The socket still fails visibly; no reconnect or replay is attempted.
// Auth/quota failures from the initial handshake retain the normal policy.
type codexDuplexConnectionError struct{ cause error }

func (e *codexDuplexConnectionError) Error() string         { return e.cause.Error() }
func (e *codexDuplexConnectionError) Unwrap() error         { return e.cause }
func (e *codexDuplexConnectionError) IsRequestScoped() bool { return true }

// streamCodexDuplex owns the already authenticated socket until downstream
// disconnect. A response terminal event is not a connection terminal event:
// accepted steering may produce a successor or wait for client tool results.
// There is no redial, credential selection, local acknowledgement, or replay
// in this path. Only the upstream can take ownership of a steering submission.
func (e *CodexWebsocketsExecutor) streamCodexDuplex(
	ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options,
	sess *codexWebsocketSession, conn *websocket.Conn, readCh chan codexWebsocketRead,
	input <-chan cliproxyexecutor.WebsocketInput, initial *codexWebsocketPrepared,
	initialReporter *helps.UsageReporter, headers http.Header, unlock func(),
) *cliproxyexecutor.StreamResult {
	streamCtx, cancel := context.WithCancel(ctx)
	// Duplex successors bypass dispatch, including automatic steering responses.
	// Reader usage reporting uses ctx so post-cancel failure publishing still has
	// request metadata; writer-side local HTTP calls derive from streamCtx so an
	// upstream websocket disconnect cancels in-flight HTTP compaction immediately.
	followupCtx := ctx
	writerFollowupCtx := streamCtx
	if selection := coreusage.HomeSelectionFromContext(ctx); selection != nil {
		reusedSelection := *selection
		reusedSelection.Reused = true
		followupCtx = coreusage.WithHomeSelection(ctx, reusedSelection)
		writerFollowupCtx = coreusage.WithHomeSelection(streamCtx, reusedSelection)
	}
	out := make(chan cliproxyexecutor.StreamChunk)
	// This first frame was successfully written by ExecuteStream before handoff.
	log.Infof("codex websockets: request forwarded session=%s auth=%s url=%s event=response.create", sess.sessionID, auth.ID, initial.wsURL)
	type writerFailure struct {
		err                    error
		preserveClassification bool
	}
	writeErrors := make(chan writerFailure, 1)
	// Explicit creates have their own request settings. Automatic successors
	// inherit the preceding response settings, just as they do upstream.
	var metadataMu sync.Mutex
	pending := []*codexWebsocketPrepared{initial}
	// Serialize explicit creates against steering continuations. A response.created
	// alone does not identify whether upstream created it automatically.
	var unacknowledgedSteers []string
	acceptedSteers := make(map[string]string)
	current := initial
	responseID := ""
	latestTurnResponseID := ""
	responseSettings := make(map[string]*codexWebsocketPrepared)
	steeringSettings := make(map[string]*codexWebsocketPrepared)
	var responseOrder []string
	firstResponse := true
	responseActive := false
	terminalDeliveryPending := false
	localCompactionItems := make(map[string]string)
	var localCompactionOrder []string
	storeResponseSettingsLocked := func(id string, snapshot *codexWebsocketPrepared) {
		if id == "" || snapshot == nil {
			return
		}
		responseSettings[id] = snapshot
		responseOrder = append(responseOrder, id)
		if len(responseOrder) > 16 {
			delete(responseSettings, responseOrder[0])
			responseOrder = responseOrder[1:]
		}
	}
	releaseSteeringSettings := func(parent string) {
		for _, target := range unacknowledgedSteers {
			if target == parent {
				return
			}
		}
		for _, target := range acceptedSteers {
			if target == parent {
				return
			}
		}
		delete(steeringSettings, parent)
	}
	waitingParent := ""
	automaticActive := false
	stateChanged := make(chan struct{}, 1)
	wakeWriter := func() {
		select {
		case stateChanged <- struct{}{}:
		default:
		}
	}
	waitFor := func(ready func() bool) bool {
		for {
			metadataMu.Lock()
			ok := ready()
			metadataMu.Unlock()
			if ok {
				return streamCtx.Err() == nil
			}
			select {
			case <-stateChanged:
			case <-streamCtx.Done():
				return false
			}
		}
	}
	// Do not consume follow-ups until bootstrap succeeds. A rejected initial
	// request may retry on another credential using the same input channel.
	inputReady := make(chan struct{})
	writerDone := make(chan struct{})
	var pendingCreates [][]byte
	readyForCreate := func() bool {
		metadataMu.Lock()
		defer metadataMu.Unlock()
		if len(unacknowledgedSteers) > 0 || automaticActive {
			return false
		}
		for _, parent := range acceptedSteers {
			if parent != waitingParent {
				return false
			}
		}
		return true
	}
	go func() {
		defer close(writerDone)
		failWithClassification := func(err error, preserveClassification bool) {
			select {
			case writeErrors <- writerFailure{err: err, preserveClassification: preserveClassification}:
			default:
			}
			cancel()
		}
		fail := func(err error) { failWithClassification(err, false) }
		emitDuplexError := func(status int, errType, code, param, message, eventID string) bool {
			errBody := map[string]string{
				"type":    errType,
				"message": message,
			}
			if code != "" {
				errBody["code"] = code
			}
			if param != "" {
				errBody["param"] = param
			}
			if eventID != "" {
				errBody["event_id"] = eventID
			}
			payload, _ := json.Marshal(map[string]any{
				"type":   "error",
				"status": status,
				"error":  errBody,
			})
			// Reader cleanup joins this writer before closing out. Cancellation must
			// also release a local error blocked behind a disconnected downstream.
			select {
			case out <- cliproxyexecutor.StreamChunk{Payload: payload}:
				return true
			case <-streamCtx.Done():
				return false
			}
		}
		reject := func(message string) bool {
			return emitDuplexError(http.StatusBadRequest, "invalid_request_error", "", "", message, "")
		}
		rejectPreviousResponseNotFound := func(eventID string) bool {
			return emitDuplexError(
				http.StatusConflict,
				"invalid_request_error",
				"previous_response_not_found",
				"previous_response_id",
				"Previous response is not available on this websocket; resend the full conversation input without previous_response_id",
				eventID,
			)
		}
		initialModel := ""
		if initial != nil {
			initialModel = strings.TrimSpace(ojson.GetBytesNoCopy(initial.originalPayload, "model").String())
		}
		boundModel := helps.PayloadRequestedModel(opts, initialModel)
		if boundModel == "" {
			boundModel = req.Model
		}
		authorizePayload := func(payload []byte, forceBoundModel bool) (allowed bool, keepOpen bool) {
			authPayload := payload
			explicitModel := strings.TrimSpace(ojson.GetBytesNoCopy(authPayload, "model").String())
			if boundModel != "" && json.Valid(authPayload) && (forceBoundModel || explicitModel == "" || explicitModel == req.Model) {
				if withModel, errSet := ojson.SetBytes(bytes.Clone(authPayload), "model", boundModel); errSet == nil {
					authPayload = withModel
				}
			}
			errPayload, errAuth := cliproxyexecutor.AuthorizeWebsocketTurn(streamCtx, authPayload)
			if errAuth != nil {
				fail(errAuth)
				return false, false
			}
			if len(errPayload) > 0 {
				select {
				case out <- cliproxyexecutor.StreamChunk{Payload: errPayload}:
					return false, true
				case <-streamCtx.Done():
					return false, false
				}
			}
			return true, true
		}
		finalBeforeHeaders := func(current, intercepted http.Header, clear []string) http.Header {
			if intercepted != nil {
				if len(intercepted) == 0 {
					return nil
				}
				return intercepted.Clone()
			}
			if len(current) == 0 {
				return nil
			}
			out := current.Clone()
			for _, key := range clear {
				out.Del(key)
			}
			if len(out) == 0 {
				return nil
			}
			return out
		}
		mergeAfterHeaders := func(current, updates http.Header, clear []string) http.Header {
			if updates == nil && len(clear) == 0 {
				if len(current) == 0 {
					return nil
				}
				return current.Clone()
			}
			out := http.Header{}
			if len(current) > 0 {
				out = current.Clone()
			}
			for _, key := range clear {
				out.Del(key)
			}
			for key, values := range updates {
				out.Del(key)
				for _, value := range values {
					out.Add(key, value)
				}
			}
			if len(out) == 0 {
				return nil
			}
			return out
		}
		rejectTerminated := func(resp cliproxyexecutor.RequestAfterAuthInterceptResponse) bool {
			status := resp.StatusCode
			if status < 400 || status > 599 {
				status = http.StatusForbidden
			}
			msg := "request terminated by plugin"
			if len(resp.ResponseBody) > 0 {
				if parsed := strings.TrimSpace(ojson.GetBytesNoCopy(resp.ResponseBody, "error.message").String()); parsed != "" {
					msg = parsed
				} else if trimmed := strings.TrimSpace(string(resp.ResponseBody)); trimmed != "" {
					msg = trimmed
				}
			}
			payload, _ := json.Marshal(map[string]any{
				"type": "error", "status": status,
				"error": map[string]string{"type": "invalid_request_error", "message": msg},
			})
			select {
			case out <- cliproxyexecutor.StreamChunk{Payload: payload}:
				return true
			case <-streamCtx.Done():
				return false
			}
		}
		interceptTurnPayload := func(payload []byte, baseHeaders http.Header) ([]byte, http.Header, bool, bool) {
			if opts.RequestBeforeAuthInterceptor == nil && opts.RequestAfterAuthInterceptor == nil {
				return payload, baseHeaders, true, true
			}
			curHeaders := baseHeaders
			if curHeaders != nil {
				curHeaders = curHeaders.Clone()
			}
			interceptReq := cliproxyexecutor.RequestAfterAuthInterceptRequest{
				SourceFormat:   opts.SourceFormat,
				Model:          req.Model,
				RequestedModel: boundModel,
				Stream:         true,
				Headers:        curHeaders,
				Body:           payload,
				Metadata:       opts.Metadata,
			}
			if opts.RequestBeforeAuthInterceptor != nil {
				resp := opts.RequestBeforeAuthInterceptor(streamCtx, interceptReq)
				curHeaders = finalBeforeHeaders(curHeaders, resp.Headers, resp.ClearHeaders)
				if len(resp.Body) > 0 {
					payload = resp.Body
					interceptReq.Body = payload
				}
				interceptReq.Headers = curHeaders
				if resp.Terminate {
					return payload, curHeaders, false, rejectTerminated(resp)
				}
			}
			if opts.RequestAfterAuthInterceptor != nil {
				interceptReq.ToFormat = sdktranslator.FromString("codex")
				resp := opts.RequestAfterAuthInterceptor(streamCtx, interceptReq)
				curHeaders = mergeAfterHeaders(curHeaders, resp.Headers, resp.ClearHeaders)
				if len(resp.Body) > 0 {
					payload = resp.Body
				}
				if resp.Terminate {
					return payload, curHeaders, false, rejectTerminated(resp)
				}
			}
			return payload, curHeaders, true, true
		}
		processCreatePayload := func(payload []byte) bool {
			if allowed, keepOpen := authorizePayload(payload, false); !allowed {
				return keepOpen
			}
			eventID := strings.Clone(strings.TrimSpace(ojson.GetBytesNoCopy(payload, "event_id").String()))
			metadataMu.Lock()
			isAppend := ojson.GetBytesNoCopy(payload, "type").String() == "response.append"
			prevID := strings.TrimSpace(ojson.GetBytesNoCopy(payload, "previous_response_id").String())
			if isAppend && prevID == "" && latestTurnResponseID != "" {
				prevID = latestTurnResponseID
				payload, _ = ojson.SetBytes(payload, "previous_response_id", prevID)
			}
			localCompactionItem, hasLocalCompactionParent := localCompactionItems[prevID]
			wrongParent := len(acceptedSteers) > 0 && prevID != waitingParent
			metadataMu.Unlock()
			if wrongParent {
				if !reject("response.create must continue the response waiting for required input") {
					return false
				}
				return true
			}
			if prevID != "" && !hasLocalCompactionParent && helps.RecognizedResponsesCompactionResponseID(prevID) {
				return rejectPreviousResponseNotFound(eventID)
			}
			model := strings.TrimSpace(ojson.GetBytesNoCopy(payload, "model").String())
			if model != "" && model != req.Model && model != boundModel {
				fail(cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError())
				return false
			}
			var turnHeaders http.Header
			var allowed, keepOpen bool
			payload, turnHeaders, allowed, keepOpen = interceptTurnPayload(payload, opts.Headers)
			if !allowed {
				return keepOpen
			}
			model = req.Model
			if model == "" {
				model = boundModel
			}
			createDoc := ojson.NewDoc(payload)
			_ = createDoc.Set("model", model)
			if (isAppend || hasLocalCompactionParent) && !createDoc.Get("instructions").Exists() {
				metadataMu.Lock()
				parentSettings := responseSettings[prevID]
				metadataMu.Unlock()
				targetSettings := parentSettings
				if targetSettings == nil {
					targetSettings = initial
				}
				var inst ojson.Result
				if targetSettings != nil {
					inst = ojson.GetBytesNoCopy(targetSettings.clientBody, "instructions")
					if !inst.Exists() {
						inst = ojson.GetBytesNoCopy(targetSettings.originalPayload, "instructions")
					}
				}
				if !inst.Exists() && initial != nil {
					inst = ojson.GetBytesNoCopy(initial.clientBody, "instructions")
					if !inst.Exists() {
						inst = ojson.GetBytesNoCopy(initial.originalPayload, "instructions")
					}
				}
				if inst.Exists() {
					_ = createDoc.SetRawBytes("instructions", []byte(inst.Raw))
				}
			}
			if hasLocalCompactionParent {
				existingInput := createDoc.Get("input")
				rebuiltItems := make([]string, 0, 2)
				rebuiltItems = append(rebuiltItems, localCompactionItem)
				switch {
				case existingInput.IsArray():
					for _, item := range existingInput.Array() {
						rebuiltItems = append(rebuiltItems, item.Raw)
					}
				case existingInput.Type == ojson.String:
					message := ojson.NewDocString(`{"type":"message","role":"user"}`)
					_ = message.Set("content", existingInput.String())
					rebuiltItems = append(rebuiltItems, message.String())
				case existingInput.Exists():
					return emitDuplexError(http.StatusBadRequest, "invalid_request_error", "", "input", "input must be a string or an array", eventID)
				}
				_ = createDoc.SetRaw("input", "["+strings.Join(rebuiltItems, ",")+"]")
				_ = createDoc.Delete("previous_response_id")
				_ = createDoc.Set("type", "response.create")
				isAppend = false
				prevID = ""
			}
			payload = createDoc.Bytes()
			nextReq, nextOpts := req, opts
			// Dispatch already resolved aliases to the bound upstream execution model.
			// The client model above is only used for authorization and consistency.
			nextReq.Model = req.Model
			nextReq.Payload = payload
			nextOpts.OriginalRequest = payload
			nextOpts.Headers = turnHeaders
			turnBaseModel := thinking.ParseSuffix(nextReq.Model).ModelName
			if turnIsCompat := e.resolveCodexModelIsCompat(auth, nextReq, turnBaseModel); e.shouldCompactCompatResponsesTrigger(turnIsCompat, nextReq, nextOpts) {
				if _, errExpand := e.expandCodexResponsesCompaction(auth, &nextReq, &nextOpts, turnBaseModel); errExpand != nil {
					return emitDuplexError(http.StatusBadRequest, "invalid_request_error", "", "", errExpand.Error(), eventID)
				}
				if prevID != "" || isAppend {
					return rejectPreviousResponseNotFound(eventID)
				}
				if !helps.HasResponsesCompactionConversationInput(nextReq.Payload) {
					return emitDuplexError(http.StatusBadRequest, "invalid_request_error", "", "", "local summary compaction requires non-empty conversation input", eventID)
				}
				if !waitFor(func() bool {
					ready := !firstResponse && !responseActive && !terminalDeliveryPending && len(pending) == 0 && len(unacknowledgedSteers) == 0 && !automaticActive
					if e.duplexStateObserver != nil {
						e.duplexStateObserver("compaction_wait", ready)
					}
					return ready
				}) {
					return false
				}
				if !cliproxyexecutor.WebsocketAuthEnabled(streamCtx, auth.ID) {
					fail(fmt.Errorf("websocket credential is no longer enabled"))
					return false
				}
				// Build business settings snapshot from the turn request (before summary
				// prompt preparation strips tools/fields) so subsequent response.append
				// or synthetic-parent continuations inherit this turn's instructions.
				snapshotDoc := ojson.NewDoc([]byte("{}"))
				if reasoning := ojson.GetBytesNoCopy(nextReq.Payload, "reasoning"); reasoning.Exists() {
					_ = snapshotDoc.SetRawBytes("reasoning", []byte(reasoning.Raw))
				}
				if inst := ojson.GetBytesNoCopy(nextReq.Payload, "instructions"); inst.Exists() {
					_ = snapshotDoc.SetRawBytes("instructions", []byte(inst.Raw))
				} else if initial != nil {
					initInst := ojson.GetBytesNoCopy(initial.clientBody, "instructions")
					if !initInst.Exists() {
						initInst = ojson.GetBytesNoCopy(initial.originalPayload, "instructions")
					}
					if initInst.Exists() {
						_ = snapshotDoc.SetRawBytes("instructions", []byte(initInst.Raw))
					}
				}
				if serviceTier := ojson.GetBytesNoCopy(nextReq.Payload, "service_tier"); serviceTier.Exists() {
					_ = snapshotDoc.SetRawBytes("service_tier", []byte(serviceTier.Raw))
				}
				compactionSettings := &codexWebsocketPrepared{
					to:         sdktranslator.FromString("codex"),
					clientBody: snapshotDoc.Bytes(),
				}
				compactionResp, errCompact := e.CodexExecutor.executeCodexResponsesCompaction(writerFollowupCtx, auth, nextReq, nextOpts)
				if errCompact != nil {
					if streamCtx.Err() != nil {
						return false
					}
					var statusHolder interface{ StatusCode() int }
					status := http.StatusBadGateway
					if errors.As(errCompact, &statusHolder) && statusHolder.StatusCode() > 0 {
						status = statusHolder.StatusCode()
					}
					if status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusTooManyRequests {
						failWithClassification(errCompact, true)
						return false
					}
					errType := "server_error"
					if status >= 400 && status < 500 {
						errType = "invalid_request_error"
					}
					return emitDuplexError(status, errType, "", "", errCompact.Error(), eventID)
				}
				chunks, errChunks := helps.BuildResponsesCompactionStreamChunksFromResponse(turnBaseModel, compactionResp.Payload)
				if errChunks != nil {
					return emitDuplexError(http.StatusBadGateway, "server_error", "", "", errChunks.Error(), eventID)
				}
				compactedRespID := ""
				compactedItemRaw := ""
				for i := len(chunks) - 1; i >= 0; i-- {
					for _, line := range bytes.Split(chunks[i], []byte("\n")) {
						trimmedLine := bytes.TrimSpace(line)
						if !bytes.HasPrefix(trimmedLine, dataTag) {
							continue
						}
						eventData := bytes.TrimSpace(trimmedLine[len(dataTag):])
						if ojson.GetBytesNoCopy(eventData, "type").String() == "response.completed" {
							compactedRespID = strings.Clone(strings.TrimSpace(ojson.GetBytesNoCopy(eventData, "response.id").String()))
							compactedItemRaw = strings.Clone(ojson.GetBytesNoCopy(eventData, "response.output.0").Raw)
							break
						}
					}
					if compactedRespID != "" {
						break
					}
				}
				if compactedRespID != "" && compactedItemRaw != "" {
					metadataMu.Lock()
					latestTurnResponseID = compactedRespID
					waitingParent = ""
					storeResponseSettingsLocked(compactedRespID, compactionSettings)
					localCompactionItems[compactedRespID] = compactedItemRaw
					localCompactionOrder = append(localCompactionOrder, compactedRespID)
					if len(localCompactionOrder) > 16 {
						delete(localCompactionItems, localCompactionOrder[0])
						localCompactionOrder = localCompactionOrder[1:]
					}
					metadataMu.Unlock()
				}
				for _, chunk := range chunks {
					select {
					case out <- cliproxyexecutor.StreamChunk{Payload: chunk}:
					case <-streamCtx.Done():
						return false
					}
				}
				return true
			}
			prepared, errPrepare := e.prepareCodexWebsocketStream(streamCtx, auth, nextReq, nextOpts)
			if errPrepare != nil {
				fail(errPrepare)
				return false
			}
			if prepared.wsURL != initial.wsURL {
				fail(cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError())
				return false
			}
			payload = frameCodexWebsocketRequestBody(prepared.clientBody)
			metadataMu.Lock()
			if len(pending) >= 16 {
				metadataMu.Unlock()
				fail(fmt.Errorf("too many outstanding response.create requests"))
				return false
			}
			pending = append(pending, prepared)
			metadataMu.Unlock()
			if prepared.optimizeMultiAgentV2 || prepared.multiAgentV2Conflict {
				sess.setMultiAgentV2Optimized(conn, prepared.optimizeMultiAgentV2 && !prepared.multiAgentV2Conflict)
			}
			if !cliproxyexecutor.WebsocketAuthEnabled(streamCtx, auth.ID) {
				fail(fmt.Errorf("websocket credential is no longer enabled"))
				return false
			}
			helps.RecordAPIWebsocketRequest(streamCtx, e.cfg, helps.UpstreamRequestLog{
				URL: initial.wsURL, Method: "WEBSOCKET", Body: payload, Provider: e.Identifier(), AuthID: auth.ID,
			})
			if errWrite := writeCodexWebsocketMessage(sess, conn, payload); errWrite != nil {
				fail(mapCodexWebsocketWriteError(sess, conn, errWrite))
				return false
			}
			log.Infof("codex websockets: request forwarded session=%s auth=%s url=%s event=%s", sess.sessionID, auth.ID, initial.wsURL, ojson.GetBytesNoCopy(payload, "type").String())
			return true
		}
		flushPendingCreates := func() bool {
			for len(pendingCreates) > 0 && readyForCreate() {
				item := pendingCreates[0]
				pendingCreates = pendingCreates[1:]
				if !processCreatePayload(item) {
					return false
				}
			}
			return true
		}
		select {
		case <-streamCtx.Done():
			return
		case <-inputReady:
		}
		for {
			if !flushPendingCreates() {
				return
			}
			select {
			case <-streamCtx.Done():
				return
			case <-stateChanged:
				if !flushPendingCreates() {
					return
				}
				continue
			case message, ok := <-input:
				if !ok {
					fail(context.Canceled)
					return
				}
				if message.Err != nil {
					fail(message.Err)
					return
				}
				if !cliproxyexecutor.WebsocketAuthEnabled(streamCtx, auth.ID) {
					// A fresh client connection can select an enabled credential. Do not send
					// this frame on a disabled account or replay it on a different socket.
					fail(fmt.Errorf("websocket credential is no longer enabled"))
					return
				}
				payload := message.Payload
				if !json.Valid(payload) {
					if !reject("invalid websocket request JSON") {
						return
					}
					continue
				}
				switch ojson.GetBytesNoCopy(payload, "type").String() {
				case "response.steer":
					if allowed, keepOpen := authorizePayload(payload, true); !allowed {
						if !keepOpen {
							return
						}
						continue
					}
					var steerHeaders http.Header
					var allowed, keepOpen bool
					payload, steerHeaders, allowed, keepOpen = interceptTurnPayload(payload, opts.Headers)
					if !allowed {
						if !keepOpen {
							return
						}
						continue
					}
					// Steering carries business input but must not inherit response.create defaults.
					steerReq := req
					steerReq.Payload = payload
					steerOpts := opts
					steerOpts.OriginalRequest = payload
					steerOpts.Headers = steerHeaders
					steerBaseModel := thinking.ParseSuffix(req.Model).ModelName
					steerIsCompat, errExpand := e.expandCodexResponsesCompaction(auth, &steerReq, &steerOpts, steerBaseModel)
					if errExpand != nil {
						fail(statusErr{code: http.StatusBadRequest, msg: errExpand.Error()})
						return
					}
					if steerIsCompat && helps.HasResponsesCompactionTrigger(steerReq.Payload) {
						steerEventID := strings.Clone(strings.TrimSpace(ojson.GetBytesNoCopy(steerReq.Payload, "event_id").String()))
						if !emitDuplexError(http.StatusBadRequest, "invalid_request_error", "", "", "compaction_trigger is not supported in response.steer", steerEventID) {
							return
						}
						continue
					}
					steerDoc := ojson.NewDoc(steerReq.Payload)
					if codexUseSIWCForRequest(ctx, auth, steerHeaders) {
						thinking.StripConfigurationUpdatesDoc(steerDoc)
					}
					steerCtx := helps.WithPayloadFinalizer(streamCtx, helps.NewPayloadFinalizer(e.cfg, "codex-websockets", thinking.ParseSuffix(req.Model).ModelName, "codex", "", steerReq.Payload, steerReq, steerOpts))
					helps.FinalizePayloadDoc(steerCtx, steerDoc)
					_ = steerDoc.Set("type", "response.steer")
					parent := strings.Clone(steerDoc.Get("previous_response_id").String())
					payload = steerDoc.Bytes()
					metadataMu.Lock()
					settings := responseSettings[parent]
					metadataMu.Unlock()
					if settings == nil {
						if !waitFor(func() bool { return len(pending) == 0 }) {
							return
						}
						metadataMu.Lock()
						settings = responseSettings[parent]
						metadataMu.Unlock()
					}
					metadataMu.Lock()
					unacknowledgedSteers = append(unacknowledgedSteers, parent)
					if settings != nil {
						steeringSettings[parent] = settings
					}
					metadataMu.Unlock()
					// Control frames bypass response.create translations and built-in defaults.
					// Except for SIWC configuration_update stripping, unknown fields and unsupported
					// input are left to upstream validation.
					if !cliproxyexecutor.WebsocketAuthEnabled(streamCtx, auth.ID) {
						fail(fmt.Errorf("websocket credential is no longer enabled"))
						return
					}
					helps.RecordAPIWebsocketRequest(streamCtx, e.cfg, helps.UpstreamRequestLog{
						URL: initial.wsURL, Method: "WEBSOCKET", Body: payload, Provider: e.Identifier(), AuthID: auth.ID,
					})
					if errWrite := writeCodexWebsocketMessage(sess, conn, payload); errWrite != nil {
						fail(mapCodexWebsocketWriteError(sess, conn, errWrite))
						return
					}
					log.Infof("codex websockets: request forwarded session=%s auth=%s url=%s event=response.steer", sess.sessionID, auth.ID, initial.wsURL)
					continue

				case "response.create", "response.append":
					if len(pendingCreates) == 0 && readyForCreate() {
						if !processCreatePayload(payload) {
							return
						}
					} else {
						if len(pendingCreates) >= 16 {
							fail(fmt.Errorf("too many outstanding response.create requests"))
							return
						}
						pendingCreates = append(pendingCreates, payload)
					}
					continue

				default:
					if !reject(fmt.Sprintf("unsupported websocket request type: %s", ojson.GetBytesNoCopy(payload, "type").String())) {
						return
					}
					continue
				}
			}
		}
	}()
	go func() {
		defer close(out)
		defer func() {
			cancel()
			// Close releases a writer blocked in the network, then join it before
			// releasing the execution session. No goroutine outlives its socket.
			e.invalidateUpstreamConnWithoutDisconnectNotify(sess, conn, "duplex_closed", nil)
			<-writerDone
			sess.clearActive(conn, readCh)
			unlock()
		}()
		send := func(chunk cliproxyexecutor.StreamChunk) bool {
			select {
			case out <- chunk:
				return true
			case <-ctx.Done():
				return false
			}
		}
		reporter := initialReporter
		outputItems := make(map[int64][]byte)
		var outputFallback [][]byte
		for {
			kind, payload, errRead := readCodexWebsocketMessage(streamCtx, sess, conn, readCh)
			if errRead != nil {
				preserveClassification := false
				select {
				case failure := <-writeErrors:
					errRead = failure.err
					preserveClassification = failure.preserveClassification
				default:
				}
				cancel()
				if ctx.Err() == nil {
					var connectionErr error = &codexDuplexConnectionError{cause: errRead}
					if preserveClassification {
						// HTTP business failures retain credential and cooldown policy.
						connectionErr = errRead
					}
					reporter.PublishFailure(ctx, connectionErr)
					send(cliproxyexecutor.StreamChunk{Err: connectionErr})
				}
				return
			}
			if kind != websocket.TextMessage {
				continue
			}
			payload = bytes.TrimSpace(payload)
			eventType := ojson.GetBytesNoCopy(payload, "type").String()
			metadataMu.Lock()
			establishing := firstResponse && eventType == "response.created"
			metadataMu.Unlock()
			if eventType == "response.created" {
				metadataMu.Lock()
				parent := strings.Clone(ojson.GetBytesNoCopy(payload, "response.previous_response_id").String())
				if parent == "" {
					parent = responseID
				}
				if !firstResponse && len(pending) == 0 {
					settings := steeringSettings[parent]
					if settings == nil {
						settings = responseSettings[parent]
					}
					if settings == nil {
						metadataMu.Unlock()
						connectionErr := &codexDuplexConnectionError{cause: fmt.Errorf("automatic successor has no retained parent settings")}
						reporter.PublishFailure(ctx, connectionErr)
						send(cliproxyexecutor.StreamChunk{Err: connectionErr})
						return
					}
					current = settings
				}
				for id, target := range acceptedSteers {
					if target == parent {
						delete(acceptedSteers, id)
					}
				}
				waitingParent = ""
				automaticActive = !firstResponse && len(pending) == 0
				if len(pending) > 0 {
					current, pending = pending[0], pending[1:]
				}
				responseID = strings.Clone(ojson.GetBytesNoCopy(payload, "response.id").String())
				latestTurnResponseID = responseID
				// Retain response settings, not request history or authorization headers.
				// In-flight steering pins its parent's settings independently of this window.
				snapshot := *current
				snapshot.originalPayload, snapshot.wsHeaders = nil, nil
				snapshotDoc := ojson.NewDoc([]byte("{}"))
				if reasoning := ojson.GetBytesNoCopy(current.clientBody, "reasoning"); reasoning.Exists() {
					_ = snapshotDoc.SetRawBytes("reasoning", []byte(reasoning.Raw))
				}
				if inst := ojson.GetBytesNoCopy(current.clientBody, "instructions"); inst.Exists() {
					_ = snapshotDoc.SetRawBytes("instructions", []byte(inst.Raw))
				}
				if serviceTier := ojson.GetBytesNoCopy(current.clientBody, "service_tier"); serviceTier.Exists() {
					_ = snapshotDoc.SetRawBytes("service_tier", []byte(serviceTier.Raw))
				}
				snapshot.clientBody = snapshotDoc.Bytes()
				storeResponseSettingsLocked(responseID, &snapshot)
				wasFirstResponse := firstResponse
				firstResponse = false
				responseActive = true
				releaseSteeringSettings(parent)
				metadataMu.Unlock()
				wakeWriter()
				if !wasFirstResponse {
					reporter = helps.NewExecutorUsageReporter(followupCtx, e, req.Model, auth)
					reporter.SetTranslatedReasoningEffort(current.clientBody, current.to.String())
					reporter.StartResponseTTFT()
				}
				outputItems = make(map[int64][]byte)
				outputFallback = nil
			}
			eventPrepared, eventReporter := current, reporter
			failureEvent := !firstResponse && (eventType == "response.failed" || eventType == "error")
			ambiguousFailure := false
			if failureEvent {
				failedID := ojson.GetBytes(payload, "response.id").String()
				if failedID == "" {
					failedID = ojson.GetBytes(payload, "response_id").String()
				}
				metadataMu.Lock()
				// Resolve request ownership before usage accounting and response processing.
				// A rejection before response.created owns the oldest pending create;
				// an active response failure retains its current request settings.
				currentFailure := failedID != "" && failedID == responseID
				ambiguousFailure = failedID == "" && ((len(pending) > 0 && responseActive) || len(unacknowledgedSteers) > 0)
				if len(pending) > 0 && !currentFailure && !ambiguousFailure {
					eventPrepared, pending = pending[0], pending[1:]
					eventReporter = helps.NewExecutorUsageReporter(followupCtx, e, req.Model, auth)
					eventReporter.SetTranslatedReasoningEffort(eventPrepared.clientBody, eventPrepared.to.String())
				} else if !ambiguousFailure {
					responseActive = false
					automaticActive = false
				}
				if !ambiguousFailure {
					terminalDeliveryPending = true
				}
				metadataMu.Unlock()
			}
			observeCodexTokenEvent(eventReporter, payload)
			helps.AppendCodexAPIWebsocketResponse(ctx, e.cfg, payload)
			helps.EmitWebSocketResponseEvent(ctx, opts, auth, e.Identifier(), req.Model, payload)
			// Steering acknowledgements, pending notifications and failures are opaque:
			// preserve their IDs, input, sequence numbers and event types byte-for-byte.
			if strings.HasPrefix(eventType, "response.steer.") {
				id := strings.Clone(ojson.GetBytesNoCopy(payload, "steer.id").String())
				parent := strings.Clone(ojson.GetBytesNoCopy(payload, "steer.previous_response_id").String())
				metadataMu.Lock()
				if parent == "" {
					parent = responseID
				}
				consumeSubmission := func() {
					for i, target := range unacknowledgedSteers {
						if target == parent || target == "" {
							unacknowledgedSteers = append(unacknowledgedSteers[:i], unacknowledgedSteers[i+1:]...)
							break
						}
					}
				}
				switch eventType {
				case "response.steer.accepted":
					consumeSubmission()
					acceptedSteers[id] = parent
				case "response.steer.failed":
					if _, accepted := acceptedSteers[id]; accepted {
						delete(acceptedSteers, id)
					} else {
						consumeSubmission()
					}
					releaseSteeringSettings(parent)
				case "response.steer.pending":
					// Tool results may already be waiting in the writer. No automatic
					// successor can start until an explicit continuation supplies them.
					waitingParent = parent
				}
				metadataMu.Unlock()
				wakeWriter()
				if !send(cliproxyexecutor.StreamChunk{Payload: payload}) {
					return
				}
				continue
			}
			if failureEvent {
				var credentialErr error
				if wsErr, ok := parseCodexWebsocketErrorWithCooling(payload, e.modelLevelCooling()); ok {
					credentialErr = wsErr
				} else if streamErr, _, ok := codexTerminalFailureErrWithCooling(payload, e.modelLevelCooling()); ok {
					credentialErr = streamErr
				}
				switch codexWebsocketErrorStatusCode(credentialErr) {
				case http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden, http.StatusTooManyRequests:
					// Account health is independent of which queued request failed.
					// The conductor records the original classification without replaying
					// this already-started stream on another credential.
					eventReporter.PublishFailure(ctx, credentialErr)
					if send(cliproxyexecutor.StreamChunk{Payload: payload}) {
						send(cliproxyexecutor.StreamChunk{Err: credentialErr})
					}
					return
				}
				if ambiguousFailure {
					// Ambiguous request failures end the socket with their original
					// event. Credential failures above retain account-level handling.
					connectionErr := &codexDuplexConnectionError{cause: fmt.Errorf("cannot associate websocket failure with a response or pending create")}
					reporter.PublishFailure(ctx, connectionErr)
					if send(cliproxyexecutor.StreamChunk{Payload: payload}) {
						send(cliproxyexecutor.StreamChunk{Err: connectionErr})
					}
					return
				}
				wakeWriter()
			}
			restoreMultiAgent := !eventPrepared.multiAgentV2Conflict && (eventPrepared.optimizeMultiAgentV2 || sess.isMultiAgentV2Optimized(conn))
			payload = helps.RestoreCodexMultiAgentV2Response(payload, restoreMultiAgent)
			// Parse and invalidate replay for every rejected request, using the
			// metadata that belongs to this event. Only the first rejection can
			// enter conductor bootstrap retry; later failures stay on this socket.
			var terminalErr, replayErr error
			if wsErr, ok := parseCodexWebsocketErrorWithCooling(payload, e.modelLevelCooling()); ok {
				terminalErr = wsErr
				replayErr = clearCodexReasoningReplayOnWebsocketError(ctx, eventPrepared.replayScope, payload)
			} else if streamErr, body, ok := codexTerminalFailureErrWithCooling(payload, e.modelLevelCooling()); ok {
				terminalErr = streamErr
				replayErr = clearCodexReasoningReplayOnInvalidSignature(ctx, eventPrepared.replayScope, streamErr.StatusCode(), body)
			}
			if replayErr != nil {
				helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_clear_error", replayErr)
				eventReporter.PublishFailure(ctx, replayErr)
				send(cliproxyexecutor.StreamChunk{Err: replayErr})
				return
			}
			if terminalErr != nil {
				helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", terminalErr)
				eventReporter.PublishFailure(ctx, terminalErr)
				if firstResponse {
					send(cliproxyexecutor.StreamChunk{Err: terminalErr})
					return
				}
			}
			if eventType == "response.output_item.done" {
				collectCodexOutputItemDone(payload, outputItems, &outputFallback)
			}
			if eventType == "response.completed" || eventType == "response.done" || eventType == "response.incomplete" {
				metadataMu.Lock()
				responseActive = false
				automaticActive = false
				terminalDeliveryPending = true
				metadataMu.Unlock()
				payload = normalizeCodexWebsocketCompletion(payload)
				if !current.preserveNativeOutput {
					payload = patchCodexCompletedOutput(payload, outputItems, outputFallback)
				}
				if eventType != "response.incomplete" {
					cacheCodexReasoningReplayFromCompleted(current.replayScope, payload)
				}
				if detail, ok := helps.ParseCodexUsageWithOutputItems(payload, outputItems, outputFallback); ok {
					reporter.Publish(ctx, detail)
				} else {
					reporter.EnsurePublished(ctx)
				}
				publishCodexImageToolUsageWithOutputItems(ctx, reporter, current.clientBody, payload, outputItems, outputFallback)
				if e.duplexStateObserver != nil {
					e.duplexStateObserver("terminal_pending", false)
				}
			}
			if !send(cliproxyexecutor.StreamChunk{Payload: helps.EnsureResponsesUsageDetails(payload), Incomplete: eventType == "response.incomplete"}) {
				return
			}
			switch eventType {
			case "response.completed", "response.done", "response.incomplete", "response.failed", "error":
				metadataMu.Lock()
				terminalDeliveryPending = false
				metadataMu.Unlock()
				wakeWriter()
			}
			if establishing {
				// Deliver response.created before any locally generated error so the
				// downstream handler also observes successful bootstrap first.
				close(inputReady)
			}
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: headers, Chunks: out}
}
