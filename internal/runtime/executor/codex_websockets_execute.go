package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/MoeForge/moego/ojson"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

func (e *CodexWebsocketsExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = helps.WithCodexCommentaryPolicy(ctx, e.cfg)
	if opts.Alt == "responses/compact" {
		return e.CodexExecutor.executeCompact(ctx, auth, req, opts)
	}

	baseModel := thinking.ParseSuffix(req.Model).ModelName
	isCompat, errExpand := e.expandCodexResponsesCompaction(auth, &req, &opts, baseModel)
	if errExpand != nil {
		return resp, statusErr{code: http.StatusBadRequest, msg: errExpand.Error()}
	}
	if e.shouldCompactCompatResponsesTrigger(isCompat, req, opts) {
		return e.CodexExecutor.executeCodexResponsesCompaction(ctx, auth, req, opts)
	}
	if errPrev := rejectSyntheticCompactionPreviousResponseID(ctx, req.Payload); errPrev != nil {
		return resp, errPrev
	}
	apiKey, baseURL := codexCredsForRequest(ctx, auth, opts.Headers)
	if errSIWC := ensureCodexSIWCCredentialsForRequest(ctx, auth, apiKey, opts.Headers); errSIWC != nil {
		return resp, errSIWC
	}
	useSIWC := codexUseSIWCForRequest(ctx, auth, opts.Headers)
	if baseURL == "" {
		if useSIWC {
			baseURL = defaultOpenAISIWCBaseURL
		} else {
			baseURL = "https://chatgpt.com/backend-api/codex"
		}
	}

	reporter := helps.NewExecutorUsageReporter(ctx, e, baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	nativeRequest := helps.IsNativeCodexRequest(req.Payload, opts)
	to := sdktranslator.FromString("codex")
	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalPayload := originalPayloadSource
	targetModel := resolveResponsesSignatureTargetModel(baseModel, req, opts)
	originalTranslated, body, updatesChanged, err := translateCodexRequestPairWithUpdateIntentContext(ctx, from, to, targetModel, originalPayload, req.Payload, false, isCompat)
	if err != nil {
		return resp, err
	}
	originalTranslated = helps.SetStringIfDifferent(originalTranslated, "model", baseModel)
	doc := ojson.NewDoc(body)
	if shouldPreserveFullCodexOpenAIInclude(ctx, auth, baseURL, opts.Headers) {
		originalTranslated = helps.PreserveCodexOpenAIResponsesInclude(originalTranslated, originalPayload, from)
		helps.PreserveCodexOpenAIResponsesIncludeDoc(doc, req.Payload, from)
	}
	normalizeCodexOutputBudgetDoc(doc)
	helps.SetStringIfDifferentDoc(doc, "model", baseModel)

	if err = helps.ApplyRequestThinkingDoc(doc, req, opts, from.String(), to.String(), e.Identifier(), updatesChanged); err != nil {
		return resp, err
	}

	instructionsResult := ojson.GetBytesNoCopy(req.Payload, "instructions")
	hasExplicitInstructions := instructionsResult.Exists() && strings.TrimSpace(instructionsResult.String()) != ""
	ctx = helps.WithPayloadFinalizer(ctx, helps.NewPayloadFinalizer(e.cfg, "codex-websockets", baseModel, to.String(), "", originalTranslated, req, opts, auth))
	helps.SetStringIfDifferentDoc(doc, "model", baseModel)
	helps.SetBoolIfDifferentDoc(doc, "stream", true)
	_ = doc.Delete("prompt_cache_retention")
	_ = doc.Delete("safety_identifier")
	ensureWebSearchToolForHistoryDoc(doc, opts.Headers)
	responsesLite := normalizeCodexResponsesLiteRequestDoc(doc, opts.Headers)
	if !useSIWC {
		if !responsesLite {
			normalizeCodexInstructionsDoc(doc, nativeRequest)
		}
		if e.cfg == nil || e.cfg.DisableImageGeneration == config.DisableImageGenerationOff {
			checkModel := req.Model
			if strings.TrimSpace(checkModel) == "" {
				checkModel = baseModel
			}
			ensureImageGenerationToolDoc(doc, checkModel, auth, opts.Headers)
			if !codexAuthUsesAPIKey(auth) {
				normalizeCodexImageGenerationToolsDoc(doc)
			}
		}
	}
	helps.NormalizeCodexToolSchemasDoc(doc)
	multiAgentV2Conflict := helps.HasCodexMultiAgentV2NamespaceConflictDoc(doc)
	optimizeMultiAgentV2 := helps.OptimizeCodexMultiAgentV2RequestForAuthDoc(doc, ctx, opts.Headers, e.cfg, auth, isCompat)
	replayScope, _, errReplay := applyCodexReasoningReplayCacheRequiredDoc(ctx, from, req, opts, doc)
	if errReplay != nil {
		return resp, errReplay
	}
	sanitizeOpenAIResponsesReasoningEncryptedContentWithCompatModelAndBaseURLDoc(ctx, "codex websockets executor", doc, isCompat, targetModel, baseURL)

	httpURL := strings.TrimSuffix(baseURL, "/") + "/responses"
	wsURL, err := buildCodexResponsesWebsocketURL(httpURL)
	if err != nil {
		return resp, err
	}

	wsHeaders, errPromptCache := applyCodexPromptCacheHeadersWithContextDoc(ctx, from, req, doc, opts.Headers)
	if errPromptCache != nil {
		return resp, errPromptCache
	}
	helps.SanitizeCodexInputItemIDsDoc(doc)
	if useSIWC {
		applyCodexSIWCWebsocketBodyRulesDoc(doc, hasExplicitInstructions, cliproxyexecutor.Options{KeepTopLevelEffort: thinking.HasThinkingSuffix(req.Model)})
	}
	executionSessionID := executionSessionIDFromOptions(opts)
	var sess *codexWebsocketSession
	isEphemeralSession := false
	sessionLocked := false
	unlockSession := func() {
		if sessionLocked {
			sess.reqMu.Unlock()
			sessionLocked = false
		}
	}
	if executionSessionID != "" {
		sess = e.getOrCreateSession(executionSessionID)
		sess.reqMu.Lock()
		sessionLocked = true
		defer unlockSession()
	} else {
		isEphemeralSession = true
		sess = newEphemeralCodexWebsocketSession()
	}

	sessionInstructions, errSession := e.prepareSessionInstructions(ctx, doc, auth, req, opts, sess, wsURL, useSIWC)
	if errSession != nil {
		return resp, errSession
	}
	helps.FinalizePayloadDoc(ctx, doc)
	body = doc.Bytes()
	if errMarker := helps.ValidateCodexSessionInstructionMarker(body, from); errMarker != nil {
		return resp, errMarker
	}
	if errFinal := sessionInstructions.CheckFinal(body); errFinal != nil {
		return resp, errFinal
	}
	upstreamModel := strings.TrimSpace(doc.Get("model").String())
	if upstreamModel != "" && upstreamModel != baseModel {
		reporter.SetUpstreamModel(upstreamModel)
	}
	reporter.SetTranslatedReasoningEffort(body, to.String())
	wsHeaders = applyCodexWebsocketHeaders(ctx, wsHeaders, auth, apiKey, e.cfg, nativeRequest, opts.Headers)
	applyCodexRoutingHint(ctx, wsHeaders, auth, upstreamModel, body, opts.Headers)
	clientHeaders := util.ResolveClientHeaders((&http.Request{}).WithContext(ctx), opts.Headers)
	finalizeCodexWebsocketHeaders(ctx, wsHeaders, auth, useSIWC, body, upstreamModel, clientHeaders)
	if errValidate := helps.ValidateCodexHostedImageGenerationDoc(doc, wsHeaders, e.cfg, !useSIWC && !codexAuthUsesAPIKey(auth)); errValidate != nil {
		return resp, errValidate
	}

	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}

	wsReqBody := frameCodexWebsocketRequestBody(body)
	wsReqLog := helps.UpstreamRequestLog{
		URL:       wsURL,
		Method:    "WEBSOCKET",
		Headers:   wsHeaders.Clone(),
		Body:      sessionInstructions.LogBody(wsReqBody),
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	}
	helps.RecordAPIWebsocketRequest(ctx, e.cfg, wsReqLog)

	var conn *websocket.Conn
	var closer *websocketConnectionCloser
	var respHS *http.Response
	var upstreamCreated bool
	var errDial error
	dialCtx := ctx
	requiresConnection := cliproxyexecutor.RequiredUpstreamWebsocket(ctx) || sessionInstructions.ConnectionBound()
	if requiresConnection {
		conn, closer = existingWebsocketSessionConn(sess, authID, wsURL, executionProxyURL(ctx, sess.effectiveConfig(authID, e.cfg), auth), useSIWC)
		if conn == nil {
			return resp, codexMissingRequiredWebsocketError(sessionInstructions)
		}
	} else {
		dialCtx = cliproxyexecutor.WithUpstreamAttemptTracker(ctx)
		conn, closer, respHS, upstreamCreated, errDial = e.ensureUpstreamConn(dialCtx, auth, sess, authID, wsURL, wsHeaders, useSIWC)
	}
	if errDial != nil {
		bodyErr := recordWebsocketDialFailure(ctx, e.cfg, wsReqLog, respHS, errDial)
		if respHS != nil && respHS.StatusCode == http.StatusUpgradeRequired {
			if opts.ExecutionLifecycle == nil && !cliproxyexecutor.DownstreamWebsocket(ctx) {
				return e.CodexExecutor.Execute(ctx, auth, req, opts)
			}
			if cliproxyexecutor.UpstreamAttempted(dialCtx) {
				cliproxyexecutor.MarkUpstreamAttempt(ctx)
			}
			return resp, statusErr{code: respHS.StatusCode, msg: string(bodyErr)}
		}
		if cliproxyexecutor.UpstreamAttempted(dialCtx) {
			cliproxyexecutor.MarkUpstreamAttempt(ctx)
		}
		if respHS != nil && respHS.StatusCode > 0 {
			return resp, newCodexStatusErrWithHeadersAndCooling(respHS.StatusCode, respHS.Header, bodyErr, e.modelLevelCooling())
		}
		return resp, errDial
	}
	if errConn := sessionInstructions.OnConnection(conn); errConn != nil {
		return resp, errConn
	}
	if errBind := sess.bindExecutionLifecycle(opts, conn, closer, req.Model); errBind != nil {
		unlockSession()
		closeWebsocketAfterBindFailure(sess, conn, closer)
		return resp, errBind
	}
	recordAPIWebsocketHandshake(ctx, e.cfg, respHS)
	reporter.StartResponseTTFT()
	if !isEphemeralSession && cliproxyexecutor.DownstreamWebsocket(ctx) && upstreamCreated && codexWebsocketRequestNeedsTranscriptReplayOnReset(wsReqBody) {
		errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "upstream_recreated"}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
		return resp, errReplay
	}
	if isEphemeralSession {
		defer func() {
			reason := "completed"
			if err != nil {
				reason = "error"
			}
			closeCodexWebsocketSession(sess, reason)
		}()
	}

	readCh := sess.activateCodex(conn)
	defer func() {
		sess.clearActive(conn, readCh)
	}()
	restoreMultiAgentV2 := !multiAgentV2Conflict && (optimizeMultiAgentV2 || sess.isMultiAgentV2Optimized(conn))

	sessionReceipt := sess.sessionInstructions.Sent(conn, sessionInstructions)
	cliproxyexecutor.MarkUpstreamAttempt(ctx)
	if errSend := writeCodexWebsocketMessage(sess, conn, wsReqBody); errSend != nil {
		errSend = mapCodexWebsocketWriteError(sess, conn, errSend)
		if !isEphemeralSession {
			helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
			if !shouldRetryCodexWebsocketSend(errSend) {
				mode := codexDropNotifyDownstream
				if requiresConnection {
					mode = codexDropQuiet
				}
				e.dropUpstreamConn(sess, conn, "send_error", errSend, mode)
				return resp, errSend
			}
			e.dropUpstreamConn(sess, conn, "send_error", errSend, codexDropForRecovery)
			if requiresConnection {
				return resp, codexMissingRequiredWebsocketError(sessionInstructions)
			}
			sess.clearActive(conn, readCh)
			if codexWebsocketRequestNeedsTranscriptReplayOnReset(wsReqBody) {
				errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "send_error", cause: errSend}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
				return resp, errReplay
			}

			// Retry once with a fresh websocket connection. This is mainly to handle
			// upstream closing the socket between sequential requests within the same
			// execution session.
			connRetry, closerRetry, respHSRetry, _, errDialRetry := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders, useSIWC)
			if errDialRetry != nil || connRetry == nil {
				bodyErrRetry := recordWebsocketDialRetryFailure(ctx, e.cfg, wsReqLog, respHSRetry, errDialRetry)
				if respHSRetry != nil && respHSRetry.StatusCode > 0 {
					errDialRetry = newCodexStatusErrWithHeadersAndCooling(respHSRetry.StatusCode, respHSRetry.Header, bodyErrRetry, e.modelLevelCooling())
				}
				return resp, errDialRetry
			}
			conn = connRetry
			closer = closerRetry
			if errBind := sess.bindExecutionLifecycle(opts, conn, closer, req.Model); errBind != nil {
				unlockSession()
				closeWebsocketAfterBindFailure(sess, conn, closer)
				return resp, errBind
			}
			readCh = sess.activateCodex(conn)
			restoreMultiAgentV2 = !multiAgentV2Conflict && (optimizeMultiAgentV2 || sess.isMultiAgentV2Optimized(conn))
			helps.RecordAPIWebsocketRequest(ctx, e.cfg, helps.UpstreamRequestLog{
				URL:       wsURL,
				Method:    "WEBSOCKET",
				Headers:   wsHeaders.Clone(),
				Body:      sessionInstructions.LogBody(wsReqBody),
				Provider:  e.Identifier(),
				AuthID:    authID,
				AuthLabel: authLabel,
				AuthType:  authType,
				AuthValue: authValue,
			})
			recordAPIWebsocketHandshake(ctx, e.cfg, respHSRetry)
			reporter.StartResponseTTFT()
			sessionReceipt = sess.sessionInstructions.Sent(conn, sessionInstructions)
			cliproxyexecutor.MarkUpstreamAttempt(ctx)
			if errSendRetry := writeCodexWebsocketMessage(sess, conn, wsReqBody); errSendRetry != nil {
				errSendRetry = mapCodexWebsocketWriteError(sess, conn, errSendRetry)
				mode := codexDropNotifyDownstream
				if cliproxyexecutor.DownstreamWebsocket(ctx) {
					mode = codexDropQuiet
				}
				e.dropUpstreamConn(sess, conn, "send_error", errSendRetry, mode)
				sess.clearActive(conn, readCh)
				helps.RecordAPIWebsocketError(ctx, e.cfg, "send_retry", errSendRetry)
				return resp, errSendRetry
			}
		} else {
			e.dropUpstreamConn(sess, conn, "send_error", errSend, codexDropNotifyDownstream)
			sess.clearActive(conn, readCh)
			closeCodexWebsocketSession(sess, "send_error")
			helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
			return resp, errSend
		}
	}

	if optimizeMultiAgentV2 || multiAgentV2Conflict {
		sess.setMultiAgentV2Optimized(conn, optimizeMultiAgentV2 && !multiAgentV2Conflict)
	}

	replayableRead := cliproxyexecutor.DownstreamWebsocket(ctx) || codexWebsocketRequestNeedsTranscriptReplayOnReset(wsReqBody)
	outputItemsByIndex := make(map[int64][]byte)
	var outputItemsFallback [][]byte
	sawOutputDelta := false
	for {
		if ctx != nil && ctx.Err() != nil {
			return resp, ctx.Err()
		}
		msgType, payload, errRead := readCodexWebsocketMessage(ctx, sess, conn, readCh)
		if errRead != nil {
			if ctx != nil && ctx.Err() != nil {
				return resp, ctx.Err()
			}
			if errReset := codexWebsocketReadResetError(errRead, replayableRead, sessionInstructions); errReset != nil {
				helps.RecordAPIWebsocketError(ctx, e.cfg, "read_reset", errReset)
				return resp, errReset
			}
			mappedErr := mapCodexWebsocketReadError(errRead)
			var upstreamReset codexWebsocketUpstreamResetError
			if !errors.As(errRead, &upstreamReset) {
				mode := codexDropNotifyDownstream
				if cliproxyexecutor.DownstreamWebsocket(ctx) {
					mode = codexDropQuiet
				}
				e.dropUpstreamConn(sess, conn, "read_error", mappedErr, mode)
			}
			helps.RecordAPIWebsocketError(ctx, e.cfg, "read", mappedErr)
			return resp, mappedErr
		}
		if msgType != websocket.TextMessage {
			if msgType == websocket.BinaryMessage {
				err = fmt.Errorf("codex websockets executor: unexpected binary message")
				e.dropUpstreamConn(sess, conn, "unexpected_binary", err, codexDropNotifyDownstream)
				helps.RecordAPIWebsocketError(ctx, e.cfg, "unexpected_binary", err)
				return resp, err
			}
			continue
		}

		payload = bytes.TrimSpace(payload)
		if len(payload) == 0 {
			continue
		}
		observeCodexTokenEvent(reporter, payload)
		helps.AppendCodexAPIWebsocketResponse(ctx, e.cfg, payload)
		helps.EmitWebSocketResponseEvent(ctx, opts, auth, e.Identifier(), req.Model, payload)
		payload = helps.RestoreCodexMultiAgentV2Response(payload, restoreMultiAgentV2)

		if failure, ok := parseCodexWebsocketTerminalFailure(ctx, payload, e.modelLevelCooling()); ok {
			if failure.replayRequired {
				e.dropUpstreamConn(sess, conn, "transcript_replay", failure.err, codexDropForRecovery)
			} else if reason := codexWebsocketUpstreamErrorDropReason(payload, failure.rawErr); reason != "" {
				e.dropUpstreamConn(sess, conn, reason, failure.rawErr, codexDropQuiet)
			} else {
				e.dropUpstreamConn(sess, conn, failure.reason, failure.rawErr, codexDropNotifyDownstream)
			}
			unlockSession()
			var errClearReplay error
			if failure.isWebsocketError {
				errClearReplay = clearCodexReasoningReplayOnWebsocketError(ctx, replayScope, payload)
			} else {
				errClearReplay = clearCodexReasoningReplayOnInvalidSignature(ctx, replayScope, codexWebsocketErrorStatusCode(failure.rawErr), failure.body)
			}
			if errClearReplay != nil {
				return resp, errClearReplay
			}
			helps.RecordAPIWebsocketError(ctx, e.cfg, failure.reason, failure.err)
			reporter.PublishFailure(ctx, failure.err)
			return resp, failure.err
		}

		sessionReceipt.Observe(payload)
		payload = normalizeCodexWebsocketCompletion(payload)
		eventType := ojson.GetBytesNoCopy(payload, "type").String()
		if helps.HasMeaningfulCodexOutputDelta(payload) {
			sawOutputDelta = true
		}
		switch eventType {
		case "response.output_item.done":
			collectCodexOutputItemDone(payload, outputItemsByIndex, &outputItemsFallback)
		case "response.completed", "response.done", "response.incomplete":
			if helps.IsCodexTerminalEmptyIncomplete(payload, len(outputItemsByIndex)+len(outputItemsFallback), sawOutputDelta) {
				mode := codexDropNotifyDownstream
				if cliproxyexecutor.DownstreamWebsocket(ctx) {
					mode = codexDropQuiet
				}
				e.dropUpstreamConn(sess, conn, "terminal_empty_incomplete", nil, mode)
				unlockSession()
				streamErr := newCodexEmptyIncompleteStreamError()
				helps.RecordAPIResponseError(ctx, e.cfg, streamErr)
				reporter.PublishFailure(ctx, streamErr)
				return resp, streamErr
			}
			payload = patchCodexCompletedOutput(payload, outputItemsByIndex, outputItemsFallback)
			if eventType != "response.incomplete" {
				cacheCodexReasoningReplayFromCompleted(replayScope, payload)
			}
			if detail, ok := helps.ParseCodexUsageWithOutputItems(payload, outputItemsByIndex, outputItemsFallback); ok {
				reporter.Publish(ctx, detail)
			} else {
				reporter.EnsurePublished(ctx)
			}
			publishCodexImageToolUsageWithOutputItems(ctx, reporter, body, payload, outputItemsByIndex, outputItemsFallback)
			var param any
			out := sdktranslator.TranslateNonStream(ctx, to, responseFormat, req.Model, originalPayload, body, payload, &param)
			if responseFormat == sdktranslator.FormatOpenAIResponse {
				out = helps.EnsureResponsesUsageDetails(out)
			}
			resp = cliproxyexecutor.Response{Payload: out, Incomplete: eventType == "response.incomplete"}
			return resp, nil
		}
	}
}
