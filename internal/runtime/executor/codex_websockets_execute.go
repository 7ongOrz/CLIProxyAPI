package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func (e *CodexWebsocketsExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Alt == "responses/compact" {
		return e.CodexExecutor.executeCompact(ctx, auth, req, opts)
	}

	baseModel := thinking.ParseSuffix(req.Model).ModelName
	apiKey, baseURL := codexCreds(auth)
	if baseURL == "" {
		baseURL = "https://chatgpt.com/backend-api/codex"
	}

	reporter := helps.NewExecutorUsageReporter(ctx, e, baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	to := sdktranslator.FromString("codex")
	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalPayload := originalPayloadSource
	originalTranslated, body := translateCodexRequestPair(from, to, baseModel, originalPayload, req.Payload, false)

	body, err = helps.ApplyRequestThinking(body, req, opts, from.String(), to.String(), e.Identifier())
	if err != nil {
		return resp, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	requestPath := helps.PayloadRequestPath(opts)
	body = helps.ApplyPayloadConfigWithRequest(e.cfg, baseModel, to.String(), from.String(), "", body, originalTranslated, requestedModel, requestPath, opts.Headers)
	body = helps.SetStringIfDifferent(body, "model", baseModel)
	body = helps.SetBoolIfDifferent(body, "stream", true)
	body, _ = sjson.DeleteBytes(body, "prompt_cache_retention")
	body, _ = sjson.DeleteBytes(body, "safety_identifier")
	body = normalizeCodexInstructions(body)
	body, responsesLite := normalizeCodexResponsesLiteRequest(body, opts.Headers)
	if !responsesLite && (e.cfg == nil || e.cfg.DisableImageGeneration == config.DisableImageGenerationOff) {
		body = ensureImageGenerationTool(body, baseModel, auth, opts.Headers)
	}
	body = sanitizeOpenAIResponsesReasoningEncryptedContent(ctx, "codex websockets executor", body)
	body = normalizeCodexWebsocketParallelToolCalls(body, opts.Headers)
	body, optimizeMultiAgentV2 := helps.OptimizeCodexMultiAgentV2RequestForAuth(ctx, opts.Headers, body, e.cfg, auth, baseModel)
	body, replayScope, errReplay := applyCodexReasoningReplayCacheRequired(ctx, from, req, opts, body)
	if errReplay != nil {
		return resp, errReplay
	}

	httpURL := strings.TrimSuffix(baseURL, "/") + "/responses"
	wsURL, err := buildCodexResponsesWebsocketURL(httpURL)
	if err != nil {
		return resp, err
	}

	body, wsHeaders, errPromptCache := applyCodexPromptCacheHeadersWithContext(ctx, from, req, body, opts.Headers)
	if errPromptCache != nil {
		return resp, errPromptCache
	}
	clientBody := body
	var identityState codexIdentityConfuseState
	upstreamBody, identityState := applyCodexIdentityConfuseBody(e.cfg, auth, originalPayloadSource, body)
	reporter.SetTranslatedReasoningEffort(clientBody, to.String())
	wsHeaders = applyCodexWebsocketHeaders(ctx, wsHeaders, auth, apiKey, e.cfg)
	finalizeCodexWebsocketHeaders(wsHeaders, upstreamBody, baseModel, &identityState)

	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}

	executionSessionID := executionSessionIDFromOptions(opts)
	var sess *codexWebsocketSession
	sessionLocked := false
	unlockSession := func() {
		if sess != nil && sessionLocked {
			sess.reqMu.Unlock()
			sessionLocked = false
		}
	}
	if executionSessionID != "" {
		sess = e.getOrCreateSession(executionSessionID)
		sess.reqMu.Lock()
		sessionLocked = true
		defer unlockSession()
	}

	wsReqBody := buildCodexWebsocketRequestBody(upstreamBody)
	wsReqLog := helps.UpstreamRequestLog{
		URL:       wsURL,
		Method:    "WEBSOCKET",
		Headers:   wsHeaders.Clone(),
		Body:      wsReqBody,
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
	if cliproxyexecutor.RequiredUpstreamWebsocket(ctx) {
		conn, closer = existingWebsocketSessionConn(sess, authID, wsURL)
		if conn == nil {
			return resp, cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
		}
	} else {
		conn, closer, respHS, upstreamCreated, errDial = e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
	}
	if errDial != nil {
		bodyErr := websocketHandshakeBody(respHS)
		if respHS != nil {
			helps.RecordAPIWebsocketUpgradeRejection(ctx, e.cfg, websocketUpgradeRequestLog(wsReqLog), respHS.StatusCode, respHS.Header.Clone(), bodyErr)
		}
		if respHS != nil && respHS.StatusCode == http.StatusUpgradeRequired {
			if opts.ExecutionLifecycle != nil || cliproxyexecutor.DownstreamWebsocket(ctx) {
				return resp, statusErr{code: respHS.StatusCode, msg: string(bodyErr)}
			}
			return e.CodexExecutor.Execute(ctx, auth, req, opts)
		}
		if respHS != nil && respHS.StatusCode > 0 {
			return resp, statusErr{code: respHS.StatusCode, msg: string(bodyErr)}
		}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "dial", errDial)
		return resp, errDial
	}
	if errBind := sess.bindExecutionLifecycle(opts, conn, closer, req.Model); errBind != nil {
		unlockSession()
		closeWebsocketAfterBindFailure(sess, conn, closer)
		return resp, errBind
	}
	recordAPIWebsocketHandshake(ctx, e.cfg, respHS)
	reporter.StartResponseTTFT()
	if sess != nil && cliproxyexecutor.DownstreamWebsocket(ctx) && upstreamCreated && codexWebsocketRequestNeedsTranscriptReplayOnReset(wsReqBody) {
		errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "upstream_recreated"}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
		return resp, errReplay
	}
	if sess == nil {
		logCodexWebsocketConnected(executionSessionID, authID, wsURL)
		defer func() {
			reason := "completed"
			if err != nil {
				reason = "error"
			}
			logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, reason, err)
			if errClose := closer.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
		}()
	}

	var readCh chan codexWebsocketRead
	if sess != nil {
		readCh = sess.activate(conn)
		defer func() {
			sess.clearActive(conn, readCh)
		}()
	}

	if errSend := writeCodexWebsocketMessage(sess, conn, wsReqBody); errSend != nil {
		errSend = mapCodexWebsocketWriteError(sess, conn, errSend)
		if sess != nil {
			if cliproxyexecutor.RequiredUpstreamWebsocket(ctx) {
				if !shouldRetryCodexWebsocketSend(errSend) {
					e.invalidateUpstreamConnWithoutDisconnectNotify(sess, conn, "send_error", errSend)
					helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
					return resp, errSend
				}
				e.detachUpstreamConnForRecovery(sess, conn, "send_error", errSend)
				return resp, cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
			}
			if !shouldRetryCodexWebsocketSend(errSend) {
				e.invalidateUpstreamConn(sess, conn, "send_error", errSend)
				helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
				return resp, errSend
			}
			e.detachUpstreamConnForRecovery(sess, conn, "send_error", errSend)
			sess.clearActive(conn, readCh)
			if codexWebsocketRequestNeedsTranscriptReplayOnReset(wsReqBody) {
				errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "send_error", cause: errSend}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
				return resp, errReplay
			}

			// Retry once with a fresh websocket connection. This is mainly to handle
			// upstream closing the socket between sequential requests within the same
			// execution session.
			connRetry, closerRetry, respHSRetry, _, errDialRetry := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
			if errDialRetry != nil || connRetry == nil {
				closeHTTPResponseBody(respHSRetry, "codex websockets executor: close handshake response body error")
				helps.RecordAPIWebsocketError(ctx, e.cfg, "dial_retry", errDialRetry)
				return resp, errDialRetry
			}
			previousConn, previousReadCh := conn, readCh
			conn = connRetry
			closer = closerRetry
			if errBind := sess.bindExecutionLifecycle(opts, conn, closer, req.Model); errBind != nil {
				clearRetryActiveState(sess, previousConn, previousReadCh)
				unlockSession()
				closeWebsocketAfterBindFailure(sess, conn, closer)
				return resp, errBind
			}
			readCh = sess.activate(conn)
			wsReqBodyRetry := buildCodexWebsocketRequestBody(upstreamBody)
			helps.RecordAPIWebsocketRequest(ctx, e.cfg, helps.UpstreamRequestLog{
				URL:       wsURL,
				Method:    "WEBSOCKET",
				Headers:   wsHeaders.Clone(),
				Body:      wsReqBodyRetry,
				Provider:  e.Identifier(),
				AuthID:    authID,
				AuthLabel: authLabel,
				AuthType:  authType,
				AuthValue: authValue,
			})
			recordAPIWebsocketHandshake(ctx, e.cfg, respHSRetry)
			reporter.StartResponseTTFT()
			if errSendRetry := writeCodexWebsocketMessage(sess, conn, wsReqBodyRetry); errSendRetry != nil {
				errSendRetry = mapCodexWebsocketWriteError(sess, conn, errSendRetry)
				e.invalidateUpstreamConn(sess, conn, "send_error", errSendRetry)
				sess.clearActive(conn, readCh)
				helps.RecordAPIWebsocketError(ctx, e.cfg, "send_retry", errSendRetry)
				return resp, errSendRetry
			}
			wsReqBody = wsReqBodyRetry
		} else {
			helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
			return resp, errSend
		}
	}

	outputItemsByIndex := make(map[int64][]byte)
	var outputItemsFallback [][]byte
	for {
		if ctx != nil && ctx.Err() != nil {
			return resp, ctx.Err()
		}
		msgType, payload, errRead := readCodexWebsocketMessage(ctx, sess, conn, readCh)
		if errRead != nil {
			if sess != nil && ctx != nil && ctx.Err() != nil {
				return resp, ctx.Err()
			}
			if codexWebsocketReadErrorRequiresTranscriptReplay(wsReqBody, errRead, cliproxyexecutor.DownstreamWebsocket(ctx)) {
				errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "read_error", cause: errRead}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
				return resp, errReplay
			}
			mappedErr := mapCodexWebsocketReadError(errRead)
			var upstreamReset codexWebsocketUpstreamResetError
			if sess != nil && !errors.As(errRead, &upstreamReset) {
				e.invalidateUpstreamConn(sess, conn, "read_error", mappedErr)
			}
			helps.RecordAPIWebsocketError(ctx, e.cfg, "read", mappedErr)
			return resp, mappedErr
		}
		if msgType != websocket.TextMessage {
			if msgType == websocket.BinaryMessage {
				err = fmt.Errorf("codex websockets executor: unexpected binary message")
				if sess != nil {
					e.invalidateUpstreamConn(sess, conn, "unexpected_binary", err)
				}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "unexpected_binary", err)
				return resp, err
			}
			continue
		}

		payload = bytes.TrimSpace(payload)
		if len(payload) == 0 {
			continue
		}
		reporter.MarkFirstResponseByte()
		appendCodexWebsocketResponseLog(ctx, e.cfg, payload, identityState)
		payload = helps.RestoreCodexMultiAgentV2Response(payload, optimizeMultiAgentV2)

		if wsErr, ok := parseCodexWebsocketError(payload); ok {
			if sess != nil {
				if shouldDropCodexWebsocketUpstreamErrorQuietly(payload, wsErr) {
					e.dropUpstreamConn(sess, conn, codexWebsocketUpstreamErrorDropReason(payload, wsErr), wsErr, false)
				} else {
					e.invalidateUpstreamConn(sess, conn, "upstream_error", wsErr)
				}
				unlockSession()
			}
			if errClearReplay := clearCodexReasoningReplayOnWebsocketError(ctx, replayScope, payload); errClearReplay != nil {
				return resp, errClearReplay
			}
			helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", wsErr)
			reporter.PublishFailure(ctx, wsErr)
			return resp, withCodexWebsocketIdentityClientError(payload, identityState, wsErr)
		}

		if upstreamTerminalErr, terminalReason, ok := parseCodexResponseTerminalError(payload); ok {
			if errClearReplay := clearCodexReasoningReplayOnWebsocketTerminalError(ctx, replayScope, payload); errClearReplay != nil {
				return resp, errClearReplay
			}
			if sess != nil {
				e.dropUpstreamConn(sess, conn, terminalReason, upstreamTerminalErr, false)
				unlockSession()
			}
			helps.RecordAPIWebsocketError(ctx, e.cfg, terminalReason, upstreamTerminalErr)
			reporter.PublishFailure(ctx, upstreamTerminalErr)
			return resp, withCodexIdentityClientError(upstreamTerminalErr, identityState)
		}
		if streamErr, terminalBody, ok := codexTerminalFailureErr(payload); ok {
			if sess != nil {
				if shouldDropCodexWebsocketUpstreamErrorQuietly(payload, streamErr) {
					e.dropUpstreamConn(sess, conn, codexWebsocketUpstreamErrorDropReason(payload, streamErr), streamErr, false)
				} else {
					e.invalidateUpstreamConn(sess, conn, "terminal_failure", streamErr)
				}
				unlockSession()
			}
			if errClearReplay := clearCodexReasoningReplayOnInvalidSignature(ctx, replayScope, streamErr.StatusCode(), terminalBody); errClearReplay != nil {
				return resp, errClearReplay
			}
			return resp, withCodexIdentityClientError(streamErr, identityState)
		}

		payload = normalizeCodexWebsocketCompletion(payload)
		eventType := gjson.GetBytes(payload, "type").String()
		switch eventType {
		case "response.output_item.done":
			collectCodexOutputItemDone(payload, outputItemsByIndex, &outputItemsFallback)
		case "response.completed":
			payload = patchCodexCompletedOutput(payload, outputItemsByIndex, outputItemsFallback)
			cacheCodexReasoningReplayFromCompleted(replayScope, payload)
			if detail, ok := helps.ParseCodexUsage(payload); ok {
				reporter.Publish(ctx, detail)
			}
			var param any
			clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
			out := sdktranslator.TranslateNonStream(ctx, to, responseFormat, req.Model, originalPayload, clientBody, clientPayload, &param)
			resp = cliproxyexecutor.Response{Payload: out}
			return resp, nil
		}
	}
}
