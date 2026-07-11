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
)

func (e *CodexWebsocketsExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	log.Debugf("Executing Codex Websockets stream request with auth ID: %s, model: %s", auth.ID, req.Model)
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Alt == "responses/compact" {
		return nil, statusErr{code: http.StatusBadRequest, msg: "streaming not supported for /responses/compact"}
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
	originalTranslated, body := translateCodexRequestPair(from, to, baseModel, originalPayload, req.Payload, true)

	body, err = helps.ApplyRequestThinking(body, req, opts, from.String(), to.String(), e.Identifier())
	if err != nil {
		return nil, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	requestPath := helps.PayloadRequestPath(opts)
	body = helps.ApplyPayloadConfigWithRequest(e.cfg, baseModel, to.String(), from.String(), "", body, originalTranslated, requestedModel, requestPath, opts.Headers)
	body = helps.SetStringIfDifferent(body, "model", baseModel)
	body = normalizeCodexInstructions(body)
	body, responsesLite := normalizeCodexResponsesLiteRequest(body, opts.Headers)
	if !responsesLite && (e.cfg == nil || e.cfg.DisableImageGeneration == config.DisableImageGenerationOff) {
		body = ensureImageGenerationTool(body, baseModel, auth, opts.Headers)
	}
	body = sanitizeOpenAIResponsesReasoningEncryptedContent(ctx, "codex websockets executor", body)
	body = normalizeCodexWebsocketParallelToolCalls(body, opts.Headers)
	multiAgentV2Conflict := helps.HasCodexMultiAgentV2NamespaceConflict(body)
	body, optimizeMultiAgentV2 := helps.OptimizeCodexMultiAgentV2RequestForAuth(ctx, opts.Headers, body, e.cfg, auth, baseModel)
	body, replayScope, errReplay := applyCodexReasoningReplayCacheRequired(ctx, from, req, opts, body)
	if errReplay != nil {
		return nil, errReplay
	}

	httpURL := strings.TrimSuffix(baseURL, "/") + "/responses"
	wsURL, err := buildCodexResponsesWebsocketURL(httpURL)
	if err != nil {
		return nil, err
	}

	body, wsHeaders, errPromptCache := applyCodexPromptCacheHeadersWithContext(ctx, from, req, body, opts.Headers)
	if errPromptCache != nil {
		return nil, errPromptCache
	}
	clientBody := body
	var identityState codexIdentityConfuseState
	upstreamBody, identityState := applyCodexIdentityConfuseBody(e.cfg, auth, originalPayloadSource, body)
	reporter.SetTranslatedReasoningEffort(clientBody, to.String())
	wsHeaders = applyCodexWebsocketHeaders(ctx, wsHeaders, auth, apiKey, e.cfg, opts.Headers)
	finalizeCodexWebsocketHeaders(wsHeaders, upstreamBody, baseModel, auth, &identityState)

	var authID, authLabel, authType, authValue string
	authID = auth.ID
	authLabel = auth.Label
	authType, authValue = auth.AccountInfo()

	executionSessionID := executionSessionIDFromOptions(opts)
	var sess *codexWebsocketSession
	if executionSessionID != "" {
		sess = e.getOrCreateSession(executionSessionID)
		if sess != nil {
			sess.reqMu.Lock()
		}
	}
	streamSessionLocked := sess != nil
	unlockSessionRequest := func() {
		if sess != nil && streamSessionLocked {
			sess.reqMu.Unlock()
			streamSessionLocked = false
		}
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
			unlockSessionRequest()
			return nil, cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
		}
	} else {
		conn, closer, respHS, upstreamCreated, errDial = e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
	}
	var upstreamHeaders http.Header
	if respHS != nil {
		upstreamHeaders = respHS.Header.Clone()
	} else if sess != nil {
		upstreamHeaders = sess.takeHandshakeHeadersForReplay(conn)
	}
	if errDial != nil {
		bodyErr := websocketHandshakeBody(respHS)
		if respHS != nil {
			helps.RecordAPIWebsocketUpgradeRejection(ctx, e.cfg, websocketUpgradeRequestLog(wsReqLog), respHS.StatusCode, respHS.Header.Clone(), bodyErr)
		}
		if respHS != nil && respHS.StatusCode == http.StatusUpgradeRequired {
			if opts.ExecutionLifecycle != nil || cliproxyexecutor.DownstreamWebsocket(ctx) {
				unlockSessionRequest()
				return nil, statusErr{code: respHS.StatusCode, msg: string(bodyErr)}
			}
			unlockSessionRequest()
			return e.CodexExecutor.ExecuteStream(ctx, auth, req, opts)
		}
		if respHS != nil && respHS.StatusCode > 0 {
			unlockSessionRequest()
			return nil, statusErr{code: respHS.StatusCode, msg: string(bodyErr)}
		}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "dial", errDial)
		unlockSessionRequest()
		return nil, errDial
	}
	if errBind := sess.bindExecutionLifecycle(opts, conn, closer, req.Model); errBind != nil {
		unlockSessionRequest()
		closeWebsocketAfterBindFailure(sess, conn, closer)
		return nil, errBind
	}
	recordAPIWebsocketHandshake(ctx, e.cfg, respHS)
	reporter.StartResponseTTFT()
	if sess != nil && cliproxyexecutor.DownstreamWebsocket(ctx) && upstreamCreated && codexWebsocketRequestNeedsTranscriptReplayOnReset(wsReqBody) {
		errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "upstream_recreated"}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
		sess.storeHandshakeHeadersForReplay(conn, upstreamHeaders)
		unlockSessionRequest()
		return nil, errReplay
	}

	if sess == nil {
		logCodexWebsocketConnected(executionSessionID, authID, wsURL)
	}

	var readCh chan codexWebsocketRead
	if sess != nil {
		readCh = sess.activate(conn)
	}
	restoreMultiAgentV2 := !multiAgentV2Conflict && (optimizeMultiAgentV2 || sess.isMultiAgentV2Optimized(conn))

	if errSend := writeCodexWebsocketMessage(sess, conn, wsReqBody); errSend != nil {
		errSend = mapCodexWebsocketWriteError(sess, conn, errSend)
		helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
		if sess != nil {
			if cliproxyexecutor.RequiredUpstreamWebsocket(ctx) {
				if shouldRetryCodexWebsocketSend(errSend) {
					e.detachUpstreamConnForRecovery(sess, conn, "send_error", errSend)
				} else {
					e.invalidateUpstreamConnWithoutDisconnectNotify(sess, conn, "send_error", errSend)
				}
				sess.clearActive(conn, readCh)
				unlockSessionRequest()
				if !shouldRetryCodexWebsocketSend(errSend) {
					return nil, errSend
				}
				return nil, cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
			}
			if !shouldRetryCodexWebsocketSend(errSend) {
				e.invalidateUpstreamConn(sess, conn, "send_error", errSend)
				sess.clearActive(conn, readCh)
				unlockSessionRequest()
				return nil, errSend
			}
			e.detachUpstreamConnForRecovery(sess, conn, "send_error", errSend)
			sess.clearActive(conn, readCh)
			if codexWebsocketRequestNeedsTranscriptReplayOnReset(wsReqBody) {
				errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "send_error", cause: errSend}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
				unlockSessionRequest()
				return nil, errReplay
			}

			// Retry once with a new websocket connection for the same execution session.
			connRetry, closerRetry, respHSRetry, _, errDialRetry := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
			if errDialRetry != nil || connRetry == nil {
				closeHTTPResponseBody(respHSRetry, "codex websockets executor: close handshake response body error")
				helps.RecordAPIWebsocketError(ctx, e.cfg, "dial_retry", errDialRetry)
				unlockSessionRequest()
				return nil, errDialRetry
			}
			previousConn, previousReadCh := conn, readCh
			conn = connRetry
			closer = closerRetry
			if errBind := sess.bindExecutionLifecycle(opts, conn, closer, req.Model); errBind != nil {
				clearRetryActiveState(sess, previousConn, previousReadCh)
				unlockSessionRequest()
				closeWebsocketAfterBindFailure(sess, conn, closer)
				return nil, errBind
			}
			readCh = sess.activate(conn)
			restoreMultiAgentV2 = !multiAgentV2Conflict && (optimizeMultiAgentV2 || sess.isMultiAgentV2Optimized(conn))
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
				helps.RecordAPIWebsocketError(ctx, e.cfg, "send_retry", errSendRetry)
				e.invalidateUpstreamConn(sess, conn, "send_error", errSendRetry)
				sess.clearActive(conn, readCh)
				unlockSessionRequest()
				return nil, errSendRetry
			}
			if respHSRetry != nil {
				upstreamHeaders = respHSRetry.Header.Clone()
			}
			wsReqBody = wsReqBodyRetry
		} else {
			logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, "send_error", errSend)
			if errClose := closer.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
			return nil, errSend
		}
	}

	if optimizeMultiAgentV2 || multiAgentV2Conflict {
		sess.setMultiAgentV2Optimized(conn, optimizeMultiAgentV2 && !multiAgentV2Conflict)
	}

	buffering := e.cfg != nil && e.cfg.Codex.StreamBootstrapBuffering

	claudeInputTokens := helps.NewClaudeInputTokenState(from, to, responseFormat, originalPayload)
	var param any
	outputItemsByIndex := make(map[int64][]byte)
	var outputItemsFallback [][]byte

	var bufferedChunks [][]byte
	var initialChunks [][]byte
	immediateTerminal := false
	var bootstrapTerminalChunk *cliproxyexecutor.StreamChunk
	// bootstrapTerminalErr holds a non-overload terminal failure seen while buffering. It is
	// delivered as an in-stream chunk after the buffered handshake so downstream behaviour stays
	// identical to the unbuffered path instead of silently turning into a credential failover.
	var bootstrapTerminalErr error

	if buffering {
		for {
			if ctx != nil && ctx.Err() != nil {
				if sess != nil {
					sess.clearActive(conn, readCh)
					unlockSessionRequest()
				} else {
					_ = closer.Close()
				}
				return nil, ctx.Err()
			}
			msgType, payload, errRead := readCodexWebsocketMessage(ctx, sess, conn, readCh)
			if errRead != nil {
				if sess != nil && codexWebsocketReadErrorRequiresTranscriptReplay(wsReqBody, errRead, cliproxyexecutor.DownstreamWebsocket(ctx)) {
					errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "read_error", cause: errRead}
					sess.clearActive(conn, readCh)
					unlockSessionRequest()
					helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
					reporter.PublishFailure(ctx, errReplay)
					return nil, errReplay
				}
				mappedErr := mapCodexWebsocketReadError(errRead)
				if sess != nil {
					e.invalidateUpstreamConn(sess, conn, "read_error", mappedErr)
					sess.clearActive(conn, readCh)
					unlockSessionRequest()
				} else {
					logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, "read_error", mappedErr)
					_ = closer.Close()
				}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "read", mappedErr)
				reporter.PublishFailure(ctx, mappedErr)
				return nil, mappedErr
			}
			if msgType != websocket.TextMessage {
				if msgType == websocket.BinaryMessage {
					errBinary := fmt.Errorf("codex websockets executor: unexpected binary message")
					if sess != nil {
						e.invalidateUpstreamConn(sess, conn, "unexpected_binary", errBinary)
						sess.clearActive(conn, readCh)
						unlockSessionRequest()
					} else {
						logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, "unexpected_binary", errBinary)
						_ = closer.Close()
					}
					helps.RecordAPIWebsocketError(ctx, e.cfg, "unexpected_binary", errBinary)
					reporter.PublishFailure(ctx, errBinary)
					return nil, errBinary
				}
				continue
			}

			payload = bytes.TrimSpace(payload)
			if len(payload) == 0 {
				continue
			}
			reporter.MarkFirstResponseByte()
			appendCodexWebsocketResponseLog(ctx, e.cfg, payload, identityState)
			payload = helps.RestoreCodexMultiAgentV2Response(payload, restoreMultiAgentV2)

			websocketErr, isWebsocketError := parseCodexWebsocketError(payload)
			streamErr, terminalReason, isResponseTerminalError := parseCodexResponseTerminalError(payload)
			terminalBody, terminalFailure := codexTerminalFailureBody(payload)
			terminalErr := error(streamErr)
			if isWebsocketError {
				terminalErr = websocketErr
				terminalBody = []byte(websocketErr.Error())
				terminalReason = "upstream_error"
				terminalFailure = true
			} else if !isResponseTerminalError {
				var ok bool
				streamErr, terminalBody, ok = codexTerminalFailureErr(payload)
				terminalErr = streamErr
				terminalFailure = ok
			} else if !terminalFailure {
				terminalBody = []byte(streamErr.Error())
				terminalFailure = true
			}
			if terminalFailure {
				// A transient capacity rejection is retried on another credential, so the
				// downstream websocket session must survive this upstream teardown. Notifying
				// the disconnect here would close the client connection before the retry can
				// deliver anything. Downstream websocket failures also stay quiet because
				// their original terminal payload is forwarded before the session ends.
				failoverPending := isCodexOverloadBootstrapFailure(terminalBody)
				if terminalReason == "" {
					terminalReason = "terminal_failure"
				}
				if sess != nil {
					unlockSessionRequest()
					if failoverPending || cliproxyexecutor.DownstreamWebsocket(ctx) {
						e.invalidateUpstreamConnWithoutDisconnectNotify(sess, conn, terminalReason, terminalErr)
					} else {
						e.invalidateUpstreamConn(sess, conn, terminalReason, terminalErr)
					}
					sess.clearActive(conn, readCh)
				} else {
					logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, terminalReason, terminalErr)
					_ = closer.Close()
				}
				var errClearReplay error
				if isWebsocketError {
					errClearReplay = clearCodexReasoningReplayOnWebsocketError(ctx, replayScope, payload)
				} else {
					errClearReplay = clearCodexReasoningReplayOnInvalidSignature(ctx, replayScope, streamErr.StatusCode(), terminalBody)
				}
				if errClearReplay != nil {
					helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_clear_error", errClearReplay)
					reporter.PublishFailure(ctx, errClearReplay)
					return nil, errClearReplay
				}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", terminalErr)
				reporter.PublishFailure(ctx, terminalErr)
				if failoverPending {
					// Fail the attempt before the downstream headers are committed so the
					// conductor can transparently retry on another credential.
					helps.LogWithRequestID(ctx).Debugf("codex websockets executor: bootstrap overload rejection after %d buffered handshake events, failing over", len(bufferedChunks))
					if isWebsocketError {
						return nil, withCodexWebsocketIdentityClientError(payload, identityState, websocketErr)
					}
					return nil, withCodexIdentityClientError(newCodexBootstrapOverloadErr(terminalBody), identityState)
				}
				if cliproxyexecutor.DownstreamWebsocket(ctx) {
					clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
					bootstrapTerminalChunk = &cliproxyexecutor.StreamChunk{
						Payload:   helps.EnsureResponsesUsageDetails(clientPayload),
						ResultErr: terminalErr,
					}
				} else if isWebsocketError {
					bootstrapTerminalErr = withCodexWebsocketIdentityClientError(payload, identityState, websocketErr)
				} else {
					bootstrapTerminalErr = withCodexIdentityClientError(streamErr, identityState)
				}
				break
			}

			eventType := gjson.GetBytes(payload, "type").String()
			isTerminalEvent := eventType == "response.completed" || eventType == "response.done" || eventType == "error"
			if eventType == "response.output_item.done" {
				collectCodexOutputItemDone(payload, outputItemsByIndex, &outputItemsFallback)
			}
			completedPayload := payload
			if eventType == "response.completed" || eventType == "response.done" {
				completedPayload = normalizeCodexWebsocketCompletion(completedPayload)
				completedPayload = patchCodexCompletedOutput(completedPayload, outputItemsByIndex, outputItemsFallback)
				cacheCodexReasoningReplayFromCompleted(replayScope, completedPayload)
				if detail, ok := helps.ParseCodexUsage(completedPayload); ok {
					reporter.Publish(ctx, detail)
				}
			}

			var currentChunks [][]byte
			if cliproxyexecutor.DownstreamWebsocket(ctx) {
				clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
				downstreamPayload := helps.EnsureResponsesUsageDetails(clientPayload)
				currentChunks = [][]byte{downstreamPayload}
			} else {
				payload = normalizeCodexWebsocketCompletion(payload)
				if eventType == "response.completed" || eventType == "response.done" {
					payload = completedPayload
				}
				clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
				line := encodeCodexWebsocketAsSSE(clientPayload)
				currentChunks = helps.TranslateStreamWithClaudeInputTokens(ctx, to, responseFormat, req.Model, originalPayload, clientBody, line, &param, claudeInputTokens)
			}

			if isCodexHandshakeMetadataEvent(eventType) && !isTerminalEvent {
				if len(bufferedChunks) < codexBootstrapMaxBufferedEvents {
					bufferedChunks = append(bufferedChunks, currentChunks...)
					continue
				}
				helps.LogWithRequestID(ctx).Debugf("codex websockets executor: bootstrap buffer limit %d reached, releasing stream without overload probing", codexBootstrapMaxBufferedEvents)
			}

			initialChunks = currentChunks
			if isTerminalEvent {
				immediateTerminal = true
			}
			break
		}
	}

	chanCapacity := len(bufferedChunks) + len(initialChunks)
	if bootstrapTerminalChunk != nil {
		chanCapacity++
	}
	if bootstrapTerminalErr != nil {
		chanCapacity++
	}
	out := make(chan cliproxyexecutor.StreamChunk, chanCapacity)
	for _, chunk := range bufferedChunks {
		out <- cliproxyexecutor.StreamChunk{Payload: chunk}
	}
	for _, chunk := range initialChunks {
		out <- cliproxyexecutor.StreamChunk{Payload: chunk}
	}
	if bootstrapTerminalChunk != nil {
		out <- *bootstrapTerminalChunk
		close(out)
		return &cliproxyexecutor.StreamResult{Headers: upstreamHeaders, Chunks: out}, nil
	}
	if bootstrapTerminalErr != nil {
		// The upstream connection was already invalidated and released in the terminal-failure
		// branch above, so only the buffered payloads plus the in-stream error remain to emit.
		out <- cliproxyexecutor.StreamChunk{Err: bootstrapTerminalErr}
		close(out)
		return &cliproxyexecutor.StreamResult{Headers: upstreamHeaders, Chunks: out}, nil
	}
	if immediateTerminal {
		if sess != nil {
			sess.clearActive(conn, readCh)
			unlockSessionRequest()
		} else {
			logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, "completed", nil)
			if errClose := closer.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
		}
		close(out)
		return &cliproxyexecutor.StreamResult{Headers: upstreamHeaders, Chunks: out}, nil
	}

	go func() {
		terminateReason := "completed"
		var terminateErr error

		defer close(out)
		defer func() {
			if sess != nil {
				sess.clearActive(conn, readCh)
				unlockSessionRequest()
				return
			}
			logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, terminateReason, terminateErr)
			if errClose := closer.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
		}()

		send := func(chunk cliproxyexecutor.StreamChunk) bool {
			if ctx == nil {
				out <- chunk
				return true
			}
			select {
			case out <- chunk:
				return true
			case <-ctx.Done():
				return false
			}
		}
		dropTerminalConnection := func(reason string, terminalErr error) {
			if sess == nil {
				return
			}
			if cliproxyexecutor.DownstreamWebsocket(ctx) {
				e.dropUpstreamConn(sess, conn, reason, terminalErr, false)
			} else {
				e.invalidateUpstreamConn(sess, conn, reason, terminalErr)
			}
			unlockSessionRequest()
		}

		for {
			if ctx != nil && ctx.Err() != nil {
				terminateReason = "context_done"
				terminateErr = ctx.Err()
				_ = send(cliproxyexecutor.StreamChunk{Err: ctx.Err()})
				return
			}
			msgType, payload, errRead := readCodexWebsocketMessage(ctx, sess, conn, readCh)
			if errRead != nil {
				if sess != nil && ctx != nil && ctx.Err() != nil {
					terminateReason = "context_done"
					terminateErr = ctx.Err()
					_ = send(cliproxyexecutor.StreamChunk{Err: ctx.Err()})
					return
				}
				terminateReason = "read_error"
				if codexWebsocketReadErrorRequiresTranscriptReplay(wsReqBody, errRead, cliproxyexecutor.DownstreamWebsocket(ctx)) {
					errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "read_error", cause: errRead}
					terminateErr = errReplay
					helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
					reporter.PublishFailure(ctx, errReplay)
					_ = send(cliproxyexecutor.StreamChunk{Err: errReplay})
					return
				}
				mappedErr := mapCodexWebsocketReadError(errRead)
				var upstreamReset codexWebsocketUpstreamResetError
				if sess != nil && !errors.As(errRead, &upstreamReset) {
					defer func() {
						dropTerminalConnection("read_error", mappedErr)
					}()
				}
				terminateErr = mappedErr
				helps.RecordAPIWebsocketError(ctx, e.cfg, "read", mappedErr)
				reporter.PublishFailure(ctx, mappedErr)
				_ = send(cliproxyexecutor.StreamChunk{Err: mappedErr})
				return
			}
			if msgType != websocket.TextMessage {
				if msgType == websocket.BinaryMessage {
					errBinary := fmt.Errorf("codex websockets executor: unexpected binary message")
					terminateReason = "unexpected_binary"
					terminateErr = errBinary
					helps.RecordAPIWebsocketError(ctx, e.cfg, "unexpected_binary", errBinary)
					reporter.PublishFailure(ctx, errBinary)
					if sess != nil {
						defer func() {
							dropTerminalConnection("unexpected_binary", errBinary)
						}()
					}
					_ = send(cliproxyexecutor.StreamChunk{Err: errBinary})
					return
				}
				continue
			}

			payload = bytes.TrimSpace(payload)
			if len(payload) == 0 {
				continue
			}
			reporter.MarkFirstResponseByte()
			appendCodexWebsocketResponseLog(ctx, e.cfg, payload, identityState)
			payload = helps.RestoreCodexMultiAgentV2Response(payload, restoreMultiAgentV2)

			if wsErr, ok := parseCodexWebsocketError(payload); ok {
				terminateReason = "upstream_error"
				terminateErr = wsErr
				if sess != nil {
					defer func() {
						if cliproxyexecutor.DownstreamWebsocket(ctx) {
							e.dropUpstreamConn(sess, conn, "upstream_error", wsErr, false)
						} else if shouldDropCodexWebsocketUpstreamErrorQuietly(payload, wsErr) {
							e.dropUpstreamConn(sess, conn, codexWebsocketUpstreamErrorDropReason(payload, wsErr), wsErr, false)
						} else {
							e.invalidateUpstreamConn(sess, conn, "upstream_error", wsErr)
						}
						unlockSessionRequest()
					}()
				}
				if errClearReplay := clearCodexReasoningReplayOnWebsocketError(ctx, replayScope, payload); errClearReplay != nil {
					terminateErr = errClearReplay
					helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_clear_error", errClearReplay)
					reporter.PublishFailure(ctx, errClearReplay)
					_ = send(cliproxyexecutor.StreamChunk{Err: errClearReplay})
					return
				}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", wsErr)
				reporter.PublishFailure(ctx, wsErr)
				if cliproxyexecutor.DownstreamWebsocket(ctx) {
					clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
					_ = send(cliproxyexecutor.StreamChunk{Payload: clientPayload, ResultErr: wsErr})
					return
				}
				_ = send(cliproxyexecutor.StreamChunk{Err: withCodexWebsocketIdentityClientError(payload, identityState, wsErr)})
				return
			}
			if _, _, responseTerminal := parseCodexResponseTerminalError(payload); !responseTerminal {
				if streamErr, terminalBody, ok := codexTerminalFailureErr(payload); ok {
					terminateReason = "upstream_error"
					terminateErr = streamErr
					if sess != nil {
						defer func() {
							if cliproxyexecutor.DownstreamWebsocket(ctx) {
								e.dropUpstreamConn(sess, conn, "terminal_failure", streamErr, false)
							} else if shouldDropCodexWebsocketUpstreamErrorQuietly(payload, streamErr) {
								e.dropUpstreamConn(sess, conn, codexWebsocketUpstreamErrorDropReason(payload, streamErr), streamErr, false)
							} else {
								e.invalidateUpstreamConn(sess, conn, "terminal_failure", streamErr)
							}
							unlockSessionRequest()
						}()
					}
					if errClearReplay := clearCodexReasoningReplayOnInvalidSignature(ctx, replayScope, streamErr.StatusCode(), terminalBody); errClearReplay != nil {
						terminateErr = errClearReplay
						helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_clear_error", errClearReplay)
						reporter.PublishFailure(ctx, errClearReplay)
						_ = send(cliproxyexecutor.StreamChunk{Err: errClearReplay})
						return
					}
					helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", streamErr)
					reporter.PublishFailure(ctx, streamErr)
					if cliproxyexecutor.DownstreamWebsocket(ctx) {
						clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
						_ = send(cliproxyexecutor.StreamChunk{Payload: clientPayload, ResultErr: streamErr})
						return
					}
					_ = send(cliproxyexecutor.StreamChunk{Err: withCodexIdentityClientError(streamErr, identityState)})
					return
				}
			}

			eventType := gjson.GetBytes(payload, "type").String()
			if eventType == "response.output_item.done" {
				collectCodexOutputItemDone(payload, outputItemsByIndex, &outputItemsFallback)
			}
			completedPayload := payload
			if eventType == "response.completed" || eventType == "response.done" {
				completedPayload = normalizeCodexWebsocketCompletion(completedPayload)
				completedPayload = patchCodexCompletedOutput(completedPayload, outputItemsByIndex, outputItemsFallback)
				cacheCodexReasoningReplayFromCompleted(replayScope, completedPayload)
				if detail, ok := helps.ParseCodexUsage(completedPayload); ok {
					reporter.Publish(ctx, detail)
				}
			}

			clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
			upstreamTerminalErr, terminalReason, isResponseTerminalError := parseCodexResponseTerminalError(payload)
			if isResponseTerminalError {
				if sess != nil {
					defer func() {
						e.dropUpstreamConn(sess, conn, terminalReason, upstreamTerminalErr, false)
						unlockSessionRequest()
					}()
				}
			}
			isTerminalEvent := eventType == "response.completed" || eventType == "response.done" || eventType == "error" || isResponseTerminalError
			if cliproxyexecutor.DownstreamWebsocket(ctx) {
				if isResponseTerminalError {
					if errClearReplay := clearCodexReasoningReplayOnWebsocketTerminalError(ctx, replayScope, payload); errClearReplay != nil {
						terminateErr = errClearReplay
						helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_clear_error", errClearReplay)
						reporter.PublishFailure(ctx, errClearReplay)
						_ = send(cliproxyexecutor.StreamChunk{Err: errClearReplay})
						return
					}
					terminateReason = terminalReason
					terminateErr = upstreamTerminalErr
					helps.RecordAPIWebsocketError(ctx, e.cfg, terminalReason, upstreamTerminalErr)
					reporter.PublishFailure(ctx, upstreamTerminalErr)
				}
				chunk := cliproxyexecutor.StreamChunk{Payload: helps.EnsureResponsesUsageDetails(clientPayload)}
				if isResponseTerminalError {
					chunk.ResultErr = upstreamTerminalErr
				}
				if !send(chunk) {
					terminateReason = "context_done"
					terminateErr = ctx.Err()
					return
				}
				if isTerminalEvent {
					return
				}
				continue
			}
			if isResponseTerminalError {
				if errClearReplay := clearCodexReasoningReplayOnWebsocketTerminalError(ctx, replayScope, payload); errClearReplay != nil {
					terminateErr = errClearReplay
					helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_clear_error", errClearReplay)
					reporter.PublishFailure(ctx, errClearReplay)
					_ = send(cliproxyexecutor.StreamChunk{Err: errClearReplay})
					return
				}
				terminateReason = terminalReason
				terminateErr = upstreamTerminalErr
				helps.RecordAPIWebsocketError(ctx, e.cfg, terminalReason, upstreamTerminalErr)
				reporter.PublishFailure(ctx, upstreamTerminalErr)
				_ = send(cliproxyexecutor.StreamChunk{Err: withCodexIdentityClientError(upstreamTerminalErr, identityState)})
				return
			}

			payload = normalizeCodexWebsocketCompletion(payload)
			if eventType == "response.completed" || eventType == "response.done" {
				payload = completedPayload
			}
			eventType = gjson.GetBytes(payload, "type").String()
			clientPayload = applyCodexIdentityExposeResponsePayload(payload, identityState)
			line := encodeCodexWebsocketAsSSE(clientPayload)
			chunks := helps.TranslateStreamWithClaudeInputTokens(ctx, to, responseFormat, req.Model, originalPayload, clientBody, line, &param, claudeInputTokens)
			for i := range chunks {
				if !send(cliproxyexecutor.StreamChunk{Payload: chunks[i]}) {
					terminateReason = "context_done"
					terminateErr = ctx.Err()
					return
				}
			}
			if eventType == "response.completed" || eventType == "response.done" {
				return
			}
		}
	}()

	return &cliproxyexecutor.StreamResult{Headers: upstreamHeaders, Chunks: out}, nil
}
