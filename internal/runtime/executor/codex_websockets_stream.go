package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	log "github.com/sirupsen/logrus"
)

func (e *CodexWebsocketsExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	authID := ""
	if auth != nil {
		authID = auth.ID
	}
	log.Debugf("Executing Codex Websockets stream request with auth ID: %s, model: %s", authID, req.Model)
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = helps.WithCodexCommentaryPolicy(ctx, e.cfg)
	if opts.Alt == "responses/compact" {
		return nil, statusErr{code: http.StatusBadRequest, msg: "streaming not supported for /responses/compact"}
	}

	baseModel := thinking.ParseSuffix(req.Model).ModelName
	isCompat, errExpand := e.expandCodexResponsesCompaction(auth, &req, &opts, baseModel)
	if errExpand != nil {
		return nil, statusErr{code: http.StatusBadRequest, msg: errExpand.Error()}
	}
	if e.shouldCompactCompatResponsesTrigger(isCompat, req, opts) {
		executionSessionID := executionSessionIDFromOptions(opts)
		var sess *codexWebsocketSession
		if executionSessionID != "" {
			sess = e.getOrCreateSession(executionSessionID)
			if sess != nil {
				sess.reqMu.Lock()
				defer sess.reqMu.Unlock()
			}
		}
		res, errCompact := e.CodexExecutor.executeCodexResponsesCompactionStream(ctx, auth, req, opts)
		if errCompact == nil && sess != nil {
			sess.connMu.Lock()
			existingConn := sess.conn
			sess.connMu.Unlock()
			if existingConn != nil {
				e.dropUpstreamConn(sess, existingConn, "local_compaction", nil, codexDropQuiet)
			}
		}
		return res, errCompact
	}

	reporter := helps.NewExecutorUsageReporter(ctx, e, baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	executionSessionID := executionSessionIDFromOptions(opts)
	var sess *codexWebsocketSession
	isEphemeralSession := false
	if executionSessionID != "" {
		sess = e.getOrCreateSession(executionSessionID)
		sess.reqMu.Lock()
	} else {
		isEphemeralSession = true
		sess = newEphemeralCodexWebsocketSession()
	}
	streamSessionLocked := !isEphemeralSession
	unlockSessionRequest := func() {
		if streamSessionLocked {
			sess.reqMu.Unlock()
			streamSessionLocked = false
		}
	}

	prepared, err := e.prepareCodexWebsocketStream(ctx, auth, req, opts, sess)
	if err != nil {
		unlockSessionRequest()
		return nil, err
	}
	if prepared.upstreamModel != "" && prepared.upstreamModel != baseModel {
		reporter.SetUpstreamModel(prepared.upstreamModel)
	}
	from := prepared.from
	responseFormat := prepared.responseFormat
	to := prepared.to
	preserveNativeOutput := prepared.preserveNativeOutput
	originalPayload := prepared.originalPayload
	clientBody := prepared.clientBody
	wsURL := prepared.wsURL
	wsHeaders := prepared.wsHeaders
	useSIWC := prepared.useSIWC
	replayScope := prepared.replayScope
	optimizeMultiAgentV2 := prepared.optimizeMultiAgentV2
	multiAgentV2Conflict := prepared.multiAgentV2Conflict
	reporter.SetTranslatedReasoningEffort(clientBody, to.String())

	var authLabel, authType, authValue string
	if auth != nil {
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}

	wsReqBody := frameCodexWebsocketRequestBody(clientBody)
	wsReqLog := helps.UpstreamRequestLog{
		URL:       wsURL,
		Method:    "WEBSOCKET",
		Headers:   wsHeaders.Clone(),
		Body:      prepared.sessionInstructions.LogBody(wsReqBody),
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
	requiresConnection := cliproxyexecutor.RequiredUpstreamWebsocket(ctx) || prepared.sessionInstructions.ConnectionBound()
	if requiresConnection {
		conn, closer = existingWebsocketSessionConn(sess, authID, wsURL, executionProxyURL(ctx, sess.effectiveConfig(authID, e.cfg), auth), useSIWC)
		if conn == nil {
			unlockSessionRequest()
			return nil, codexMissingRequiredWebsocketError(prepared.sessionInstructions)
		}
	} else {
		dialCtx = cliproxyexecutor.WithUpstreamAttemptTracker(ctx)
		conn, closer, respHS, upstreamCreated, errDial = e.ensureUpstreamConn(dialCtx, auth, sess, authID, wsURL, wsHeaders, useSIWC)
	}
	var upstreamHeaders http.Header
	if respHS != nil {
		upstreamHeaders = respHS.Header.Clone()
	} else {
		upstreamHeaders = sess.takeHandshakeHeadersForReplay(conn)
	}
	if errDial != nil {
		bodyErr := recordWebsocketDialFailure(ctx, e.cfg, wsReqLog, respHS, errDial)
		if respHS != nil && respHS.StatusCode == http.StatusUpgradeRequired {
			if opts.ExecutionLifecycle == nil && !cliproxyexecutor.DownstreamWebsocket(ctx) {
				unlockSessionRequest()
				return e.CodexExecutor.ExecuteStream(ctx, auth, req, opts)
			}
			if cliproxyexecutor.UpstreamAttempted(dialCtx) {
				cliproxyexecutor.MarkUpstreamAttempt(ctx)
			}
			unlockSessionRequest()
			return nil, statusErr{code: respHS.StatusCode, msg: string(bodyErr)}
		}
		if cliproxyexecutor.UpstreamAttempted(dialCtx) {
			cliproxyexecutor.MarkUpstreamAttempt(ctx)
		}
		if respHS != nil && respHS.StatusCode > 0 {
			unlockSessionRequest()
			return nil, newCodexStatusErrWithHeadersAndCooling(respHS.StatusCode, respHS.Header, bodyErr, e.modelLevelCooling())
		}
		unlockSessionRequest()
		return nil, errDial
	}
	if errConn := prepared.sessionInstructions.OnConnection(conn); errConn != nil {
		unlockSessionRequest()
		return nil, errConn
	}
	if errBind := sess.bindExecutionLifecycle(opts, conn, closer, req.Model); errBind != nil {
		unlockSessionRequest()
		closeWebsocketAfterBindFailure(sess, conn, closer)
		return nil, errBind
	}
	recordAPIWebsocketHandshake(ctx, e.cfg, respHS)
	reporter.StartResponseTTFT()
	if !isEphemeralSession && cliproxyexecutor.DownstreamWebsocket(ctx) && upstreamCreated && codexWebsocketRequestNeedsTranscriptReplayOnReset(wsReqBody) {
		errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "upstream_recreated"}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
		sess.storeHandshakeHeadersForReplay(conn, upstreamHeaders)
		unlockSessionRequest()
		return nil, errReplay
	}

	readCh := sess.activateCodex(conn)
	restoreMultiAgentV2 := !multiAgentV2Conflict && (optimizeMultiAgentV2 || sess.isMultiAgentV2Optimized(conn))

	prepared.sessionReceipt = sess.sessionInstructions.Sent(conn, prepared.sessionInstructions)
	cliproxyexecutor.MarkUpstreamAttempt(ctx)
	if errSend := writeCodexWebsocketMessage(sess, conn, wsReqBody); errSend != nil {
		errSend = mapCodexWebsocketWriteError(sess, conn, errSend)
		helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
		if !isEphemeralSession {
			if !shouldRetryCodexWebsocketSend(errSend) {
				mode := codexDropNotifyDownstream
				if requiresConnection {
					mode = codexDropQuiet
				}
				e.dropUpstreamConn(sess, conn, "send_error", errSend, mode)
				sess.clearActive(conn, readCh)
				unlockSessionRequest()
				return nil, errSend
			}
			e.dropUpstreamConn(sess, conn, "send_error", errSend, codexDropForRecovery)
			sess.clearActive(conn, readCh)
			if requiresConnection {
				unlockSessionRequest()
				return nil, codexMissingRequiredWebsocketError(prepared.sessionInstructions)
			}
			if codexWebsocketRequestNeedsTranscriptReplayOnReset(wsReqBody) {
				errReplay := codexWebsocketTranscriptReplayRequiredError{reason: "send_error", cause: errSend}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_required", errReplay)
				unlockSessionRequest()
				return nil, errReplay
			}

			// Retry once with a new websocket connection for the same execution session.
			connRetry, closerRetry, respHSRetry, _, errDialRetry := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders, useSIWC)
			if errDialRetry != nil || connRetry == nil {
				bodyErrRetry := recordWebsocketDialRetryFailure(ctx, e.cfg, wsReqLog, respHSRetry, errDialRetry)
				if respHSRetry != nil && respHSRetry.StatusCode > 0 {
					errDialRetry = newCodexStatusErrWithHeadersAndCooling(respHSRetry.StatusCode, respHSRetry.Header, bodyErrRetry, e.modelLevelCooling())
				}
				unlockSessionRequest()
				return nil, errDialRetry
			}
			conn = connRetry
			closer = closerRetry
			if errBind := sess.bindExecutionLifecycle(opts, conn, closer, req.Model); errBind != nil {
				unlockSessionRequest()
				closeWebsocketAfterBindFailure(sess, conn, closer)
				return nil, errBind
			}
			readCh = sess.activateCodex(conn)
			restoreMultiAgentV2 = !multiAgentV2Conflict && (optimizeMultiAgentV2 || sess.isMultiAgentV2Optimized(conn))
			helps.RecordAPIWebsocketRequest(ctx, e.cfg, helps.UpstreamRequestLog{
				URL:       wsURL,
				Method:    "WEBSOCKET",
				Headers:   wsHeaders.Clone(),
				Body:      prepared.sessionInstructions.LogBody(wsReqBody),
				Provider:  e.Identifier(),
				AuthID:    authID,
				AuthLabel: authLabel,
				AuthType:  authType,
				AuthValue: authValue,
			})
			recordAPIWebsocketHandshake(ctx, e.cfg, respHSRetry)
			reporter.StartResponseTTFT()
			prepared.sessionReceipt = sess.sessionInstructions.Sent(conn, prepared.sessionInstructions)
			cliproxyexecutor.MarkUpstreamAttempt(ctx)
			if errSendRetry := writeCodexWebsocketMessage(sess, conn, wsReqBody); errSendRetry != nil {
				errSendRetry = mapCodexWebsocketWriteError(sess, conn, errSendRetry)
				helps.RecordAPIWebsocketError(ctx, e.cfg, "send_retry", errSendRetry)
				mode := codexDropNotifyDownstream
				if cliproxyexecutor.DownstreamWebsocket(ctx) {
					mode = codexDropQuiet
				}
				e.dropUpstreamConn(sess, conn, "send_error", errSendRetry, mode)
				sess.clearActive(conn, readCh)
				unlockSessionRequest()
				return nil, errSendRetry
			}
			if respHSRetry != nil {
				upstreamHeaders = respHSRetry.Header.Clone()
			}
		} else {
			e.dropUpstreamConn(sess, conn, "send_error", errSend, codexDropNotifyDownstream)
			sess.clearActive(conn, readCh)
			closeCodexWebsocketSession(sess, "send_error")
			return nil, errSend
		}
	}

	if optimizeMultiAgentV2 || multiAgentV2Conflict {
		sess.setMultiAgentV2Optimized(conn, optimizeMultiAgentV2 && !multiAgentV2Conflict)
	}

	if input := cliproxyexecutor.WebsocketInputFromContext(ctx); input != nil && e.cfg != nil && (e.cfg.Codex.ResponseSteering || e.cfg.CodexResponseSteering) {
		return e.streamCodexDuplex(ctx, auth, req, opts, sess, conn, readCh, input, prepared, reporter, upstreamHeaders, unlockSessionRequest), nil
	}

	buffering := e.cfg != nil && e.cfg.Codex.StreamBootstrapBuffering
	var bootstrapTimeout time.Duration
	var bootstrapStart time.Time
	var exhaustionLogged bool
	if buffering {
		bootstrapTimeout = e.cfg.Codex.StreamBootstrapTimeoutDuration()
		bootstrapStart = nowCodexBootstrap()
	}

	claudeInputTokens := helps.NewClaudeInputTokenState(from, to, responseFormat, originalPayload)
	var param any
	outputItemsByIndex := make(map[int64][]byte)
	var outputItemsFallback [][]byte
	publishTerminalFailure := func(payload []byte, terminalErr error) {
		if detail, ok := helps.ParseCodexUsageWithOutputItems(payload, outputItemsByIndex, outputItemsFallback); ok {
			reporter.PublishFailureWithDetail(ctx, detail, terminalErr)
		} else {
			reporter.PublishFailure(ctx, terminalErr)
		}
		publishCodexImageToolUsageWithOutputItems(ctx, reporter, clientBody, payload, outputItemsByIndex, outputItemsFallback)
	}

	replayableRead := cliproxyexecutor.DownstreamWebsocket(ctx) || codexWebsocketRequestNeedsTranscriptReplayOnReset(wsReqBody)
	var bufferedChunks [][]byte
	// bufferedFrames counts every websocket message read during bootstrap, including the ones the
	// loop skips, so a peer that only sends frames the loop ignores cannot keep the window open.
	bufferedFrames := 0
	bufferedBytes := 0
	var initialChunks [][]byte
	immediateTerminal := false
	immediateIncomplete := false
	var bootstrapTerminalChunk *cliproxyexecutor.StreamChunk
	// bootstrapTerminalErr holds a non-overload terminal failure seen while buffering. It is
	// delivered as an in-stream chunk after the buffered handshake so downstream behaviour stays
	// identical to the unbuffered path instead of silently turning into a credential failover.
	var bootstrapTerminalErr error
	sawOutputDelta := false

	if buffering {
		for {
			if ctx != nil && ctx.Err() != nil {
				sess.clearActive(conn, readCh)
				unlockSessionRequest()
				if isEphemeralSession {
					closeCodexWebsocketSession(sess, "context_done")
				}
				return nil, ctx.Err()
			}
			msgType, payload, errRead := readCodexWebsocketMessage(ctx, sess, conn, readCh)
			if errRead != nil {
				if errReset := codexWebsocketReadResetError(errRead, replayableRead, prepared.sessionInstructions); errReset != nil {
					sess.clearActive(conn, readCh)
					unlockSessionRequest()
					if isEphemeralSession {
						closeCodexWebsocketSession(sess, "read_reset")
					}
					helps.RecordAPIWebsocketError(ctx, e.cfg, "read_reset", errReset)
					reporter.PublishFailure(ctx, errReset)
					return nil, errReset
				}
				mappedErr := mapCodexWebsocketReadError(errRead)
				mode := codexDropNotifyDownstream
				if cliproxyexecutor.DownstreamWebsocket(ctx) {
					mode = codexDropQuiet
				}
				e.dropUpstreamConn(sess, conn, "read_error", mappedErr, mode)
				sess.clearActive(conn, readCh)
				unlockSessionRequest()
				if isEphemeralSession {
					closeCodexWebsocketSession(sess, "read_error")
				}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "read", mappedErr)
				reporter.PublishFailure(ctx, mappedErr)
				return nil, mappedErr
			}
			// Count every message ReadMessage returns, including the ones this loop goes on to skip,
			// so a peer sending only skippable text frames still closes the window. Control frames
			// are not counted: the websocket library answers ping and pong inside ReadMessage and
			// never returns them, so only the read deadline bounds a peer that sends nothing else.
			// windowOpen is carried into the skip branches below rather than breaking here, because
			// this message has not been processed yet and dropping it would lose a token, or a
			// terminal event, from the turn.
			bufferedFrames++
			timeSinceStart := nowCodexBootstrap().Sub(bootstrapStart)
			timeoutReached := bootstrapTimeout > 0 && timeSinceStart >= bootstrapTimeout
			windowOpen := bufferedFrames <= codexBootstrapMaxBufferedFrames && !timeoutReached
			if !windowOpen && !exhaustionLogged {
				exhaustionLogged = true
				exhausted := "frame budget"
				if timeoutReached {
					exhausted = "time budget"
				}
				helps.LogWithRequestID(ctx).Debugf("codex websockets executor: bootstrap %s exhausted after %d messages read / %v; this message will be released", exhausted, bufferedFrames, timeSinceStart)
			}
			if msgType != websocket.TextMessage {
				if msgType == websocket.BinaryMessage {
					errBinary := fmt.Errorf("codex websockets executor: unexpected binary message")
					e.dropUpstreamConn(sess, conn, "unexpected_binary", errBinary, codexDropNotifyDownstream)
					sess.clearActive(conn, readCh)
					unlockSessionRequest()
					if isEphemeralSession {
						closeCodexWebsocketSession(sess, "unexpected_binary")
					}
					helps.RecordAPIWebsocketError(ctx, e.cfg, "unexpected_binary", errBinary)
					reporter.PublishFailure(ctx, errBinary)
					return nil, errBinary
				}
				// No window check here: ReadMessage only ever returns text or binary, and binary
				// returned just above, so nothing reaches this line. The empty-payload skip below
				// is the reachable one and does consult the window.
				continue
			}

			payload = bytes.TrimSpace(payload)
			if len(payload) == 0 {
				if !windowOpen {
					break
				}
				continue
			}
			observeCodexTokenEvent(reporter, payload)
			helps.AppendCodexAPIWebsocketResponse(ctx, e.cfg, payload)
			helps.EmitWebSocketResponseEvent(ctx, opts, auth, e.Identifier(), req.Model, payload)
			payload = helps.RestoreCodexMultiAgentV2Response(payload, restoreMultiAgentV2)

			if helps.HasMeaningfulCodexOutputDelta(payload) {
				sawOutputDelta = true
			}
			if helps.IsCodexTerminalEmptyIncomplete(payload, len(outputItemsByIndex)+len(outputItemsFallback), sawOutputDelta) {
				streamErr := newCodexEmptyIncompleteStreamError()
				helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", streamErr)
				publishTerminalFailure(payload, streamErr)
				mode := codexDropNotifyDownstream
				if cliproxyexecutor.DownstreamWebsocket(ctx) {
					mode = codexDropQuiet
				}
				e.dropUpstreamConn(sess, conn, "terminal_empty_incomplete", streamErr, mode)
				sess.clearActive(conn, readCh)
				unlockSessionRequest()
				bootstrapTerminalErr = streamErr
				break
			}

			if failure, ok := parseCodexWebsocketTerminalFailure(ctx, payload, e.modelLevelCooling()); ok {
				// A capacity or quota rejection is retried on another credential, so the
				// downstream websocket session must survive this upstream teardown. Notifying
				// the disconnect here would close the client connection before the retry can
				// deliver anything. Downstream websocket failures also stay quiet because
				// their original terminal payload is forwarded before the session ends.
				overloadFailover := isCodexOverloadBootstrapFailure(failure.body)
				quotaFailover := isCodexUsageLimitError(failure.body) || isCodexBillingQuotaError(failure.body)
				failoverPending := !failure.replayRequired && (overloadFailover || quotaFailover) && !timeoutReached
				if failure.replayRequired {
					e.dropUpstreamConn(sess, conn, "transcript_replay", failure.err, codexDropForRecovery)
				} else if failoverPending || cliproxyexecutor.DownstreamWebsocket(ctx) {
					e.dropUpstreamConn(sess, conn, failure.reason, failure.err, codexDropQuiet)
				} else if reason := codexWebsocketUpstreamErrorDropReason(payload, failure.rawErr); reason != "" {
					e.dropUpstreamConn(sess, conn, reason, failure.rawErr, codexDropQuiet)
				} else {
					e.dropUpstreamConn(sess, conn, failure.reason, failure.err, codexDropNotifyDownstream)
				}
				sess.clearActive(conn, readCh)
				unlockSessionRequest()
				var errClearReplay error
				if failure.isWebsocketError {
					errClearReplay = clearCodexReasoningReplayOnWebsocketError(ctx, replayScope, payload)
				} else {
					errClearReplay = clearCodexReasoningReplayOnInvalidSignature(ctx, replayScope, codexWebsocketErrorStatusCode(failure.rawErr), failure.body)
				}
				if errClearReplay != nil {
					if isEphemeralSession {
						closeCodexWebsocketSession(sess, "replay_clear_error")
					}
					helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_clear_error", errClearReplay)
					reporter.PublishFailure(ctx, errClearReplay)
					return nil, errClearReplay
				}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", failure.err)
				publishTerminalFailure(payload, failure.err)
				if failoverPending {
					if isEphemeralSession {
						closeCodexWebsocketSession(sess, "bootstrap_failover")
					}
					// Fail the attempt before the downstream headers are committed so the
					// conductor can transparently retry on another credential.
					helps.LogWithRequestID(ctx).Debugf("codex websockets executor: bootstrap rejection after %d messages read, failing over", bufferedFrames)
					if failure.isWebsocketError || quotaFailover {
						return nil, failure.rawErr
					}
					if overloadFailover {
						var retryAfter *time.Duration
						if ra, ok := failure.rawErr.(interface{ RetryAfter() *time.Duration }); ok {
							retryAfter = ra.RetryAfter()
						}
						if codexWebsocketErrorStatusCode(failure.rawErr) == http.StatusTooManyRequests && retryAfter != nil {
							return nil, failure.rawErr
						}
						return nil, newCodexBootstrapOverloadErrWithRetryAfter(failure.body, retryAfter)
					}
					return nil, failure.rawErr
				}
				if cliproxyexecutor.DownstreamWebsocket(ctx) {
					bootstrapTerminalChunk = &cliproxyexecutor.StreamChunk{
						Payload:   helps.EnsureResponsesUsageDetails(payload),
						ResultErr: failure.err,
					}
				} else {
					bootstrapTerminalErr = failure.rawErr
				}
				break
			}

			prepared.sessionReceipt.Observe(payload)
			eventType := ojson.GetBytesNoCopy(payload, "type").String()
			isTerminalEvent := eventType == "response.completed" || eventType == "response.done" || eventType == "response.incomplete" || eventType == "response.failed" || eventType == "error"
			if eventType == "response.output_item.done" {
				collectCodexOutputItemDone(payload, outputItemsByIndex, &outputItemsFallback)
			}
			completedPayload := payload
			if eventType == "response.completed" || eventType == "response.done" || eventType == "response.incomplete" {
				completedPayload = normalizeCodexWebsocketCompletion(completedPayload)
				if !preserveNativeOutput {
					completedPayload = patchCodexCompletedOutput(completedPayload, outputItemsByIndex, outputItemsFallback)
				}
				if eventType == "response.completed" || eventType == "response.done" {
					cacheCodexReasoningReplayFromCompleted(replayScope, completedPayload)
				}
				if detail, ok := helps.ParseCodexUsageWithOutputItems(completedPayload, outputItemsByIndex, outputItemsFallback); ok {
					reporter.Publish(ctx, detail)
				} else {
					reporter.EnsurePublished(ctx)
				}
				publishCodexImageToolUsageWithOutputItems(ctx, reporter, clientBody, completedPayload, outputItemsByIndex, outputItemsFallback)
			}

			var currentChunks [][]byte
			if cliproxyexecutor.DownstreamWebsocket(ctx) {
				if eventType == "response.completed" || eventType == "response.done" || eventType == "response.incomplete" {
					payload = completedPayload
				}
				downstreamPayload := helps.EnsureResponsesUsageDetails(payload)
				currentChunks = [][]byte{downstreamPayload}
			} else {
				payload = normalizeCodexWebsocketCompletion(payload)
				if eventType == "response.completed" || eventType == "response.done" || eventType == "response.incomplete" {
					payload = completedPayload
				}
				line := encodeCodexWebsocketAsSSE(payload)
				currentChunks = helps.TranslateStreamWithClaudeInputTokens(ctx, to, responseFormat, req.Model, originalPayload, clientBody, line, &param, claudeInputTokens)
			}

			// !isTerminalEvent is redundant against the closed allow-list, which admits no terminal
			// type, and the empty-payload rule cannot fire on a payload already known non-empty. It
			// stays as the guard a reader expects to find, and its SSE counterpart is !terminalSuccess.
			if windowOpen && isCodexBootstrapBufferableEvent(eventType, payload) && !isTerminalEvent {
				frameBytes := len(payload)
				for i := range currentChunks {
					frameBytes += len(currentChunks[i])
				}
				if bufferedBytes+frameBytes <= codexBootstrapMaxBufferedBytes {
					bufferedBytes += frameBytes
					bufferedChunks = append(bufferedChunks, currentChunks...)
					continue
				}
				helps.LogWithRequestID(ctx).Debugf("codex websockets executor: bootstrap byte limit reached after %d messages / %d bytes, releasing stream without overload probing", bufferedFrames, bufferedBytes)
			}

			initialChunks = currentChunks
			if isTerminalEvent {
				immediateTerminal = true
				immediateIncomplete = eventType == "response.incomplete"
			}
			break
		}
	}

	chanCapacity := len(bufferedChunks) + len(initialChunks)
	if bootstrapTerminalChunk != nil {
		chanCapacity++
	}
	if bootstrapTerminalErr != nil || (immediateIncomplete && len(initialChunks) == 0) {
		chanCapacity++
	}
	out := make(chan cliproxyexecutor.StreamChunk, chanCapacity)
	for _, chunk := range bufferedChunks {
		out <- cliproxyexecutor.StreamChunk{Payload: chunk}
	}
	for _, chunk := range initialChunks {
		out <- cliproxyexecutor.StreamChunk{Payload: chunk, Incomplete: immediateIncomplete}
	}
	if immediateIncomplete && len(initialChunks) == 0 {
		out <- cliproxyexecutor.StreamChunk{Incomplete: true}
	}
	if bootstrapTerminalChunk != nil {
		if isEphemeralSession {
			closeCodexWebsocketSession(sess, "bootstrap_terminal_chunk")
		}
		out <- *bootstrapTerminalChunk
		close(out)
		return &cliproxyexecutor.StreamResult{Headers: upstreamHeaders, Chunks: out}, nil
	}
	if bootstrapTerminalErr != nil {
		if isEphemeralSession {
			closeCodexWebsocketSession(sess, "bootstrap_terminal_error")
		}
		// The upstream connection was already invalidated and released in the terminal-failure
		// branch above, so only the buffered payloads plus the in-stream error remain to emit.
		out <- cliproxyexecutor.StreamChunk{Err: bootstrapTerminalErr}
		close(out)
		return &cliproxyexecutor.StreamResult{Headers: upstreamHeaders, Chunks: out}, nil
	}
	if immediateTerminal {
		sess.clearActive(conn, readCh)
		unlockSessionRequest()
		if isEphemeralSession {
			closeCodexWebsocketSession(sess, "completed")
		}
		close(out)
		return &cliproxyexecutor.StreamResult{Headers: upstreamHeaders, Chunks: out}, nil
	}

	go func() {
		terminateReason := "completed"

		defer close(out)
		defer func() {
			sess.clearActive(conn, readCh)
			unlockSessionRequest()
			if isEphemeralSession {
				closeCodexWebsocketSession(sess, terminateReason)
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
			mode := codexDropNotifyDownstream
			if cliproxyexecutor.DownstreamWebsocket(ctx) {
				mode = codexDropQuiet
			}
			e.dropUpstreamConn(sess, conn, reason, terminalErr, mode)
			unlockSessionRequest()
		}

		for {
			if ctx != nil && ctx.Err() != nil {
				terminateReason = "context_done"
				_ = send(cliproxyexecutor.StreamChunk{Err: ctx.Err()})
				return
			}
			msgType, payload, errRead := readCodexWebsocketMessage(ctx, sess, conn, readCh)
			if errRead != nil {
				if ctx != nil && ctx.Err() != nil {
					terminateReason = "context_done"
					_ = send(cliproxyexecutor.StreamChunk{Err: ctx.Err()})
					return
				}
				terminateReason = "read_error"
				if errReset := codexWebsocketReadResetError(errRead, replayableRead, prepared.sessionInstructions); errReset != nil {
					helps.RecordAPIWebsocketError(ctx, e.cfg, "read_reset", errReset)
					reporter.PublishFailure(ctx, errReset)
					_ = send(cliproxyexecutor.StreamChunk{Err: errReset})
					return
				}
				mappedErr := mapCodexWebsocketReadError(errRead)
				var upstreamReset codexWebsocketUpstreamResetError
				if !errors.As(errRead, &upstreamReset) {
					defer func() {
						dropTerminalConnection("read_error", mappedErr)
					}()
				}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "read", mappedErr)
				reporter.PublishFailure(ctx, mappedErr)
				_ = send(cliproxyexecutor.StreamChunk{Err: mappedErr})
				return
			}
			if msgType != websocket.TextMessage {
				if msgType == websocket.BinaryMessage {
					errBinary := fmt.Errorf("codex websockets executor: unexpected binary message")
					terminateReason = "unexpected_binary"
					helps.RecordAPIWebsocketError(ctx, e.cfg, "unexpected_binary", errBinary)
					reporter.PublishFailure(ctx, errBinary)
					defer func() {
						dropTerminalConnection("unexpected_binary", errBinary)
					}()
					_ = send(cliproxyexecutor.StreamChunk{Err: errBinary})
					return
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
				terminateReason = failure.reason
				// Deliver the error before disconnecting so cancellation preserves cooldown accounting.
				defer func() {
					if failure.replayRequired {
						e.dropUpstreamConn(sess, conn, "transcript_replay", failure.err, codexDropForRecovery)
					} else if cliproxyexecutor.DownstreamWebsocket(ctx) {
						e.dropUpstreamConn(sess, conn, failure.reason, failure.rawErr, codexDropQuiet)
					} else if reason := codexWebsocketUpstreamErrorDropReason(payload, failure.rawErr); reason != "" {
						e.dropUpstreamConn(sess, conn, reason, failure.rawErr, codexDropQuiet)
					} else {
						e.dropUpstreamConn(sess, conn, failure.reason, failure.rawErr, codexDropNotifyDownstream)
					}
					unlockSessionRequest()
				}()
				var errClearReplay error
				if failure.isWebsocketError {
					errClearReplay = clearCodexReasoningReplayOnWebsocketError(ctx, replayScope, payload)
				} else {
					errClearReplay = clearCodexReasoningReplayOnInvalidSignature(ctx, replayScope, codexWebsocketErrorStatusCode(failure.rawErr), failure.body)
				}
				if errClearReplay != nil {
					helps.RecordAPIWebsocketError(ctx, e.cfg, "replay_clear_error", errClearReplay)
					reporter.PublishFailure(ctx, errClearReplay)
					_ = send(cliproxyexecutor.StreamChunk{Err: errClearReplay})
					return
				}
				helps.RecordAPIWebsocketError(ctx, e.cfg, failure.reason, failure.err)
				publishTerminalFailure(payload, failure.err)
				if cliproxyexecutor.DownstreamWebsocket(ctx) {
					_ = send(cliproxyexecutor.StreamChunk{Payload: helps.EnsureResponsesUsageDetails(payload), ResultErr: failure.err})
					return
				}
				_ = send(cliproxyexecutor.StreamChunk{Err: failure.rawErr})
				return
			}

			prepared.sessionReceipt.Observe(payload)
			eventType := ojson.GetBytesNoCopy(payload, "type").String()
			if helps.HasMeaningfulCodexOutputDelta(payload) {
				sawOutputDelta = true
			}
			if helps.IsCodexTerminalEmptyIncomplete(payload, len(outputItemsByIndex)+len(outputItemsFallback), sawOutputDelta) {
				streamErr := newCodexEmptyIncompleteStreamError()
				helps.RecordAPIResponseError(ctx, e.cfg, streamErr)
				publishTerminalFailure(payload, streamErr)
				defer dropTerminalConnection("terminal_empty_incomplete", streamErr)
				_ = send(cliproxyexecutor.StreamChunk{Err: streamErr})
				terminateReason = "terminal_empty_incomplete"
				return
			}
			if eventType == "response.output_item.done" {
				collectCodexOutputItemDone(payload, outputItemsByIndex, &outputItemsFallback)
			}
			completedPayload := payload
			if eventType == "response.completed" || eventType == "response.done" || eventType == "response.incomplete" {
				completedPayload = normalizeCodexWebsocketCompletion(completedPayload)
				if !preserveNativeOutput {
					completedPayload = patchCodexCompletedOutput(completedPayload, outputItemsByIndex, outputItemsFallback)
				}
				if eventType == "response.completed" || eventType == "response.done" {
					cacheCodexReasoningReplayFromCompleted(replayScope, completedPayload)
				}
				if detail, ok := helps.ParseCodexUsageWithOutputItems(completedPayload, outputItemsByIndex, outputItemsFallback); ok {
					reporter.Publish(ctx, detail)
				} else {
					reporter.EnsurePublished(ctx)
				}
				publishCodexImageToolUsageWithOutputItems(ctx, reporter, clientBody, completedPayload, outputItemsByIndex, outputItemsFallback)
			}

			isTerminalEvent := eventType == "response.completed" || eventType == "response.done" || eventType == "response.incomplete" || eventType == "error"
			if cliproxyexecutor.DownstreamWebsocket(ctx) {
				clientPayload := payload
				if eventType == "response.completed" || eventType == "response.done" || eventType == "response.incomplete" {
					clientPayload = completedPayload
				}
				chunk := cliproxyexecutor.StreamChunk{Payload: helps.EnsureResponsesUsageDetails(clientPayload), Incomplete: eventType == "response.incomplete"}
				if !send(chunk) {
					terminateReason = "context_done"
					return
				}
				if isTerminalEvent {
					return
				}
				continue
			}

			payload = normalizeCodexWebsocketCompletion(payload)
			if eventType == "response.completed" || eventType == "response.done" || eventType == "response.incomplete" {
				payload = completedPayload
			}
			eventType = ojson.GetBytesNoCopy(payload, "type").String()
			isIncomplete := eventType == "response.incomplete"
			line := encodeCodexWebsocketAsSSE(payload)
			chunks := helps.TranslateStreamWithClaudeInputTokens(ctx, to, responseFormat, req.Model, originalPayload, clientBody, line, &param, claudeInputTokens)
			for i := range chunks {
				if !send(cliproxyexecutor.StreamChunk{Payload: chunks[i], Incomplete: isIncomplete}) {
					terminateReason = "context_done"
					return
				}
			}
			if isIncomplete && len(chunks) == 0 {
				if !send(cliproxyexecutor.StreamChunk{Incomplete: true}) {
					terminateReason = "context_done"
					return
				}
			}
			if isTerminalEvent || eventType == "response.completed" || eventType == "response.done" || eventType == "response.incomplete" {
				return
			}
		}
	}()

	return &cliproxyexecutor.StreamResult{Headers: upstreamHeaders, Chunks: out}, nil
}

// codexWebsocketPrepared contains the request pipeline output shared by the
// ordinary per-response stream and subsequent creates on a duplex connection.
type codexWebsocketPrepared struct {
	from                 sdktranslator.Format
	responseFormat       sdktranslator.Format
	to                   sdktranslator.Format
	preserveNativeOutput bool
	originalPayload      []byte
	clientBody           []byte
	upstreamModel        string
	wsURL                string
	wsHeaders            http.Header
	useSIWC              bool
	replayScope          codexReasoningReplayScope
	optimizeMultiAgentV2 bool
	multiAgentV2Conflict bool
	sessionInstructions  helps.CodexInstructionTurn
	sessionReceipt       helps.CodexInstructionReceipt

	// Immutable pre-payload JSON value for duplex inheritance; never rebuild from clientBody.
	// An empty Go string means absent, not an explicitly empty JSON string.
	unconfiguredInstructions string
}

func (e *CodexWebsocketsExecutor) prepareCodexWebsocketStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, sess *codexWebsocketSession) (*codexWebsocketPrepared, error) {
	if errPrev := rejectSyntheticCompactionPreviousResponseID(ctx, req.Payload); errPrev != nil {
		return nil, errPrev
	}
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	isCompat, errExpand := e.expandCodexResponsesCompaction(auth, &req, &opts, baseModel)
	if errExpand != nil {
		return nil, statusErr{code: http.StatusBadRequest, msg: errExpand.Error()}
	}
	apiKey, baseURL := codexCredsForRequest(ctx, auth, opts.Headers)
	if errSIWC := ensureCodexSIWCCredentialsForRequest(ctx, auth, apiKey, opts.Headers); errSIWC != nil {
		return nil, errSIWC
	}
	useSIWC := codexUseSIWCForRequest(ctx, auth, opts.Headers)
	if baseURL == "" {
		if useSIWC {
			baseURL = defaultOpenAISIWCBaseURL
		} else {
			baseURL = "https://chatgpt.com/backend-api/codex"
		}
	}
	var err error
	from := opts.SourceFormat
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	preserveNativeOutput := helps.IsNativeCodexRequest(req.Payload, opts)
	to := sdktranslator.FromString("codex")
	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalPayload := originalPayloadSource
	targetModel := resolveResponsesSignatureTargetModel(baseModel, req, opts)
	originalTranslated, body, updatesChanged, err := translateCodexRequestPairWithUpdateIntentContext(ctx, from, to, targetModel, originalPayload, req.Payload, true, isCompat)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	instructionsResult := ojson.GetBytesNoCopy(req.Payload, "instructions")
	hasExplicitInstructions := instructionsResult.Exists() && strings.TrimSpace(instructionsResult.String()) != ""
	ctx = helps.WithPayloadFinalizer(ctx, helps.NewPayloadFinalizer(e.cfg, "codex-websockets", baseModel, to.String(), "", originalTranslated, req, opts, auth))
	helps.SetStringIfDifferentDoc(doc, "model", baseModel)
	ensureWebSearchToolForHistoryDoc(doc, opts.Headers)
	// Do not let a built-in empty default become caller-provided on the next turn.
	hadUnconfiguredInstructions := doc.Get("instructions").Exists()
	responsesLite := normalizeCodexResponsesLiteRequestDoc(doc, opts.Headers)
	if !useSIWC {
		if !responsesLite {
			normalizeCodexInstructionsDoc(doc, preserveNativeOutput)
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
		return nil, errReplay
	}
	sanitizeOpenAIResponsesReasoningEncryptedContentWithCompatModelAndBaseURLDoc(ctx, "codex websockets executor", doc, isCompat, targetModel, baseURL)

	httpURL := strings.TrimSuffix(baseURL, "/") + "/responses"
	wsURL, err := buildCodexResponsesWebsocketURL(httpURL)
	if err != nil {
		return nil, err
	}

	wsHeaders, errPromptCache := applyCodexPromptCacheHeadersWithContextDoc(ctx, from, req, doc, opts.Headers)
	if errPromptCache != nil {
		return nil, errPromptCache
	}
	helps.SanitizeCodexInputItemIDsDoc(doc)
	// Retain explicit clearing intent before SIWC removes empty instructions.
	// The outbound body still follows SIWC cleanup; only inheritance keeps the value.
	var unconfiguredInstructions string
	if hadUnconfiguredInstructions {
		unconfiguredInstructions = strings.Clone(doc.Get("instructions").Raw)
	}
	if useSIWC {
		applyCodexSIWCWebsocketBodyRulesDoc(doc, hasExplicitInstructions, cliproxyexecutor.Options{KeepTopLevelEffort: thinking.HasThinkingSuffix(req.Model)})
	}
	sessionInstructions, errSession := e.prepareSessionInstructions(ctx, doc, auth, req, opts, sess, wsURL, useSIWC)
	if errSession != nil {
		return nil, errSession
	}
	helps.FinalizePayloadDoc(ctx, doc)
	body = doc.Bytes()
	if errMarker := helps.ValidateCodexSessionInstructionMarker(body, from); errMarker != nil {
		return nil, errMarker
	}
	if errFinal := sessionInstructions.CheckFinal(body); errFinal != nil {
		return nil, errFinal
	}
	upstreamModel := strings.TrimSpace(doc.Get("model").String())
	wsHeaders = applyCodexWebsocketHeaders(ctx, wsHeaders, auth, apiKey, e.cfg, preserveNativeOutput, opts.Headers)
	applyCodexRoutingHint(ctx, wsHeaders, auth, upstreamModel, body, opts.Headers)
	clientHeaders := util.ResolveClientHeaders((&http.Request{}).WithContext(ctx), opts.Headers)
	finalizeCodexWebsocketHeaders(ctx, wsHeaders, auth, useSIWC, body, upstreamModel, clientHeaders)
	if errValidate := helps.ValidateCodexHostedImageGenerationDoc(doc, wsHeaders, e.cfg, !useSIWC && !codexAuthUsesAPIKey(auth)); errValidate != nil {
		return nil, errValidate
	}

	return &codexWebsocketPrepared{
		from:                 from,
		responseFormat:       responseFormat,
		to:                   to,
		preserveNativeOutput: preserveNativeOutput,
		originalPayload:      originalPayload,
		clientBody:           body,
		upstreamModel:        upstreamModel,
		wsURL:                wsURL,
		wsHeaders:            wsHeaders,
		useSIWC:              useSIWC,
		replayScope:          replayScope,
		optimizeMultiAgentV2: optimizeMultiAgentV2,
		multiAgentV2Conflict: multiAgentV2Conflict,
		sessionInstructions:  sessionInstructions,

		unconfiguredInstructions: unconfiguredInstructions,
	}, nil
}
