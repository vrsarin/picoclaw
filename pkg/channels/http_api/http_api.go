package http_api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
)

const (
	defaultPort    = 18791
	idleTimeout    = 2 * time.Second
	maxWaitTimeout = 120 * time.Second
)

// HTTPAPIChannel exposes PicoClaw's agent engine over HTTP REST.
//
// Two-step OpenClaw protocol (matches openclaw-adapter exactly):
//
//	POST /agent        — start session: {sessionKey, message, metadata, allowedTools} → {runId, acceptedAt}
//	POST /agent.wait   — poll result:   {runId, timeoutMs}  → {status: ok|timeout|error, ...}
//	POST /agent.cancel — cancel run:    {runId}
//
// OpenAI-compatible (for LibreChat):
//
//	POST /v1/chat/completions
//	GET  /v1/models
//	GET  /health
type HTTPAPIChannel struct {
	*channels.BaseChannel
	cfg        *config.HTTPAPISettings
	bus        *bus.MessageBus
	server     *http.Server
	modelNames []string
	// pending maps runId → *pendingRequest for in-flight sessions.
	pending sync.Map
	ctx     context.Context
	cancel  context.CancelFunc
}

// pendingRequest tracks one in-flight agent session.
// The idle timer is not started until the first Send() call so a slow agent
// does not trigger a premature close. Each subsequent Send() resets the timer.
// Once the timer fires (idleTimeout after the last Send()), done is closed
// and the accumulated content is available.
type pendingRequest struct {
	mu      sync.Mutex
	content string
	phase   string      // stored for response parsing
	timer   *time.Timer // nil until first Send()
	done    chan struct{}
	closed  atomic.Bool
}

func newPendingRequest(phase string) *pendingRequest {
	return &pendingRequest{
		done:  make(chan struct{}),
		phase: phase,
	}
}

// update stores the latest content from the agent and (re)starts the idle timer.
// PicoClaw may call Send() multiple times with incremental full-content updates;
// we keep only the last one.
func (pr *pendingRequest) update(content string) {
	pr.mu.Lock()
	pr.content = content
	if pr.timer == nil {
		pr.timer = time.AfterFunc(idleTimeout, pr.close)
	} else {
		pr.timer.Reset(idleTimeout)
	}
	pr.mu.Unlock()
}

func (pr *pendingRequest) close() {
	if pr.closed.CompareAndSwap(false, true) {
		pr.mu.Lock()
		if pr.timer != nil {
			pr.timer.Stop()
		}
		pr.mu.Unlock()
		close(pr.done)
	}
}

// --- Request / response DTOs (field names match openclaw-adapter exactly) ---

type agentStartRequest struct {
	SessionKey   string            `json:"sessionKey"`
	Message      string            `json:"message"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	AllowedTools []string          `json:"allowedTools,omitempty"`
}

type agentAccepted struct {
	RunID      string    `json:"runId"`
	AcceptedAt time.Time `json:"acceptedAt"`
}

type agentPollRequest struct {
	RunID     string `json:"runId"`
	TimeoutMs int    `json:"timeoutMs"`
}

// agentPollResult field names match openclaw-adapter AgentWaitResult exactly.
type agentPollResult struct {
	Status    string            `json:"status"`
	Error     string            `json:"error,omitempty"`
	Summary   string            `json:"summary,omitempty"`
	Outcome   string            `json:"outcome,omitempty"`
	Artifacts map[string]string `json:"artifacts,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type sseDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

type sseChoice struct {
	Index        int      `json:"index"`
	Delta        sseDelta `json:"delta"`
	FinishReason *string  `json:"finish_reason"`
}

type sseChunk struct {
	ID      string      `json:"id"`
	Object  string      `json:"object"`
	Model   string      `json:"model"`
	Choices []sseChoice `json:"choices"`
}

type chatCompletionRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
}

type chatCompletionResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Model   string       `json:"model"`
	Choices []chatChoice `json:"choices"`
}

type chatChoice struct {
	Index   int         `json:"index"`
	Message chatMessage `json:"message"`
	Reason  string      `json:"finish_reason"`
}

// --- Channel implementation ---

func NewHTTPAPIChannel(
	bc *config.Channel,
	cfg *config.HTTPAPISettings,
	b *bus.MessageBus,
	modelNames []string,
) (*HTTPAPIChannel, error) {
	port := cfg.Port
	if port == 0 {
		port = defaultPort
	}
	host := cfg.Host
	if host == "" {
		host = "0.0.0.0"
	}

	ch := &HTTPAPIChannel{
		BaseChannel: channels.NewBaseChannel(
			config.ChannelHTTPAPI,
			cfg,
			b,
			bc.AllowFrom,
			channels.WithReasoningChannelID(bc.ReasoningChannelID),
		),
		cfg:        cfg,
		bus:        b,
		modelNames: modelNames,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /agent", ch.handleAgent)
	mux.HandleFunc("POST /agent.wait", ch.handleAgentWait)
	mux.HandleFunc("POST /agent.cancel", ch.handleAgentCancel)
	mux.HandleFunc("POST /v1/chat/completions", ch.handleChatCompletions)
	mux.HandleFunc("GET /v1/models", ch.handleModels)
	mux.HandleFunc("GET /health", ch.handleHealth)

	ch.server = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", host, port),
		Handler: ch.authMiddleware(mux),
	}

	return ch, nil
}

func (c *HTTPAPIChannel) authMiddleware(next http.Handler) http.Handler {
	token := c.cfg.Token.String()
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (c *HTTPAPIChannel) Start(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.SetRunning(true)

	logger.InfoCF("http_api", "HTTP API channel listening", map[string]any{
		"addr": c.server.Addr,
	})

	go func() {
		if err := c.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.ErrorCF("http_api", "HTTP server error", map[string]any{"error": err})
		}
	}()

	go func() {
		<-c.ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.server.Shutdown(shutCtx)
		c.SetRunning(false)
	}()

	return nil
}

func (c *HTTPAPIChannel) Stop(ctx context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	return nil
}

// Send is called by the channel Manager when the agent produces output for a
// session that originated from this channel. Multiple calls per turn deliver
// incremental full-content updates; we keep the latest.
func (c *HTTPAPIChannel) Send(_ context.Context, msg bus.OutboundMessage) ([]string, error) {
	v, ok := c.pending.Load(msg.ChatID)
	if !ok {
		return nil, nil
	}
	v.(*pendingRequest).update(msg.Content)
	return nil, nil
}

// --- Handlers ---

// POST /agent — start an agent session.
// The sessionKey is used as the runId AND as the PicoClaw chatID so that session
// state is maintained across the planning/implementation/review/release phases.
func (c *HTTPAPIChannel) handleAgent(w http.ResponseWriter, r *http.Request) {
	var req agentStartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	runID := req.SessionKey
	if runID == "" {
		runID = uuid.New().String()
	}

	phase := req.Metadata["phase"]

	// Overwrite any stale pending for this runID (handles Temporal activity retries).
	if old, loaded := c.pending.LoadAndDelete(runID); loaded {
		old.(*pendingRequest).close()
	}
	pr := newPendingRequest(phase)
	c.pending.Store(runID, pr)

	if err := c.publish(r.Context(), runID, req.Message, req.Metadata); err != nil {
		c.pending.Delete(runID)
		pr.close()
		http.Error(w, "failed to dispatch", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusAccepted, agentAccepted{
		RunID:      runID,
		AcceptedAt: time.Now().UTC(),
	})
}

// POST /agent.wait — poll for the result of a previously started session.
// Returns {status: "timeout"} when timeoutMs elapses so the caller can retry;
// returns {status: "ok"} with parsed output once the agent finishes.
func (c *HTTPAPIChannel) handleAgentWait(w http.ResponseWriter, r *http.Request) {
	var req agentPollRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	v, ok := c.pending.Load(req.RunID)
	if !ok {
		writeJSON(w, http.StatusOK, agentPollResult{
			Status: "error",
			Error:  "run not found: " + req.RunID,
		})
		return
	}
	pr := v.(*pendingRequest)

	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	if timeout <= 0 || timeout > maxWaitTimeout {
		timeout = maxWaitTimeout
	}

	waitCtx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	select {
	case <-pr.done:
		pr.mu.Lock()
		content := pr.content
		phase := pr.phase
		pr.mu.Unlock()
		c.pending.Delete(req.RunID)

		summary, outcome, artifacts := parseAgentResponse(content, phase)
		writeJSON(w, http.StatusOK, agentPollResult{
			Status:    "ok",
			Summary:   summary,
			Outcome:   outcome,
			Artifacts: artifacts,
		})

	case <-waitCtx.Done():
		// Return "timeout" without deleting the pending entry so the caller can retry.
		writeJSON(w, http.StatusOK, agentPollResult{Status: "timeout"})
	}
}

// POST /agent.cancel — cancel an in-flight session.
func (c *HTTPAPIChannel) handleAgentCancel(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RunID string `json:"runId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if v, ok := c.pending.LoadAndDelete(req.RunID); ok {
		v.(*pendingRequest).close()
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

// POST /v1/chat/completions — OpenAI-compatible endpoint for LibreChat.
// Checks OPA model access, then proxies the full request to LiteLLM verbatim.
// The model field from the request is used as-is so LibreChat model selection works.
// Streaming responses are forwarded transparently (SSE pass-through).
func (c *HTTPAPIChannel) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	var req chatCompletionRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	model := req.Model
	if model == "" {
		model = "qwen3-thinking"
	}

	// OPA model-access check (skipped when OpaURL is not configured).
	if opaURL := strings.TrimRight(c.cfg.OpaURL, "/"); opaURL != "" {
		roles := rolesFromHeader(r)
		allowed, reason := checkModelAccess(r.Context(), opaURL, model, roles)
		if !allowed {
			writeJSON(w, http.StatusForbidden, map[string]string{
				"error":  "model access denied",
				"reason": reason,
			})
			return
		}
	}

	// Proxy to LiteLLM — forward the request body unmodified so the model name,
	// messages, stream flag, and any extra parameters reach LiteLLM unchanged.
	src := strings.TrimRight(c.cfg.ModelsSourceURL, "/")
	if src == "" {
		http.Error(w, "no LiteLLM backend configured", http.StatusServiceUnavailable)
		return
	}

	proxyReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
		src+"/v1/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		http.Error(w, "failed to build proxy request", http.StatusInternalServerError)
		return
	}
	proxyReq.Header.Set("Content-Type", "application/json")
	if key := os.Getenv("LITELLM_MASTER_KEY"); key != "" {
		proxyReq.Header.Set("Authorization", "Bearer "+key)
	}

	resp, err := http.DefaultClient.Do(proxyReq)
	if err != nil {
		http.Error(w, "LiteLLM request failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// GET /v1/models — proxies ModelsSourceURL (e.g. litellm) when configured,
// falling back to the static model list from config.
func (c *HTTPAPIChannel) handleModels(w http.ResponseWriter, r *http.Request) {
	if src := strings.TrimRight(c.cfg.ModelsSourceURL, "/"); src != "" {
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, src+"/v1/models", nil)
		if err == nil {
			if key := os.Getenv("LITELLM_MASTER_KEY"); key != "" {
				req.Header.Set("Authorization", "Bearer "+key)
			}
			if resp, err := http.DefaultClient.Do(req); err == nil {
				defer resp.Body.Close()
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(resp.StatusCode)
				_, _ = io.Copy(w, resp.Body)
				return
			}
		}
		logger.WarnCF("http_api", "models proxy failed, falling back to config list",
			map[string]any{"url": src, "error": err})
	}

	data := make([]map[string]string, 0, len(c.modelNames))
	for _, name := range c.modelNames {
		data = append(data, map[string]string{"id": name, "object": "model"})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   data,
	})
}

// GET /health
func (c *HTTPAPIChannel) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// publish places an InboundMessage on the bus. chatID (= sessionKey = runId)
// gives PicoClaw a stable session identity across retry calls for the same phase.
func (c *HTTPAPIChannel) publish(ctx context.Context, chatID, content string, metadata map[string]string) error {
	raw := make(map[string]string, len(metadata))
	for k, v := range metadata {
		raw[k] = v
	}
	return c.bus.PublishInbound(ctx, bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:  config.ChannelHTTPAPI,
			ChatID:   chatID,
			SenderID: "http-caller",
			Raw:      raw,
		},
		Sender: bus.SenderInfo{
			Platform:    config.ChannelHTTPAPI,
			PlatformID:  "http-caller",
			CanonicalID: config.ChannelHTTPAPI + ":http-caller",
			DisplayName: "HTTP API",
		},
		Content:    content,
		SessionKey: chatID,
		Channel:    config.ChannelHTTPAPI,
		SenderID:   "http-caller",
		ChatID:     chatID,
	})
}

// writeSSE emits an OpenAI-compatible SSE stream: role chunk → content chunk → [DONE].
// LangChain.js and LibreChat both require streaming format when stream:true is sent.
func writeSSE(w http.ResponseWriter, id, model, content string) {
	flusher, ok := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	emit := func(delta sseDelta, finish *string) {
		chunk := sseChunk{
			ID:     "chatcmpl-" + id,
			Object: "chat.completion.chunk",
			Model:  model,
			Choices: []sseChoice{{
				Index:        0,
				Delta:        delta,
				FinishReason: finish,
			}},
		}
		data, _ := json.Marshal(chunk)
		fmt.Fprintf(w, "data: %s\n\n", data)
		if ok {
			flusher.Flush()
		}
	}

	stopReason := "stop"
	emit(sseDelta{Role: "assistant", Content: ""}, nil)
	emit(sseDelta{Content: content}, nil)
	emit(sseDelta{}, &stopReason)
	fmt.Fprintf(w, "data: [DONE]\n\n")
	if ok {
		flusher.Flush()
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// rolesFromHeader reads caller roles from X-User-Roles (comma-separated).
// Falls back to ["developer"] so unrestricted models stay accessible without a header.
func rolesFromHeader(r *http.Request) []string {
	raw := strings.TrimSpace(r.Header.Get("X-User-Roles"))
	if raw == "" {
		return []string{"developer"}
	}
	parts := strings.Split(raw, ",")
	roles := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			roles = append(roles, t)
		}
	}
	return roles
}

// checkModelAccess calls OPA's agent/models policy and returns (allowed, reason).
// If OPA is unreachable the call fails open (allowed=true) with a warning logged.
func checkModelAccess(ctx context.Context, opaURL, model string, roles []string) (bool, string) {
	input := map[string]any{
		"input": map[string]any{
			"model": model,
			"roles": roles,
		},
	}
	body, _ := json.Marshal(input)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		opaURL+"/v1/data/agent/models/allow", strings.NewReader(string(body)))
	if err != nil {
		logger.WarnCF("http_api", "OPA request build failed, failing open", map[string]any{"error": err})
		return true, ""
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.WarnCF("http_api", "OPA unreachable, failing open", map[string]any{"error": err})
		return true, ""
	}
	defer resp.Body.Close()

	var result struct {
		Result bool `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		logger.WarnCF("http_api", "OPA response parse failed, failing open", map[string]any{"error": err})
		return true, ""
	}

	if !result.Result {
		return false, "model access denied by policy"
	}
	return true, ""
}
