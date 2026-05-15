package http_api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
)

func newTestChannel(t *testing.T) *HTTPAPIChannel {
	t.Helper()
	b := bus.NewMessageBus()
	bc := &config.Channel{AllowFrom: []string{"*"}}
	cfg := &config.HTTPAPISettings{Host: "127.0.0.1", Port: 0}
	ch, err := NewHTTPAPIChannel(bc, cfg, b)
	if err != nil {
		t.Fatalf("NewHTTPAPIChannel: %v", err)
	}
	return ch
}

// simulateAgentReply delivers a fake agent reply to the pending request keyed by id.
func simulateAgentReply(t *testing.T, ch *HTTPAPIChannel, id, content string) {
	t.Helper()
	_, _ = ch.Send(context.Background(), bus.OutboundMessage{
		ChatID:  id,
		Content: content,
	})
}

// pendingID returns the first key found in the pending map (test helper).
func pendingID(ch *HTTPAPIChannel) string {
	var key string
	ch.pending.Range(func(k, _ any) bool {
		key = k.(string)
		return false
	})
	return key
}

// --- pendingRequest unit tests ---

func TestPendingRequest_IdleTimerStartsOnFirstSend(t *testing.T) {
	pr := newPendingRequest("planning")
	// done must not be closed before any Send()
	select {
	case <-pr.done:
		t.Fatal("done closed before any Send()")
	case <-time.After(10 * time.Millisecond):
	}

	pr.update("hello")

	// done should close within idleTimeout + headroom
	select {
	case <-pr.done:
	case <-time.After(idleTimeout + 500*time.Millisecond):
		t.Fatal("done not closed after idle timeout")
	}

	pr.mu.Lock()
	got := pr.content
	pr.mu.Unlock()
	if got != "hello" {
		t.Errorf("want %q, got %q", "hello", got)
	}
}

func TestPendingRequest_TimerResetsOnUpdate(t *testing.T) {
	pr := newPendingRequest("review")
	pr.update("first")
	// Reset the timer before it fires.
	time.Sleep(idleTimeout / 2)
	pr.update("second")

	select {
	case <-pr.done:
		pr.mu.Lock()
		got := pr.content
		pr.mu.Unlock()
		if got != "second" {
			t.Errorf("want final content %q, got %q", "second", got)
		}
	case <-time.After(idleTimeout + 500*time.Millisecond):
		t.Fatal("done not closed after idle timeout")
	}
}

func TestPendingRequest_DoubleClose(t *testing.T) {
	pr := newPendingRequest("chat")
	pr.close()
	pr.close() // must not panic
}

func TestPendingRequest_ContextCancellation(t *testing.T) {
	pr := newPendingRequest("planning")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Simulate agent.wait select behaviour.
	select {
	case <-pr.done:
		t.Fatal("should not have fired done")
	case <-ctx.Done():
		// expected
	}
}

// --- /health ---

func TestHandleHealth(t *testing.T) {
	ch := newTestChannel(t)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	ch.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("want 200, got %d", w.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("want status=ok, got %q", body["status"])
	}
}

// --- /v1/models ---

func TestHandleModels(t *testing.T) {
	ch := newTestChannel(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()
	ch.handleModels(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("want 200, got %d", w.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["object"] != "list" {
		t.Errorf("want object=list, got %v", body["object"])
	}
}

// --- POST /agent ---

func TestHandleAgent_ReturnsRunID(t *testing.T) {
	ch := newTestChannel(t)
	payload, _ := json.Marshal(agentStartRequest{
		SessionKey: "wf-001:planning",
		Message:    "plan the feature",
		Metadata:   map[string]string{"phase": "planning"},
	})
	req := httptest.NewRequest(http.MethodPost, "/agent", bytes.NewReader(payload))
	w := httptest.NewRecorder()
	ch.handleAgent(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d — %s", w.Code, w.Body.String())
	}
	var resp agentAccepted
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.RunID != "wf-001:planning" {
		t.Errorf("want runId=wf-001:planning, got %q", resp.RunID)
	}
	if resp.AcceptedAt.IsZero() {
		t.Error("acceptedAt should be set")
	}
}

func TestHandleAgent_GeneratesRunIDWhenSessionKeyEmpty(t *testing.T) {
	ch := newTestChannel(t)
	payload, _ := json.Marshal(agentStartRequest{Message: "hello"})
	req := httptest.NewRequest(http.MethodPost, "/agent", bytes.NewReader(payload))
	w := httptest.NewRecorder()
	ch.handleAgent(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d", w.Code)
	}
	var resp agentAccepted
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.RunID == "" {
		t.Error("want generated runId, got empty")
	}
}

// --- POST /agent.wait ---

func TestHandleAgentWait_ReturnsOKAfterReply(t *testing.T) {
	ch := newTestChannel(t)

	// Start the session.
	startPayload, _ := json.Marshal(agentStartRequest{
		SessionKey: "wf-002:review",
		Message:    "review the code",
		Metadata:   map[string]string{"phase": "review"},
	})
	startReq := httptest.NewRequest(http.MethodPost, "/agent", bytes.NewReader(startPayload))
	ch.handleAgent(httptest.NewRecorder(), startReq)

	// Poll for result in a goroutine.
	resCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		waitPayload, _ := json.Marshal(agentPollRequest{
			RunID:     "wf-002:review",
			TimeoutMs: 10000,
		})
		req := httptest.NewRequest(http.MethodPost, "/agent.wait", bytes.NewReader(waitPayload))
		w := httptest.NewRecorder()
		ch.handleAgentWait(w, req)
		resCh <- w
	}()

	// Deliver a reply after a short pause.
	time.Sleep(20 * time.Millisecond)
	simulateAgentReply(t, ch, "wf-002:review", "Code looks good. tests_passed")

	select {
	case w := <-resCh:
		if w.Code != http.StatusOK {
			t.Fatalf("want 200, got %d — %s", w.Code, w.Body.String())
		}
		var result agentPollResult
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if result.Status != "ok" {
			t.Errorf("want status=ok, got %q", result.Status)
		}
		if result.Outcome != "tests_passed" {
			t.Errorf("want outcome=tests_passed, got %q", result.Outcome)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("agent.wait did not return")
	}
}

func TestHandleAgentWait_ReturnsTimeoutWhenAgentSlow(t *testing.T) {
	ch := newTestChannel(t)

	startPayload, _ := json.Marshal(agentStartRequest{
		SessionKey: "wf-003:planning",
		Message:    "plan it",
	})
	ch.handleAgent(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/agent", bytes.NewReader(startPayload)))

	// Wait with a very short timeout — agent never replies.
	waitPayload, _ := json.Marshal(agentPollRequest{
		RunID:     "wf-003:planning",
		TimeoutMs: 50,
	})
	req := httptest.NewRequest(http.MethodPost, "/agent.wait", bytes.NewReader(waitPayload))
	w := httptest.NewRecorder()
	ch.handleAgentWait(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var result agentPollResult
	_ = json.Unmarshal(w.Body.Bytes(), &result)
	if result.Status != "timeout" {
		t.Errorf("want status=timeout, got %q", result.Status)
	}

	// Pending must still exist for the retry.
	if pendingID(ch) == "" {
		t.Error("pending entry should survive timeout so caller can retry")
	}
}

func TestHandleAgentWait_ReturnsErrorForUnknownRunID(t *testing.T) {
	ch := newTestChannel(t)
	waitPayload, _ := json.Marshal(agentPollRequest{RunID: "no-such-run", TimeoutMs: 100})
	req := httptest.NewRequest(http.MethodPost, "/agent.wait", bytes.NewReader(waitPayload))
	w := httptest.NewRecorder()
	ch.handleAgentWait(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var result agentPollResult
	_ = json.Unmarshal(w.Body.Bytes(), &result)
	if result.Status != "error" {
		t.Errorf("want status=error, got %q", result.Status)
	}
}

// --- POST /agent.cancel ---

func TestHandleAgentCancel(t *testing.T) {
	ch := newTestChannel(t)
	pr := newPendingRequest("planning")
	ch.pending.Store("cancel-me", pr)

	payload, _ := json.Marshal(map[string]string{"runId": "cancel-me"})
	req := httptest.NewRequest(http.MethodPost, "/agent.cancel", bytes.NewReader(payload))
	w := httptest.NewRecorder()
	ch.handleAgentCancel(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("want 200, got %d", w.Code)
	}
	if _, ok := ch.pending.Load("cancel-me"); ok {
		t.Error("pending entry should be removed after cancel")
	}
	select {
	case <-pr.done:
	default:
		t.Error("pendingRequest.done should be closed after cancel")
	}
}

// --- POST /v1/chat/completions ---

func TestHandleChatCompletions_NoUserMessage(t *testing.T) {
	ch := newTestChannel(t)
	payload, _ := json.Marshal(chatCompletionRequest{
		Model:    "default",
		Messages: []chatMessage{{Role: "system", Content: "you are helpful"}},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(payload))
	w := httptest.NewRecorder()
	ch.handleChatCompletions(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d", w.Code)
	}
}

func TestHandleChatCompletions_ReceivesReply(t *testing.T) {
	ch := newTestChannel(t)

	payload, _ := json.Marshal(chatCompletionRequest{
		Model:    "default",
		Messages: []chatMessage{{Role: "user", Content: "hello"}},
	})

	resCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(payload))
		w := httptest.NewRecorder()
		ch.handleChatCompletions(w, req)
		resCh <- w
	}()

	time.Sleep(20 * time.Millisecond)
	rid := pendingID(ch)
	if rid == "" {
		t.Fatal("no pending request registered")
	}
	simulateAgentReply(t, ch, rid, "Hello back!")

	select {
	case w := <-resCh:
		if w.Code != http.StatusOK {
			t.Fatalf("want 200, got %d — %s", w.Code, w.Body.String())
		}
		var resp chatCompletionResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(resp.Choices) == 0 || resp.Choices[0].Message.Content != "Hello back!" {
			t.Errorf("unexpected choices: %+v", resp.Choices)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("chat completions did not return")
	}
}
