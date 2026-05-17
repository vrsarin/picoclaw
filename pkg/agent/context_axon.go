package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/sipeed/picoclaw/pkg/providers/protocoltypes"
)

// ── Shared HTTP client ────────────────────────────────────────────────────────

// axonSessionMsg mirrors pgvector.SessionMessage for JSON (de)serialisation.
// Defined here to avoid importing the axon module into ai-hub.
type axonSessionMsg struct {
	ID               int64  `json:"id,omitempty"`
	Role             string `json:"role"`
	Content          string `json:"content"`
	ReasoningContent string `json:"reasoning_content,omitempty"`
	ToolCallID       string `json:"tool_call_id,omitempty"`
	Parts            []any  `json:"parts,omitempty"`
	TokenCount       int    `json:"token_count"`
}

type axonAssembleResult struct {
	Messages []axonSessionMsg `json:"messages"`
	Summary  string           `json:"summary"`
}

type axonHTTPClient struct {
	baseURL string
	scopeID string
	http    *http.Client
}

func newAxonHTTPClient(baseURL, scopeID string) *axonHTTPClient {
	return &axonHTTPClient{
		baseURL: baseURL,
		scopeID: scopeID,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *axonHTTPClient) postJSON(ctx context.Context, path string, body any) (*http.Response, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.http.Do(req)
}

func (c *axonHTTPClient) deleteJSON(ctx context.Context, path string, body any) (*http.Response, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+path, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.http.Do(req)
}

func (c *axonHTTPClient) ingestSession(ctx context.Context, sessionKey string, msgs []axonSessionMsg) error {
	resp, err := c.postJSON(ctx, "/session/ingest", map[string]any{
		"session_key": sessionKey,
		"scope_id":    c.scopeID,
		"messages":    msgs,
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("axon ingest: status %d", resp.StatusCode)
	}
	return nil
}

func (c *axonHTTPClient) assembleSession(ctx context.Context, sessionKey string, budget, maxTokens int) (*axonAssembleResult, error) {
	url := fmt.Sprintf("%s/session/assemble?session_key=%s&scope_id=%s&budget=%d&max_tokens=%d",
		c.baseURL, sessionKey, c.scopeID, budget, maxTokens)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("axon assemble: status %d", resp.StatusCode)
	}
	var result axonAssembleResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *axonHTTPClient) compactSession(ctx context.Context, sessionKey string, budget int) error {
	resp, err := c.postJSON(ctx, "/session/compact", map[string]any{
		"session_key": sessionKey,
		"scope_id":    c.scopeID,
		"budget":      budget,
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("axon compact: status %d", resp.StatusCode)
	}
	return nil
}

func (c *axonHTTPClient) clearSession(ctx context.Context, sessionKey string) error {
	resp, err := c.deleteJSON(ctx, "/session", map[string]any{
		"session_key": sessionKey,
		"scope_id":    c.scopeID,
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("axon clear: status %d", resp.StatusCode)
	}
	return nil
}

func (c *axonHTTPClient) retrieveMemory(ctx context.Context, taskDescription string) (string, error) {
	resp, err := c.postJSON(ctx, "/retrieve", map[string]any{
		"task_description": taskDescription,
		"scope_id":         c.scopeID,
	})
	if err != nil {
		return "", nil // non-fatal: degrade gracefully
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", nil
	}
	var out struct {
		RenderedPrompt string `json:"rendered_prompt"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", nil
	}
	return out.RenderedPrompt, nil
}

// axonEnvStr returns the environment variable value or the default.
func axonEnvStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ── AxonContextManager ────────────────────────────────────────────────────────

type axonContextManager struct {
	client *axonHTTPClient
}

type axonContextConfig struct {
	URL     string `json:"url"`
	ScopeID string `json:"scope_id"`
}

func newAxonContextManager(cfg json.RawMessage, _ *AgentLoop) (ContextManager, error) {
	conf := axonContextConfig{
		URL:     axonEnvStr("AXON_URL", "http://localhost:19823"),
		ScopeID: axonEnvStr("AXON_SCOPE_ID", "default"),
	}
	if len(cfg) > 0 {
		_ = json.Unmarshal(cfg, &conf)
	}
	if conf.URL == "" {
		return nil, fmt.Errorf("axon context manager: AXON_URL is required")
	}
	return &axonContextManager{
		client: newAxonHTTPClient(conf.URL, conf.ScopeID),
	}, nil
}

func (m *axonContextManager) Assemble(ctx context.Context, req *AssembleRequest) (*AssembleResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("axon assemble: nil request")
	}
	budget := req.Budget
	if budget <= 0 {
		budget = 100000
	}
	result, err := m.client.assembleSession(ctx, req.SessionKey, budget, req.MaxTokens)
	if err != nil {
		return nil, err
	}
	return &AssembleResponse{
		History: axonMsgsToProvider(result.Messages),
		Summary: result.Summary,
	}, nil
}

func (m *axonContextManager) Compact(ctx context.Context, req *CompactRequest) error {
	if req == nil {
		return nil
	}
	return m.client.compactSession(ctx, req.SessionKey, req.Budget)
}

func (m *axonContextManager) Ingest(ctx context.Context, req *IngestRequest) error {
	if req == nil {
		return nil
	}
	msg := providerToAxonMsg(req.Message)
	return m.client.ingestSession(ctx, req.SessionKey, []axonSessionMsg{msg})
}

func (m *axonContextManager) Clear(ctx context.Context, sessionKey string) error {
	return m.client.clearSession(ctx, sessionKey)
}

// ── message conversion ────────────────────────────────────────────────────────

func providerToAxonMsg(msg protocoltypes.Message) axonSessionMsg {
	out := axonSessionMsg{
		Role:             msg.Role,
		Content:          msg.Content,
		ReasoningContent: msg.ReasoningContent,
		ToolCallID:       msg.ToolCallID,
		TokenCount:       EstimateMessageTokens(msg),
	}
	for _, tc := range msg.ToolCalls {
		name := ""
		args := ""
		if tc.Function != nil {
			name = tc.Function.Name
			args = tc.Function.Arguments
		}
		out.Parts = append(out.Parts, map[string]any{
			"type":         "tool_use",
			"name":         name,
			"arguments":    args,
			"tool_call_id": tc.ID,
		})
	}
	return out
}

func axonMsgsToProvider(msgs []axonSessionMsg) []protocoltypes.Message {
	out := make([]protocoltypes.Message, 0, len(msgs))
	for _, m := range msgs {
		pm := protocoltypes.Message{
			Role:             m.Role,
			Content:          m.Content,
			ReasoningContent: m.ReasoningContent,
			ToolCallID:       m.ToolCallID,
		}
		for _, raw := range m.Parts {
			part, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if part["type"] == "tool_use" {
				tc := protocoltypes.ToolCall{Type: "function"}
				if id, ok := part["tool_call_id"].(string); ok {
					tc.ID = id
				}
				tc.Function = &protocoltypes.FunctionCall{}
				if name, ok := part["name"].(string); ok {
					tc.Function.Name = name
				}
				if args, ok := part["arguments"].(string); ok {
					tc.Function.Arguments = args
				}
				pm.ToolCalls = append(pm.ToolCalls, tc)
			}
		}
		out = append(out, pm)
	}
	return out
}

func init() {
	if err := RegisterContextManager("axon", newAxonContextManager); err != nil {
		panic(fmt.Sprintf("register axon context manager: %v", err))
	}
}
