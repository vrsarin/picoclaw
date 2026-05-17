package agent

import (
	"context"
	"time"
)

// AxonMemoryProvider implements MemoryProvider backed by the Axon sidecar.
// GetMemoryContext calls /retrieve and returns the rendered semantic memory
// context; WatchedPaths returns nil because there are no local files to watch.
type AxonMemoryProvider struct {
	client          *axonHTTPClient
	taskDescription string
}

// NewAxonMemoryProvider creates an AxonMemoryProvider.
// baseURL is the Axon sidecar root (e.g. "http://localhost:19823").
// scopeID scopes memory to a user or workspace.
// taskDescription is injected into /retrieve as-is; pass "" for generic retrieval.
func NewAxonMemoryProvider(baseURL, scopeID, taskDescription string) *AxonMemoryProvider {
	return &AxonMemoryProvider{
		client:          newAxonHTTPClient(baseURL, scopeID),
		taskDescription: taskDescription,
	}
}

// GetMemoryContext calls Axon /retrieve and returns the rendered prompt.
// Returns empty string on error so the agent continues without memory context.
func (p *AxonMemoryProvider) GetMemoryContext() string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	text, _ := p.client.retrieveMemory(ctx, p.taskDescription)
	return text
}

// WatchedPaths returns nil because Axon memory is remote; no local file changes
// will trigger cache invalidation.
func (p *AxonMemoryProvider) WatchedPaths() []string {
	return nil
}
