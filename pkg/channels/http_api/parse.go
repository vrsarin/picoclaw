package http_api

import (
	"regexp"
	"strings"
)

// artifactHeaderRe matches a ## filename.ext line at the start of a line.
// The filename must contain at least one dot followed by a word-character extension
// so that prose headings like "## Overview" are not treated as artifacts.
var artifactHeaderRe = regexp.MustCompile(`(?m)^## ([\w.\-]+\.\w+)\s*$`)

// parseAgentResponse extracts the summary, workflow outcome, and named artifact
// files from the agent's text response.
//
// Artifacts: blocks delimited by `## filename.ext` headers. Each block runs from
// the end of its header to the start of the next header (or end of text).
//
// Outcome: detected by scanning for the literal markers "tests_failed" or
// "tests_passed" anywhere in the full text. All other phases return "completed".
//
// Summary: the text before the first artifact block, truncated to 2000 characters.
// If there are no artifact blocks the full content is used as the summary.
func parseAgentResponse(content, _ string) (summary, outcome string, artifacts map[string]string) {
	// Outcome — scan full text for well-known markers.
	switch {
	case strings.Contains(content, "tests_failed"):
		outcome = "tests_failed"
	case strings.Contains(content, "tests_passed"):
		outcome = "tests_passed"
	default:
		outcome = "completed"
	}

	locs := artifactHeaderRe.FindAllStringIndex(content, -1)
	if len(locs) == 0 {
		summary = truncate(strings.TrimSpace(content), 2000)
		return
	}

	summary = truncate(strings.TrimSpace(content[:locs[0][0]]), 2000)

	artifacts = make(map[string]string, len(locs))
	for i, loc := range locs {
		header := content[loc[0]:loc[1]]
		sub := artifactHeaderRe.FindStringSubmatch(header)
		if len(sub) < 2 {
			continue
		}
		name := sub[1]

		bodyStart := loc[1]
		bodyEnd := len(content)
		if i+1 < len(locs) {
			bodyEnd = locs[i+1][0]
		}
		artifacts[name] = strings.TrimSpace(content[bodyStart:bodyEnd])
	}
	return
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
