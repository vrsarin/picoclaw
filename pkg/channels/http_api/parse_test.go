package http_api

import (
	"strings"
	"testing"
)

func TestParseAgentResponse_Artifacts(t *testing.T) {
	content := "Here is the plan summary.\n\n## plan.md\n# My Plan\nStep 1\nStep 2\n\n## risk-review.md\nRisk: low"

	summary, outcome, artifacts := parseAgentResponse(content, "planning")

	if summary != "Here is the plan summary." {
		t.Errorf("unexpected summary: %q", summary)
	}
	if outcome != "completed" {
		t.Errorf("want outcome=completed, got %q", outcome)
	}
	if artifacts["plan.md"] != "# My Plan\nStep 1\nStep 2" {
		t.Errorf("unexpected plan.md: %q", artifacts["plan.md"])
	}
	if artifacts["risk-review.md"] != "Risk: low" {
		t.Errorf("unexpected risk-review.md: %q", artifacts["risk-review.md"])
	}
}

func TestParseAgentResponse_OutcomeTestsFailed(t *testing.T) {
	content := "All checks complete. tests_failed due to assertion errors."
	_, outcome, _ := parseAgentResponse(content, "review")
	if outcome != "tests_failed" {
		t.Errorf("want tests_failed, got %q", outcome)
	}
}

func TestParseAgentResponse_OutcomeTestsPassed(t *testing.T) {
	content := "Verification complete.\ntests_passed"
	_, outcome, _ := parseAgentResponse(content, "fix_tests")
	if outcome != "tests_passed" {
		t.Errorf("want tests_passed, got %q", outcome)
	}
}

func TestParseAgentResponse_NoArtifacts(t *testing.T) {
	content := "This is a simple response with no artifact blocks."
	summary, outcome, artifacts := parseAgentResponse(content, "planning")
	if summary != content {
		t.Errorf("unexpected summary: %q", summary)
	}
	if outcome != "completed" {
		t.Errorf("want completed, got %q", outcome)
	}
	if len(artifacts) != 0 {
		t.Errorf("expected no artifacts, got %v", artifacts)
	}
}

func TestParseAgentResponse_IgnoresNonFileHeadings(t *testing.T) {
	// "Overview" has no file extension — must not become an artifact.
	content := "## Overview\nJust a prose heading.\n\n## notes.txt\nsome notes"
	_, _, artifacts := parseAgentResponse(content, "planning")
	if _, ok := artifacts["Overview"]; ok {
		t.Error("Overview should not be treated as an artifact")
	}
	if artifacts["notes.txt"] != "some notes" {
		t.Errorf("unexpected notes.txt: %q", artifacts["notes.txt"])
	}
}

func TestParseAgentResponse_SummaryTruncation(t *testing.T) {
	long := strings.Repeat("x", 3000)
	summary, _, _ := parseAgentResponse(long, "planning")
	if len(summary) != 2000 {
		t.Errorf("expected summary truncated to 2000 chars, got %d", len(summary))
	}
}

func TestParseAgentResponse_EmptyContent(t *testing.T) {
	summary, outcome, artifacts := parseAgentResponse("", "planning")
	if summary != "" {
		t.Errorf("want empty summary, got %q", summary)
	}
	if outcome != "completed" {
		t.Errorf("want completed, got %q", outcome)
	}
	if len(artifacts) != 0 {
		t.Errorf("want no artifacts, got %v", artifacts)
	}
}
