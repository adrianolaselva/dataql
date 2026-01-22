package github

import (
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.Owner != "adrianolaselva" {
		t.Errorf("expected owner 'adrianolaselva', got '%s'", config.Owner)
	}

	if config.Repo != "dataql" {
		t.Errorf("expected repo 'dataql', got '%s'", config.Repo)
	}
}

func TestNewClient(t *testing.T) {
	config := Config{Owner: "test", Repo: "testrepo"}
	client := NewClient(config)

	if client == nil {
		t.Error("expected non-nil client")
	}

	if client.config.Owner != "test" {
		t.Errorf("expected owner 'test', got '%s'", client.config.Owner)
	}
}

func TestNewDefaultClient(t *testing.T) {
	client := NewDefaultClient()

	if client == nil {
		t.Error("expected non-nil client")
	}

	if client.config.Owner != "adrianolaselva" {
		t.Errorf("expected owner 'adrianolaselva', got '%s'", client.config.Owner)
	}
}

func TestExtractKeywords(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int // minimum expected keywords
	}{
		{
			name:     "simple error message",
			input:    "failed to parse CSV file with invalid delimiter",
			expected: 3,
		},
		{
			name:     "empty string",
			input:    "",
			expected: 0,
		},
		{
			name:     "only stop words",
			input:    "the a an and or",
			expected: 0,
		},
		{
			name:     "technical error",
			input:    "DuckDB storage insert failed with type mismatch",
			expected: 3,
		},
		{
			name:     "long message",
			input:    "The database connection to PostgreSQL server at localhost:5432 failed with authentication error",
			expected: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keywords := extractKeywords(tt.input)
			if len(keywords) < tt.expected {
				t.Errorf("expected at least %d keywords, got %d: %v", tt.expected, len(keywords), keywords)
			}
		})
	}
}

func TestCalculateSimilarity(t *testing.T) {
	tests := []struct {
		name     string
		s1       string
		s2       string
		minScore float64
		maxScore float64
	}{
		{
			name:     "identical strings",
			s1:       "csv parsing failed",
			s2:       "csv parsing failed",
			minScore: 1.0,
			maxScore: 1.0,
		},
		{
			name:     "completely different",
			s1:       "csv parsing failed",
			s2:       "mongodb connection timeout",
			minScore: 0.0,
			maxScore: 0.2,
		},
		{
			name:     "similar strings",
			s1:       "csv parsing failed with delimiter",
			s2:       "csv parsing error with delimiter",
			minScore: 0.6,
			maxScore: 1.0,
		},
		{
			name:     "empty strings",
			s1:       "",
			s2:       "",
			minScore: 0.0,
			maxScore: 0.0,
		},
		{
			name:     "one empty string",
			s1:       "test",
			s2:       "",
			minScore: 0.0,
			maxScore: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score := calculateSimilarity(tt.s1, tt.s2)
			if score < tt.minScore || score > tt.maxScore {
				t.Errorf("expected similarity between %.2f and %.2f, got %.2f", tt.minScore, tt.maxScore, score)
			}
		})
	}
}

func TestIssueCreateRequest(t *testing.T) {
	req := IssueCreateRequest{
		Title:  "Test Issue",
		Body:   "Test body content",
		Labels: []string{"bug", "auto-reported"},
	}

	if req.Title != "Test Issue" {
		t.Errorf("expected title 'Test Issue', got '%s'", req.Title)
	}

	if len(req.Labels) != 2 {
		t.Errorf("expected 2 labels, got %d", len(req.Labels))
	}
}

func TestLabelsConstants(t *testing.T) {
	// Verify label constants are defined correctly
	labels := map[string]string{
		"LabelBug":           LabelBug,
		"LabelAutoReported":  LabelAutoReported,
		"LabelFileHandler":   LabelFileHandler,
		"LabelDatabase":      LabelDatabase,
		"LabelStorage":       LabelStorage,
		"LabelMCP":           LabelMCP,
		"LabelREPL":          LabelREPL,
		"LabelExport":        LabelExport,
		"LabelEnhancement":   LabelEnhancement,
		"LabelDocumentation": LabelDocumentation,
	}

	for name, value := range labels {
		if value == "" {
			t.Errorf("label %s should not be empty", name)
		}
	}
}
