// Package github provides GitHub issue management functionality for DataQL.
package github

// Issue represents a GitHub issue.
// Fields use GitHub API JSON naming convention.
type Issue struct {
	Number      int      `json:"number"`
	Title       string   `json:"title"`
	Body        string   `json:"body"`
	State       string   `json:"state"`
	Labels      []string `json:"labels"`
	URL         string   `json:"url"`
	HTMLURL     string   `json:"html_url"` //nolint:tagliatelle // GitHub API naming
	CreatedAt   string   `json:"created_at"`
	UpdatedAt   string   `json:"updated_at"`
	ClosedAt    string   `json:"closed_at,omitempty"`
	CommentsURL string   `json:"comments_url"`
}

// IssueCreateRequest represents a request to create a new issue.
type IssueCreateRequest struct {
	Title  string   `json:"title"`
	Body   string   `json:"body"`
	Labels []string `json:"labels,omitempty"`
}

// IssueSearchResult represents the result of searching for issues.
// Uses GitHub API field names.
type IssueSearchResult struct {
	Issues     []Issue `json:"items"` //nolint:tagliatelle // GitHub API naming
	TotalCount int     `json:"total_count"`
}

// CommentRequest represents a request to add a comment to an issue.
type CommentRequest struct {
	Body string `json:"body"`
}

// Config holds the configuration for GitHub operations.
type Config struct {
	Owner string
	Repo  string
}

// DefaultConfig returns the default configuration for DataQL repository.
func DefaultConfig() Config {
	return Config{
		Owner: "adrianolaselva",
		Repo:  "dataql",
	}
}

// Common labels used for DataQL issues.
const (
	LabelBug           = "bug"
	LabelAutoReported  = "auto-reported"
	LabelFileHandler   = "file-handler"
	LabelDatabase      = "database"
	LabelStorage       = "storage"
	LabelMCP           = "mcp"
	LabelREPL          = "repl"
	LabelExport        = "export"
	LabelEnhancement   = "enhancement"
	LabelDocumentation = "documentation"
)
