package github

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

// Client provides methods for interacting with GitHub issues.
type Client struct {
	config Config
}

// NewClient creates a new GitHub client with the given configuration.
func NewClient(config Config) *Client {
	return &Client{config: config}
}

// NewDefaultClient creates a new GitHub client with default DataQL configuration.
func NewDefaultClient() *Client {
	return NewClient(DefaultConfig())
}

// SearchIssues searches for issues matching the given query.
// Returns a list of matching issues.
func (c *Client) SearchIssues(query string, state string, limit int) ([]Issue, error) {
	if !c.isGHInstalled() {
		return nil, fmt.Errorf("gh CLI is not installed. Please install it from https://cli.github.com/")
	}

	args := []string{
		"issue", "list",
		"--repo", fmt.Sprintf("%s/%s", c.config.Owner, c.config.Repo),
		"--search", query,
		"--json", "number,title,body,state,labels,url,createdAt,updatedAt",
	}

	if state != "" {
		args = append(args, "--state", state)
	} else {
		args = append(args, "--state", "all")
	}

	if limit > 0 {
		args = append(args, "--limit", fmt.Sprintf("%d", limit))
	}

	cmd := exec.Command("gh", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to search issues: %s", stderr.String())
	}

	var issues []Issue
	if err := json.Unmarshal(stdout.Bytes(), &issues); err != nil {
		return nil, fmt.Errorf("failed to parse issues: %w", err)
	}

	return issues, nil
}

// FindDuplicates searches for issues that might be duplicates of the given title/error.
// Returns issues that have similar titles or contain similar error messages.
func (c *Client) FindDuplicates(errorKeywords string) ([]Issue, error) {
	// Extract key terms from the error message
	keywords := extractKeywords(errorKeywords)
	if len(keywords) == 0 {
		return nil, nil
	}

	query := strings.Join(keywords, " ")
	return c.SearchIssues(query, "all", 20)
}

// IsDuplicate checks if there's likely a duplicate issue for the given error.
// Returns the duplicate issue if found, nil otherwise.
func (c *Client) IsDuplicate(title string, errorMessage string) (*Issue, error) {
	// Search by title keywords first
	issues, err := c.FindDuplicates(title)
	if err != nil {
		return nil, err
	}

	// Check for similar titles
	titleLower := strings.ToLower(title)
	for _, issue := range issues {
		issueTitleLower := strings.ToLower(issue.Title)
		if calculateSimilarity(titleLower, issueTitleLower) > 0.7 {
			return &issue, nil
		}
	}

	// Search by error message if no match found
	if errorMessage != "" {
		issues, err = c.FindDuplicates(errorMessage)
		if err != nil {
			return nil, err
		}

		errorLower := strings.ToLower(errorMessage)
		for _, issue := range issues {
			bodyLower := strings.ToLower(issue.Body)
			if strings.Contains(bodyLower, errorLower) {
				return &issue, nil
			}
		}
	}

	return nil, nil
}

// CreateIssue creates a new issue with the given details.
func (c *Client) CreateIssue(req IssueCreateRequest) (*Issue, error) {
	if !c.isGHInstalled() {
		return nil, fmt.Errorf("gh CLI is not installed. Please install it from https://cli.github.com/")
	}

	args := []string{
		"issue", "create",
		"--repo", fmt.Sprintf("%s/%s", c.config.Owner, c.config.Repo),
		"--title", req.Title,
		"--body", req.Body,
	}

	if len(req.Labels) > 0 {
		args = append(args, "--label", strings.Join(req.Labels, ","))
	}

	cmd := exec.Command("gh", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to create issue: %s", stderr.String())
	}

	// The output is the issue URL
	url := strings.TrimSpace(stdout.String())
	return &Issue{URL: url, HTMLURL: url, Title: req.Title}, nil
}

// AddComment adds a comment to an existing issue.
func (c *Client) AddComment(issueNumber int, body string) error {
	if !c.isGHInstalled() {
		return fmt.Errorf("gh CLI is not installed. Please install it from https://cli.github.com/")
	}

	args := []string{
		"issue", "comment",
		fmt.Sprintf("%d", issueNumber),
		"--repo", fmt.Sprintf("%s/%s", c.config.Owner, c.config.Repo),
		"--body", body,
	}

	cmd := exec.Command("gh", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to add comment: %s", stderr.String())
	}

	return nil
}

// IsAuthenticated checks if the gh CLI is authenticated.
func (c *Client) IsAuthenticated() bool {
	cmd := exec.Command("gh", "auth", "status")
	return cmd.Run() == nil
}

// isGHInstalled checks if the gh CLI is installed.
func (c *Client) isGHInstalled() bool {
	_, err := exec.LookPath("gh")
	return err == nil
}

// extractKeywords extracts significant keywords from a text for searching.
func extractKeywords(text string) []string {
	// Common words to exclude
	stopWords := map[string]bool{
		"the": true, "a": true, "an": true, "and": true, "or": true,
		"is": true, "are": true, "was": true, "were": true, "be": true,
		"to": true, "of": true, "in": true, "for": true, "on": true,
		"with": true, "at": true, "by": true, "from": true, "as": true,
		"it": true, "this": true, "that": true, "these": true, "those": true,
		"error": true, "failed": true, "bug": true, "issue": true,
	}

	words := strings.Fields(strings.ToLower(text))
	var keywords []string

	for _, word := range words {
		// Clean the word
		word = strings.Trim(word, ".,;:!?\"'()[]{}*")

		// Skip short words and stop words
		if len(word) < 3 || stopWords[word] {
			continue
		}

		keywords = append(keywords, word)

		// Limit to first 5 keywords
		if len(keywords) >= 5 {
			break
		}
	}

	return keywords
}

// calculateSimilarity calculates a simple similarity score between two strings.
// Returns a value between 0 and 1.
func calculateSimilarity(s1, s2 string) float64 {
	words1 := strings.Fields(s1)
	words2 := strings.Fields(s2)

	// Empty strings have no similarity
	if len(words1) == 0 || len(words2) == 0 {
		return 0.0
	}

	if s1 == s2 {
		return 1.0
	}

	// Count matching words
	wordSet := make(map[string]bool)
	for _, w := range words1 {
		wordSet[w] = true
	}

	matches := 0
	for _, w := range words2 {
		if wordSet[w] {
			matches++
		}
	}

	// Jaccard similarity
	union := len(words1) + len(words2) - matches
	if union == 0 {
		return 0.0
	}

	return float64(matches) / float64(union)
}
