package mcpctl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/adrianolaselva/dataql/internal/dataql"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/spf13/cobra"
)

// McpCtl is the interface for the MCP controller
type McpCtl interface {
	Command() *cobra.Command
}

type mcpCtl struct {
	debug bool
}

// New creates a new McpCtl instance
func New() McpCtl {
	return &mcpCtl{}
}

// Command returns the cobra command for the mcp subcommand
func (c *mcpCtl) Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Model Context Protocol server for LLM integration",
		Long: `Start a Model Context Protocol (MCP) server that enables LLMs to query data using DataQL.

The MCP server provides tools for querying, previewing, and exporting data from various sources
(CSV, JSON, Parquet, databases, etc.) using SQL queries.

Supported LLM clients:
- Claude Code (Anthropic)
- OpenAI Codex
- Google Gemini
- Any MCP-compatible client`,
	}

	cmd.AddCommand(c.serveCommand())

	return cmd
}

func (c *mcpCtl) serveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the MCP server",
		Long: `Start the MCP server using STDIO transport.

Configure in Claude Code (~/.claude/settings.json):
{
  "mcpServers": {
    "dataql": {
      "type": "stdio",
      "command": "dataql",
      "args": ["mcp", "serve"]
    }
  }
}`,
		RunE: c.runServe,
	}

	cmd.Flags().BoolVarP(&c.debug, "debug", "d", false, "Enable debug logging")

	return cmd
}

func (c *mcpCtl) runServe(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true

	// Create MCP server
	s := server.NewMCPServer(
		"dataql",
		"1.0.0",
		server.WithToolCapabilities(true),
	)

	// Register tools
	registerTools(s)

	// Start server with STDIO transport
	if err := server.ServeStdio(s); err != nil {
		return fmt.Errorf("failed to start MCP server: %w", err)
	}

	return nil
}

func registerTools(s *server.MCPServer) {
	// Tool: dataql_query - Execute SQL queries on data sources
	s.AddTool(
		mcp.NewTool("dataql_query",
			mcp.WithDescription("Execute a SQL query on a data file, URL, or database. Returns query results as JSON."),
			mcp.WithString("source",
				mcp.Required(),
				mcp.Description("Data source: file path (CSV, JSON, Parquet, etc.), URL, S3 URI, or database connection string"),
			),
			mcp.WithString("query",
				mcp.Required(),
				mcp.Description("SQL query to execute. Use table name derived from filename (e.g., 'users' for users.csv)"),
			),
			mcp.WithString("delimiter",
				mcp.Description("CSV delimiter character (default: comma)"),
			),
		),
		handleQuery,
	)

	// Tool: dataql_schema - Get schema/structure of a data source
	s.AddTool(
		mcp.NewTool("dataql_schema",
			mcp.WithDescription("Get the schema (column names and types) of a data source. Use this before querying to understand the data structure."),
			mcp.WithString("source",
				mcp.Required(),
				mcp.Description("Data source: file path, URL, S3 URI, or database connection string"),
			),
		),
		handleSchema,
	)

	// Tool: dataql_preview - Preview first N rows
	s.AddTool(
		mcp.NewTool("dataql_preview",
			mcp.WithDescription("Preview the first N rows of a data source. Useful for understanding data before running complex queries."),
			mcp.WithString("source",
				mcp.Required(),
				mcp.Description("Data source: file path, URL, S3 URI, or database connection string"),
			),
			mcp.WithNumber("limit",
				mcp.Description("Number of rows to preview (default: 5, max: 100)"),
			),
		),
		handlePreview,
	)

	// Tool: dataql_aggregate - Common aggregations
	s.AddTool(
		mcp.NewTool("dataql_aggregate",
			mcp.WithDescription("Perform common aggregation operations (count, sum, avg, min, max) on a column."),
			mcp.WithString("source",
				mcp.Required(),
				mcp.Description("Data source: file path, URL, S3 URI, or database connection string"),
			),
			mcp.WithString("column",
				mcp.Required(),
				mcp.Description("Column name to aggregate"),
			),
			mcp.WithString("operation",
				mcp.Required(),
				mcp.Description("Aggregation operation: count, sum, avg, min, max"),
			),
			mcp.WithString("group_by",
				mcp.Description("Optional column to group by"),
			),
		),
		handleAggregate,
	)
}

// Handler functions

func handleQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	source, err := request.RequireString("source")
	if err != nil {
		return mcp.NewToolResultError("source parameter is required"), nil
	}

	query, err := request.RequireString("query")
	if err != nil {
		return mcp.NewToolResultError("query parameter is required"), nil
	}

	delimiter := ","
	if d, err := request.RequireString("delimiter"); err == nil && d != "" {
		delimiter = d
	}

	// Execute query using dataql
	result, err := executeDataQL(source, query, delimiter)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Query failed: %v", err)), nil
	}

	return mcp.NewToolResultText(result), nil
}

func handleSchema(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	source, err := request.RequireString("source")
	if err != nil {
		return mcp.NewToolResultError("source parameter is required"), nil
	}

	tableName := getTableName(source)
	query := fmt.Sprintf(".schema %s", tableName)

	result, err := executeDataQL(source, query, ",")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Failed to get schema: %v", err)), nil
	}

	return mcp.NewToolResultText(result), nil
}

func handlePreview(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	source, err := request.RequireString("source")
	if err != nil {
		return mcp.NewToolResultError("source parameter is required"), nil
	}

	limit := 5
	if args, ok := request.Params.Arguments.(map[string]interface{}); ok {
		if l, exists := args["limit"]; exists {
			if lf, ok := l.(float64); ok {
				limit = int(lf)
				if limit > 100 {
					limit = 100
				}
				if limit < 1 {
					limit = 1
				}
			}
		}
	}

	tableName := getTableName(source)
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d", tableName, limit)

	result, err := executeDataQL(source, query, ",")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Preview failed: %v", err)), nil
	}

	return mcp.NewToolResultText(result), nil
}

func handleAggregate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	source, err := request.RequireString("source")
	if err != nil {
		return mcp.NewToolResultError("source parameter is required"), nil
	}

	column, err := request.RequireString("column")
	if err != nil {
		return mcp.NewToolResultError("column parameter is required"), nil
	}

	operation, err := request.RequireString("operation")
	if err != nil {
		return mcp.NewToolResultError("operation parameter is required"), nil
	}

	// Validate operation
	validOps := map[string]string{
		"count": "COUNT",
		"sum":   "SUM",
		"avg":   "AVG",
		"min":   "MIN",
		"max":   "MAX",
	}

	sqlOp, ok := validOps[strings.ToLower(operation)]
	if !ok {
		return mcp.NewToolResultError("Invalid operation. Use: count, sum, avg, min, max"), nil
	}

	tableName := getTableName(source)

	var query string
	if groupBy, err := request.RequireString("group_by"); err == nil && groupBy != "" {
		query = fmt.Sprintf("SELECT %s, %s(%s) as result FROM %s GROUP BY %s ORDER BY result DESC",
			groupBy, sqlOp, column, tableName, groupBy)
	} else {
		query = fmt.Sprintf("SELECT %s(%s) as result FROM %s", sqlOp, column, tableName)
	}

	result, err := executeDataQL(source, query, ",")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Aggregation failed: %v", err)), nil
	}

	return mcp.NewToolResultText(result), nil
}

// Helper functions

func getTableName(source string) string {
	// Extract filename from path or URL
	base := filepath.Base(source)
	// Remove extension
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	// Handle query parameters in URLs
	if idx := strings.Index(name, "?"); idx != -1 {
		name = name[:idx]
	}
	return name
}

func executeDataQL(source, query, delimiter string) (string, error) {
	params := dataql.Params{
		FileInputs: []string{source},
		Query:      query,
		Delimiter:  delimiter,
	}

	dql, err := dataql.New(params)
	if err != nil {
		return "", fmt.Errorf("failed to initialize dataql: %w", err)
	}
	defer dql.Close()

	// Capture output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err = dql.Run()

	w.Close()
	os.Stdout = oldStdout

	if err != nil {
		return "", err
	}

	// Read captured output
	outputBytes, _ := io.ReadAll(r)
	output := string(outputBytes)

	// Try to convert to JSON for better LLM consumption
	jsonOutput := tryConvertToJSON(output)
	if jsonOutput != "" {
		return jsonOutput, nil
	}

	return output, nil
}

func tryConvertToJSON(output string) string {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) < 2 {
		return ""
	}

	// Check if it looks like a table output
	if !strings.Contains(lines[0], "|") {
		return ""
	}

	// Parse table format
	var headers []string
	var rows []map[string]interface{}

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "+") || strings.HasPrefix(line, "-") {
			continue
		}

		parts := strings.Split(line, "|")
		var values []string
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				values = append(values, p)
			}
		}

		if len(values) == 0 {
			continue
		}

		if len(headers) == 0 {
			headers = values
			continue
		}

		// Skip separator line
		if i == 1 && strings.Contains(line, "-") {
			continue
		}

		row := make(map[string]interface{})
		for j, v := range values {
			if j < len(headers) {
				row[headers[j]] = v
			}
		}
		rows = append(rows, row)
	}

	if len(rows) == 0 {
		return ""
	}

	result := map[string]interface{}{
		"columns": headers,
		"rows":    rows,
		"count":   len(rows),
	}

	jsonBytes, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return ""
	}

	return string(jsonBytes)
}
