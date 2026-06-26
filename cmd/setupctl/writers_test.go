package setupctl

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeJSONNested_FreshConfig(t *testing.T) {
	entry := map[string]any{"type": "stdio", "command": "/usr/local/bin/dataql", "args": []any{"mcp", "serve"}}
	out, changed, err := mergeJSONNested(nil, "mcpServers", entry)
	require.NoError(t, err)
	assert.True(t, changed)

	var got map[string]any
	require.NoError(t, json.Unmarshal(out, &got))
	servers := got["mcpServers"].(map[string]any)
	srv := servers["dataql"].(map[string]any)
	assert.Equal(t, "/usr/local/bin/dataql", srv["command"])
	assert.Equal(t, []any{"mcp", "serve"}, srv["args"])
}

func TestMergeJSONNested_Idempotent(t *testing.T) {
	entry := map[string]any{"type": "stdio", "command": "dataql", "args": []any{"mcp", "serve"}}
	first, changed, err := mergeJSONNested(nil, "mcpServers", entry)
	require.NoError(t, err)
	require.True(t, changed)

	second, changed2, err := mergeJSONNested(first, "mcpServers", entry)
	require.NoError(t, err)
	assert.False(t, changed2, "re-running must be a no-op")
	assert.Equal(t, first, second)
}

func TestMergeJSONNested_PreservesUnrelated(t *testing.T) {
	in := []byte(`{
  "theme": "dark",
  "mcpServers": { "other": { "command": "othertool" } }
}`)
	entry := map[string]any{"type": "stdio", "command": "dataql", "args": []any{"mcp", "serve"}}
	out, changed, err := mergeJSONNested(in, "mcpServers", entry)
	require.NoError(t, err)
	require.True(t, changed)

	var got map[string]any
	require.NoError(t, json.Unmarshal(out, &got))
	assert.Equal(t, "dark", got["theme"], "unrelated top-level key preserved")
	servers := got["mcpServers"].(map[string]any)
	assert.Contains(t, servers, "other", "unrelated MCP server preserved")
	assert.Contains(t, servers, "dataql", "dataql server added")
}

func TestMergeJSONNested_InvalidJSON(t *testing.T) {
	_, _, err := mergeJSONNested([]byte("{not json"), "mcpServers", map[string]any{})
	assert.Error(t, err)
}

func TestEnsureJSONServer_WritesAndIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".claude.json")

	changed, err := ensureClaudeServer(path, "dataql", false)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.FileExists(t, path)

	changed2, err := ensureClaudeServer(path, "dataql", false)
	require.NoError(t, err)
	assert.False(t, changed2)
}

func TestEnsureJSONServer_DryRunDoesNotWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".claude.json")

	changed, err := ensureClaudeServer(path, "dataql", true)
	require.NoError(t, err)
	assert.True(t, changed)
	_, statErr := os.Stat(path)
	assert.True(t, os.IsNotExist(statErr), "dry-run must not create the file")
}

func TestEnsureCodexServer_AppendIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	require.NoError(t, os.WriteFile(path, []byte("model = \"gpt-5\"\n"), 0o600))

	changed, err := ensureCodexServer(path, "dataql", false)
	require.NoError(t, err)
	require.True(t, changed)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(data), "model = \"gpt-5\"", "existing config preserved")
	assert.Contains(t, string(data), "[mcp_servers.dataql]")

	changed2, err := ensureCodexServer(path, "dataql", false)
	require.NoError(t, err)
	assert.False(t, changed2, "second run must not append again")
}

func TestEnsureOpencodeServer_Shape(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "opencode.json")

	changed, err := ensureOpencodeServer(path, "/bin/dataql", false)
	require.NoError(t, err)
	require.True(t, changed)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	srv := got["mcp"].(map[string]any)["dataql"].(map[string]any)
	assert.Equal(t, "local", srv["type"])
	assert.Equal(t, []any{"/bin/dataql", "mcp", "serve"}, srv["command"])
	assert.Equal(t, true, srv["enabled"])
}
