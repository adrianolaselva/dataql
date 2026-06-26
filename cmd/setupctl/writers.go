package setupctl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

const serverKey = "dataql"

// ensureClaudeServer registers the dataql MCP server under "mcpServers" in
// Claude Code's JSON config. Returns whether a change was (or would be) made.
func ensureClaudeServer(path, exe string, dryRun bool) (bool, error) {
	entry := map[string]any{
		"type":    "stdio",
		"command": exe,
		"args":    []any{"mcp", "serve"},
	}
	return applyJSON(path, "mcpServers", entry, dryRun)
}

// ensureOpencodeServer registers the dataql MCP server for opencode, whose
// schema nests servers under "mcp" with a command array and an enabled flag.
func ensureOpencodeServer(path, exe string, dryRun bool) (bool, error) {
	entry := map[string]any{
		"type":    "local",
		"command": []any{exe, "mcp", "serve"},
		"enabled": true,
	}
	return applyJSON(path, "mcp", entry, dryRun)
}

func applyJSON(path, topKey string, entry map[string]any, dryRun bool) (bool, error) {
	existing, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	out, changed, err := mergeJSONNested(existing, topKey, entry)
	if err != nil {
		return false, err
	}
	if changed && !dryRun {
		if err := writeWithBackup(path, out); err != nil {
			return false, err
		}
	}
	return changed, nil
}

// mergeJSONNested sets root[topKey][serverKey] = entry in a JSON document,
// preserving every other key. It returns changed=false when the value already
// matches (idempotent). An empty input is treated as an empty object.
func mergeJSONNested(raw []byte, topKey string, entry map[string]any) ([]byte, bool, error) {
	server := serverKey
	root := map[string]any{}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &root); err != nil {
			return nil, false, fmt.Errorf("existing config is not valid JSON: %w", err)
		}
	}

	top, _ := root[topKey].(map[string]any)
	if top == nil {
		top = map[string]any{}
	}
	if existing, ok := top[server]; ok && reflect.DeepEqual(normalize(existing), normalize(entry)) {
		return raw, false, nil
	}
	top[server] = entry
	root[topKey] = top

	out, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		return nil, false, err
	}
	out = append(out, '\n')
	return out, true, nil
}

// normalize round-trips a value through JSON so maps/slices compare structurally
// regardless of concrete Go types.
func normalize(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out any
	_ = json.Unmarshal(b, &out)
	return out
}

// ensureCodexServer registers the dataql MCP server in Codex's config.toml.
// Codex uses TOML; rather than depend on a TOML library, we append a
// [mcp_servers.dataql] table if one is not already present (idempotent).
func ensureCodexServer(path, exe string, dryRun bool) (bool, error) {
	existing, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	if bytes.Contains(existing, []byte("[mcp_servers.dataql]")) {
		return false, nil
	}
	block := fmt.Sprintf("\n[mcp_servers.dataql]\ncommand = %q\nargs = [\"mcp\", \"serve\"]\n", exe)
	out := existing
	if len(out) > 0 && !bytes.HasSuffix(out, []byte("\n")) {
		out = append(out, '\n')
	}
	out = append(out, []byte(strings.TrimPrefix(block, "\n"))...)
	if !dryRun {
		if err := writeWithBackup(path, out); err != nil {
			return false, err
		}
	}
	return true, nil
}

// writeWithBackup writes data to path, creating parent dirs and backing up an
// existing file to "<path>.bak" first.
func writeWithBackup(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	if prev, err := os.ReadFile(path); err == nil {
		// #nosec G703 -- path is an agent config file under the user's own home
		// dir (os.UserHomeDir() + a constant filename), not untrusted input.
		if err := os.WriteFile(path+".bak", prev, 0o600); err != nil {
			return err
		}
	}
	// #nosec G703 -- see above: path is the user's own agent config file.
	return os.WriteFile(path, data, 0o600)
}
