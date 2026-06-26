// Package setupctl implements `dataql setup`, which configures the MCP server
// for the supported agents (Claude Code, Codex, opencode). It is idempotent,
// merges into existing config without clobbering unrelated entries, and makes no
// network calls.
package setupctl

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

type setupCtl struct {
	dryRun bool
	only   string
}

// New creates the setup command controller.
func New() *setupCtl { return &setupCtl{} }

// Command returns the `setup` cobra command.
func (c *setupCtl) Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "setup",
		Short: "Configure DataQL's MCP server for your AI agents",
		Long: `Register the DataQL MCP server with the AI agents installed on this
machine (Claude Code, Codex, opencode), so they can query data through DataQL
with no manual configuration.

The command is idempotent and only adds/updates the "dataql" MCP entry; it never
removes unrelated configuration. It performs no network access.`,
		RunE: c.run,
	}
	cmd.Flags().BoolVar(&c.dryRun, "dry-run", false, "Show what would change without writing")
	cmd.Flags().StringVar(&c.only, "agent", "", "Configure only this agent (claude, codex, opencode)")
	return cmd
}

func (c *setupCtl) run(cmd *cobra.Command, _ []string) error {
	cmd.SilenceUsage = true

	exe, err := os.Executable()
	if err != nil || exe == "" {
		exe = "dataql" // fall back to PATH lookup
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("cannot determine home directory: %w", err)
	}

	agents := supportedAgents(home)
	configured := 0
	for _, a := range agents {
		if c.only != "" && c.only != a.id {
			continue
		}
		if !a.present() {
			continue
		}
		changed, err := a.ensure(exe, c.dryRun)
		if err != nil {
			fmt.Printf("  ✗ %s: %v\n", a.name, err)
			continue
		}
		switch {
		case c.dryRun && changed:
			fmt.Printf("  • %s: would add the dataql MCP server (%s)\n", a.name, a.configPath)
		case changed:
			fmt.Printf("  ✓ %s: configured the dataql MCP server (%s)\n", a.name, a.configPath)
		default:
			fmt.Printf("  ✓ %s: already configured\n", a.name)
		}
		configured++
	}

	if configured == 0 {
		fmt.Println("No supported agents detected (Claude Code, Codex, opencode). Nothing to do.")
		return nil
	}
	if !c.dryRun {
		fmt.Println("\nDone. Restart your agent to pick up the dataql MCP server.")
		fmt.Println("Tip: run `dataql skills install` to add the DataQL skills too.")
	}
	return nil
}

// agent describes one supported agent's MCP configuration target.
type agent struct {
	id         string
	name       string
	configPath string
	// ensure adds/updates the dataql MCP entry; returns whether a change was (or
	// would be) made. It must be idempotent and preserve unrelated config.
	ensure func(exe string, dryRun bool) (bool, error)
	// present reports whether this agent appears to be installed.
	present func() bool
}

func supportedAgents(home string) []agent {
	claudePath := filepath.Join(home, ".claude.json")
	codexPath := filepath.Join(home, ".codex", "config.toml")
	opencodePath := filepath.Join(home, ".config", "opencode", "opencode.json")

	return []agent{
		{
			id: "claude", name: "Claude Code", configPath: claudePath,
			present: func() bool { return fileExists(claudePath) || dirExists(filepath.Join(home, ".claude")) },
			ensure: func(exe string, dry bool) (bool, error) {
				return ensureClaudeServer(claudePath, exe, dry)
			},
		},
		{
			id: "opencode", name: "opencode", configPath: opencodePath,
			present: func() bool { return fileExists(opencodePath) || dirExists(filepath.Join(home, ".config", "opencode")) },
			ensure: func(exe string, dry bool) (bool, error) {
				return ensureOpencodeServer(opencodePath, exe, dry)
			},
		},
		{
			id: "codex", name: "Codex", configPath: codexPath,
			present: func() bool { return fileExists(codexPath) || dirExists(filepath.Join(home, ".codex")) },
			ensure: func(exe string, dry bool) (bool, error) {
				return ensureCodexServer(codexPath, exe, dry)
			},
		},
	}
}

func fileExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

func dirExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && info.IsDir()
}
