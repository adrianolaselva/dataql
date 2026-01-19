package cmd

import (
	"strings"
	"testing"

	"github.com/adrianolaselva/dataql/cmd/dataqlctl"
	"github.com/adrianolaselva/dataql/cmd/mcpctl"
	"github.com/adrianolaselva/dataql/cmd/skillsctl"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewCLI tests that CLI is properly created
func TestNewCLI(t *testing.T) {
	cli := New()
	require.NotNil(t, cli, "CLI should not be nil")

	cliImpl, ok := cli.(*cliBase)
	require.True(t, ok, "CLI should be of type *cliBase")
	require.NotNil(t, cliImpl.rootCmd, "Root command should not be nil")
	assert.Equal(t, "dataql", cliImpl.rootCmd.Use, "Root command should be 'dataql'")
}

// TestRootCommandConfiguration tests root command configuration
func TestRootCommandConfiguration(t *testing.T) {
	cli := New()
	cliImpl := cli.(*cliBase)

	assert.Equal(t, "dataql", cliImpl.rootCmd.Use)
	assert.NotEmpty(t, cliImpl.rootCmd.Long, "Long description should be set")
	assert.NotEmpty(t, cliImpl.rootCmd.Version, "Version should be set")
}

// TestAllCommandsCanBeRegistered ensures all expected commands can be created and registered
// This test prevents regression where commands might fail to initialize
func TestAllCommandsCanBeRegistered(t *testing.T) {
	rootCmd := &cobra.Command{
		Use: "dataql",
	}

	// Test dataqlctl command registration
	t.Run("dataqlctl command", func(t *testing.T) {
		dataQlCtl, err := dataqlctl.New().Command()
		require.NoError(t, err, "dataqlctl.Command() should not error")
		require.NotNil(t, dataQlCtl, "dataqlctl command should not be nil")
		assert.Equal(t, "run", dataQlCtl.Use, "dataqlctl command should be 'run'")
		rootCmd.AddCommand(dataQlCtl)
	})

	// Test skillsctl command registration
	t.Run("skillsctl command", func(t *testing.T) {
		skillsCmd := skillsctl.New().Command()
		require.NotNil(t, skillsCmd, "skillsctl command should not be nil")
		assert.Equal(t, "skills", skillsCmd.Use, "skillsctl command should be 'skills'")
		rootCmd.AddCommand(skillsCmd)
	})

	// Test mcpctl command registration - CRITICAL for LLM integration
	t.Run("mcpctl command", func(t *testing.T) {
		mcpCmd := mcpctl.New().Command()
		require.NotNil(t, mcpCmd, "mcpctl command should not be nil")
		assert.Equal(t, "mcp", mcpCmd.Use, "mcpctl command should be 'mcp'")
		rootCmd.AddCommand(mcpCmd)
	})

	// Verify all commands are registered on root
	commands := rootCmd.Commands()
	commandNames := make(map[string]bool)
	for _, cmd := range commands {
		commandNames[cmd.Use] = true
	}

	assert.True(t, commandNames["run"], "run command should be registered on root")
	assert.True(t, commandNames["skills"], "skills command should be registered on root")
	assert.True(t, commandNames["mcp"], "mcp command should be registered on root")
}

// TestMCPCommandStructure tests MCP command structure
// This is critical because MCP server needs 'serve' subcommand
func TestMCPCommandStructure(t *testing.T) {
	mcpCmd := mcpctl.New().Command()
	require.NotNil(t, mcpCmd)

	// Check MCP command properties
	assert.Equal(t, "mcp", mcpCmd.Use)
	// Short description should mention MCP or Model Context Protocol
	assert.True(t,
		strings.Contains(mcpCmd.Short, "MCP") || strings.Contains(mcpCmd.Short, "Model Context Protocol"),
		"Short description should mention MCP or Model Context Protocol")
	assert.Contains(t, mcpCmd.Long, "Model Context Protocol", "Long description should explain MCP")

	// Check 'serve' subcommand exists
	subcommands := mcpCmd.Commands()
	var serveFound bool
	for _, cmd := range subcommands {
		if cmd.Use == "serve" {
			serveFound = true
			assert.Contains(t, cmd.Short, "Start", "Serve command should indicate it starts the server")
			break
		}
	}
	assert.True(t, serveFound, "MCP command should have 'serve' subcommand")
}

// TestSkillsCommandStructure tests skills command structure
func TestSkillsCommandStructure(t *testing.T) {
	skillsCmd := skillsctl.New().Command()
	require.NotNil(t, skillsCmd)

	assert.Equal(t, "skills", skillsCmd.Use)
	assert.Contains(t, skillsCmd.Short, "Claude Code", "Short description should mention Claude Code")

	// Check expected subcommands
	subcommands := skillsCmd.Commands()
	subcommandNames := make(map[string]bool)
	for _, cmd := range subcommands {
		subcommandNames[cmd.Use] = true
	}

	assert.True(t, subcommandNames["install"], "skills should have 'install' subcommand")
	assert.True(t, subcommandNames["list"], "skills should have 'list' subcommand")
	assert.True(t, subcommandNames["uninstall"], "skills should have 'uninstall' subcommand")
}

// TestDataQLCommandStructure tests run command structure
func TestDataQLCommandStructure(t *testing.T) {
	runCmd, err := dataqlctl.New().Command()
	require.NoError(t, err)
	require.NotNil(t, runCmd)

	assert.Equal(t, "run", runCmd.Use)

	// Check essential flags exist (they are PersistentFlags)
	flags := runCmd.PersistentFlags()
	assert.NotNil(t, flags.Lookup("file"), "run should have --file flag")
	assert.NotNil(t, flags.Lookup("query"), "run should have --query flag")
	assert.NotNil(t, flags.Lookup("delimiter"), "run should have --delimiter flag")
}

// TestCommandCount ensures we have exactly 3 top-level commands
func TestCommandCount(t *testing.T) {
	rootCmd := &cobra.Command{Use: "dataql"}

	// Register all commands
	dataQlCtl, _ := dataqlctl.New().Command()
	rootCmd.AddCommand(dataQlCtl)
	rootCmd.AddCommand(skillsctl.New().Command())
	rootCmd.AddCommand(mcpctl.New().Command())

	// We expect exactly 3 user-facing commands (excluding help/completion)
	userCommands := 0
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use != "help" && cmd.Use != "completion" {
			userCommands++
		}
	}

	assert.Equal(t, 3, userCommands, "Should have exactly 3 user-facing commands: run, skills, mcp")
}
