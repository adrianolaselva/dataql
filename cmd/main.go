package cmd

import (
	"fmt"

	"github.com/adrianolaselva/dataql/cmd/dataqlctl"
	"github.com/adrianolaselva/dataql/cmd/describectl"
	"github.com/adrianolaselva/dataql/cmd/mcpctl"
	"github.com/adrianolaselva/dataql/cmd/skillsctl"
	"github.com/adrianolaselva/dataql/internal/dataql"
	"github.com/spf13/cobra"
)

// Build information - set via ldflags during build
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

const (
	commandBase = "dataql"
	bannerPrint = `DataQL - Query and transform data across multiple formats`
)

type CliBase interface {
	Execute() error
}

type cliBase struct {
	rootCmd *cobra.Command
}

func New() CliBase {
	// Propagate version to internal packages for REPL .version command
	dataql.Version = Version

	versionInfo := Version
	if Commit != "unknown" {
		versionInfo = fmt.Sprintf("%s (commit: %s, built: %s)", Version, Commit, BuildDate)
	}

	cmd := &cobra.Command{
		Use:     commandBase,
		Version: versionInfo,
		Long:    bannerPrint,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   false,
			DisableNoDescFlag:   false,
			DisableDescriptions: false,
			HiddenDefaultCmd:    true,
		},
	}

	return &cliBase{rootCmd: cmd}
}

func (c *cliBase) Execute() error {
	dataQlCtl, err := dataqlctl.New().Command()
	if err != nil {
		return fmt.Errorf("failed to execute command: %w", err)
	}

	c.rootCmd.AddCommand(dataQlCtl)

	// Add describe command for exploratory statistics
	describeCmd, err := describectl.New().Command()
	if err != nil {
		return fmt.Errorf("failed to initialize describe command: %w", err)
	}
	c.rootCmd.AddCommand(describeCmd)

	// Add skills command for Claude Code integration
	c.rootCmd.AddCommand(skillsctl.New().Command())

	// Add MCP server command for LLM integration
	c.rootCmd.AddCommand(mcpctl.New().Command())

	if err := c.rootCmd.Execute(); err != nil {
		return fmt.Errorf("failed to execute command %w", err)
	}

	return nil
}
