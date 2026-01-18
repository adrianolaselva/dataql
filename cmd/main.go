package cmd

import (
	"fmt"
	"github.com/adrianolaselva/dataql/cmd/dataqlctl"
	"github.com/spf13/cobra"
	"syscall"
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
	var release = "latest"
	if value, ok := syscall.Getenv("VERSION"); ok {
		release = value
	}

	cmd := &cobra.Command{
		Use:     commandBase,
		Version: release,
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

	if err := c.rootCmd.Execute(); err != nil {
		return fmt.Errorf("failed to execute command %w", err)
	}

	return nil
}
