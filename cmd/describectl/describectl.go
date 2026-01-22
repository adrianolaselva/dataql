package describectl

import (
	"fmt"

	"github.com/adrianolaselva/dataql/internal/dataql"
	"github.com/spf13/cobra"
)

const (
	fileParam               = "file"
	fileShortParam          = "f"
	fileDelimiterParam      = "delimiter"
	fileShortDelimiterParam = "d"
	storageParam            = "storage"
	storageShortParam       = "s"
	linesParam              = "lines"
	linesShortParam         = "l"
	tableNameParam          = "collection"
	tableNameShortParam     = "c"
	verboseParam            = "verbose"
	verboseShortParam       = "v"
	inputFormatParam        = "input-format"
	inputFormatShortParam   = "i"
	quietParam              = "quiet"
	quietShortParam         = "Q"
)

// DescribeCtl is the interface for the describe controller
type DescribeCtl interface {
	Command() (*cobra.Command, error)
	runE(cmd *cobra.Command, args []string) error
}

type describeCtl struct {
	params dataql.Params
}

// New creates a new DescribeCtl instance
func New() DescribeCtl {
	return &describeCtl{}
}

// Command returns the cobra command for the describe subcommand
func (c *describeCtl) Command() (*cobra.Command, error) {
	command := &cobra.Command{
		Use:   "describe",
		Short: "Show exploratory statistics for data files",
		Long: `Show comprehensive statistics for data files including:
  - Row count
  - Data types
  - Min/Max values (for numeric and date columns)
  - Mean, median, standard deviation (for numeric columns)
  - Null count per column
  - Unique values count`,
		Example: `  dataql describe -f data.csv
  dataql describe -f sales.json
  dataql describe -f users.parquet -c mydata`,
		RunE: c.runE,
	}

	command.
		PersistentFlags().
		StringArrayVarP(&c.params.FileInputs, fileParam, fileShortParam, []string{}, "origin file (csv, json, etc.)")

	command.
		PersistentFlags().
		StringVarP(&c.params.Delimiter, fileDelimiterParam, fileShortDelimiterParam, ",", "csv delimiter")

	command.
		PersistentFlags().
		StringVarP(&c.params.DataSourceName, storageParam, storageShortParam, "", "DuckDB file path for persistence (default: in-memory)")

	command.
		PersistentFlags().
		IntVarP(&c.params.Lines, linesParam, linesShortParam, 0, "number of lines to be read")

	command.
		PersistentFlags().
		StringVarP(&c.params.Collection, tableNameParam, tableNameShortParam, "", "custom table name (collection) for the imported data")

	command.
		PersistentFlags().
		BoolVarP(&c.params.Verbose, verboseParam, verboseShortParam, false, "enable verbose output with detailed logging")

	command.
		PersistentFlags().
		StringVarP(&c.params.InputFormat, inputFormatParam, inputFormatShortParam, "csv", "input format when using stdin (csv, json, jsonl, xml, yaml)")

	command.
		PersistentFlags().
		BoolVarP(&c.params.Quiet, quietParam, quietShortParam, false, "suppress progress bar output (useful for pipelines)")

	return command, nil
}

func (c *describeCtl) runE(cmd *cobra.Command, _ []string) error {
	cmd.SilenceUsage = true

	// Check if we have file inputs or storage-only mode
	hasFileInputs := len(c.params.FileInputs) > 0
	hasStorage := c.params.DataSourceName != ""

	// If no file inputs and no storage, we need at least one source
	if !hasFileInputs && !hasStorage {
		return fmt.Errorf("either --file or --storage with an existing DuckDB file is required")
	}

	// If no file inputs but storage is provided, describe existing DuckDB
	if !hasFileInputs && hasStorage {
		dql, err := dataql.NewStorageOnly(c.params)
		if err != nil {
			return fmt.Errorf("failed to initialize dataql: %w", err)
		}
		defer func(dql dataql.DataQL) {
			_ = dql.Close()
		}(dql)

		if err := dql.DescribeAll(); err != nil {
			return fmt.Errorf("failed to describe data: %w", err)
		}
		return nil
	}

	// Normal mode with file inputs
	dql, err := dataql.New(c.params)
	if err != nil {
		return fmt.Errorf("failed to initialize dataql: %w", err)
	}
	defer func(dql dataql.DataQL) {
		_ = dql.Close()
	}(dql)

	if err := dql.RunAndDescribe(); err != nil {
		return fmt.Errorf("failed to describe data: %w", err)
	}

	return nil
}
