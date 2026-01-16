package dataqlctl

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
	queryParam              = "query"
	queryShortParam         = "q"
	storageParam            = "storage"
	storageShortParam       = "s"
	exportParam             = "export"
	exportShortParam        = "e"
	typeParam               = "type"
	typeShortParam          = "t"
	linesParam              = "lines"
	linesShortParam         = "l"
	tableNameParam          = "collection"
	tableNameShortParam     = "c"
	verboseParam            = "verbose"
	verboseShortParam       = "v"
	inputFormatParam        = "input-format"
	inputFormatShortParam   = "i"
)

// DataQlCtl is the interface for the dataql controller
type DataQlCtl interface {
	Command() (*cobra.Command, error)
	runE(cmd *cobra.Command, args []string) error
}

type dataQlCtl struct {
	params dataql.Params
}

// New creates a new DataQlCtl instance
func New() DataQlCtl {
	return &dataQlCtl{}
}

// Command returns the cobra command for the run subcommand
func (c *dataQlCtl) Command() (*cobra.Command, error) {
	command := &cobra.Command{
		Use:     "run",
		Short:   "Load and run queries from data files",
		Long:    `./dataql run -f test.csv -d ";"`,
		Example: `./dataql run -f test.csv -d ";"`,
		RunE:    c.runE,
	}

	command.
		PersistentFlags().
		StringArrayVarP(&c.params.FileInputs, fileParam, fileShortParam, []string{}, "origin file (csv, json, etc.)")

	command.
		PersistentFlags().
		StringVarP(&c.params.Delimiter, fileDelimiterParam, fileShortDelimiterParam, ",", "csv delimiter")

	command.
		PersistentFlags().
		StringVarP(&c.params.Query, queryParam, queryShortParam, "", "SQL query to execute")

	command.
		PersistentFlags().
		StringVarP(&c.params.Export, exportParam, exportShortParam, "", "export path")

	command.
		PersistentFlags().
		StringVarP(&c.params.Type, typeParam, typeShortParam, "", "export format type [`jsonl`,`csv`]")

	command.
		PersistentFlags().
		StringVarP(&c.params.DataSourceName, storageParam, storageShortParam, "", "sqlite file path")

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

	if err := command.MarkPersistentFlagRequired(fileParam); err != nil {
		return nil, fmt.Errorf("failed to validate flag %s: %w", fileParam, err)
	}

	if c.params.Export != "" && c.params.Type == "" {
		return nil, fmt.Errorf("export type is required when export path is specified")
	}

	return command, nil
}

func (c *dataQlCtl) runE(cmd *cobra.Command, _ []string) error {
	cmd.SilenceUsage = true
	dql, err := dataql.New(c.params)
	if err != nil {
		return fmt.Errorf("failed to initialize dataql: %w", err)
	}
	defer func(dql dataql.DataQL) {
		_ = dql.Close()
	}(dql)

	if err := dql.Run(); err != nil {
		return fmt.Errorf("failed to run dataql: %w", err)
	}

	return nil
}
