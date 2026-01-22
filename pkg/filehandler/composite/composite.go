// Package composite provides a file handler that can process multiple files of different formats
package composite

import (
	"github.com/adrianolaselva/dataql/pkg/filehandler"
	avroHandler "github.com/adrianolaselva/dataql/pkg/filehandler/avro"
	csvHandler "github.com/adrianolaselva/dataql/pkg/filehandler/csv"
	excelHandler "github.com/adrianolaselva/dataql/pkg/filehandler/excel"
	jsonHandler "github.com/adrianolaselva/dataql/pkg/filehandler/json"
	jsonlHandler "github.com/adrianolaselva/dataql/pkg/filehandler/jsonl"
	orcHandler "github.com/adrianolaselva/dataql/pkg/filehandler/orc"
	parquetHandler "github.com/adrianolaselva/dataql/pkg/filehandler/parquet"
	xmlHandler "github.com/adrianolaselva/dataql/pkg/filehandler/xml"
	yamlHandler "github.com/adrianolaselva/dataql/pkg/filehandler/yaml"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/schollz/progressbar/v3"
)

// CompositeHandler handles multiple files of different formats
type CompositeHandler struct {
	handlers   []filehandler.FileHandler
	totalLines int
}

// NewCompositeHandler creates a new composite handler for files with mixed formats
func NewCompositeHandler(
	files []string,
	delimiter rune,
	bar *progressbar.ProgressBar,
	storage storage.Storage,
	limitLines int,
	collection string,
) (*CompositeHandler, error) {
	// Group files by format
	filesByFormat, err := filehandler.GroupFilesByFormat(files)
	if err != nil {
		return nil, err
	}

	var handlers []filehandler.FileHandler

	// Create appropriate handler for each format group
	for format, formatFiles := range filesByFormat {
		var handler filehandler.FileHandler

		switch format {
		case filehandler.FormatCSV:
			handler = csvHandler.NewCsvHandler(formatFiles, delimiter, bar, storage, limitLines, collection)
		case filehandler.FormatJSON:
			handler = jsonHandler.NewJsonHandler(formatFiles, bar, storage, limitLines, collection)
		case filehandler.FormatJSONL:
			handler = jsonlHandler.NewJsonlHandler(formatFiles, bar, storage, limitLines, collection)
		case filehandler.FormatXML:
			handler = xmlHandler.NewXmlHandler(formatFiles, bar, storage, limitLines, collection)
		case filehandler.FormatExcel:
			handler = excelHandler.NewExcelHandler(formatFiles, bar, storage, limitLines, collection)
		case filehandler.FormatParquet:
			handler = parquetHandler.NewParquetHandler(formatFiles, bar, storage, limitLines, collection)
		case filehandler.FormatYAML:
			handler = yamlHandler.NewYamlHandler(formatFiles, bar, storage, limitLines, collection)
		case filehandler.FormatAVRO:
			handler = avroHandler.NewAvroHandler(formatFiles, bar, storage, limitLines, collection)
		case filehandler.FormatORC:
			handler = orcHandler.NewOrcHandler(formatFiles, bar, storage, limitLines, collection)
		}

		if handler != nil {
			handlers = append(handlers, handler)
		}
	}

	return &CompositeHandler{
		handlers: handlers,
	}, nil
}

// NewCompositeHandlerWithAliases creates a new composite handler for files with mixed formats and table aliases
func NewCompositeHandlerWithAliases(
	files []string,
	delimiter rune,
	bar *progressbar.ProgressBar,
	storage storage.Storage,
	limitLines int,
	collection string,
	aliases map[string]string,
) (*CompositeHandler, error) {
	// Group files by format
	filesByFormat, err := filehandler.GroupFilesByFormat(files)
	if err != nil {
		return nil, err
	}

	var handlers []filehandler.FileHandler

	// Create appropriate handler for each format group
	for format, formatFiles := range filesByFormat {
		var handler filehandler.FileHandler

		switch format {
		case filehandler.FormatCSV:
			handler = csvHandler.NewCsvHandlerWithAliases(formatFiles, delimiter, bar, storage, limitLines, collection, aliases)
		case filehandler.FormatJSON:
			handler = jsonHandler.NewJsonHandlerWithAliases(formatFiles, bar, storage, limitLines, collection, aliases)
		case filehandler.FormatJSONL:
			handler = jsonlHandler.NewJsonlHandlerWithAliases(formatFiles, bar, storage, limitLines, collection, aliases)
		case filehandler.FormatXML:
			handler = xmlHandler.NewXmlHandlerWithAliases(formatFiles, bar, storage, limitLines, collection, aliases)
		case filehandler.FormatExcel:
			handler = excelHandler.NewExcelHandlerWithAliases(formatFiles, bar, storage, limitLines, collection, aliases)
		case filehandler.FormatParquet:
			handler = parquetHandler.NewParquetHandlerWithAliases(formatFiles, bar, storage, limitLines, collection, aliases)
		case filehandler.FormatYAML:
			handler = yamlHandler.NewYamlHandlerWithAliases(formatFiles, bar, storage, limitLines, collection, aliases)
		case filehandler.FormatAVRO:
			handler = avroHandler.NewAvroHandlerWithAliases(formatFiles, bar, storage, limitLines, collection, aliases)
		case filehandler.FormatORC:
			handler = orcHandler.NewOrcHandlerWithAliases(formatFiles, bar, storage, limitLines, collection, aliases)
		}

		if handler != nil {
			handlers = append(handlers, handler)
		}
	}

	return &CompositeHandler{
		handlers: handlers,
	}, nil
}

// Import imports data from all handlers
func (h *CompositeHandler) Import() error {
	h.totalLines = 0
	for _, handler := range h.handlers {
		if err := handler.Import(); err != nil {
			return err
		}
		h.totalLines += handler.Lines()
	}
	return nil
}

// Lines returns the total number of lines imported across all handlers
func (h *CompositeHandler) Lines() int {
	return h.totalLines
}

// Close closes all handlers
func (h *CompositeHandler) Close() error {
	for _, handler := range h.handlers {
		if err := handler.Close(); err != nil {
			return err
		}
	}
	return nil
}
