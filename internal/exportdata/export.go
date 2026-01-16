package exportdata

import (
	"database/sql"
	"fmt"

	"github.com/adrianolaselva/dataql/pkg/exportdata"
	"github.com/adrianolaselva/dataql/pkg/exportdata/csv"
	"github.com/adrianolaselva/dataql/pkg/exportdata/excel"
	"github.com/adrianolaselva/dataql/pkg/exportdata/json"
	"github.com/adrianolaselva/dataql/pkg/exportdata/jsonl"
	"github.com/adrianolaselva/dataql/pkg/exportdata/parquet"
	"github.com/adrianolaselva/dataql/pkg/exportdata/xml"
	exportyaml "github.com/adrianolaselva/dataql/pkg/exportdata/yaml"
	"github.com/schollz/progressbar/v3"
)

const (
	CSVLineExportType   = "csv"
	JSONLineExportType  = "jsonl"
	JSONExportType      = "json"
	ExcelExportType     = "excel"
	ExcelXLSXExportType = "xlsx"
	ParquetExportType   = "parquet"
	XMLExportType       = "xml"
	YAMLExportType      = "yaml"
	YMLExportType       = "yml"
)

func NewExport(exportType string, rows *sql.Rows, exportPath string, bar *progressbar.ProgressBar) (exportdata.Export, error) {
	switch exportType {
	case CSVLineExportType:
		return csv.NewCsvExport(rows, exportPath, bar), nil
	case JSONLineExportType:
		return jsonl.NewJsonlExport(rows, exportPath, bar), nil
	case JSONExportType:
		return json.NewJsonExport(rows, exportPath, bar), nil
	case ExcelExportType, ExcelXLSXExportType:
		return excel.NewExcelExport(rows, exportPath, bar), nil
	case ParquetExportType:
		return parquet.NewParquetExport(rows, exportPath, bar), nil
	case XMLExportType:
		return xml.NewXmlExport(rows, exportPath, bar), nil
	case YAMLExportType, YMLExportType:
		return exportyaml.NewYamlExport(rows, exportPath, bar), nil
	}

	return nil, fmt.Errorf("export type %s not defined", exportType)
}
