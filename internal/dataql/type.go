package dataql

type Params struct {
	FileInputs     []string
	DataSourceName string
	Delimiter      string
	Query          string
	Export         string
	Type           string
	Lines          int
	Collection     string
	Verbose        bool
	Quiet          bool   // Suppress progress bar output
	NoSchema       bool   // Suppress table schema display before query results
	InputFormat    string // Input format for stdin (csv, json, jsonl, xml, yaml)
}
