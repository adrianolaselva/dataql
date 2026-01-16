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
	InputFormat    string // Input format for stdin (csv, json, jsonl, xml, yaml)
}
