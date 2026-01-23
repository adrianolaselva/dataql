package describectl

import (
	"testing"
)

func TestNew(t *testing.T) {
	ctl := New()
	if ctl == nil {
		t.Error("New() should not return nil")
	}
}

func TestCommand(t *testing.T) {
	ctl := New()
	cmd, err := ctl.Command()
	if err != nil {
		t.Errorf("Command() returned error: %v", err)
	}
	if cmd == nil {
		t.Error("Command() should not return nil")
	}

	// Check command properties
	if cmd.Use != "describe" {
		t.Errorf("Expected Use to be 'describe', got '%s'", cmd.Use)
	}

	if cmd.Short == "" {
		t.Error("Short description should not be empty")
	}

	if cmd.Long == "" {
		t.Error("Long description should not be empty")
	}

	if cmd.Example == "" {
		t.Error("Example should not be empty")
	}
}

func TestCommand_Flags(t *testing.T) {
	ctl := New()
	cmd, err := ctl.Command()
	if err != nil {
		t.Fatalf("Command() returned error: %v", err)
	}

	// Test required flags exist
	flags := []struct {
		name      string
		shorthand string
	}{
		{"file", "f"},
		{"delimiter", "d"},
		{"storage", "s"},
		{"lines", "l"},
		{"collection", "c"},
		{"verbose", "v"},
		{"input-format", "i"},
		{"quiet", "Q"},
	}

	for _, flag := range flags {
		f := cmd.PersistentFlags().Lookup(flag.name)
		if f == nil {
			t.Errorf("Flag '%s' should exist", flag.name)
			continue
		}
		if f.Shorthand != flag.shorthand {
			t.Errorf("Flag '%s' shorthand should be '%s', got '%s'", flag.name, flag.shorthand, f.Shorthand)
		}
	}
}

func TestCommand_Defaults(t *testing.T) {
	ctl := New()
	cmd, err := ctl.Command()
	if err != nil {
		t.Fatalf("Command() returned error: %v", err)
	}

	// Check default values
	delimiterFlag := cmd.PersistentFlags().Lookup("delimiter")
	if delimiterFlag.DefValue != "," {
		t.Errorf("Default delimiter should be ',', got '%s'", delimiterFlag.DefValue)
	}

	inputFormatFlag := cmd.PersistentFlags().Lookup("input-format")
	if inputFormatFlag.DefValue != "csv" {
		t.Errorf("Default input-format should be 'csv', got '%s'", inputFormatFlag.DefValue)
	}
}
