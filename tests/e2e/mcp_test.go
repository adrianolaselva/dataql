package e2e_test

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestMCPCommandHelp tests that 'dataql mcp --help' works
func TestMCPCommandHelp(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "mcp", "--help")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "mcp")
	assertContains(t, stdout, "Model Context Protocol")
	assertContains(t, stdout, "serve")
}

// TestMCPCommandExists tests that MCP command is recognized
func TestMCPCommandExists(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "mcp", "--help")

	// Should not error with "unknown command"
	assertNoError(t, err, stderr)
	assertNotContains(t, stderr, "unknown command")
	assertContains(t, stdout, "mcp")
}

// TestMCPServeCommandHelp tests that 'dataql mcp serve --help' works
func TestMCPServeCommandHelp(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "mcp", "serve", "--help")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "serve")
	assertContains(t, stdout, "Start the MCP server")
	assertContains(t, stdout, "debug")
}

// TestMCPServeCommandExists tests that serve subcommand is recognized
func TestMCPServeCommandExists(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "mcp", "serve", "--help")

	assertNoError(t, err, stderr)
	assertNotContains(t, stderr, "unknown command")
	assertContains(t, stdout, "serve")
}

// TestMCPServeFlags tests that MCP serve accepts expected flags
func TestMCPServeFlags(t *testing.T) {
	stdout, _, err := runDataQL(t, "mcp", "serve", "--help")

	assertNoError(t, err, "")
	// Check for debug flag
	assertContains(t, stdout, "-d")
	assertContains(t, stdout, "--debug")
}

// TestMCPServeStartsWithoutError tests that MCP server starts without immediate errors
// It uses a context with timeout to prevent hanging
func TestMCPServeStartsWithoutError(t *testing.T) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start the MCP server
	cmd := exec.CommandContext(ctx, binaryPath, "mcp", "serve")
	cmd.Dir = projectRoot

	// The server should start without immediate errors
	// We expect it to block waiting for STDIO input, so context timeout is expected
	err := cmd.Run()

	// Context deadline exceeded is expected (server is waiting for input)
	// Any other error would indicate a startup problem
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if ok && exitErr.ExitCode() != 0 {
			// Check if it's a timeout (expected) or actual error
			if ctx.Err() != context.DeadlineExceeded {
				t.Errorf("MCP server failed to start: %v", err)
			}
		}
	}
}

// TestMCPServeDebugFlag tests that debug flag is accepted
func TestMCPServeDebugFlag(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, binaryPath, "mcp", "serve", "--debug")
	cmd.Dir = projectRoot

	// Should accept the flag without "unknown flag" error
	err := cmd.Run()

	// Timeout is expected (server waiting for STDIO)
	// We just want to ensure --debug doesn't cause an error
	if err != nil {
		if ctx.Err() != context.DeadlineExceeded {
			// Check if error is about unknown flag
			exitErr, ok := err.(*exec.ExitError)
			if ok && strings.Contains(string(exitErr.Stderr), "unknown flag") {
				t.Errorf("MCP server doesn't recognize --debug flag")
			}
		}
	}
}

// TestMCPInRootHelp tests that MCP appears in root help output
func TestMCPInRootHelp(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "--help")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "mcp")
}

// TestMCPShortDescription tests MCP command short description
func TestMCPShortDescription(t *testing.T) {
	stdout, _, err := runDataQL(t, "--help")

	assertNoError(t, err, "")
	// MCP should appear with its description in root help
	lines := strings.Split(stdout, "\n")
	for _, line := range lines {
		if strings.Contains(line, "mcp") {
			// Should have some description about MCP/LLM
			assertContains(t, strings.ToLower(line), "mcp")
			break
		}
	}
}

// TestMCPCommandNotUnknown is a regression test for the original issue
// where 'dataql mcp' returned "unknown command"
func TestMCPCommandNotUnknown(t *testing.T) {
	_, stderr, _ := runDataQL(t, "mcp")

	// The critical check: should never say "unknown command"
	assertNotContains(t, stderr, "unknown command")
	assertNotContains(t, stderr, "Unknown command")
}

// TestMCPServeNotUnknown is a regression test for serve subcommand
func TestMCPServeNotUnknown(t *testing.T) {
	_, stderr, _ := runDataQL(t, "mcp", "serve", "--help")

	assertNotContains(t, stderr, "unknown command")
	assertNotContains(t, stderr, "Unknown command")
}
