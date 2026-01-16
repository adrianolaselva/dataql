package e2e_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

var binaryPath string
var fixturesPath string
var projectRoot string

func TestMain(m *testing.M) {
	// Get project root
	projectRoot = getProjectRoot()

	// Build binary before tests
	binaryName := "dataql_test"
	if runtime.GOOS == "windows" {
		binaryName = "dataql_test.exe"
	}

	// Explicitly set CGO_ENABLED=1 for sqlite3
	// Build from root main.go (not cmd/main.go which is a library)
	cmd := exec.Command("go", "build", "-o", binaryName, ".")
	cmd.Dir = projectRoot
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		panic("Failed to build binary: " + err.Error() + "\nOutput: " + string(output))
	}

	binaryPath = filepath.Join(projectRoot, binaryName)
	fixturesPath = filepath.Join(projectRoot, "tests", "fixtures")

	code := m.Run()

	// Cleanup
	os.Remove(binaryPath)
	os.Exit(code)
}

func getProjectRoot() string {
	// Get the directory of this test file
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("Failed to get current file path")
	}

	// Go up from tests/e2e to project root
	dir := filepath.Dir(filename)
	return filepath.Join(dir, "..", "..")
}

// runDataQL executes the dataql binary with the given arguments
func runDataQL(t *testing.T, args ...string) (stdout string, stderr string, err error) {
	t.Helper()
	cmd := exec.Command(binaryPath, args...)
	cmd.Dir = projectRoot

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err = cmd.Run()
	return stdoutBuf.String(), stderrBuf.String(), err
}

// fixture returns the full path to a fixture file
func fixture(name string) string {
	return filepath.Join(fixturesPath, name)
}

// tempFile creates a temporary file and returns its path
func tempFile(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(t.TempDir(), name)
}

// assertContains checks if output contains expected string
func assertContains(t *testing.T, output, expected string) {
	t.Helper()
	if !strings.Contains(output, expected) {
		t.Errorf("Expected output to contain %q, but got:\n%s", expected, output)
	}
}

// assertNotContains checks if output does not contain the string
func assertNotContains(t *testing.T, output, notExpected string) {
	t.Helper()
	if strings.Contains(output, notExpected) {
		t.Errorf("Expected output to NOT contain %q, but it did:\n%s", notExpected, output)
	}
}

// assertNoError checks that there was no error
func assertNoError(t *testing.T, err error, stderr string) {
	t.Helper()
	if err != nil {
		t.Errorf("Expected no error, but got: %v\nStderr: %s", err, stderr)
	}
}

// assertError checks that there was an error
func assertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Error("Expected an error, but got none")
	}
}

// fileExists checks if a file exists
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// readFile reads the content of a file
func readFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read file %s: %v", path, err)
	}
	return string(content)
}

// countLines counts the number of non-empty lines in a string
func countLines(s string) int {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	count := 0
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}

// runDataQLWithStdin executes the dataql binary with the given stdin input
func runDataQLWithStdin(t *testing.T, stdinData string, args ...string) (stdout string, stderr string, err error) {
	t.Helper()
	cmd := exec.Command(binaryPath, args...)
	cmd.Dir = projectRoot
	cmd.Stdin = strings.NewReader(stdinData)

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err = cmd.Run()
	return stdoutBuf.String(), stderrBuf.String(), err
}
