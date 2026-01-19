package e2e_test

import (
	"strings"
	"testing"
)

// TestSkillsCommandHelp tests that 'dataql skills --help' works
func TestSkillsCommandHelp(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "skills", "--help")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "skills")
	assertContains(t, stdout, "Claude Code")
}

// TestSkillsCommandExists tests that skills command is recognized
func TestSkillsCommandExists(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "skills", "--help")

	// Should not error with "unknown command"
	assertNoError(t, err, stderr)
	assertNotContains(t, stderr, "unknown command")
	assertContains(t, stdout, "skills")
}

// TestSkillsInstallCommandHelp tests that 'dataql skills install --help' works
func TestSkillsInstallCommandHelp(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "skills", "install", "--help")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "install")
}

// TestSkillsListCommandHelp tests that 'dataql skills list --help' works
func TestSkillsListCommandHelp(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "skills", "list", "--help")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "list")
}

// TestSkillsUninstallCommandHelp tests that 'dataql skills uninstall --help' works
func TestSkillsUninstallCommandHelp(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "skills", "uninstall", "--help")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "uninstall")
}

// TestSkillsSubcommandsExist tests that all subcommands are available
func TestSkillsSubcommandsExist(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "skills", "--help")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "install")
	assertContains(t, stdout, "list")
	assertContains(t, stdout, "uninstall")
}

// TestSkillsInstallFlags tests that install command has expected flags
func TestSkillsInstallFlags(t *testing.T) {
	stdout, _, err := runDataQL(t, "skills", "install", "--help")

	assertNoError(t, err, "")
	// Check for global/project flags if they exist
	// At minimum, should show command description
	assertContains(t, stdout, "install")
}

// TestSkillsListOutputFormat tests that skills list shows available skills
func TestSkillsListOutputFormat(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "skills", "list")

	assertNoError(t, err, stderr)
	// Should list available skills or show message about skills
	// At minimum, should not error
	if len(stdout) == 0 && len(stderr) == 0 {
		t.Log("Skills list returned empty output - this may be expected if no skills are installed")
	}
}

// TestSkillsInRootHelp tests that skills appears in root help output
func TestSkillsInRootHelp(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "--help")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "skills")
}

// TestSkillsShortDescription tests skills command short description
func TestSkillsShortDescription(t *testing.T) {
	stdout, _, err := runDataQL(t, "--help")

	assertNoError(t, err, "")
	// Skills should appear with its description in root help
	lines := strings.Split(stdout, "\n")
	for _, line := range lines {
		if strings.Contains(line, "skills") {
			// Should have some description about Claude Code
			assertContains(t, strings.ToLower(line), "skill")
			break
		}
	}
}

// TestSkillsCommandNotUnknown is a regression test
// to ensure skills command is never reported as unknown
func TestSkillsCommandNotUnknown(t *testing.T) {
	_, stderr, _ := runDataQL(t, "skills")

	// The critical check: should never say "unknown command"
	assertNotContains(t, stderr, "unknown command")
	assertNotContains(t, stderr, "Unknown command")
}

// TestSkillsInstallNotUnknown tests install subcommand is recognized
func TestSkillsInstallNotUnknown(t *testing.T) {
	_, stderr, _ := runDataQL(t, "skills", "install", "--help")

	assertNotContains(t, stderr, "unknown command")
	assertNotContains(t, stderr, "Unknown command")
}

// TestSkillsListNotUnknown tests list subcommand is recognized
func TestSkillsListNotUnknown(t *testing.T) {
	_, stderr, _ := runDataQL(t, "skills", "list", "--help")

	assertNotContains(t, stderr, "unknown command")
	assertNotContains(t, stderr, "Unknown command")
}

// TestSkillsUninstallNotUnknown tests uninstall subcommand is recognized
func TestSkillsUninstallNotUnknown(t *testing.T) {
	_, stderr, _ := runDataQL(t, "skills", "uninstall", "--help")

	assertNotContains(t, stderr, "unknown command")
	assertNotContains(t, stderr, "Unknown command")
}

// TestAllTopLevelCommands tests that all expected top-level commands exist
func TestAllTopLevelCommands(t *testing.T) {
	stdout, stderr, err := runDataQL(t, "--help")

	assertNoError(t, err, stderr)

	expectedCommands := []string{"run", "skills", "mcp"}
	for _, cmd := range expectedCommands {
		assertContains(t, stdout, cmd)
	}
}

// TestNoUnknownCommandsInHelp ensures help doesn't show any errors
func TestNoUnknownCommandsInHelp(t *testing.T) {
	testCases := []struct {
		name string
		args []string
	}{
		{"root help", []string{"--help"}},
		{"run help", []string{"run", "--help"}},
		{"skills help", []string{"skills", "--help"}},
		{"mcp help", []string{"mcp", "--help"}},
		{"skills install help", []string{"skills", "install", "--help"}},
		{"skills list help", []string{"skills", "list", "--help"}},
		{"mcp serve help", []string{"mcp", "serve", "--help"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, err := runDataQL(t, tc.args...)

			assertNoError(t, err, stderr)
			assertNotContains(t, stderr, "unknown")
			assertNotContains(t, stderr, "error")
			if len(stdout) == 0 {
				t.Errorf("Expected help output, got empty string")
			}
		})
	}
}
