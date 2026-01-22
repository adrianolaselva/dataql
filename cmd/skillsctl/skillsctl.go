package skillsctl

import (
	"bufio"
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

//go:embed embedded/skills/*
var skillsFS embed.FS

//go:embed embedded/commands/*
var commandsFS embed.FS

// SkillInfo contains metadata about a skill
type SkillInfo struct {
	Name        string
	Description string
	Path        string
}

// Available skills
var availableSkills = []SkillInfo{
	{
		Name:        "dataql-analysis",
		Description: "Full data analysis with SQL queries on any file format",
		Path:        "embedded/skills/dataql-analysis",
	},
	{
		Name:        "dataql-quick",
		Description: "Quick data inspection and simple queries",
		Path:        "embedded/skills/dataql-quick",
	},
	{
		Name:        "dataql-auto-issue",
		Description: "Auto-create GitHub issues on errors with duplicate validation",
		Path:        "embedded/skills/dataql-auto-issue",
	},
}

// SkillsCtl is the interface for the skills controller
type SkillsCtl interface {
	Command() *cobra.Command
}

type skillsCtl struct {
	global  bool
	project bool
}

// New creates a new SkillsCtl instance
func New() SkillsCtl {
	return &skillsCtl{}
}

// Command returns the cobra command for the skills subcommand
func (c *skillsCtl) Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "skills",
		Short: "Manage Claude Code skills for DataQL",
		Long: `Install, list, and manage Claude Code skills that teach Claude how to use DataQL efficiently.

Skills are markdown files that teach Claude Code how to work with DataQL for data analysis tasks.
Once installed, Claude will automatically use DataQL when you ask about querying data files.`,
	}

	// Subcommands
	cmd.AddCommand(c.installCommand())
	cmd.AddCommand(c.listCommand())
	cmd.AddCommand(c.uninstallCommand())

	return cmd
}

func (c *skillsCtl) installCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Install DataQL skills for Claude Code",
		Long: `Install DataQL skills to your system. Skills can be installed either:
- Globally (~/.config/claude/skills/) - Available in all projects
- Per-project (./.claude/skills/) - Available only in the current project`,
		Example: `  # Interactive installation
  dataql skills install

  # Install to global directory
  dataql skills install --global

  # Install to current project
  dataql skills install --project`,
		RunE: c.runInstall,
	}

	cmd.Flags().BoolVarP(&c.global, "global", "g", false, "Install to global user directory (~/.config/claude/skills/)")
	cmd.Flags().BoolVarP(&c.project, "project", "p", false, "Install to current project directory (./.claude/skills/)")

	return cmd
}

func (c *skillsCtl) listCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List available and installed skills",
		Long:  `Display all available DataQL skills and their installation status.`,
		RunE:  c.runList,
	}
}

func (c *skillsCtl) uninstallCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Remove installed skills",
		Long:  `Remove DataQL skills from your system.`,
		Example: `  # Remove from global directory
  dataql skills uninstall --global

  # Remove from current project
  dataql skills uninstall --project`,
		RunE: c.runUninstall,
	}

	cmd.Flags().BoolVarP(&c.global, "global", "g", false, "Remove from global user directory")
	cmd.Flags().BoolVarP(&c.project, "project", "p", false, "Remove from current project directory")

	return cmd
}

func (c *skillsCtl) runInstall(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true

	fmt.Println("\nDataQL Skills Installer")
	fmt.Println(strings.Repeat("=", 40))

	// Show available skills
	fmt.Println("\nAvailable skills:")
	for i, skill := range availableSkills {
		fmt.Printf("  [%d] %-20s - %s\n", i+1, skill.Name, skill.Description)
	}

	// Determine destination
	var destDir string
	if c.global {
		destDir = getGlobalSkillsDir()
	} else if c.project {
		destDir = getProjectSkillsDir()
	} else {
		// Interactive mode
		fmt.Println("\nWhere do you want to install the skills?")
		fmt.Printf("  [1] Global (%s) - Available in all projects\n", getGlobalSkillsDir())
		fmt.Printf("  [2] Project (%s) - Available only in this project\n", getProjectSkillsDir())
		fmt.Print("\nSelect destination (1 or 2): ")

		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		switch input {
		case "1":
			destDir = getGlobalSkillsDir()
		case "2":
			destDir = getProjectSkillsDir()
		default:
			return fmt.Errorf("invalid selection: %s", input)
		}
	}

	fmt.Printf("\nInstalling skills to: %s\n", destDir)

	// Install each skill
	for _, skill := range availableSkills {
		if err := installSkill(skill, destDir); err != nil {
			fmt.Printf("  x %s - FAILED: %v\n", skill.Name, err)
		} else {
			fmt.Printf("  + %s installed\n", skill.Name)
		}
	}

	// Install commands
	commandsDir := filepath.Join(filepath.Dir(destDir), "commands")
	if err := installCommands(commandsDir); err != nil {
		fmt.Printf("\nWarning: Failed to install commands: %v\n", err)
	} else {
		fmt.Printf("  + commands installed to %s\n", commandsDir)
	}

	fmt.Println("\nSkills installed successfully!")
	fmt.Println("Restart Claude Code to load the new skills.")

	return nil
}

func (c *skillsCtl) runList(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true

	fmt.Println("\nDataQL Skills")
	fmt.Println(strings.Repeat("=", 60))

	fmt.Println("\nAvailable skills:")
	for _, skill := range availableSkills {
		globalInstalled := isSkillInstalled(skill.Name, getGlobalSkillsDir())
		projectInstalled := isSkillInstalled(skill.Name, getProjectSkillsDir())

		status := "not installed"
		if globalInstalled && projectInstalled {
			status = "installed (global + project)"
		} else if globalInstalled {
			status = "installed (global)"
		} else if projectInstalled {
			status = "installed (project)"
		}

		fmt.Printf("  %-20s - %s\n", skill.Name, status)
		fmt.Printf("    %s\n\n", skill.Description)
	}

	fmt.Println("Installation locations:")
	fmt.Printf("  Global:  %s\n", getGlobalSkillsDir())
	fmt.Printf("  Project: %s\n", getProjectSkillsDir())

	return nil
}

func (c *skillsCtl) runUninstall(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true

	if !c.global && !c.project {
		return fmt.Errorf("please specify --global or --project")
	}

	var destDir string
	if c.global {
		destDir = getGlobalSkillsDir()
	} else {
		destDir = getProjectSkillsDir()
	}

	fmt.Printf("\nRemoving skills from: %s\n", destDir)

	for _, skill := range availableSkills {
		skillDir := filepath.Join(destDir, skill.Name)
		if err := os.RemoveAll(skillDir); err != nil {
			if !os.IsNotExist(err) {
				fmt.Printf("  x %s - FAILED: %v\n", skill.Name, err)
			}
		} else {
			fmt.Printf("  - %s removed\n", skill.Name)
		}
	}

	// Remove commands
	commandsDir := filepath.Join(filepath.Dir(destDir), "commands")
	if err := os.RemoveAll(commandsDir); err != nil {
		if !os.IsNotExist(err) {
			fmt.Printf("\nWarning: Failed to remove commands: %v\n", err)
		}
	}

	fmt.Println("\nSkills uninstalled successfully!")

	return nil
}

// Helper functions

func getGlobalSkillsDir() string {
	homeDir, _ := os.UserHomeDir()
	return filepath.Join(homeDir, ".config", "claude", "skills")
}

func getProjectSkillsDir() string {
	cwd, _ := os.Getwd()
	return filepath.Join(cwd, ".claude", "skills")
}

func isSkillInstalled(skillName, dir string) bool {
	skillPath := filepath.Join(dir, skillName, "SKILL.md")
	_, err := os.Stat(skillPath)
	return err == nil
}

func installSkill(skill SkillInfo, destDir string) error {
	// Create skill directory
	skillDir := filepath.Join(destDir, skill.Name)
	if err := os.MkdirAll(skillDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Read skill content from embedded FS
	content, err := skillsFS.ReadFile(filepath.Join(skill.Path, "SKILL.md"))
	if err != nil {
		return fmt.Errorf("failed to read embedded skill: %w", err)
	}

	// Write to destination
	destPath := filepath.Join(skillDir, "SKILL.md")
	if err := os.WriteFile(destPath, content, 0644); err != nil {
		return fmt.Errorf("failed to write skill file: %w", err)
	}

	return nil
}

func installCommands(destDir string) error {
	// Create commands directory
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create commands directory: %w", err)
	}

	// List of commands to install
	commands := []string{"dataql.md", "dataql-schema.md", "dataql-issue.md"}

	for _, cmdFile := range commands {
		content, err := commandsFS.ReadFile(filepath.Join("embedded/commands", cmdFile))
		if err != nil {
			return fmt.Errorf("failed to read embedded command %s: %w", cmdFile, err)
		}

		destPath := filepath.Join(destDir, cmdFile)
		if err := os.WriteFile(destPath, content, 0644); err != nil {
			return fmt.Errorf("failed to write command file %s: %w", cmdFile, err)
		}
	}

	return nil
}
