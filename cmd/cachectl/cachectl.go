package cachectl

import (
	"fmt"
	"os"
	"strings"

	"github.com/adrianolaselva/dataql/pkg/cachehandler"
	"github.com/fatih/color"
	"github.com/rodaine/table"
	"github.com/spf13/cobra"
)

const (
	cacheDirParam      = "cache-dir"
	cacheDirShortParam = "d"
)

// CacheCtl is the interface for the cache controller
type CacheCtl interface {
	Command() *cobra.Command
}

type cacheCtl struct {
	cacheDir string
}

// New creates a new CacheCtl instance
func New() CacheCtl {
	return &cacheCtl{}
}

// Command returns the cobra command for the cache subcommand
func (c *cacheCtl) Command() *cobra.Command {
	command := &cobra.Command{
		Use:   "cache",
		Short: "Manage data cache",
		Long: `Manage the data cache for faster query execution.

The cache stores imported data in DuckDB format, allowing subsequent
queries on the same files to skip the import step.`,
	}

	// Add cache-dir flag to root command
	command.PersistentFlags().StringVarP(&c.cacheDir, cacheDirParam, cacheDirShortParam, "", "cache directory (default: ~/.dataql/cache)")

	// Add subcommands
	command.AddCommand(c.listCommand())
	command.AddCommand(c.clearCommand())
	command.AddCommand(c.statsCommand())

	return command
}

func (c *cacheCtl) listCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all cached entries",
		RunE: func(cmd *cobra.Command, args []string) error {
			handler, err := cachehandler.NewCacheHandler(c.cacheDir, true)
			if err != nil {
				return fmt.Errorf("failed to initialize cache handler: %w", err)
			}

			entries, err := handler.ListCache()
			if err != nil {
				return fmt.Errorf("failed to list cache: %w", err)
			}

			if len(entries) == 0 {
				fmt.Println("No cached entries found.")
				return nil
			}

			// Create table
			tbl := table.New("Key", "Files", "Tables", "Rows", "Size", "Cached At").
				WithHeaderFormatter(color.New(color.FgGreen, color.Underline).SprintfFunc()).
				WithFirstColumnFormatter(color.New(color.FgYellow).SprintfFunc()).
				WithWriter(os.Stdout)

			for _, entry := range entries {
				// Truncate file list if too long
				files := strings.Join(entry.SourceFiles, ", ")
				if len(files) > 50 {
					files = files[:47] + "..."
				}

				tables := strings.Join(entry.Tables, ", ")
				if len(tables) > 30 {
					tables = tables[:27] + "..."
				}

				tbl.AddRow(
					entry.CacheKey[:8]+"...",
					files,
					tables,
					entry.TotalRows,
					cachehandler.FormatSize(entry.SizeBytes),
					entry.CachedAt.Format("2006-01-02 15:04:05"),
				)
			}

			tbl.Print()
			fmt.Printf("\nTotal: %d cached entries\n", len(entries))

			return nil
		},
	}
}

func (c *cacheCtl) clearCommand() *cobra.Command {
	var all bool

	cmd := &cobra.Command{
		Use:   "clear [cache-key]",
		Short: "Clear cache entries",
		Long: `Clear cache entries.

Without arguments, prompts for confirmation before clearing all cache.
With --all flag, clears all cache without prompting.
With a cache-key argument, clears only that specific entry.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			handler, err := cachehandler.NewCacheHandler(c.cacheDir, true)
			if err != nil {
				return fmt.Errorf("failed to initialize cache handler: %w", err)
			}

			if len(args) > 0 {
				// Clear specific entry
				cacheKey := args[0]
				if err := handler.ClearCacheEntry(cacheKey); err != nil {
					return fmt.Errorf("failed to clear cache entry: %w", err)
				}
				fmt.Printf("Cleared cache entry: %s\n", cacheKey)
				return nil
			}

			if !all {
				// Prompt for confirmation
				count, size, err := handler.GetCacheStats()
				if err != nil {
					return fmt.Errorf("failed to get cache stats: %w", err)
				}

				if count == 0 {
					fmt.Println("Cache is already empty.")
					return nil
				}

				fmt.Printf("This will clear %d cache entries (%s).\n", count, cachehandler.FormatSize(size))
				fmt.Print("Are you sure? [y/N] ")

				var response string
				fmt.Scanln(&response)
				if response != "y" && response != "Y" {
					fmt.Println("Aborted.")
					return nil
				}
			}

			if err := handler.ClearCache(); err != nil {
				return fmt.Errorf("failed to clear cache: %w", err)
			}

			return nil
		},
	}

	cmd.Flags().BoolVarP(&all, "all", "a", false, "clear all cache without prompting")

	return cmd
}

func (c *cacheCtl) statsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stats",
		Short: "Show cache statistics",
		RunE: func(cmd *cobra.Command, args []string) error {
			handler, err := cachehandler.NewCacheHandler(c.cacheDir, true)
			if err != nil {
				return fmt.Errorf("failed to initialize cache handler: %w", err)
			}

			count, size, err := handler.GetCacheStats()
			if err != nil {
				return fmt.Errorf("failed to get cache stats: %w", err)
			}

			fmt.Printf("Cache directory: %s\n", handler.GetCacheDir())
			fmt.Printf("Cached entries: %d\n", count)
			fmt.Printf("Total size: %s\n", cachehandler.FormatSize(size))

			return nil
		},
	}
}
