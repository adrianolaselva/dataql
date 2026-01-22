package e2e_test

import (
	"compress/gzip"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// createGzipFile creates a gzip compressed file with the given content
func createGzipFile(t *testing.T, path string, content string) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Failed to create gzip file: %v", err)
	}
	defer file.Close()

	gzWriter := gzip.NewWriter(file)
	defer gzWriter.Close()

	if _, err := gzWriter.Write([]byte(content)); err != nil {
		t.Fatalf("Failed to write gzip content: %v", err)
	}
}

// createBz2File creates a bzip2 compressed file using the bzip2 command
func createBz2File(t *testing.T, uncompressedPath string) string {
	t.Helper()

	// Check if bzip2 is available
	if _, err := exec.LookPath("bzip2"); err != nil {
		t.Skip("bzip2 command not available")
	}

	cmd := exec.Command("bzip2", "-k", "-f", uncompressedPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to create bz2 file: %v\nOutput: %s", err, output)
	}

	return uncompressedPath + ".bz2"
}

// createXzFile creates an xz compressed file using the xz command
func createXzFile(t *testing.T, uncompressedPath string) string {
	t.Helper()

	// Check if xz is available
	if _, err := exec.LookPath("xz"); err != nil {
		t.Skip("xz command not available")
	}

	cmd := exec.Command("xz", "-k", "-f", uncompressedPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to create xz file: %v\nOutput: %s", err, output)
	}

	return uncompressedPath + ".xz"
}

func TestCompression_GzipCSV(t *testing.T) {
	// Create a temp gzip file
	tmpDir := t.TempDir()
	gzPath := filepath.Join(tmpDir, "data.csv.gz")

	content := "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n"
	createGzipFile(t, gzPath, content)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", gzPath,
		"-q", "SELECT * FROM data ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "Charlie")
	assertContains(t, stdout, "(3 rows)")
}

func TestCompression_GzipJSON(t *testing.T) {
	// Create a temp gzip file
	tmpDir := t.TempDir()
	gzPath := filepath.Join(tmpDir, "users.json.gz")

	content := `[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`
	createGzipFile(t, gzPath, content)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", gzPath,
		"-q", "SELECT * FROM users ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "(2 rows)")
}

func TestCompression_GzipJSONL(t *testing.T) {
	// Create a temp gzip file
	tmpDir := t.TempDir()
	gzPath := filepath.Join(tmpDir, "events.jsonl.gz")

	content := `{"event": "login", "user": "alice"}
{"event": "logout", "user": "bob"}
{"event": "purchase", "user": "charlie"}
`
	createGzipFile(t, gzPath, content)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", gzPath,
		"-q", "SELECT * FROM events")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "login")
	assertContains(t, stdout, "logout")
	assertContains(t, stdout, "purchase")
	assertContains(t, stdout, "(3 rows)")
}

func TestCompression_Bzip2CSV(t *testing.T) {
	// Create a temp file and compress with bzip2
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "bz2data.csv")

	content := "id,name,value\n1,Alice,100\n2,Bob,200\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}

	bz2Path := createBz2File(t, csvPath)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", bz2Path,
		"-q", "SELECT * FROM bz2data ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "(2 rows)")
}

func TestCompression_XzCSV(t *testing.T) {
	// Create a temp file and compress with xz
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "xzdata.csv")

	content := "id,name,value\n1,Alice,100\n2,Bob,200\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}

	xzPath := createXzFile(t, csvPath)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", xzPath,
		"-q", "SELECT * FROM xzdata ORDER BY id")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "(2 rows)")
}

func TestCompression_WithExplicitAlias(t *testing.T) {
	// Create a temp gzip file
	tmpDir := t.TempDir()
	gzPath := filepath.Join(tmpDir, "compressed.csv.gz")

	content := "id,name,value\n1,Test,100\n"
	createGzipFile(t, gzPath, content)

	// Use explicit alias
	stdout, stderr, err := runDataQL(t, "run",
		"-f", gzPath+":myalias",
		"-q", "SELECT * FROM myalias")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Test")
	assertContains(t, stdout, "(1 row")
}

func TestCompression_WithCollection(t *testing.T) {
	// Create a temp gzip file
	tmpDir := t.TempDir()
	gzPath := filepath.Join(tmpDir, "data.csv.gz")

	content := "id,name,value\n1,Test,100\n2,Test2,200\n"
	createGzipFile(t, gzPath, content)

	// Use collection name
	stdout, stderr, err := runDataQL(t, "run",
		"-f", gzPath,
		"-c", "mycollection",
		"-q", "SELECT * FROM mycollection")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Test")
	assertContains(t, stdout, "(2 rows)")
}

func TestCompression_WithLineLimit(t *testing.T) {
	// Create a temp gzip file with more data
	tmpDir := t.TempDir()
	gzPath := filepath.Join(tmpDir, "large.csv.gz")

	content := "id,name\n1,A\n2,B\n3,C\n4,D\n5,E\n6,F\n7,G\n8,H\n9,I\n10,J\n"
	createGzipFile(t, gzPath, content)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", gzPath,
		"-l", "5",
		"-q", "SELECT COUNT(*) as count FROM large")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "5")
}

func TestCompression_WhereClause(t *testing.T) {
	// Create a temp gzip file
	tmpDir := t.TempDir()
	gzPath := filepath.Join(tmpDir, "filter.csv.gz")

	content := "id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35\n4,Diana,25\n"
	createGzipFile(t, gzPath, content)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", gzPath,
		"-q", "SELECT name FROM filter WHERE age = 25 ORDER BY name")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "Diana")
	assertContains(t, stdout, "(2 rows)")
}

func TestCompression_Aggregation(t *testing.T) {
	// Create a temp gzip file
	tmpDir := t.TempDir()
	gzPath := filepath.Join(tmpDir, "sales.csv.gz")

	content := "product,amount\nA,100\nB,200\nA,150\nB,50\n"
	createGzipFile(t, gzPath, content)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", gzPath,
		"-q", "SELECT product, SUM(amount) as total FROM sales GROUP BY product ORDER BY product")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "A")
	assertContains(t, stdout, "250")
	assertContains(t, stdout, "B")
}

func TestCompression_Export(t *testing.T) {
	// Create a temp gzip file
	tmpDir := t.TempDir()
	gzPath := filepath.Join(tmpDir, "source.csv.gz")
	exportPath := filepath.Join(tmpDir, "exported.csv")

	content := "id,name\n1,Alice\n2,Bob\n"
	createGzipFile(t, gzPath, content)

	_, stderr, err := runDataQL(t, "run",
		"-f", gzPath,
		"-q", "SELECT * FROM source ORDER BY id",
		"-e", exportPath,
		"-t", "csv")

	assertNoError(t, err, stderr)

	// Verify exported file exists and has content
	exportedContent, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read exported file: %v", err)
	}

	if len(exportedContent) == 0 {
		t.Error("Exported file is empty")
	}

	assertContains(t, string(exportedContent), "Alice")
	assertContains(t, string(exportedContent), "Bob")
}

func TestCompression_InvalidFile(t *testing.T) {
	// Create a temp file that looks like gzip but isn't
	tmpDir := t.TempDir()
	fakePath := filepath.Join(tmpDir, "fake.csv.gz")

	if err := os.WriteFile(fakePath, []byte("this is not gzip content"), 0644); err != nil {
		t.Fatalf("Failed to write fake file: %v", err)
	}

	_, _, err := runDataQL(t, "run",
		"-f", fakePath,
		"-q", "SELECT * FROM fake")

	assertError(t, err)
}

func TestCompression_MultipleFiles(t *testing.T) {
	// Create two gzip files
	tmpDir := t.TempDir()
	gzPath1 := filepath.Join(tmpDir, "users.csv.gz")
	gzPath2 := filepath.Join(tmpDir, "orders.csv.gz")

	content1 := "id,name\n1,Alice\n2,Bob\n"
	content2 := "order_id,user_id,amount\n100,1,50\n101,2,75\n"

	createGzipFile(t, gzPath1, content1)
	createGzipFile(t, gzPath2, content2)

	stdout, stderr, err := runDataQL(t, "run",
		"-f", gzPath1,
		"-f", gzPath2,
		"-q", "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY u.name")

	assertNoError(t, err, stderr)
	assertContains(t, stdout, "Alice")
	assertContains(t, stdout, "50")
	assertContains(t, stdout, "Bob")
	assertContains(t, stdout, "75")
}
