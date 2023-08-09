package atlasexec

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"testing/fstest"
	"text/template"

	"github.com/stretchr/testify/require"
)

func TestContextExecer(t *testing.T) {
	src := fstest.MapFS{
		"bar": &fstest.MapFile{Data: []byte("bar-content")},
	}
	ce, err := NewWorkingDir()
	checkFileContent := func(t *testing.T, name, expected string) {
		t.Helper()
		full := filepath.Join(ce.dir, name)
		require.FileExists(t, full, "The file %q should exist", name)
		actual, err := os.ReadFile(full)
		require.NoError(t, err)
		require.Equal(t, expected, string(actual), "The file %q should have the expected content", name)
	}
	require.NoError(t, err)
	require.DirExists(t, ce.dir, "The temporary directory should exist")
	require.NoFileExists(t, filepath.Join(ce.dir, "atlas.hcl"), "The file atlas.hcl should not exist")
	require.NoError(t, ce.Close())

	// Test WithMigrations.
	ce, err = NewWorkingDir(WithMigrations(src))
	require.NoError(t, err)
	checkFileContent(t, filepath.Join("migrations", "bar"), "bar-content")
	require.NoError(t, ce.Close())

	// Test WithAtlasHCL.
	ce, err = NewWorkingDir(
		WithAtlasHCL(func(w io.Writer) error {
			return template.Must(template.New("").Parse(`{{ .foo }} & {{ .bar }}`)).
				Execute(w, map[string]any{
					"foo": "foo",
					"bar": "bar",
				})
		}),
		WithMigrations(src),
	)
	require.NoError(t, err)
	require.DirExists(t, ce.dir, "tmpDir")
	checkFileContent(t, filepath.Join("migrations", "bar"), "bar-content")
	checkFileContent(t, "atlas.hcl", "foo & bar")

	// Test WriteFile.
	_, err = ce.WriteFile(filepath.Join("migrations", "foo"), []byte("foo-content"))
	require.NoError(t, err)
	checkFileContent(t, filepath.Join("migrations", "foo"), "foo-content")

	// Test RunCommand.
	buf := &bytes.Buffer{}
	cmd := exec.Command("ls")
	cmd.Dir = "fake-dir"
	cmd.Stdout = buf
	require.NoError(t, ce.RunCommand(cmd))
	require.Equal(t, "fake-dir", cmd.Dir)
	require.Equal(t, "atlas.hcl\nmigrations\n", buf.String())
	require.NoError(t, ce.Close())
}
