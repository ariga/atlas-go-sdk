package atlasexec_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"ariga.io/atlas-go-sdk/atlasexec"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestError(t *testing.T) {
	err := atlasexec.Error{}
	require.NotPanics(t, func() {
		err.ExitCode()
	})
}

func TestNewClient(t *testing.T) {
	execPath, err := exec.LookPath("atlas")
	require.NoError(t, err)

	// Test that we can create a client with a custom exec path.
	_, err = atlasexec.NewClient(t.TempDir(), execPath)
	require.NoError(t, err)

	// Atlas-CLI is installed in the PATH.
	_, err = atlasexec.NewClient(t.TempDir(), "atlas")
	require.NoError(t, err)

	// Atlas-CLI is not found for the given exec path.
	_, err = atlasexec.NewClient(t.TempDir(), "/foo/atlas")
	require.ErrorContains(t, err, `no such file or directory`)
}

func TestVersion(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(t.TempDir(), filepath.Join(wd, "./mock-atlas.sh"))
	require.NoError(t, err)

	for _, tt := range []struct {
		env    string
		expect *atlasexec.Version
	}{
		{
			env:    "v1.2.3",
			expect: &atlasexec.Version{Version: "1.2.3"},
		},
		{
			env: "v0.14.1-abcdef-canary",
			expect: &atlasexec.Version{
				Version: "0.14.1",
				SHA:     "abcdef",
				Canary:  true,
			},
		},
		{
			env: "v11.22.33-sha",
			expect: &atlasexec.Version{
				Version: "11.22.33",
				SHA:     "sha",
			},
		},
	} {
		t.Run(tt.env, func(t *testing.T) {
			t.Setenv("TEST_ARGS", "version")
			t.Setenv("TEST_STDOUT", fmt.Sprintf("atlas version %s", tt.env))
			v, err := c.Version(context.Background())
			require.NoError(t, err)
			require.Equal(t, tt.expect, v)
			if tt.env != "" {
				require.Equal(t, "atlas version "+tt.env, v.String())
			}
		})
	}
}

func TestWhoAmI(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(t.TempDir(), filepath.Join(wd, "./mock-atlas.sh"))
	require.NoError(t, err)
	t.Setenv("TEST_ARGS", "whoami --format {{ json . }}")
	// Test success.
	t.Setenv("TEST_STDOUT", `{"Org":"boring"}`)
	v, err := c.WhoAmI(context.Background())
	require.NoError(t, err)
	require.NotNil(t, v)
	require.Equal(t, "boring", v.Org)
	// Test error.
	t.Setenv("TEST_STDOUT", "")
	t.Setenv("TEST_STDERR", `Error: command requires 'atlas login'`)
	_, err = c.WhoAmI(context.Background())
	require.EqualError(t, err, "command requires 'atlas login'")
	require.ErrorIs(t, err, atlasexec.ErrRequireLogin)
}

func TestVars2(t *testing.T) {
	var vars = atlasexec.Vars2{
		"key1": "value1",
		"key2": "value2",
		"key3": []string{"value3", "value4"},
		"key4": 100,
		"key5": []int{1, 2, 3},
		"key6": []stringer{{}, {}},
	}
	require.Equal(t, []string{
		"--var", "key1=value1",
		"--var", "key2=value2",
		"--var", "key3=value3",
		"--var", "key3=value4",
		"--var", "key4=100",
		"--var", "key5=1",
		"--var", "key5=2",
		"--var", "key5=3",
		"--var", "key6=foo",
		"--var", "key6=foo",
	}, vars.AsArgs())
}

func generateHCL(t *testing.T, token string, srv *httptest.Server) string {
	st := fmt.Sprintf(
		`atlas { 
			cloud {	
				token = %q
				url = %q
			}
		}
		env "test" {}
		`, token, srv.URL)
	atlasConfigURL, clean, err := atlasexec.TempFile(st, "hcl")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, clean())
	})
	return atlasConfigURL
}

func sqlitedb(t *testing.T) string {
	td := t.TempDir()
	dbpath := filepath.Join(td, "file.db")
	_, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared&_fk=1", dbpath))
	require.NoError(t, err)
	return dbpath
}

type stringer struct{}

func (s stringer) String() string {
	return "foo"
}
