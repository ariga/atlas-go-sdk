package atlasexec_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"ariga.io/atlas-go-sdk/atlasexec"
	"github.com/stretchr/testify/require"
)

func TestSchema_Test(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(t.TempDir(), filepath.Join(wd, "./mock-atlas.sh"))
	require.NoError(t, err)

	for _, tt := range []struct {
		name   string
		params *atlasexec.SchemaTestParams
		args   string
		stdout string
	}{
		{
			name:   "no params",
			params: &atlasexec.SchemaTestParams{},
			args:   "schema test",
			stdout: "test result",
		},
		{
			name: "with env",
			params: &atlasexec.SchemaTestParams{
				Env: "test",
			},
			args:   "schema test --env test",
			stdout: "test result",
		},
		{
			name: "with config",
			params: &atlasexec.SchemaTestParams{
				ConfigURL: "file://config.hcl",
			},
			args:   "schema test --config file://config.hcl",
			stdout: "test result",
		},
		{
			name: "with dev-url",
			params: &atlasexec.SchemaTestParams{
				DevURL: "sqlite://file?_fk=1&cache=shared&mode=memory",
			},
			args:   "schema test --dev-url sqlite://file?_fk=1&cache=shared&mode=memory",
			stdout: "test result",
		},
		{
			name: "with run",
			params: &atlasexec.SchemaTestParams{
				Run: "example",
			},
			args:   "schema test --run example",
			stdout: "test result",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEST_ARGS", tt.args)
			t.Setenv("TEST_STDOUT", tt.stdout)
			result, err := c.SchemaTest(context.Background(), tt.params)
			require.NoError(t, err)
			require.Equal(t, tt.stdout, result)
		})
	}
}

func TestSchema_Inspect(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(t.TempDir(), filepath.Join(wd, "./mock-atlas.sh"))
	require.NoError(t, err)

	for _, tt := range []struct {
		name   string
		params *atlasexec.SchemaInspectParams
		args   string
		stdout string
	}{
		{
			name:   "no params",
			params: &atlasexec.SchemaInspectParams{},
			args:   "schema inspect",
			stdout: `schema "public" {}`,
		},
		{
			name: "with env",
			params: &atlasexec.SchemaInspectParams{
				Env: "test",
			},
			args:   "schema inspect --env test",
			stdout: `schema "public" {}`,
		},
		{
			name: "with config",
			params: &atlasexec.SchemaInspectParams{
				ConfigURL: "file://config.hcl",
				Env:       "test",
			},
			args:   "schema inspect --env test --config file://config.hcl",
			stdout: `schema "public" {}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEST_ARGS", tt.args)
			t.Setenv("TEST_STDOUT", tt.stdout)
			result, err := c.SchemaInspect(context.Background(), tt.params)
			require.NoError(t, err)
			require.Equal(t, tt.stdout, result)
		})
	}
}

func TestAtlasSchema_Apply(t *testing.T) {
	ce, err := atlasexec.NewWorkingDir()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ce.Close())
	})
	f, err := os.CreateTemp("", "sqlite-test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	u := fmt.Sprintf("sqlite://%s?_fk=1", f.Name())
	c, err := atlasexec.NewClient(ce.Path(), "atlas")
	require.NoError(t, err)

	s1 := `
	-- create table "users
	CREATE TABLE users(
		id int NOT NULL,
		name varchar(100) NULL,
		PRIMARY KEY(id)
	);`
	path, err := ce.WriteFile("schema.sql", []byte(s1))
	to := fmt.Sprintf("file://%s", path)
	require.NoError(t, err)
	_, err = c.SchemaApply(context.Background(), &atlasexec.SchemaApplyParams{
		URL:    u,
		To:     to,
		DevURL: "sqlite://file?_fk=1&cache=shared&mode=memory",
	})
	require.NoError(t, err)
	_, err = ce.WriteFile("schema.sql", []byte(s1+`
	-- create table "blog_posts"
	CREATE TABLE blog_posts(
		id int NOT NULL,
		title varchar(100) NULL,
		body text NULL,
		author_id int NULL,
		PRIMARY KEY(id),
		CONSTRAINT author_fk FOREIGN KEY(author_id) REFERENCES users(id)
	);`))
	require.NoError(t, err)
	_, err = c.SchemaApply(context.Background(), &atlasexec.SchemaApplyParams{
		URL:    u,
		To:     to,
		DevURL: "sqlite://file?_fk=1&cache=shared&mode=memory",
	})
	require.NoError(t, err)

	s, err := c.SchemaInspect(context.Background(), &atlasexec.SchemaInspectParams{
		URL: u,
	})
	require.NoError(t, err)
	require.Equal(t, `table "users" {
  schema = schema.main
  column "id" {
    null = false
    type = int
  }
  column "name" {
    null = true
    type = varchar
  }
  primary_key {
    columns = [column.id]
  }
}
table "blog_posts" {
  schema = schema.main
  column "id" {
    null = false
    type = int
  }
  column "title" {
    null = true
    type = varchar
  }
  column "body" {
    null = true
    type = text
  }
  column "author_id" {
    null = true
    type = int
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "author_fk" {
    columns     = [column.author_id]
    ref_columns = [table.users.column.id]
    on_update   = NO_ACTION
    on_delete   = NO_ACTION
  }
}
schema "main" {
}
`, s)
}
