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

func TestSchema_Plan(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(t.TempDir(), filepath.Join(wd, "./mock-atlas.sh"))
	require.NoError(t, err)

	testCases := []struct {
		name   string
		params *atlasexec.SchemaPlanParams
		args   string
	}{
		{
			name:   "no params",
			params: &atlasexec.SchemaPlanParams{},
			args:   "schema plan --format {{ json . }} --auto-approve",
		},
		{
			name: "with env",
			params: &atlasexec.SchemaPlanParams{
				Env: "test",
			},
			args: "schema plan --format {{ json . }} --env test --auto-approve",
		},
		{
			name: "with from to",
			params: &atlasexec.SchemaPlanParams{
				From: []string{"1", "2"},
				To:   []string{"2", "3"},
			},
			args: `schema plan --format {{ json . }} --from 1,2 --to 2,3 --auto-approve`,
		},
		{
			name: "with config",
			params: &atlasexec.SchemaPlanParams{
				ConfigURL: "file://config.hcl",
			},
			args: "schema plan --format {{ json . }} --config file://config.hcl --auto-approve",
		},
		{
			name: "with dev-url",
			params: &atlasexec.SchemaPlanParams{
				DevURL: "sqlite://file?_fk=1&cache=shared&mode=memory",
			},
			args: "schema plan --format {{ json . }} --dev-url sqlite://file?_fk=1&cache=shared&mode=memory --auto-approve",
		},
		{
			name: "with name",
			params: &atlasexec.SchemaPlanParams{
				Name: "example",
			},
			args: "schema plan --format {{ json . }} --name example --auto-approve",
		},
		{
			name: "with dry-run",
			params: &atlasexec.SchemaPlanParams{
				DryRun: true,
			},
			args: "schema plan --format {{ json . }} --dry-run",
		},
		{
			name: "with save",
			params: &atlasexec.SchemaPlanParams{
				Save: true,
			},
			args: "schema plan --format {{ json . }} --save --auto-approve",
		},
		{
			name: "with push",
			params: &atlasexec.SchemaPlanParams{
				Repo: "testing-repo",
				Push: true,
			},
			args: "schema plan --format {{ json . }} --repo testing-repo --push --auto-approve",
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEST_ARGS", tt.args)
			t.Setenv("TEST_STDOUT", `{"Repo":"foo"}`)
			result, err := c.SchemaPlan(context.Background(), tt.params)
			require.NoError(t, err)
			require.Equal(t, "foo", result.Repo)
		})
	}
}

func TestSchema_PlanPush(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(t.TempDir(), filepath.Join(wd, "./mock-atlas.sh"))
	require.NoError(t, err)

	testCases := []struct {
		name   string
		params *atlasexec.SchemaPlanPushParams
		args   string
	}{
		{
			name: "with auto-approve",
			params: &atlasexec.SchemaPlanPushParams{
				Repo: "testing-repo",
				File: "file://plan.hcl",
			},
			args: "schema plan push --format {{ json . }} --file file://plan.hcl --repo testing-repo --auto-approve",
		},
		{
			name: "with pending status",
			params: &atlasexec.SchemaPlanPushParams{
				Pending: true,
				File:    "file://plan.hcl",
			},
			args: "schema plan push --format {{ json . }} --file file://plan.hcl --pending",
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEST_ARGS", tt.args)
			t.Setenv("TEST_STDOUT", `{"Repo":"foo"}`)
			result, err := c.SchemaPlanPush(context.Background(), tt.params)
			require.NoError(t, err)
			require.Equal(t, `{"Repo":"foo"}`, result)
		})
	}
}

func TestSchema_PlanLint(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(t.TempDir(), filepath.Join(wd, "./mock-atlas.sh"))
	require.NoError(t, err)

	testCases := []struct {
		name   string
		params *atlasexec.SchemaPlanLintParams
		args   string
	}{
		{
			name: "with repo",
			params: &atlasexec.SchemaPlanLintParams{
				Repo: "testing-repo",
				File: "file://plan.hcl",
			},
			args: "schema plan lint --format {{ json . }} --file file://plan.hcl --repo testing-repo --auto-approve",
		},
		{
			name: "with file only",
			params: &atlasexec.SchemaPlanLintParams{
				File: "file://plan.hcl",
			},
			args: "schema plan lint --format {{ json . }} --file file://plan.hcl --auto-approve",
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEST_ARGS", tt.args)
			t.Setenv("TEST_STDOUT", `{"Repo":"foo"}`)
			result, err := c.SchemaPlanLint(context.Background(), tt.params)
			require.NoError(t, err)
			require.Equal(t, "foo", result.Repo)
		})
	}
}

func TestSchema_PlanValidate(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(t.TempDir(), filepath.Join(wd, "./mock-atlas.sh"))
	require.NoError(t, err)

	testCases := []struct {
		name   string
		params *atlasexec.SchemaPlanValidateParams
		args   string
	}{
		{
			name: "with repo",
			params: &atlasexec.SchemaPlanValidateParams{
				Repo: "testing-repo",
				File: "file://plan.hcl",
			},
			args: "schema plan validate --file file://plan.hcl --repo testing-repo --auto-approve",
		},
		{
			name: "with file only",
			params: &atlasexec.SchemaPlanValidateParams{
				File: "file://plan.hcl",
			},
			args: "schema plan validate --file file://plan.hcl --auto-approve",
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEST_ARGS", tt.args)
			t.Setenv("TEST_STDOUT", `{"Repo":"foo"}`)
			err := c.SchemaPlanValidate(context.Background(), tt.params)
			require.NoError(t, err)
		})
	}
}

func TestSchema_PlanApprove(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(t.TempDir(), filepath.Join(wd, "./mock-atlas.sh"))
	require.NoError(t, err)

	testCases := []struct {
		name   string
		params *atlasexec.SchemaPlanApproveParams
		args   string
	}{
		{
			name: "with url",
			params: &atlasexec.SchemaPlanApproveParams{
				URL: "atlas://app1/plans/foo-plan",
			},
			args: "schema plan approve --format {{ json . }} --url atlas://app1/plans/foo-plan",
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEST_ARGS", tt.args)
			t.Setenv("TEST_STDOUT", `{"URL":"atlas://app1/plans/foo-plan", "Link":"some-link", "Status":"APPROVED"}`)
			result, err := c.SchemaPlanApprove(context.Background(), tt.params)
			require.NoError(t, err)
			require.Equal(t, "atlas://app1/plans/foo-plan", result.URL)
		})
	}
}

func TestSchema_PlanPull(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(t.TempDir(), filepath.Join(wd, "./mock-atlas.sh"))
	require.NoError(t, err)

	testCases := []struct {
		name   string
		params *atlasexec.SchemaPlanPullParams
		args   string
	}{
		{
			name: "with url",
			params: &atlasexec.SchemaPlanPullParams{
				URL: "atlas://app1/plans/foo-plan",
			},
			args: "schema plan pull --url atlas://app1/plans/foo-plan",
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEST_ARGS", tt.args)
			t.Setenv("TEST_STDOUT", "excited-plan")
			result, err := c.SchemaPlanPull(context.Background(), tt.params)
			require.NoError(t, err)
			require.Equal(t, "excited-plan", result)
		})
	}
}

func TestSchema_PlanList(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(t.TempDir(), filepath.Join(wd, "./mock-atlas.sh"))
	require.NoError(t, err)

	testCases := []struct {
		name   string
		params *atlasexec.SchemaPlanListParams
		args   string
	}{
		{
			name:   "no params",
			params: &atlasexec.SchemaPlanListParams{},
			args:   "schema plan list --format {{ json . }} --auto-approve",
		},
		{
			name: "with repo",
			params: &atlasexec.SchemaPlanListParams{
				Repo: "atlas://testing-repo",
				From: []string{"env://url"},
			},
			args: "schema plan list --format {{ json . }} --from env://url --repo atlas://testing-repo --auto-approve",
		},
		{
			name: "with repo and pending",
			params: &atlasexec.SchemaPlanListParams{
				Repo:    "atlas://testing-repo",
				Pending: true,
			},
			args: "schema plan list --format {{ json . }} --repo atlas://testing-repo --pending --auto-approve",
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEST_ARGS", tt.args)
			t.Setenv("TEST_STDOUT", `[{"Name":"pr-2-ufnTS7Nr"}]`)
			result, err := c.SchemaPlanList(context.Background(), tt.params)
			require.NoError(t, err)
			require.Equal(t, "pr-2-ufnTS7Nr", result[0].Name)
		})
	}
}

func TestSchema_Push(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(t.TempDir(), filepath.Join(wd, "./mock-atlas.sh"))
	require.NoError(t, err)

	testCases := []struct {
		name   string
		params *atlasexec.SchemaPushParams
		args   string
	}{
		{
			name:   "no params",
			params: &atlasexec.SchemaPushParams{},
			args:   "schema push",
		},
		{
			name: "with repo",
			params: &atlasexec.SchemaPushParams{
				Repo: "atlas-action",
			},
			args: "schema push atlas-action",
		},
		{
			name: "with repo and tag",
			params: &atlasexec.SchemaPushParams{
				Repo: "atlas-action",
				Tag:  "v1.0.0",
			},
			args: "schema push --tag v1.0.0 atlas-action",
		},
		{
			name: "with repo and tag and description",
			params: &atlasexec.SchemaPushParams{
				Repo:        "atlas-action",
				Tag:         "v1.0.0",
				Description: "release-v1",
			},
			args: "schema push --tag v1.0.0 --desc release-v1 atlas-action",
		},
		{
			name: "with repo and tag, version and description",
			params: &atlasexec.SchemaPushParams{
				Repo:        "atlas-action",
				Tag:         "v1.0.0",
				Version:     "20240829100417",
				Description: "release-v1",
			},
			args: "schema push --tag v1.0.0 --version 20240829100417 --desc release-v1 atlas-action",
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEST_ARGS", tt.args)
			t.Setenv("TEST_STDOUT", `https://gh.atlasgo.cloud/schemas/141733920810`)
			result, err := c.SchemaPush(context.Background(), tt.params)
			require.NoError(t, err)
			require.Equal(t, "https://gh.atlasgo.cloud/schemas/141733920810", result)
		})
	}
}

func TestSchema_Apply(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(t.TempDir(), filepath.Join(wd, "./mock-atlas.sh"))
	require.NoError(t, err)

	testCases := []struct {
		name   string
		params *atlasexec.SchemaApplyParams
		args   string
	}{
		{
			name:   "no params",
			params: &atlasexec.SchemaApplyParams{},
			args:   "schema apply --format {{ json . }} --auto-approve",
		},
		{
			name: "with plan",
			params: &atlasexec.SchemaApplyParams{
				PlanURL: "atlas://app1/plans/foo-plan",
			},
			args: "schema apply --format {{ json . }} --plan atlas://app1/plans/foo-plan --auto-approve",
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEST_ARGS", tt.args)
			t.Setenv("TEST_STDOUT", `{"Driver":"sqlite3"}`)
			result, err := c.SchemaApply(context.Background(), tt.params)
			require.NoError(t, err)
			require.Equal(t, "sqlite3", result.Driver)
		})
	}
}
