package atlasexec_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"ariga.io/atlas-go-sdk/atlasexec"

	_ "github.com/mattn/go-sqlite3"

	"github.com/stretchr/testify/require"
)

func Test_MigrateApply(t *testing.T) {
	r := require.New(t)
	type args struct {
		ctx  context.Context
		data *atlasexec.ApplyParams
	}
	td := t.TempDir()
	tests := []struct {
		name       string
		args       args
		wantTarget string
		wantErr    bool
	}{
		{
			args: args{
				ctx: context.Background(),
				data: &atlasexec.ApplyParams{
					DirURL: "file://testdata/migrations",
					URL:    fmt.Sprintf("sqlite://%s/file.db", td),
				},
			},
			wantTarget: "20230727105615",
		},
	}
	wd, err := os.Getwd()
	r.NoError(err)
	c, err := atlasexec.NewClient(wd, "atlas")
	r.NoError(err)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.Apply(tt.args.ctx, tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("migrateApply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.EqualValues(t, tt.wantTarget, got.Target)
		})
	}
}

func Test_MigrateStatus(t *testing.T) {
	r := require.New(t)
	type args struct {
		ctx  context.Context
		data *atlasexec.StatusParams
	}
	tests := []struct {
		name        string
		args        args
		wantCurrent string
		wantNext    string
		wantErr     bool
	}{
		{
			args: args{
				ctx: context.Background(),
				data: &atlasexec.StatusParams{
					DirURL: "file://testdata/migrations",
				},
			},
			wantCurrent: "No migration applied yet",
			wantNext:    "20230727105553",
		},
	}
	wd, err := os.Getwd()
	r.NoError(err)
	c, err := atlasexec.NewClient(wd, "atlas")
	r.NoError(err)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbpath := sqlitedb(t)
			path := fmt.Sprintf("sqlite://%s", dbpath)
			tt.args.data.URL = path
			got, err := c.Status(tt.args.ctx, tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("migrateStatus() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.Equal(t, tt.wantCurrent, got.Current)
			require.Equal(t, tt.wantNext, got.Next)
		})
	}
}

func Test_SchemaApply(t *testing.T) {
	f, err := os.CreateTemp("", "sqlite-test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	u := fmt.Sprintf("sqlite://%s?_fk=1", f.Name())
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(wd, "atlas")
	require.NoError(t, err)

	s1 := `
	-- create table "users
	CREATE TABLE users(
		id int NOT NULL,
		name varchar(100) NULL,
		PRIMARY KEY(id)
	);
	`
	to, clean, err := atlasexec.TempFile(s1, "sql")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, clean())
	}()
	_, err = c.SchemaApply(context.Background(), &atlasexec.SchemaApplyParams{
		URL:    u,
		To:     to,
		DevURL: "sqlite://file?_fk=1&cache=shared&mode=memory",
	})
	require.NoError(t, err)

	s2 := s1 + `
	-- create table "blog_posts"
	CREATE TABLE blog_posts(
		id int NOT NULL,
		title varchar(100) NULL,
		body text NULL,
		author_id int NULL,
		PRIMARY KEY(id),
		CONSTRAINT author_fk FOREIGN KEY(author_id) REFERENCES users(id)
	);`
	to, clean2, err := atlasexec.TempFile(s2, "sql")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, clean2())
	}()
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

func sqlitedb(t *testing.T) string {
	td := t.TempDir()
	dbpath := filepath.Join(td, "file.db")
	_, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared&_fk=1", dbpath))
	require.NoError(t, err)
	return dbpath
}
