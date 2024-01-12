package e2etest

import (
	"context"
	"os"
	"testing"

	"ariga.io/atlas-go-sdk/atlasexec"
	"github.com/stretchr/testify/require"
)

func Test_SQLite(t *testing.T) {
	runTestWithVersions(t, []string{"latest"}, "versioned-basic", func(t *testing.T, ver *atlasexec.Version, c *atlasexec.Client) {
		url := "sqlite://file.db?_fk=1"
		ctx := context.Background()
		s, err := c.MigrateStatus(ctx, &atlasexec.MigrateStatusParams{
			URL: url,
			Env: "local",
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(s.Pending))
		require.Equal(t, "20240112070806", s.Pending[0].Version)

		r, err := c.MigrateApply(ctx, &atlasexec.MigrateApplyParams{
			URL: url,
			Env: "local",
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(r.Applied), "Should be one migration applied")
		require.Equal(t, "20240112070806", r.Applied[0].Version, "Should be the correct migration applied")

		// Apply again, should be a no-op.
		r, err = c.MigrateApply(ctx, &atlasexec.MigrateApplyParams{
			URL: url,
			Env: "local",
		})
		require.NoError(t, err, "Should be no error")
		require.Equal(t, 0, len(r.Applied), "Should be no migrations applied")
	})
}

func Test_PostgreSQL(t *testing.T) {
	u := os.Getenv("ATLASEXEC_E2ETEST_POSTGRES_URL")
	if u == "" {
		t.Skip("ATLASEXEC_E2ETEST_POSTGRES_URL not set")
	}
	runTestWithVersions(t, []string{"latest"}, "versioned-basic", func(t *testing.T, ver *atlasexec.Version, c *atlasexec.Client) {
		url := u
		ctx := context.Background()
		s, err := c.MigrateStatus(ctx, &atlasexec.MigrateStatusParams{
			URL: url,
			Env: "local",
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(s.Pending))
		require.Equal(t, "20240112070806", s.Pending[0].Version)

		r, err := c.MigrateApply(ctx, &atlasexec.MigrateApplyParams{
			URL: url,
			Env: "local",
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(r.Applied), "Should be one migration applied")
		require.Equal(t, "20240112070806", r.Applied[0].Version, "Should be the correct migration applied")

		// Apply again, should be a no-op.
		r, err = c.MigrateApply(ctx, &atlasexec.MigrateApplyParams{
			URL: url,
			Env: "local",
		})
		require.NoError(t, err, "Should be no error")
		require.Equal(t, 0, len(r.Applied), "Should be no migrations applied")
	})
}

func Test_MultiTenants(t *testing.T) {
	t.Setenv("ATLASEXEC_E2ETEST_ATLAS_PATH", "atlas")
	runTestWithVersions(t, []string{"latest"}, "multi-tenants", func(t *testing.T, ver *atlasexec.Version, c *atlasexec.Client) {
		ctx := context.Background()
		r, err := c.MultipleMigrateApply(ctx, &atlasexec.MigrateApplyParams{
			Env: "local",
		})
		require.NoError(t, err)
		require.Len(t, r, 2, "Should be two tenants")
		require.Equal(t, 1, len(r[0].Applied), "Should be one migration applied")
		require.Equal(t, "20240112070806", r[0].Applied[0].Version, "Should be the correct migration applied")

		// Apply again, should be a no-op.
		r, err = c.MultipleMigrateApply(ctx, &atlasexec.MigrateApplyParams{
			Env: "local",
		})
		require.NoError(t, err, "Should be no error")
		require.Equal(t, 0, len(r[0].Applied), "Should be no migrations applied")
	})
}
