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

func TestCopilot(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	c, err := atlasexec.NewClient(t.TempDir(), filepath.Join(wd, "./mock-atlas.sh"))
	require.NoError(t, err)

	p := &atlasexec.CopilotParams{Prompt: "What is the capital of France?"}
	t.Setenv("TEST_ARGS", "copilot -q "+p.Prompt)
	t.Setenv("TEST_STDOUT", `{"sessionID":"id","type":"message","content":"The capital of"}
{"sessionID":"id","type":"message","content":" France is Paris."}`)
	copilot, err := c.Copilot(context.Background(), p)
	require.NoError(t, err)
	require.Equal(t, "The capital of France is Paris.", copilot.String())

	p = &atlasexec.CopilotParams{Prompt: "And Germany?", Session: "id"}
	t.Setenv("TEST_ARGS", fmt.Sprintf("copilot -q %s -r %s", p.Prompt, p.Session))
	t.Setenv("TEST_STDOUT", `{"sessionID":"id","type":"message","content":"Berlin."}`)
	copilot, err = c.Copilot(context.Background(), p)
	require.NoError(t, err)
	require.Equal(t, "Berlin.", copilot.String())
}
