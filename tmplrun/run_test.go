package tmplrun

import (
	"bytes"
	_ "embed"
	"testing"
	"text/template"

	"github.com/stretchr/testify/require"
)

var (
	//go:embed testdata/app.tmpl
	testTmpl   string
	loaderTmpl = template.Must(template.New("loader").Parse(testTmpl))
)

func TestRunner(t *testing.T) {
	var buf bytes.Buffer
	runner := New("test", loaderTmpl)
	runner.out = &buf
	err := runner.Run(struct {
		Message string
	}{
		Message: "Hello, World!",
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "Hello, World!")
}
