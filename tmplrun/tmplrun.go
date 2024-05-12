// package tmplrun provides a Runner for templated go programs. It is commonly used
// by Go Atlas providers to compile ad-hoc programs that emit the desired SQL schema for
// data models defined for Go ORMs.

package tmplrun

import (
	"bytes"
	"errors"
	"fmt"
	"go/format"
	"os"
	"os/exec"
	"strings"
	"text/template"
	"time"
)

// Runner is a go template runner.  It accepts a go template and data, and runs the
// rendered template as a go program.
type Runner struct {
	name string
	tmpl *template.Template
}

// New returns a new Runner.
func New(name string, tmpl *template.Template) *Runner {
	return &Runner{name: name, tmpl: tmpl}
}

// Run runs the template and returns the output.
func (r *Runner) Run(data interface{}) (string, error) {
	var buf bytes.Buffer
	if err := r.tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	source, err := format.Source(buf.Bytes())
	if err != nil {
		return "", err
	}
	return r.gorun(source)
}

func (r *Runner) gorun(src []byte) (string, error) {
	dir := fmt.Sprintf(".%s", r.name)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return "", err
	}
	target := fmt.Sprintf("%s/%s.go", dir, r.filename(r.name))
	if err := os.WriteFile(target, src, 0644); err != nil {
		return "", fmt.Errorf("%s: write file %s: %w", r.name, target, err)
	}
	defer os.RemoveAll(dir)
	return gorun(target)
}

// run 'go run' command and return its output.
func gorun(target string) (string, error) {
	s, err := gocmd("run", target)
	if err != nil {
		return "", fmt.Errorf("tmplrun: %s", err)
	}
	return s, nil
}

// goCmd runs a go command and returns its output.
func gocmd(command, target string) (string, error) {
	args := []string{command}
	args = append(args, target)
	cmd := exec.Command("go", args...)
	stderr := bytes.NewBuffer(nil)
	stdout := bytes.NewBuffer(nil)
	cmd.Stderr = stderr
	cmd.Stdout = stdout
	if err := cmd.Run(); err != nil {
		return "", errors.New(stderr.String())
	}
	return stdout.String(), nil
}

func (r *Runner) filename(pkg string) string {
	name := strings.ReplaceAll(pkg, "/", "_")
	return fmt.Sprintf("%s_%s_%d", r.name, name, time.Now().Unix())
}
