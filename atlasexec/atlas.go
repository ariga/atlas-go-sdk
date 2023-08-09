package atlasexec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type (
	// Client is a client for the Atlas CLI.
	Client struct {
		execPath   string
		workingDir string
	}
	// ApplyParams are the parameters for the `migrate apply` command.
	ApplyParams struct {
		Env             string
		ConfigURL       string
		DirURL          string
		URL             string
		RevisionsSchema string
		BaselineVersion string
		TxMode          string
		Amount          uint64
	}
	// StatusParams are the parameters for the `migrate status` command.
	StatusParams struct {
		Env             string
		ConfigURL       string
		DirURL          string
		URL             string
		RevisionsSchema string
	}
	// LintParams are the parameters for the `migrate lint` command.
	LintParams struct {
		Env       string
		ConfigURL string
		DevURL    string
		DirURL    string
		Latest    uint64
		Vars      Vars
	}
	// SchemaApplyParams are the parameters for the `schema apply` command.
	SchemaApplyParams struct {
		Env       string
		ConfigURL string
		DevURL    string
		DryRun    bool
		Exclude   []string
		Schema    []string
		To        string
		URL       string
		Vars      Vars
	}
	// SchemaInspectParams are the parameters for the `schema inspect` command.
	SchemaInspectParams struct {
		Env       string
		ConfigURL string
		DevURL    string
		Exclude   []string
		Format    string
		Schema    []string
		URL       string
	}
	Vars map[string]string
)

// NewClient returns a new Atlas client.
// The client will try to find the Atlas CLI in the current directory,
// and in the PATH.
func NewClient(dir, name string) (*Client, error) {
	path, err := execPath(dir, name)
	if err != nil {
		return nil, err
	}
	return NewClientWithDir("", path)
}

// NewClientWD returns a new Atlas client with the given atlas-cli path.
func NewClientWithDir(workingDir, execPath string) (*Client, error) {
	if execPath == "" {
		return nil, fmt.Errorf("execPath cannot be empty")
	}
	if workingDir != "" {
		_, err := os.Stat(workingDir)
		if err != nil {
			return nil, fmt.Errorf("initializing Atlas with working dir %q: %w", workingDir, err)
		}
	}
	return &Client{
		execPath:   execPath,
		workingDir: workingDir,
	}, nil
}

// Apply runs the 'migrate apply' command.
func (c *Client) Apply(ctx context.Context, data *ApplyParams) (*ApplyReport, error) {
	args := []string{"migrate", "apply", "--log", "{{ json . }}"}
	if data.Env != "" {
		args = append(args, "--env", data.Env)
	}
	if data.ConfigURL != "" {
		args = append(args, "--config", data.ConfigURL)
	}
	if data.URL != "" {
		args = append(args, "--url", data.URL)
	}
	if data.DirURL != "" {
		args = append(args, "--dir", data.DirURL)
	}
	if data.RevisionsSchema != "" {
		args = append(args, "--revisions-schema", data.RevisionsSchema)
	}
	if data.BaselineVersion != "" {
		args = append(args, "--baseline", data.BaselineVersion)
	}
	if data.TxMode != "" {
		args = append(args, "--tx-mode", data.TxMode)
	}
	if data.Amount > 0 {
		args = append(args, strconv.FormatUint(data.Amount, 10))
	}
	return jsonDecode[ApplyReport](c.runCommand(ctx, args))
}

// SchemaApply runs the 'schema apply' command.
func (c *Client) SchemaApply(ctx context.Context, data *SchemaApplyParams) (*SchemaApply, error) {
	args := []string{"schema", "apply", "--format", "{{ json . }}"}
	if data.Env != "" {
		args = append(args, "--env", data.Env)
	}
	if data.ConfigURL != "" {
		args = append(args, "--config", data.ConfigURL)
	}
	if data.URL != "" {
		args = append(args, "--url", data.URL)
	}
	if data.To != "" {
		args = append(args, "--to", data.To)
	}
	if data.DryRun {
		args = append(args, "--dry-run")
	} else {
		args = append(args, "--auto-approve")
	}
	if data.DevURL != "" {
		args = append(args, "--dev-url", data.DevURL)
	}
	if len(data.Schema) > 0 {
		args = append(args, "--schema", strings.Join(data.Schema, ","))
	}
	if len(data.Exclude) > 0 {
		args = append(args, "--exclude", strings.Join(data.Exclude, ","))
	}
	args = append(args, data.Vars.AsArgs()...)
	return jsonDecode[SchemaApply](c.runCommand(ctx, args))
}

// SchemaInspect runs the 'schema inspect' command.
func (c *Client) SchemaInspect(ctx context.Context, data *SchemaInspectParams) (string, error) {
	args := []string{"schema", "inspect"}
	if data.Env != "" {
		args = append(args, "--env", data.Env)
	}
	if data.ConfigURL != "" {
		args = append(args, "--config", data.ConfigURL)
	}
	if data.URL != "" {
		args = append(args, "--url", data.URL)
	}
	if data.DevURL != "" {
		args = append(args, "--dev-url", data.DevURL)
	}
	if data.Format == "sql" {
		args = append(args, "--format", "{{ sql . }}")
	}
	if len(data.Schema) > 0 {
		args = append(args, "--schema", strings.Join(data.Schema, ","))
	}
	if len(data.Exclude) > 0 {
		args = append(args, "--exclude", strings.Join(data.Exclude, ","))
	}
	return stringVal(c.runCommand(ctx, args))
}

// Lint runs the 'migrate lint' command.
func (c *Client) Lint(ctx context.Context, data *LintParams) (*SummaryReport, error) {
	args := []string{"migrate", "lint", "--log", "{{ json . }}"}
	if data.Env != "" {
		args = append(args, "--env", data.Env)
	}
	if data.ConfigURL != "" {
		args = append(args, "--config", data.ConfigURL)
	}
	if data.DevURL != "" {
		args = append(args, "--dev-url", data.DevURL)
	}
	if data.DirURL != "" {
		args = append(args, "--dir", data.DirURL)
	}
	if data.Latest > 0 {
		args = append(args, "--latest", strconv.FormatUint(data.Latest, 10))
	}
	args = append(args, data.Vars.AsArgs()...)
	return jsonDecode[SummaryReport](c.runCommand(ctx, args))
}

// Status runs the 'migrate status' command.
func (c *Client) Status(ctx context.Context, data *StatusParams) (*StatusReport, error) {
	args := []string{"migrate", "status", "--log", "{{ json . }}"}
	if data.Env != "" {
		args = append(args, "--env", data.Env)
	}
	if data.ConfigURL != "" {
		args = append(args, "--config", data.ConfigURL)
	}
	if data.URL != "" {
		args = append(args, "--url", data.URL)
	}
	if data.DirURL != "" {
		args = append(args, "--dir", data.DirURL)
	}
	if data.RevisionsSchema != "" {
		args = append(args, "--revisions-schema", data.RevisionsSchema)
	}
	return jsonDecode[StatusReport](c.runCommand(ctx, args))
}

// runCommand runs the given command and unmarshals the output into the given
// interface.
func (c *Client) runCommand(ctx context.Context, args []string) (io.Reader, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, c.execPath, args...)
	cmd.Dir = c.workingDir
	cmd.Env = append(cmd.Env, "ATLAS_NO_UPDATE_NOTIFIER=1")
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		switch {
		case stderr.Len() > 0:
			// Atlas CLI writes the error to stderr.
			// So this's critical issue, return the error.
			return nil, &cliError{
				summary: stderr.String(),
				detail:  stdout.String(),
			}
		case !json.Valid(stdout.Bytes()):
			// When the output is not valid JSON, it means that
			// the command failed.
			return nil, &cliError{
				summary: "Atlas CLI",
				detail:  stdout.String(),
			}
		case cmd.ProcessState.ExitCode() == 1:
			// When the exit code is 1, it means that the command
			// failed but the output is still valid JSON.
			//
			// `atlas migrate lint` returns 1 when there are
			// linting errors.
		default:
			// When the exit code is not 1, it means that the
			// command wasn't executed successfully.
			return nil, err
		}
	}
	return &stdout, nil
}

// LatestVersion returns the latest version of the migrations directory.
func (r StatusReport) LatestVersion() string {
	if l := len(r.Available); l > 0 {
		return r.Available[l-1].Version
	}
	return ""
}

// Amount returns the number of migrations need to apply
// for the given version.
//
// The second return value is true if the version is found
// and the database is up-to-date.
//
// If the version is not found, it returns 0 and the second
// return value is false.
func (r StatusReport) Amount(version string) (amount uint64, ok bool) {
	if version == "" {
		amount := uint64(len(r.Pending))
		return amount, amount == 0
	}
	if r.Current == version {
		return amount, true
	}
	for idx, v := range r.Pending {
		if v.Version == version {
			amount = uint64(idx + 1)
			break
		}
	}
	return amount, false
}

// TempFile creates a temporary file with the given content and extension.
func TempFile(content, ext string) (string, func() error, error) {
	f, err := os.CreateTemp("", "atlasexec-*."+ext)
	if err != nil {
		return "", nil, err
	}
	defer f.Close()
	_, err = f.WriteString(content)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("file://%s", f.Name()), func() error {
		return os.Remove(f.Name())
	}, nil
}

func execPath(dir, name string) (file string, err error) {
	file = filepath.Join(dir, name)
	if _, err = os.Stat(file); err == nil {
		return file, nil
	}
	// If the binary is not in the current directory,
	// try to find it in the PATH.
	return exec.LookPath(name)
}

type cliError struct {
	summary string
	detail  string
}

// Error implements the error interface.
func (e cliError) Error() string {
	return fmt.Sprintf("atlasexec: %s, %s", e.Summary(), e.Detail())
}

// Summary implements the diag.Diagnostic interface.
func (e cliError) Summary() string {
	if strings.HasPrefix(e.summary, "Error: ") {
		return e.summary[7:]
	}
	return e.summary
}

// Detail implements the diag.Diagnostic interface.
func (e cliError) Detail() string {
	return strings.TrimSpace(e.detail)
}

func (v Vars) AsArgs() []string {
	var args []string
	for k, v := range v {
		args = append(args, "--var", fmt.Sprintf("%s=%s", k, v))
	}
	return args
}

func stringVal(r io.Reader, err error) (string, error) {
	if err != nil {
		return "", err
	}
	s, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	return string(s), nil
}

func jsonDecode[T any](r io.Reader, err error) (*T, error) {
	if err != nil {
		return nil, err
	}
	var dst T
	if err = json.NewDecoder(r).Decode(&dst); err != nil {
		return nil, err
	}
	return &dst, nil
}
