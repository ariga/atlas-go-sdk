package atlasexec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

type (
	// Client is a client for the Atlas CLI.
	Client struct {
		execPath   string
		workingDir string
	}
	// LoginParams are the parameters for the `login` command.
	LoginParams struct {
		Token string
	}
	// MigratePushParams are the parameters for the `migrate push` command.
	MigratePushParams struct {
		Name        string
		Tag         string
		DevURL      string
		DirURL      string
		DirFormat   string
		LockTimeout string
		Context     string
		ConfigURL   string
		Env         string
		Vars        Vars
	}
	// MigrateApplyParams are the parameters for the `migrate apply` command.
	MigrateApplyParams struct {
		Env             string
		ConfigURL       string
		DirURL          string
		URL             string
		RevisionsSchema string
		BaselineVersion string
		TxMode          string
		Amount          uint64
		Vars            Vars
	}
	// MigrateStatusParams are the parameters for the `migrate status` command.
	MigrateStatusParams struct {
		Env             string
		ConfigURL       string
		DirURL          string
		URL             string
		RevisionsSchema string
		Vars            Vars
	}
	// MigrateLintParams are the parameters for the `migrate lint` command.
	MigrateLintParams struct {
		Env       string
		ConfigURL string
		DevURL    string
		DirURL    string
		Context   string
		Web       bool
		Latest    uint64
		Vars      Vars
		Writer    io.Writer
		Base      string
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
		Vars      Vars
	}
	Vars map[string]string
)

type (
	// Deprecated: MigrateApplyParams instead.
	ApplyParams = MigrateApplyParams
	// Deprecated: use MigrateStatusParams instead.
	StatusParams = MigrateStatusParams
	// Deprecated: use MigrateLintParams instead.
	LintParams = MigrateLintParams
)

// NewClient returns a new Atlas client with the given atlas-cli path.
func NewClient(workingDir, execPath string) (*Client, error) {
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
// Deprecated: use MigrateApply instead.
func (c *Client) Apply(ctx context.Context, params *MigrateApplyParams) (*MigrateApply, error) {
	return c.MigrateApply(ctx, params)
}

// Lint runs the 'migrate lint' command.
// Deprecated: use MigrateLint instead.
func (c *Client) Lint(ctx context.Context, params *MigrateLintParams) (*SummaryReport, error) {
	return c.MigrateLint(ctx, params)
}

// Status runs the 'migrate status' command.
// Deprecated: use MigrateStatus instead.
func (c *Client) Status(ctx context.Context, params *MigrateStatusParams) (*MigrateStatus, error) {
	return c.MigrateStatus(ctx, params)
}

// Login runs the 'login' command.
func (c *Client) Login(ctx context.Context, params *LoginParams) error {
	if params.Token == "" {
		return errors.New("token cannot be empty")
	}
	_, err := c.runCommand(ctx, []string{"login", "--token", params.Token})
	return err
}

// Logout runs the 'logout' command.
func (c *Client) Logout(ctx context.Context) error {
	_, err := c.runCommand(ctx, []string{"logout"})
	return err
}

// MigratePush runs the 'migrate push' command.
func (c *Client) MigratePush(ctx context.Context, params *MigratePushParams) (string, error) {
	args := []string{"migrate", "push"}
	if params.DevURL != "" {
		args = append(args, "--dev-url", params.DevURL)
	}
	if params.DirURL != "" {
		args = append(args, "--dir", params.DirURL)
	}
	if params.DirFormat != "" {
		args = append(args, "--dir-format", params.DirFormat)
	}
	if params.LockTimeout != "" {
		args = append(args, "--lock-timeout", params.LockTimeout)
	}
	if params.Context != "" {
		args = append(args, "--context", params.Context)
	}
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	args = append(args, params.Vars.AsArgs()...)
	if params.Name == "" {
		return "", errors.New("directory name cannot be empty")
	}
	if params.Tag != "" {
		args = append(args, fmt.Sprintf("%s:%s", params.Name, params.Tag))
	} else {
		args = append(args, params.Name)
	}
	resp, err := stringVal(c.runCommand(ctx, args))
	return strings.TrimSpace(resp), err
}

// MigrateApply runs the 'migrate apply' command.
func (c *Client) MigrateApply(ctx context.Context, params *MigrateApplyParams) (*MigrateApply, error) {
	args := []string{"migrate", "apply", "--format", "{{ json . }}"}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.URL != "" {
		args = append(args, "--url", params.URL)
	}
	if params.DirURL != "" {
		args = append(args, "--dir", params.DirURL)
	}
	if params.RevisionsSchema != "" {
		args = append(args, "--revisions-schema", params.RevisionsSchema)
	}
	if params.BaselineVersion != "" {
		args = append(args, "--baseline", params.BaselineVersion)
	}
	if params.TxMode != "" {
		args = append(args, "--tx-mode", params.TxMode)
	}
	if params.Amount > 0 {
		args = append(args, strconv.FormatUint(params.Amount, 10))
	}
	args = append(args, params.Vars.AsArgs()...)
	return jsonDecode[MigrateApply](c.runCommand(ctx, args))
}

// SchemaApply runs the 'schema apply' command.
func (c *Client) SchemaApply(ctx context.Context, params *SchemaApplyParams) (*SchemaApply, error) {
	args := []string{"schema", "apply", "--format", "{{ json . }}"}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.URL != "" {
		args = append(args, "--url", params.URL)
	}
	if params.To != "" {
		args = append(args, "--to", params.To)
	}
	if params.DryRun {
		args = append(args, "--dry-run")
	} else {
		args = append(args, "--auto-approve")
	}
	if params.DevURL != "" {
		args = append(args, "--dev-url", params.DevURL)
	}
	if len(params.Schema) > 0 {
		args = append(args, "--schema", strings.Join(params.Schema, ","))
	}
	if len(params.Exclude) > 0 {
		args = append(args, "--exclude", strings.Join(params.Exclude, ","))
	}
	args = append(args, params.Vars.AsArgs()...)
	return jsonDecode[SchemaApply](c.runCommand(ctx, args))
}

// SchemaInspect runs the 'schema inspect' command.
func (c *Client) SchemaInspect(ctx context.Context, params *SchemaInspectParams) (string, error) {
	args := []string{"schema", "inspect"}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.URL != "" {
		args = append(args, "--url", params.URL)
	}
	if params.DevURL != "" {
		args = append(args, "--dev-url", params.DevURL)
	}
	if params.Format == "sql" {
		args = append(args, "--format", "{{ sql . }}")
	}
	if len(params.Schema) > 0 {
		args = append(args, "--schema", strings.Join(params.Schema, ","))
	}
	if len(params.Exclude) > 0 {
		args = append(args, "--exclude", strings.Join(params.Exclude, ","))
	}
	args = append(args, params.Vars.AsArgs()...)
	return stringVal(c.runCommand(ctx, args))
}

func lintArgs(params *MigrateLintParams) []string {
	args := []string{"migrate", "lint"}
	if params.Web {
		args = append(args, "-w")
	} else {
		args = append(args, "--format", "{{ json . }}")
	}
	if params.Context != "" {
		args = append(args, "--context", params.Context)
	}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.DevURL != "" {
		args = append(args, "--dev-url", params.DevURL)
	}
	if params.DirURL != "" {
		args = append(args, "--dir", params.DirURL)
	}
	if params.Base != "" {
		args = append(args, "--base", params.Base)
	}
	if params.Latest > 0 {
		args = append(args, "--latest", strconv.FormatUint(params.Latest, 10))
	}
	args = append(args, params.Vars.AsArgs()...)
	return args
}

// MigrateLint runs the 'migrate lint' command.
func (c *Client) MigrateLint(ctx context.Context, params *MigrateLintParams) (*SummaryReport, error) {
	if params.Writer != nil || params.Web {
		return nil, errors.New("atlasexec: Writer or Web reporting are not supported with MigrateLint, use MigrateLintError")
	}
	r, err := c.runCommand(ctx, lintArgs(params))
	if cliErr := (cliError{}); errors.As(err, &cliErr) && cliErr.stderr == "" {
		r = strings.NewReader(cliErr.stdout)
		err = nil
	}
	return jsonDecode[SummaryReport](r, err)
}

// MigrateLintError runs the 'migrate lint' command, the output is written to params.Writer
func (c *Client) MigrateLintError(ctx context.Context, params *MigrateLintParams) error {
	r, err := c.runCommand(ctx, lintArgs(params))
	if cliErr := (cliError{}); errors.As(err, &cliErr) && cliErr.stderr == "" {
		r = strings.NewReader(cliErr.stdout)
		err = fmt.Errorf("linting failed, for more details, go to %v", cliErr.stdout)
	}
	if params.Writer != nil && r != nil {
		if _, ioErr := io.Copy(params.Writer, r); ioErr != nil {
			err = errors.Join(err, ioErr)
		}
	}
	return err
}

// MigrateStatus runs the 'migrate status' command.
func (c *Client) MigrateStatus(ctx context.Context, params *MigrateStatusParams) (*MigrateStatus, error) {
	args := []string{"migrate", "status", "--format", "{{ json . }}"}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.URL != "" {
		args = append(args, "--url", params.URL)
	}
	if params.DirURL != "" {
		args = append(args, "--dir", params.DirURL)
	}
	if params.RevisionsSchema != "" {
		args = append(args, "--revisions-schema", params.RevisionsSchema)
	}
	args = append(args, params.Vars.AsArgs()...)
	return jsonDecode[MigrateStatus](c.runCommand(ctx, args))
}

var reVersion = regexp.MustCompile(`^atlas version v(\d+\.\d+.\d+)-?([a-z0-9]*)?`)

// Version runs the 'version' command.
func (c *Client) Version(ctx context.Context) (*Version, error) {
	r, err := c.runCommand(ctx, []string{"version"})
	if err != nil {
		return nil, err
	}
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	v := reVersion.FindSubmatch(out)
	if v == nil {
		return nil, errors.New("unexpected output format")
	}
	var sha string
	if len(v) > 2 {
		sha = string(v[2])
	}
	return &Version{
		Version: string(v[1]),
		SHA:     sha,
		Canary:  strings.Contains(string(out), "canary"),
	}, nil
}

// runCommand runs the given command and returns its output.
func (c *Client) runCommand(ctx context.Context, args []string) (io.Reader, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, c.execPath, args...)
	cmd.Dir = c.workingDir
	// Set ATLAS_NO_UPDATE_NOTIFIER=1 to disable the update notifier.
	// use os.Environ() to avoid overriding the user's environment.
	cmd.Env = append(os.Environ(), "ATLAS_NO_UPDATE_NOTIFIER=1")
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return nil, cliError{
			stderr: strings.TrimSpace(stderr.String()),
			stdout: strings.TrimSpace(stdout.String()),
		}
	}
	return &stdout, nil
}

// LatestVersion returns the latest version of the migration directory.
func (r MigrateStatus) LatestVersion() string {
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
func (r MigrateStatus) Amount(version string) (amount uint64, ok bool) {
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

type cliError struct {
	stdout string
	stderr string
}

// Error implements the error interface.
func (e cliError) Error() string {
	if e.stderr != "" {
		return e.stderr
	}
	return e.stdout
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
	buf, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var dst T
	if err = json.Unmarshal(buf, &dst); err != nil {
		return nil, cliError{
			stdout: string(buf),
		}
	}
	return &dst, nil
}
