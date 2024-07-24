package atlasexec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

type (
	// Client is a client for the Atlas CLI.
	Client struct {
		execPath   string
		workingDir string
		env        Environ
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
		Context     *RunContext
		ConfigURL   string
		Env         string
		Vars        Vars
	}
	// TriggerType defines the type for the "trigger_type" enum field.
	TriggerType string
	// MigrateExecOrder define how Atlas computes and executes pending migration files to the database.
	// See: https://atlasgo.io/versioned/apply#execution-order
	MigrateExecOrder string
	// DeployRunContext describes what triggered this command (e.g., GitHub Action, v1.2.3)
	DeployRunContext struct {
		TriggerType    TriggerType `json:"triggerType,omitempty"`
		TriggerVersion string      `json:"triggerVersion,omitempty"`
	}
	// MigrateApplyParams are the parameters for the `migrate apply` command.
	MigrateApplyParams struct {
		Env             string
		ConfigURL       string
		Context         *DeployRunContext
		DirURL          string
		AllowDirty      bool
		URL             string
		RevisionsSchema string
		BaselineVersion string
		TxMode          string
		ExecOrder       MigrateExecOrder
		Amount          uint64
		DryRun          bool
		Vars            Vars
	}
	// MigrateDownParams are the parameters for the `migrate down` command.
	MigrateDownParams struct {
		Env             string
		ConfigURL       string
		DevURL          string
		Context         *DeployRunContext
		DirURL          string
		URL             string
		RevisionsSchema string
		Amount          uint64
		ToVersion       string
		ToTag           string
		Vars            Vars

		// Not yet supported
		// DryRun          bool
		// TxMode          string
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
	// RunContext describes what triggered this command (e.g., GitHub Action).
	RunContext struct {
		Repo     string `json:"repo,omitempty"`
		Path     string `json:"path,omitempty"`
		Branch   string `json:"branch,omitempty"`
		Commit   string `json:"commit,omitempty"`
		URL      string `json:"url,omitempty"`
		Username string `json:"username,omitempty"` // The username that triggered the event that initiated the command.
		UserID   string `json:"userID,omitempty"`   // The user ID that triggered the event that initiated the command.
		SCMType  string `json:"scmType,omitempty"`  // Source control management system type.
	}
	// MigrateLintParams are the parameters for the `migrate lint` command.
	MigrateLintParams struct {
		Env       string
		ConfigURL string
		DevURL    string
		DirURL    string
		Context   *RunContext
		Web       bool
		Latest    uint64
		Vars      Vars
		Writer    io.Writer
		Base      string
		Format    string
	}
	// MigrateTestParams are the parameters for the `migrate test` command.
	MigrateTestParams struct {
		Env             string
		ConfigURL       string
		DirURL          string
		Context         *RunContext
		DevURL          string
		DirFormat       string
		Run             string
		RevisionsSchema string
		Vars            Vars
	}
	// SchemaApplyParams are the parameters for the `schema apply` command.
	SchemaApplyParams struct {
		Env       string
		ConfigURL string
		DevURL    string
		DryRun    bool
		TxMode    string
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
	// SchemaTestParams are the parameters for the `schema test` command.
	SchemaTestParams struct {
		Env       string
		ConfigURL string
		URL       string
		DevURL    string
		Run       string
		Vars      Vars
	}
	Vars map[string]string
	// Environ is a map of environment variables.
	Environ map[string]string
)

// TriggerType values.
const (
	TriggerTypeCLI          TriggerType = "CLI"
	TriggerTypeKubernetes   TriggerType = "KUBERNETES"
	TriggerTypeTerraform    TriggerType = "TERRAFORM"
	TriggerTypeGithubAction TriggerType = "GITHUB_ACTION"
	TriggerTypeCircleCIOrb  TriggerType = "CIRCLECI_ORB"
)

// ExecutionOrder values.
const (
	ExecOrderLinear     MigrateExecOrder = "linear" // Default
	ExecOrderLinearSkip MigrateExecOrder = "linear-skip"
	ExecOrderNonLinear  MigrateExecOrder = "non-linear"
)

// NewClient returns a new Atlas client with the given atlas-cli path.
func NewClient(workingDir, execPath string) (_ *Client, err error) {
	if execPath == "" {
		return nil, fmt.Errorf("execPath cannot be empty")
	} else if execPath, err = exec.LookPath(execPath); err != nil {
		return nil, fmt.Errorf("looking up atlas-cli: %w", err)
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

// WithWorkDir creates a new client with the given working directory.
// It is useful to run multiple commands in the multiple directories.
//
// Example:
//
//	client := atlasexec.NewClient("", "atlas")
//	err := client.WithWorkDir("dir1", func(c *atlasexec.Client) error {
//	  _, err := c.MigrateApply(ctx, &atlasexec.MigrateApplyParams{
//	  })
//	  return err
//	})
func (c *Client) WithWorkDir(dir string, fn func(*Client) error) error {
	wd := c.workingDir
	defer func() { c.workingDir = wd }()
	c.workingDir = dir
	return fn(c)
}

// SetEnv allows we override the environment variables for the atlas-cli.
// To append new environment variables to environment from OS, use NewOSEnviron() then add new variables.
func (c *Client) SetEnv(env map[string]string) error {
	for k := range env {
		if _, ok := defaultEnvs[k]; ok {
			return fmt.Errorf("atlasexec: cannot override the default environment variable %q", k)
		}
	}
	c.env = env
	return nil
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
	if params.Context != nil {
		buf, err := json.Marshal(params.Context)
		if err != nil {
			return "", err
		}
		args = append(args, "--context", string(buf))
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
	return firstResult(c.MigrateApplySlice(ctx, params))
}

// MigrateApplySlice runs the 'migrate apply' command for multiple targets.
func (c *Client) MigrateApplySlice(ctx context.Context, params *MigrateApplyParams) ([]*MigrateApply, error) {
	args := []string{"migrate", "apply", "--format", "{{ json . }}"}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.Context != nil {
		buf, err := json.Marshal(params.Context)
		if err != nil {
			return nil, err
		}
		args = append(args, "--context", string(buf))
	}
	if params.URL != "" {
		args = append(args, "--url", params.URL)
	}
	if params.DirURL != "" {
		args = append(args, "--dir", params.DirURL)
	}
	if params.AllowDirty {
		args = append(args, "--allow-dirty")
	}
	if params.DryRun {
		args = append(args, "--dry-run")
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
	if params.ExecOrder != "" {
		args = append(args, "--exec-order", string(params.ExecOrder))
	}
	if params.Amount > 0 {
		args = append(args, strconv.FormatUint(params.Amount, 10))
	}
	args = append(args, params.Vars.AsArgs()...)
	return jsonDecodeErr[MigrateApply](newMigrateApplyError)(c.runCommand(ctx, args))
}

// MigrateDown runs the 'migrate down' command.
func (c *Client) MigrateDown(ctx context.Context, params *MigrateDownParams) (*MigrateDown, error) {
	args := []string{"migrate", "down", "--format", "{{ json . }}"}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.DevURL != "" {
		args = append(args, "--dev-url", params.DevURL)
	}
	if params.Context != nil {
		buf, err := json.Marshal(params.Context)
		if err != nil {
			return nil, err
		}
		args = append(args, "--context", string(buf))
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
	if params.ToVersion != "" {
		args = append(args, "--to-version", params.ToVersion)
	}
	if params.ToTag != "" {
		args = append(args, "--to-tag", params.ToTag)
	}
	if params.Amount > 0 {
		args = append(args, strconv.FormatUint(params.Amount, 10))
	}
	args = append(args, params.Vars.AsArgs()...)
	r, err := c.runCommand(ctx, args)
	if cliErr := (&Error{}); errors.As(err, &cliErr) && cliErr.Stderr == "" {
		r = strings.NewReader(cliErr.Stdout)
		err = nil
	}
	// NOTE: This command only support one result.
	return firstResult(jsonDecode[MigrateDown](r, err))
}

// MigrateTest runs the 'migrate test' command.
func (c *Client) MigrateTest(ctx context.Context, params *MigrateTestParams) (string, error) {
	args := []string{"migrate", "test"}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.DirURL != "" {
		args = append(args, "--dir", params.DirURL)
	}
	if params.DirFormat != "" {
		args = append(args, "--dir-format", params.DirFormat)
	}
	if params.DevURL != "" {
		args = append(args, "--dev-url", params.DevURL)
	}
	if params.Context != nil {
		buf, err := json.Marshal(params.Context)
		if err != nil {
			return "", err
		}
		args = append(args, "--context", string(buf))
	}
	if params.RevisionsSchema != "" {
		args = append(args, "--revisions-schema", params.RevisionsSchema)
	}
	if params.Run != "" {
		args = append(args, "--run", params.Run)
	}
	args = append(args, params.Vars.AsArgs()...)
	return stringVal(c.runCommand(ctx, args))
}

// SchemaApply runs the 'schema apply' command.
func (c *Client) SchemaApply(ctx context.Context, params *SchemaApplyParams) (*SchemaApply, error) {
	return firstResult(c.SchemaApplySlice(ctx, params))
}

// SchemaApplySlice runs the 'schema apply' command for multiple targets.
func (c *Client) SchemaApplySlice(ctx context.Context, params *SchemaApplyParams) ([]*SchemaApply, error) {
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
	if params.TxMode != "" {
		args = append(args, "--tx-mode", params.TxMode)
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
	return jsonDecodeErr[SchemaApply](newSchemaApplyError)(c.runCommand(ctx, args))
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
	switch {
	case params.Format == "sql":
		args = append(args, "--format", "{{ sql . }}")
	case params.Format != "":
		args = append(args, "--format", params.Format)
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

// SchemaTest runs the 'schema test' command.
func (c *Client) SchemaTest(ctx context.Context, params *SchemaTestParams) (string, error) {
	args := []string{"schema", "test"}
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
	if params.Run != "" {
		args = append(args, "--run", params.Run)
	}
	args = append(args, params.Vars.AsArgs()...)
	return stringVal(c.runCommand(ctx, args))
}

func lintArgs(params *MigrateLintParams) ([]string, error) {
	args := []string{"migrate", "lint"}
	if params.Web {
		args = append(args, "-w")
	}
	if params.Context != nil {
		buf, err := json.Marshal(params.Context)
		if err != nil {
			return nil, err
		}
		args = append(args, "--context", string(buf))
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
	format := "{{ json . }}"
	if params.Format != "" {
		format = params.Format
	}
	args = append(args, "--format", format)
	return args, nil
}

// MigrateLint runs the 'migrate lint' command.
func (c *Client) MigrateLint(ctx context.Context, params *MigrateLintParams) (*SummaryReport, error) {
	if params.Writer != nil || params.Web {
		return nil, errors.New("atlasexec: Writer or Web reporting are not supported with MigrateLint, use MigrateLintError")
	}
	args, err := lintArgs(params)
	if err != nil {
		return nil, err
	}
	r, err := c.runCommand(ctx, args)
	if cliErr := (&Error{}); errors.As(err, &cliErr) && cliErr.Stderr == "" {
		r = strings.NewReader(cliErr.Stdout)
		err = nil
	}
	// NOTE: This command only support one result.
	return firstResult(jsonDecode[SummaryReport](r, err))
}

// LintErr is returned when the 'migrate lint' finds a diagnostic that is configured to
// be reported as an error, such as destructive changes by default.
var LintErr = errors.New("lint error")

// MigrateLintError runs the 'migrate lint' command, the output is written to params.Writer and reports
// if an error occurred. If the error is a setup error, a Error is returned. If the error is a lint error,
// LintErr is returned.
func (c *Client) MigrateLintError(ctx context.Context, params *MigrateLintParams) error {
	args, err := lintArgs(params)
	if err != nil {
		return err
	}
	r, err := c.runCommand(ctx, args)
	var (
		cliErr *Error
		isCLI  = errors.As(err, &cliErr)
	)
	// Setup errors.
	if isCLI && cliErr.Stderr != "" {
		return cliErr
	}
	// Lint errors.
	if isCLI && cliErr.Stdout != "" {
		err = LintErr
		r = strings.NewReader(cliErr.Stdout)
	}
	// Unknown errors.
	if err != nil && !isCLI {
		return err
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
	// NOTE: This command only support one result.
	return firstResult(jsonDecode[MigrateStatus](c.runCommand(ctx, args)))
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

// NewOSEnviron returns the current environment variables from the OS.
func NewOSEnviron() Environ {
	env := map[string]string{}
	for _, ev := range os.Environ() {
		parts := strings.SplitN(ev, "=", 2)
		if len(parts) == 0 {
			continue
		}
		k := parts[0]
		v := ""
		if len(parts) == 2 {
			v = parts[1]
		}
		env[k] = v
	}
	return env
}

// ToSlice converts the environment variables to a slice.
func (e Environ) ToSlice() []string {
	keys := make([]string, 0, len(e))
	for k := range e {
		keys = append(keys, k)
	}
	// Ensure the order of the keys.
	slices.Sort(keys)
	env := make([]string, 0, len(e))
	for _, k := range keys {
		env = append(env, k+"="+e[k])
	}
	return env
}

var defaultEnvs = map[string]string{
	// Disable the update notifier and upgrade suggestions.
	"ATLAS_NO_UPDATE_NOTIFIER":     "1",
	"ATLAS_NO_UPGRADE_SUGGESTIONS": "1",
}

// runCommand runs the given command and returns its output.
func (c *Client) runCommand(ctx context.Context, args []string) (io.Reader, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, c.execPath, args...)
	cmd.Dir = c.workingDir
	var env Environ
	if c.env == nil {
		// Initialize the environment variables from the OS.
		env = NewOSEnviron()
	} else {
		env = maps.Clone(c.env)
	}
	maps.Copy(env, defaultEnvs)
	cmd.Env = env.ToSlice()
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		cerr := &Error{
			Stderr: strings.TrimSpace(stderr.String()),
			Stdout: strings.TrimSpace(stdout.String()),
		}
		if exitErr := (&exec.ExitError{}); errors.As(err, &exitErr) {
			cerr.err = exitErr
		}
		return nil, cerr
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

type Error struct {
	err    *exec.ExitError
	Stdout string
	Stderr string
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e.Stderr != "" {
		return e.Stderr
	}
	return e.Stdout
}

// ExitCode returns the exit code of the command.
func (e *Error) ExitCode() int {
	if e.err == nil {
		return new(exec.ExitError).ExitCode()
	}
	return e.err.ExitCode()
}

func (e *Error) Unwrap() error {
	return e.err
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

func firstResult[T ~[]E, E any](r T, err error) (e E, _ error) {
	switch {
	case err != nil:
		return e, err
	case len(r) == 1:
		return r[0], nil
	default:
		return e, errors.New("The command returned more than one result, use Slice function instead")
	}
}

func jsonDecode[T any](r io.Reader, err error) ([]*T, error) {
	if err != nil {
		return nil, err
	}
	buf, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var dst []*T
	dec := json.NewDecoder(bytes.NewReader(buf))
	for {
		var m T
		switch err := dec.Decode(&m); err {
		case io.EOF:
			return dst, nil
		case nil:
			dst = append(dst, &m)
		default:
			return nil, &Error{
				Stdout: string(buf),
			}
		}
	}
}

func jsonDecodeErr[T any](fn func([]*T) error) func(io.Reader, error) ([]*T, error) {
	return func(r io.Reader, err error) ([]*T, error) {
		if err != nil {
			if cliErr := (&Error{}); errors.As(err, &cliErr) && cliErr.Stderr == "" {
				d, err := jsonDecode[T](strings.NewReader(cliErr.Stdout), nil)
				if err == nil {
					return nil, fn(d)
				}
				// If the error is not a JSON, return the original error.
			}
			return nil, err
		}
		return jsonDecode[T](r, err)
	}
}
