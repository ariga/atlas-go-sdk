package atlasexec

import (
	"context"
	"encoding/json"
	"fmt"
)

type (
	// SchemaApplyParams are the parameters for the `schema apply` command.
	SchemaApplyParams struct {
		ConfigURL string
		Env       string
		Vars      VarArgs
		DevURL    string

		URL     string
		To      string
		TxMode  string
		Exclude []string
		Schema  []string
		DryRun  bool
	}
	// SchemaApply contains a summary of a 'schema apply' execution on a database.
	SchemaApply struct {
		Env
		Changes Changes `json:"Changes,omitempty"`
		// General error that occurred during execution.
		// e.g., when committing or rolling back a transaction.
		Error string `json:"Error,omitempty"`
	}
	// SchemaApplyError is returned when an error occurred
	// during a schema applying attempt.
	SchemaApplyError struct {
		Result []*SchemaApply
	}
	// SchemaInspectParams are the parameters for the `schema inspect` command.
	SchemaInspectParams struct {
		ConfigURL string
		Env       string
		Vars      VarArgs
		Format    string
		DevURL    string

		URL     string
		Exclude []string
		Schema  []string
	}
	// SchemaTestParams are the parameters for the `schema test` command.
	SchemaTestParams struct {
		ConfigURL string
		Env       string
		Vars      VarArgs
		DevURL    string

		URL string
		Run string
	}
	// SchemaPlanParams are the parameters for the `schema plan` command.
	SchemaPlanParams struct {
		ConfigURL string
		Env       string
		Vars      VarArgs
		Context   *RunContext
		DevURL    string

		From, To []string
		Repo     string
		Name     string
		// The below are mutually exclusive and can be replaced
		// with the 'schema plan' sub-commands instead.
		DryRun     bool // If false, --auto-approve is set.
		Pending    bool
		Push, Save bool
	}
	// SchemaPlanListParams are the parameters for the `schema plan list` command.
	SchemaPlanListParams struct {
		ConfigURL string
		Env       string
		Vars      VarArgs
		Context   *RunContext
		DevURL    string

		From, To []string
		Repo     string
		Pending  bool // If true, only pending plans are listed.
	}
	// SchemaPlanPushParams are the parameters for the `schema plan push` command.
	SchemaPlanPushParams struct {
		ConfigURL string
		Env       string
		Vars      VarArgs
		Context   *RunContext
		DevURL    string

		From, To []string
		Repo     string
		Pending  bool // Push plan in pending state.
		File     string
	}
	// SchemaPlanPullParams are the parameters for the `schema plan pull` command.
	SchemaPlanPullParams struct {
		ConfigURL string
		Env       string
		Vars      VarArgs
		URL       string
	}
	// SchemaPlanLintParams are the parameters for the `schema plan lint` command.
	SchemaPlanLintParams struct {
		ConfigURL string
		Env       string
		Vars      VarArgs
		Context   *RunContext
		DevURL    string

		From, To []string
		Repo     string
		File     string
	}
	// SchemaPlanValidateParams are the parameters for the `schema plan validate` command.
	SchemaPlanValidateParams struct {
		ConfigURL string
		Env       string
		Vars      VarArgs
		Context   *RunContext
		DevURL    string

		From, To []string
		Repo     string
		Name     string
		File     string
	}
	// SchemaPlanApproveParams are the parameters for the `schema plan approve` command.
	SchemaPlanApproveParams struct {
		ConfigURL string
		Env       string
		Vars      VarArgs

		URL string
	}
	// SchemaPlan is the result of a 'schema plan' command.
	SchemaPlan struct {
		Env   Env             `json:"Env,omitempty"`   // Environment info.
		Repo  string          `json:"Repo,omitempty"`  // Repository name.
		Lint  *SummaryReport  `json:"Lint,omitempty"`  // Lint report.
		File  *SchemaPlanFile `json:"File,omitempty"`  // Plan file.
		Error string          `json:"Error,omitempty"` // Any error occurred during planning.
	}
	// SchemaPlanApprove is the result of a 'schema plan approve' command.
	SchemaPlanApprove struct {
		URL    string `json:"URL,omitempty"`    // URL of the plan in Atlas format.
		Link   string `json:"Link,omitempty"`   // Link to the plan in the registry.
		Status string `json:"Status,omitempty"` // Status of the plan in the registry.
	}
	// SchemaPlanFile is a JSON representation of a schema plan file.
	SchemaPlanFile struct {
		Name      string `json:"Name,omitempty"`      // Name of the plan.
		FromHash  string `json:"FromHash,omitempty"`  // Hash of the 'from' realm.
		ToHash    string `json:"ToHash,omitempty"`    // Hash of the 'to' realm.
		Migration string `json:"Migration,omitempty"` // Migration SQL.
		// registry only fields.
		URL    string `json:"URL,omitempty"`    // URL of the plan in Atlas format.
		Link   string `json:"Link,omitempty"`   // Link to the plan in the registry.
		Status string `json:"Status,omitempty"` // Status of the plan in the registry.
	}
)

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
		args = append(args, "--schema", listString(params.Schema))
	}
	if len(params.Exclude) > 0 {
		args = append(args, "--exclude", listString(params.Exclude))
	}
	if params.Vars != nil {
		args = append(args, params.Vars.AsArgs()...)
	}
	return jsonDecodeErr(newSchemaApplyError)(c.runCommand(ctx, args))
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
		args = append(args, "--schema", listString(params.Schema))
	}
	if len(params.Exclude) > 0 {
		args = append(args, "--exclude", listString(params.Exclude))
	}
	if params.Vars != nil {
		args = append(args, params.Vars.AsArgs()...)
	}
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
	if params.Vars != nil {
		args = append(args, params.Vars.AsArgs()...)
	}
	return stringVal(c.runCommand(ctx, args))
}

// SchemaPlan runs the `schema plan` command.
func (c *Client) SchemaPlan(ctx context.Context, params *SchemaPlanParams) (*SchemaPlan, error) {
	args := []string{"schema", "plan", "--format", "{{ json . }}"}
	// Global flags
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.Vars != nil {
		args = append(args, params.Vars.AsArgs()...)
	}
	// Hidden flags
	if params.Context != nil {
		buf, err := json.Marshal(params.Context)
		if err != nil {
			return nil, err
		}
		args = append(args, "--context", string(buf))
	}
	// Flags of the 'schema plan' sub-commands
	if params.DevURL != "" {
		args = append(args, "--dev-url", params.DevURL)
	}
	if len(params.From) > 0 {
		args = append(args, "--from", listString(params.From))
	}
	if len(params.To) > 0 {
		args = append(args, "--to", listString(params.To))
	}
	if params.Name != "" {
		args = append(args, "--name", params.Name)
	}
	if params.Repo != "" {
		args = append(args, "--repo", params.Repo)
	}
	if params.Save {
		args = append(args, "--save")
	}
	if params.Push {
		args = append(args, "--push")
	}
	if params.Pending {
		args = append(args, "--pending")
	}
	if params.DryRun {
		args = append(args, "--dry-run")
	} else {
		args = append(args, "--auto-approve")
	}
	// NOTE: This command only support one result.
	return firstResult(jsonDecode[SchemaPlan](c.runCommand(ctx, args)))
}

// SchemaPlanList runs the `schema plan list` command.
func (c *Client) SchemaPlanList(ctx context.Context, params *SchemaPlanListParams) ([]SchemaPlanFile, error) {
	args := []string{"schema", "plan", "list", "--format", "{{ json . }}"}
	// Global flags
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.Vars != nil {
		args = append(args, params.Vars.AsArgs()...)
	}
	// Hidden flags
	if params.Context != nil {
		buf, err := json.Marshal(params.Context)
		if err != nil {
			return nil, err
		}
		args = append(args, "--context", string(buf))
	}
	// Flags of the 'schema plan lint' sub-commands
	if params.DevURL != "" {
		args = append(args, "--dev-url", params.DevURL)
	}
	if len(params.From) > 0 {
		args = append(args, "--from", listString(params.From))
	}
	if len(params.To) > 0 {
		args = append(args, "--to", listString(params.To))
	}
	if params.Repo != "" {
		args = append(args, "--repo", params.Repo)
	}
	if params.Pending {
		args = append(args, "--pending")
	}
	args = append(args, "--auto-approve")
	// NOTE: This command only support one result.
	v, err := firstResult(jsonDecode[[]SchemaPlanFile](c.runCommand(ctx, args)))
	if err != nil {
		return nil, err
	}
	return *v, nil
}

// SchemaPlanPush runs the `schema plan push` command.
func (c *Client) SchemaPlanPush(ctx context.Context, params *SchemaPlanPushParams) (string, error) {
	args := []string{"schema", "plan", "push", "--format", "{{ json . }}"}
	// Global flags
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.Vars != nil {
		args = append(args, params.Vars.AsArgs()...)
	}
	// Hidden flags
	if params.Context != nil {
		buf, err := json.Marshal(params.Context)
		if err != nil {
			return "", err
		}
		args = append(args, "--context", string(buf))
	}
	// Flags of the 'schema plan push' sub-commands
	if params.DevURL != "" {
		args = append(args, "--dev-url", params.DevURL)
	}
	if len(params.From) > 0 {
		args = append(args, "--from", listString(params.From))
	}
	if len(params.To) > 0 {
		args = append(args, "--to", listString(params.To))
	}
	if params.File != "" {
		args = append(args, "--file", params.File)
	} else {
		return "", &InvalidParamsError{"schema plan push", "missing required flag --file"}
	}
	if params.Repo != "" {
		args = append(args, "--repo", params.Repo)
	}
	if params.Pending {
		args = append(args, "--pending")
	} else {
		args = append(args, "--auto-approve")
	}
	return stringVal(c.runCommand(ctx, args))
}

// SchemaPlanPush runs the `schema plan pull` command.
func (c *Client) SchemaPlanPull(ctx context.Context, params *SchemaPlanPullParams) (string, error) {
	args := []string{"schema", "plan", "pull"}
	// Global flags
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.Vars != nil {
		args = append(args, params.Vars.AsArgs()...)
	}
	// Flags of the 'schema plan pull' sub-commands
	if params.URL != "" {
		args = append(args, "--url", params.URL)
	} else {
		return "", &InvalidParamsError{"schema plan pull", "missing required flag --url"}
	}
	return stringVal(c.runCommand(ctx, args))
}

// SchemaPlanLint runs the `schema plan lint` command.
func (c *Client) SchemaPlanLint(ctx context.Context, params *SchemaPlanLintParams) (*SchemaPlan, error) {
	args := []string{"schema", "plan", "lint", "--format", "{{ json . }}"}
	// Global flags
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.Vars != nil {
		args = append(args, params.Vars.AsArgs()...)
	}
	// Hidden flags
	if params.Context != nil {
		buf, err := json.Marshal(params.Context)
		if err != nil {
			return nil, err
		}
		args = append(args, "--context", string(buf))
	}
	// Flags of the 'schema plan lint' sub-commands
	if params.DevURL != "" {
		args = append(args, "--dev-url", params.DevURL)
	}
	if len(params.From) > 0 {
		args = append(args, "--from", listString(params.From))
	}
	if len(params.To) > 0 {
		args = append(args, "--to", listString(params.To))
	}
	if params.File != "" {
		args = append(args, "--file", params.File)
	} else {
		return nil, &InvalidParamsError{"schema plan lint", "missing required flag --file"}
	}
	if params.Repo != "" {
		args = append(args, "--repo", params.Repo)
	}
	args = append(args, "--auto-approve")
	// NOTE: This command only support one result.
	return firstResult(jsonDecode[SchemaPlan](c.runCommand(ctx, args)))
}

// SchemaPlanValidate runs the `schema plan validate` command.
func (c *Client) SchemaPlanValidate(ctx context.Context, params *SchemaPlanValidateParams) error {
	args := []string{"schema", "plan", "validate"}
	// Global flags
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.Vars != nil {
		args = append(args, params.Vars.AsArgs()...)
	}
	// Hidden flags
	if params.Context != nil {
		buf, err := json.Marshal(params.Context)
		if err != nil {
			return err
		}
		args = append(args, "--context", string(buf))
	}
	// Flags of the 'schema plan validate' sub-commands
	if params.DevURL != "" {
		args = append(args, "--dev-url", params.DevURL)
	}
	if len(params.From) > 0 {
		args = append(args, "--from", listString(params.From))
	}
	if len(params.To) > 0 {
		args = append(args, "--to", listString(params.To))
	}
	if params.File != "" {
		args = append(args, "--file", params.File)
	} else {
		return &InvalidParamsError{"schema plan validate", "missing required flag --file"}
	}
	if params.Name != "" {
		args = append(args, "--name", params.Name)
	}
	if params.Repo != "" {
		args = append(args, "--repo", params.Repo)
	}
	args = append(args, "--auto-approve")
	_, err := stringVal(c.runCommand(ctx, args))
	return err
}

// SchemaPlanApprove runs the `schema plan approve` command.
func (c *Client) SchemaPlanApprove(ctx context.Context, params *SchemaPlanApproveParams) (*SchemaPlanApprove, error) {
	args := []string{"schema", "plan", "approve", "--format", "{{ json . }}"}
	// Global flags
	if params.ConfigURL != "" {
		args = append(args, "--config", params.ConfigURL)
	}
	if params.Env != "" {
		args = append(args, "--env", params.Env)
	}
	if params.Vars != nil {
		args = append(args, params.Vars.AsArgs()...)
	}
	// Flags of the 'schema plan approve' sub-commands
	if params.URL != "" {
		args = append(args, "--url", params.URL)
	} else {
		return nil, &InvalidParamsError{"schema plan approve", "missing required flag --url"}
	}
	// NOTE: This command only support one result.
	return firstResult(jsonDecode[SchemaPlanApprove](c.runCommand(ctx, args)))
}

// InvalidParamsError is an error type for invalid parameters.
type InvalidParamsError struct {
	cmd string
	msg string
}

// Error returns the error message.
func (e *InvalidParamsError) Error() string {
	return fmt.Sprintf("atlasexec: command %q has invalid parameters: %v", e.cmd, e.msg)
}
func newSchemaApplyError(r []*SchemaApply) error {
	return &SchemaApplyError{Result: r}
}

// Error implements the error interface.
func (e *SchemaApplyError) Error() string { return last(e.Result).Error }