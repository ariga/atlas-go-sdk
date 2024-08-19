package atlasexec

import (
	"context"
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

func newSchemaApplyError(r []*SchemaApply) error {
	return &SchemaApplyError{Result: r}
}

// Error implements the error interface.
func (e *SchemaApplyError) Error() string { return last(e.Result).Error }
