package atlasexec

import (
	"fmt"
	"strings"
	"time"

	"ariga.io/atlas/sql/sqlcheck"
	"ariga.io/atlas/sql/sqlclient"
)

type (
	// File wraps migrate.File to implement json.Marshaler.
	File struct {
		Name        string `json:"Name,omitempty"`
		Version     string `json:"Version,omitempty"`
		Description string `json:"Description,omitempty"`
	}
	// AppliedFile is part of a MigrateApply containing information about an applied file in a migration attempt.
	AppliedFile struct {
		File
		Start   time.Time
		End     time.Time
		Skipped int           // Amount of skipped SQL statements in a partially applied file.
		Applied []string      // SQL statements applied with success
		Checks  []*FileChecks // Assertion checks
		Error   *struct {
			SQL   string // SQL statement that failed.
			Error string // Error returned by the database.
		}
	}
	// RevertedFile is part of a MigrateDown containing information about a reverted file in a downgrade attempt.
	RevertedFile struct {
		File
		Start   time.Time
		End     time.Time
		Skipped int      // Amount of skipped SQL statements in a partially applied file.
		Applied []string // SQL statements applied with success
		Scope   string   // Scope of the revert. e.g., statement, versions, etc.
		Error   *struct {
			SQL   string // SQL statement that failed.
			Error string // Error returned by the database.
		}
	}
	// MigrateApply contains a summary of a migration applying attempt on a database.
	MigrateApply struct {
		Env
		Pending []File         `json:"Pending,omitempty"` // Pending migration files
		Applied []*AppliedFile `json:"Applied,omitempty"` // Applied files
		Current string         `json:"Current,omitempty"` // Current migration version
		Target  string         `json:"Target,omitempty"`  // Target migration version
		Start   time.Time
		End     time.Time
		// Error is set even then, if it was not caused by a statement in a migration file,
		// but by Atlas, e.g. when committing or rolling back a transaction.
		Error string `json:"Error,omitempty"`
	}
	// MigrateDown contains a summary of a migration down attempt on a database.
	MigrateDown struct {
		Planned  []File          `json:"Planned,omitempty"`  // Planned migration files
		Reverted []*RevertedFile `json:"Reverted,omitempty"` // Reverted files
		Current  string          `json:"Current,omitempty"`  // Current migration version
		Target   string          `json:"Target,omitempty"`   // Target migration version
		Total    int             `json:"Total,omitempty"`    // Total number of migrations to revert
		Start    time.Time
		End      time.Time
		// URL and Status are set only when the migration is planned or executed in the cloud.
		URL    string `json:"URL,omitempty"`
		Status string `json:"Status,omitempty"`
		// Error is set even then, if it was not caused by a statement in a migration file,
		// but by Atlas, e.g. when committing or rolling back a transaction.
		Error string `json:"Error,omitempty"`
	}
	// MigrateStatus contains a summary of the migration status of a database.
	MigrateStatus struct {
		Available []File      `json:"Available,omitempty"` // Available migration files
		Pending   []File      `json:"Pending,omitempty"`   // Pending migration files
		Applied   []*Revision `json:"Applied,omitempty"`   // Applied migration files
		Current   string      `json:"Current,omitempty"`   // Current migration version
		Next      string      `json:"Next,omitempty"`      // Next migration version
		Count     int         `json:"Count,omitempty"`     // Count of applied statements of the last revision
		Total     int         `json:"Total,omitempty"`     // Total statements of the last migration
		Status    string      `json:"Status,omitempty"`    // Status of migration (OK, PENDING)
		Error     string      `json:"Error,omitempty"`     // Last Error that occurred
		SQL       string      `json:"SQL,omitempty"`       // SQL that caused the last Error
	}
	// A SummaryReport contains a summary of the analysis of all files.
	// It is used as an input to templates to report the CI results.
	SummaryReport struct {
		URL string `json:"URL,omitempty"` // URL of the report, if exists.

		// Env holds the environment information.
		Env struct {
			Driver string         `json:"Driver,omitempty"` // Driver name.
			URL    *sqlclient.URL `json:"URL,omitempty"`    // URL to dev database.
			Dir    string         `json:"Dir,omitempty"`    // Path to migration directory.
		}

		// Schema versions found by the runner.
		Schema struct {
			Current string `json:"Current,omitempty"` // Current schema.
			Desired string `json:"Desired,omitempty"` // Desired schema.
		}

		// Steps of the analysis. Added in verbose mode.
		Steps []*StepReport `json:"Steps,omitempty"`

		// Files reports. Non-empty in case there are findings.
		Files []*FileReport `json:"Files,omitempty"`
	}
	// StepReport contains a summary of the analysis of a single step.
	StepReport struct {
		Name   string      `json:"Name,omitempty"`   // Step name.
		Text   string      `json:"Text,omitempty"`   // Step description.
		Error  string      `json:"Error,omitempty"`  // Error that cause the execution to halt.
		Result *FileReport `json:"Result,omitempty"` // Result of the step. For example, a diagnostic.
	}
	// FileReport contains a summary of the analysis of a single file.
	FileReport struct {
		Name    string            `json:"Name,omitempty"`    // Name of the file.
		Text    string            `json:"Text,omitempty"`    // Contents of the file.
		Reports []sqlcheck.Report `json:"Reports,omitempty"` // List of reports.
		Error   string            `json:"Error,omitempty"`   // File specific error.
	}

	// FileChecks represents a set of checks to run before applying a file.
	FileChecks struct {
		Name  string     `json:"Name,omitempty"`  // File/group name.
		Stmts []*Check   `json:"Stmts,omitempty"` // Checks statements executed.
		Error *StmtError `json:"Error,omitempty"` // Assertion error.
		Start time.Time  `json:"Start,omitempty"` // Start assertion time.
		End   time.Time  `json:"End,omitempty"`   // End assertion time.
	}
	// Check represents an assertion and its status.
	Check struct {
		Stmt  string  `json:"Stmt,omitempty"`  // Assertion statement.
		Error *string `json:"Error,omitempty"` // Assertion error, if any.
	}
	// StmtError groups a statement with its execution error.
	StmtError struct {
		Stmt string `json:"Stmt,omitempty"` // SQL statement that failed.
		Text string `json:"Text,omitempty"` // Error message as returned by the database.
	}
	// Env holds the environment information.
	Env struct {
		Driver string         `json:"Driver,omitempty"` // Driver name.
		URL    *sqlclient.URL `json:"URL,omitempty"`    // URL to dev database.
		Dir    string         `json:"Dir,omitempty"`    // Path to migration directory.
	}
	// Changes represents a list of changes that are pending or applied.
	Changes struct {
		Applied []string   `json:"Applied,omitempty"` // SQL changes applied with success
		Pending []string   `json:"Pending,omitempty"` // SQL changes that were not applied
		Error   *StmtError `json:"Error,omitempty"`   // Error that occurred during applying
	}
	// SchemaApply contains a summary of a 'schema apply' execution on a database.
	SchemaApply struct {
		Env
		Changes Changes `json:"Changes,omitempty"`
		// General error that occurred during execution.
		// e.g., when committing or rolling back a transaction.
		Error string `json:"Error,omitempty"`
	}
	// A Revision denotes an applied migration in a deployment. Used to track migration executions state of a database.
	Revision struct {
		Version         string        `json:"Version"`             // Version of the migration.
		Description     string        `json:"Description"`         // Description of this migration.
		Type            string        `json:"Type"`                // Type of the migration.
		Applied         int           `json:"Applied"`             // Applied amount of statements in the migration.
		Total           int           `json:"Total"`               // Total amount of statements in the migration.
		ExecutedAt      time.Time     `json:"ExecutedAt"`          // ExecutedAt is the starting point of execution.
		ExecutionTime   time.Duration `json:"ExecutionTime"`       // ExecutionTime of the migration.
		Error           string        `json:"Error,omitempty"`     // Error of the migration, if any occurred.
		ErrorStmt       string        `json:"ErrorStmt,omitempty"` // ErrorStmt is the statement that raised Error.
		OperatorVersion string        `json:"OperatorVersion"`     // OperatorVersion that executed this migration.
	}
	// Version contains the result of an 'atlas version' run.
	Version struct {
		Version string `json:"Version"`
		SHA     string `json:"SHA,omitempty"`
		Canary  bool   `json:"Canary,omitempty"`
	}
)

type (
	// MigrateApplyError is returned when an error occurred
	// during a migration applying attempt.
	MigrateApplyError struct {
		Result []*MigrateApply
	}
	// SchemaApplyError is returned when an error occurred
	// during a schema applying attempt.
	SchemaApplyError struct {
		Result []*SchemaApply
	}
)

// Summary of the migration attempt.
func (a *MigrateApply) Summary(ident string) string {
	var (
		passedC, failedC int
		passedS, failedS int
		passedF, failedF int
		lines            = make([]string, 0, 3)
	)
	for _, f := range a.Applied {
		// For each check file, count the
		// number of failed assertions.
		for _, cf := range f.Checks {
			for _, s := range cf.Stmts {
				if s.Error != nil {
					failedC++
				} else {
					passedC++
				}
			}
		}
		passedS += len(f.Applied)
		if f.Error != nil {
			failedF++
			// Last statement failed (not an assertion).
			if len(f.Checks) == 0 || f.Checks[len(f.Checks)-1].Error == nil {
				passedS--
				failedS++
			}
		} else {
			passedF++
		}
	}
	// Execution time.
	lines = append(lines, a.End.Sub(a.Start).String())
	// Executed files.
	switch {
	case passedF > 0 && failedF > 0:
		lines = append(lines, fmt.Sprintf("%d migration%s ok, %d with errors", passedF, plural(passedF), failedF))
	case passedF > 0:
		lines = append(lines, fmt.Sprintf("%d migration%s", passedF, plural(passedF)))
	case failedF > 0:
		lines = append(lines, fmt.Sprintf("%d migration%s with errors", failedF, plural(failedF)))
	}
	// Executed checks.
	switch {
	case passedC > 0 && failedC > 0:
		lines = append(lines, fmt.Sprintf("%d check%s ok, %d failure%s", passedC, plural(passedC), failedC, plural(failedC)))
	case passedC > 0:
		lines = append(lines, fmt.Sprintf("%d check%s", passedC, plural(passedC)))
	case failedC > 0:
		lines = append(lines, fmt.Sprintf("%d check error%s", failedC, plural(failedC)))
	}
	// Executed statements.
	switch {
	case passedS > 0 && failedS > 0:
		lines = append(lines, fmt.Sprintf("%d sql statement%s ok, %d with errors", passedS, plural(passedS), failedS))
	case passedS > 0:
		lines = append(lines, fmt.Sprintf("%d sql statement%s", passedS, plural(passedS)))
	case failedS > 0:
		lines = append(lines, fmt.Sprintf("%d sql statement%s with errors", failedS, plural(failedS)))
	}
	var b strings.Builder
	for i, l := range lines {
		b.WriteString("--")
		b.WriteByte(' ')
		b.WriteString(l)
		if i < len(lines)-1 {
			b.WriteByte('\n')
			b.WriteString(ident)
		}
	}
	return b.String()
}

func plural(n int) (s string) {
	if n > 1 {
		s += "s"
	}
	return
}

// Error implements the error interface.
func (e *MigrateApplyError) Error() string { return last(e.Result).Error }

// Error implements the error interface.
func (e *SchemaApplyError) Error() string { return last(e.Result).Error }

// DiagnosticsCount returns the total number of diagnostics in the report.
func (r *SummaryReport) DiagnosticsCount() int {
	var n int
	for _, f := range r.Files {
		for _, r := range f.Reports {
			n += len(r.Diagnostics)
		}
	}
	return n
}

func newMigrateApplyError(r []*MigrateApply) error {
	return &MigrateApplyError{Result: r}
}

func newSchemaApplyError(r []*SchemaApply) error {
	return &SchemaApplyError{Result: r}
}

func last[A ~[]E, E any](a A) (_ E) {
	if l := len(a); l > 0 {
		return a[l-1]
	}
	return
}
