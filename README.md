# Atlas SDK for Go

An SDK for building ariga/atlas providers in Go.

## Installation

```bash
go get -u ariga.io/atlas-go-sdk
```

## How to use

To use the SDK, you need to create a new client with your `migrations` folder and the `atlas` binary path.

```go
package main

import (
    ...
    "ariga.io/atlas-go-sdk/atlasexec"
)

func main() {
    // Create a new client
    client, err := atlasexec.NewClient("my-migration-folder", "my-atlas-cli-path")
    if err != nil {
        log.Fatalf("failed to create client: %v", err)
    }
}
```

## APIs

The SDK provides the following APIs:

- `Login`: Login to the [Atlas Cloud](https://atlasgo.cloud/)
- `Logout`: runs the "atlas logout" command.
- `MigratePush`: runs the "atlas migrate push" command.
- `MigrateLint`: runs the "atlas migrate lint" command.
- `MigrateApplySlice`: runs the "atlas migrate apply" command for multiple targets.
- `MigrateApply`: runs the "atlas migrate apply" command.
- `MigrateApplySlice`: runs the 'atlas migrate apply' command for multiple targets.
- `MigrateDown`: runs the "atlas migrate down" command.
- `MigrateStatus`: runs the "atlas migrate status" command.
- `SchemaApply`: runs the "atlas schema apply" command.
- `SchemaInspect`: runs the "atlas schema inspect" command.
- `SchemaDiff`: runs the "atlas schema diff" command.

Example with `MigrateApply` API:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"

    "ariga.io/atlas-go-sdk/atlasexec"
)

func main() {
    // Define the execution context, supplying a migration directory
    // and potentially an `atlas.hcl` configuration file using `atlasexec.WithHCL`.
    workdir, err := atlasexec.NewWorkingDir(
        atlasexec.WithMigrations(
            os.DirFS("./migrations"),
        ),
    )
    if err != nil {
        log.Fatalf("failed to load working directory: %v", err)
    }
    // atlasexec works on a temporary directory, so we need to close it
    defer workdir.Close()

    // Initialize the client.
    client, err := atlasexec.NewClient(workdir.Path(), "atlas")
    if err != nil {
        log.Fatalf("failed to initialize client: %v", err)
    }
    // Run `atlas migrate apply` on a SQLite database under /tmp.
    res, err := client.MigrateApply(context.Background(), &atlasexec.MigrateApplyParams{
        URL: "sqlite:///tmp/demo.db?_fk=1&cache=shared",
    })
    if err != nil {
        log.Fatalf("failed to apply migrations: %v", err)
    }
    fmt.Printf("Applied %d migrations\n", len(res.Applied))
}
```