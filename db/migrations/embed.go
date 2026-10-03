// Package migrations embeds every service schema's SQL migrations into the
// binaries, so the code that runs and the schema it expects ship together.
package migrations

import (
	"embed"
	"io/fs"
)

//go:embed auth/*.sql booking/*.sql payment/*.sql
var files embed.FS

// Schema is one service-owned PostgreSQL schema and its migrations.
type Schema struct {
	Name  string
	Files fs.FS
}

// All returns every schema in apply order. Each service owns exactly one
// schema; no service reads another service's tables.
func All() []Schema {
	return []Schema{
		{Name: "booking", Files: sub("booking")},
		{Name: "payment", Files: sub("payment")},
		{Name: "auth", Files: sub("auth")},
	}
}

func sub(dir string) fs.FS {
	s, err := fs.Sub(files, dir)
	if err != nil {
		panic(err) // unreachable: dir is embedded above
	}
	return s
}
