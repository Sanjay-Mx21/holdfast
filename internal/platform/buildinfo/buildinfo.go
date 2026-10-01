// Package buildinfo reports which binary is running: version, commit and build
// time. Values come from -ldflags in release builds and otherwise from the VCS
// stamp the Go toolchain embeds automatically.
package buildinfo

import (
	"runtime"
	"runtime/debug"
	"sync"
)

// Overridden at link time, for example:
//
//	go build -ldflags "-X github.com/Sanjay-Mx21/holdfast/internal/platform/buildinfo.Version=v0.1.0"
var (
	Version string
	Commit  string
	Date    string
)

// Info describes the running binary.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	Date      string `json:"date"`
	GoVersion string `json:"goVersion"`
	Modified  bool   `json:"modified"`
}

// Get returns build metadata. It is computed once per process.
var Get = sync.OnceValue(func() Info {
	info := Info{Version: "dev", Commit: "unknown", Date: "unknown", GoVersion: runtime.Version()}
	if bi, ok := debug.ReadBuildInfo(); ok {
		if v := bi.Main.Version; v != "" && v != "(devel)" {
			info.Version = v
		}
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				info.Commit = s.Value
			case "vcs.time":
				info.Date = s.Value
			case "vcs.modified":
				info.Modified = s.Value == "true"
			}
		}
	}
	if Version != "" {
		info.Version = Version
	}
	if Commit != "" {
		info.Commit = Commit
	}
	if Date != "" {
		info.Date = Date
	}
	return info
})
