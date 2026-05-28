package buildinfo

import (
	"fmt"
	"runtime/debug"
)

func String() string {
	revision := "unknown"
	modified := "unknown"
	buildTime := "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				modified = setting.Value
			case "vcs.time":
				buildTime = setting.Value
			}
		}
	}
	return fmt.Sprintf("revision=%s modified=%s time=%s", revision, modified, buildTime)
}
