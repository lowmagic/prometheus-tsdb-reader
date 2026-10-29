package query

import (
	"fmt"
	"runtime/debug"
)

const prometheusModule = "github.com/prometheus/prometheus"

// PromQLVersion returns the Prometheus release whose engine the binary embeds, as in "3.15.1", read from the
// module version in the build info. It's false when the binary carries no build info for the module.
func PromQLVersion() (string, bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false
	}
	for _, m := range info.Deps {
		if m.Path != prometheusModule {
			continue
		}
		if m.Replace != nil {
			m = m.Replace
		}
		return release(m.Version), true
	}
	return "", false
}

// release maps a Prometheus module version to the release it was tagged from. The module is tagged at major
// version 0: release 3.MINOR.PATCH is v0.3MINOR.PATCH, with MINOR in two digits, and release 2.MINOR.PATCH is
// v0.MINOR.PATCH. A version outside that pattern, such as a pseudo-version, comes back as it is.
func release(module string) string {
	var minor, patch int
	_, err := fmt.Sscanf(module, "v0.%d.%d", &minor, &patch)
	if err != nil || fmt.Sprintf("v0.%d.%d", minor, patch) != module {
		return module
	}
	if minor >= 300 {
		return fmt.Sprintf("%d.%d.%d", minor/100, minor%100, patch)
	}
	return fmt.Sprintf("2.%d.%d", minor, patch)
}
