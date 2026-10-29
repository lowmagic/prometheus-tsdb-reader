package query

import "testing"

func TestRelease(t *testing.T) {
	tests := []struct{ module, want string }{
		{"v0.315.1", "3.15.1"},
		{"v0.300.0", "3.0.0"},
		{"v0.305.2", "3.5.2"},
		{"v0.55.1", "2.55.1"},
		{"v0.315.2-0.20261001120000-0123456789ab", "v0.315.2-0.20261001120000-0123456789ab"},
		{"(devel)", "(devel)"},
	}
	for _, tt := range tests {
		if got := release(tt.module); got != tt.want {
			t.Errorf("release(%q) = %q, want %q", tt.module, got, tt.want)
		}
	}
}
