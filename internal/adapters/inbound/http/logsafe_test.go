package http

import (
	"strings"
	"testing"
)

func TestLogSafeStripsLineBreaks(t *testing.T) {
	for in, want := range map[string]string{
		"/exceptions":  "/exceptions",
		"/a\r\nb":      "/ab",
		"injected\n":   "injected",
		"\r":           "",
		"keep \t tabs": "keep \t tabs",
	} {
		if got := logSafe(in); got != want {
			t.Errorf("logSafe(%q) = %q, want %q", in, got, want)
		}
	}
	if strings.ContainsAny(logSafe("x\r\ny"), "\r\n") {
		t.Fatal("logSafe left a line break behind")
	}
}
