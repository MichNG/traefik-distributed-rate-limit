package redis

// Logging seam of this package.
//
// It is an interface with a *method* rather than an injected function value on
// purpose: yaegi 0.16.1, used by Traefik 3.5.x, crashes while compiling a call
// to a variadic *function-typed struct field*
// (interp.run.go, callBin: "index out of range" while building the call
// wrapper). Calls through an interface are compiled correctly by that version.

// Logger formats and logs one message.
type Logger interface {
	Logf(format string, args ...interface{})
}

// nopLogger drops every message; used when the caller passes a nil logger.
type nopLogger struct{}

func (nopLogger) Logf(string, ...interface{}) {}

// OrNop returns a usable logger.
func OrNop(logger Logger) Logger {
	if logger == nil {
		return nopLogger{}
	}

	return logger
}
