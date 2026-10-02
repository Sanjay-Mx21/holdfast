// Package grpcx is HoldFast's gRPC toolkit for service-to-service calls.
//
// Servers get tracing (otelgrpc), metrics, logging, panic recovery, a
// required deadline and service-token authentication with a per-method
// allowlist of callers. Clients get tracing, service tokens, a default
// deadline and bounded retries of UNAVAILABLE, which is safe because every
// HoldFast RPC is idempotent.
//
// Errors are gRPC statuses with a google.rpc.ErrorInfo detail whose reason
// is a stable, machine-readable code (Error, Reason).
//
// Traffic is plaintext on the internal network; encryption in transit (TLS or
// a service mesh) is a deployment concern for Phase 6.
package grpcx

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorDomain is the ErrorInfo domain of every HoldFast error.
const ErrorDomain = "holdfast"

// Error returns a status error with code and an ErrorInfo carrying reason.
func Error(code codes.Code, reason, msg string) error {
	st := status.New(code, msg)
	if withInfo, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: ErrorDomain}); err == nil {
		st = withInfo
	}
	return st.Err()
}

// Reason returns the ErrorInfo reason of a status error, or "".
func Reason(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetDomain() == ErrorDomain {
			return info.GetReason()
		}
	}
	return ""
}

// Code returns the status code of err (codes.Unknown for non-status errors,
// codes.OK for nil).
func Code(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	var se interface{ GRPCStatus() *status.Status }
	if errors.As(err, &se) {
		return se.GRPCStatus().Code()
	}
	return codes.Unknown
}
