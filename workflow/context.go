package workflow

import (
	"go.temporal.io/sdk/internal"
)

// Context is a clone of context.Context with Done() returning Channel instead
// of native channel.
// A Context carries a deadline, a cancellation signal, and other values across
// API boundaries.
//
// Context's methods may be called by multiple goroutines simultaneously.
type Context = internal.Context

// ContextAware is an optional interface that can be implemented alongside
// DataConverter. This interface allows Temporal to pass Workflow/Activity
// contexts to the DataConverter so that it may tailor its behavior.
//
// Note that data converters may be called in non-context-aware situations to
// convert payloads that may not be customized per context. Data converter
// implementers should not expect or require contextual data be present.
type ContextAware = internal.ContextAware

// ErrCanceled is the error returned by Context.Err when the context is canceled.
var ErrCanceled = internal.ErrCanceled

// ErrDeadlineExceeded is the error returned by Context.Err when the context's
// deadline passes.
var ErrDeadlineExceeded = internal.ErrDeadlineExceeded

// CancelFunc tells an operation to abandon its work.
// A CancelFunc does not wait for the work to stop.
// After the first call, subsequent calls to a CancelFunc do nothing.
type CancelFunc = internal.CancelFunc

// WithCancel returns a copy of parent with a new Done channel. The returned
// context's Done channel is closed when the returned cancel function is called
// or when the parent context's Done channel is closed, whichever happens first.
//
// Canceling this context releases resources associated with it, so code should
// call cancel as soon as the operations running in this Context complete.
func WithCancel(parent Context) (ctx Context, cancel CancelFunc) {
	return internal.WithCancel(parent)
}

// WithValue returns a copy of parent in which the value associated with key is
// val.
//
// Use context Values only for request-scoped data that transits processes and
// APIs, not for passing optional parameters to functions.
func WithValue(parent Context, key any, val any) Context {
	return internal.WithValue(parent, key, val)
}

// NewDisconnectedContext returns a new context that won't propagate parent's cancellation to the new child context.
// One common use case is to do cleanup work after workflow is canceled.
//
//	err := workflow.ExecuteActivity(ctx, ActivityFoo).Get(ctx, &activityFooResult)
//	if err != nil && temporal.IsCanceledError(ctx.Err()) {
//	  // activity failed, and workflow context is canceled
//	  disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx);
//	  workflow.ExecuteActivity(disconnectedCtx, handleCancellationActivity).Get(disconnectedCtx, nil)
//	  return err // workflow return CanceledError
//	}
func NewDisconnectedContext(parent Context) (ctx Context, cancel CancelFunc) {
	return internal.NewDisconnectedContext(parent)
}

// TransferTypeConverter converts between a model type and its transfer type;
// see [TransferTypeConvertible] for usage.
// Implementations must be pure, threadsafe, and deterministic.
//
// Outside a workflow, data conversion will invoke ToTransfer/FromTransfer with
// the context provided by [ContextAware.WithContext] if one is available;
// otherwise it will use context.Background().
//
// Inside a workflow, data conversion will instead pass the workflow context
// provided by [ContextAware.WithWorkflowContext].
type TransferTypeConverter[Model, Transfer any] = internal.TransferTypeConverter[Model, Transfer]

// TransferTypeConvertible is a marker struct that associates Model with a
// [TransferTypeConverter].
//
// The first time the SDK sees a Model value with an embedded
// TransferTypeConvertible[Model, Transfer, C], the SDK creates a zero value
// of C and caches it. Model values will be encoded by first applying
// C.ToTransfer and then applying the data converter to the resulting
// Transfer value. To decode a Model value, the SDK gets a Transfer value
// from the data converter and applies C.FromTransfer to it.
//
// The SDK will panic during data conversion if it sees a value of type T that
// embeds TransferTypeConvertible[A,B,C] where A != T. Do not embed a struct
// that embeds TransferTypeConvertible inside another struct.
//
// NOTE: Experimental.
type TransferTypeConvertible[Model, Transfer any, C TransferTypeConverter[Model, Transfer]] = internal.TransferTypeConvertible[Model, Transfer, C]
