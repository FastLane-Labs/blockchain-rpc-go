package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

// Newer HTTP transports return the cancellation cause instead of ctx.Err().
var errParallelSuperseded = fmt.Errorf("parallel RPC response selected: %w", context.Canceled)

func isParallelCancellation(ctx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) && ctx != nil && context.Cause(ctx) == errParallelSuperseded
}

// Preserve the original broadcast error text while exposing provider causes.
type providerErrors []error

func (errs providerErrors) Error() string {
	messages := make([]string, len(errs))
	for i, err := range errs {
		messages[i] = err.Error()
	}
	return strings.Join(messages, "; ")
}

func (errs providerErrors) Unwrap() []error { return errs }

// Socket deadlines can fire before the context timer runs. Normalize their
// errors at the end of the attempt, while its deadline is still available.
// Checking later in the coordinator could misclassify an earlier network error
// that was buffered while a higher-weight attempt was pending.
func parallelAttemptError(ctx context.Context, err error) error {
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var timeoutErr net.Error
	if !errors.As(err, &timeoutErr) || !timeoutErr.Timeout() {
		return err
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Now().Before(deadline) {
		return err
	}
	// Keep the transport cause available through errors.Is / errors.As.
	return fmt.Errorf("%w: %w", context.DeadlineExceeded, err)
}

// At the current weight, every provider answer, success or error, ends selection
// except a copy that timed out. Lower weights become eligible only after all
// higher-weight copies time out. The local limiter rejecting a wait that would
// exceed the attempt deadline has the same outcome for its copy. Caller
// cancellation is checked separately.
func isParallelTimeout(err error) bool {
	var waitErr rateLimitWaitError
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &waitErr)
}
