package rpc

import (
	"context"
	"errors"
)

// At the current weight, every provider answer, success or error, ends selection
// except a copy that timed out. Lower weights become eligible only after all
// higher-weight copies time out. The local limiter rejecting a wait that would
// exceed the attempt deadline has the same outcome for its copy. Caller
// cancellation is checked separately.
func isParallelTimeout(err error) bool {
	var waitErr rateLimitWaitError
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &waitErr)
}
