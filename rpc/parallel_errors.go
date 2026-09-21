package rpc

import (
	"context"
	"errors"
)

// Every provider answer, success or error, ends the race, except a copy that
// timed out: another copy may still answer within the caller's deadline. The
// local limiter rejecting a wait that would exceed that deadline is the same
// outcome for its copy. Caller cancellation is checked separately.
func isParallelTimeout(err error) bool {
	var waitErr rateLimitWaitError
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &waitErr)
}
