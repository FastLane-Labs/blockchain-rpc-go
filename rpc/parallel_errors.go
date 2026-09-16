package rpc

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/gorilla/websocket"
)

// Remote RPC errors are answers, regardless of their code or message. Only
// transport failures and interrupted provider calls allow another copy to win.
// The caller's context is checked separately to stop the entire race.
func isParallelTransportError(err error) bool {
	if err == nil {
		return false
	}
	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, rpc.ErrClientQuit) || errors.Is(err, websocket.ErrCloseSent) {
		return true
	}
	var networkErr net.Error
	var httpErr rpc.HTTPError
	var closeErr *websocket.CloseError
	return errors.As(err, &networkErr) || errors.As(err, &httpErr) || errors.As(err, &closeErr)
}
