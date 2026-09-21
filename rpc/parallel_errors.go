package rpc

import (
	"context"
	"crypto/tls"
	"encoding/json"
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
		errors.Is(err, net.ErrClosed) || errors.Is(err, rpc.ErrClientQuit) || errors.Is(err, rpc.ErrNoResult) ||
		errors.Is(err, websocket.ErrCloseSent) || errors.Is(err, websocket.ErrBadHandshake) {
		return true
	}
	// Workers decode into raw JSON, so decoder errors mean the provider answered
	// with malformed or misshapen JSON-RPC, not that the caller's receiver is
	// incompatible. Such providers must not win over a healthy one.
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &syntaxErr) || errors.As(err, &typeErr) {
		return true
	}
	// Geth's two internal connection sentinels are unexported and untyped.
	// RPC errors were excluded above, so node error messages never reach this.
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		switch cause.Error() {
		case "client reconnected", "connection lost":
			return true
		}
	}
	var waitErr rateLimitWaitError
	var networkErr net.Error
	var httpErr rpc.HTTPError
	var closeErr *websocket.CloseError
	var certificateErr *tls.CertificateVerificationError
	var tlsErr tls.RecordHeaderError
	return errors.As(err, &waitErr) || errors.As(err, &networkErr) || errors.As(err, &httpErr) ||
		errors.As(err, &closeErr) || errors.As(err, &certificateErr) || errors.As(err, &tlsErr)
}
