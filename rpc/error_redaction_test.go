package rpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
)

const testSecretURL = "https://user:password@rpc.example:8545/v2/path-secret?key=query-secret#fragment-secret"

func assertRedacted(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, format := range []string{"%s", "%v", "%+v", "%#v", "%q"} {
		text := fmt.Sprintf(format, err)
		if !strings.Contains(text, redactedURL) {
			t.Errorf("%s missing redaction marker: %s", format, text)
		}
		for _, secret := range []string{"password", "rpc.example", "path-secret", "query-secret", "fragment-secret"} {
			if strings.Contains(text, secret) {
				t.Errorf("%s leaked %q: %s", format, secret, text)
			}
		}
	}
}

func TestRedactErrorURLs(t *testing.T) {
	for _, message := range []string{
		"upstream " + testSecretURL + " failed",
		fmt.Sprintf("Post %q: connection refused", testSecretURL),
		"first " + testSecretURL + " failed; second wss://rpc.example/path-secret failed",
		`provider {"url":"https:\/\/rpc.example\/path-secret?key=query-secret","message":"failed"}`,
		`provider "https://rpc.example/path-secret'query-secret" failed`,
		`provider 'https://rpc.example/path-secret"query-secret' failed`,
		`provider 'https://rpc.example/path-secret'query-secret' failed`,
		`provider https://rpc.example/path-secret'query-secret failed`,
		`provider "https://rpc.example/path-secret\"query-secret" failed`,
		"WSS://rpc.example/path-secret failed; HTTPS://rpc.example/query-secret failed",
		"ws://[::1]:8546/path-secret?key=query-secret failed",
		"unsupported transport custom+rpc://rpc.example/path-secret",
	} {
		t.Run(message, func(t *testing.T) {
			original := errors.New(message)
			err := redactErrorURLs(original)
			assertRedacted(t, err)
			if !errors.Is(err, original) {
				t.Fatal("original error identity was lost")
			}
			if redactErrorURLs(err) != err {
				t.Fatal("redaction should be idempotent")
			}
		})
	}
}

func TestRedactionPreservesUnchangedErrors(t *testing.T) {
	for _, err := range []error{nil, context.Canceled, context.DeadlineExceeded, errors.New("execution reverted: 0x12345678")} {
		if got := redactErrorURLs(err); got != err {
			t.Fatalf("changed error without URLs: %v", err)
		}
	}
	err := redactErrorURLs(fmt.Errorf("provider-1: request to %s failed: timeout", testSecretURL))
	if want := "provider-1: request to " + redactedURL + " failed: timeout"; err.Error() != want {
		t.Fatalf("lost useful error context: got %q, want %q", err.Error(), want)
	}
}

func TestRedactionPreservesErrorChain(t *testing.T) {
	for _, endpoint := range []string{testSecretURL, `rpc.example/path-secret`, `https://rpc.example/path-secret'" query-secret`} {
		original := &url.Error{Op: "Post", URL: endpoint, Err: context.DeadlineExceeded}
		joined := errors.Join(fmt.Errorf("first provider: %w", original), fmt.Errorf("second provider %s: %w", testSecretURL, context.Canceled))
		err := redactErrorURLs(joined)
		assertRedacted(t, err)
		var urlErr *url.Error
		if !errors.As(err, &urlErr) || urlErr != original || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, context.Canceled) {
			t.Fatalf("error inspection changed: %v", err)
		}
		if original.URL != endpoint {
			t.Fatal("redaction mutated the original error")
		}
	}
}

type testRPCURLError struct{}

func (testRPCURLError) Error() string  { return "execution reverted by " + testSecretURL }
func (testRPCURLError) ErrorCode() int { return -32000 }
func (testRPCURLError) ErrorData() any { return "0x12345678" }

func TestRPCErrorRedaction(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, count := range []int{1, 2} {
			t.Run(fmt.Sprintf("parallel=%v/providers=%d", parallel, count), func(t *testing.T) {
				handlers := make([]testHandler, count)
				for i := range handlers {
					handlers[i] = func(context.Context, testRequest) (any, error) { return nil, testRPCURLError{} }
				}
				c := testMultiClient(t, handlers...)
				c.parallelCalls = parallel
				// Provider labels must not reintroduce a URL during aggregation.
				c.allClients[0].rpcClient.id = testSecretURL
				err := c.Call(nil, "eth_call")
				assertRedacted(t, err)
				var rpcErr gethrpc.Error
				var dataErr gethrpc.DataError
				if !errors.As(err, &rpcErr) || rpcErr.ErrorCode() != -32000 || !errors.As(err, &dataErr) || dataErr.ErrorData() != "0x12345678" || isErrorRetryable(err) {
					t.Fatalf("RPC error semantics changed: %v", err)
				}
				var result string
				batch := []gethrpc.BatchElem{{Method: "eth_call", Result: &result}}
				if err := c.BatchCall(batch); err != nil {
					t.Fatalf("RPC error should be per-element: %v", err)
				}
				assertRedacted(t, batch[0].Error)
				if !errors.As(batch[0].Error, &dataErr) || dataErr.ErrorData() != "0x12345678" {
					t.Fatal("batch error data was lost")
				}
				assertRedacted(t, c.Call(nil, "eth_sendRawTransaction"))
			})
		}
	}
}

func TestTransportErrorRedaction(t *testing.T) {
	ctx := testContext(t)
	client, err := gethrpc.DialOptions(ctx, testSecretURL, gethrpc.WithHTTPClient(&http.Client{Transport: failingTransport{}}))
	if err != nil {
		t.Fatal(err)
	}
	c := &RpcClient{c: client}
	t.Cleanup(c.Close)
	var result string
	batch := []gethrpc.BatchElem{{Method: "eth_call", Result: &result}}
	for name, call := range map[string]func() error{
		"Call":             func() error { return c.Call(&result, "eth_call") },
		"CallContext":      func() error { return c.CallContext(ctx, &result, "eth_call") },
		"BatchCall":        func() error { return c.BatchCall(batch) },
		"BatchCallContext": func() error { return c.BatchCallContext(ctx, batch) },
		"Notify":           func() error { return c.Notify(ctx, "test_notify") },
		"SupportedModules": func() error { _, err := c.SupportedModules(); return err },
	} {
		t.Run(name, func(t *testing.T) {
			err := call()
			assertRedacted(t, err)
			var urlErr *url.Error
			if !errors.As(err, &urlErr) || !isErrorRetryable(err) {
				t.Fatalf("transport error semantics changed: %v", err)
			}
		})
	}
}

func TestDialErrorRedaction(t *testing.T) {
	for _, endpoint := range []string{
		"https://user:password@rpc.example/path-secret/%zz?key=query-secret",
		"wss://user:password@rpc.example/path-secret/%zz?key=query-secret",
	} {
		_, err := DialContext(testContext(t), &RpcClientConfig{RpcClientData: RpcClientData{Url: endpoint}})
		assertRedacted(t, err)
	}
}

func TestHTTPBodyErrorRedaction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprintf(w, `{"upstream":%q,"message":"failed"}`, testSecretURL)
	}))
	defer server.Close()
	c, err := Dial(&RpcClientConfig{RpcClientData: RpcClientData{Url: server.URL}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = c.Call(nil, "eth_call")
	assertRedacted(t, err)
	var httpErr gethrpc.HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusBadGateway {
		t.Fatalf("HTTP error status was lost: %v", err)
	}
}

type redactionSubscriptionAPI struct{}

func (*redactionSubscriptionAPI) Heads(context.Context) (*gethrpc.Subscription, error) {
	return nil, testRPCURLError{}
}

func TestSubscriptionErrorRedaction(t *testing.T) {
	server := gethrpc.NewServer()
	defer server.Stop()
	for _, namespace := range []string{"eth", "shh"} {
		if err := server.RegisterName(namespace, new(redactionSubscriptionAPI)); err != nil {
			t.Fatal(err)
		}
	}
	httpServer := httptest.NewServer(server.WebsocketHandler([]string{"*"}))
	defer httpServer.Close()
	ctx := testContext(t)
	c, err := DialContext(ctx, &RpcClientConfig{RpcClientData: RpcClientData{Url: "ws" + strings.TrimPrefix(httpServer.URL, "http")}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ch := make(chan string)
	_, err = c.EthSubscribe(ctx, ch, "heads")
	assertRedacted(t, err)
	_, err = c.ShhSubscribe(ctx, ch, "heads")
	assertRedacted(t, err)
	_, err = c.Subscribe(ctx, "eth", ch, "heads")
	assertRedacted(t, err)
}

type urlFailingCodec struct{}

func (*urlFailingCodec) MarshalJSON() ([]byte, error) { return nil, testRPCURLError{} }
func (*urlFailingCodec) UnmarshalJSON([]byte) error   { return testRPCURLError{} }

func TestParallelCodecErrorRedaction(t *testing.T) {
	handler := func(context.Context, testRequest) (any, error) { return "ok", nil }
	c := testMultiClient(t, handler, handler)
	codec := new(urlFailingCodec)
	assertRedacted(t, c.Call(codec, "eth_call"))
	assertRedacted(t, c.Call(nil, "eth_call", codec))
	batch := []gethrpc.BatchElem{{Method: "eth_call", Result: codec}}
	if err := c.BatchCall(batch); err != nil {
		t.Fatal(err)
	}
	assertRedacted(t, batch[0].Error)
	batch = []gethrpc.BatchElem{{Method: "eth_call", Args: []any{codec}, Result: codec}}
	assertRedacted(t, c.BatchCall(batch))
}

func BenchmarkErrorURLRedaction(b *testing.B) {
	for name, err := range map[string]error{
		"success":   nil,
		"unchanged": errors.New("execution reverted"),
		"transport": &url.Error{Op: "Post", URL: testSecretURL, Err: context.DeadlineExceeded},
		"rpc":       testRPCURLError{},
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				redactErrorURLs(err)
			}
		})
	}
}
