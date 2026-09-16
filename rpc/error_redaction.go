package rpc

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const redactedURL = "[REDACTED_URL]"

// Include JSON-escaped slashes and quoted URLs (which may contain spaces or
// apostrophes in malformed endpoints). Redact the entire URL, not only userinfo:
// provider credentials commonly live in paths or query parameters.
const errorURLStart = `[a-zA-Z][a-zA-Z0-9+.-]*:(?://|\\/\\/)`

var errorURLs = regexp.MustCompile(`"` + errorURLStart + `(?:\\.|[^"\\])*"|'` + errorURLStart + `[^']*'[^\s<>]*|` + errorURLStart + `(?:\\.|[^\s<>\\])*`)

// Keep the nil path inlineable: successful calls do not format or scan errors.
func redactErrorURLs(err error) error {
	if err == nil {
		return nil
	}
	return redactNonNilErrorURLs(err)
}

func redactNonNilErrorURLs(err error) error {
	if _, ok := err.(*urlRedactedError); ok {
		return err
	}
	message := err.Error()
	redacted := redactURLErrorFields(message, err)
	if strings.Contains(redacted, "://") || strings.Contains(redacted, `:\/\/`) {
		redacted = errorURLs.ReplaceAllString(redacted, redactedURL)
	}
	if redacted == message {
		return err
	}
	return &urlRedactedError{message: redacted, cause: err}
}

// url.Error identifies a URL even when it is relative or too malformed for
// textual detection. Its Error method quotes the complete URL with strconv.Quote.
func redactURLErrorFields(message string, err error) string {
	switch e := err.(type) {
	case *urlRedactedError:
		return message
	case *url.Error:
		if e.URL != "" {
			message = strings.ReplaceAll(message, strconv.Quote(e.URL), `"`+redactedURL+`"`)
		}
	}
	switch e := err.(type) {
	case interface{ Unwrap() error }:
		return redactURLErrorFields(message, e.Unwrap())
	case interface{ Unwrap() []error }:
		for _, cause := range e.Unwrap() {
			message = redactURLErrorFields(message, cause)
		}
	}
	return message
}

// Preserve the original chain for errors.Is/As, including retry classification
// and RPC revert data. Only displayed error text is redacted; explicitly
// unwrapping the error still exposes the original diagnostic information.
type urlRedactedError struct {
	message string
	cause   error
}

func (e *urlRedactedError) Error() string    { return e.message }
func (e *urlRedactedError) GoString() string { return e.message }
func (e *urlRedactedError) Unwrap() error    { return e.cause }
