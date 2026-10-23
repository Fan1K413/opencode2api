package app

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var proxyCooldowns = struct {
	sync.Mutex
	until map[string]time.Time
}{until: make(map[string]time.Time)}

func proxyCooldownRemaining(addr string) time.Duration {
	proxyCooldowns.Lock()
	defer proxyCooldowns.Unlock()
	return time.Until(proxyCooldowns.until[addr])
}

// availableProxyIndex preserves the caller's distribution, skipping cooled
// exits. If every exit is cooling, pick the one that recovers first.
func availableProxyIndex(proxies []Socks5Proxy, start int) int {
	best := start
	remaining := proxyCooldownRemaining(proxies[start].Addr)
	for offset := 0; offset < len(proxies); offset++ {
		i := (start + offset) % len(proxies)
		d := proxyCooldownRemaining(proxies[i].Addr)
		if d <= 0 {
			return i
		}
		if d < remaining {
			best, remaining = i, d
		}
	}
	return best
}

// The transport retains the actual exit identity even if routing configuration
// or sticky bindings change while a request is in flight.
type proxyPoolTransport struct {
	addr string
	base http.RoundTripper
}

const proxyCooldownHeader = "X-Opencode2api-Proxy-Cooldown"

func (t *proxyPoolTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := req.Context().Err(); err != nil {
		if req.Body != nil {
			req.Body.Close()
		}
		return nil, err
	}
	if d := proxyCooldownRemaining(t.addr); d > 0 {
		if req.Body != nil {
			req.Body.Close()
		}
		seconds := int64(d/time.Second) + 1
		body := fmt.Sprintf(`{"error":{"type":"rate_limit_error","code":"proxy_pool_cooldown","message":"Proxy pool temporarily unavailable: selected proxy is cooling down. Retry after %d seconds.","retry_after_seconds":%d}}`, seconds, seconds)
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header: http.Header{
				"Content-Type":      {"application/json"},
				"Retry-After":       {strconv.FormatInt(seconds, 10)},
				proxyCooldownHeader: {"true"},
			},
			Body:    io.NopCloser(strings.NewReader(body)),
			Request: req,
		}, nil
	}
	resp, err := t.base.RoundTrip(req)
	if resp != nil {
		// Only locally generated responses may carry this internal marker.
		resp.Header.Del(proxyCooldownHeader)
	}
	if err == nil && resp.StatusCode == http.StatusTooManyRequests {
		d := parseRetryAfter(resp.Header.Get("Retry-After"))
		if d <= 0 {
			d = time.Minute
		}
		until := time.Now().Add(d)
		proxyCooldowns.Lock()
		if until.After(proxyCooldowns.until[t.addr]) {
			proxyCooldowns.until[t.addr] = until
		}
		proxyCooldowns.Unlock()
	}
	return resp, err
}

func (t *proxyPoolTransport) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
