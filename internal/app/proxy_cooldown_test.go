package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

type cooldownRoundTripper func(*http.Request) (*http.Response, error)

func (f cooldownRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func resetProxyCooldowns(t *testing.T) {
	t.Helper()
	proxyCooldowns.Lock()
	old := proxyCooldowns.until
	proxyCooldowns.until = make(map[string]time.Time)
	proxyCooldowns.Unlock()
	t.Cleanup(func() {
		proxyCooldowns.Lock()
		proxyCooldowns.until = old
		proxyCooldowns.Unlock()
	})
}

func TestProxyPool_CooldownIsolatedAndStickyReassigned(t *testing.T) {
	for _, sticky := range []bool{false, true} {
		t.Run(map[bool]string{false: "round_robin", true: "sticky"}[sticky], func(t *testing.T) {
			resetProxyCooldowns(t)
			withStickyProxyEnv(t, []Socks5Proxy{{Addr: "a:1080"}, {Addr: "b:1080"}}, socks5RR, sticky, false)
			auth := UpstreamAuth{Mode: AuthRoutePublic}
			_, client := selectUpstreamTarget(auth, nil, nil, "session-a")
			transport := client.Transport.(*proxyPoolTransport)
			failed := transport.addr
			transport.base = cooldownRoundTripper(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"120"}}, Body: io.NopCloser(strings.NewReader("limited"))}, nil
			})
			req, _ := http.NewRequest("GET", "http://upstream.test", nil)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if d := proxyCooldownRemaining(failed); d < 119*time.Second || d > 120*time.Second {
				t.Fatalf("Retry-After not respected: %v", d)
			}
			for _, session := range []string{"session-a", "session-b", "session-c"} {
				_, next := selectUpstreamTarget(auth, nil, nil, session)
				if next.Transport.(*proxyPoolTransport).addr == failed {
					t.Fatal("selected cooling proxy")
				}
			}
			proxyCooldowns.Lock()
			proxyCooldowns.until[failed] = time.Now().Add(-time.Second)
			proxyCooldowns.Unlock()
			idx := 0
			if socks5Proxies[1].Addr == failed {
				idx = 1
			}
			if availableProxyIndex(socks5Proxies, idx) != idx {
				t.Fatal("expired proxy not restored")
			}
		})
	}
}

func TestProxyPool_AllCoolingCancellationAndEarliest(t *testing.T) {
	resetProxyCooldowns(t)
	proxyCooldowns.until["a"] = time.Now().Add(time.Minute)
	proxyCooldowns.until["b"] = time.Now().Add(2 * time.Minute)
	if availableProxyIndex([]Socks5Proxy{{Addr: "a"}, {Addr: "b"}}, 1) != 0 {
		t.Fatal("did not pick earliest recovery")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://upstream.test", nil)
	tr := &proxyPoolTransport{addr: "a", base: cooldownRoundTripper(func(*http.Request) (*http.Response, error) {
		t.Fatal("request sent while cooling")
		return nil, nil
	})}
	if _, err := tr.RoundTrip(req); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestProxyPool_DefaultCooldownAndDirectUnaffected(t *testing.T) {
	resetProxyCooldowns(t)
	withStickyProxyEnv(t, []Socks5Proxy{{Addr: "a"}}, socks5RR, true, true)
	tr := &proxyPoolTransport{addr: "a", base: cooldownRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 429, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	req, _ := http.NewRequest("GET", "http://upstream.test", nil)
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if d := proxyCooldownRemaining("a"); d < 59*time.Second || d > time.Minute {
		t.Fatalf("default cooldown = %v", d)
	}
	if getHTTPClientForTier(TierPaid) != httpClient {
		t.Fatal("paid direct changed")
	}
	if _, ok := buildProxyClient(Socks5Proxy{Addr: "a"}, false).Transport.(*proxyPoolTransport); ok {
		t.Fatal("fixed proxy has pool cooldown")
	}
}

func TestProxyPool_AllCoolingReturnsImmediately(t *testing.T) {
	for _, sticky := range []bool{false, true} {
		t.Run(map[bool]string{false: "round_robin", true: "sticky"}[sticky], func(t *testing.T) {
			installFakeOpenCodeClient(t, nil)
			resetProxyCooldowns(t)
			withStickyProxyEnv(t, []Socks5Proxy{{Addr: "a:1080"}, {Addr: "b:1080"}}, socks5RR, sticky, false)
			until := time.Now().Add(time.Minute)
			proxyCooldowns.until["a:1080"] = until
			proxyCooldowns.until["b:1080"] = until.Add(time.Minute)
			state := ocSessionState.Load()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			body, status, header, err := callOpenCodeEndpoint(ctx, "chat/completions", []byte(`{"model":"test-model","messages":[]}`), "test-model", UpstreamAuth{Mode: AuthRoutePublic})
			if err != nil {
				t.Fatalf("expected immediate cooldown response, got %v", err)
			}
			defer body.Close()
			payload, err := io.ReadAll(body)
			if err != nil || !json.Valid(payload) || !strings.Contains(string(payload), "proxy_pool_cooldown") {
				t.Fatalf("invalid cooldown response: %s, error %v", payload, err)
			}
			seconds, err := strconv.Atoi(header.Get("Retry-After"))
			if status != 429 || err != nil || seconds < 59 || seconds > 60 {
				t.Fatalf("status=%d Retry-After=%q", status, header.Get("Retry-After"))
			}
			if header.Get(proxyCooldownHeader) != "" {
				t.Fatal("internal marker leaked")
			}
			if ocSessionState.Load() != state || !proxyCooldowns.until["a:1080"].Equal(until) {
				t.Fatal("local cooldown response changed session or extended cooldown")
			}
		})
	}
}
