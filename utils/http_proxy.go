package utils

// Inspired from: https://gist.github.com/jim3ma/3750675f141669ac4702bc9deaf31c6b

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"golang.org/x/net/proxy"
)

type direct struct{}

// Direct is a direct proxy: one that makes network connections directly.
var Direct = direct{}

func (direct) Dial(network, addr string) (net.Conn, error) {
	return Direct.DialContext(context.Background(), network, addr)
}

func (direct) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, addr)
}

// httpProxy is an HTTP/HTTPS connect proxy.
type httpProxy struct {
	host     string
	haveAuth bool
	username string
	password string
	forward  proxy.ContextDialer
}

func newHTTPProxy(uri *url.URL, forward proxy.Dialer) (proxy.Dialer, error) {
	s := new(httpProxy)
	s.host = uri.Host
	contextForward, ok := forward.(proxy.ContextDialer)
	if !ok {
		return nil, fmt.Errorf("proxy forwarding dialer does not support cancellation")
	}
	s.forward = contextForward
	logger.L().Info("setting proxy", helpers.String("scheme", uri.Scheme), helpers.String("host", s.host))
	if uri.User != nil {
		s.haveAuth = true
		s.username = uri.User.Username()
		s.password, _ = uri.User.Password()
		logger.L().Info("setting basic auth for proxy", helpers.String("username", s.username), helpers.Int("len(password)", len(s.password)))
	}

	return s, nil
}

func (s *httpProxy) Dial(network, addr string) (net.Conn, error) {
	return s.DialContext(context.Background(), network, addr)
}

func (s *httpProxy) DialContext(ctx context.Context, _, addr string) (conn net.Conn, err error) {
	// Dial and create the https client connection.
	c, err := s.forward.DialContext(ctx, "tcp", s.host)
	if err != nil {
		return nil, fmt.Errorf("dial proxy: %w", err)
	}

	// Close in-flight handshake I/O on cancellation, then detach the callback
	// before handing a successful tunnel to the caller. Waiting for an already
	// running callback prevents a late close racing with connection handoff.
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = c.Close(); close(canceled) })
	defer func() {
		if !stop() {
			<-canceled
		}
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		if err != nil {
			_ = c.Close()
			conn = nil
		}
	}()
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.SetDeadline(deadline); err != nil {
			return nil, fmt.Errorf("set CONNECT deadline: %w", err)
		}
	}

	// HACK. http.ReadRequest also does this.
	reqURL, err := url.Parse("http://" + addr)
	if err != nil {
		return nil, fmt.Errorf("parse addr: %w", err)
	}
	reqURL.Scheme = ""

	req, err := http.NewRequestWithContext(ctx, "CONNECT", reqURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create CONNECT request: %w", err)
	}
	req.Close = false
	if s.haveAuth {
		credential := base64.StdEncoding.EncodeToString([]byte(s.username + ":" + s.password))
		req.Header.Set("Proxy-Authorization", "Basic "+credential)
	}

	err = req.Write(c)
	if err != nil {
		return nil, fmt.Errorf("write CONNECT request: %w", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(c), req)
	if err != nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return nil, fmt.Errorf("read CONNECT response: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("CONNECT response: %s", resp.Status)
	}

	if err := c.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("clear CONNECT deadline: %w", err)
	}
	return c, nil
}

func init() {
	proxy.RegisterDialerType("http", newHTTPProxy)
	proxy.RegisterDialerType("https", newHTTPProxy)
}

// GetDialer returns a net.DialContext function that uses a proxy if necessary.
// HTTP_PROXY and HTTPS_PROXY are used to determine the proxy to use.
// NO_PROXY is currently ignored.
func GetDialer() func(context.Context, string, string) (net.Conn, error) {
	// read proxies from HTTP env variables, HTTPS takes precedent
	var proxyURI *url.URL
	envVars := []string{"HTTP_PROXY", "HTTPS_PROXY"}
	for _, envVar := range envVars {
		if httpProxy := os.Getenv(envVar); httpProxy != "" {
			uri, err := url.Parse(httpProxy)
			if err != nil {
				logger.L().Fatal("failed to parse proxy URI", helpers.Error(err))
			}
			proxyURI = uri
		}
	}
	// default proxy net dial
	proxyNetDial := proxy.FromEnvironment()
	// if custom URI is defined, generate proxy net dial from that
	if proxyURI != nil {
		dial, err := proxy.FromURL(proxyURI, Direct)
		if err != nil {
			logger.L().Fatal("failed to create proxy dialer", helpers.Error(err))
		}
		proxyNetDial = dial
	}
	// return dialer as a net.DialContext function
	return func(ctx context.Context, a, b string) (net.Conn, error) {
		if d, ok := proxyNetDial.(proxy.ContextDialer); ok {
			return d.DialContext(ctx, a, b)
		}
		return nil, fmt.Errorf("proxy dialer does not support cancellation")
	}
}
