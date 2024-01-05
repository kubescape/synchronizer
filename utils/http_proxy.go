package utils

// Inspired from: https://gist.github.com/jim3ma/3750675f141669ac4702bc9deaf31c6b

import (
	"bufio"
	"context"
	"fmt"
	"golang.org/x/net/proxy"
	"net"
	"net/http"
	"net/url"
	"os"
)

type direct struct{}

// Direct is a direct proxy: one that makes network connections directly.
var Direct = direct{}

func (direct) Dial(network, addr string) (net.Conn, error) {
	return net.Dial(network, addr)
}

// httpProxy is an HTTP/HTTPS connect proxy.
type httpProxy struct {
	host     string
	haveAuth bool
	username string
	password string
	forward  proxy.Dialer
}

func newHTTPProxy(uri *url.URL, forward proxy.Dialer) (proxy.Dialer, error) {
	s := new(httpProxy)
	s.host = uri.Host
	s.forward = forward
	if uri.User != nil {
		s.haveAuth = true
		s.username = uri.User.Username()
		s.password, _ = uri.User.Password()
	}

	return s, nil
}

func (s *httpProxy) Dial(_, addr string) (net.Conn, error) {
	// Dial and create the https client connection.
	c, err := s.forward.Dial("tcp", s.host)
	if err != nil {
		return nil, fmt.Errorf("dial proxy: %w", err)
	}

	// HACK. http.ReadRequest also does this.
	reqURL, err := url.Parse("http://" + addr)
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("parse addr: %w", err)
	}
	reqURL.Scheme = ""

	req, err := http.NewRequest("CONNECT", reqURL.String(), nil)
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("create CONNECT request: %w", err)
	}
	req.Close = false
	if s.haveAuth {
		req.SetBasicAuth(s.username, s.password)
	}

	err = req.Write(c)
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("write CONNECT request: %w", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(c), req)
	if err != nil {
		_ = resp.Body.Close()
		_ = c.Close()
		return nil, fmt.Errorf("read CONNECT response: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		_ = c.Close()
		return nil, fmt.Errorf("CONNECT response: %s", resp.Status)
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
			if err == nil {
				proxyURI = uri
			}
		}
	}
	// default proxy net dial
	proxyNetDial := proxy.FromEnvironment()
	// if custom URI is defined, generate proxy net dial from that
	if proxyURI != nil {
		dial, err := proxy.FromURL(proxyURI, Direct)
		if err == nil {
			proxyNetDial = dial
		}
	}
	// return dialer as a net.DialContext function
	return func(ctx context.Context, a, b string) (net.Conn, error) {
		return proxyNetDial.Dial(a, b)
	}
}
