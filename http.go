package main

import (
	"crypto/tls"
	"net"
	"net/http"

	log "github.com/sirupsen/logrus"
)

var httpClient *http.Client

// InitHTTPClient initializes new httpClient according to configuraion
func InitHTTPClient(HTTPTimeout Duration, SSLSkipVerify bool) {
	log.WithFields(log.Fields{
		"HTTPTimeout":   HTTPTimeout,
		"SSLSkipVerify": SSLSkipVerify,
	}).Debug("Initializing http client")

	timeout := HTTPTimeout.Duration
	dialTimeout := func(network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, addr, timeout)
	}
	httpTransport := &http.Transport{
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: SSLSkipVerify},
		Dial:                  dialTimeout,
		ResponseHeaderTimeout: timeout,
	}
	httpClient = &http.Client{Transport: httpTransport}
}
