//go:build !linux
// +build !linux

package stream

import "net/http"

func httpClient() *http.Client {
	return &http.Client{}
}
