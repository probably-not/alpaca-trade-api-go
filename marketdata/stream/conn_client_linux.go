//go:build linux
// +build linux

package stream

import (
	"log"
	"net"
	"net/http"
	"syscall"

	"golang.org/x/sys/unix"
)

func httpClient() *http.Client {
	dialer := &net.Dialer{
		Control: func(network, address string, conn syscall.RawConn) error {
			var operr error
			if err := conn.Control(func(fd uintptr) {
				operr = syscall.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.TCP_QUICKACK, 1)
			}); err != nil {
				return err
			}
			return operr
		},
	}
	log.Println("using TCP_QUICKACK")
	return &http.Client{
		Transport: &http.Transport{
			DialContext: dialer.DialContext,
		},
	}
}
