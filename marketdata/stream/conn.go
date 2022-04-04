package stream

import (
	"context"
	"errors"
	"net/url"
	"time"
)

// conn represents a websocket connection between the server and the client
type conn interface {
	// close closes the websocket connection
	close() error
	// ping sends a ping to the the server
	ping(ctx context.Context) error
	// readMessage blocks until it reads a single message
	readMessage(ctx context.Context) (data []byte, err error)
	// writeMessage writes a single message
	writeMessage(ctx context.Context, data []byte) error
}

const (
	writeWait  = 5 * time.Second  // Time allowed to write a message to the peer
	pongWait   = 5 * time.Second  // Time allowed to read the next pong message from the peer
	pingPeriod = 10 * time.Second // Send pings to peer with this period

	maxFrameSize = 1024 * 1024
)

func GetConnectionCreator(wslib string) (func(context.Context, url.URL) (conn, error), error) {
	switch wslib {
	case "gobwas":
		return Gobwas, nil
	case "gorilla":
		return Gorilla, nil
	case "nhooyr":
		return Nhooyr, nil
	}
	return nil, errors.New("invalid websocket library")
}
