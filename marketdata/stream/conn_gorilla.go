package stream

import (
	"context"
	"errors"
	"net/url"
)

type gorillaWebsocketConn struct {
}

func newGorillaWebsocketConn(ctx context.Context, u url.URL) (conn, error) {
	return &gorillaWebsocketConn{}, errors.New("not implemented")
}

// close closes the websocket connection
func (c *gorillaWebsocketConn) close() error {
	return errors.New("not implemented")
}

// ping sends a ping to the client
func (c *gorillaWebsocketConn) ping(ctx context.Context) error {
	return errors.New("not implemented")
}

// readMessage blocks until it reads a single message
func (c *gorillaWebsocketConn) readMessage(ctx context.Context) (data []byte, err error) {
	return nil, errors.New("not implemented")
}

// writeMessage writes a single message
func (c *gorillaWebsocketConn) writeMessage(ctx context.Context, data []byte) error {
	return errors.New("not implemented")
}
