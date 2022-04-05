package stream

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

type gorillaWebsocketConn struct {
	conn *websocket.Conn
}

func newGorillaWebsocketConn(ctx context.Context, u url.URL) (conn, error) {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), http.Header{
		"Content-Type": []string{"application/msgpack"},
	})
	if err != nil {
		return nil, fmt.Errorf("websocket dial: %w", err)
	}

	conn.SetReadLimit(maxFrameSize)
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pingPeriod + pongWait))
	})

	return &gorillaWebsocketConn{
		conn: conn,
	}, nil
}

// close closes the websocket connection
func (c *gorillaWebsocketConn) close() error {
	return c.conn.Close()
}

// ping sends a ping to the client
func (c *gorillaWebsocketConn) ping(ctx context.Context) error {
	return c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait))
}

// readMessage blocks until it reads a single message
func (c *gorillaWebsocketConn) readMessage(ctx context.Context) (data []byte, err error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(pingPeriod + pongWait)); err != nil {
		return nil, err
	}
	_, p, err := c.conn.ReadMessage()
	return p, err
}

// writeMessage writes a single message
func (c *gorillaWebsocketConn) writeMessage(ctx context.Context, data []byte) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		return err
	}
	return c.conn.WriteMessage(websocket.BinaryMessage, data)
}
