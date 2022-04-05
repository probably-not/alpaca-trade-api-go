package stream

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

type gobwasWebsocketConn struct {
	conn net.Conn
}

var (
	compiledPing = ws.MustCompileFrame(ws.MaskFrameInPlace(ws.NewPingFrame(nil)))
)

func newGobwasWebsocketConn(ctx context.Context, u url.URL) (conn, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	conn, _, _, err := ws.Dialer{
		Header: ws.HandshakeHeaderHTTP{
			"Content-Type": []string{"application/msgpack"},
		},
	}.Dial(ctxWithTimeout, u.String())
	if err != nil {
		return nil, fmt.Errorf("websocket dial: %w", err)
	}

	return &gobwasWebsocketConn{
		conn: conn,
	}, nil
}

// close closes the websocket connection
func (c *gobwasWebsocketConn) close() error {
	return c.conn.Close()
}

// ping sends a ping to the client
func (c *gobwasWebsocketConn) ping(ctx context.Context) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		return err
	}
	_, err := c.conn.Write(compiledPing)
	return err
}

// readMessage blocks until it reads a single message
func (c *gobwasWebsocketConn) readMessage(ctx context.Context) (data []byte, err error) {
	if err := c.conn.SetWriteDeadline(time.Now().Add(pingPeriod + pongWait)); err != nil {
		return nil, err
	}
	return wsutil.ReadServerBinary(c.conn)
}

// writeMessage writes a single message
func (c *gobwasWebsocketConn) writeMessage(ctx context.Context, data []byte) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		return err
	}
	return wsutil.WriteClientBinary(c.conn, data)
}
