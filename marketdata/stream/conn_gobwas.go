package stream

import (
	"context"
	"crypto/tls"
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
	compiledPing  = ws.MustCompileFrame(ws.MaskFrameInPlace(ws.NewPingFrame(nil)))
	compiledClose = ws.MustCompileFrame(ws.MaskFrameInPlace(ws.NewCloseFrame(nil)))
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

	var tcpConn *net.TCPConn
	switch c := conn.(type) {
	case *net.TCPConn:
		tcpConn = c
	case *tls.Conn:
		tcpConn = c.NetConn().(*net.TCPConn)
	default:
		fmt.Println("error")
	}

	for _, size := range []int{16384, 8192, 4092, 2048, 1024, 512} {
		if err := tcpConn.SetReadBuffer(size * 1024); err == nil {
			fmt.Println("set SO_RCVBUF to", size*1024)
			break
		}
	}

	return &gobwasWebsocketConn{
		conn: conn,
	}, nil
}

func deadline(ctx context.Context) time.Time {
	dl, ok := ctx.Deadline()
	if !ok {
		dl = time.Time{}
	}
	return dl
}

// close closes the websocket connection
func (c *gobwasWebsocketConn) close() error {
	if _, err := c.conn.Write(compiledClose); err != nil {
		return err
	}
	return c.conn.Close()
}

// ping sends a ping to the client
func (c *gobwasWebsocketConn) ping(ctx context.Context) error {
	if err := c.conn.SetWriteDeadline(deadline(ctx)); err != nil {
		return err
	}
	_, err := c.conn.Write(compiledPing)
	return err
}

// readMessage blocks until it reads a single message
func (c *gobwasWebsocketConn) readMessage(ctx context.Context) (data []byte, err error) {
	if err := c.conn.SetReadDeadline(deadline(ctx)); err != nil {
		return nil, err
	}
	return wsutil.ReadServerBinary(c.conn)
}

// writeMessage writes a single message
func (c *gobwasWebsocketConn) writeMessage(ctx context.Context, data []byte) error {
	if err := c.conn.SetWriteDeadline(deadline(ctx)); err != nil {
		return err
	}
	return wsutil.WriteClientBinary(c.conn, data)
}
