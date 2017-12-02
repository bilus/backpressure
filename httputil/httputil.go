package httputil

import (
	"github.com/bilus/backpressure/colors"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

func ListenAndServeWithClose(addr string, handler http.Handler, wg *sync.WaitGroup) (io.Closer, error) {
	var (
		listener  net.Listener
		srvCloser io.Closer
		err       error
	)

	srv := &http.Server{Addr: addr, Handler: handler}

	if addr == "" {
		addr = ":http"
	}

	listener, err = net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := srv.Serve(tcpKeepAliveListener{listener.(*net.TCPListener)})
		if err != nil {
			log.Printf(colors.Red("HTTP server error: %v"), err)
			return
		}
	}()

	srvCloser = listener
	return srvCloser, nil
}

type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}
