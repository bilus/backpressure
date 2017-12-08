package httputil

import (
	"context"
	"github.com/bilus/backpressure/colors"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

func ListenAndServeWithContext(ctx context.Context, addr string, handler http.Handler, wg *sync.WaitGroup) error {
	srv := &http.Server{Addr: addr, Handler: handler}

	if addr == "" {
		addr = ":http"
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := srv.Serve(tcpKeepAliveListener{listener.(*net.TCPListener)})
		if err != nil {
			log.Printf(colors.Red("HTTP server error: %v"), err)
			return
		}
		log.Println("Exiting server")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		listener.Close()
	}()

	return nil
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
