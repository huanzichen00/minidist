package main

import (
	"context"
	"flag"
	"log"
	"minidist/internal/node"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	addr := flag.String(
		"addr",
		"127.0.0.1:8081",
		"node address",
	)

	cluster := flag.String(
		"cluster",
		"127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083",
		"cluster nodes",
	)

	flag.Parse()

	nodes := strings.Split(*cluster, ",")

	n := node.New(*addr, nodes)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	n.RunBackground(ctx)

	log.Printf("node listening on %s", *addr)

	server := &http.Server{
		Addr:    *addr,
		Handler: n.Handler(),
	}

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		_ = server.Shutdown(shutdownCtx)
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
