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

	walPath := flag.String(
		"wal",
		"",
		"WAL file path",
	)

	flag.Parse()

	nodes := strings.Split(*cluster, ",")

	if *walPath == "" {
		*walPath = "minidist-" + strings.ReplaceAll(*addr, ":", "_") + ".wal"
	}

	n, err := node.New(*addr, nodes, *walPath)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := n.Close(); err != nil {
			log.Printf("close node: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n.RunBackground(ctx)

	log.Printf("node listening on %s", *addr)

	server := &http.Server{
		Addr:    *addr,
		Handler: n.Handler(),
	}

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case <-signals:
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)

	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}
}
