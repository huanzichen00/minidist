package main

import (
	"flag"
	"log"
	"minidist/internal/node"
	"net/http"
	"strings"
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

	log.Printf("node listening on %s", *addr)

	if err := http.ListenAndServe(*addr, n.Handler()); err != nil {
		log.Fatal(err)
	}
}
