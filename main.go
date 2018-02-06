// minimal working example of grpc deadlock on concurrent single client connection
package main

//go:generate protoc --go_out=plugins=grpc:. service.proto

import (
	"context"
	"log"
	"math/rand"
	"net"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
)

type service struct{}

func (m *service) Method(_ context.Context, _ *Request) (*Response, error) {
	// emulate some work
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
	return &Response{}, nil
}

type msg struct {
	n       int
	payload []byte
}

func queue(n int, ch chan<- msg) {
	t := time.NewTimer(time.Minute)
	defer t.Stop()
	// variable oversized message payload to procuce some errors
	// seems to tigger the error faster (default limit: 4M)
	p := make([]byte, rand.Intn(6*1024*1024))
	select {
	case ch <- msg{n: n, payload: p}:
	case <-t.C:
		// stalled, dump core & die
		syscall.Kill(syscall.Getpid(), syscall.SIGABRT)
	}
}

func client(l net.Listener) {
	conn, err := grpc.Dial(l.Addr().String(), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	c := NewServiceClient(conn)
	ch := make(chan msg)
	wg := sync.WaitGroup{}
	// spawn workers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(c, ch)
		}()
	}
	// feed workers
	for i := 0; i < 10000; i++ {
		queue(i, ch)
	}
	close(ch)
	wg.Wait()
	log.Println("done")
}

func worker(c ServiceClient, ch <-chan msg) {
	for v := range ch {
		_, err := c.Method(context.Background(), &Request{Payload: v.payload})
		if err != nil {
			log.Println(v.n, err)
			continue
		}
		log.Println(v.n, "...")
	}
}

func server(l net.Listener) {
	s := grpc.NewServer()
	RegisterServiceServer(s, &service{})
	log.Fatal(s.Serve(l))
}

func main() {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	go server(l)
	client(l)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
