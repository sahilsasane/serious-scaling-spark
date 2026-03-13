package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type AddRequest struct {
	A int `json:"a"`
	B int `json:"b"`
}

type AddResponse struct {
	Sum int `json:"sum"`
}

func addHandler(ctx context.Context, payload json.RawMessage) (any, error) {
	var req AddRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return AddResponse{Sum: req.A + req.B}, nil
}

func startServer(addr string) {
	s := NewServer()
	s.Register("math.add", addHandler)
	go func() {
		if err := s.ListenAndServe(addr); err != nil {
			log.Fatalf("server %s failed: %v", addr, err)
		}
	}()
}

func main() {
	startServer(":9001")
	startServer(":9002")
	time.Sleep(150 * time.Millisecond)

	client := NewClient(
		[]string{"localhost:9001", "localhost:9002"},
		RetryPolicy{
			MaxRetries:  2,
			BaseBackoff: 50 * time.Millisecond,
			MaxBackoff:  300 * time.Millisecond,
			DialTimeout: 150 * time.Millisecond,
			CallTimeout: 400 * time.Millisecond,
		},
	)

	for i := 0; i < 5; i++ {
		var out AddResponse
		err := client.Call(context.Background(), "math.add", AddRequest{A: i, B: i + 1}, true, &out)
		if err != nil {
			fmt.Printf("call %d failed: %v\n", i, err)
			continue
		}
		fmt.Printf("call %d result: %d\n", i, out.Sum)
	}
}
