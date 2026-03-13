package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

type Handler func(ctx context.Context, payload json.RawMessage) (any, error)

type Server struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewServer() *Server {
	return &Server{handlers: make(map[string]Handler)}
}

func (s *Server) Register(method string, handler Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[method] = handler
}

func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	fmt.Printf("rpc server listening on %s\n", addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	req, err := decodeRequest(conn)
	if err != nil {
		_ = encodeResponse(conn, Response{Success: false, Error: err.Error()})
		return
	}

	s.mu.RLock()
	h, ok := s.handlers[req.Method]
	s.mu.RUnlock()
	if !ok {
		_ = encodeResponse(conn, Response{RequestID: req.RequestID, Success: false, Error: "unknown method"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result, callErr := h(ctx, req.Payload)
	if callErr != nil {
		_ = encodeResponse(conn, Response{RequestID: req.RequestID, Success: false, Error: callErr.Error()})
		return
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		_ = encodeResponse(conn, Response{RequestID: req.RequestID, Success: false, Error: "marshal result failed"})
		return
	}
	_ = encodeResponse(conn, Response{RequestID: req.RequestID, Success: true, Result: resultBytes})
}
