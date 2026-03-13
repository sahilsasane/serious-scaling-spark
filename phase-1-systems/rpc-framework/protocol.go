package main

import (
"encoding/json"
"errors"
"net"
)

var errInvalidMessage = errors.New("invalid rpc message")

type Request struct {
	RequestID  string          `json:"request_id"`
	Method     string          `json:"method"`
	Payload    json.RawMessage `json:"payload"`
	Idempotent bool            `json:"idempotent"`
}

type Response struct {
	RequestID string          `json:"request_id"`
	Success   bool            `json:"success"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     string          `json:"error,omitempty"`
}

func decodeRequest(conn net.Conn) (Request, error) {
	var req Request
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		return Request{}, err
	}
	if req.RequestID == "" || req.Method == "" {
		return Request{}, errInvalidMessage
	}
	return req, nil
}

func encodeResponse(conn net.Conn, resp Response) error {
	return json.NewEncoder(conn).Encode(resp)
}
