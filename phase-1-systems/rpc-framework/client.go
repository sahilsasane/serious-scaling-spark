package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
)

type RetryPolicy struct {
	MaxRetries  int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	DialTimeout time.Duration
	CallTimeout time.Duration
}

type Client struct {
	endpoints []string
	policy    RetryPolicy
	rr        uint32
	seq       uint64
}

func NewClient(endpoints []string, policy RetryPolicy) *Client {
	if len(endpoints) == 0 {
		panic("at least one endpoint required")
	}
	if policy.BaseBackoff == 0 {
		policy.BaseBackoff = 50 * time.Millisecond
	}
	if policy.MaxBackoff == 0 {
		policy.MaxBackoff = 800 * time.Millisecond
	}
	if policy.DialTimeout == 0 {
		policy.DialTimeout = 200 * time.Millisecond
	}
	if policy.CallTimeout == 0 {
		policy.CallTimeout = 500 * time.Millisecond
	}
	return &Client{endpoints: endpoints, policy: policy}
}

func (c *Client) nextEndpoint() string {
	idx := atomic.AddUint32(&c.rr, 1)
	return c.endpoints[(int(idx)-1)%len(c.endpoints)]
}

func (c *Client) nextRequestID() string {
	id := atomic.AddUint64(&c.seq, 1)
	return fmt.Sprintf("req-%d", id)
}

func (c *Client) Call(ctx context.Context, method string, payload any, idempotent bool, out any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	var lastErr error
	attempts := c.policy.MaxRetries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		endpoint := c.nextEndpoint()
		lastErr = c.callOnce(ctx, endpoint, method, payloadBytes, idempotent, out)
		if lastErr == nil {
			return nil
		}

		if !idempotent || !isRetryable(lastErr) || attempt == attempts-1 {
			break
		}

		backoff := computeBackoff(c.policy.BaseBackoff, c.policy.MaxBackoff, attempt)
		jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
		wait := backoff + jitter
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}

	return lastErr
}

func (c *Client) callOnce(ctx context.Context, endpoint, method string, payloadBytes []byte, idempotent bool, out any) error {
	conn, err := net.DialTimeout("tcp", endpoint, c.policy.DialTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	callDeadline := time.Now().Add(c.policy.CallTimeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(callDeadline) {
		callDeadline = dl
	}
	if err := conn.SetDeadline(callDeadline); err != nil {
		return err
	}

	req := Request{
		RequestID:  c.nextRequestID(),
		Method:     method,
		Payload:    payloadBytes,
		Idempotent: idempotent,
	}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return err
	}

	var resp Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return err
	}
	if !resp.Success {
		return errors.New(resp.Error)
	}
	if out != nil {
		if err := json.Unmarshal(resp.Result, out); err != nil {
			return err
		}
	}
	return nil
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}

func computeBackoff(base, max time.Duration, attempt int) time.Duration {
	scaled := float64(base) * math.Pow(2, float64(attempt))
	if time.Duration(scaled) > max {
		return max
	}
	return time.Duration(scaled)
}
