package push

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type Kind string

const (
	KindRequestPosition Kind = "REQUEST_POSITION"
)

type Request struct {
	RequestID   uuid.UUID         `json:"request_id"`
	RecipientID uuid.UUID         `json:"recipient_id"`
	Kind        Kind              `json:"kind"`
	Title       string            `json:"title,omitempty"`
	Body        string            `json:"body,omitempty"`
	Data        map[string]string `json:"data,omitempty"`
}

type Client struct {
	baseURL string
	http    *http.Client
}

func New(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: baseURL,
		http: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (c *Client) SendRequestPosition(ctx context.Context, tripID, driverID uuid.UUID) error {
	req := Request{
		RequestID:   uuid.New(),
		RecipientID: driverID,
		Kind:        KindRequestPosition,
		Data:        map[string]string{"trip_id": tripID.String()},
	}
	return c.send(ctx, req)
}

func (c *Client) send(ctx context.Context, req Request) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal push request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/push", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build push request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return fmt.Errorf("push request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}

	detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	switch {
	case resp.StatusCode >= 500:
		return &ServerError{StatusCode: resp.StatusCode, Body: string(detail)}
	default:
		return &ClientError{StatusCode: resp.StatusCode, Body: string(detail)}
	}
}

type ServerError struct {
	StatusCode int
	Body       string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("push service server error: status=%d body=%q", e.StatusCode, e.Body)
}

type ClientError struct {
	StatusCode int
	Body       string
}

func (e *ClientError) Error() string {
	return fmt.Sprintf("push service client error: status=%d body=%q", e.StatusCode, e.Body)
}

func IsTransport(err error) bool {
	var se *ServerError
	var ce *ClientError
	return err != nil && !errors.As(err, &se) && !errors.As(err, &ce)
}

func (c *Client) CloseIdleConnections() {
	c.http.CloseIdleConnections()
}
