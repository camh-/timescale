package main

import "context"

// sendQuery sends the query q on the output channel and returns true if it was
// able to send it before the context is cancelled, otherwise false is returned.
func sendQuery(ctx context.Context, q query, output chan<- query) bool {
	select {
	case <-ctx.Done():
		return false
	case output <- q:
		return true
	}
}

// recvQuery returns the next query on the input channel and true if a query
// was received, or false if ctx is done or the input channel is closed.
func recvQuery(ctx context.Context, q *query, input <-chan query) (ok bool) {
	select {
	case <-ctx.Done():
		return false
	case *q, ok = <-input:
		return
	}
}

func sendQueryResult(ctx context.Context, qr queryResult, output chan<- queryResult) bool {
	select {
	case <-ctx.Done():
		return false
	case output <- qr:
		return true
	}
}

func recvQueryResult(ctx context.Context, qr *queryResult, input <-chan queryResult) (ok bool) {
	select {
	case <-ctx.Done():
		return false
	case *qr, ok = <-input:
		return
	}
}
