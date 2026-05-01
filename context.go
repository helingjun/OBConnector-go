package oceanbase

import (
	"context"
)

type contextKey int

const (
	partitionIDKey contextKey = iota
	traceIDKey
)

func WithPartitionID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, partitionIDKey, id)
}

func WithTraceID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceIDKey, id)
}

func partitionIDFromContext(ctx context.Context) (int64, bool) {
	id, ok := ctx.Value(partitionIDKey).(int64)
	return id, ok
}

func traceIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(traceIDKey).(string)
	return id, ok
}
