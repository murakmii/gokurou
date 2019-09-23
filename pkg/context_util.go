package pkg

import (
	"context"
	"fmt"
)

const (
	gwnContextKey = "GOKUROU_CTX_KEY_GWN"
)

func ContextWithGWN(ctx context.Context, gwn uint16) context.Context {
	return context.WithValue(ctx, gwnContextKey, gwn)
}

func GWNFromContext(ctx context.Context) uint16 {
	gwn, ok := ctx.Value(gwnContextKey).(uint16)
	if !ok {
		panic(fmt.Errorf("can't fetch global worker number from context"))
	}

	return gwn
}
