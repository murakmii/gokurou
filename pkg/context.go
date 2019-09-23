package pkg

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/xerrors"

	"github.com/sirupsen/logrus"
)

const (
	gwnContextKey    = "GOKUROU_CTX_KEY_GWN"
	loggerContextKey = "GOKUROU_CTX_KEY_LOGGER"
)

func RootContext() context.Context {
	// TODO: log level
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})

	ctx, cancel := context.WithCancel(context.Background())
	ctx = ContextWithLogger(ctx, logrus.NewEntry(logger))

	// いくつかのシグナルを受信したらクロールを終了する
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT)

		select {
		case <-sigCh:
			cancel()
		}
	}()

	return ctx
}

func WorkerContext(ctx context.Context, gwn uint16) (context.Context, context.CancelFunc) {
	ctx = ContextWithGWN(ctx, gwn)
	ctx = ContextWithLogger(ctx, LoggerFromContext(ctx).WithField("gwn", gwn))
	return context.WithCancel(ctx)
}

func ComponentContext(ctx context.Context, name string) context.Context {
	return ContextWithLogger(ctx, LoggerFromContext(ctx).WithField("component", name))
}

func ContextWithLogger(ctx context.Context, logger *logrus.Entry) context.Context {
	return context.WithValue(ctx, loggerContextKey, logger)
}

func ContextWithGWN(ctx context.Context, gwn uint16) context.Context {
	return context.WithValue(ctx, gwnContextKey, gwn)
}

func LoggerFromContext(ctx context.Context) *logrus.Entry {
	logger, ok := ctx.Value(loggerContextKey).(*logrus.Entry)
	if !ok {
		panic(xerrors.New("can't fetch logger from context"))
	}

	return logger
}

func GWNFromContext(ctx context.Context) uint16 {
	gwn, ok := ctx.Value(gwnContextKey).(uint16)
	if !ok {
		panic(xerrors.New("can't fetch global worker number from context"))
	}

	return gwn
}
