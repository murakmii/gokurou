package gokurou

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
	tracerContextKey = "GOKUROU_CTX_KEY_TRACER"
)

func RootContext(conf *Configuration) (context.Context, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	if conf.DebugLevelLogging {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx = ContextWithLogger(ctx, logrus.NewEntry(logger))

	if conf.TracerProvider != nil {
		tracer, err := conf.TracerProvider(conf)
		if err != nil {
			return nil, xerrors.Errorf("failed to setup context: %w", err)
		}
		ctx = ContextWithTracer(ctx, tracer)
	} else {
		ctx = ContextWithTracer(ctx, NewNullTracer())
	}

	// いくつかのシグナルを受信したらクロールを終了する
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT)

		select {
		case <-sigCh:
			cancel()
		}
	}()

	return ctx, nil
}

func MustRootContext(conf *Configuration) context.Context {
	ctx, err := RootContext(conf)
	if err != nil {
		panic(err)
	}

	return ctx
}

func WorkerContext(ctx context.Context, gwn uint16) (context.Context, context.CancelFunc) {
	ctx = ContextWithGWN(ctx, gwn)
	ctx = ContextWithLogger(ctx, LoggerFromContext(ctx).WithField("gwn", gwn))
	return context.WithCancel(ctx)
}

func SubSystemContext(ctx context.Context, name string) context.Context {
	return ContextWithLogger(ctx, LoggerFromContext(ctx).WithField("subsys", name))
}

func ContextWithLogger(ctx context.Context, logger *logrus.Entry) context.Context {
	return context.WithValue(ctx, loggerContextKey, logger)
}

func ContextWithGWN(ctx context.Context, gwn uint16) context.Context {
	return context.WithValue(ctx, gwnContextKey, gwn)
}

func ContextWithTracer(ctx context.Context, tracer Tracer) context.Context {
	return context.WithValue(ctx, tracerContextKey, tracer)
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

func TracerFromContext(ctx context.Context) Tracer {
	tracer, ok := ctx.Value(tracerContextKey).(Tracer)
	if !ok {
		panic(xerrors.New("can't fetch tracer from context"))
	}

	return tracer
}
