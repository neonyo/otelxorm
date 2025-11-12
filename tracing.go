package otelxorm

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"go.opentelemetry.io/otel/trace"
	"xorm.io/xorm"
	"xorm.io/xorm/contexts"
)

const (
	tracerName = "github.com/jenbonzhang/otelxorm"
)

type OpenTelemetryHook struct {
	config *config
}

func Hook(opts ...Option) contexts.Hook {
	cfg := &config{}
	for _, opt := range opts {
		opt.apply(cfg)
	}
	if cfg.tracerProvider == nil {
		cfg.tracerProvider = otel.GetTracerProvider()
	}
	if cfg.tracer == nil {
		cfg.tracer = cfg.tracerProvider.Tracer(
			tracerName,
			trace.WithInstrumentationVersion(SemVersion()),
		)
	}
	if cfg.formatSQL == nil {
		cfg.formatSQL = defaultFormatSQL
	}
	for _, attr := range cfg.attrs {
		if attr.Key == semconv.DBNameKey {
			cfg.dbName = attr.Value.AsString()
		}
	}
	return &OpenTelemetryHook{
		config: cfg,
	}
}

func WrapEngine(e *xorm.Engine, opts ...Option) {
	e.AddHook(Hook(opts...))
}

func WrapEngineGroup(eg *xorm.EngineGroup, opts ...Option) {
	eg.AddHook(Hook(opts...))
}

func (h *OpenTelemetryHook) BeforeProcess(c *contexts.ContextHook) (context.Context, error) {
	spanName := "sql"
	if h.config.spanName != "" {
		spanName = h.config.spanName
	}
	if c.Ctx == nil {
		return context.Background(), nil
	}
	if ctx, ok := c.Ctx.Value("spanCtx").(context.Context); ok {
		newCtx, _ := h.config.tracer.Start(ctx,
			spanName,
			trace.WithSpanKind(trace.SpanKindClient),
		)
		if h.config.beforeHook != nil {
			h.config.beforeHook(c)
		}
		return context.WithValue(context.Background(), "span", newCtx), nil
	}
	return c.Ctx, nil
}

func (h *OpenTelemetryHook) AfterProcess(c *contexts.ContextHook) error {
	if c.Ctx == nil {
		return nil
	}
	if ctx, ok := c.Ctx.Value("span").(context.Context); ok {
		span := trace.SpanFromContext(ctx)
		attrs := make([]attribute.KeyValue, 0)
		defer span.End()

		attrs = append(attrs, h.config.attrs...)
		attrs = append(attrs, attribute.Key("go.orm").String("xorm"))
		attrs = append(attrs, semconv.DBStatement(h.config.formatSQL(c.SQL, c.Args)))

		if c.Result != nil {
			rows, _ := c.Result.RowsAffected()
			attrs = append(attrs, attribute.Int64("db.rows.affected", rows))
		}

		if c.Err != nil {
			span.RecordError(c.Err)
			span.SetStatus(codes.Error, c.Err.Error())
		}
		span.SetAttributes(attrs...)
		if h.config.afterHook != nil {
			h.config.afterHook(c)
		}
	}
	return nil
}
