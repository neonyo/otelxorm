package otelxorm

import (
	"encoding/json"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"go.opentelemetry.io/otel/trace"
	"regexp"
	"strings"
	"time"
	"xorm.io/xorm/contexts"
)

// Option specifies instrumentation configuration options.
type Option interface {
	apply(*config)
}
type optionFunc func(*config)

func (o optionFunc) apply(c *config) {
	o(c)
}

type config struct {
	dbName         string
	tracerProvider trace.TracerProvider
	tracer         trace.Tracer
	attrs          []attribute.KeyValue
	beforeHook     func(c *contexts.ContextHook)
	afterHook      func(c *contexts.ContextHook)
	formatSQL      func(sql string, args []interface{}) string
}

// WithTracerProvider with tracer provider.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return optionFunc(func(cfg *config) {
		cfg.tracerProvider = provider
	})
}

// WithDBSystem configures a db.system attribute. You should prefer using
// WithAttributes and semconv, for example, `otelsql.WithAttributes(semconv.DBSystemSqlite)`.
func WithDBSystem(system string) Option {
	return optionFunc(func(c *config) {
		c.attrs = append(c.attrs, semconv.DBSystemKey.String(system))
	})
}

// WithDBName configures a db.name attribute.
func WithDBName(name string) Option {
	return optionFunc(func(c *config) {
		c.attrs = append(c.attrs, semconv.DBName(name))
	})
}

func WithFormatSQL(formatSQL func(sql string, args []interface{}) string) Option {
	return optionFunc(func(c *config) {
		c.formatSQL = formatSQL
	})
}

func WithFormatSQLReplace() Option {
	return WithFormatSQL(formatSQLReplace)
}

func WithBeforeHookHook(fn func(c *contexts.ContextHook)) Option {
	return optionFunc(func(c *config) {
		c.beforeHook = fn
	})
}

func WithAfterHook(fn func(c *contexts.ContextHook)) Option {
	return optionFunc(func(c *config) {
		c.afterHook = fn
	})
}

func defaultFormatSQL(sql string, args []interface{}) string {
	argsStr := fmt.Sprintf("%v", args)
	m, err := json.Marshal(args)
	if err == nil {
		argsStr = string(m)
	}
	return fmt.Sprintf("%v %v", sql, argsStr)
}

func formatSQLReplace(sql string, args []interface{}) string {
	if len(args) == 0 {
		return sql
	}

	re := regexp.MustCompile(`\$\d+`)
	matches := re.FindAllStringIndex(sql, -1)

	if len(matches) == 0 {
		// 如果没有找到占位符，但提供了参数，我们将参数添加到SQL语句的末尾
		return fmt.Sprintf("%s /* Unused args: %v */", sql, args)
	}

	var sb strings.Builder
	lastIndex := 0
	argIndex := 0

	for _, match := range matches {
		sb.WriteString(sql[lastIndex:match[0]])

		if argIndex < len(args) {
			sb.WriteString(formatValue(args[argIndex]))
			argIndex++
		} else {
			// 如果参数不足，保留原始占位符
			sb.WriteString(sql[match[0]:match[1]])
		}

		lastIndex = match[1]
	}

	sb.WriteString(sql[lastIndex:])

	// 如果还有未使用的参数，将它们作为注释添加到SQL的末尾
	if argIndex < len(args) {
		sb.WriteString(fmt.Sprintf(" /* Unused args: %v */", args[argIndex:]))
	}

	return sb.String()
}

func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	var data string
	switch val := v.(type) {
	case string:
		data = val
	case time.Time:
		data = fmt.Sprintf("'%s'", val.Format("2006-01-02 15:04:05"))
	case []byte:
		data = string(val)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		data = fmt.Sprintf("%v", val)
	default:
		d, _ := json.Marshal(val)
		data = string(d)
	}
	return fmt.Sprintf("'%v'", data)
}
