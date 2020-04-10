package ocxorm

import (
	"context"
	"fmt"

	"go.opencensus.io/trace"
	"xorm.io/xorm"
	"xorm.io/xorm/contexts"
)

var _ contexts.Hook = &Hook{}

type Hook struct {
}

func NewHook() *Hook {
	return &Hook{}
}

func WrapEngine(e *xorm.Engine) {
	e.AddHook(NewHook())
}

func (w Hook) BeforeProcess(c *contexts.ContextHook) (context.Context, error) {
	ctx, span := trace.StartSpan(c.Ctx, "xorm.sql", trace.WithSpanKind(trace.SpanKindClient))
	span.AddAttributes(
		trace.StringAttribute("sql", fmt.Sprintf("%v %v", c.SQL, c.Args)),
	)
	return ctx, nil
}

func (w Hook) AfterProcess(c *contexts.ContextHook) error {
	if span := trace.FromContext(c.Ctx); span != nil {
		setSpanStatus(span, c.Err)
		if c.ExecuteTime > 0 {
			span.AddAttributes(trace.Int64Attribute("execute_time_ms", c.ExecuteTime.Milliseconds()))
		}
		span.End()
	}
	return nil
}

func setSpanStatus(span *trace.Span, err error) {
	var status trace.Status
	if err == nil {
		status.Code = trace.StatusCodeOK
		span.SetStatus(status)
		return
	}
	switch err {
	case xorm.ErrNotExist:
		status.Code = trace.StatusCodeNotFound
	case context.Canceled:
		status.Code = trace.StatusCodeCancelled
	case context.DeadlineExceeded:
		status.Code = trace.StatusCodeDeadlineExceeded
	default:
		status.Code = trace.StatusCodeUnknown
	}
	status.Message = err.Error()
	span.SetStatus(status)
}
