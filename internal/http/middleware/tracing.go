package middleware

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func Tracing() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				route := chi.RouteContext(r.Context()).RoutePattern()
				if route == "" {
					route = "unknown"
				}
				routeAttr := attribute.String("http.route", route)
				span := trace.SpanFromContext(r.Context())
				span.SetName(r.Method + " " + route)
				span.SetAttributes(routeAttr)

				if labeler, ok := otelhttp.LabelerFromContext(r.Context()); ok {
					labeler.Add(routeAttr)
				}
			}()
			next.ServeHTTP(w, r)
		})

		return otelhttp.NewHandler(handler, "http")
	}
}
