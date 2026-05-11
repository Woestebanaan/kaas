package protocol

// Middleware wraps a Handler so cross-cutting concerns (metrics,
// tracing, structured logging) can run before/after every request
// without each handler having to repeat the boilerplate. Before
// middleware existed, only produce.go and fetch.go bothered with
// the latency-histogram defer block; the other ~28 handlers
// silently missed the timeseries. Middleware fixes that uniformly.
//
// Wire ordering: the first Use() call is the OUTERMOST layer (runs
// first on the way in, last on the way out); the last Use() is the
// INNERMOST (closest to the handler). Onion order.
//
// Middleware receives the API key at Register time so it can label
// metrics / spans without having to plumb the key through every
// Handler.Handle signature. The Handler.Handle signature stays
// unchanged — middleware composes purely at the type level.
//
// Performance: the chain is applied once at Register, not per
// Dispatch. The hot path sees a single (already-chained) Handler
// per registered API key; no closure stack rebuild per request.
type Middleware func(apiKey int16, next Handler) Handler

// Use registers a middleware. MUST be called BEFORE Register —
// middlewares added after Register has run won't apply to the
// handlers already registered. (Documented contract; we don't
// retroactively rewrap.)
func (d *Dispatcher) Use(mw Middleware) {
	d.middleware = append(d.middleware, mw)
}

// chain composes the registered middleware stack onto the base
// handler for the given API key. Used by Register.
func (d *Dispatcher) chain(apiKey int16, h Handler) Handler {
	for i := len(d.middleware) - 1; i >= 0; i-- {
		h = d.middleware[i](apiKey, h)
	}
	return h
}
