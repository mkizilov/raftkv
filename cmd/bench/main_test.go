package main

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestHistogramPercentileBounds(t *testing.T) {
	var h histogram
	h.observe(time.Microsecond)
	h.observe(200 * time.Microsecond)
	h.observe(7 * time.Second)
	if got := h.percentile(50); got != 300*time.Microsecond { t.Fatalf("p50 upper bound = %s", got) }
	if got := h.percentile(99); got != 7*time.Second { t.Fatalf("p99 upper bound = %s", got) }
}

func TestInvalidBenchmarkOptions(t *testing.T) {
	if err := run(context.Background(), 0, 1, io.Discard); err == nil { t.Fatal("accepted zero duration") }
	if err := run(context.Background(), time.Second, 0, io.Discard); err == nil { t.Fatal("accepted zero operation limit") }
	if err := run(context.Background(), 31*time.Second, 1, io.Discard); err == nil { t.Fatal("accepted unbounded duration") }
	if err := run(context.Background(), time.Second, hardMaxOps+1, io.Discard); err == nil { t.Fatal("accepted unbounded operation count") }
}
