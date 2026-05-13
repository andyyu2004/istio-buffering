// Slow HTTPS client that forces a tiny SO_RCVBUF on its TCP socket so the
// TCP receive window stays small and the server experiences real, kernel-level
// flow control — unlike `curl --limit-rate` (post-read sleep) or toxiproxy
// (large internal buffer).
//
// Usage:
//
//	go run runs/slow_client.go -url https://127.0.0.1:32768/testdata.bin -rcvbuf 8192
//
// Lower -rcvbuf means tighter backpressure. 8192 is a good starting point;
// drop to 4096 if you want to see the chain really squeezed.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	url := flag.String("url", "https://127.0.0.1:32768/testdata.bin", "URL to GET")
	rcvbuf := flag.Int("rcvbuf", 8192, "SO_RCVBUF in bytes (Linux doubles internally)")
	rate := flag.Float64("rate", 125, "Target read rate in KB/s (0 = unlimited). Combine with small -rcvbuf for real backpressure.")
	timeout := flag.Duration("timeout", 60*time.Second, "Overall request timeout")
	progress := flag.Duration("progress", 2*time.Second, "Progress reporting interval")
	flag.Parse()

	dialer := &net.Dialer{
		Control: func(network, address string, c syscall.RawConn) error {
			var setErr error
			ctrlErr := c.Control(func(fd uintptr) {
				if e := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, *rcvbuf); e != nil {
					setErr = fmt.Errorf("setsockopt SO_RCVBUF: %w", e)
					return
				}
				actual, e := syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
				if e != nil {
					setErr = fmt.Errorf("getsockopt SO_RCVBUF: %w", e)
					return
				}
				fmt.Fprintf(os.Stderr, "set SO_RCVBUF=%d, kernel reports %d (%s)\n",
					*rcvbuf, actual, network)
			})
			if ctrlErr != nil {
				return ctrlErr
			}
			return setErr
		},
	}

	transport := &http.Transport{
		DialContext:     dialer.DialContext,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		// Force HTTP/1.1 — empty TLSNextProto disables ALPN-negotiated h2.
		TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	client := &http.Client{Transport: transport, Timeout: *timeout}

	fmt.Fprintf(os.Stderr, "GET %s  (requested SO_RCVBUF=%d, kernel will double)\n", *url, *rcvbuf)

	req, err := http.NewRequestWithContext(context.Background(), "GET", *url, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "request:", err)
		os.Exit(1)
	}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "GET error:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	fmt.Fprintf(os.Stderr, "status=%s\n", resp.Status)

	var totalRead int64
	stop := make(chan struct{})
	go reportProgress(start, *progress, &totalRead, stop)

	written, err := readSlowly(resp.Body, &totalRead, *rate)
	close(stop)
	elapsed := time.Since(start)
	if err != nil && err != context.Canceled {
		fmt.Fprintln(os.Stderr, "read error:", err)
	}
	avgKBps := float64(written) / elapsed.Seconds() / 1024
	fmt.Fprintf(os.Stderr, "Done. bytes=%d  duration=%.2fs  avg=%.1f KB/s\n",
		written, elapsed.Seconds(), avgKBps)
}

// readSlowly drains r at most `rateKBps` KB/s. With a small SO_RCVBUF on the
// underlying socket, this keeps the kernel recv buffer near-zero, advertises a
// tiny TCP rwnd to the server, and exercises real kernel-level backpressure —
// unlike curl --limit-rate (post-read sleep with large kernel buffer) or
// toxiproxy (large internal buffer in the proxy).
func readSlowly(r io.Reader, total *int64, rateKBps float64) (int64, error) {
	// Read in small chunks so the throttle is fine-grained. Chunks slightly
	// larger than typical MTU mean each read is roughly one ack-able batch.
	buf := make([]byte, 2048)
	var written int64
	next := time.Now()
	for {
		n, err := r.Read(buf)
		if n > 0 {
			written += int64(n)
			atomic.AddInt64(total, int64(n))
			if rateKBps > 0 {
				next = next.Add(time.Duration(float64(n) / (rateKBps * 1024) * float64(time.Second)))
				if d := time.Until(next); d > 0 {
					time.Sleep(d)
				} else if d < -1*time.Second {
					// Don't accumulate slack > 1s — keeps the rate honest if we
					// fall behind early (e.g. TLS handshake delay).
					next = time.Now()
				}
			}
		}
		if err == io.EOF {
			return written, nil
		}
		if err != nil {
			return written, err
		}
	}
}

func reportProgress(start time.Time, interval time.Duration, total *int64, stop <-chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()
	var prev int64
	prevT := start
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			cur := atomic.LoadInt64(total)
			dt := now.Sub(prevT).Seconds()
			rate := float64(cur-prev) / dt / 1024
			fmt.Fprintf(os.Stderr, "  +%.1fs:  total=%d B  interval=%.1f KB/s\n",
				now.Sub(start).Seconds(), cur, rate)
			prev = cur
			prevT = now
		}
	}
}
