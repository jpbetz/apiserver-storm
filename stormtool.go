// stormtool: minimal raw-HTTP harness for inducing kube-apiserver peak load.
// No client-go dependency; reflector-like semantics implemented directly so the
// herd behavior (LIST on connect, WATCH until disconnect, immediate re-LIST) is
// explicit and tunable.
//
// Subcommands:
//
//	seed      -n <count> -size <bytes> -c <concurrency> -ns <namespace>
//	herd      -w <watchers> -duration <dur> [-list-rv 0|""]
//	listburst -b <concurrency> -count <total> [-rv 0|""] [-selector k=v]
//
// Common flags: -server, -cert, -key
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	server   string
	certFile string
	keyFile  string
	resource string
)

func commonFlags(fs *flag.FlagSet) {
	fs.StringVar(&server, "server", "https://127.0.0.1:6443", "API server URL")
	fs.StringVar(&certFile, "cert", "", "client certificate file")
	fs.StringVar(&keyFile, "key", "", "client key file")
	fs.StringVar(&resource, "resource", "configmaps", "resource to target: configmaps|pods")
}

func collectionURL(ns string) string {
	if ns == "" {
		return server + "/api/v1/" + resource
	}
	return server + "/api/v1/namespaces/" + ns + "/" + resource
}

func acceptHeader(kind string) string {
	switch kind {
	case "proto":
		return "application/vnd.kubernetes.protobuf"
	case "table":
		return "application/json;as=Table;v=v1;g=meta.k8s.io"
	default:
		return "application/json"
	}
}

func newClient() *http.Client {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		fatalf("load keypair: %v", err)
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true,
		},
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 512,
		ForceAttemptHTTP2:   true,
	}
	return &http.Client{Transport: tr}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL "+format+"\n", args...)
	os.Exit(1)
}

var logMu sync.Mutex

func event(format string, args ...any) {
	logMu.Lock()
	defer logMu.Unlock()
	fmt.Printf("EVENT ts=%d ", time.Now().UnixMilli())
	fmt.Printf(format+"\n", args...)
}

var rvRe = regexp.MustCompile(`"resourceVersion"\s*:\s*"(\d+)"`)

// drainList reads a LIST response, returning (bytes, resourceVersion).
func drainList(body io.Reader) (int64, string) {
	head := make([]byte, 4096)
	n, _ := io.ReadFull(body, head)
	rv := ""
	if m := rvRe.FindSubmatch(head[:n]); m != nil {
		rv = string(m[1])
	}
	rest, _ := io.Copy(io.Discard, body)
	return int64(n) + rest, rv
}

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: stormtool <seed|herd|listburst> [flags]")
	}
	switch os.Args[1] {
	case "seed":
		seedCmd(os.Args[2:])
	case "herd":
		herdCmd(os.Args[2:])
	case "listburst":
		listburstCmd(os.Args[2:])
	case "churn":
		churnCmd(os.Args[2:])
	default:
		fatalf("unknown subcommand %q", os.Args[1])
	}
}

func seedCmd(args []string) {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	commonFlags(fs)
	n := fs.Int("n", 15000, "number of configmaps")
	size := fs.Int("size", 4096, "payload bytes per configmap")
	conc := fs.Int("c", 64, "concurrent creators")
	ns := fs.String("ns", "storm", "namespace")
	seed := fs.Int64("seed", 42, "deterministic payload seed (byte-identical objects across runs)")
	fs.Parse(args)
	rng := rand.New(rand.NewSource(*seed))

	client := newClient()

	// Create namespace (ignore 409).
	nsBody := fmt.Sprintf(`{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"%s"}}`, *ns)
	resp, err := client.Post(server+"/api/v1/namespaces", "application/json", strings.NewReader(nsBody))
	if err != nil {
		fatalf("create namespace: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resource == "pods" {
		// No controller-manager runs against this apiserver, so the namespace
		// never gets its default ServiceAccount — create it or the SA
		// admission plugin rejects every pod.
		saBody := `{"apiVersion":"v1","kind":"ServiceAccount","metadata":{"name":"default"}}`
		resp, err := client.Post(server+"/api/v1/namespaces/"+*ns+"/serviceaccounts", "application/json", strings.NewReader(saBody))
		if err != nil {
			fatalf("create serviceaccount: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 201 && resp.StatusCode != 409 {
			fatalf("create serviceaccount: HTTP %d", resp.StatusCode)
		}
	}

	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	payload := make([]byte, *size)
	for i := range payload {
		payload[i] = letters[rng.Intn(len(letters))]
	}

	// Pods share images/labels/env names across the collection (the realistic,
	// interning-friendly shape); per-pod identity comes from name/idx.
	podTemplate := `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"pod-%06d","labels":{"app":"storm","tier":"backend","release":"r2026-07","zone":"z1","batch":"b%d"},"annotations":{"storm.example.com/pad":"%s"}},"spec":{"containers":[{"name":"main","image":"registry.k8s.io/pause:3.10","env":[{"name":"STORM_IDX","value":"%d"}],"resources":{"requests":{"cpu":"100m","memory":"128Mi"},"limits":{"cpu":"500m","memory":"256Mi"}}},{"name":"sidecar","image":"registry.k8s.io/e2e-test-images/agnhost:2.53","resources":{"requests":{"cpu":"10m","memory":"32Mi"}}}],"terminationGracePeriodSeconds":30,"automountServiceAccountToken":false}}`

	var created, conflicts, errs int64
	start := time.Now()
	work := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				var body string
				if resource == "pods" {
					body = fmt.Sprintf(podTemplate, i, i%10, payload, i)
				} else {
					body = fmt.Sprintf(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-%06d"},"data":{"payload":"%s","idx":"%d"}}`,
						i, payload, i)
				}
				resp, err := client.Post(collectionURL(*ns), "application/json", strings.NewReader(body))
				if err != nil {
					atomic.AddInt64(&errs, 1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				switch resp.StatusCode {
				case 201:
					atomic.AddInt64(&created, 1)
				case 409:
					atomic.AddInt64(&conflicts, 1)
				default:
					atomic.AddInt64(&errs, 1)
				}
			}
		}()
	}
	for i := 0; i < *n; i++ {
		work <- i
	}
	close(work)
	wg.Wait()
	event("kind=seed_done created=%d conflicts=%d errors=%d dur_ms=%d", created, conflicts, errs, time.Since(start).Milliseconds())
	if errs > 0 {
		os.Exit(1)
	}
}

func herdCmd(args []string) {
	fs := flag.NewFlagSet("herd", flag.ExitOnError)
	commonFlags(fs)
	w := fs.Int("w", 100, "number of watchers")
	duration := fs.Duration("duration", 30*time.Minute, "how long to run")
	listRV := fs.String("list-rv", "0", `resourceVersion for relists: "0" (cache) or "" (consistent read)`)
	retryDelay := fs.Duration("retry-delay", 200*time.Millisecond, "delay before relist after disconnect/error")
	watchList := fs.Bool("watchlist", false, "use WatchList streaming (watch with sendInitialEvents) instead of LIST+WATCH")
	fs.Parse(args)

	client := newClient()
	deadline := time.Now().Add(*duration)

	var wg sync.WaitGroup
	for i := 0; i < *w; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Small start jitter so initial connection isn't a synthetic
			// single-instant spike; disconnect-driven herds stay synchronized.
			time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond)
			for time.Now().Before(deadline) {
				if *watchList {
					// WatchList (KEP-3157): one streaming watch replaces
					// LIST+WATCH; initial state arrives as ADDED events.
					t0 := time.Now()
					wlurl := collectionURL("") + "?watch=1&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&resourceVersion=0&allowWatchBookmarks=true&timeoutSeconds=300"
					resp, err := client.Get(wlurl)
					if err != nil {
						event("kind=watchlist_error watcher=%d err=%q", id, err.Error())
						time.Sleep(*retryDelay)
						continue
					}
					if resp.StatusCode != 200 {
						io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
						event("kind=watchlist_http_error watcher=%d code=%d", id, resp.StatusCode)
						time.Sleep(*retryDelay)
						continue
					}
					wbytes, _ := io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					event("kind=watchlist_closed watcher=%d bytes=%d dur_ms=%d", id, wbytes, time.Since(t0).Milliseconds())
					time.Sleep(*retryDelay)
					continue
				}
				// LIST
				t0 := time.Now()
				url := collectionURL("") + "?resourceVersion=" + *listRV
				if *listRV == "" {
					url = collectionURL("")
				}
				resp, err := client.Get(url)
				if err != nil {
					event("kind=list_error watcher=%d err=%q", id, err.Error())
					time.Sleep(*retryDelay)
					continue
				}
				if resp.StatusCode != 200 {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					event("kind=list_http_error watcher=%d code=%d", id, resp.StatusCode)
					time.Sleep(*retryDelay)
					continue
				}
				bytes, rv := drainList(resp.Body)
				resp.Body.Close()
				event("kind=relist watcher=%d bytes=%d ms=%d rv=%s", id, bytes, time.Since(t0).Milliseconds(), rv)
				if rv == "" {
					time.Sleep(*retryDelay)
					continue
				}
				// WATCH from rv until disconnect.
				t1 := time.Now()
				wurl := fmt.Sprintf("%s?watch=1&resourceVersion=%s&allowWatchBookmarks=true&timeoutSeconds=300", collectionURL(""), rv)
				wresp, err := client.Get(wurl)
				if err != nil {
					event("kind=watch_error watcher=%d err=%q", id, err.Error())
					time.Sleep(*retryDelay)
					continue
				}
				wbytes, _ := io.Copy(io.Discard, wresp.Body)
				wresp.Body.Close()
				event("kind=watch_closed watcher=%d bytes=%d dur_ms=%d", id, wbytes, time.Since(t1).Milliseconds())
				time.Sleep(*retryDelay)
			}
		}(i)
	}
	wg.Wait()
	event("kind=herd_done")
}

// churnCmd applies steady annotation-update write load to the seeded objects.
// Needed to expose concurrency effects (e.g. watch-cache lock contention is
// only visible for LISTs racing writes) and to keep resourceVersions moving.
func churnCmd(args []string) {
	fs := flag.NewFlagSet("churn", flag.ExitOnError)
	commonFlags(fs)
	ns := fs.String("ns", "storm", "namespace")
	n := fs.Int("n", 15000, "number of seeded objects (cm-%06d / pod-%06d names)")
	qps := fs.Float64("qps", 200, "target update QPS")
	conc := fs.Int("c", 16, "concurrent updaters")
	duration := fs.Duration("duration", 30*time.Minute, "how long to churn")
	fs.Parse(args)

	client := newClient()
	prefix := "cm"
	if resource == "pods" {
		prefix = "pod"
	}
	deadline := time.Now().Add(*duration)
	interval := time.Duration(float64(time.Second) / *qps * float64(*conc))
	var updates, errs int64
	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := id
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for time.Now().Before(deadline) {
				<-ticker.C
				name := fmt.Sprintf("%s-%06d", prefix, i%*n)
				i += *conc
				patch := fmt.Sprintf(`{"metadata":{"annotations":{"storm.example.com/churn":"%d"}}}`, i)
				req, _ := http.NewRequest("PATCH", collectionURL(*ns)+"/"+name, strings.NewReader(patch))
				req.Header.Set("Content-Type", "application/strategic-merge-patch+json")
				resp, err := client.Do(req)
				if err != nil {
					atomic.AddInt64(&errs, 1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == 200 {
					atomic.AddInt64(&updates, 1)
				} else {
					atomic.AddInt64(&errs, 1)
				}
			}
		}(w)
	}
	wg.Wait()
	event("kind=churn_done updates=%d errors=%d", updates, errs)
}

func listburstCmd(args []string) {
	fs := flag.NewFlagSet("listburst", flag.ExitOnError)
	commonFlags(fs)
	b := fs.Int("b", 40, "concurrent listers")
	count := fs.Int("count", 80, "total LIST requests")
	rv := fs.String("rv", "0", `resourceVersion: "0" (cache) or "" (consistent read)`)
	selector := fs.String("selector", "", "labelSelector to apply (forces per-object filtering)")
	accept := fs.String("accept", "json", "response encoding: json|proto|table")
	fs.Parse(args)

	client := newClient()
	url := collectionURL("")
	sep := "?"
	if *rv != "" {
		url += sep + "resourceVersion=" + *rv
		sep = "&"
	}
	if *selector != "" {
		url += sep + "labelSelector=" + *selector
	}

	lat := make([]int64, 0, *count)
	var totalBytes int64
	var latMu sync.Mutex
	work := make(chan struct{})
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *b; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range work {
				t0 := time.Now()
				req, _ := http.NewRequest("GET", url, nil)
				req.Header.Set("Accept", acceptHeader(*accept))
				resp, err := client.Do(req)
				if err != nil {
					event("kind=burst_error err=%q", err.Error())
					continue
				}
				if resp.StatusCode != 200 {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					event("kind=burst_http_error code=%d", resp.StatusCode)
					continue
				}
				n, _ := drainList(resp.Body)
				resp.Body.Close()
				ms := time.Since(t0).Milliseconds()
				latMu.Lock()
				lat = append(lat, ms)
				totalBytes += n
				latMu.Unlock()
			}
		}()
	}
	for i := 0; i < *count; i++ {
		work <- struct{}{}
	}
	close(work)
	wg.Wait()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) int64 {
		if len(lat) == 0 {
			return -1
		}
		idx := int(p * float64(len(lat)-1))
		return lat[idx]
	}
	event("kind=burst_done url=%q n=%d wall_ms=%d bytes=%d p50=%d p90=%d p99=%d max=%d",
		url, len(lat), time.Since(start).Milliseconds(), totalBytes, pct(0.50), pct(0.90), pct(0.99), pct(1.0))
}
