package relay

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"math/rand"

	"github.com/influxdata/influxdb/models"
)

// HTTP is a relay for HTTP influxdb writes
type HTTP struct {
	addr   string
	name   string
	schema string

	cert string
	rp   string

	closing int64
	l       net.Listener

	backends []*httpBackend

	queries []*httpBackend
}

const (
	DefaultHTTPTimeout      = 10 * time.Second
	DefaultMaxDelayInterval = 10 * time.Second
	DefaultBatchSizeKB      = 512

	KB = 1024
	MB = 1024 * KB
)

func NewHTTP(cfg HTTPConfig) (Relay, error) {
	h := new(HTTP)

	h.addr = cfg.Addr
	h.name = cfg.Name

	h.cert = cfg.SSLCombinedPem
	h.rp = cfg.DefaultRetentionPolicy

	h.schema = "http"
	if h.cert != "" {
		h.schema = "https"
	}

	for i := range cfg.Outputs {
		backend, err := newHTTPBackend(&cfg.Outputs[i])
		if err != nil {
			return nil, err
		}

		h.backends = append(h.backends, backend)
	}

	for i := range cfg.Queries {
		query_backend, err := newHttpQueryBackend(&cfg.Queries[i])
		if err != nil {
			return nil, err
		}
		h.queries = append(h.queries, query_backend)
	}

	return h, nil
}

func (h *HTTP) Name() string {
	if h.name == "" {
		return fmt.Sprintf("%s://%s", h.schema, h.addr)
	}
	return h.name
}

func (h *HTTP) Run() error {
	l, err := net.Listen("tcp", h.addr)
	if err != nil {
		return err
	}

	// support HTTPS
	if h.cert != "" {
		cert, err := tls.LoadX509KeyPair(h.cert, h.cert)
		if err != nil {
			return err
		}

		l = tls.NewListener(l, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
	}

	h.l = l

	log.Printf("Starting %s relay %q on %v", strings.ToUpper(h.schema), h.Name(), h.addr)

	err = http.Serve(l, h)
	if atomic.LoadInt64(&h.closing) != 0 {
		return nil
	}
	return err
}

func (h *HTTP) Stop() error {
	atomic.StoreInt64(&h.closing, 1)
	return h.l.Close()
}

func (h *HTTP) servePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("X-InfluxDB-Version", "relay")
	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTP) serveQuery(w http.ResponseWriter, r *http.Request) {
	single_node_request := func(w http.ResponseWriter, r *http.Request) {
		// use random query backend
		rand.Seed(time.Now().UnixNano())
		n := rand.Intn(len(h.queries))
		resp, err := h.queries[n].poster.post(
			[]byte(""),
			r.URL.Query().Encode(),
			r.Header.Get("Authorization"))

		if err == nil {
			for k, v := range resp.Headers {
				w.Header().Set(k, v)
			}
			w.Write([]byte(resp.Body))
		} else {
			jsonError(w, http.StatusBadRequest, "request failed")
		}
	}

	all_node_request := func(w http.ResponseWriter, r *http.Request) {
		var resp *responseData
		var err error
		for _, q := range h.queries {
			resp, err = q.poster.post(
				[]byte{},
				r.URL.Query().Encode(),
				r.Header.Get("Authorization"))

			if err == nil {
				continue
			}
			// todo fix the partial success
		}

		if err == nil {
			w.Write([]byte(resp.Body))
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}

	error_request := func(w http.ResponseWriter, r *http.Request, token string) {
		msg := "relay is not support `" + token + "` expr!"
		jsonError(w, http.StatusBadRequest, msg)
	}

	q := strings.Trim(strings.ToUpper(r.URL.Query().Get("q")), " \t\r\n")
	tokens := strings.Split(q, " ")
	switch tokens[0] {
	case "SELECT", "SHOW":
		single_node_request(w, r) // proxy to one node is ok (ASSUME all backend have the same data)
	case "DELETE", "DROP", "GRANT", "REVOKE", "ALTER", "SET", "CREATE":
		all_node_request(w, r) // must proxy to all node
	case "KILL":
		error_request(w, r, tokens[0]) // not supported (we don't know the request should be proxy to which server)
	default:
		error_request(w, r, tokens[0]) // unknown command (direct return error)
	}
}

func (h *HTTP) serveWrite(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
		} else {
			jsonError(w, http.StatusMethodNotAllowed, "invalid write method")
		}
		return
	}

	queryParams := r.URL.Query()

	// fail early if we're missing the database
	if queryParams.Get("db") == "" {
		jsonError(w, http.StatusBadRequest, "missing parameter: db")
		return
	}

	if queryParams.Get("rp") == "" && h.rp != "" {
		queryParams.Set("rp", h.rp)
	}

	var body = r.Body

	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			jsonError(w, http.StatusBadRequest, "unable to decode gzip body")
		}
		defer b.Close()
		body = b
	}

	bodyBuf := getBuf()
	_, err := bodyBuf.ReadFrom(body)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "problem reading request body")
		return
	}

	precision := queryParams.Get("precision")
	points, err := models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "unable to parse points")
		return
	}

	outBuf := getBuf()
	for _, p := range points {
		if _, err = outBuf.WriteString(p.PrecisionString(precision)); err != nil {
			break
		}
		if err = outBuf.WriteByte('\n'); err != nil {
			break
		}
	}

	if err != nil {
		jsonError(w, http.StatusInternalServerError, "problem writing points")
		return
	}

	// normalize query string
	query := queryParams.Encode()

	outBytes := outBuf.Bytes()

	// check for authorization performed via the header
	authHeader := r.Header.Get("Authorization")

	for _, b := range h.backends {
		b := b
		go func() {
			resp, err := b.post(outBytes, query, authHeader)
			if err != nil {
				log.Printf("Problem posting to relay %q backend %q: %v", h.Name(), b.name, err)
			} else {
				if resp.StatusCode/100 == 5 {
					log.Printf("5xx response for relay %q backend %q: %v", h.Name(), b.name, resp.StatusCode)
				}
			}
		}()
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/ping":
		h.servePing(w, r)
	case "/query":
		h.serveQuery(w, r)
	case "/write":
		h.serveWrite(w, r)
	default:
		jsonError(w, http.StatusNotFound, "invalid write endpoint")
	}
}

type responseData struct {
	Headers map[string]string
	//ContentType        string
	//ContentEncoding    string
	//X-Influxdb-Version string
	StatusCode int
	Body       []byte
}

func (rd *responseData) Write(w http.ResponseWriter) {
	if rd.Headers["ContentType"] != "" {
		w.Header().Set("Content-Type", rd.Headers["ContentType"])
	}

	if rd.Headers["ContentEncoding"] != "" {
		w.Header().Set("Content-Encoding", rd.Headers["ContentEncoding"])
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(rd.Body)))
	w.WriteHeader(rd.StatusCode)
	w.Write(rd.Body)
}

func jsonError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	data := fmt.Sprintf("{\"error\":%q}\n", message)
	w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	w.WriteHeader(code)
	w.Write([]byte(data))
}

type poster interface {
	post([]byte, string, string) (*responseData, error)
}

type simplePoster struct {
	client   *http.Client
	location string
}

func newSimplePoster(location string, timeout time.Duration, skipTLSVerification bool) *simplePoster {
	// Configure custom transport for http.Client
	// Used for support skip-tls-verification option
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: skipTLSVerification,
		},
	}

	return &simplePoster{
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		location: location,
	}
}

func (b *simplePoster) post(buf []byte, query string, auth string) (*responseData, error) {
	req, err := http.NewRequest("POST", b.location, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	req.URL.RawQuery = query
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if err = resp.Body.Close(); err != nil {
		return nil, err
	}

	// for k, v := range resp.Header {
	//
	//   }
	// Loop through headers

	m := make(map[string]string)

	for name, headers := range resp.Header {
		name = strings.ToLower(name)
		for _, h := range headers {
			m[name] = h
		}
	}

	return &responseData{
		Headers: m,
		//ContentType:        resp.Header.Get("Content-Type"),
		//ContentEncoding:    resp.Header.Get("Content-Encoding"),
		//X-Influxdb-Version: resp.Header.Get("X-Influxdb-Version"),
		StatusCode: resp.StatusCode,
		Body:       data,
	}, nil
}

type httpBackend struct {
	poster
	name string
}

func newHttpQueryBackend(cfg *HTTPQueryConfig) (*httpBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	timeout := DefaultHTTPTimeout
	if cfg.Timeout != "" {
		t, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return nil, fmt.Errorf("error parsing HTTP timeout '%v'", err)
		}
		timeout = t
	}

	// todo use config skipTLSVerification ?
	var p poster = newSimplePoster(cfg.Location, timeout, true)

	return &httpBackend{
		poster: p,
		name:   cfg.Name,
	}, nil
}

func newHTTPBackend(cfg *HTTPOutputConfig) (*httpBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	timeout := DefaultHTTPTimeout
	if cfg.Timeout != "" {
		t, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return nil, fmt.Errorf("error parsing HTTP timeout '%v'", err)
		}
		timeout = t
	}

	var p poster = newSimplePoster(cfg.Location, timeout, cfg.SkipTLSVerification)

	// If configured, create a retryBuffer per backend.
	// This way we serialize retries against each backend.
	if cfg.BufferSizeMB > 0 {
		max := DefaultMaxDelayInterval
		if cfg.MaxDelayInterval != "" {
			m, err := time.ParseDuration(cfg.MaxDelayInterval)
			if err != nil {
				return nil, fmt.Errorf("error parsing max retry time %v", err)
			}
			max = m
		}

		batch := DefaultBatchSizeKB * KB
		if cfg.MaxBatchKB > 0 {
			batch = cfg.MaxBatchKB * KB
		}

		p = newRetryBuffer(cfg.BufferSizeMB*MB, batch, max, p)
	}

	return &httpBackend{
		poster: p,
		name:   cfg.Name,
	}, nil
}

var ErrBufferFull = errors.New("retry buffer full")

// use bufPool may lost data
// var bufPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}

// use bufPool may lost data
func getBuf() *bytes.Buffer {
	return bytes.NewBuffer(make([]byte, 2*KB))
}

// use bufPool may lost data
func putBuf(b *bytes.Buffer) {
	b.Reset()
}
