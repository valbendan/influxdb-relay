package relay

import (
	"time"
	"net/http"
	"log"
	"bytes"
)

const (
	retryInitial    = 500 * time.Millisecond
	retryMultiplier = 2
)

type Operation func() error

type batchPoints struct {
	Query string
	Auth  string
	Data  []byte
}

type cachedPoints struct {
	Query   string
	Auth    string
	Buf     bytes.Buffer
	BufSize int
	Time    time.Time // create time of this cachedPoints
}

// Buffers and retries operations, if the buffer is full operations are dropped.
// Only tries one operation at a time, the next operation is not attempted
// until success or timeout of the previous operation.
// There is no delay between attempts of different operations.
type retryBuffer struct {
	initialInterval time.Duration
	multiplier      time.Duration
	maxInterval     time.Duration

	maxBuffered int
	maxBatch    int

	cachedItems []*cachedPoints // cachedPoints
	itemChan    chan batchPoints

	p poster
}

func newRetryBuffer(size, batch int, max time.Duration, p poster) *retryBuffer {
	r := &retryBuffer{
		initialInterval: retryInitial,
		multiplier:      retryMultiplier,
		maxInterval:     max,
		maxBuffered:     size,
		maxBatch:        batch,
		p:               p,
		cachedItems:     make([]*cachedPoints, 0),
		itemChan:        make(chan batchPoints, 10000),
	}
	go r.run()
	return r
}

func (r *retryBuffer) post(buf []byte, query string, auth string) (*responseData, error) {

	// direct pass to chan
	r.itemChan <- batchPoints{
		Query: query,
		Auth:  auth,
		Data:  buf,
	}

	return &responseData{
		StatusCode: http.StatusNoContent,
		Body:       []byte{},
	}, nil
}

func (r *retryBuffer) run() {
	addToCache := func(points *batchPoints) {
		addToCachedFlag := false
		for _, cached := range r.cachedItems {
			if cached.Auth == points.Auth && cached.Query == points.Query {
				cached.Buf.Write(points.Data)
				cached.BufSize += len(points.Data)
				addToCachedFlag = true
				break
			}
		}
		if addToCachedFlag == false {
			cached := cachedPoints{
				Query:   points.Query,
				Auth:    points.Auth,
				Time:    time.Now(),
				Buf:     *bytes.NewBuffer([]byte{}),
				BufSize: 0,
			}
			r.cachedItems = append(r.cachedItems, &cached)
		}
	}

	postToInfluxDB := func(data []byte, query string, auth string) {
		interval := r.initialInterval
		maxInterval := r.maxInterval
		for {
			resp, err := r.p.post(data, query, auth)
			if err == nil && resp.StatusCode/100 != 5 {
				log.Print("send data: ", len(data))
				break
			} else if interval >= maxInterval {
				// resp.StatusCode == 5xx
				// this prevent the forever loop of InfluxDB server return 5xx
				log.Print("lost data: ", string(data))
				break
			}

			if interval <= maxInterval {
				interval *= r.multiplier
				if interval > maxInterval {
					interval = maxInterval
				}
			}

			time.Sleep(interval)
		}
	}

	for {
		select {
		case points := <-r.itemChan:
			addToCache(&points)
		default:
			time.Sleep(10 * time.Millisecond)
		}

		for index, cached := range r.cachedItems {
			nowTime := time.Now()
			if nowTime.Sub(cached.Time) > r.maxInterval || cached.BufSize > r.maxBuffered {
				// remove cached from r.cachedItems
				r.cachedItems = append(r.cachedItems[:index], r.cachedItems[index+1:]...)

				// send removed item to InfluxDB server
				postToInfluxDB(cached.Buf.Bytes(), cached.Query, cached.Auth)
			}
		}
	}
}
