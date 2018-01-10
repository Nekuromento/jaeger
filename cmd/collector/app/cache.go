package app

import (
	"fmt"
	"sync"
	"time"

	"github.com/jaegertracing/jaeger/model"
)

type decision int

const (
	unknown decision = iota
	filter
	store
)

const (
	cacheCleanupFrequency = 1 * time.Minute
)

type cacheItem struct {
	time     time.Time
	decision decision
	spans    []*model.Span
}

func (i cacheItem) expired() bool {
	return time.Now().After(i.time)
}

func isParent(span *model.Span) bool {
	return span.ParentSpanID == 0
}

func isSlow(span *model.Span, threshold time.Duration) bool {
	return span.Duration >= threshold
}

type cache struct {
	sync.Map
	ttl       time.Duration
	threshold time.Duration
}

func cleanupExpiredItems(cache *cache) {
	//TODO: add loop break
	for {
		fmt.Println("Spleeping")
		time.Sleep(cacheCleanupFrequency)

		var deleting []interface{}

		cache.Range(func(key, value interface{}) bool {
			item := value.(cacheItem)

			if item.expired() {
				deleting = append(deleting, key)
			}

			return true
		})

		if len(deleting) > 0 {
			go func() {
				fmt.Println("Deleting:", len(deleting))
				fmt.Println()
				for _, key := range deleting {
					cache.Delete(key)
				}
			}()
		}
	}
}

func NewFilterCache(ttl, threshold time.Duration) SpanFilter {
	cache := &cache{ttl: ttl, threshold: threshold}

	go cleanupExpiredItems(cache)

	return cache
}

func (c *cache) FilterSpans(spans []*model.Span) []*model.Span {
	results := make([]*model.Span, 0, len(spans))

	fmt.Println()
	for _, span := range spans {
		item := cacheItem{time: time.Now().Add(c.ttl)}

		if value, found := c.Load(span.TraceID.String()); found {
			item = value.(cacheItem)
		}

		if isSlow(span, c.threshold) {
			item.decision = store
		} else if isParent(span) {
			item.decision = filter
		}

		if item.decision == unknown {
			item.spans = append(item.spans, span)
		}

		if isParent(span) || isSlow(span, c.threshold) {
			results = append(results, item.spans...)
			item.spans = nil
		}

		c.Store(span.TraceID.String(), item)

		if item.decision == store {
			results = append(results, span)
		}

		if item.decision != unknown {
			fmt.Println("Span: id=", span.TraceID, " : ", span.SpanID, " duration=", span.Duration, " ", span.OperationName)
			fmt.Println("Item: parent=", isParent(span), " slow=", isSlow(span, c.threshold), " status=", item.decision, " expired=", item.expired(), " spans=", len(item.spans))
		}
	}
	fmt.Println()
	fmt.Println("Passed filter", len(results), "/", len(spans))
	fmt.Println()

	return results
}
