package redisutil

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestLimiter(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}

	defer s.Close()

	cli := redis.NewClient(&redis.Options{
		Addr:     s.Addr(),
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	ctx := context.Background()
	Convey("Test1QPS", t, func() {
		limiter := NewLimiter(cli, Every(time.Second), 1, "Test1QPS")
		assert.NotNil(t, limiter)

		assert.True(t, limiter.Allow(ctx), "first access should be allowed")
		assert.False(t, limiter.Allow(ctx), "second access should be rejected")
	})

	Convey("Test1QP2S", t, func() {
		limiter := NewLimiter(cli, Every(2*time.Second), 1, "Test1QP2S")
		assert.NotNil(t, limiter)

		assert.True(t, limiter.Allow(ctx), "first access should be allowed")
		assert.False(t, limiter.Allow(ctx), "second access should be rejected")
		<-time.After(2 * time.Second)
		assert.True(t, limiter.Allow(ctx), "third access should be allowed")
	})

	Convey("Test10QPS", t, func() {
		limiter := NewLimiter(cli, Every(100*time.Millisecond), 10, "Test10QPS")
		assert.NotNil(t, limiter)

		for i := 0; i < 10; i++ {
			assert.True(t, limiter.Allow(ctx), "access should be allowed")
		}
		assert.False(t, limiter.Allow(ctx), "access should be rejected")
	})

	Convey("TestConcurrent10QPS", t, func() {
		var count = 5
		var limiters []*Limiter

		for i := 0; i < count; i++ {
			limiters = append(limiters, NewLimiter(cli, Every(100*time.Millisecond), 10, "TestConcurrent10QPS"))
			assert.NotNil(t, limiters[i])
		}

		var wg sync.WaitGroup
		wg.Add(count)

		var l sync.Mutex
		totalAllows := 0

		for i := 0; i < count; i++ {
			go func(index int) {
				for j := 0; j < 10; j++ {
					if limiters[index].Allow(ctx) {
						l.Lock()
						totalAllows++
						l.Unlock()
					}
				}
				wg.Done()
			}(i)
		}

		wg.Wait()
		assert.Equal(t, 10, totalAllows)
	})
}
