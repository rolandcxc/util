package redisutil

import (
	"context"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestMutex(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	key := "testMutex" + strconv.Itoa(int(time.Now().Unix()))
	var (
		count int32
		sg    = sync.WaitGroup{}
	)

	cli := redis.NewClient(&redis.Options{
		Addr:     s.Addr(),
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		sg.Add(1)
		go func(index int) {
			defer sg.Done()
			mutex := NewMutex(cli, key)
			if err := mutex.TryLock(ctx, time.Second*5); err != nil {
				log.Printf("%d: %s", index, err)
				return
			}
			log.Printf("%d: get mutex", index)
			atomic.AddInt32(&count, 1)
			time.Sleep(time.Second * 10)
			mutex.Unlock(context.Background())
		}(i)
	}

	time.Sleep(time.Second * 6)
	assert.Equal(t, count, int32(1))

	for i := 0; i < 10; i++ {
		sg.Add(1)
		go func(index int) {
			defer sg.Done()
			mutex := NewMutex(cli, key)
			err := mutex.TryLock(ctx, time.Second*10)
			assert.NotEqual(t, err, nil)
			log.Printf("%d: %s", index, err)
		}(i)
	}

	sg.Wait()
	mutex := NewMutex(cli, key)
	err = mutex.TryLock(context.Background(), time.Second*10)
	assert.Equal(t, err, nil)
	mutex.Unlock(context.Background())
}
