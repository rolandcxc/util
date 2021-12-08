package client_manager

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClientManager(t *testing.T) {
	expect := sync.Map{}
	cm := New(func(ctx context.Context, key string) (interface{}, error) {
		tmp := key
		fmt.Printf("create client for %s", key)
		_, loaded := expect.LoadOrStore(key, &tmp)
		if loaded {
			panic("init twice")
		}
		return &tmp, nil
	})

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			rand.Seed(time.Now().UnixNano())
			key := fmt.Sprintf("%d", rand.Intn(10))
			v, _ := cm.GetClient(context.Background(), key)
			client := v.(*string)
			load, ok := expect.Load(key)
			assert.Equal(t, true, ok)
			assert.Equal(t, load, client)
			wg.Done()
		}()
	}
	wg.Wait()
}
