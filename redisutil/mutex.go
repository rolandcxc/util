package redisutil

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

// 释放锁
func delByKeyWhenValueEquals(ctx context.Context, rdb *redis.Client, key string, value interface{}) (bool, error) {
	lua := `
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
else
	return 0
end
`
	scriptKeys := []string{key}

	val, err := rdb.Eval(ctx, lua, scriptKeys, value).Result()
	if err != nil {
		return false, err
	}

	return val == int64(1), nil
}

type Mutex struct {
	cli       *redis.Client
	key       string
	value     int
	closeChan chan struct{}
}

func NewMutex(cli *redis.Client, key string) *Mutex {
	rand.Seed(time.Now().UnixNano())
	return &Mutex{
		cli:       cli,
		key:       key,
		value:     rand.Int(),
		closeChan: make(chan struct{}, 1),
	}
}

var ErrLocked = errors.New("mutex: Locked by another session")

func (m *Mutex) TryLock(ctx context.Context, expiration time.Duration) error {
	set, err := m.cli.SetNX(ctx, m.key, m.value, expiration).Result()
	if err != nil {
		return err
	}

	if !set {
		return ErrLocked
	}

	go m.watchDog(ctx, expiration)

	return nil
}

func (m *Mutex) Unlock(ctx context.Context) {
	del, err := delByKeyWhenValueEquals(ctx, m.cli, m.key, m.value)
	log.Printf("unlock mutex, del=%v, err=%v\n", del, err)
	close(m.closeChan)
}

func (m *Mutex) watchDog(ctx context.Context, expiration time.Duration) {
	for {
		select {
		case <-m.closeChan:
			return
		case <-ctx.Done():
			return
		default:
			for i := 0; i < 3; i++ {
				// 失败重试
				if err := m.cli.PExpire(ctx, m.key, expiration).Err(); err != nil {
					log.Printf("[RedisMutex]PExpire error: %s\n", err)
					time.Sleep(time.Millisecond * 200)
					continue
				}
				break
			}

			time.Sleep(expiration / 2)
		}
	}
}
