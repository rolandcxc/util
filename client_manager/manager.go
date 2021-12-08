package client_manager

import (
	"context"
	"sync"
)

// ClientManager 是一个用来管理Client的工具。例如当应用程序中需要访问某服务的多个集群，不同集群都需要初始化一个客户端时。
// ClientManager 可以用来方便地初始化和管理这些客户端，并保证并发条件下对该服务相同的集群只初始化一个客户端。
type ClientManager struct {
	f       InitFunc
	clients sync.Map
}

// InitFunc 用来初始化一个服务的Client。
type InitFunc func(ctx context.Context, key string) (interface{}, error)

func New(f InitFunc) *ClientManager {
	return &ClientManager{
		f:       f,
		clients: sync.Map{},
	}
}

func (cm *ClientManager) GetClient(ctx context.Context, key string) (interface{}, error) {
	var helper *clientItem

	load, ok := cm.clients.Load(key)
	if ok {
		helper = load.(*clientItem)
	} else {
		store, _ := cm.clients.LoadOrStore(key, cm.newClientItem(key))
		helper = store.(*clientItem)
	}

	value, done := helper.getValue()
	if done {
		return value, nil
	}

	return helper.initClient(ctx)

}

func (cm *ClientManager) newClientItem(key string) *clientItem {
	return &clientItem{
		key:   key,
		value: nil,
		mu:    sync.RWMutex{},
		f:     cm.f,
		done:  false,
	}
}

type clientItem struct {
	key   string
	value interface{}
	mu    sync.RWMutex
	f     InitFunc
	done  bool
}

func (i *clientItem) initClient(ctx context.Context) (interface{}, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.done {
		return i.value, nil
	}

	v, err := i.f(ctx, i.key)
	if err != nil {
		return nil, err
	}

	i.value = v
	i.done = true

	return i.value, nil
}

func (i *clientItem) getValue() (interface{}, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.value, i.done
}
