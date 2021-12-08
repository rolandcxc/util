package id_generator

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdmain"
)

func TestIDGenerator(t *testing.T) {
	go etcdmain.Main([]string{"etcd"})
	time.Sleep(time.Second * 2)
	num := 10
	generators := make([]*Generator, num)

	cli, err := v3.New(v3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		panic(err)
	}

	for i := 0; i < num; i++ {
		generators[i] = NewIDGenerator(cli, "test", fmt.Sprintf("node-%d", i))
	}

	rand.Seed(time.Now().Unix())
	var sg sync.WaitGroup
	for i := 0; i < num; i++ {
		sg.Add(1)
		go func(index int) {
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			generators[index].Init(context.Background())
			sg.Done()
		}(i)
	}
	sg.Wait()

	for i := 0; i < num; i++ {
		id := generators[i].GetStringID()
		assert.NotEmpty(t, id)
	}

	checkID := map[int64]bool{}
	for i := 0; i < num; i++ {
		exist := checkID[generators[i].nodeID]
		assert.Equal(t, false, exist)
		checkID[generators[i].nodeID] = true
	}
}
