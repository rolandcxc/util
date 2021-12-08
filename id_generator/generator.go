package id_generator

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/bwmarrin/snowflake"
	v3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	nodeIDLimit = 1 << 10
)

type Generator struct {
	groupName  string
	cli        *v3.Client
	nodeName   string
	nodeID     int64
	snow       *snowflake.Node
	session    *concurrency.Session
	initNotify chan struct{}
}

func NewIDGenerator(cli *v3.Client, groupName, nodeName string) *Generator {
	return &Generator{
		cli:        cli,
		groupName:  groupName,
		nodeName:   nodeName,
		nodeID:     0,
		snow:       nil,
		session:    nil,
		initNotify: make(chan struct{}, 1),
	}
}

func (n *Generator) prefixMutex() string {
	return fmt.Sprintf("/%s/nodeIDMutex", n.groupName)
}
func (n *Generator) prefixNodeID() string {
	return fmt.Sprintf("/%s/nodeID/", n.groupName)
}

func (n *Generator) prefixMaxNodeID() string {
	return fmt.Sprintf("/%s/nodeIDMax", n.groupName)
}

func (n *Generator) run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wait.UntilWithContext(ctx, n.start, time.Second*3)
}

func (n *Generator) start(ctx context.Context) {
	defer func() {
		if e := recover(); e != nil {
			n.reset()
			log.Printf("[IDGenerator][Panic] when starting ID Generator, err: %s, Stack: %s", e, debug.Stack())
		}
	}()

	var needNotify bool
	if n.snow == nil {
		needNotify = true
	}

	err := n.register(ctx)
	if err != nil {
		log.Printf("[IDGenerator]register node err: %s", err)
		n.reset()
		return
	}

	n.snow, err = snowflake.NewNode(n.nodeID)
	if err != nil {
		log.Printf("[IDGenerator]init globalSnow err: %s, nodeID: %d", err, n.nodeID)
		n.reset()
		return
	}

	if needNotify {
		n.initNotify <- struct{}{}
	}

	log.Printf("register succeed, nodeID: %d", n.nodeID)
	for {
		select {
		case <-n.session.Done():
			log.Printf("[IDGenerator]session closed, nodeID: %d", n.nodeID)
			n.reset()
			return
		case <-ctx.Done():
			log.Printf("stopped by signal")
			n.reset()
			return
		}
	}
}

func (n *Generator) register(ctx context.Context) (e error) {
	s, err := concurrency.NewSession(n.cli, concurrency.WithTTL(10))
	if err != nil {
		return err
	}

	mutex := concurrency.NewMutex(s, n.prefixMutex())
	if err := mutex.Lock(ctx); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(ctx); err != nil {
			e = err
		}
	}()

	getRes, err := s.Client().Get(ctx, n.prefixMaxNodeID())
	if err != nil {
		return err
	}
	maxStr := "0"
	if getRes.Count > 0 {
		maxStr = string(getRes.Kvs[0].Value)
	}

	maxID, err := strconv.ParseInt(maxStr, 10, 64)
	if err != nil {
		return err
	}

	nodeID := (maxID + 1) % nodeIDLimit
	k := fmt.Sprintf("%s%d", n.prefixNodeID(), nodeID)

	log.Printf("try to register nodeID: %d for node: %s", nodeID, n.nodeName)
	txRes, err := s.Client().Txn(ctx).
		If(v3.Compare(v3.CreateRevision(k), "=", 0)).
		Then(v3.OpPut(k, n.nodeName, v3.WithLease(s.Lease())), v3.OpPut(n.prefixMaxNodeID(), strconv.FormatInt(nodeID, 10))).
		Commit()
	if err != nil {
		return err
	}

	if !txRes.Succeeded {
		return fmt.Errorf("failed to register nodeID: %d, resp: %v", nodeID, txRes)
	}

	n.nodeID = nodeID
	n.session = s

	return nil
}

func (n *Generator) reset() {
	if n.session != nil {
		n.session.Close()
		n.session = nil
	}

	n.nodeID = 0
}

func (n *Generator) Init(ctx context.Context) {
	go n.run(ctx)
	<-n.initNotify
}

func (n *Generator) GetStringID() string {
	id := n.snow.Generate()
	return id.String()
}

func (n *Generator) GetUintID() uint64 {
	id := n.snow.Generate()
	return uint64(id)
}
