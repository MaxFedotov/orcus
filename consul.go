package main

import (
	"fmt"
	"sync"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/patrickmn/go-cache"
	log "github.com/sirupsen/logrus"
)

// ConsulKV holds client configuration and cache for data
type ConsulKV struct {
	client    *consulapi.Client
	sessionID string
	renewCh   chan struct{}
	lock      *consulapi.Lock
	cache     *cache.Cache
	sync.Mutex
}

// NewConsulKV constructs a new Consul client with cache
func NewConsulKV(consulAddress string, consulToken string, cacheTTL time.Duration) *ConsulKV {
	consulKV := &ConsulKV{
		cache: cache.New(cacheTTL, cacheTTL),
	}
	log.WithFields(log.Fields{"ConsulAddress": consulAddress, "ConsulToken": consulToken}).Debug("Initializing consul client")

	consulConfig := consulapi.DefaultConfig()
	consulConfig.Address = consulAddress
	consulConfig.Token = consulToken
	if client, err := consulapi.NewClient(consulConfig); err != nil {
		log.WithFields(log.Fields{"consulConfig.Address": consulAddress, "consulConfig.Token": consulToken}).Fatal("Unable to initialize consul client")
	} else {
		consulKV.client = client
	}
	return consulKV
}

// CreateSession creates new Consul session
func (c *ConsulKV) CreateSession(ttl Duration) (err error) {
	sessionConfig := &consulapi.SessionEntry{Name: "orcus", Behavior: "release", LockDelay: 1 * time.Second, TTL: ttl.Duration.String()}
	c.sessionID, _, err = c.client.Session().CreateNoChecks(sessionConfig, nil)
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{"SessionConfiguration": fmt.Sprintf("%+v", sessionConfig), "SessionID": c.sessionID}).Info("Created Consul session")
	c.renewCh = make(chan struct{})
	go func() {
		sessionID := c.sessionID
		log.WithFields(log.Fields{"initialTTL": ttl.Duration.String(), "SessionID": sessionID}).Debug("Started Consul RenewPeriodic routine")
		c.client.Session().RenewPeriodic(ttl.Duration.String(), sessionID, nil, c.renewCh)
		log.WithFields(log.Fields{"SessionID": sessionID}).Debug("Stopped Consul RenewPeriodic routine")
	}()
	return nil
}

func (c *ConsulKV) RecreateSession(ttl Duration) (err error) {
	log.WithField("sessionID", c.sessionID).Warning("Cannot find Consul session. Trying to recreate.")
	if c.sessionID != "" {
		close(c.renewCh)
	}
	c.sessionID = ""
	return c.CreateSession(ttl)
}

func (c *ConsulKV) CreateLock(consulKVPrefix string) (lostCh <-chan struct{}, err error) {
	lockKey := fmt.Sprintf("%s/.lock", consulKVPrefix)
	lockOpts := &consulapi.LockOptions{Key: lockKey, Value: []byte("ok"), Session: c.sessionID, LockWaitTime: 1 * time.Second}
	lock, _ := c.client.LockOpts(lockOpts)
	log.WithField("lockOpts", lockOpts).Debug("Creating lock in Consul")
	lostCh, err = lock.Lock(nil)
	if err != nil {
		return nil, err
	}
	c.lock = lock
	return lostCh, nil
}

func (c *ConsulKV) Get(key string) (value string, found bool, err error) {
	consulPair, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return value, found, err
	}
	if consulPair == nil {
		return value, found, nil
	}
	return string(consulPair.Value), (consulPair != nil), nil
}

func (c *ConsulKV) Put(key string, value string) error {
	pair := &consulapi.KVPair{Key: key, Value: []byte(value)}
	_, err := c.client.KV().Put(pair, nil)
	return err
}

func (c *ConsulKV) Delete(key string) error {
	_, err := c.client.KV().Delete(key, nil)
	return err
}
