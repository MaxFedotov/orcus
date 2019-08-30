package main

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/gorilla/mux"
	"github.com/patrickmn/go-cache"
	log "github.com/sirupsen/logrus"
)

type Metrics struct {
	LastSyncTime        time.Time
	LastSyncTS          int64
	SyncDurationSeconds float64
	SyncedClusters      uint64
	SyncErrors          uint64
	sync.RWMutex
}

var SyncMetrics Metrics

func ContinuousSync(config *Config, consulKV *ConsulKV, cancelCh <-chan struct{}) {
	log.WithField("syncInterval", config.General.SyncInterval.Duration).Info("Starting continuous synchronization process")
	ticker := time.NewTicker(config.General.SyncInterval.Duration)
	for ; true; <-ticker.C {
		Sync(config, "", consulKV, cancelCh)
	}
}

func ForceSync(w http.ResponseWriter, r *http.Request, config *Config, consulKV *ConsulKV, cancelCh chan struct{}) {
	vars := mux.Vars(r)
	if err := consulKV.lock.Unlock(); err == nil {
		cancelCh <- struct{}{}
	}
	Sync(config, vars["clusterName"], consulKV, cancelCh)
}

func Sync(config *Config, clusterName string, consulKV *ConsulKV, cancelCh <-chan struct{}) {
	if clusterName == "" {
		log.Info("Starting scheduled sync")
	} else {
		log.Info("Starting force sync")
	}
	consulKV.Lock()
	defer consulKV.Unlock()
	if isRunningOnOrchestratorLeader(config.Orchestrator.URL) {
		started := time.Now()
		var syncErrors, syncedClusters uint64
		var clusters []string
		releasedCh := make(<-chan struct{})
		defer func(started time.Time) {
			finished := time.Now()
			SyncMetrics.Lock()
			SyncMetrics.LastSyncTime = finished
			SyncMetrics.LastSyncTS = finished.Unix()
			SyncMetrics.SyncDurationSeconds = finished.Sub(started).Seconds()
			SyncMetrics.SyncedClusters = syncedClusters
			SyncMetrics.SyncErrors = syncErrors
			SyncMetrics.Unlock()
		}(started)

		// Trying to get consul lock
		ticker := time.NewTicker(config.Consul.RetryInterval.Duration)
		for ; true; <-ticker.C {
			var err error
			if se, _, err := consulKV.client.Session().Info(consulKV.sessionID, nil); err != nil || se == nil {
				err := consulKV.RecreateSession(config.Consul.TTL)
				if err != nil {
					log.WithFields(log.Fields{"RetryInterval": config.Consul.RetryInterval.Duration, "error": err}).Warning("Unable to create Consul session, retrying")
					continue
				}
			}
			releasedCh, err = consulKV.CreateLock(config.Consul.KVPrefix)
			if err != nil {
				log.WithFields(log.Fields{"RetryInterval": config.Consul.RetryInterval.Duration, "error": err}).Warning("Unable to create lock in Consul, retrying")
				continue
			}
			break
		}

		stopCh := make(chan struct{})
		doneCh := make(chan struct{})
		wp := workerpool.New(config.General.Threads)
		go func() {
			log.Debug("Started termination watcher routine")
			select {
			case <-cancelCh:
				log.Info("Got external termination signal. Stopping sync process.")
				close(stopCh)
			case <-releasedCh:
				log.WithField("SessionID", consulKV.sessionID).Error("Consul lock invalidated. Stopping sync process.")
				close(stopCh)
			case <-doneCh:
			}
			log.Debug("Stopping termination watcher routine")
			return
		}()
		// Scheduled sync
		if clusterName == "" {
			var err error
			clusters, err = GetClusters(config.Orchestrator.URL)
			if err != nil {
				log.WithFields(log.Fields{"orchestratorURL": config.Orchestrator.URL, "error": err}).Error("Unable to get information about clusters from Orchestrator")
				close(doneCh)
				consulKV.lock.Unlock()
				return
			}
			// Force sync
		} else {
			clusters = append(clusters, clusterName)
			log.WithField("ForceSyncDelay", config.Orchestrator.ForceSyncDelay.Duration.String()).Debug("Waiting before starting force sync")
			time.Sleep(config.Orchestrator.ForceSyncDelay.Duration)
		}
		if config.Orchestrator.SubmitMastersToConsul {
			if err := SubmitMastersToConsul(config.Orchestrator.URL, clusterName); err != nil {
				log.WithField("error", err).Warning("Unable to submit clusters masters to Consul")
			}
		}
		log.WithField("clusters", clusters).Info("Submitting clusters to workers for processing")
		for _, cluster := range clusters {
			cluster := cluster
			wp.Submit(func() {
				select {
				case <-stopCh:
					return
				default:
					consulKVkey := fmt.Sprintf(config.Consul.KVPrefix + "/" + cluster + "/replicas")
					replicas, err := GetReplicas(config.Orchestrator.URL, cluster)
					if err != nil {
						log.WithFields(log.Fields{"cluster": cluster, "error": err}).Error("Unable to get information about cluster replicas from Orchestrator")
						atomic.AddUint64(&syncErrors, 1)
						return
					}
					cacheReplicas, foundInCache := consulKV.cache.Get(cluster)
					if !foundInCache {
						log.WithFields(log.Fields{"cluster": cluster, "replicas": replicas}).Debug("Cluster not found in cache. Adding to cache")
						consulKV.cache.Set(cluster, replicas, cache.DefaultExpiration)

						if replicas == "" {
							log.WithFields(log.Fields{"cluster": cluster, "replicas": replicas}).Info("Cluster sync completed. No replicas found")
							return
						}

						consulReplicas, foundInConsul, err := consulKV.Get(consulKVkey)
						if err != nil {
							log.WithFields(log.Fields{"cluster": cluster, "error": err}).Error("Unable to get information about cluster replicas from Consul")
							atomic.AddUint64(&syncErrors, 1)
							return
						}

						if !foundInConsul || consulReplicas != replicas {
							if err := consulKV.Put(consulKVkey, replicas); err != nil {
								log.WithFields(log.Fields{"cluster": cluster, "error": err}).Error("Unable to save cluster to Consul")
								atomic.AddUint64(&syncErrors, 1)
								return
							}
							log.WithFields(log.Fields{"cluster": cluster, "replicas": replicas, "consulReplicas": consulReplicas}).Info("Cluster sync completed. Updated Consul")
							atomic.AddUint64(&syncedClusters, 1)
							return
						}

						log.WithFields(log.Fields{"cluster": cluster, "replicas": replicas, "consulReplicas": consulReplicas}).Info("Cluster sync completed. No diffs found")
						return
					}
					if cacheReplicas.(string) != replicas {
						if replicas == "" {
							if err := consulKV.Delete(consulKVkey); err != nil {
								log.WithFields(log.Fields{"cluster": cluster, "error": err}).Error("Unable to delete cluster from Consul")
								atomic.AddUint64(&syncErrors, 1)
								return
							}
							log.WithFields(log.Fields{"cluster": cluster, "replicas": replicas, "consulReplicas": cacheReplicas.(string)}).Info("Cluster sync completed. Deleted from Consul")
							atomic.AddUint64(&syncedClusters, 1)
						} else {
							if err := consulKV.Put(consulKVkey, replicas); err != nil {
								log.WithFields(log.Fields{"cluster": cluster, "error": err}).Error("Unable to save cluster to Consul")
								atomic.AddUint64(&syncErrors, 1)
								return
							}
							log.WithFields(log.Fields{"cluster": cluster, "replicas": replicas, "consulReplicas": cacheReplicas.(string)}).Info("Cluster sync completed. Updated consul")
							atomic.AddUint64(&syncedClusters, 1)
						}

						consulKV.cache.Set(cluster, replicas, cache.DefaultExpiration)
						log.WithFields(log.Fields{"cluster": cluster, "replicas": replicas, "cacheReplicas": cacheReplicas.(string)}).Debug("Cluster replicas differs from cache. Updating cache")
						return
					}
					log.WithFields(log.Fields{"cluster": cluster, "replicas": replicas, "consulReplicas": cacheReplicas.(string)}).Info("Cluster sync completed. No diffs found")
					return
				}
			})
		}
		wp.StopWait()
		close(doneCh)
		consulKV.lock.Unlock()
		log.WithField("clusters", clusters).Info("Sync completed")
	} else {
		log.Info("Current node is not Orchestrator leader")
	}
	return
}

func isRunningOnOrchestratorLeader(OrchestratorURL string) bool {
	leaderCheckURL, err := url.Parse(OrchestratorURL + "/api/leader-check")
	log.WithFields(log.Fields{"leaderCheckURL": leaderCheckURL}).Debug("Checking if Orchestrator node is a leader")
	if err != nil {
		log.WithFields(log.Fields{"leaderCheckURL": leaderCheckURL, "error": err}).Error("Unable to parse Orchestrator leader-check URL")
		return false
	}
	resp, err := httpClient.Get(leaderCheckURL.String())
	if err != nil {
		log.WithFields(log.Fields{"error": err, "OrchestratorURL": OrchestratorURL}).Error("Unable to get response from orchestrator")
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true
	}
	return false
}
