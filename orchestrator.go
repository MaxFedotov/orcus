package main

import (
	"encoding/json"
	"net/url"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
)

type ClusterInfo struct {
	ClusterName string `json:"ClusterName"`
}

func GetClusters(orchestratorURL string) (clusters []string, err error) {
	var clustersData []map[string]interface{}
	clustersInfoURL, err := url.Parse(orchestratorURL + "/api/clusters-info")
	log.WithField("clustersInfoURL", clustersInfoURL).Debug("Getting information about clusters from Orchestrator")
	if err != nil {
		return clusters, err
	}
	resp, err := httpClient.Get(clustersInfoURL.String())
	if err != nil {
		return clusters, err
	}
	defer resp.Body.Close()

	json.NewDecoder(resp.Body).Decode(&clustersData)
	for _, cluster := range clustersData {
		clusters = append(clusters, cluster["ClusterAlias"].(string))
	}
	return clusters, nil
}

func GetReplicas(orchestratorURL string, clusterName string) (string, error) {
	log.WithField("clusterName", clusterName).Debug("Getting information about cluster replicas from Orchestrator")
	var replicasData []map[string]interface{}
	var replicas []string
	replicasInfoURL, err := url.Parse(orchestratorURL + "/api/cluster/" + clusterName)
	if err != nil {
		return "", err
	}
	resp, err := httpClient.Get(replicasInfoURL.String())
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	json.NewDecoder(resp.Body).Decode(&replicasData)
	if len(replicasData) == 1 {
		return "", nil
	}
	for _, replica := range replicasData {
		if replica["MasterKey"].(map[string]interface{})["Hostname"].(string) != "" {
			if int(replica["ReplicationSQLThreadState"].(float64)) == 1 && int(replica["ReplicationIOThreadState"].(float64)) == 1 {
				replicas = append(replicas, replica["Key"].(map[string]interface{})["Hostname"].(string))
			}
		}
	}
	sort.Strings(replicas)
	return strings.Join(replicas, ","), nil
}

func SubmitMastersToConsul(orchestratorURL string, clusterName string) error {
	log.WithField("clusterName", clusterName).Debug("Submiting clusters masters to Consul")
	submitMastersUrl, err := url.Parse(orchestratorURL + "/api/submit-masters-to-kv-stores/" + clusterName)
	if err != nil {
		return err
	}
	resp, err := httpClient.Get(submitMastersUrl.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
