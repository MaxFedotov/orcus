package main

import (
	"fmt"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// Config holds Configuration Options that can be set by Config File
type Config struct {
	General      GeneralConfig      `toml:"general"`
	Orchestrator OrchestratorConfig `toml:"orchestrator"`
	Consul       ConsulConfig       `toml:"consul"`
}

type GeneralConfig struct {
	ListenAddress string   `toml:"listen_address"`
	LogFile       string   `toml:"log_file"`
	LogLevel      string   `toml:"log_level"`
	SyncInterval  Duration `toml:"sync_interval"`
	SSLSkipVerify bool     `toml:"ssl_skip_verify"`
	HTTPTimeout   Duration `toml:"http_timeout"`
	Threads       int      `toml:"threads"`
	CacheTTL      Duration `toml:"cache_ttl"`
}

type OrchestratorConfig struct {
	URL                   string   `toml:"url"`
	ForceSyncDelay        Duration `toml:"force_sync_delay"`
	SubmitMastersToConsul bool     `toml:"submit_masters_to_consul"`
}

type ConsulConfig struct {
	Address       string   `toml:"address"`
	ACLToken      string   `toml:"acl_token"`
	KVPrefix      string   `toml:"kv_prefix"`
	TTL           Duration `toml:"lock_ttl"`
	RetryInterval Duration `toml:"retry_interval"`
}

// NewConfig constructs a new Config with default values
func NewConfig() *Config {
	return &Config{
		General: GeneralConfig{
			ListenAddress: "127.0.0.1:3008",
			LogFile:       "/var/log/orcus/orcus.log",
			LogLevel:      "info",
			SyncInterval: Duration{
				10 * time.Minute,
			},
			SSLSkipVerify: true,
			HTTPTimeout: Duration{
				10 * time.Second,
			},
			Threads: 5,
			CacheTTL: Duration{
				24 * time.Hour,
			},
		},
		Orchestrator: OrchestratorConfig{
			URL: "http://127.0.0.1:3000",
			ForceSyncDelay: Duration{
				0 * time.Second,
			},
			SubmitMastersToConsul: true,
		},
		Consul: ConsulConfig{
			Address:  "127.0.0.1:8513",
			ACLToken: "",
			KVPrefix: "db/mysql",
			TTL: Duration{
				5 * time.Minute,
			},
			RetryInterval: Duration{
				5 * time.Second,
			},
		},
	}
}

// Validate check connections to Consul and Orchestrator
func (c *Config) Validate(consulKV *ConsulKV) error {
	log.WithFields(log.Fields{"config": fmt.Sprintf("%+v", c)}).Debug("Validating configuration")
	resp, err := httpClient.Get(c.Orchestrator.URL)
	if err != nil {
		return fmt.Errorf("Unable to connect to Orchestrator: %+v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Unable to connect to Orchestrator: %s", resp.Status)
	}
	_, err = consulKV.client.Status().Peers()
	if err != nil {
		return fmt.Errorf("Unable to connect to Consul: %s", err)
	}
	return nil
}
