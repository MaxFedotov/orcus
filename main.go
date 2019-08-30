package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
}

var Version, GitCommit string

func main() {
	flagSet := flag.NewFlagSet("orcus", flag.ExitOnError)
	configFile := flagSet.String("config", "/etc/orcus.cnf", "Path to config file")
	debug := flagSet.Bool("debug", false, "Debug mode")
	version := flagSet.Bool("version", false, "Print version")

	flagSet.Parse(os.Args[1:])

	if *version {
		fmt.Println("Version:\t", Version)
		fmt.Println("Git commit:\t", GitCommit)
		os.Exit(0)
	}

	log.SetLevel(log.InfoLevel)
	cancelCh := make(chan struct{})
	config := NewConfig()

	if _, err := toml.DecodeFile(*configFile, &config); err != nil {
		log.WithField("config", *configFile).Fatal("Unable to read configuration file")
	}

	if strings.ToLower(config.General.LogLevel) == "debug" {
		*debug = true
	}

	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	if config.General.LogFile != "" {
		logFile, err := os.OpenFile(config.General.LogFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0640)
		if err != nil {
			log.WithField("log_file", config.General.LogFile).Fatal("Unable to open log file")
		}
		mw := io.MultiWriter(os.Stdout, logFile)
		log.SetOutput(mw)
	}

	startText := "Starting orcus"
	if Version != "" {
		startText += ", version: " + Version
	}
	if GitCommit != "" {
		startText += ", git commit: " + GitCommit
	}

	log.WithFields(log.Fields{"config": *configFile, "debug": *debug}).Info(startText)

	InitHTTPClient(config.General.HTTPTimeout, config.General.SSLSkipVerify)

	consulKV := NewConsulKV(config.Consul.Address, config.Consul.ACLToken, config.General.CacheTTL.Duration)

	if err := config.Validate(consulKV); err != nil {
		log.WithField("error", err).Fatal("Invalid configuration. Exiting")
	}

	if err := consulKV.CreateSession(config.Consul.TTL); err != nil {
		log.WithField("ConsulConfig", fmt.Sprintf("%+v", config.Consul)).Fatal("Unable to create Consul session. Exiting")
	}

	router := mux.NewRouter()

	router.HandleFunc("/sync/{clusterName}", func(w http.ResponseWriter, r *http.Request) {
		ForceSync(w, r, config, consulKV, cancelCh)
	}).Methods("GET")

	router.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		func() {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(&SyncMetrics)
		}()
	}).Methods("GET")

	go ContinuousSync(config, consulKV, cancelCh)

	log.WithField("ListenAddress", config.General.ListenAddress).Info("Starting HTTP listener")
	if err := http.ListenAndServe(config.General.ListenAddress, router); err != nil {
		log.WithField("ListenAddress", config.General.ListenAddress).Fatal("Unable to start HTTP listener")
	}
}
