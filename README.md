# orcus
Orchestrator to Consul Synchronization tool

## Features
- Synchronizes replicas (and optionally masters) hosts from Orchestrator to Consul on configured interval
- Provides endpoint (/sync/{cluster_name}), which can be used in Orchestrator hooks to sync cluster replicas to Consul during failovers
- Provides status endpoint (/metrics), which can be used by different monitoring tools for alerting
- All replicas hosts for cluster are written under __{kv_prefix}/{cluster_alias}/replicas__ key

## Download
- Get the latest binary\rpm\deb package from the [releases](https://github.com/MaxFedotov/orcus/releases)
- Or get and complile from sources
```shell
go get github.com/MaxFedotov/orcus
```

## Puppet
[puppet-orcus](https://github.com/MaxFedotov/puppet-orcus) module can be used in order to automate Orcus installation and configuration

## Install notes
To provide high availability for Orcus, it should be installed on the same hosts where Orchestrator is installed. Before performing sync process it will check, if current host is Orchestrator leader using Orchestrator API, and if not - it will sleep till next sync interval. It also uses Consul distributed locks as additional way to provide mutually exclusive access to Consul KV for single Orcus instance.

## Usage
```
Usage of orcus:
  -config string
        Path to config file (default "/etc/orcus.cnf")
  -debug
        Debug mode
  -version
        Print version
```

## Configuration
```
[general]
listen_address = "127.0.0.1:3008"      # Address where Orcus HTTP should listen for connections
log_file = "/var/log/orcus/orcus.log"  # Path to Orcus log file
log_level = "info"                     # Log level (debug|info)
sync_interval = "10m"                  # Interval between scheduled sync to Consul
ssl_skip_verify = true                 # Ignore SSL certification error when using SSL
http_timeout = "5s"                    # Timeout for HTTP connections to Orchestrator and Consul
threads = 5                            # Number of gourutines used to load data to Consul
cache_ttl = "24h"                      # Orcus uses local cache to store data about replicas. This setting sets TTL for cache keys


[orchestrator]
url = "http://localhost:3000"          # Orchestrator URL
force_sync_delay = "5s"                # When using /sync/{cluster_name} endpoint in Orchestrator PostFailover hooks, it takes some time for Orchestrator to rebuild cluster topology. This setting allows to wait for configured time delay before starting cluster replicas sychronization process.
submit_masters_to_consul = true        # Call Orchestrator API to also sumbit masters information to Consul during synchronization process

[consul]
address = "127.0.0.1:8500"             # Consul address
acl_token = ""                         # Consul ACL token
kv_prefix = "db/mysql"                 # Consul KV prefix, where replicas information should be stored
lock_ttl = "1m"                        # When loading data to Consul, Orcus creates lock in it in order to guarantee than only single Orcus instance will be able to update data in Consul KV. This variable defines TTL for this lock
retry_interval = "5s"                  # Retry interval for Consul connection errors
```

## Orchestrator configuration
In order to update replicas information in Consul after Orchestrator failover add following to Orchestrator configuration file
```json
    'PostFailoverProcesses' => [
      "curl -X GET localhost:3008/sync/{failureClusterAlias} &"
    ],
```

