---
title: Configuration
sidebar_position: 1
---

# Server Configuration

import PartialConfig from '../_configs/_partial_config.mdx';

All configurations can be set in Fluss configuration file `conf/server.yaml`

The configuration is parsed and evaluated when the Fluss processes are started.
Changes to the configuration file require restarting the relevant processes.

Users can organize config in format `key: value`, such as:
```yaml title="conf/server.yaml"
default.bucket.number: 8
default.replication.factor: 3
remote.data.dir: /home/fluss/data
remote.fs.write-buffer-size: 10mb
auto-partition.check.interval: 5min
```

Server configuration refers to a set of configurations used to specify the running
parameters of a server. These settings can only be configured at the time of cluster
startup and do not support dynamic modification during the Fluss cluster working.

<PartialConfig />
