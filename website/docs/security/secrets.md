---
sidebar_label: Secrets in Configuration
title: Secrets in Configuration
sidebar_position: 4
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Secrets in Configuration

Configuration values such as credentials for lake catalogs or object storage do not need to be
written into `server.yaml` (or client configuration) as literals. Instead, a value can be an
**indirection marker** that points at an external source, and Fluss resolves it at load time:

```yaml title="conf/server.yaml"
config.providers: directory,env
config.providers.directory.param.allowed.paths: /etc/fluss/secrets

# read from the file /etc/fluss/secrets/s3-secret-key
datalake.paimon.s3.secret-key: ${directory:/etc/fluss/secrets:s3-secret-key}
# read from an environment variable
datalake.paimon.s3.access-key: ${env:PAIMON_S3_ACCESS_KEY}
```

This keeps the configuration file free of secret material, so it can be checked into version
control or shipped as a plain Kubernetes ConfigMap while the secrets live in a mounted
`Secret` volume or injected environment variables.

## Markers

A marker has the form `${provider:[path:]key}` and must be the **entire** configuration value;
markers embedded inside larger values are not resolved. To use a literal value that starts with
`${`, escape it by doubling the dollar sign: `$${literal}` becomes `${literal}`.

Resolution happens once, when the configuration is loaded: on server startup, when a client
connection is created via `ConnectionFactory`, and when the lake tiering service parses its
arguments. Rotating a secret requires a restart of the affected process. Markers are rejected in
dynamically altered cluster configuration. If a value looks like a marker but `config.providers`
is not set, it is kept as a literal and a warning is logged.

## Providers

Providers are enabled explicitly with `config.providers`; a marker referencing a provider that is
not declared fails the startup. Provider parameters are passed as
`config.providers.<identifier>.param.<parameter>`.

| Provider | Marker | Description |
| --- | --- | --- |
| `directory` | `${directory:/dir:file}` | Reads the content of `/dir/file` verbatim (UTF-8). Matches a Kubernetes `Secret` mounted as a volume, where each secret key becomes a file. |
| `env` | `${env:VAR}` | Reads an environment variable, e.g. one injected via `secretKeyRef`. |
| `file` | `${file:/path/creds.properties:key}` | Reads a single property from a Java properties file. |

The filesystem-reading providers (`directory`, `file`) require the `allowed.paths` parameter — a
comma-separated list of roots they may read from. A marker resolving to a file outside every root
is rejected, so a stray marker cannot read arbitrary files. The `env` provider optionally accepts
`allowlist.pattern`, a regular expression the variable name must match.

Custom providers can be added by implementing
`org.apache.fluss.config.provider.ConfigProvider` and registering the implementation for
Java `ServiceLoader` discovery. The provider must be visible to the classloader that loads Fluss
itself: place the jar on the server classpath (`lib/`), or bundle it with the Fluss connector for
Flink jobs. Plugin directories are not scanned.

## Kubernetes example

```yaml
# server.yaml shipped as a plain ConfigMap — contains no secrets
config.providers: directory
config.providers.directory.param.allowed.paths: /etc/fluss/secrets
datalake.paimon.s3.secret-key: ${directory:/etc/fluss/secrets:s3-secret-key}
```

```yaml
# pod spec: the secret is a normal Secret volume, never part of the ConfigMap
volumes:
  - name: fluss-secrets
    secret:
      secretName: fluss-paimon-creds
containers:
  - name: tablet-server
    volumeMounts:
      - name: fluss-secrets
        mountPath: /etc/fluss/secrets
        readOnly: true
```

Values resolved through a provider are treated as sensitive and are hidden when the configuration
is logged, regardless of the key name.
