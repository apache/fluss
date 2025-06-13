---
sidebar_label: Authentication
title: Security Authentication
sidebar_position: 2
---

<!--
 Copyright (c) 2025 Alibaba Group Holding Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Authentication
Fluss provides a pluggable authentication mechanism, allowing users to configure client and server authentication methods based on their security requirements.

## Overview
Authentication in Fluss is handled through listeners, where each connection triggers a specific authentication protocol based on the configuration. Supported mechanisms include:
* **PLAINTEXT**: Default, no authentication.
* **SASL/PLAIN**: Username/password-based authentication.
* **Custom plugins**: Extendable via interfaces for enterprise or third-party integrations.

You can configure different authentication protocols per listener using the `security.protocol.map` property in conf/server.yaml.

## PLAINTEXT
The PLAINTEXT authentication method is the default used by Fluss. It does not perform any identity verification and is suitable for:
* Local development and debugging.
* Internal communication within trusted clusters.
* Lightweight deployments without access control.

No additional configuration is required for this mode.

## SAS/PLAIN
SASL/PLAIN is a username/password-based authentication method that provides basic but effective security for production environments.

### sasl server related configuration
| Property Name                                                  | Description                                                            | Default Value |
|----------------------------------------------------------------|------------------------------------------------------------------------|---------------|
| security.sasl.enabled.mechanisms                               | Comma-separated list of enabled SASL mechanisms  (e.g., PLAIN).        | PLAIN         |
| `security.sasl.listener.name.{listenerName}.plain.jaas.config` | JAAS configuration for a specific listener and mechanism.              | (none)        |  
| `security.sasl.plain.jaas.config`                              | Global JAAS configuration for all listeners using the PLAIN mechanism. | (none)        | 

⚠️ The system tries to load JAAS configurations in the following order:
1. Listener-specific config: `security.sasl.listener.name.{listenerName}.{mechanism}.jaas.config`
2. Mechanism-wide config: `security.sasl.{mechanism}.jaas.config`
3. System-level fallback: `-Djava.security.auth.login.config` JVM option

Here is an example where Port 9093 requires SASL/PLAIN authentication for the users "admin" and "fluss":
```yaml title="conf/server.yaml"
# port 9092 use SASL authentication for clients
listeners: INTERNAL://localhost:9092, CLIENT://localhost:9093
advertised.listeners: CLIENT://host1:9093, INTERNAL://host2:9092
security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
internal.listener.name: INTERNAL
# use SASL/PLAIN
security.sasl.enabled.mechanisms: PLAIN
security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_fluss="fluss-pass";
```


### sasl client related configuration
Clients must specify the appropriate security protocol and authentication mechanism when connecting to Fluss brokers.

| Property Name                    | Description                                                                                                         | Default Value |
|----------------------------------|---------------------------------------------------------------------------------------------------------------------|---------------|
| client.security.protocol         | The security protocol used to communicate with brokers. Use sasl to enable SASL authentication.                     | PLAINTEXT     |
| client.security.sasl.mechanism   | The SASL mechanism used for authentication. Only support PLAIN now, but will support more mechanisms in the future. | (none)        |
| client.security.sasl.jaas.config | JAAS configuration for SASL. If not set, falls back to system property -Djava.security.auth.login.config.           |


Here is an example client configuration(flink catalog):
```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'fluss-server-1:9123',
  'client.security.protocol' = 'SASL',
  'client.sasl.mechanism' = 'PLAIN',
  'client.sasl.jaas.plain.config' = 'com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required username="fluss" password="fluss-pass";'
);
```


## Extending Authentication Methods (For Developers)

Fluss supports custom authentication logic through its plugin architecture.

Steps to implement a custom authenticator:
1. **Implement AuthenticationPlugin Interfaces**: Implement ClientAuthenticationPlugin for client-side logic and implement ServerAuthenticationPlugin for server-side logic.
2. **Package the Plugin**: Compile your implementation into a JAR file, and then place it in the Fluss plugin directory for automatic loading.
3. **Enable the Plugin**:  Configure the desired protocol via:
  * `security.protocol.map` – for server-side listener authentication. 
  * `client.security.protocol` – for client-side authentication.