---
sidebar_label: Authentication
title: Security Authentication
sidebar_position: 2
---

# Authentication
Fluss provides a pluggable authentication mechanism, allowing users to configure client and server authentication methods based on their security requirements.

## Overview
Authentication in Fluss is handled through listeners, where each connection triggers a specific authentication protocol based on the configuration. Supported mechanisms include:
* **PLAINTEXT**: Default, no authentication.
* **SASL**: This mechanism is based on SASL (Simple Authentication and Security Layer) authentication. Currently, only SASL/PLAIN is supported, which involves authentication using a username and password.
* **Custom plugins**: Extendable via interfaces for enterprise or third-party integrations.

You can configure different authentication protocols per listener using the `security.protocol.map` property in `conf/server.yaml`.

## PLAINTEXT
The PLAINTEXT authentication method is the default used by Fluss. It does not perform any identity verification and is suitable for:
* Local development and debugging.
* Internal communication within trusted clusters.
* Lightweight deployments without access control.

No additional configuration is required for this mode.

## SASL
This mechanism is based on SASL (Simple Authentication and Security Layer) authentication. Currently, only SASL/PLAIN is supported, which involves authentication using a username and password. It is recommended for production environments.

### SASL Client-Side Configuration
| Option                                                         | Type   | Default Value | Description                                                                                                                           |
|----------------------------------------------------------------|--------|---------------|---------------------------------------------------------------------------------------------------------------------------------------|
| security.sasl.enabled.mechanisms                               | List   | PLAIN         | Comma-separated list of enabled SASL mechanisms. Only support PLAIN(which involves authentication using a username and password) now. |
| `security.sasl.listener.name.{listenerName}.plain.jaas.config` | String | (none)        | JAAS configuration for a specific listener and PLAIN mechanism.                                                                       |
| `security.sasl.plain.jaas.config`                              | String | (none)        | Global JAAS configuration for all listeners using the PLAIN mechanism.                                                                |


⚠️ The system tries to load JAAS configurations in the following order:
1. Listener-specific config: `security.sasl.listener.name.{listenerName}.{mechanism}.jaas.config`
2. Mechanism-wide config: `security.sasl.{mechanism}.jaas.config`
3. System-level fallback: `-Djava.security.auth.login.config` JVM option

Here is an example where port 9093 requires SASL/PLAIN authentication for the users "admin" and "fluss":
```yaml title="conf/server.yaml"
# port 9093 use SASL authentication for clients
bind.listeners: INTERNAL://localhost:9092, CLIENT://localhost:9093
advertised.listeners: CLIENT://host:9093,
security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
internal.listener.name: INTERNAL
# use SASL/PLAIN
security.sasl.enabled.mechanisms: PLAIN
security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_fluss="fluss-pass";
```


### SASL Client-Side Configuration
Clients must specify the appropriate security protocol and authentication mechanism when connecting to Fluss brokers.

| Option                           | Type   | Default Value | Description                                                                                                                                                                                                                                                                               |
|----------------------------------|--------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| client.security.protocol         | String | PLAINTEXT     | The security protocol used to communicate with brokers. Currently, only `PLAINTEXT` and `SASL` are supported, the configuration value is case insensitive.                                                                                                                                |
| client.security.sasl.mechanism   | String | PLAIN         | The SASL mechanism used for authentication. Only support PLAIN now, but will support more mechanisms in the future.                                                                                                                                                                       |
| client.security.sasl.username    | String | (none)        | The password to use for client-side SASL JAAS authentication. This is used when the client connects to the Fluss cluster with SASL authentication enabled. If not provided, the username will be read from the JAAS configuration string specified by `client.security.sasl.jaas.config`. |
| client.security.sasl.password    | String | (none)        | The password to use for client-side SASL JAAS authentication. This is used when the client connects to the Fluss cluster with SASL authentication enabled. If not provided, the password will be read from the JAAS configuration string specified by `client.security.sasl.jaas.config`. |
| client.security.sasl.jaas.config | String | (none)        | JAAS configuration for SASL. If not set, fallback to system property `-Djava.security.auth.login.config`.                                                                                                                                                                                 |



Here is an example client configuration in Flink SQL with Catalog:

```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'fluss-server-1:9123',
  'client.security.protocol' = 'SASL',
  'client.security.sasl.mechanism' = 'PLAIN',
  'client.security.sasl.username' = '<my-username>',
  'client.security.sasl.password' = '<my-password>',
);
```


## Extending Authentication Methods (For Developers)

Fluss supports custom authentication logic through its plugin architecture.

Steps to implement a custom authenticator:
1. **Implement AuthenticationPlugin Interfaces**: 
Implement `ClientAuthenticationPlugin` for client-side logic and implement `ServerAuthenticationPlugin` for server-side logic.
2.  **Server-Side Plugin Installation**:
Build the plugin as a standalone JAR and copy it to the Fluss server’s plugin directory: `<FLUSS_HOME>/plugins/<custom_auth_plugin>/`. The server will automatically load the plugin at startup.
3.  **Client-Side Plugin Packaging**  :
To enable plugin functionality on the client side, include the plugin JAR in your application’s classpath. This allows the Fluss client to auto-discover the plugin during runtime.
4. **Configure the Desired Protocol**:
  * `security.protocol.map` – for server-side listener authentication and use the `com.alibaba.fluss.security.auth.AuthenticationPlugin#authProtocol()` as protocol identifier.
  * `client.security.protocol` – for client-side authentication and use the `com.alibaba.fluss.security.auth.AuthenticationPlugin#authProtocol()` as protocol identifier