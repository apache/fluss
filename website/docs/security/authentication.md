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
* **SASL**: This mechanism is based on SASL (Simple Authentication and Security Layer) authentication. Fluss supports SASL/PLAIN and SASL/OAUTHBEARER.
* **Custom plugins**: Extendable via interfaces for enterprise or third-party integrations.

You can configure different authentication protocols per listener using the `security.protocol.map` property in `conf/server.yaml`.

## PLAINTEXT
The PLAINTEXT authentication method is the default used by Fluss. It does not perform any identity verification and is suitable for:
* Local development and debugging.
* Internal communication within trusted clusters.
* Lightweight deployments without access control.

No additional configuration is required for this mode.

## SASL
This mechanism is based on SASL (Simple Authentication and Security Layer) authentication. Fluss supports PLAIN username/password authentication and OAUTHBEARER authentication with OAuth 2.0 client credentials and RS256 JWT access tokens.

### SASL Server-Side Configuration
| Option                                                         | Type   | Default Value | Description                                                                                                                           |
|----------------------------------------------------------------|--------|---------------|---------------------------------------------------------------------------------------------------------------------------------------|
| security.sasl.enabled.mechanisms                               | List   | PLAIN         | Comma-separated list of enabled SASL mechanisms. Supported values are `PLAIN` and `OAUTHBEARER`.                                       |
| `security.sasl.listener.name.{listenerName}.plain.jaas.config` | String | (none)        | JAAS configuration for a specific listener and PLAIN mechanism.                                                                       |
| `security.sasl.plain.jaas.config`                              | String | (none)        | Global JAAS configuration for all listeners using the PLAIN mechanism.                                                                |
| `security.sasl.plain.credentials`                              | Map    | (none)        | Map of users in `username:password` format, e.g. `admin:admin-secret,bob:bob-secret`. Syntactic sugar that generates the PLAIN JAAS config, and can be updated without a restart. |


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
security.sasl.plain.jaas.config: org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_fluss="fluss-pass";
```


### Managing Multiple Users
Instead of writing a JAAS config string by hand, you can declare users with the map option `security.sasl.plain.credentials`.
Fluss generates the equivalent `security.sasl.plain.jaas.config` from it, so the example above can be written as:

```yaml title="conf/server.yaml"
security.sasl.enabled.mechanisms: PLAIN
security.sasl.plain.credentials: admin:admin-pass,fluss:fluss-pass
```

Usernames may only contain letters, digits, and underscores, because they become part of the JAAS option key `user_<username>`.
Passwords may not contain commas, colons, double quotes, semicolons, backslashes, or control characters, since these would break the map format or the generated JAAS config string.

Unlike `security.sasl.plain.jaas.config`, this option is a [dynamic cluster config](maintenance/operations/updating-configs.md#updating-cluster-configs): users can be added, changed, and removed on a running cluster, and the change takes effect for new connections on all servers.

```sql title="Flink SQL"
-- Add user "bob"
CALL sys.append_cluster_configs(
  config_pairs => 'security.sasl.plain.credentials', 'bob:bob-secret'
);

-- Remove user "bob" (the supplied secret is ignored, removal matches on the username)
CALL sys.subtract_cluster_configs(
  config_pairs => 'security.sasl.plain.credentials', 'bob:any-secret'
);
```

Note the following when using both options together:
* Users defined in the `security.sasl.plain.jaas.config` of `conf/server.yaml` stay valid; the credentials map is merged on top of them and wins on a username conflict. Removing a user from the map therefore only revokes it if the user is not also defined in `conf/server.yaml`.
* The map applies to the mechanism-wide JAAS config only. A listener that sets `security.sasl.listener.name.{listenerName}.plain.jaas.config` keeps using that config and ignores the map.
* Passwords are redacted when configs are read back, for example via `sys.get_cluster_configs`.


### SASL Client-Side Configuration
Clients must specify the appropriate security protocol and authentication mechanism when connecting to Fluss brokers.

| Option                           | Type   | Default Value | Description                                                                                                                                                                                                                                                                               |
|----------------------------------|--------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| client.security.protocol         | String | PLAINTEXT     | The security protocol used to communicate with brokers. Currently, only `PLAINTEXT` and `SASL` are supported, the configuration value is case insensitive.                                                                                                                                |
| client.security.sasl.mechanism   | String | PLAIN         | The SASL mechanism used for authentication. Supported values are `PLAIN` and `OAUTHBEARER`.                                                                                                                                                                                               |
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

### SASL/OAUTHBEARER

With OAUTHBEARER, the client synchronously obtains a JWT access token from an OAuth 2.0 token endpoint by using the client credentials grant. The server retrieves public keys from a JWKS endpoint, verifies the RS256 signature and JWT claims, and creates the authenticated principal as `User:<sub>`.

#### OAUTHBEARER Client Configuration

| Option | Type | Default Value | Description |
| --- | --- | --- | --- |
| `client.security.sasl.oauthbearer.token.endpoint` | String | (none) | Required absolute HTTP(S) OAuth 2.0 token endpoint. HTTPS is recommended. |
| `client.security.sasl.oauthbearer.client-id` | String | (none) | Required OAuth 2.0 client ID. |
| `client.security.sasl.oauthbearer.client-secret` | Password | (none) | Required OAuth 2.0 client secret. The value is redacted from configuration logs. |
| `client.security.sasl.oauthbearer.scope` | String | (none) | Optional scope sent to the token endpoint. |
| `client.security.sasl.oauthbearer.request-timeout` | Duration | 10 s | Timeout used for both connecting to and reading from the token endpoint. |

Each new server connection synchronously requests its own access token. The token is retained only by that connection's authenticator for authentication retries. Fluss does not share or refresh client tokens and does not run a background refresh task. When a connection is re-established, the client requests a new token from the token endpoint. If the request fails, authentication for that connection fails.

```yaml
client.security.protocol: SASL
client.security.sasl.mechanism: OAUTHBEARER
client.security.sasl.oauthbearer.token.endpoint: https://idp.example.com/oauth2/token
client.security.sasl.oauthbearer.client-id: fluss-client
config.providers: env
client.security.sasl.oauthbearer.client-secret: ${env:FLUSS_OAUTH_CLIENT_SECRET}
client.security.sasl.oauthbearer.scope: fluss
```

Configuration provider markers such as `${env:FLUSS_OAUTH_CLIENT_SECRET}` are resolved by the existing [Secrets in Configuration](secrets.md) mechanism when the client is created through `ConnectionFactory`.

#### OAUTHBEARER Server Configuration

| Option | Type | Default Value | Description |
| --- | --- | --- | --- |
| `security.sasl.oauthbearer.jwks.endpoint` | String | (none) | Required absolute HTTP(S) JWKS endpoint. HTTPS is recommended. |
| `security.sasl.oauthbearer.expected-issuer` | String | (none) | Required issuer that must exactly match the JWT `iss` claim. |
| `security.sasl.oauthbearer.expected-audiences` | List | (none) | Required accepted audiences. At least one value must match the JWT `aud` claim. |
| `security.sasl.oauthbearer.jwks.request-timeout` | Duration | 5 s | Timeout used for both connecting to and reading from the JWKS endpoint. |
| `security.sasl.oauthbearer.jwks.refresh-min-interval` | Duration | 30 s | Minimum reprieve before refreshing non-empty cached JWKS keys again for an unknown JWT `kid`. |

```yaml title="conf/server.yaml"
bind.listeners: INTERNAL://localhost:9092, CLIENT://localhost:9093
security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
internal.listener.name: INTERNAL
security.sasl.enabled.mechanisms: OAUTHBEARER
security.sasl.oauthbearer.jwks.endpoint: https://idp.example.com/.well-known/jwks.json
security.sasl.oauthbearer.expected-issuer: https://idp.example.com/
security.sasl.oauthbearer.expected-audiences: fluss
```

The server fetches JWKS lazily on a key miss and shares the cache within the server process. It does not preload keys or run a background refresh task. Each JWT is verified once during connection authentication. Subsequent RPCs use the principal stored on that connection and only compare the current time with the authenticated JWT `exp`; an expired connection is rejected and closed when its next RPC arrives. Idle expired connections remain subject to the normal connection idle timeout.


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
  * `security.protocol.map` – for server-side listener authentication and use the `org.apache.fluss.security.auth.AuthenticationPlugin#authProtocol()` as protocol identifier.
  * `client.security.protocol` – for client-side authentication and use the `org.apache.fluss.security.auth.AuthenticationPlugin#authProtocol()` as protocol identifier
