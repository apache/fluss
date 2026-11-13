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

### SASL Server-Side Configuration
| Option                                                         | Type   | Default Value | Description                                                                                                                           |
|----------------------------------------------------------------|--------|---------------|---------------------------------------------------------------------------------------------------------------------------------------|
| security.sasl.enabled.mechanisms                               | List   | (none)        | Comma-separated list of enabled SASL mechanisms. PLAIN is the supported mechanism. |
| `security.sasl.listener.name.{listenerName}.plain.jaas.config` | String | (none)        | JAAS configuration for a specific listener and PLAIN mechanism.                                                                       |
| `security.sasl.plain.jaas.config`                              | String | (none)        | Global JAAS configuration for all listeners using the PLAIN mechanism.                                                                |
| `security.sasl.plain.credentials`                              | Map    | (none)        | Map of users in `username:password` format, e.g. `admin:admin-secret,bob:bob-secret`. Syntactic sugar that generates the PLAIN JAAS config, and can be updated without a restart. |
| `security.sasl.listener.name.{listenerName}.plain.proxy-users` | Map    | (none)        | Proxy grants for a Fluss protocol listener using SASL/PLAIN, keyed by the authenticated username. Use the lowercase listener name in the option key. |


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
| client.security.sasl.mechanism   | String | PLAIN         | The SASL mechanism used for authentication. Only support PLAIN now, but will support more mechanisms in the future.                                                                                                                                                                       |
| client.security.sasl.authorization-id | String | (none) | The user to act as with SASL/PLAIN. Unset or empty uses the authenticated account; another user requires a server-side proxy grant. |
| client.security.sasl.username    | String | (none)        | The username to use for client-side SASL JAAS authentication. This is used when the client connects to the Fluss cluster with SASL authentication enabled. If not provided, the username will be read from the JAAS configuration string specified by `client.security.sasl.jaas.config`. |
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


## Impersonation

A service can use SASL/PLAIN with its own credentials to act as another user. For example, configure the `CLIENT` listener to allow `gateway` to act as `alice` or `bob`:

```yaml title="conf/server.yaml"
security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
security.sasl.enabled.mechanisms: PLAIN
security.sasl.plain.credentials: admin:admin-pass,gateway:gateway-pass
security.sasl.listener.name.client.plain.proxy-users: gateway:alice;bob
authorizer.enabled: true
super.users: User:admin
```

On the client, authenticate as the service account and set the SASL authorization identity:

```properties
client.security.protocol=SASL
client.security.sasl.username=gateway
client.security.sasl.password=gateway-pass
client.security.sasl.authorization-id=alice
```

Fluss verifies the service account's password and the grant during the handshake. ACL checks use the target user for the lifetime of that connection. An unset or empty authorization identity uses the authenticated account. The target only needs a principal in Fluss ACLs.

Separate target usernames with semicolons and service accounts with commas. The service account must match the authenticated username exactly. Target names and `super.users` entries follow the ACL principal case setting (`security.acl.principal.ignore-case`): exact by default, case-insensitive when enabled. An explicit wildcard grant (`gateway:*`) permits any target other than a super user. Grant it only to a service that authenticates its callers and derives the authorization ID from the authenticated identity.

Grants apply only to the configured listener and are validated at server startup. Dynamic updates to `security.sasl.plain.credentials` affect authentication on new connections. Use TLS to protect the service account password in transit.

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
