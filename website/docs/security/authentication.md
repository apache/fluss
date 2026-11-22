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
* **mTLS**: Mutual TLS. The client authenticates with the X.509 certificate it presents during the TLS handshake.
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
| client.security.protocol         | String | PLAINTEXT     | The security protocol used to communicate with brokers. Currently, `PLAINTEXT`, `SASL` and `mTLS` are supported, the configuration value is case insensitive.                                                                                                                              |
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

## mTLS
With mutual TLS (mTLS), the client presents an X.509 certificate during the TLS handshake. The server validates the certificate against its truststore and derives the principal from the distinguished name (DN) of the certificate subject. The client sends no password.

A listener uses mTLS when its `security.protocol.map` entry is `mTLS`. Such a listener must also be listed in `security.ssl.enabled.listeners`. The server then requires a client certificate on that listener and rejects a client without one.

### mTLS Server-Side Configuration
| Option                                | Type   | Default Value | Description                                                                                                                                   |
|---------------------------------------|--------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| security.protocol.map                 | Map    | (none)        | Set the listener to `mTLS`, e.g. `CLIENT:mTLS`.                                                                                               |
| security.ssl.enabled.listeners        | List   | (none)        | Listener names with TLS enabled. Every `mTLS` listener must be in this list.                                                                  |
| security.ssl.keystore.path            | String | (none)        | Keystore with the private key and certificate of the server.                                                                                  |
| security.ssl.keystore.password        | String | (none)        | The password of the server keystore.                                                                                                          |
| security.ssl.truststore.path          | String | (none)        | Truststore with the certificates the server trusts. Required for `mTLS` listeners. The server accepts only client certificates it can verify with this truststore. |
| security.ssl.truststore.password      | String | (none)        | The password of the server truststore.                                                                                                        |
| security.ssl.principal.mapping.rules  | String | DEFAULT       | Rules that map the DN of a client certificate to a principal name. See [Principal Mapping Rules](#principal-mapping-rules).                    |

### Principal Mapping Rules
The server reads the DN in the RFC 2253 form, for example `CN=alice,OU=Engineering,O=Example`. It applies the rules in `security.ssl.principal.mapping.rules` in order and uses the first rule that matches. The principal type is always `User`.

The value is a comma-separated list of rules. Each rule is one of:
* `DEFAULT`: uses the full DN as the principal name.
* `RULE:pattern/replacement/`: if the Java regular expression `pattern` matches the full DN, the principal name is `replacement`, where `$1`, `$2`, ... refer to the groups of the pattern. Append `L` or `U` to convert the name to lower or upper case, e.g. `RULE:pattern/replacement/L`.

The following rules use the common name (CN) as the principal name, and the full DN for a certificate whose subject does not start with a CN:

```yaml title="conf/server.yaml"
security.ssl.principal.mapping.rules: RULE:^CN=([^,]+).*$/$1/,DEFAULT
```

With these rules, the certificate `CN=alice,OU=Engineering,O=Example` authenticates as `User:alice`. If no rule matches a DN, the server rejects the client. The server checks the syntax of the rules at startup and fails to start if a rule is invalid.

To make an mTLS principal a super user, add it to `super.users` as `User:<name>`. The name is what the mapping rules produce. Separate several entries with semicolons:

* With the rules above, the name is the CN: `super.users: User:admin;User:ops`
* With the `DEFAULT` rule, the name is the full DN: `super.users: User:CN=admin,O=Example;User:CN=ops,O=Example`

A name may contain commas and colons, e.g. `User:spiffe://example.org/fluss`. A name cannot contain a semicolon, because a semicolon ends the entry. If a certificate would get such a name, use a mapping rule that produces a different one.

### mTLS Client-Side Configuration
| Option                                  | Type    | Default Value | Description                                                                                         |
|-----------------------------------------|---------|---------------|-----------------------------------------------------------------------------------------------------|
| client.security.protocol                | String  | PLAINTEXT     | Set to `mTLS`.                                                                                      |
| client.security.ssl.enabled             | Boolean | false         | Must be `true` for `mTLS`.                                                                          |
| client.security.ssl.keystore.path       | String  | (none)        | Keystore with the private key and certificate of the client. Required for `mTLS`.                   |
| client.security.ssl.keystore.password   | String  | (none)        | The password of the client keystore.                                                                |
| client.security.ssl.truststore.path     | String  | (none)        | Truststore with the certificates the client trusts. If unset, the JVM default truststore is used. |
| client.security.ssl.truststore.password | String  | (none)        | The password of the client truststore.                                                              |

The client certificate must allow TLS client authentication. If it has an extended key usage (EKU) extension, the extension must include `clientAuth`. Otherwise the server rejects the TLS handshake, and the client fails without a retry.

Here is an example where the `CLIENT` listener requires mTLS and the internal listener uses PLAINTEXT:

```yaml title="conf/server.yaml"
bind.listeners: INTERNAL://localhost:9092, CLIENT://localhost:9093
security.protocol.map: CLIENT:mTLS, INTERNAL:PLAINTEXT
internal.listener.name: INTERNAL
security.ssl.enabled.listeners: CLIENT
security.ssl.keystore.path: /etc/fluss/server.keystore.jks
security.ssl.keystore.password: <keystore-password>
security.ssl.truststore.path: /etc/fluss/server.truststore.jks
security.ssl.truststore.password: <truststore-password>
security.ssl.principal.mapping.rules: RULE:^CN=([^,]+).*$/$1/,DEFAULT
authorizer.enabled: true
super.users: User:admin
```

```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'fluss-server-1:9093',
  'client.security.protocol' = 'mTLS',
  'client.security.ssl.enabled' = 'true',
  'client.security.ssl.keystore.path' = '/etc/fluss/client.keystore.jks',
  'client.security.ssl.keystore.password' = '<keystore-password>',
  'client.security.ssl.truststore.path' = '/etc/fluss/client.truststore.jks',
  'client.security.ssl.truststore.password' = '<truststore-password>'
);
```

### mTLS on the Internal Listener
Servers connect to each other over the internal listener with the `client.security.*` options of their own `conf/server.yaml`. If the internal listener uses mTLS, each server needs `client.security.protocol: mTLS`, `client.security.ssl.enabled: true`, and a client keystore in `client.security.ssl.keystore.*`. The server truststore must accept the certificate in that keystore. A server fails to start if `client.security.protocol` differs from the authentication protocol of its internal listener.

The server skips ACL checks for every request on the internal listener. An mTLS internal listener therefore accepts only principals listed in `super.users`. Add the principal that the mapping rules derive from the server certificate, e.g. `super.users: User:admin;User:fluss-server`. The server rejects every other certificate on the internal listener, including one that an mTLS client listener accepts. If clients and servers share one listener, only super users can connect to it. Configure a separate client listener for regular users.

A certificate that a server uses both as server certificate and as client certificate needs `serverAuth` and `clientAuth` in its EKU extension, if it has one.

### Certificate Validity
The server validates the client certificate during the TLS handshake only. A connection stays open after its client certificate expires. The server checks the certificate again only when the client opens a new connection.

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
