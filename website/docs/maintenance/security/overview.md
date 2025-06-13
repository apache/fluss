---
sidebar_label: Security Overview
title: Security Overview
sidebar_position: 1
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

# Overview
By default, Fluss does not enable authentication or authorization, meaning any client can access the system without identity verification or permission checks. While this setup is convenient for development and testing environments, it is not suitable for production use due to potential security risks.

Fluss currently supports the following security features:
* **[Authentication](authentication.md)**: Provides a pluggable mechanism that allows users to configure client and server authentication methods based on their specific requirements.
* **[Authorization](authorization.md)**: Provides a pluggable framework for managing access control using Access Control Lists (ACLs).

## End-to-End Security Flow
Here is the full end-to-end security flow in Fluss:
1. **Client connects to an endpoint**
   * Each endpoint is defined by a listener configured via listeners.
   * Example: CLIENT://localhost:9093.
2. **Authentication mechanism is triggered**
   * Based on the mapping in `security.protocol.map`, the system selects the appropriate authentication protocol (e.g., PLAINTEXT, SASL/PLAIN).
   * The ServerAuthenticator handles the token exchange and verifies credentials.
3. **Fluss principal is created**
   * Upon successful authentication, the server creates a FlussPrincipal, representing the authenticated identity. 
   * This can be based on username/password, Kerberos ticket, or any custom credential type. 
4. **Authorization is performed based on the principal**
   * The client’s actions are checked against ACL rules by the Authorizer using the FlussPrincipal. 
   * Access decisions are made based on resource-level permissions.

##  Listener-Based Security Configuration
In Fluss, a listener refers to a network endpoint that the server listens on for incoming connections from clients or internal services. Each listener is associated with a name (e.g., CLIENT, INTERNAL) and an address/port binding.

Listeners allow Fluss to support multiple connection endpoints with different purposes and security requirements: one listener may be used for internal communication between servers while others may be exposed to external clients and require different authentication protocols.
This flexibility enables operators to apply different security protocols per listener, ensuring appropriate protection levels based on who or what is connecting.

Server Side Configuration Properties:
| Property | Description | Default Value |
| --- | --- | --- |
| bind.listeners | The network address and port to which the server binds for accepting connections. This defines the interface and port where the server will listen for incoming requests. The format is `listener_name://host:port`, and multiple addresses can be specified, separated by commas. Use `0.0.0.0` for the `host` to bind to all available interfaces which is dangerous on production and not suggested for production usage. The `listener_name` serves as an identifier for the address in the configuration. For example, `internal.listener.name` specifies the address used for internal server communication. If multiple addresses are configured, ensure that the `listener_name` values are unique. | FLUSS://localhost:9123 |
| advertised.listeners | The externally advertised address and port for client connections. Required in distributed environments when the bind address is not publicly reachable. Format matches `bind.listeners` (listener_name://host:port). Defaults to the value of `bind.listeners` if not explicitly configured. | (none) |
| internal.listener.name | The listener name used for internal server communication. | FLUSS |
| security.protocol.map | A map defining the authentication protocol for each listener. The format is `listenerName1:protocol1,listenerName2:protocol2`, e.g.,`CLIENT:SASL, INTERNAL:PLAINTEXT`. | (none) |



Here is an example server side configuration:
```yaml title="conf/server.yaml"
listeners: INTERNAL://localhost:9092, CLIENT://localhost:9093
advertised.listeners: CLIENT://host1:9093, INTERNAL://host2:9092
security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
internal.listener.name: INTERNAL
```
In this example:
* Port 9092 (INTERNAL) uses PLAINTEXT for internal communication (no authentication).
* Port 9093 (CLIENT) requires SASL/PLAIN authentication for secure client access.
 
Each connection must use the protocol defined in security.protocol.map. If a client attempts to connect using an incorrect protocol, authentication will fail.


## Authentication
Fluss supports a pluggable authentication mechanism, allowing developers to implement custom plugins. Built-in support includes:
* PLAINTEXT: No authentication (default).
* SASL/PLAIN: Username/password-based authentication.


Clients must authenticate using credentials compatible with the protocol configured on the target listener.
Upon successful authentication, a FlussPrincipal is generated to uniquely identify the authenticated user or role.

### Server-Side Authentication Properties
| Property | Description | Default Value |
| --- | --- |---------------|
| security.protocol.map | A map defining the authentication protocol for each listener. The format is `listenerName1:protocol1,listenerName2:protocol2`, e.g.,`CLIENT:SASL, INTERNAL:PLAINTEXT`. | (none)        |
| `security.${protocol}.*` | Protocol-specific configuration properties. For example, security.sasl.jaas.config for SASL authentication settings.| (none)        |

### Client-Side Authentication Properties
| Property | Description                                                                                                           | Default Value |
| --- |-----------------------------------------------------------------------------------------------------------------------|---------------|
| client.security.protocol | The security protocol used to communicate with brokers.                                                               | PLAINTEXT     |
| `client.security.{protocol}.*` | Client-side configuration properties for a specific authentication protocol. E.g., client.security.sasl.jaas.config.  | (none)        |

## Fluss Principal
The FlussPrincipal is a core concept in the Fluss security architecture. It represents the identity of an authenticated entity (such as a user or service) and serves as the central bridge between authentication and authorization.Once a client successfully authenticates via a supported mechanism (e.g., SASL/PLAIN, Kerberos), a FlussPrincipal is created to represent that client's identity. 
This principal is then used throughout the system for access control decisions, linking who the user is with what they are allowed to do.

The principal type indicates the category of the principal (e. g., "User", "Group", "Role"), while the name identifies the specific entity within that category. By default, the simple authorizer uses "User" as the principal type, but custom authorizers can extend this to support role-based or group-based access control lists (ACLs).
Example usage:
* `new FlussPrincipal("admin", "User")` – A standard user principal.
* `new FlussPrincipal("admins", "Group")` – A group-based principal for authorization.

## Authorization
Fluss provides a pluggable authorization framework that uses Access Control Lists (ACLs) to determine whether a given FlussPrincipal is allowed to perform an operation on a specific resource.

| Property Name      | Description                                                      | Default Value |
|--------------------|------------------------------------------------------------------|---------------|
| authorizer.enabled | Specifies whether to enable the authorization feature.           | false         |
| authorizer.type    | Specifies the type of authorizer to be used for access control. This value corresponds to the identifier of the authorization plugin. The default value is `default`, which indicates the built-in authorizer implementation. Custom authorizers can be implemented by providing a matching plugin identifier.| default       |
| super.users        | A semicolon-separated list of superusers who have unrestricted access to all operations and resources. Each super user should be specified in the format `principal_type:principal_name`, e.g., `User:admin;User:bob`, This configuration is critical for defining administrative privileges in the system. | (none)         |




