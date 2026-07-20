---
sidebar_label: Authorization and ACLs
title: Authorization and ACLs
sidebar_position: 3
---

# Authorization and ACLs

## Configuration 
Fluss provides a pluggable authorization framework that uses Access Control Lists (ACLs) to determine whether a given FlussPrincipal is allowed to perform an operation on a specific resource.


| Option             | Type    | Default Value | Description                                                                                                                                                                                                                                                                                                    |
|--------------------|---------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| authorizer.enabled | Boolean | false         | Specifies whether to enable the authorization feature.                                                                                                                                                                                                                                                         |
| authorizer.type    | String  | default       | Specifies the type of authorizer to be used for access control. This value corresponds to the identifier of the authorization plugin. The default value is `default`, which indicates the built-in authorizer implementation. Custom authorizers can be implemented by providing a matching plugin identifier. |


## Core Components of ACLs

Fluss uses an Access Control List (ACL) mechanism to enforce fine-grained permissions on resources such as clusters, databases, and tables. This allows administrators to define who (principals) can perform what actions (operations) on which objects (resources).

Fluss ACLs are defined in the general format:
```
Principal {P} is Allowed Operation {O} From Host {H} on any Resource {R}.
```

### Resource
In Fluss, a Resource represents an object to which access control can be applied. **Resources are organized in a hierarchical structure**, enabling fine-grained permission management as well as permission inheritance from higher-level scopes (e.g., database-level permissions apply to all tables within that database).

There are three main types of resources:

| Resource Type | Resource Name Format Example | Description                                                                                                             |
|---------------|------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| Cluster       | cluster                      | The cluster resource represents the entire Fluss cluster and is used for cluster-wide permissions.                      |
| Database      | default_db                   | A database resource represents a specific database within the Fluss cluster and is used for database-level permissions. |
| Table         | default_db.default_table     | A table resource represents a specific table within a database and is used for table-level permissions.                 |

This hierarchy follows this pattern:
```text
Cluster
 └── Database
     └── Table
```

### Operation
In Fluss, an OperationType defines the type of action a principal (user or role) is attempting to perform on a resource (cluster, database, or table).

| Operation Type | Description |
|----------------| --- |
| `ANY`            | Matches any operation type and is used exclusively in filters or queries to match ACL entries. It should not be used when granting actual permissions.|
| `ALL`            | Grants permission for all operations on a resource. |
| `READ` | Allows reading data from a resource (e.g., querying tables).|
| `WRITE` | Allows writing data to a resource (e.g., inserting or updating data in tables).|
| `CREATE` | Allows creating a new resource (e.g., creating a new database or table).|
| `DROP` | Allows dropping a resource (e.g., dropping a database or table).|
| `ALTER` | Allows modifying the structure of a resource (e.g., altering the schema of a table).|
| `DESCRIBE` | Allows describing a resource (e.g., retrieving metadata about a table).|


Fluss implements a permission inheritance model, where certain operations imply others. This helps reduce redundancy in ACL rules by avoiding the need to explicitly grant every low-level permission.
* `ALL` implies all other operations.
* `READ`, `WRITE`, `CREATE`, `DROP`, `ALTER` each imply `DESCRIBE`.

### Fluss Principal
The FlussPrincipal is a core concept in the Fluss security architecture. It represents the identity of an authenticated entity (such as a user or service) and serves as the central bridge between authentication and authorization. Once a client successfully authenticates via a supported mechanism (e.g., SASL/PLAIN, Kerberos), a FlussPrincipal is created to represent that client's identity.
This principal is then used throughout the system for access control decisions, linking who the user is with what they are allowed to do.

The principal type indicates the category of the principal (e. g., "User", "Group", "Role"), while the name identifies the specific entity within that category. By default, the simple authorizer uses "User" as the principal type, but custom authorizers can extend this to support role-based or group-based access control lists (ACLs).
Example usage:
* `new FlussPrincipal("admin", "User")` – A standard user principal.
* `new FlussPrincipal("admins", "Group")` – A group-based principal for authorization.

## Operations and Resources on Protocols
Below is a summary of the currently public protocols and their relationship with ACL operations and resource types. `None` means the protocol does not perform ACL authorization.

| Protocol | Operation | Resource | Note |
| --- | --- | --- | --- |
| API_VERSIONS | None | None | Lists supported APIs and versions. |
| CREATE_DATABASE | CREATE | Cluster | |
| DROP_DATABASE | DROP | Database | |
| LIST_DATABASES | DESCRIBE | Database | Only databases that the user has permission to access are returned. |
| DATABASE_EXISTS | DESCRIBE | Database | Returns `false` if the user lacks permission. The default database is exempted for compatibility. |
| GET_DATABASE_INFO | DESCRIBE | Database | |
| ALTER_DATABASE | ALTER | Database | |
| CREATE_TABLE | CREATE | Database | |
| DROP_TABLE | DROP | Table | |
| ALTER_TABLE | ALTER | Table | |
| GET_TABLE_INFO | DESCRIBE | Table | |
| GET_TABLE_SCHEMA | DESCRIBE | Table | |
| TABLE_EXISTS | DESCRIBE | Table | Returns `false` if the user lacks permission. |
| LIST_TABLES | DESCRIBE | Table | Only tables that the user has permission to access are returned. |
| LIST_PARTITION_INFOS | DESCRIBE | Table | Requires permission on the owning table. Partition-level ACLs are not checked. |
| GET_METADATA | DESCRIBE | Table | Only metadata that the user has permission to access is returned. |
| GET_LATEST_KV_SNAPSHOTS | DESCRIBE | Table | |
| GET_KV_SNAPSHOT_METADATA | DESCRIBE | Table | |
| GET_LAKE_SNAPSHOT | DESCRIBE | Table | |
| LIST_REMOTE_LOG_MANIFESTS | DESCRIBE | Table | |
| LIST_KV_SNAPSHOTS | DESCRIBE | Table | |
| PRODUCE_LOG | WRITE | Table | |
| FETCH_LOG | READ | Table | Normal client fetches require `READ`; replication follower fetches are treated as internal requests. |
| PUT_KV | WRITE | Table | |
| LOOKUP | READ or WRITE | Table | Normal lookup requires `READ`; lookup with `insertIfNotExists` requires `WRITE`. |
| PREFIX_LOOKUP | READ | Table | |
| LIMIT_SCAN | READ | Table | |
| SCAN_KV | READ | Table | Opening a scan requires `READ` on the table. |
| GET_TABLE_STATS | READ | Table | |
| LIST_OFFSETS | DESCRIBE | Table | |
| INIT_WRITER | WRITE | Table | The user is authorized if it has `WRITE` permission on one of the requested tables. |
| CREATE_PARTITION | WRITE | Table | |
| DROP_PARTITION | WRITE | Table | |
| CREATE_ACLS | ALL | Target resource | Requires `ALL` on the resource whose ACLs are being created. For table ACLs, the minimum authorization granularity is the database. |
| DROP_ACLS | ALL | Target resource | Requires `ALL` on each matched resource. For table ACLs, the minimum authorization granularity is the database. |
| LIST_ACLS | DESCRIBE | Target resource | Only ACLs on resources the user can `DESCRIBE` are returned. |
| DESCRIBE_CLUSTER_CONFIGS | DESCRIBE | Cluster | |
| ALTER_CLUSTER_CONFIGS | ALTER | Cluster | |
| ADD_SERVER_TAG | ALTER | Cluster | |
| REMOVE_SERVER_TAG | ALTER | Cluster | |
| REBALANCE | WRITE | Cluster | |
| LIST_REBALANCE_PROGRESS | DESCRIBE | Cluster | |
| CANCEL_REBALANCE | WRITE | Cluster | |
| GET_CLUSTER_HEALTH | DESCRIBE | Cluster | |
| REGISTER_PRODUCER_OFFSETS | WRITE | Table | Requires `WRITE` on all tables in the request. |
| GET_PRODUCER_OFFSETS | READ | Table | Offsets for tables without `READ` permission are filtered out. |
| DELETE_PRODUCER_OFFSETS | WRITE | Table | Requires `WRITE` on all tables in the producer offset snapshot. |
| ACQUIRE_KV_SNAPSHOT_LEASE | READ | Table | Requires `READ` on all tables in the request. |
| RELEASE_KV_SNAPSHOT_LEASE | READ | Table | Requires `READ` on all tables in the request. |
| DROP_KV_SNAPSHOT_LEASE | READ | Table | Requires `READ` on all tables held by the lease. |
| GET_FILESYSTEM_SECURITY_TOKEN | None | None | Currently no ACL is enforced for this protocol. |

## ACL Operation
Fluss provides a FLINK SQL interface to manage Access Control Lists (ACLs) using the Flink CALL statement. This allows administrators and users to dynamically control access permissions for principals (users or roles) on various resources such as clusters, databases, and tables.

### Add ACL
The general syntax is:
```sql title="Flink SQL"
-- Recommended, use named argument (only supported since Flink 1.19)
CALL [catalog].sys.add_acl(
    resource => '[resource]',
    permission => 'ALLOW',
    principal => '[principal_type:principal_name]',
    operation  => '[operation_type]',
    host => '[host]'
);
     
-- Use indexed argument
CALL [catalog].sys.add_acl(
    '[resource]', 
    '[permission]',
    '[principal_type:principal_name]', 
    '[operation_type]',
    '[host]'
);
```

| Parameter  | Required | Description |
|------------|----------|-------------|
| resource   | Yes      | The resource to apply the ACL to (e.g., `cluster`, `cluster.db1`, `cluster.db1.table1`). |
| permission | Yes      | The permission to grant to the principal on the resource. Currently only `ALLOW` is supported. |
| principal  | Yes      | The principal to apply the ACL to (e.g., `User:alice`, `Role:admin`). |
| operation  | Yes      | The operation to allow for the principal on the resource (e.g., `READ`, `WRITE`, `CREATE`, `DROP`, `ALTER`, `DESCRIBE`, `ALL`). `ANY` is only for filters and should not be used when adding ACLs. |
| host       | No       | The host to apply the ACL to (e.g., `127.0.0.1`). If not specified, the ACL applies to all hosts (same as `*`). |

### Remove ACL
The general syntax is:
```sql title="Flink SQL"
-- Recommended, use named argument (only supported since Flink 1.19)
CALL [catalog].sys.drop_acl(
    resource => '[resource]',
    permission => 'ALLOW',
    principal => '[principal_type:principal_name]',
    operation  => '[operation_type]',
    host => '[host]'
);
     
-- Use indexed argument
CALL [catalog].sys.drop_acl(
    '[resource]', 
    '[permission]',
    '[principal_type:principal_name]', 
    '[operation_type]',
    '[host]'
);
```
| Parameter  | Required | Description |
|------------|----------|-------------|
| resource   | No       | The resource to match (e.g., `cluster`, `cluster.db1`, `cluster.db1.table1`). If not specified, it matches all resources (same as `ANY`). |
| permission | No       | The permission to match (e.g., `ALLOW`, `ANY`). If not specified, it matches all permissions (same as `ANY`). |
| principal  | No       | The principal to match (e.g., `User:alice`, `Role:admin`). If not specified, it matches all principals (same as `ANY`). |
| operation  | No       | The operation to match (e.g., `READ`, `WRITE`, `CREATE`, `DROP`, `ALTER`, `DESCRIBE`, `ANY`, `ALL`). If not specified, it matches all operations (same as `ANY`). |
| host       | No       | The host to match (e.g., `127.0.0.1`). If not specified, it matches all hosts (same as `ANY`). |

### List ACL
List ACL will return a list of ACLs that match the specified criteria.
The general syntax is:
```sql title="Flink SQL"
-- Recommended, use named argument (only supported since Flink 1.19)
CALL [catalog].sys.list_acl(
    resource => '[resource]',
    permission => 'ALLOW',
    principal => '[principal_type:principal_name]',
    operation  => '[operation_type]',
    host => '[host]'
);
     
-- Use indexed argument
CALL [catalog].sys.list_acl(
    '[resource]', 
    '[permission]',
    '[principal_type:principal_name]', 
    '[operation_type]',
    '[host]'
);
```
| Parameter  | Required | Description |
|------------|----------|-------------|
| resource   | No       | The resource to match (e.g., `cluster`, `cluster.db1`, `cluster.db1.table1`). If not specified, it matches all resources (same as `ANY`). |
| permission | No       | The permission to match (e.g., `ALLOW`, `ANY`). If not specified, it matches all permissions (same as `ANY`). |
| principal  | No       | The principal to match (e.g., `User:alice`, `Role:admin`). If not specified, it matches all principals (same as `ANY`). |
| operation  | No       | The operation to match (e.g., `READ`, `WRITE`, `CREATE`, `DROP`, `ALTER`, `DESCRIBE`, `ANY`, `ALL`). If not specified, it matches all operations (same as `ANY`). |
| host       | No       | The host to match (e.g., `127.0.0.1`). If not specified, it matches all hosts (same as `ANY`). |


## Extending Authorization Methods (For Developers)

Fluss supports custom authorization logic through its plugin architecture.

Steps to Implement a Custom Authorization Logic:
1. **Implement `AuthorizationPlugin` Interfaces**.
2.  **Server-Side Plugin Installation**:
    Build the plugin as a standalone JAR and copy it to the Fluss server’s plugin directory: `<FLUSS_HOME>/plugins/<custom_auth_plugin>/`. The server will automatically load the plugin at startup.
3. **Configure the Desired Protocol**: Set  `org.apache.fluss.server.authorizer.AuthorizationPlugin.identifier` as the value of `authorizer.type` in the Fluss server configuration file.
