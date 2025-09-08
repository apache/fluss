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
| `DELETE` | Allows deleting a resource (e.g., deleting a database or table).|
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
Below is a summary of the currently public protocols and their relationship with operations and resource types:
| Protocol | Operations | Resources | Note |
| --- | --- | --- | --- |
| CREATE_DATABASE | CREATE | Cluster | |
| DROP_DATABASE | DELETE | Database | |
| LIST_DATABASES | DESCRIBE | Database | Only databases that the user has permission to access are returned. Databases for which the user lacks sufficient privileges are automatically filtered from the results.  |
| CREATE_TABLE | CREATE | Database | |
| DROP_TABLE | DELETE | Table | |
| GET_TABLE_INFO | DESCRIBE | Table | |
| LIST_TABLES | DESCRIBE | Table | Only tables that the user has permission to access are returned. Tables for which the user lacks sufficient privileges are automatically filtered from the results. |
| LIST_PARTITION_INFOS | DESCRIBE | Table | Only partitions that the user has permission to access are returned. Partitions for which the user lacks sufficient privileges are automatically filtered from the results. |
| GET_METADATA | DESCRIBE | Table | Only metadata that the user has permission to access is returned. Metadata for which the user lacks sufficient privileges is automatically filtered from the results.|
| PRODUCE_LOG | WRITE | Table | |
| FETCH_LOG | READ | Table | |
| PUT_KV | WRITE | Cluster | |
| LOOKUP | READ | Cluster | |
| INIT_WRITER | WRITE | Table | User has the INIT_WRITER permission if it has the WRITE permission for one of the requested tables. |
| LIMIT_SCAN | READ | Table | |
| PREFIX_LOOKUP | READ | Table | |
| GET_DATABASE_INFO | DESCRIBE | Database | |
| CREATE_PARTITION | WRITE | Table | |
| DROP_PARTITION | WRITE | Table | |
| CREATE_ACLS | ALTER | Cluster | |
| DROP_ACLS | ALTER | Cluster | |
| LIST_ACLS | DESCRIBE | Cluster | |

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

| Parameter  | Required | Description                                                                                                   |
|------------|----------|---------------------------------------------------------------------------------------------------------------|
| resource   | Yes      | The resource to apply the ACL to (e.g., `cluster`, `cluster.db1`, `cluster.db1.table1`)                             |
| permission | Yes      | The permission to grant to the principal on the resource, currently only `ALLOW` is supported.                    |
| principal  | Yes      | The principal to apply the ACL to (e.g., `User:alice`, `Role:admin`)                                              |
| operation  | Yes      | The operation to allow or deny for the principal on the resource (e.g., `READ`, `WRITE`, `CREATE`, `DELETE`, `ALTER`, `DESCRIBE`, `ANY`, `ALL`) |
| host       | No       | The host to apply the ACL to (e.g., `127.0.0.1`). If not specified, the ACL applies to all hosts (same as `*`)  |

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
| Parameter  | Required | Description                                                                                                                                                                                           |
|------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| resource   | NO       | The resource to apply the ACL to (e.g., `cluster`, `cluster.db1`, `cluster.db1.table1`). If not specified, it will filter all the resource (same as `ANY`)                              |
| permission | NO       | The permission to grant or deny to the principal on the resource (e.g., `ALLOW`, `DENY`). If If not specified, it will filter all the permission(same as `ANY`)                                           |
| principal  | NO       | The principal to apply the ACL to (e.g., `User:alice`, `Role:admin`). If If not specified, it will filter all the principal(same as `ANY`)                                                                |
| operation  | NO       | The operation to allow or deny for the principal on the resource (e.g., `READ`, `WRITE`, `CREATE`, `DELETE`, `ALTER`, `DESCRIBE`, `ANY`, `ALL`). If If not specified, it will filter all the operation(same as `ANY`) |
| host       | NO       | The host to apply the ACL to (e.g., `127.0.0.1`). If not specified, the ACL applies to all hosts( same as `ANY`)                                                                                        |

### List ACL
List ACL will return a list of ACLs that match the specified criteria.
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
| Parameter  | Required | Description                                                                                                                                                                                           |
|------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| resource   | NO       | The resource to apply the ACL to (e.g., `cluster`, `cluster.db1`, `cluster.db1.table1`). If If not specified, it will filter all the resource(same as `ANY`)                              |
| permission | NO       | The permission to grant or deny to the principal on the resource (e.g., `ALLOW`, `DENY`). If If not specified, it will filter all the permission(same as `ANY`)                                           |
| principal  | NO       | The principal to apply the ACL to (e.g., `User:alice`, `Role:admin`). If If not specified, it will filter all the principal(same as `ANY`)                                                                |
| operation  | NO       | The operation to allow or deny for the principal on the resource (e.g., `READ`, `WRITE`, `CREATE`, `DELETE`, `ALTER`, `DESCRIBE`, `ANY`, `ALL`). If If not specified, it will filter all the operation(same as `ANY`) |
| host       | NO       | The host to apply the ACL to (e.g., `127.0.0.1`). If not specified, the ACL applies to all hosts( same as `ANY`)                                                                                        |


## Extending Authorization Methods (For Developers)

Fluss supports custom authorization logic through its plugin architecture.

Steps to Implement a Custom Authorization Logic:
1. **Implement `AuthorizationPlugin` Interfaces**.
2.  **Server-Side Plugin Installation**:
    Build the plugin as a standalone JAR and copy it to the Fluss server’s plugin directory: `<FLUSS_HOME>/plugins/<custom_auth_plugin>/`. The server will automatically load the plugin at startup.
3. **Configure the Desired Protocol**: Set  `org.apache.fluss.server.authorizer.AuthorizationPlugin.identifier` as the value of `authorizer.type` in the Fluss server configuration file.

