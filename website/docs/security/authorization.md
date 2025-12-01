---
sidebar_label: Authorization and ACLs
title: Authorization and ACLs
sidebar_position: 3
---

# Authorization and ACLs

## Configuration 
Fluss provides a **pluggable authorization framework** that uses **Access Control Lists (ACLs)** to determine whether a given **Fluss Principal** is allowed to perform an operation on a specific resource.


| Option             | Type    | Default Value | Description                                                                                                                                                                                                                                                                                                    |
|--------------------|---------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| authorizer.enabled | Boolean | false         | Enables the authorization feature. The default value is `false`, which means all operations and resources are accessible to all users and applications.                                                                                                                                                                                                                                                         |
| authorizer.type    | String  | default       | Specifies the type of authorizer to be used for access control. This value corresponds to the identifier of the authorization plugin. The default value, `default`, indicates the built-in authorizer implementation. Custom authorizers can be implemented by providing a matching plugin identifier. |


## Core Components of ACLs

Fluss uses an **Access Control List (ACL)** mechanism to enforce **fine-grained permissions** on resources such as clusters, databases, and tables. This allows administrators to define:

- **Who** (`Principal`) can perform 
- **What** (`Operation`) actions can be performed
- **On which** (`Resource`) objects

Fluss ACLs are defined using the following general format:
```text
Principal {P} is Allowed Operation {O} From Host {H} on any Resource {R}.
```

### Resource
In Fluss, a **Resource** represents an object to which access control can be applied. **Resources are organized in a hierarchical structure**, enabling **fine-grained permission management** and **permission inheritance** from higher-level scopes. For example, database-level permissions apply to all tables within that database.

There are three main types of resources:

| Resource Type | Resource Name Format Example | Description                                                                                                             |
|---------------|------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| Cluster       | cluster                      | Represents the entire Fluss cluster and is used for cluster-wide permissions.                      |
| Database      | default_db                   | Represents a specific database within the Fluss cluster and is used for database-level permissions. |
| Table         | default_db.default_table     | Represents a specific table within a database and is used for table-level permissions.                 |

This hierarchy follows this pattern:

```text
Cluster
 └── Database
     └── Table
```

### Operation
In Fluss, an **OperationType** defines the type of action a principal (user or role) attempts to perform on a resource (cluster, database, or table).

| Operation Type | Description |
|----------------| --- |
| `ANY`            | Matches any operation type; used exclusively in filters or queries to match ACL entries. **Do not use** when granting actual permissions.|
| `ALL`            | Grants permission for all operations on a resource. |
| `READ` | Allows reading data from a resource (e.g., querying tables).|
| `WRITE` | Allows writing data to a resource (e.g., inserting or updating tables).|
| `CREATE` | Allows creating a new resource (e.g., database or table).|
| `DROP` | Allows dropping a resource (e.g., database or table).|
| `ALTER` | Allows modifying the structure of a resource (e.g., altering table schema).|
| `DESCRIBE` | Allows describing a resource (e.g., retrieving metadata about a table).|


Fluss implements a **permission inheritance model**, where certain operations imply others, reducing redundancy in ACL rules:
* `ALL` implies all other operations.
* `READ`, `WRITE`, `CREATE`, `DROP`, `ALTER` each imply `DESCRIBE`.

### Fluss Principal
The **Fluss Principal** is a core concept in Fluss security architecture. It represents the identity of an authenticated entity (user or service) and acts as the central link between **authentication** and **authorization**. 

Once a client successfully authenticates (e.g., via `SASL/PLAIN`), a **FlussPrincipal** is created to represent that client's identity. This principal is used system-wide for access control decisions, linking **who** the user is with **what** they are allowed to do.

The **principal type** indicates the category (e.g., `User`, `Group`, `Role`), while the **name** identifies the specific entity. By default, the simple authorizer uses `User` as the type, but custom authorizers can extend this to support **role-based or group-based ACLs**.

Examples:
* `new FlussPrincipal("admin", "User")` – a standard user principal.
* `new FlussPrincipal("admins", "Group")` – a group-based principal.

## Operations and Resources on Protocols
Below is a summary of the currently public protocols and their relationship with operations and resource types:
| Protocol | Operations | Resources | Note |
| --- | --- | --- | --- |
| CREATE_DATABASE | CREATE | Cluster | |
| DROP_DATABASE | DROP | Database | |
| LIST_DATABASES | DESCRIBE | Database | Only databases the user has permission to access are returned; others are filtered.  |
| CREATE_TABLE | CREATE | Database | |
| DROP_TABLE | DROP | Table | |
| GET_TABLE_INFO | DESCRIBE | Table | |
| LIST_TABLES | DESCRIBE | Table | Only tables the user has permission to access are returned. |
| LIST_PARTITION_INFOS | DESCRIBE | Table | Only partitions the user has permission to access are returned. |
| GET_METADATA | DESCRIBE | Table | Only accessible metadata is returned. |
| PRODUCE_LOG | WRITE | Table | |
| FETCH_LOG | READ | Table | |
| PUT_KV | WRITE | Cluster | |
| LOOKUP | READ | Cluster | |
| INIT_WRITER | WRITE | Table | Requires WRITE permission on one of the requested tables. |
| LIMIT_SCAN | READ | Table | |
| PREFIX_LOOKUP | READ | Table | |
| GET_DATABASE_INFO | DESCRIBE | Database | |
| CREATE_PARTITION | WRITE | Table | |
| DROP_PARTITION | WRITE | Table | |
| CREATE_ACLS | ALTER | Cluster | |
| DROP_ACLS | ALTER | Cluster | |
| LIST_ACLS | DESCRIBE | Cluster | |

## ACL Operation
Fluss provides a **FLINK SQL interface** to manage ACLs using the `CALL` statement. Administrators can dynamically control access permissions for principals on clusters, databases, and tables.

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
| resource   | Yes      | Resource to apply the ACL (e.g., `cluster`, `cluster.db1`, `cluster.db1.table1`).                             |
| permission | Yes      | Permission to grant (currently only `ALLOW` is supported).                    |
| principal  | Yes      | Principal to apply ACL to (e.g., `User:alice`, `Role:admin`).                                              |
| operation  | Yes      | Operation to allow or deny (e.g., `READ`, `WRITE`, `CREATE`, `DROP`, `ALTER`, `DESCRIBE`, `ANY`, `ALL`). |
| host       | No       | Host to apply the ACL (e.g., `127.0.0.1`). If not specified or set to `*`, the ACL applies to all hosts.  |

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

> **Note:**  Parameters follow the same rules as `add_acl`. If omitted, filters default to `ANY`.

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

> **Note:**  Parameters follow the same rules as `add_acl`. If omitted, filters default to `ANY`.


## Extending Authorization Methods (For Developers)

Fluss supports **custom authorization logic** via plugins.

**Steps to implement a custom plugin**:
1. Implement the `AuthorizationPlugin` interface.
2.  **Server-Side Installation**:
    Build the plugin as a standalone JAR and copy it to `<FLUSS_HOME>/plugins/<custom_auth_plugin>/`. The server loads it at startup.
3. **Configure the Plugin**: Set `authorizer.type` in the server configuration to match the plugin's `identifier` (i.e., the value returned by ``org.apache.fluss.server.authorizer.AuthorizationPlugin.identifier``). 

> **Note:** `authorizer.type` must match your plugin identifier to be loaded at startup.

