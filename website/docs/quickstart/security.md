---
title: Secure Your Fluss Cluster
sidebar_position: 2
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

#  Secure Your Fluss Cluster in Minutes
This guide demonstrates how to secure your Fluss cluster using two practical examples:
1. Securing a Fluss cluster within a department with different roles
2. Enabling multi-tenant isolation in a Fluss cluster

These scenarios will help you understand how to configure authentication and authorization, manage access control, and implement data isolation in real-world use cases.

## Example 1: Secure Fluss with Different Roles
In this example, we assume there are three users within a department:
* `admin`: A superuser who can manage the entire Fluss cluster.
* `developer`: A user that is allowed to read and write data.
* `consumer`: A user that is allowed to read data only.

### Environment Setup
#### Prerequisites

Before proceeding with this guide, ensure that [Docker](https://docs.docker.com/engine/install/) and the [Docker Compose plugin](https://docs.docker.com/compose/install/linux/) are installed on your machine.
All commands were tested with Docker version 27.4.0 and Docker Compose version v2.30.3.

:::note
We encourage you to use a recent version of Docker and [Compose v2](https://docs.docker.com/compose/releases/migrate/) (however, Compose v1 might work with a few adaptations).
:::

#### Starting required components
We will use docker compose to spin up the required components for this tutorial.

1. Create a working directory for this guide.
```shell
mkdir fluss-quickstart-security
cd fluss-quickstart-security
```

2. Create a `docker-compose.yml` file with the following content:

```yaml
services:
  #begin Fluss cluster
  coordinator-server:
    image: fluss/fluss:$FLUSS_VERSION$
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://coordinator-server:0, CLIENT://coordinator-server:9123
        internal.listener.name: INTERNAL
        remote.data.dir: /tmp/fluss/remote-data
        # security properties
        security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
        security.sasl.enabled.mechanisms: PLAIN
        security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_developer="developer-pass" user_consumer="consumer-pass";
        authorizer.enabled: true
        super.users: User:admin
  tablet-server:
    image: fluss/fluss:$FLUSS_VERSION$
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://tablet-server:0, CLIENT://tablet-server:9123
        internal.listener.name: INTERNAL
        tablet-server.id: 0
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
        # security properties
        security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
        security.sasl.enabled.mechanisms: PLAIN
        security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_developer="developer-pass" user_consumer="consumer-pass";
        authorizer.enabled: true
        super.users: User:admin
    volumes:
      - shared-tmpfs:/tmp/fluss
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  #end
  #begin Flink cluster
  jobmanager:
    image: fluss/quickstart-flink:1.20-0.7-SNAPSHOT
    ports:
      - "8083:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
    volumes:
      - shared-tmpfs:/tmp/paimon
  taskmanager:
    image: fluss/quickstart-flink:1.20-0.7-SNAPSHOT
    depends_on:
      - jobmanager
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 10
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.framework.off-heap.size: 256m
    volumes:
      - shared-tmpfs:/tmp/paimon
  #end

volumes:
  shared-tmpfs:
    driver: local
    driver_opts:
      type: "tmpfs"
      device: "tmpfs"
```


The Docker Compose environment consists of the following containers:
- **Fluss Cluster:** a Fluss `CoordinatorServer`, a Fluss `TabletServer` and a `ZooKeeper` server.
  It uses SASL/PLAIN for user authentication and defines three users: `admin`, `developer`, and `consumer`. The `admin` user is a `super.users` who has full administrative privileges on the Fluss cluster.
- **Flink Cluster**: a Flink `JobManager` and a Flink `TaskManager` container to execute queries.

**Note:** The `fluss/quickstart-flink` image is based on [flink:1.20.1-java17](https://hub.docker.com/layers/library/flink/1.20-java17/images/sha256:bf1af6406c4f4ad8faa46efe2b3d0a0bf811d1034849c42c1e3484712bc83505) and
includes the [fluss-flink connector](engine-flink/getting-started.md) to simplify this guide.

3. To start all containers, run:
```shell
docker compose up -d
```
This command automatically starts all the containers defined in the Docker Compose configuration in detached mode.

Run
```shell
docker container ls -a
```
to check whether all containers are running properly.

You can also visit http://localhost:8083/ to see if Flink is running normally.

### Enter into SQL-Client
First, use the following command to enter the Flink SQL CLI Container:
```shell
docker compose exec jobmanager ./sql-client
```

### Create Catalogs for Each User
Create separate catalogs for each user:
```sql title="Flink SQL"
CREATE CATALOG admin_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123',
    'client.security.protocol' = 'SASL',
    'client.security.sasl.mechanism' = 'PLAIN',
    'client.security.sasl.username' = 'admin',
    'client.security.sasl.password' = 'admin-pass'
);
```

```sql title="Flink SQL"
CREATE CATALOG developer_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123',
    'client.security.protocol' = 'SASL',
    'client.security.sasl.mechanism' = 'PLAIN',
    'client.security.sasl.username' = 'developer',
    'client.security.sasl.password' = 'developer-pass'
);

```

```sql title="Flink SQL"
CREATE CATALOG consumer_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123',
    'client.security.protocol' = 'SASL',
    'client.security.sasl.mechanism' = 'PLAIN',
    'client.security.sasl.username' = 'consumer',
    'client.security.sasl.password' = 'consumer-pass'
);
```


### Add ACLs for Users
As the `admin` user, add ACLs to grant permissions:

Allow `developer` user to read and write data:
```sql
CALL admin_catalog.sys.add_acl(
    resource => 'cluster', 
    permission => 'ALLOW',
    principal => 'User:developer', 
    operation => 'WRITE'
);

CALL admin_catalog.sys.add_acl(
    resource => 'cluster', 
    permission => 'ALLOW',
    principal => 'User:developer', 
    operation => 'READ'
);
```

Allow `consumer` user to read data:
```sql
CALL admin_catalog.sys.add_acl(
    resource => 'cluster', 
    permission => 'ALLOW',
    principal => 'User:consumer', 
    operation => 'READ'
);
```

Lookup the ACLs:
```sql
CALL admin_catalog.sys.list_acl(
    resource => 'cluster'
);
```
Output will show like:

```text
+---------------------------------------------------------------------------------------------+
|                                                                                      result |
+---------------------------------------------------------------------------------------------+
|  resource="cluster";permission="ALLOW";principal="User:developer";operation="READ";host="*" |
| resource="cluster";permission="ALLOW";principal="User:developer";operation="WRITE";host="*" |
|   resource="cluster";permission="ALLOW";principal="User:consumer";operation="READ";host="*" |
+---------------------------------------------------------------------------------------------+
3 rows in set
```

### Create Tables Using Different Users
Only the `admin` user can create tables:
```sql
-- switch to admin user context
USE CATALOG admin_catalog;

-- create table using admin credentials
CREATE TABLE fluss_order (
    `order_key`  INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```
**Output:**

```text
[INFO] Execute statement succeeded.
```

The `developer` user cannot create tables:
```sql
-- switch to developer user context
USE CATALOG developer_catalog;

-- create table using developer credentials
CREATE TABLE fluss_order1(
    `order_key`  INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```
**Output:**

```text
[ERROR] Could not execute SQL statement. Reason:
com.alibaba.fluss.exception.AuthorizationException: Principal FlussPrincipal{name='developer', type='User'} have no authorization to operate CREATE on resource Resource{type=DATABASE, name='fluss'} 
```


The `consumer` user also cannot create tables:

```sql
-- switch to consumer user context
USE CATALOG consumer_catalog;

-- create table using consumer credentials
CREATE TABLE fluss_order2(
    `order_key`  INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```
**Output:**

```text
[ERROR] Could not execute SQL statement. Reason:
com.alibaba.fluss.exception.AuthorizationException: Principal FlussPrincipal{name='consumer', type='User'} have no authorization to operate CREATE on resource Resource{type=DATABASE, name='fluss'} 
```



### Write Data 
Write data using the `developer` user:
```sql
-- switch to developer user context
USE CATALOG developer_catalog;

-- write data using developer credentials
INSERT INTO fluss_order VALUES (1, 1.0);
```
The job should succeed as shown in the Flink UI.


Attempting to write data using the `consumer` user will fail in the Flink UI:
```sql
-- switch to consumer user context
USE CATALOG consumer_catalog;

-- write data using consumer credentials
INSERT INTO fluss_order VALUES (1, 1.0);
```
**Output:**

```text
Caused by: java.util.concurrent.CompletionException: com.alibaba.fluss.exception.AuthorizationException: No WRITE permission among all the tables: [fluss.fluss_order]
```

### Read Data 

Read data using the `consumer` user:
```sql
SET 'execution.runtime-mode' = 'batch';
-- use tableau result mode
SET 'sql-client.execution.result-mode' = 'tableau';
    
-- switch to consumer user context
USE CATALOG consumer_catalog;
-- read data using consumer credentials
SELECT * FROM `consumer_catalog`.`fluss`.`fluss_order` LIMIT 10;
```
**Output:**
```text
+-----------+-------------+
| order_key | total_price |
+-----------+-------------+
|         1 |        1.00 |
+-----------+-------------+
1 row in set (5.27 seconds)
```


Attempting to read data using the `developer` user also get the same result:
```sql
SET 'execution.runtime-mode' = 'batch';
-- use tableau result mode
SET 'sql-client.execution.result-mode' = 'tableau';
-- switch to developer user context
USE CATALOG developer_catalog;

-- read data using developer credentials
SELECT * FROM `developer_catalog`.`fluss`.`fluss_order` LIMIT 10;
```


## Example 2: Multi-Tenant Isolation in a Fluss Cluster
This example shows how to enable multi-tenant isolation in a Fluss cluster.

We'll demonstrate two departments — `marketing` and `finance` — each with its own dedicated database. The cluster includes the following users:
* `admin`: A superuser with full access.
* `marketing`: A user who can only access the `marketing_db` database.
* `finance`: A user who can only access the `finance_db` database.

### Environment Setup
All the steps are same as Example 1, but update the JAAS configuration to include the new users:
```yaml
services:
  #begin Fluss cluster
  coordinator-server:
    image: fluss/fluss:$FLUSS_VERSION$
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://coordinator-server:0, CLIENT://coordinator-server:9123
        internal.listener.name: INTERNAL
        remote.data.dir: /tmp/fluss/remote-data
        # security properties
        security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
        security.sasl.enabled.mechanisms: PLAIN
        security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_marketing="marketing-pass" user_finance="finance-pass";
        authorizer.enabled: true
        super.users: User:admin
  tablet-server:
    image: fluss/fluss:$FLUSS_VERSION$
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://tablet-server:0, CLIENT://tablet-server:9123
        internal.listener.name: INTERNAL
        tablet-server.id: 0
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
        # security properties
        security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
        security.sasl.enabled.mechanisms: PLAIN
        security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_marketing="marketing-pass" user_finance="finance-pass";
        authorizer.enabled: true
        super.users: User:admin
    volumes:
      - shared-tmpfs:/tmp/fluss
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  #end
  #begin Flink cluster
  jobmanager:
    image: fluss/quickstart-flink:1.20-0.7-SNAPSHOT
    ports:
      - "8083:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
    volumes:
      - shared-tmpfs:/tmp/paimon
  taskmanager:
    image: fluss/quickstart-flink:1.20-0.7-SNAPSHOT
    depends_on:
      - jobmanager
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 10
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.framework.off-heap.size: 256m
    volumes:
      - shared-tmpfs:/tmp/paimon
  #end
volumes:
  shared-tmpfs:
    driver: local
    driver_opts:
      type: "tmpfs"
      device: "tmpfs"
```

### Enter into SQL-Client
First, use the following command to enter the Flink SQL CLI Container:
```shell
docker compose exec jobmanager ./sql-client
```

### Create Catalogs for Each User
Create separate catalogs for the `admin`, `marketing`, and `finance` users:
```sql title="Flink SQL"
CREATE CATALOG admin_catalog WITH (
'type' = 'fluss',
'bootstrap.servers' = 'coordinator-server:9123',
'client.security.protocol' = 'SASL',
'client.security.sasl.mechanism' = 'PLAIN',
'client.security.sasl.username' = 'admin',
'client.security.sasl.password' = 'admin-pass'
);
```

```sql title="Flink SQL"
CREATE CATALOG marketing_catalog WITH (
'type' = 'fluss',
'bootstrap.servers' = 'coordinator-server:9123',
'client.security.protocol' = 'SASL',
'client.security.sasl.mechanism' = 'PLAIN',
'client.security.sasl.username' = 'marketing',
'client.security.sasl.password' = 'marketing-pass'
);

```

```sql title="Flink SQL"
CREATE CATALOG finance_catalog WITH (
'type' = 'fluss',
'bootstrap.servers' = 'coordinator-server:9123',
'client.security.protocol' = 'SASL',
'client.security.sasl.mechanism' = 'PLAIN',
'client.security.sasl.username' = 'finance',
'client.security.sasl.password' = 'finance-pass'
);
```

### Create Databases and Set ACLs
As the `admin` user, create two databases and assign appropriate ACLs:
```sql title="Flink SQL"
CREATE DATABASE `admin_catalog`.`marketing_db`;
CALL admin_catalog.sys.add_acl(
    resource => 'cluster.marketing_db', 
    permission => 'ALLOW',
    principal => 'User:marketing', 
    operation => 'ALL'
);


CREATE DATABASE `admin_catalog`.`finance_db`;
CALL admin_catalog.sys.add_acl(
    resource => 'cluster.finance_db', 
    permission => 'ALLOW',
    principal => 'User:finance', 
    operation => 'ALL'
);
```

Lookup the ACLs:
```sql
CALL admin_catalog.sys.list_acl(
    resource => 'ANY'
);
```
Output will show like:
```text
+--------------------------------------------------------------------------------------------------------+
|                                                                                                 result |
+--------------------------------------------------------------------------------------------------------+
| resource="cluster.marketing_db";permission="ALLOW";principal="User:marketing";operation="ALL";host="*" |
|     resource="cluster.finance_db";permission="ALLOW";principal="User:finance";operation="ALL";host="*" |
+--------------------------------------------------------------------------------------------------------+
2 rows in set
```


### Granularity of Database Visibility

The `marketing` user can only see the `marketing_db` database
```sql title="Flink SQL"
-- switch to marketing user context
USE CATALOG marketing_catalog;
-- show databases using marketing user credentials
SHOW DATABASES;
```
**Output:**
```text
+---------------+
| database name |
+---------------+
|  marketing_db |
+---------------+
1 row in set
```

The `finance` user can only see the `finance_db` database:
```sql title="Flink SQL"
-- switch to finance user context
USE CATALOG finance_catalog;
-- show databases using finance user credentials
SHOW DATABASES;
```

**Output:**

```text
+---------------+
| database name |
+---------------+
|    finance_db |
+---------------+
1 row in set
```

The `marketing` user can operate on their own database:
```sql title="Flink SQL"
-- switch to marketing user context
USE CATALOG marketing_catalog;
-- create table using marketing user credentials
CREATE TABLE `marketing_db`.`order` (
    `order_key`  INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```

**Output:**

```text
[INFO] Execute statement succeeded.
```

The `finance` user cannot access the `marketing` database:
```sql title="Flink SQL"
-- switch to finance user context
USE CATALOG finance_catalog;
-- create table using finance user credentials
CREATE TABLE `marketing_db`.`order` (
    `order_key`  INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```
**Output:**

```text
[ERROR] Could not execute SQL statement. Reason:
com.alibaba.fluss.exception.AuthorizationException: Principal FlussPrincipal{name='finance', type='User'} have no authorization to operate CREATE on resource Resource{type=DATABASE, name='marketing_db'} 
```


