---
title: Secure Your Fluss Cluster
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

#  Secure Your Fluss Cluster in Minutes
This guide demonstrates how to secure your Fluss cluster using two practical examples:
1. Securing a Fluss Cluster within a Department with Different Roles
2. Enabling Multi-Tenant Isolation in a Fluss Cluster

These scenarios will help you understand how to configure authentication and authorization, manage access control, and implement data isolation in real-world use cases.

## Example 1: Secure Your Fluss Cluster Within a Department Using Role-Based Access Control
In this example, we assume there are three roles within a department:
* `admin`: A superuser who can manage the entire Fluss cluster.
* `producer`: A role that is allowed to write data only.
* `consumer`: A role that is allowed to read data only.
### Prepare Environment
#### Create a `docker-compose.ym`l file
The following `docker-compose.yml` file sets up a Fluss cluster consisting of one CoordinatorServer and one TabletServer.

It uses SASL/PLAIN for user authentication and defines three users: admin, producer, and consumer. The admin user has full administrative privileges.

```yaml
services:
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
        advertised.listeners: CLIENT://localhost:9123
        internal.listener.name: INTERNAL
        remote.data.dir: /tmp/fluss/remote-data
        # security properties
        security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
        security.sasl.enabled.mechanisms: PLAIN
        security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_producer="producer-pass" user_consumer="consumer-pass";
        authorizer.enabled: true
        super.users: User:admin
    ports:
      - "9123:9123"
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
        advertised.listeners: CLIENT://localhost:9124
        internal.listener.name: INTERNAL
        tablet-server.id: 0
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
        # security properties
        security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
        security.sasl.enabled.mechanisms: PLAIN
        security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_producer="producer-pass" user_consumer="consumer-pass";
        authorizer.enabled: true
        super.users: User:admin
    ports:
        - "9124:9123"
    volumes:
      - shared-tmpfs:/tmp/fluss
  zookeeper:
    restart: always
    image: zookeeper:3.9.2

volumes:
  shared-tmpfs:
    driver: local
    driver_opts:
      type: "tmpfs"
      device: "tmpfs"
```

#### Launch the components

Save the `docker-compose.yaml` script and execute the `docker compose up -d` command in the same directory
to create the cluster.

Run the below command to check the container status:

```bash
docker container ls -a
```

#### Prepare Flink Environment
##### Start Flink Cluster
You can start a Flink standalone cluster refer to [Flink Environment Preparation](engine-flink/getting-started.md#preparation-when-using-flink-sql-client)

**Note**: Make sure the [Fluss connector jar](/downloads/) already has copied to the `lib` directory of your Flink home.
```shell
bin/start-cluster.sh 
```


##### Enter into SQL-Client
Use the following command to enter the Flink SQL CLI Container:
```shell
bin/sql-client.sh
```

### Create Catalogs for Each User
Create separate catalogs for each user:
```sql title="Flink SQL"
CREATE CATALOG admin_catalog WITH (
'type' = 'fluss',
'bootstrap.servers' = 'localhost:9123',
'client.security.protocol' = 'SASL',
'client.security.sasl.mechanism' = 'PLAIN',
'client.security.sasl.username' = 'admin',
'client.security.sasl.password' = 'admin-pass'
);
```

```sql title="Flink SQL"
CREATE CATALOG producer_catalog WITH (
'type' = 'fluss',
'bootstrap.servers' = 'localhost:9123',
'client.security.protocol' = 'SASL',
'client.security.sasl.mechanism' = 'PLAIN',
'client.security.sasl.username' = 'producer',
'client.security.sasl.password' = 'producer-pass'
);

```

```sql title="Flink SQL"
CREATE CATALOG consumer_catalog WITH (
'type' = 'fluss',
'bootstrap.servers' = 'localhost:9123',
'client.security.protocol' = 'SASL',
'client.security.sasl.mechanism' = 'PLAIN',
'client.security.sasl.username' = 'consumer',
'client.security.sasl.password' = 'consumer-pass'
);
```


### Add ACLs for Producer and Consumer
As the `admin` user, add ACLs to grant permissions:

Allow `produce`r to write data:
```sql
-- This can used for flink 1.18 and above.
CALL admin_catalog.sys.add_acl(
    'cluster', 
    'ALLOW',
    'User:producer', 
    'WRITE',
    '*'
);

-- This can only used for flink 1.19 and above.
CALL admin_catalog.sys.add_acl(
    resource => 'cluster', 
    permission => 'ALLOW',
    principal => 'User:producer', 
    operation => 'WRITE',
    host => '*'
);
```

Allow `consumer` to read data:
```sql
-- This can used for flink 1.18 and above.
CALL admin_catalog.sys.add_acl(
    'cluster', 
    'ALLOW',
    'User:consumer', 
    'READ',
    '*'
);

-- This can only used for flink 1.19 and above.
CALL admin_catalog.sys.add_acl(
    resource => 'cluster', 
    permission => 'ALLOW',
    principal => 'User:consumer', 
    operation => 'READ',
    host => '*'
);
```

Lookup the ACLs:
```sql
CALL admin_catalog.sys.list_acl(
    'cluster', 
    'ANY',
    'ANY', 
    'ANY',
    'ANY'
);

CALL admin_catalog.sys.list_acl(
    resource => 'cluster'
);
```
it will show like:
```text title="result"
+------------------------------------------------------------------------------------------------------+
|                                                                                               result |
+------------------------------------------------------------------------------------------------------+
|  resourceType="fluss-cluster";permission="ALLOW";principal="User:consumer";operation="READ";host="*" |
| resourceType="fluss-cluster";permission="ALLOW";principal="User:producer";operation="WRITE";host="*" |
+------------------------------------------------------------------------------------------------------+
2 rows in set
```

### Create Tables Using Different Users
Only the `admin` user can create tables:
```sql
CREATE TABLE `admin_catalog`.`fluss`.`fluss_order` (
     `order_key`  INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```
```text title="result"
[INFO] Execute statement succeeded.
```

The `producer` user cannot create tables:
```sql
CREATE TABLE `producer_catalog`.`fluss`.`fluss_order1`(
    `order_key`  INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```
```text title="result"
[ERROR] Could not execute SQL statement. Reason:
com.alibaba.fluss.exception.AuthorizationException: Principal FlussPrincipal{name='producer', type='User'} have no authorization to operate CREATE on resource Resource{type=DATABASE, name='fluss'} 
```


The `consumer` user also cannot create tables:
```sql
CREATE TABLE `consumer_catalog`.`fluss`.`fluss_order2`(
    `order_key`  INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```
```text title="result"
[ERROR] Could not execute SQL statement. Reason:
com.alibaba.fluss.exception.AuthorizationException: Principal FlussPrincipal{name='consumer', type='User'} have no authorization to operate CREATE on resource Resource{type=DATABASE, name='fluss'} 
```



### Write Data Using the Producer
Write data using the `producer` user:
```sql
INSERT INTO `producer_catalog`.`fluss`.`fluss_order` VALUES (1, 1.0);
```
The job should succeed as shown in the Flink UI.


Attempting to write data using the `consumer` user will fail in the Flink UI:
```sql
INSERT INTO `consumer_catalog`.`fluss`.`fluss_order` VALUES (1, 1.0);
```
```text title="result"
Caused by: java.util.concurrent.CompletionException: com.alibaba.fluss.exception.AuthorizationException: No WRITE permission among all the tables: [fluss.fluss_order]
```

### Read Data Using the Consumer

Read data using the `consumer` user:
```sql
SET 'execution.runtime-mode' = 'batch';
-- use tableau result mode
SET 'sql-client.execution.result-mode' = 'tableau';
select * from `consumer_catalog`.`fluss`.`fluss_order` limit 10;
```
```text title="result"
+-----------+-------------+
| order_key | total_price |
+-----------+-------------+
|         1 |        1.00 |
+-----------+-------------+
1 row in set (5.27 seconds)
```


Attempting to read data using the `producer` user will fail:
```sql
SET 'execution.runtime-mode' = 'batch';
-- use tableau result mode
SET 'sql-client.execution.result-mode' = 'tableau';
select * from `producer_catalog`.`fluss`.`fluss_order` limit 10;
```
```text title="result"
[ERROR] Could not execute SQL statement. Reason:
com.alibaba.fluss.exception.AuthorizationException: No permission to READ table fluss_order in database fluss
```

## Example 2: Implement Multi-Tenant Isolation in a Fluss Cluster
This example shows how to enable multi-tenant isolation in a Fluss cluster.

We'll demonstrate two departments — `marketing` and `finance` — each with its own dedicated database. The cluster includes the following users:
* `admin`: A superuser with full access.
* `marketing`: A user who can only access the marketing database.
* `finance`: A user who can only access the finance database.

### Prepare Environment
All the steps are same as Example 1, but update the JAAS configuration to include the new users:
```yaml
services:
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
        advertised.listeners: CLIENT://localhost:9123
        internal.listener.name: INTERNAL
        remote.data.dir: /tmp/fluss/remote-data
        # security properties
        security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
        security.sasl.enabled.mechanisms: PLAIN
        security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_marketing="marketing-pass" user_finance="finance-pass";
        authorizer.enabled: true
        super.users: User:admin
    ports:
      - "9123:9123"
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
        advertised.listeners: CLIENT://localhost:9124
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
    ports:
        - "9124:9123"
    volumes:
      - shared-tmpfs:/tmp/fluss
  zookeeper:
    restart: always
    image: zookeeper:3.9.2

volumes:
  shared-tmpfs:
    driver: local
    driver_opts:
      type: "tmpfs"
      device: "tmpfs"
```

### Create Catalogs for Each User
Create separate catalogs for the `admin`, `marketing`, and `finance` users:
```sql title="Flink SQL"
CREATE CATALOG admin_catalog WITH (
'type' = 'fluss',
'bootstrap.servers' = 'localhost:9123',
'client.security.protocol' = 'SASL',
'client.security.sasl.mechanism' = 'PLAIN',
'client.security.sasl.username' = 'admin',
'client.security.sasl.password' = 'admin-pass'
);
```

```sql title="Flink SQL"
CREATE CATALOG marketing_catalog WITH (
'type' = 'fluss',
'bootstrap.servers' = 'localhost:9123',
'client.security.protocol' = 'SASL',
'client.security.sasl.mechanism' = 'PLAIN',
'client.security.sasl.username' = 'marketing',
'client.security.sasl.password' = 'marketing-pass'
);

```

```sql title="Flink SQL"
CREATE CATALOG finance_catalog WITH (
'type' = 'fluss',
'bootstrap.servers' = 'localhost:9123',
'client.security.protocol' = 'SASL',
'client.security.sasl.mechanism' = 'PLAIN',
'client.security.sasl.username' = 'finance',
'client.security.sasl.password' = 'finance-pass'
);
```

### Create Databases and Set ACLs
As the `admin` user, create two databases and assign appropriate ACLs:
```sql title="Flink SQL"
CREATE DATABASE `admin_catalog`.`marketing`;
CALL admin_catalog.sys.add_acl(
    'cluster.marketing', 
    'ALLOW',
    'User:marketing', 
    'ALL',
    '*'
);


CREATE DATABASE `admin_catalog`.`finance`;
CALL admin_catalog.sys.add_acl(
    'cluster.finance', 
    'ALLOW',
    'User:finance', 
    'ALL',
    '*'
);

```

### Granularity of Database Visibility

The `marketing` user can only see the `marketing` database
```sql title="Flink SQL"
use catalog marketing_catalog;
show databases;
```
```text title="result"
+---------------+
| database name |
+---------------+
|     marketing |
+---------------+
1 row in set
```

The `finance` user can only see the `finance` database:
```sql title="Flink SQL"
use catalog finance_catalog;
show databases;
```
```text title="result"
+---------------+
| database name |
+---------------+
|       finance |
+---------------+
1 row in set
```

The `marketing` user can operate on their own database:
```sql title="Flink SQL"
CREATE TABLE `marketing_catalog`.`marketing`.`order` (
     `order_key`  INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```
```text title="result"
[INFO] Execute statement succeeded.
```

The `finance` user cannot access the `marketing` database:
```sql title="Flink SQL"
CREATE TABLE `finance_catalog`.`marketing`.`order` (
     `order_key`  INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```
```text title="result"
[ERROR] Could not execute SQL statement. Reason:
com.alibaba.fluss.exception.AuthorizationException: Principal FlussPrincipal{name='finance', type='User'} have no authorization to operate CREATE on resource Resource{type=DATABASE, name='marketing'} 
```


