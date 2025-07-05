/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.examples;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataTypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

// TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
/** Example Java Client usage for Fluss demonstrating streaming operations. */
public class JavaClientExample {
    private static final Logger logger = LoggerFactory.getLogger(JavaClientExample.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String hostname = "localhost:9123";
        logger.info("Starting {}", JavaClientExample.class.getSimpleName());

        if (args.length > 0) {
            logger.info(String.format("Default server host changed via CLI to: %s", args[0]));
            hostname = args[0];
        } else if (System.getenv("FLUSS_HOST") != null) {
            logger.info(
                    String.format(
                            "Default server host changed via ENV to: %s",
                            System.getenv("FLUSS_HOST")));
            hostname = System.getenv("FLUSS_HOST");
        }

        // creating Connection object to connect with Fluss cluster
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", hostname);
        Connection connection = ConnectionFactory.createConnection(conf);

        class User {
            private final String id;
            private final int age;
            private final LocalDateTime createdAt;
            private final boolean isActive;

            public User(String id, int age, LocalDateTime createdAt, boolean isActive) {
                this.id = id;
                this.age = age;
                this.createdAt = createdAt;
                this.isActive = isActive;
            }

            public String getId() {
                return id;
            }

            public int getAge() {
                return age;
            }

            public LocalDateTime getCreatedAt() {
                return createdAt;
            }

            public boolean isActive() {
                return isActive;
            }
        }

        // Create database descriptor
        DatabaseDescriptor databaseDescriptor =
                DatabaseDescriptor.builder()
                        .comment("This is a test database for Fluss Java Client")
                        .customProperty("owner", "data-team")
                        .build();

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("created_at", DataTypes.TIMESTAMP())
                        .column("is_active", DataTypes.BOOLEAN())
                        .primaryKey("id")
                        .build();

        // Use the schema in a table descriptor
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1, "id") // Distribute by the id column with 1 buckets
                        .build();

        //        try {
        // obtain Admin instance from the Connection
        Admin admin = connection.getAdmin();

        // Create database (true means ignore if exists)
        admin.createDatabase("my_db", databaseDescriptor, true).get();

        TablePath tablePath = TablePath.of("my_db", "user_table");

        admin.createTable(tablePath, tableDescriptor, true).get();
        logger.info("Table created successfully");

        // obtain Table instance from the Connection
        Table table = connection.getTable(tablePath);
        TableInfo tblInfo = table.getTableInfo();
        logger.info(
                String.format(
                        "BucketKeys Count: %s Table Name: %s",
                        tblInfo.getBucketKeys().size(), tblInfo.getTablePath().getTableName()));

        table.newUpsert().createWriter();

        List<User> users = new ArrayList<>();
        users.add(new User("1", 20, LocalDateTime.now(), true));
        users.add(new User("2", 22, LocalDateTime.now(), true));
        users.add(new User("3", 23, LocalDateTime.now(), true));
        users.add(new User("4", 24, LocalDateTime.now(), true));
        users.add(new User("5", 25, LocalDateTime.now(), true));

        List<GenericRow> rows =
                users.stream()
                        .map(
                                user -> {
                                    GenericRow row = new GenericRow(4);
                                    row.setField(0, BinaryString.fromString(user.getId()));
                                    row.setField(1, user.getAge());
                                    row.setField(
                                            2, TimestampNtz.fromLocalDateTime(user.getCreatedAt()));
                                    row.setField(3, user.isActive());
                                    return row;
                                })
                        .collect(Collectors.toList());

        logger.info("Upserting rows to the table");
        UpsertWriter writer = table.newUpsert().createWriter();

        // upsert() is a non-blocking call that sends data to Fluss server with batching and timeout
        rows.forEach(writer::upsert);

        // call flush() to blocking the thread until all data is written successfully
        writer.flush();

        LogScanner logScanner = table.newScan().createLogScanner();

        int numBuckets = table.getTableInfo().getNumBuckets();
        logger.info(String.format("Number of buckets: %s", numBuckets));

        for (int i = 0; i < numBuckets; i++) {
            logger.info(String.format("Subscribing to bucket %s", i));
            logScanner.subscribeFromBeginning(i);
        }

        while (true) {
            logger.info("Polling for records...");
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    InternalRow row = record.getRow();
                    logger.info(String.format("Subscribing to bucket %s", row.toString()));
                }
            }
        }
    }
}
