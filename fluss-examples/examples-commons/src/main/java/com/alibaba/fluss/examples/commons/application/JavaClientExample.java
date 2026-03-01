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
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.DataTypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
/** Example Java Client usage for Fluss demonstrating streaming operations. */
public class JavaClientExample {
    private static final Logger logger = LoggerFactory.getLogger(JavaClientExample.class);

    public static void main(String[] args) throws Exception {

        logger.info("Starting {}", JavaClientExample.class.getSimpleName());

        String hostname = System.getenv("FLUSS_HOST");
        String channel = "ENV";

        if (args.length > 0) {
            logger.info("Default server host changed via CLI to: {}", args[0]);
            hostname = args[0];
            channel = "CLI";
        }

        if (hostname.isEmpty()) {
            hostname = "localhost:9123";
            channel = "DEFAULT";
        }

        logger.info("Default server host setup via {} to: {}", hostname, channel);

        // creating Connection object to connect with Fluss cluster
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", hostname);

        Table table;
        try (Connection connection = ConnectionFactory.createConnection(conf)) {

            // Create database descriptor
            DatabaseDescriptor databaseDescriptor =
                    DatabaseDescriptor.builder()
                            .comment("This is a test database for Fluss Java Client")
                            .customProperty("owner", "data-team")
                            .build();

            Schema schema =
                    Schema.newBuilder()
                            .column("key", DataTypes.STRING())
                            .column("value", DataTypes.INT())
                            .build();

            // Use the schema in a table descriptor
            TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();

            // obtain Admin instance from the Connection
            Admin admin = connection.getAdmin();

            // Create database (true means ignore if exists)
            admin.createDatabase("my_db", databaseDescriptor, true).get();

            TablePath tablePath = TablePath.of("my_db", "basic_table");

            admin.createTable(tablePath, tableDescriptor, true).get();

            // obtain Table instance from the Connection
            table = connection.getTable(tablePath);
            logger.info("Table created successfully {}", table.getTableInfo());

            admin.dropTable(tablePath, false).get();
            logger.info("Table dropped successfully {}", tablePath);
        }
    }
}
