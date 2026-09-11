/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import com.alibaba.fluss.client.lookup.LookupResult;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.examples.commons.Descriptors;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example show how to create a Schema for a PrimaryKey table, this table type supports upsert,
 * update and delete writing operations. For reading at this example we are using a lookup, more
 * specific a PrefixLookup when it's passed a key ou set of keys from bucketing list.
 */
public class BasicPrimaryKeyTableExample {
    private static final Logger logger = LoggerFactory.getLogger(BasicPrimaryKeyTableExample.class);

    public static void main(String[] args) throws Exception {

        logger.info("Starting {}", BasicPrimaryKeyTableExample.class.getSimpleName());

        String hostname = "localhost:9123";
        String channel = "DEFAULT";

        if (System.getenv("FLUSS_HOST") != null) {
            hostname = System.getenv("FLUSS_HOST");
            channel = "ENV";
        }

        if (args.length > 0) {
            hostname = args[0];
            channel = "CLI";
        }

        logger.info("Default server host setup via {} to: {}", channel, hostname);

        // creating Connection object to connect with Fluss cluster
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", hostname);

        try (Connection connection = ConnectionFactory.createConnection(conf)) {

            // obtain Admin instance from the Connection
            Admin admin = connection.getAdmin();

            // Create database (true means ignore if exists)
            admin.createDatabase(Descriptors.DB_NAME, Descriptors.SAMPLE_DB_DESCRIPTOR, true).get();
            admin.createTable(
                            Descriptors.PRIMARYKEY_TABLE_BASIC_PATH,
                            Descriptors.PRIMARYKEY_TABLE_BASIC_DESCRIPTOR,
                            true)
                    .get();
            admin.close();

            // obtain Table instance from the Connection.
            Table myFirstTable = connection.getTable(Descriptors.PRIMARYKEY_TABLE_BASIC_PATH);
            logger.info("Table created successfully {}", myFirstTable.getTableInfo());

            // Insert the row into the table
            UpsertWriter apdWriter = myFirstTable.newUpsert().createWriter();

            // Create a row to insert
            GenericRow row = GenericRow.of(1L, BinaryString.fromString("Hello Fluss 1!"));
            GenericRow row1 = GenericRow.of(2L, BinaryString.fromString("Hello Fluss 2!"));
            GenericRow row2 = GenericRow.of(2L, BinaryString.fromString("Hello Fluss 3!"));

            apdWriter.upsert(row);
            apdWriter.upsert(row1);
            apdWriter.upsert(row2);

            apdWriter.flush();

            logger.info("Data written to Table successfully.");

            Table myReadingTable = connection.getTable(Descriptors.PRIMARYKEY_TABLE_BASIC_PATH);
            logger.info("Reading from Table {}", myReadingTable.getTableInfo());

            // Read the data back from the table from the bucket that we created
            GenericRow rowKey = GenericRow.of(2L);
            LookupResult lookup = myReadingTable.newLookup().createLookuper().lookup(rowKey).get();
            for (InternalRow record : lookup.getRowList()) {
                logger.info(
                        "Received rows for key {} id: {} name: {}",
                        rowKey,
                        record.getLong(0),
                        record.getString(1));
            }
        }
    }
}
