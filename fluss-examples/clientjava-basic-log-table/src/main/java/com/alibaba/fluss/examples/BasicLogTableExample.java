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
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.examples.commons.Descriptors;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * This example show how to use a Log table. Log Table just support append as write operation, it's
 * kind of analog to a kafka topic, here we are doing a scan analog to a kafka consumer.
 */
public class BasicLogTableExample {
    private static final Logger logger = LoggerFactory.getLogger(BasicLogTableExample.class);

    public static void main(String[] args) throws Exception {

        logger.info("Starting {}", BasicLogTableExample.class.getSimpleName());

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
                            Descriptors.LOG_TABLE_BASIC_PATH,
                            Descriptors.LOG_TABLE_BASIC_DESCRIPTOR,
                            true)
                    .get();
            admin.close();

            // obtain Table instance from the Connection.
            Table myFirstTable = connection.getTable(Descriptors.LOG_TABLE_BASIC_PATH);
            logger.info("Table created successfully {}", myFirstTable.getTableInfo());

            // Insert the row into the table
            AppendWriter apdWriter = myFirstTable.newAppend().createWriter();

            // Create a row to insert
            GenericRow row = GenericRow.of(1L, BinaryString.fromString("Hello Fluss 1!"));
            GenericRow row1 = GenericRow.of(1L, BinaryString.fromString("Hello Fluss 2!"));
            GenericRow row2 = GenericRow.of(1L, BinaryString.fromString("Hello Fluss 3!"));

            apdWriter.append(row);
            apdWriter.append(row1);
            apdWriter.append(row2);

            apdWriter.flush();

            logger.info("Data written to Table successfully.");

            Table myReadingTable = connection.getTable(Descriptors.LOG_TABLE_BASIC_PATH);
            logger.info("Reading from Table {}", myReadingTable.getTableInfo());

            // Read the data back from the table from the bucket that we created
            try (LogScanner logScanner = myReadingTable.newScan().createLogScanner()) {
                logScanner.subscribeFromBeginning(0);

                logger.info("Polling for records...");
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (TableBucket bucket : scanRecords.buckets()) {
                    for (ScanRecord record : scanRecords.records(bucket)) {
                        logger.info("Reading data from table: {}", record.getRow());
                    }
                }
            }
        }
    }
}
