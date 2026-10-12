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

package org.apache.fluss.kafka.matrix;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.AppInfoParser;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Standalone client process: deliberately depends only on the JDK and the selected Kafka client.
 */
public final class KafkaClientProbe {
    private KafkaClientProbe() {}

    /** Writes through a real endpoint; a failed check terminates the isolated process. */
    public static void main(String[] args) throws Exception {
        String expectedVersion = args[0];
        String actualVersion = AppInfoParser.getVersion();
        if (!expectedVersion.equals(actualVersion)) {
            throw new IllegalStateException(
                    "Expected " + expectedVersion + " but loaded " + actualVersion);
        }
        System.out.println("VERSION=" + actualVersion);
        System.out.println(
                "JAR=" + KafkaProducer.class.getProtectionDomain().getCodeSource().getLocation());
        System.out.println("JAVA=" + System.getProperty("java.version"));
        System.out.println("CLASSPATH=" + System.getProperty("java.class.path"));
        List<KafkaProducer<byte[], byte[]>> producers = new ArrayList<>();
        try {
            for (String acks : Arrays.asList("0", "1", "all")) {
                Map<String, Object> config = new HashMap<>();
                config.put("bootstrap.servers", args[1]);
                config.put("key.serializer", ByteArraySerializer.class);
                config.put("value.serializer", ByteArraySerializer.class);
                config.put("enable.idempotence", false);
                config.put("security.protocol", "PLAINTEXT");
                config.put("acks", acks);
                config.put("max.in.flight.requests.per.connection", 1);
                config.put("request.timeout.ms", 10000);
                config.put("max.block.ms", 30000);
                config.put("retries", 3);
                config.put("compression.type", "none");
                config.put("linger.ms", 0);
                String alias = args[3] + "_" + acks;
                KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config);
                producers.add(producer);
                for (int i = 0; i < 4; i++) {
                    String topic = i % 2 == 0 ? args[2] + "." + alias : alias;
                    RecordMetadata result =
                            producer.send(
                                            new ProducerRecord<>(
                                                    topic,
                                                    0,
                                                    123L,
                                                    "key".getBytes(StandardCharsets.UTF_8),
                                                    (actualVersion + ":" + acks + ":" + i)
                                                            .getBytes(StandardCharsets.UTF_8),
                                                    Arrays.asList(
                                                            new RecordHeader(
                                                                    "source", new byte[] {1}),
                                                            new RecordHeader(
                                                                    "source", new byte[] {2}))))
                                    .get(30, TimeUnit.SECONDS);
                    if (!result.topic().equals(topic)
                            || (!acks.equals("0") && result.offset() != i)) {
                        throw new IllegalStateException("Unexpected acknowledgement " + result);
                    }
                }
                producer.flush();
                System.out.println("ACKS=" + acks + ":SENT");
            }
            // acks=0 only confirms a socket write. Keep connections open until the
            // parent verifies native storage, rather than racing cancellation on close.
            System.out.println("WRITES_COMPLETE");
            if (System.in.read() != '\n') {
                throw new IllegalStateException("Native readback was not confirmed by the parent");
            }
            System.out.println("NATIVE_READBACK_CONFIRMED");
        } finally {
            for (KafkaProducer<byte[], byte[]> producer : producers) {
                producer.close();
            }
        }
    }
}
