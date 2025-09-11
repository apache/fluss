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

package org.apache.fluss.protogen.tests;

import org.apache.fluss.protogen.generator.generator.RecordsFieldFinder;

import io.protostuff.parser.Message;
import io.protostuff.parser.Proto;
import io.protostuff.parser.ProtoUtil;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RecordsFieldFinder}. */
public class RecordsFieldFinderTest {

    @Test
    public void testHasRecordsFieldForSimpleMessage() throws Exception {
        Proto proto = parseProtoFile("bytes.proto");
        Message rdMessage = findMessage(proto, "RD");

        RecordsFieldFinder finder = new RecordsFieldFinder();
        assertThat(finder.hasRecordsField(rdMessage))
                .as("RD message should have records field")
                .isTrue();
    }

    @Test
    public void testHasRecordsFieldForNestedMessage() throws Exception {
        Proto proto = parseProtoFile("bytes.proto");
        Message ordMessage = findMessage(proto, "ORD");

        RecordsFieldFinder finder = new RecordsFieldFinder();
        assertThat(finder.hasRecordsField(ordMessage))
                .as("ORD message should have records field through nested RD")
                .isTrue();
    }

    @Test
    public void testHasRecordsFieldForMessageWithoutRecords() throws Exception {
        Proto proto = parseProtoFile("messages.proto");
        Message xMessage = findMessage(proto, "X");

        RecordsFieldFinder finder = new RecordsFieldFinder();
        assertThat(finder.hasRecordsField(xMessage))
                .as("X message should not have records field")
                .isFalse();
    }

    @Test
    public void testCircularReferenceWithoutRecords() throws Exception {
        Proto proto = parseProtoFile("circular_reference.proto");
        Message circularAMessage = findMessage(proto, "CircularA");

        RecordsFieldFinder finder = new RecordsFieldFinder();
        assertThat(finder.hasRecordsField(circularAMessage))
                .as("CircularA message should not have records field")
                .isFalse();
    }

    @Test
    public void testCircularReferenceWithRecords() throws Exception {
        Proto proto = parseProtoFile("circular_reference.proto");
        Message circularRecordsAMessage = findMessage(proto, "CircularRecordsA");

        RecordsFieldFinder finder = new RecordsFieldFinder();
        assertThat(finder.hasRecordsField(circularRecordsAMessage))
                .as("CircularRecordsA message should have records field")
                .isTrue();
    }

    @Test
    public void testSimpleRecordsMessage() throws Exception {
        Proto proto = parseProtoFile("circular_reference.proto");
        Message simpleRecordsMessage = findMessage(proto, "SimpleRecordsMessage");

        RecordsFieldFinder finder = new RecordsFieldFinder();
        assertThat(finder.hasRecordsField(simpleRecordsMessage))
                .as("SimpleRecordsMessage should have records field")
                .isTrue();
    }

    @Test
    public void testNewFinderInstanceForEachCall() throws Exception {
        Proto proto = parseProtoFile("circular_reference.proto");
        Message circularRecordsAMessage = findMessage(proto, "CircularRecordsA");

        // Test with first finder instance
        RecordsFieldFinder finder1 = new RecordsFieldFinder();
        assertThat(finder1.hasRecordsField(circularRecordsAMessage))
                .as("CircularRecordsA should have records field (finder1)")
                .isTrue();

        // Test with second finder instance (should work independently)
        RecordsFieldFinder finder2 = new RecordsFieldFinder();
        assertThat(finder2.hasRecordsField(circularRecordsAMessage))
                .as("CircularRecordsA should have records field (finder2)")
                .isTrue();
    }

    private Proto parseProtoFile(String filename) throws Exception {
        File protoFile = new File("src/main/proto/" + filename);
        if (!protoFile.exists()) {
            throw new IOException("Cannot find proto file: " + filename);
        }

        Proto proto = new Proto();
        ProtoUtil.loadFrom(protoFile, proto);
        return proto;
    }

    private Message findMessage(Proto proto, String messageName) {
        return proto.getMessages().stream()
                .filter(message -> message.getName().equals(messageName))
                .findFirst()
                .orElseThrow(
                        () -> new IllegalArgumentException("Message not found: " + messageName));
    }
}
