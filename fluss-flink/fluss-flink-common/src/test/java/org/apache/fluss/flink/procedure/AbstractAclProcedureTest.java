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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.exception.InvalidConsumerGroupIdException;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for SQL ACL resource parsing. */
class AbstractAclProcedureTest {

    @ParameterizedTest
    @CsvSource({
        "cluster, CLUSTER, fluss-cluster",
        "CLUSTER.defaultdb, DATABASE, defaultdb",
        "cluster.defaultdb.table1, TABLE, defaultdb.table1",
        "cluster.group.default, TABLE, group.default",
        "database:defaultdb, DATABASE, defaultdb",
        "DATABASE:DefaultDB, DATABASE, DefaultDB",
        "database:*, DATABASE, *",
        "table:defaultdb.table1, TABLE, defaultdb.table1",
        "TABLE:DefaultDB.DefaultTable, TABLE, DefaultDB.DefaultTable",
        "table:*, TABLE, *",
        "group:default-consumer, GROUP, default-consumer",
        "GROUP:Default_consumer-1, GROUP, Default_consumer-1",
        "group:group1, GROUP, group1",
        "group:*, GROUP, *",
        "group:fluss-cluster, GROUP, fluss-cluster",
    })
    void testResourceNames(String input, ResourceType type, String name) throws Exception {
        TestingAclProcedure procedure = new TestingAclProcedure();
        procedure.internalCall(input, "ALLOW", "User:consumer", "DESCRIBE", "*");
        assertThat(procedure.resource).isEqualTo(new Resource(type, name));
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"ANY", "any"})
    void testAnyResource(String input) throws Exception {
        TestingAclProcedure procedure = new TestingAclProcedure();
        procedure.internalCall(input, null, null, null, null);
        assertThat(procedure.resource).isEqualTo(Resource.any());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "group:",
                "GROUP:",
                "group:default-*",
                "group:a*b",
                "group:default.consumer",
                "group:default/consumer",
                "group:default:consumer",
                "group:消费组",
                "group:__internal"
            })
    void testInvalidGroupId(String input) {
        TestingAclProcedure procedure = new TestingAclProcedure();
        assertThatThrownBy(
                        () ->
                                procedure.internalCall(
                                        input, "ALLOW", "User:consumer", "DESCRIBE", "*"))
                .isInstanceOf(InvalidConsumerGroupIdException.class)
                .hasMessageContaining("Consumer group id");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "database:",
                "DATABASE:",
                "table:",
                "TABLE:",
                "table:foo",
                "table:foo.bar.baz",
                "group.default-consumer",
                "group.*"
            })
    void testInvalidDatabaseOrTableResource(String input) {
        TestingAclProcedure procedure = new TestingAclProcedure();
        assertThatThrownBy(
                        () ->
                                procedure.internalCall(
                                        input, "ALLOW", "User:consumer", "DESCRIBE", "*"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static class TestingAclProcedure extends AbstractAclProcedure {
        private Resource resource;

        @Override
        protected String[] aclOperation(
                Resource resource,
                PermissionType permission,
                FlussPrincipal flussPrincipal,
                OperationType operationType,
                String host) {
            this.resource = resource;
            return new String[0];
        }
    }
}
