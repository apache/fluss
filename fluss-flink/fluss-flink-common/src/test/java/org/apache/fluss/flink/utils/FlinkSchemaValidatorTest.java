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

package org.apache.fluss.flink.utils;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkSchemaValidator}. */
public class FlinkSchemaValidatorTest {

    @Test
    void testValidatePrimaryKeyColumnsWithValidTypes() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", INT().notNull()),
                                Column.physical("name", STRING())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("PK_id", Collections.singletonList("id")));

        List<String> primaryKeyColumns = Collections.singletonList("id");
        FlinkSchemaValidator.validatePrimaryKeyColumns(schema, primaryKeyColumns);
    }

    @Test
    void testValidatePrimaryKeyColumnsWithArrayType() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", ARRAY(INT()).notNull()),
                                Column.physical("name", STRING())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("PK_id", Collections.singletonList("id")));

        List<String> primaryKeyColumns = Collections.singletonList("id");
        assertThatThrownBy(
                        () ->
                                FlinkSchemaValidator.validatePrimaryKeyColumns(
                                        schema, primaryKeyColumns))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Column 'id' of ARRAY type is not supported as primary key.");
    }

    @Test
    void testValidatePrimaryKeyColumnsWithMissingColumn() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Collections.singletonList(Column.physical("name", STRING())),
                        Collections.emptyList(),
                        null);

        List<String> primaryKeyColumns = Collections.singletonList("id");
        assertThatThrownBy(
                        () ->
                                FlinkSchemaValidator.validatePrimaryKeyColumns(
                                        schema, primaryKeyColumns))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Primary key column id not found in schema.");
    }

    @Test
    void testValidatePartitionKeyColumnsWithValidTypes() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", INT().notNull()),
                                Column.physical("name", STRING()),
                                Column.physical("date", STRING())),
                        Collections.emptyList(),
                        null);

        List<String> partitionKeyColumns = Arrays.asList("name", "date");
        FlinkSchemaValidator.validatePartitionKeyColumns(schema, partitionKeyColumns);
    }

    @Test
    void testValidatePartitionKeyColumnsWithArrayType() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", INT().notNull()),
                                Column.physical("tags", ARRAY(STRING())),
                                Column.physical("name", STRING())),
                        Collections.emptyList(),
                        null);

        List<String> partitionKeyColumns = Collections.singletonList("tags");
        assertThatThrownBy(
                        () ->
                                FlinkSchemaValidator.validatePartitionKeyColumns(
                                        schema, partitionKeyColumns))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Column 'tags' of ARRAY type is not supported as partition key.");
    }

    @Test
    void testValidatePartitionKeyColumnsWithMissingColumn() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Collections.singletonList(Column.physical("name", STRING())),
                        Collections.emptyList(),
                        null);

        List<String> partitionKeyColumns = Collections.singletonList("date");
        assertThatThrownBy(
                        () ->
                                FlinkSchemaValidator.validatePartitionKeyColumns(
                                        schema, partitionKeyColumns))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Partition key column date not found in schema.");
    }

    @Test
    void testValidateBucketKeyColumnsWithValidTypes() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", INT().notNull()),
                                Column.physical("name", STRING()),
                                Column.physical("category", STRING())),
                        Collections.emptyList(),
                        null);

        List<String> bucketKeyColumns = Arrays.asList("id", "name");
        FlinkSchemaValidator.validateBucketKeyColumns(schema, bucketKeyColumns);
    }

    @Test
    void testValidateBucketKeyColumnsWithArrayType() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", INT().notNull()),
                                Column.physical("tags", ARRAY(STRING())),
                                Column.physical("name", STRING())),
                        Collections.emptyList(),
                        null);

        List<String> bucketKeyColumns = Collections.singletonList("tags");
        assertThatThrownBy(
                        () ->
                                FlinkSchemaValidator.validateBucketKeyColumns(
                                        schema, bucketKeyColumns))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Column 'tags' of ARRAY type is not supported as bucket key.");
    }

    @Test
    void testValidateBucketKeyColumnsWithMissingColumn() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Collections.singletonList(Column.physical("name", STRING())),
                        Collections.emptyList(),
                        null);

        List<String> bucketKeyColumns = Collections.singletonList("bucket_col");
        assertThatThrownBy(
                        () ->
                                FlinkSchemaValidator.validateBucketKeyColumns(
                                        schema, bucketKeyColumns))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Bucket key column bucket_col not found in schema.");
    }

    @Test
    void testValidatePrimaryKeyColumnsWithMultipleColumns() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", INT().notNull()),
                                Column.physical("name", STRING().notNull()),
                                Column.physical("tags", ARRAY(STRING()))),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("PK_id_name", Arrays.asList("id", "name")));

        List<String> primaryKeyColumns = Arrays.asList("id", "name");
        FlinkSchemaValidator.validatePrimaryKeyColumns(schema, primaryKeyColumns);
    }

    @Test
    void testValidatePrimaryKeyColumnsWithMultipleColumnsOneArray() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", INT().notNull()),
                                Column.physical("name", STRING().notNull()),
                                Column.physical("tags", ARRAY(STRING()).notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("PK_id_tags", Arrays.asList("id", "tags")));

        List<String> primaryKeyColumns = Arrays.asList("id", "tags");
        assertThatThrownBy(
                        () ->
                                FlinkSchemaValidator.validatePrimaryKeyColumns(
                                        schema, primaryKeyColumns))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Column 'tags' of ARRAY type is not supported as primary key.");
    }
}
