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

package org.apache.fluss.lake.lance.utils;

import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LanceArrowUtils}. */
class LanceArrowUtilsTest {

    @Test
    void testToArrowSchemaWithNestedRowType() {
        // Create a RowType with a nested Row field
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "address",
                                DataTypes.ROW(
                                        DataTypes.FIELD("city", DataTypes.STRING()),
                                        DataTypes.FIELD("zip", DataTypes.INT()))));

        Schema arrowSchema = LanceArrowUtils.toArrowSchema(rowType);

        // Verify the schema has 3 fields
        assertThat(arrowSchema.getFields()).hasSize(3);

        // Verify the first two fields are simple types
        Field idField = arrowSchema.getFields().get(0);
        assertThat(idField.getName()).isEqualTo("id");
        assertThat(idField.getType()).isInstanceOf(ArrowType.Int.class);

        Field nameField = arrowSchema.getFields().get(1);
        assertThat(nameField.getName()).isEqualTo("name");
        assertThat(nameField.getType()).isEqualTo(ArrowType.Utf8.INSTANCE);

        // Verify the nested row field
        Field addressField = arrowSchema.getFields().get(2);
        assertThat(addressField.getName()).isEqualTo("address");
        assertThat(addressField.getType()).isEqualTo(ArrowType.Struct.INSTANCE);

        // Verify the nested fields
        List<Field> addressChildren = addressField.getChildren();
        assertThat(addressChildren).hasSize(2);

        Field cityField = addressChildren.get(0);
        assertThat(cityField.getName()).isEqualTo("city");
        assertThat(cityField.getType()).isEqualTo(ArrowType.Utf8.INSTANCE);

        Field zipField = addressChildren.get(1);
        assertThat(zipField.getName()).isEqualTo("zip");
        assertThat(zipField.getType()).isInstanceOf(ArrowType.Int.class);
    }

    @Test
    void testToArrowSchemaWithDeeplyNestedRowType() {
        // Create a RowType with deeply nested Row fields
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD(
                                "level1",
                                DataTypes.ROW(
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "level2",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "value", DataTypes.DOUBLE()))))));

        Schema arrowSchema = LanceArrowUtils.toArrowSchema(rowType);

        // Verify the schema structure
        assertThat(arrowSchema.getFields()).hasSize(2);

        // Verify level1 struct
        Field level1Field = arrowSchema.getFields().get(1);
        assertThat(level1Field.getName()).isEqualTo("level1");
        assertThat(level1Field.getType()).isEqualTo(ArrowType.Struct.INSTANCE);
        assertThat(level1Field.getChildren()).hasSize(2);

        // Verify level2 struct
        Field level2Field = level1Field.getChildren().get(1);
        assertThat(level2Field.getName()).isEqualTo("level2");
        assertThat(level2Field.getType()).isEqualTo(ArrowType.Struct.INSTANCE);
        assertThat(level2Field.getChildren()).hasSize(1);

        // Verify value field in level2
        Field valueField = level2Field.getChildren().get(0);
        assertThat(valueField.getName()).isEqualTo("value");
        assertThat(valueField.getType()).isInstanceOf(ArrowType.FloatingPoint.class);
    }

    @Test
    void testToArrowSchemaWithArrayOfRowType() {
        // Create a RowType with an array of rows
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD(
                                "items",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("price", DataTypes.DOUBLE())))));

        Schema arrowSchema = LanceArrowUtils.toArrowSchema(rowType);

        // Verify the schema structure
        assertThat(arrowSchema.getFields()).hasSize(2);

        // Verify items array field
        Field itemsField = arrowSchema.getFields().get(1);
        assertThat(itemsField.getName()).isEqualTo("items");
        assertThat(itemsField.getType()).isEqualTo(ArrowType.List.INSTANCE);

        // Verify the element type is a struct
        List<Field> itemsChildren = itemsField.getChildren();
        assertThat(itemsChildren).hasSize(1);

        Field elementField = itemsChildren.get(0);
        assertThat(elementField.getName()).isEqualTo("element");
        assertThat(elementField.getType()).isEqualTo(ArrowType.Struct.INSTANCE);

        // Verify the struct fields
        List<Field> structChildren = elementField.getChildren();
        assertThat(structChildren).hasSize(2);

        Field nameField = structChildren.get(0);
        assertThat(nameField.getName()).isEqualTo("name");
        assertThat(nameField.getType()).isEqualTo(ArrowType.Utf8.INSTANCE);

        Field priceField = structChildren.get(1);
        assertThat(priceField.getName()).isEqualTo("price");
        assertThat(priceField.getType()).isInstanceOf(ArrowType.FloatingPoint.class);
    }

    @Test
    void testToArrowSchemaWithRowContainingArray() {
        // Create a RowType with a nested row that contains an array
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD(
                                "data",
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "tags", DataTypes.ARRAY(DataTypes.STRING())),
                                        DataTypes.FIELD("count", DataTypes.INT()))));

        Schema arrowSchema = LanceArrowUtils.toArrowSchema(rowType);

        // Verify the schema structure
        assertThat(arrowSchema.getFields()).hasSize(2);

        // Verify data struct field
        Field dataField = arrowSchema.getFields().get(1);
        assertThat(dataField.getName()).isEqualTo("data");
        assertThat(dataField.getType()).isEqualTo(ArrowType.Struct.INSTANCE);

        // Verify tags array field within the struct
        List<Field> dataChildren = dataField.getChildren();
        assertThat(dataChildren).hasSize(2);

        Field tagsField = dataChildren.get(0);
        assertThat(tagsField.getName()).isEqualTo("tags");
        assertThat(tagsField.getType()).isEqualTo(ArrowType.List.INSTANCE);

        // Verify the array element type
        List<Field> tagsChildren = tagsField.getChildren();
        assertThat(tagsChildren).hasSize(1);
        assertThat(tagsChildren.get(0).getName()).isEqualTo("element");
        assertThat(tagsChildren.get(0).getType()).isEqualTo(ArrowType.Utf8.INSTANCE);
    }
}
