package com.alibaba.fluss.server.utils;

import com.alibaba.fluss.exception.InvalidTableException;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TableDescriptorValidation} . */
public class TableDescriptorValidationTest {

    @Test
    void testSystemColumns() {
        Schema schema1 =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .column("f3", DataTypes.STRING())
                        .build();
        Schema schema2 =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .column("f3", DataTypes.STRING())
                        .column("__offset", DataTypes.STRING())
                        .column("__timestamp", DataTypes.STRING())
                        .column("__bucket", DataTypes.STRING())
                        .build();
        assertThatNoException()
                .isThrownBy(
                        () -> {
                            TableDescriptorValidation.checkSystemColumns(schema1.getRowType());
                        });
        assertThatThrownBy(() -> TableDescriptorValidation.checkSystemColumns(schema2.getRowType()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessage(
                        "Unsupported columns: __offset, __timestamp, __bucket. __offset, __timestamp, __bucket are system columns, please don't use them.");
    }
}
