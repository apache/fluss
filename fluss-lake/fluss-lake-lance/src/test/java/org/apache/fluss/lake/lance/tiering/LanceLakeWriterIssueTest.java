package org.apache.fluss.lake.lance.tiering;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lance.LanceConfig;
import org.apache.fluss.lake.lance.utils.LanceArrowUtils;
import org.apache.fluss.lake.lance.utils.LanceDatasetAdapter;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

import com.lancedb.lance.WriteParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for LanceLakeWriter issue #2. */
public class LanceLakeWriterIssueTest {

    private @TempDir File tempWarehouseDir;
    private LanceLakeTieringFactory lanceLakeTieringFactory;
    private Configuration configuration;

    @BeforeEach
    void beforeEach() {
        configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toString());
        lanceLakeTieringFactory = new LanceLakeTieringFactory(configuration);
    }

    @Test
    void testWriterCompleteAfterCloseReturnsEmptyFragments() throws Exception {
        TablePath tablePath = TablePath.of("lance", "logTableCloseTest");
        Map<String, String> customProperties = new HashMap<>();
        LanceConfig config =
                LanceConfig.from(
                        configuration.toMap(),
                        customProperties,
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());

        // Create schema
        List<Schema.Column> columns = new ArrayList<>();
        columns.add(new Schema.Column("c1", DataTypes.INT()));
        columns.add(new Schema.Column("c2", DataTypes.STRING()));
        Schema.Builder schemaBuilder = Schema.newBuilder().fromColumns(columns);
        Schema schema = schemaBuilder.build();
        WriteParams params = LanceConfig.genWriteParamsFromConfig(config);
        LanceDatasetAdapter.createDataset(
                config.getDatasetUri(),
                LanceArrowUtils.toArrowSchema(schema.getRowType(), customProperties),
                params);

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(customProperties)
                        .build();
        TableInfo tableInfo =
                TableInfo.of(tablePath, 0, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);

        LakeWriter<LanceWriteResult> lakeWriter =
                lanceLakeTieringFactory.createLakeWriter(
                        new WriterInitContext() {
                            @Override
                            public TablePath tablePath() {
                                return tablePath;
                            }

                            @Override
                            public TableBucket tableBucket() {
                                return new TableBucket(0, 0L, 0);
                            }

                            @Override
                            public String partition() {
                                return null;
                            }

                            @Override
                            public TableInfo tableInfo() {
                                return tableInfo;
                            }
                        });

        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(0, 1);
        genericRow.setField(1, BinaryString.fromString("v1"));
        LogRecord logRecord =
                new GenericRecord(
                        0, System.currentTimeMillis(), ChangeType.APPEND_ONLY, genericRow);

        lakeWriter.write(logRecord);

        // Complete should return 1 fragment
        LanceWriteResult result1 = lakeWriter.complete();
        assertThat(result1.commitMessage()).hasSize(1);

        // Close the writer
        lakeWriter.close();

        // Complete again
        LanceWriteResult result2 = lakeWriter.complete();

        // Assert the bug is fixed: result2 should be empty
        assertThat(result2.commitMessage()).isEmpty();
    }
}
