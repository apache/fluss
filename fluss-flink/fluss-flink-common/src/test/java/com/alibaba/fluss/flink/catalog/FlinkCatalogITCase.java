/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.catalog;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.InvalidTableException;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.config.ConfigOptions.DEFAULT_LISTENER_NAME;
import static com.alibaba.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static com.alibaba.fluss.flink.FlinkConnectorOptions.BUCKET_KEY;
import static com.alibaba.fluss.flink.FlinkConnectorOptions.BUCKET_NUMBER;
import static com.alibaba.fluss.flink.source.testutils.FlinkTestBase.assertResultsIgnoreOrder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for {@link com.alibaba.fluss.flink.catalog.FlinkCatalog}. */
abstract class FlinkCatalogITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(initClusterConf())
                    .build();

    static Configuration initClusterConf() {
        Configuration clusterConf = new Configuration();
        // use a small check interval to cleanup partitions quickly
        clusterConf.set(ConfigOptions.AUTO_PARTITION_CHECK_INTERVAL, Duration.ofSeconds(3));
        return clusterConf;
    }

    static final String CATALOG_NAME = "testcatalog";
    static final String DEFAULT_DB = FlinkCatalogOptions.DEFAULT_DATABASE.defaultValue();
    static Catalog catalog;
    static TableEnvironment tEnv;

    @BeforeAll
    static void beforeAll() {
        // open a catalog so that we can get table from the catalog
        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        String bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        catalog =
                new FlinkCatalog(
                        CATALOG_NAME,
                        DEFAULT_DB,
                        bootstrapServers,
                        Thread.currentThread().getContextClassLoader(),
                        Collections.emptyMap());
        catalog.open();
        // create table environment
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        // crate catalog using sql
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
    }

    @AfterAll
    static void afterAll() {
        tEnv.executeSql("use catalog " + TableConfigOptions.TABLE_CATALOG_NAME.defaultValue());
        tEnv.executeSql("DROP CATALOG IF EXISTS " + CATALOG_NAME);
        if (catalog != null) {
            catalog.close();
        }
    }

    @BeforeEach
    void before() {
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        // we don't need to "USE fluss" explicitly as it is the default database
    }

    @Test
    void testCreateTable() throws Exception {
        // create a table will all supported data types
        tEnv.executeSql(
                "create table test_table "
                        + "(a int not null primary key not enforced,"
                        + " b CHAR(3),"
                        + " c STRING not null COMMENT 'STRING COMMENT',"
                        + " d STRING,"
                        + " e BOOLEAN,"
                        + " f BINARY(2),"
                        + " g BYTES COMMENT 'BYTES',"
                        + " h BYTES,"
                        + " i DECIMAL(12, 2),"
                        + " j TINYINT,"
                        + " k SMALLINT,"
                        + " l BIGINT,"
                        + " m FLOAT,"
                        + " n DOUBLE,"
                        + " o DATE,"
                        + " p TIME,"
                        + " q TIMESTAMP,"
                        + " r TIMESTAMP_LTZ,"
                        + " s ROW<a INT>) COMMENT 'a test table'");
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder
                .column("a", DataTypes.INT().notNull())
                .column("b", DataTypes.CHAR(3))
                .column("c", DataTypes.STRING().notNull())
                .withComment("STRING COMMENT")
                .column("d", DataTypes.STRING())
                .column("e", DataTypes.BOOLEAN())
                .column("f", DataTypes.BINARY(2))
                .column("g", DataTypes.BYTES())
                .withComment("BYTES")
                .column("h", DataTypes.BYTES())
                .column("i", DataTypes.DECIMAL(12, 2))
                .column("j", DataTypes.TINYINT())
                .column("k", DataTypes.SMALLINT())
                .column("l", DataTypes.BIGINT())
                .column("m", DataTypes.FLOAT())
                .column("n", DataTypes.DOUBLE())
                .column("o", DataTypes.DATE())
                .column("p", DataTypes.TIME())
                .column("q", DataTypes.TIMESTAMP())
                .column("r", DataTypes.TIMESTAMP_LTZ())
                .column("s", DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT())))
                .primaryKey("a");
        Schema expectedSchema = schemaBuilder.build();
        CatalogTable table =
                (CatalogTable) catalog.getTable(new ObjectPath(DEFAULT_DB, "test_table"));
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);
    }

    @Test
    void testCreateUnSupportedTable() {
        // test invalid property
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table test_arrow_compression"
                                                + " (a int, b int) with ("
                                                + " 'table.log.format' = 'arrow',"
                                                + " 'table.log.arrow.compression.type' = 'zstd',"
                                                + " 'table.log.arrow.compression.zstd.level' = '0')"))
                .cause()
                .hasRootCauseMessage(
                        "Invalid ZSTD compression level: 0. Expected a value between 1 and 22.");
    }

    @Test
    void testCreateNoPkTable() throws Exception {
        tEnv.executeSql("create table append_only_table(a int, b int) with ('bucket.num' = '10')");
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("a", DataTypes.INT()).column("b", DataTypes.INT());
        Schema expectedSchema = schemaBuilder.build();
        CatalogTable table =
                (CatalogTable) catalog.getTable(new ObjectPath(DEFAULT_DB, "append_only_table"));
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("bucket.num", "10");
        assertOptionsEqual(table.getOptions(), expectedOptions);
    }

    @Test
    void testPartitionedTable() throws Exception {
        ObjectPath objectPath = new ObjectPath(DEFAULT_DB, "test_partitioned_table");

        // 1. first create.
        tEnv.executeSql(
                "create table test_partitioned_table (a int, b string, dt string) partitioned by (b,dt)");
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .column("dt", DataTypes.STRING());
        Schema expectedSchema = schemaBuilder.build();
        CatalogTable table = (CatalogTable) catalog.getTable(objectPath);
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);
        List<String> partitionKeys = table.getPartitionKeys();
        assertThat(partitionKeys).isEqualTo(Arrays.asList("b", "dt"));

        // 2. add partitions.
        tEnv.executeSql("alter table test_partitioned_table add partition (b = 1,dt = 1)");
        tEnv.executeSql("alter table test_partitioned_table add partition (b = 2,dt = 1)");
        tEnv.executeSql("alter table test_partitioned_table add partition (b = 3,dt = 1)");
        List<String> expectedShowPartitionsResult =
                Arrays.asList("+I[b=1/dt=1]", "+I[b=2/dt=1]", "+I[b=3/dt=1]");
        CloseableIterator<Row> showPartitionIterator =
                tEnv.executeSql("show partitions test_partitioned_table").collect();
        assertResultsIgnoreOrder(showPartitionIterator, expectedShowPartitionsResult, true);

        // 3. drop partitions.
        tEnv.executeSql("alter table test_partitioned_table drop partition (b = 1,dt = 1)");
        expectedShowPartitionsResult = Arrays.asList("+I[b=2/dt=1]", "+I[b=3/dt=1]");
        showPartitionIterator = tEnv.executeSql("show partitions test_partitioned_table").collect();
        assertResultsIgnoreOrder(showPartitionIterator, expectedShowPartitionsResult, true);

        // 4. show partitions with spec.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "show partitions test_partitioned_table partition (b=2,dt=1)")
                                        .collect())
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testAutoPartitionedTable() throws Exception {
        ObjectPath objectPath = new ObjectPath(DEFAULT_DB, "test_auto_partitioned_table");

        // 1. test add table.
        tEnv.executeSql(
                "create table test_auto_partitioned_table (a int, b string) partitioned by (b) "
                        + "with ('table.auto-partition.enabled' = 'true',"
                        + " 'table.auto-partition.time-unit' = 'year')");
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("a", DataTypes.INT()).column("b", DataTypes.STRING());
        Schema expectedSchema = schemaBuilder.build();
        CatalogTable table = (CatalogTable) catalog.getTable(objectPath);
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);
        List<String> partitionKeys = table.getPartitionKeys();
        assertThat(partitionKeys).isEqualTo(Collections.singletonList("b"));

        TablePath tablePath = new TablePath(DEFAULT_DB, "test_auto_partitioned_table");
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);
        int currentYear = LocalDate.now().getYear();
        List<String> expectedShowPartitionsResult =
                Arrays.asList("+I[b=" + currentYear + "]", "+I[b=" + (currentYear + 1) + "]");
        CloseableIterator<Row> showPartitionIterator =
                tEnv.executeSql("show partitions test_auto_partitioned_table").collect();
        assertResultsIgnoreOrder(showPartitionIterator, expectedShowPartitionsResult, true);

        // 2. test add partitions.
        tEnv.executeSql(
                String.format(
                        "alter table test_auto_partitioned_table add partition (b = '%s')",
                        currentYear + 10));
        expectedShowPartitionsResult =
                Arrays.asList(
                        "+I[b=" + currentYear + "]",
                        "+I[b=" + (currentYear + 1) + "]",
                        "+I[b=" + (currentYear + 10) + "]");
        showPartitionIterator =
                tEnv.executeSql("show partitions test_auto_partitioned_table").collect();
        assertResultsIgnoreOrder(showPartitionIterator, expectedShowPartitionsResult, true);

        // 3. test drop partitions.
        tEnv.executeSql(
                String.format(
                        "alter table test_auto_partitioned_table drop partition (b = '%s')",
                        currentYear + 1));
        tEnv.executeSql(
                String.format(
                        "alter table test_auto_partitioned_table drop partition (b = '%s')",
                        currentYear + 10));
        expectedShowPartitionsResult = Collections.singletonList("+I[b=" + currentYear + "]");
        showPartitionIterator =
                tEnv.executeSql("show partitions test_auto_partitioned_table").collect();
        assertResultsIgnoreOrder(showPartitionIterator, expectedShowPartitionsResult, true);
    }

    @Test
    void testInvalidAutoPartitionedTableWithMultiPartitionKeys() {
        // 1. test invalid auto partition table.
        // not specify auto partition key
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table test_invalid_auto_partitioned_table_with_multi_partition_keys (a int, b string, c string, dt string) partitioned by (b,c,dt) "
                                                + "with ('table.auto-partition.enabled' = 'true',"
                                                + " 'table.auto-partition.time-unit' = 'day'"
                                                + ")"))
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessage(
                        "Currently, auto partitioned table must set one auto partition key when it has multiple partition keys. Please set table property 'table.auto-partition.key'.");

        // specified auto partition key not in partition keys
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table test_invalid_auto_partitioned_table_with_multi_partition_keys (a int, b string, c string, dt string) partitioned by (b,c,dt) "
                                                + "with ('table.auto-partition.enabled' = 'true',"
                                                + " 'table.auto-partition.time-unit' = 'day',"
                                                + " 'table.auto-partition.key' = 'a'"
                                                + ")"))
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessage(
                        "The specified key for auto partitioned table is not a partition key. Your key 'a' is not in key list [b, c, dt]");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table test_invalid_auto_partitioned_table_with_multi_partition_keys (a int, b string, c string, dt string) partitioned by (b,c,dt) "
                                                + "with ('table.auto-partition.enabled' = 'true',"
                                                + " 'table.auto-partition.time-unit' = 'day',"
                                                + " 'table.auto-partition.key' = 'dt',"
                                                + " 'table.auto-partition.num-precreate' = '2'"
                                                + ")"))
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessage(
                        "For a partitioned table with multiple partition keys, auto pre-create is unsupported and this value must be set to 0, but is 2");
    }

    @Test
    void testAutoPartitionedTableWithMultiPartitionKeys() throws Exception {
        String tblName = "test_auto_partitioned_table_with_multi_partition_keys";
        ObjectPath objectPath = new ObjectPath(DEFAULT_DB, tblName);

        // 1. test add table.
        tEnv.executeSql(
                "create table "
                        + tblName
                        + " (a int, b string, c string, hh string) partitioned by (b,c,hh) "
                        + "with ('table.auto-partition.enabled' = 'true',"
                        + " 'table.auto-partition.key' = 'hh',"
                        + " 'table.auto-partition.num-retention' = '2',"
                        + " 'table.auto-partition.time-unit' = 'hour')");
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .column("c", DataTypes.STRING())
                .column("hh", DataTypes.STRING());
        Schema expectedSchema = schemaBuilder.build();
        CatalogTable table = (CatalogTable) catalog.getTable(objectPath);
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);
        List<String> partitionKeys = table.getPartitionKeys();
        assertThat(partitionKeys).isEqualTo(Arrays.asList("b", "c", "hh"));
        assertThat(table.getOptions().get(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key()))
                .isEqualTo("0");
        TablePath tablePath = new TablePath(DEFAULT_DB, tblName);
        String datetimePattern = "yyyyMMddHH";
        String minus3hour =
                LocalDateTime.now()
                        .minusHours(3)
                        .format(DateTimeFormatter.ofPattern(datetimePattern));
        String minus2hour =
                LocalDateTime.now()
                        .minusHours(2)
                        .format(DateTimeFormatter.ofPattern(datetimePattern));
        String minus1hour =
                LocalDateTime.now()
                        .minusHours(1)
                        .format(DateTimeFormatter.ofPattern(datetimePattern));

        // 2. test add partitions.
        tEnv.executeSql(
                String.format(
                        "alter table %s add partition (b = 1,c = 1,hh = %s)", tblName, minus3hour));
        tEnv.executeSql(
                String.format(
                        "alter table %s add partition (b = 1,c = 2,hh = %s)", tblName, minus3hour));
        tEnv.executeSql(
                String.format(
                        "alter table %s add partition (b = 1,c = 1,hh = %s)", tblName, minus2hour));
        tEnv.executeSql(
                String.format(
                        "alter table %s add partition (b = 1,c = 2,hh = %s)", tblName, minus2hour));
        tEnv.executeSql(
                String.format(
                        "alter table %s add partition (b = 1,c = 1,hh = %s)", tblName, minus1hour));
        tEnv.executeSql(
                String.format(
                        "alter table %s add partition (b = 1,c = 2,hh = %s)", tblName, minus1hour));
        List<String> expectDroppedPartitions =
                Arrays.asList(
                        String.format("1$1$%s", minus3hour), String.format("1$2$%s", minus3hour));
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsDropped(tablePath, expectDroppedPartitions);

        List<String> expectedShowPartitionsResult =
                Arrays.asList(
                        "+I[b=1/c=1/hh=" + minus1hour + "]",
                        "+I[b=1/c=2/hh=" + minus1hour + "]",
                        "+I[b=1/c=1/hh=" + minus2hour + "]",
                        "+I[b=1/c=2/hh=" + minus2hour + "]");

        CloseableIterator<Row> showPartitionIterator =
                tEnv.executeSql("show partitions " + tblName).collect();
        assertResultsIgnoreOrder(showPartitionIterator, expectedShowPartitionsResult, true);
    }

    @Test
    void testTableWithExpression() throws Exception {
        // create a table with watermark and computed column
        tEnv.executeSql(
                "CREATE TABLE expression_test (\n"
                        + "    `user` BIGINT not null primary key not enforced,\n"
                        + "    product STRING COMMENT 'comment1',\n"
                        + "    price DOUBLE,\n"
                        + "    quantity DOUBLE,\n"
                        + "    cost AS price * quantity,\n"
                        + "    order_time TIMESTAMP(3),\n"
                        + "    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND\n"
                        + ") with ('k1' = 'v1')");
        CatalogTable table =
                (CatalogTable) catalog.getTable(new ObjectPath(DEFAULT_DB, "expression_test"));
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder
                .column("user", DataTypes.BIGINT().notNull())
                .column("product", DataTypes.STRING())
                .withComment("comment1")
                .column("price", DataTypes.DOUBLE())
                .column("quantity", DataTypes.DOUBLE())
                .columnByExpression("cost", "`price` * `quantity`")
                .column("order_time", DataTypes.TIMESTAMP(3))
                .watermark("order_time", "`order_time` - INTERVAL '5' SECOND")
                .primaryKey("user");
        Schema expectedSchema = schemaBuilder.build();
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("k1", "v1");
        expectedOptions.put(BUCKET_KEY.key(), "user");
        expectedOptions.put(BUCKET_NUMBER.key(), "1");
        assertOptionsEqual(table.getOptions(), expectedOptions);
    }

    @Test
    void testCreateWithUnSupportDataType() {
        // create a table with varchar datatype
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table test_table_unsupported (a varchar(10))"))
                .cause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Unsupported data type: VARCHAR(10)");

        // create a table with varbinary datatype
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table test_table_unsupported (a varbinary(10))"))
                .cause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Unsupported data type: VARBINARY(10)");

        // create a table with multiset datatype
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table test_table_unsupported (a multiset<int>)"))
                .cause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Unsupported data type: MULTISET<INT>");
    }

    @Test
    void testCreateDatabase() {
        tEnv.executeSql("create database test_db");
        List<Row> databases =
                CollectionUtil.iteratorToList(tEnv.executeSql("show databases").collect());

        assertThat(databases.stream().map(Row::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.asList(String.format("+I[%s]", DEFAULT_DB), "+I[test_db]"));
        tEnv.executeSql("drop database test_db");
        databases = CollectionUtil.iteratorToList(tEnv.executeSql("show databases").collect());
        assertThat(databases.toString()).isEqualTo(String.format("[+I[%s]]", DEFAULT_DB));
    }

    @Test
    void testFactoryCannotFindForCreateTemporaryTable() {
        // create fluss temporary table is not supported
        tEnv.executeSql(
                "create temporary table test_temp_table (a int, b int)"
                        + " with ('connector' = 'fluss', 'bootstrap.servers' = 'localhost:9092')");
        assertThatThrownBy(() -> tEnv.executeSql("insert into test_temp_table values (1, 2)"))
                .cause()
                .isInstanceOf(ValidationException.class)
                .hasMessage("Cannot discover a connector using option: 'connector'='fluss'");
    }

    @Test
    void testFactoryCannotFindForCreateCatalogTable() {
        // create fluss table under non-fluss catalog is not supported
        tEnv.executeSql("use catalog " + TableConfigOptions.TABLE_CATALOG_NAME.defaultValue());
        tEnv.executeSql(
                "create table test_catalog_table (a int, b int)"
                        + " with ('connector' = 'fluss', 'bootstrap.servers' = 'localhost:9092')");
        assertThatThrownBy(() -> tEnv.executeSql("insert into test_catalog_table values (1, 2)"))
                .cause()
                .isInstanceOf(ValidationException.class)
                .hasMessage("Cannot discover a connector using option: 'connector'='fluss'");
    }

    @Test
    void testCreateTableWithUnknownOptions() {
        // create fluss table with unknown options whose prefix is 'table.*' or 'client.*'
        tEnv.executeSql(
                "create table test_table_unknown_options (a int, b int)"
                        + " with ('connector' = 'fluss', 'bootstrap.servers' = 'localhost:9092', 'table.unknown.option' = 'table-unknown-val', 'client.unknown.option' = 'client-unknown-val')");

        // test table as source
        String sourcePlan = tEnv.explainSql("select * from test_table_unknown_options");
        assertThat(sourcePlan)
                .contains(
                        "TableSourceScan(table=[[testcatalog, fluss, test_table_unknown_options]], fields=[a, b])");

        // test table as sink
        String sinkPlan = tEnv.explainSql("insert into test_table_unknown_options values (1, 2)");
        assertThat(sinkPlan)
                .contains(
                        "Sink(table=[testcatalog.fluss.test_table_unknown_options], fields=[EXPR$0, EXPR$1])");

        // create fluss table with other invalid unknown option
        tEnv.executeSql(
                "create table test_table_other_unknown_options (a int, b int)"
                        + " with ('connector' = 'fluss', 'bootstrap.servers' = 'localhost:9092', 'other.unknown.option' = 'other-unknown-val')");

        // test invalid table as source
        assertThatThrownBy(() -> tEnv.explainSql("select * from test_table_other_unknown_options"))
                .cause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options found for 'fluss'");

        // test invalid table as sink
        assertThatThrownBy(
                        () ->
                                tEnv.explainSql(
                                        "insert into test_table_other_unknown_options values (1, 2)"))
                .cause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options found for 'fluss'");
    }

    @Test
    void testAuthentication() throws Exception {
        String clientListenerName = "CLIENT";
        Configuration serverConfig = new Configuration();
        serverConfig.setString(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:username_password");
        serverConfig.setString("security.username_password.credentials", "root:password");
        serverConfig.setString(ConfigOptions.SUPER_USERS.key(), "USER:root");
        FlussClusterExtension flussClusterExtension =
                FlussClusterExtension.builder()
                        .setCoordinatorServerListeners(
                                String.format(
                                        "%s://localhost:0, %s://localhost:0",
                                        DEFAULT_LISTENER_NAME, clientListenerName))
                        .setTabletServerListeners(
                                String.format(
                                        "%s://localhost:0, %s://localhost:0",
                                        DEFAULT_LISTENER_NAME, clientListenerName))
                        .setClusterConf(serverConfig)
                        .build();
        Catalog authenticateCatalog = null;
        try {
            flussClusterExtension.start();
            ServerNode coordinatorServerNode =
                    flussClusterExtension.getCoordinatorServerNode(clientListenerName);
            String bootstrapServers =
                    String.format(
                            "%s:%d", coordinatorServerNode.host(), coordinatorServerNode.port());
            authenticateCatalog =
                    new FlinkCatalog(
                            CATALOG_NAME,
                            DEFAULT_DB,
                            bootstrapServers,
                            Thread.currentThread().getContextClassLoader(),
                            Collections.emptyMap());
            Catalog finalAuthenticateCatalog = authenticateCatalog;
            assertThatThrownBy(finalAuthenticateCatalog::open)
                    .cause()
                    .hasMessageContaining(
                            "The connection has not completed authentication yet. This may be caused by a missing or incorrect configuration of 'client.security.protocol' on the client side.");

            Map<String, String> clientConfig = new HashMap<>();
            clientConfig.put(ConfigOptions.CLIENT_SECURITY_PROTOCOL.key(), "username_password");
            clientConfig.put("client.security.username_password.username", "root");
            clientConfig.put("client.security.username_password.password", "password");
            authenticateCatalog =
                    new FlinkCatalog(
                            CATALOG_NAME,
                            DEFAULT_DB,
                            bootstrapServers,
                            Thread.currentThread().getContextClassLoader(),
                            clientConfig);
            authenticateCatalog.open();
            assertThat(authenticateCatalog.listDatabases())
                    .containsExactlyInAnyOrderElementsOf(Collections.singletonList(DEFAULT_DB));

        } finally {
            if (authenticateCatalog != null) {
                authenticateCatalog.close();
            }
            flussClusterExtension.close();
        }
    }

    private static void assertOptionsEqual(
            Map<String, String> actualOptions, Map<String, String> expectedOptions) {
        actualOptions.remove(ConfigOptions.BOOTSTRAP_SERVERS.key());
        actualOptions.remove(ConfigOptions.TABLE_REPLICATION_FACTOR.key());
        assertThat(actualOptions.size()).isEqualTo(expectedOptions.size());
        assertThat(actualOptions).isEqualTo(expectedOptions);
    }
}
