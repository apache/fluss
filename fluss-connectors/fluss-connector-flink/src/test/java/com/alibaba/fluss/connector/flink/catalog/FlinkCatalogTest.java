/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.catalog;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import com.alibaba.fluss.utils.ExceptionUtils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.connector.flink.FlinkConnectorOptions.BUCKET_KEY;
import static com.alibaba.fluss.connector.flink.FlinkConnectorOptions.BUCKET_NUMBER;
import static com.alibaba.fluss.connector.flink.FlinkConnectorOptions.SCAN_STARTUP_MODE;
import static com.alibaba.fluss.connector.flink.utils.CatalogTableTestUtils.addOptions;
import static com.alibaba.fluss.connector.flink.utils.CatalogTableTestUtils.checkEqualsIgnoreSchema;
import static com.alibaba.fluss.connector.flink.utils.CatalogTableTestUtils.checkEqualsRespectSchema;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkCatalog}. */
class FlinkCatalogTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    private static final String CATALOG_NAME = "test-catalog";
    private static final String DEFAULT_DB = "default";
    static Catalog catalog;
    private final ObjectPath tableInDefaultDb = new ObjectPath(DEFAULT_DB, "t1");

    private ResolvedSchema createSchema() {
        return new ResolvedSchema(
                Arrays.asList(
                        Column.physical("first", DataTypes.STRING().notNull()),
                        Column.physical("second", DataTypes.INT()),
                        Column.physical("third", DataTypes.STRING().notNull())),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("PK_first_third", Arrays.asList("first", "third")));
    }

    private CatalogTable newCatalogTable(Map<String, String> options) {
        ResolvedSchema resolvedSchema = this.createSchema();
        return newCatalogTable(resolvedSchema, options);
    }

    private CatalogTable newCatalogTable(
            ResolvedSchema resolvedSchema, Map<String, String> options) {
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        Collections.emptyList(),
                        options);
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    @BeforeAll
    static void beforeAll() {
        // set fluss conf
        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        catalog =
                new FlinkCatalog(
                        CATALOG_NAME,
                        DEFAULT_DB,
                        String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS)),
                        Thread.currentThread().getContextClassLoader());
        catalog.open();
    }

    @AfterAll
    static void afterAll() {
        if (catalog != null) {
            catalog.close();
        }
    }

    @BeforeEach
    void beforeEach() throws Exception {
        // First check if database exists, and drop it if it does
        if (catalog.databaseExists(DEFAULT_DB)) {
            catalog.dropDatabase(DEFAULT_DB, true, true);
        }
        try {
            catalog.createDatabase(
                    DEFAULT_DB, new CatalogDatabaseImpl(Collections.emptyMap(), null), true);
        } catch (CatalogException e) {
            // the auto partitioned manager may create the db zk node
            // in an another thread, so if exception is NodeExistsException, just ignore
            if (!ExceptionUtils.findThrowable(e, KeeperException.NodeExistsException.class)
                    .isPresent()) {
                throw e;
            }
        }
    }

    @Test
    void testCreateTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        assertThatThrownBy(() -> catalog.getTable(tableInDefaultDb))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage(
                        String.format(
                                "Table (or view) %s does not exist in Catalog %s.",
                                tableInDefaultDb, CATALOG_NAME));
        CatalogTable table = this.newCatalogTable(options);
        catalog.createTable(this.tableInDefaultDb, table, false);
        assertThat(catalog.tableExists(this.tableInDefaultDb)).isTrue();
        // create the table again, should throw exception with ignore if exist = false
        assertThatThrownBy(() -> catalog.createTable(this.tableInDefaultDb, table, false))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessage(
                        String.format(
                                "Table (or view) %s already exists in Catalog %s.",
                                this.tableInDefaultDb, CATALOG_NAME));
        // should be ok since we set ignore if exist = true
        catalog.createTable(this.tableInDefaultDb, table, true);
        // get the table and check
        CatalogBaseTable tableCreated = catalog.getTable(this.tableInDefaultDb);

        // put bucket key option
        Map<String, String> addedOptions = new HashMap<>();
        addedOptions.put(BUCKET_KEY.key(), "first,third");
        addedOptions.put(BUCKET_NUMBER.key(), "1");
        CatalogTable expectedTable = addOptions(table, addedOptions);
        checkEqualsRespectSchema((CatalogTable) tableCreated, expectedTable);
        assertThat(tableCreated.getDescription().get()).isEqualTo("test comment");

        // list tables
        List<String> tables = catalog.listTables(DEFAULT_DB);
        assertThat(tables.size()).isEqualTo(1L);
        assertThat(tables.get(0)).isEqualTo(this.tableInDefaultDb.getObjectName());
        catalog.dropTable(this.tableInDefaultDb, false);
        assertThat(catalog.listTables(DEFAULT_DB)).isEmpty();
        // drop the table again, should throw exception with ignoreIfNotExists = false
        assertThatThrownBy(() -> catalog.dropTable(this.tableInDefaultDb, false))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage(
                        String.format(
                                "Table (or view) %s does not exist in Catalog %s.",
                                this.tableInDefaultDb, CATALOG_NAME));
        // should be ok since we set ignoreIfNotExists = true
        catalog.dropTable(this.tableInDefaultDb, true);
        // create table from an non-exist db
        ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");

        // remove bucket-key
        table.getOptions().remove("bucket-key");
        assertThatThrownBy(() -> catalog.createTable(nonExistDbPath, table, false))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage(
                        "Database %s does not exist in Catalog %s.",
                        nonExistDbPath.getDatabaseName(), CATALOG_NAME);

        // test create partition table
        options.put(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true");
        options.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), "day");
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable table2 =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                                "test comment",
                                Collections.singletonList("first"),
                                options),
                        resolvedSchema);
        catalog.createTable(this.tableInDefaultDb, table2, false);
        tableCreated = catalog.getTable(this.tableInDefaultDb);
        // need to over write the option
        addedOptions.put(BUCKET_KEY.key(), "third");

        expectedTable = addOptions(table2, addedOptions);

        checkEqualsRespectSchema((CatalogTable) tableCreated, expectedTable);
    }

    @Test
    void testCreateTableWithBucket() throws Exception {
        // for pk table;
        // set bucket count and bucket key;
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET_NUMBER.key(), "10");
        options.put(BUCKET_KEY.key(), "first,third");

        createAndCheckAndDropTable(createSchema(), tableInDefaultDb, options);

        // for non pk table
        // set nothing;
        ResolvedSchema schema =
                ResolvedSchema.of(Column.physical("first", DataTypes.STRING().notNull()));
        options = new HashMap<>();
        // default is 1
        options.put(BUCKET_NUMBER.key(), "1");
        createAndCheckAndDropTable(schema, tableInDefaultDb, options);

        // set bucket count;
        options.put(BUCKET_NUMBER.key(), "10");
        createAndCheckAndDropTable(schema, tableInDefaultDb, options);

        // set bucket count and bucket key;
        options.put("bucket-key", "first");
        createAndCheckAndDropTable(schema, tableInDefaultDb, options);

        // only set bucket key
        createAndCheckAndDropTable(schema, tableInDefaultDb, options);
    }

    @Test
    void testCreateTableWithWatermarkAndComputedCol() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("k1", "v1");
        options.put(BUCKET_NUMBER.key(), "10");
        ResolvedExpression waterMark =
                new ResolvedExpressionMock(DataTypes.TIMESTAMP(9), () -> "second");
        ResolvedExpressionMock colExpr =
                new ResolvedExpressionMock(DataTypes.STRING().notNull(), () -> "first");
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("first", DataTypes.STRING().notNull()),
                                Column.physical("second", DataTypes.TIMESTAMP()),
                                Column.computed("third", colExpr)),
                        Collections.singletonList(WatermarkSpec.of("second", waterMark)),
                        UniqueConstraint.primaryKey(
                                "PK_first", Collections.singletonList("first")));
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        Collections.emptyList(),
                        new HashMap<>(options));
        CatalogTable originResolvedTable = new ResolvedCatalogTable(origin, resolvedSchema);
        ObjectPath path = new ObjectPath(DEFAULT_DB, "t2");
        catalog.createTable(path, originResolvedTable, false);
        CatalogTable tableCreated = (CatalogTable) catalog.getTable(path);
        // resolve it and check
        TestSchemaResolver resolver = new TestSchemaResolver();
        resolver.addExpression("second", waterMark);
        resolver.addExpression("first", colExpr);
        // check the resolved schema
        assertThat(resolver.resolve(tableCreated.getUnresolvedSchema())).isEqualTo(resolvedSchema);
        // copy the origin options
        originResolvedTable = originResolvedTable.copy(options);

        // not need to check schema now
        // put bucket key option
        CatalogTable expectedTable =
                addOptions(
                        originResolvedTable, Collections.singletonMap(BUCKET_KEY.key(), "first"));
        checkEqualsIgnoreSchema(tableCreated, expectedTable);
        catalog.dropTable(path, false);
    }

    @Test
    void testUnsupportedTable() {
        // test create non fluss table
        Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), "kafka");
        final CatalogTable table = this.newCatalogTable(options);
        assertThatThrownBy(() -> catalog.createTable(this.tableInDefaultDb, table, false))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("Fluss Catalog only supports fluss tables");
        options = new HashMap<>();
        // test create with meta column
        Column metaDataCol = Column.metadata("second", DataTypes.INT(), "k1", true);
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("first", DataTypes.STRING().notNull()),
                                metaDataCol),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey(
                                "PK_first", Collections.singletonList("first")));
        CatalogTable table1 = this.newCatalogTable(resolvedSchema, options);
        assertThatThrownBy(() -> catalog.createTable(this.tableInDefaultDb, table1, false))
                .isInstanceOf(CatalogException.class)
                .hasMessage("Metadata column %s is not supported.", metaDataCol);
    }

    @Test
    void testDatabase() throws Exception {
        // test create db1
        catalog.createDatabase("db1", new CatalogDatabaseImpl(Collections.emptyMap(), null), false);
        // test create db2
        catalog.createDatabase(
                "db2",
                new CatalogDatabaseImpl(
                        Collections.singletonMap(SCAN_STARTUP_MODE.key(), "earliest"),
                        "test comment"),
                false);
        assertThat(catalog.databaseExists("db2")).isTrue();
        CatalogDatabase db2 = catalog.getDatabase("db2");
        assertThat(db2.getComment()).isEqualTo("test comment");
        assertThat(db2.getProperties())
                .isEqualTo(Collections.singletonMap(SCAN_STARTUP_MODE.key(), "earliest"));
        // create the database again should throw exception with ignore if exist = false
        assertThatThrownBy(
                () ->
                        catalog.createDatabase(
                                "db2",
                                new CatalogDatabaseImpl(Collections.emptyMap(), "test comment"),
                                false));
        // should be ok since we set ignore if exist = true
        catalog.createDatabase(
                "db2", new CatalogDatabaseImpl(Collections.emptyMap(), "test comment2"), true);
        db2 = catalog.getDatabase("db2");
        assertThat(db2.getComment()).isEqualTo("test comment");
        assertThat(db2.getProperties())
                .isEqualTo(Collections.singletonMap(SCAN_STARTUP_MODE.key(), "earliest"));
        // test create table in db1
        ObjectPath path1 = new ObjectPath("db1", "t1");
        CatalogTable table = this.newCatalogTable(new HashMap<>());
        catalog.createTable(path1, table, false);
        CatalogBaseTable tableCreated = catalog.getTable(path1);

        // put bucket key and bucket number option
        Map<String, String> addedOptions = new HashMap<>();
        addedOptions.put(BUCKET_KEY.key(), "first,third");
        addedOptions.put(BUCKET_NUMBER.key(), "1");
        CatalogTable expectedTable = addOptions(table, addedOptions);
        checkEqualsRespectSchema((CatalogTable) tableCreated, expectedTable);
        assertThat(catalog.listTables("db1")).isEqualTo(Collections.singletonList("t1"));
        assertThat(catalog.listDatabases())
                .isEqualTo(Arrays.asList(DEFAULT_DB, "db1", "db2", "fluss"));
        // test drop db1;
        // should throw exception since db1 is not empty and we set cascade = false
        assertThatThrownBy(() -> catalog.dropDatabase("db1", false, false))
                .isInstanceOf(DatabaseNotEmptyException.class)
                .hasMessage("Database %s in catalog %s is not empty.", "db1", CATALOG_NAME);
        // should be ok since we set cascade = true
        catalog.dropDatabase("db1", false, true);
        // drop it again, should throw exception since db1 is not exist and we set ignoreIfNotExists
        // = false
        assertThatThrownBy(() -> catalog.dropDatabase("db1", false, true))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage("Database %s does not exist in Catalog %s.", "db1", CATALOG_NAME);
        // should be ok since we set ignoreIfNotExists = true
        catalog.dropDatabase("db1", true, true);
        // test list db
        assertThat(catalog.listDatabases()).isEqualTo(Arrays.asList(DEFAULT_DB, "db2", "fluss"));
        catalog.dropDatabase("db2", false, true);
        // should be empty
        assertThat(catalog.listDatabases()).isEqualTo(Arrays.asList(DEFAULT_DB, "fluss"));
        // should throw exception since the db is not exist and we set ignoreIfNotExists = false
        assertThatThrownBy(() -> catalog.listTables("unknown"))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessage("Database %s does not exist in Catalog %s.", "unknown", CATALOG_NAME);
    }

    private void createAndCheckAndDropTable(
            final ResolvedSchema schema, ObjectPath tablePath, Map<String, String> options)
            throws Exception {
        CatalogTable table = newCatalogTable(schema, options);
        catalog.createTable(tablePath, table, false);
        CatalogBaseTable tableCreated = catalog.getTable(tablePath);
        checkEqualsRespectSchema((CatalogTable) tableCreated, table);
        catalog.dropTable(tablePath, false);
    }
}
