package com.alibaba.fluss.flink.catalog;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;


public class FlinkProcedureITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setClusterConf(initConfig())
                    .build();

    static final String CATALOG_NAME = "testcatalog";

    private TableEnvironment tEnv;



    @BeforeEach
    void before() throws ExecutionException, InterruptedException {
        String bootstrapServers = String.join(",", FLUSS_CLUSTER_EXTENSION.getClientConfig("CLIENT").get(ConfigOptions.BOOTSTRAP_SERVERS));
        // create table environment
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        String catalogDDL =
                String.format(
                        "create catalog %s with ( \n"
                                + "'type' = 'fluss', \n"
                                + "'bootstrap.servers' = '%s', \n"
                                + "'client.security.protocol' = 'username_password', \n"
                                + "'client.security.username_password.username' = 'root', \n"
                                + "'client.security.username_password.password' = 'password' \n"
                                + ")",
                        CATALOG_NAME, bootstrapServers);
        tEnv.executeSql(catalogDDL).await();
        tEnv.executeSql("use catalog " + CATALOG_NAME);
    }

    @Test
    void testShowProcedures() throws Exception{
       try( CloseableIterator<Row> showProcedures = tEnv.executeSql("show procedures").collect()) {
           System.out.println("show procedures" + showProcedures);
       }

    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for testing purpose
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        // set a shorter max lag time to make tests in FlussFailServerTableITCase faster
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(10));
        // set default datalake format for the cluster and enable datalake tables
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);

        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));

        // set security information.
        conf.setString(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:username_password");
        conf.setString("security.username_password.credentials", "root:password,guest:password2");
        conf.set(ConfigOptions.SUPER_USERS, "USER:root");
        conf.set(ConfigOptions.AUTHORIZER_ENABLED, true);
        return conf;
    }

}
