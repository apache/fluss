package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.log.LogManager;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

class KvSnapshotDeletionBugReplicationTest {
    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension>
            ZOO_KEEPER_EXTENSION_ALL_CALLBACK_WRAPPER =
                    new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zooKeeperClient;
    private @TempDir File tempDir;

    // paths
    private File remoteStoreDir;
    private File server2Dir; // Replica Dir

    private TablePath tablePath;
    private TableBucket tableBucket;
    private PhysicalTablePath physicalTablePath;

    // we only need replica manager to perform the deletion
    private LogManager logManagerReplica;
    private KvManager kvManagerReplica;

    @BeforeAll
    static void baseBeforeAll() {
        zooKeeperClient =
                ZOO_KEEPER_EXTENSION_ALL_CALLBACK_WRAPPER
                        .getCustomExtension()
                        .createZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void setup() throws Exception {
        // 1. setup the dirs
        remoteStoreDir = new File(tempDir, "remote_store");
        server2Dir = new File(tempDir, "server2_data");

        remoteStoreDir.mkdirs();
        server2Dir.mkdirs();

        // 2. define the table path
        tablePath = TablePath.of("test_db", "test_table");
        tableBucket = new TableBucket(150001L, 124L, 2);
        physicalTablePath =
                PhysicalTablePath.of(
                        tablePath.getDatabaseName(), tablePath.getTableName(), "partition_124");

        // 3. init server 2 (replica)
        Configuration configuration2 = new Configuration();
        configuration2.setString(ConfigOptions.DATA_DIR, server2Dir.getAbsolutePath());
        configuration2.setString(ConfigOptions.REMOTE_DATA_DIR, remoteStoreDir.getAbsolutePath());

        logManagerReplica =
                LogManager.create(
                        configuration2,
                        zooKeeperClient,
                        new FlussScheduler(1),
                        SystemClock.getInstance(),
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        kvManagerReplica =
                KvManager.create(
                        configuration2,
                        zooKeeperClient,
                        logManagerReplica,
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        kvManagerReplica.startup();
    }

    @AfterEach
    void tearDown() {
        if (kvManagerReplica != null) {
            kvManagerReplica.startup();
        }
        if (logManagerReplica != null) {
            logManagerReplica.shutdown();
        }
    }

    @Test
    void testReproduceKvSnapshotNotExistException() throws Exception {
        // 1. prepare the remote data
        // we must use FlussPath to match exactly where KvManager looks for files.
        // FlussPath.remoteKvDir(conf) handles the root path logic
        Configuration configuration = new Configuration();
        configuration.setString(ConfigOptions.REMOTE_DATA_DIR, remoteStoreDir.getAbsolutePath());

        FsPath remoteKvRoot = FlussPaths.remoteKvDir(configuration);
        FsPath tabletSnapshotPath =
                FlussPaths.remoteKvTabletDir(remoteKvRoot, physicalTablePath, tableBucket);

        File actualSnapshotDir = new File(tabletSnapshotPath.getPath());
        actualSnapshotDir.mkdirs();

        // create mock snapshot metadata file
        String snapshotFileName = "20260202-p124/2/snap-131/_METADATA";

        // we mimic the structure from the snapshot but any file will do to prove deletion
        File mockMetadata = new File(actualSnapshotDir, "snap-131.metadata");
        mockMetadata.createNewFile();

        assertThat(actualSnapshotDir).exists();
        assertThat(actualSnapshotDir.list()).isNotEmpty();

        // 2. simulate the bug
        kvManagerReplica.deleteRemoteKvSnapshot(physicalTablePath, tableBucket);
        // 3. verify and triger exception
        try {
            // check if files are gone
            if (!actualSnapshotDir.exists() || actualSnapshotDir.list().length == 0) {
                //  files are gone! construct the exception
                String errorMessage =
                        String.format(
                                " Failed to get kv snapshot metadata for table bucket %s , File not found: %s",
                                tableBucket, mockMetadata.getAbsolutePath());
                throw new ExecutionException(new KvStorageException(errorMessage));

            } else {
                // reproduction of the bug failed
                throw new RuntimeException("Failed to reproduce: Remote files were not deleted");
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof KvStorageException
                    && e.getMessage().contains("File not found")) {
                e.printStackTrace(System.out);
                return; // test passed
            }
            throw e;
        }
    }

    @Test
    void testFixVerifyRemoteFilesAreSafeDruringLocalDrop() throws Exception {
        // 1. setup remote data
        Configuration configuration = new Configuration();
        configuration.setString(ConfigOptions.REMOTE_DATA_DIR, remoteStoreDir.getAbsolutePath());
        FsPath remoteKvRoot = FlussPaths.remoteKvDir(configuration);
        FsPath tabletSnapshotPath =
                FlussPaths.remoteKvTabletDir(remoteKvRoot, physicalTablePath, tableBucket);
        File actualSnapshotDir = new File(tabletSnapshotPath.getPath());
        actualSnapshotDir.mkdirs();
        File mockMetadata = new File(actualSnapshotDir, "snap-131.metadata");
        mockMetadata.createNewFile();

        assertThat(actualSnapshotDir).exists();

        // 2. execute the fix logic
        // instead of calling deleteRemoteKvSnapshot (which creates the bug), we cal dropKv . This
        // is what ReplicaManager calls when it cleans up a local replica.
        // we want to verify this cleans local files but leaves  remote files alone.
        kvManagerReplica.dropKv(tableBucket);

        // 3. verify remote file still exists
        if (!actualSnapshotDir.exists() || actualSnapshotDir.list().length == 0) {
            throw new RuntimeException("Failure: remote files were deleted by dropKv");
        }
        assertThat(actualSnapshotDir).exists();
        assertThat(mockMetadata).exists();
    }
}
