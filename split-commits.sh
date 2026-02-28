#!/bin/bash
set -e

# 拆分大commit的脚本
# 用法: bash split-commits.sh

BASE_COMMIT="fbdb7fa9"  # [server] Fluss server support multiple paths for remote storage 的父commit
BIG_COMMIT="4b050842"   # 需要拆分的大commit
LATEST_COMMIT="f9542f1a"  # address yang's comments

echo "========================================="
echo "开始拆分 commit: $BIG_COMMIT"
echo "========================================="

# 1. 创建一个新分支用于拆分
SPLIT_BRANCH="multiple-remote-paths-split"
echo ""
echo "步骤 1: 创建新分支 $SPLIT_BRANCH"
git checkout -b $SPLIT_BRANCH $BASE_COMMIT

# =====================================
# Commit 1: 添加配置选项和工具类
# =====================================
echo ""
echo "========================================="
echo "创建 Commit 1: 添加配置选项和工具类"
echo "========================================="

git checkout $BIG_COMMIT -- \
  fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java \
  fluss-common/src/main/java/org/apache/fluss/config/FlussConfigUtils.java \
  fluss-common/src/test/java/org/apache/fluss/config/FlussConfigUtilsTest.java \
  website/docs/maintenance/configuration.md

git add -A
git commit -m "[config] Add configuration options for multiple remote data directories

Add new config options to support multiple remote storage paths:
- remote.data.dirs: list of remote data directories
- remote.data.dirs.strategy: selection strategy (ROUND_ROBIN/WEIGHTED_ROUND_ROBIN)
- remote.data.dirs.weights: weights for weighted round-robin strategy

Also add FlussConfigUtils methods for parsing and validating these configs."

# =====================================
# Commit 2: 扩展元数据模型
# =====================================
echo ""
echo "========================================="
echo "创建 Commit 2: 扩展元数据模型"
echo "========================================="

git checkout $BIG_COMMIT -- \
  fluss-common/src/main/java/org/apache/fluss/metadata/TableInfo.java \
  fluss-common/src/main/java/org/apache/fluss/metadata/PartitionInfo.java \
  fluss-common/src/main/java/org/apache/fluss/utils/FlussPaths.java \
  fluss-common/src/main/java/org/apache/fluss/remote/RemoteLogSegment.java \
  fluss-common/src/test/java/org/apache/fluss/record/TestData.java \
  fluss-common/src/test/java/org/apache/fluss/utils/PartitionUtilsTest.java \
  fluss-rpc/src/main/proto/FlussApi.proto \
  fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java \
  fluss-server/src/test/java/org/apache/fluss/server/testutils/RpcMessageTestUtils.java

git add -A
git commit -m "[metadata] Extend metadata models to support per-table/partition remote data directory

Extend TableInfo and PartitionInfo to include remoteDataDir field.
Update RPC protocol and FlussPaths utilities accordingly.
This allows each table/partition to have its own remote storage path."

# =====================================
# Commit 3: 添加ZooKeeper数据模型
# =====================================
echo ""
echo "========================================="
echo "创建 Commit 3: 添加ZooKeeper数据模型"
echo "========================================="

git checkout $BIG_COMMIT -- \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/PartitionRegistration.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/PartitionRegistrationJsonSerde.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/TableRegistration.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/TableRegistrationJsonSerde.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/ZkData.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/ZooKeeperClient.java \
  fluss-server/src/test/java/org/apache/fluss/server/zk/data/PartitionRegistrationJsonSerdeTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/zk/data/TableRegistrationJsonSerdeTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/zk/ZooKeeperClientTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/zk/ZooKeeperExtension.java \
  fluss-server/src/test/java/org/apache/fluss/server/zk/ZooKeeperTestUtils.java

git add -A
git commit -m "[zk] Add PartitionRegistration to persist remote data directory in ZooKeeper

Introduce PartitionRegistration data structure to store partition's remote
data directory in ZooKeeper. Also update TableRegistration to include
remoteDataDir field. This ensures remote paths are persisted and can
survive restarts."

# =====================================
# Commit 4: 实现远程目录选择策略
# =====================================
echo ""
echo "========================================="
echo "创建 Commit 4: 实现远程目录选择策略"
echo "========================================="

git checkout $BIG_COMMIT -- \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/remote/RemoteDirSelector.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/remote/RoundRobinRemoteDirSelector.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/remote/WeightedRoundRobinRemoteDirSelector.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/remote/RemoteDirDynamicLoader.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/remote/RoundRobinRemoteDirSelectorTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/remote/WeightedRoundRobinRemoteDirSelectorTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/remote/RemoteDirDynamicLoaderTest.java

git add -A
git commit -m "[coordinator] Implement remote directory selection strategies

Add RemoteDirSelector interface with two implementations:
- RoundRobinRemoteDirSelector: simple round-robin selection
- WeightedRoundRobinRemoteDirSelector: weighted round-robin selection

Also add RemoteDirDynamicLoader to support dynamic updates of remote
directories without restarting the coordinator."

# =====================================
# Commit 5: 集成到Coordinator Server
# =====================================
echo ""
echo "========================================="
echo "创建 Commit 5: 集成到Coordinator Server"
echo "========================================="

git checkout $BIG_COMMIT -- \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorServer.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorService.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorContext.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorEventProcessor.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorRequestBatch.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/TableManager.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/AutoPartitionManager.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/MetadataManager.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/event/CreatePartitionEvent.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/event/watcher/TableChangeWatcher.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/CoordinatorContextTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/CoordinatorEventProcessorTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/TableManagerTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/AutoPartitionManagerTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/event/watcher/TableChangeWatcherTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/rebalance/RebalanceManagerTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/statemachine/ReplicaStateMachineTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/statemachine/TableBucketStateMachineTest.java

git add -A
git commit -m "[coordinator] Integrate remote directory selector into table/partition creation

Update Coordinator to use RemoteDirSelector when creating tables and partitions.
Each table/partition is now assigned a remote data directory from the configured
pool of directories according to the selection strategy."

# =====================================
# Commit 6: 更新Tablet Server和存储层
# =====================================
echo ""
echo "========================================="
echo "创建 Commit 6: 更新Tablet Server和存储层"
echo "========================================="

git checkout $BIG_COMMIT -- \
  fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletServer.java \
  fluss-server/src/main/java/org/apache/fluss/server/TabletManagerBase.java \
  fluss-server/src/main/java/org/apache/fluss/server/RpcServiceBase.java \
  fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java \
  fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java \
  fluss-server/src/main/java/org/apache/fluss/server/log/remote/RemoteLogManager.java \
  fluss-server/src/main/java/org/apache/fluss/server/log/remote/RemoteLogTablet.java \
  fluss-server/src/main/java/org/apache/fluss/server/log/remote/RemoteLogStorage.java \
  fluss-server/src/main/java/org/apache/fluss/server/log/remote/DefaultRemoteLogStorage.java \
  fluss-server/src/main/java/org/apache/fluss/server/log/remote/RemoteLogManifest.java \
  fluss-server/src/main/java/org/apache/fluss/server/log/remote/RemoteLogManifestJsonSerde.java \
  fluss-server/src/main/java/org/apache/fluss/server/log/remote/LogTieringTask.java \
  fluss-server/src/main/java/org/apache/fluss/server/kv/KvManager.java \
  fluss-server/src/main/java/org/apache/fluss/server/kv/snapshot/DefaultSnapshotContext.java \
  fluss-server/src/main/java/org/apache/fluss/server/kv/snapshot/SnapshotContext.java \
  fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaManagerTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTestBase.java \
  fluss-server/src/test/java/org/apache/fluss/server/replica/NotifyReplicaLakeTableOffsetTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/replica/fetcher/ReplicaFetcherThreadTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/log/LogManagerTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/log/remote/RemoteLogManagerTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/log/remote/DefaultRemoteLogStorageTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/log/remote/RemoteLogManifestJsonSerdeTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/log/remote/RemoteLogTestBase.java \
  fluss-server/src/test/java/org/apache/fluss/server/log/remote/TestingRemoteLogStorage.java \
  fluss-server/src/test/java/org/apache/fluss/server/kv/snapshot/KvTabletSnapshotTargetTest.java

git add -A
git commit -m "[server] Update Tablet Server to use per-table/partition remote data directory

Update TabletServer, RemoteLogManager, and KvManager to use the remote data
directory from metadata instead of the global config. Each table bucket now
uses its assigned remote path for tiering and snapshots."

# =====================================
# Commit 7: 重构RemoteStorageCleaner和添加综合测试
# =====================================
echo ""
echo "========================================="
echo "创建 Commit 7: 重构RemoteStorageCleaner和添加综合测试"
echo "========================================="

# 获取所有剩余的修改文件
git checkout $BIG_COMMIT -- \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/remote/RemoteStorageCleaner.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/lake/LakeTableHelper.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/remote/RemoteDirsITCase.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/remote/RemoteStorageCleanerITCase.java \
  fluss-server/src/test/java/org/apache/fluss/server/kv/snapshot/KvSnapshotMultipleDirsITCase.java \
  fluss-server/src/test/java/org/apache/fluss/server/log/remote/RemoteLogITCase.java \
  fluss-server/src/test/java/org/apache/fluss/server/log/remote/RemoteLogTTLTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/log/DroppedTableRecoveryTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/TableManagerITCase.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/LakeTableTieringManagerTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/metadata/ServerSchemaCacheTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/metadata/TabletServerMetadataCacheTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/metadata/ZkBasedMetadataProviderTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/replica/KvReplicaRestoreITCase.java \
  fluss-server/src/test/java/org/apache/fluss/server/kv/snapshot/ZooKeeperCompletedSnapshotStoreTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/testutils/FlussClusterExtension.java \
  fluss-server/src/test/java/org/apache/fluss/server/authorizer/DefaultAuthorizerTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/DynamicConfigChangeTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/zk/data/lake/LakeTableHelperTest.java \
  fluss-client/src/main/java/org/apache/fluss/client/admin/FlussAdmin.java \
  fluss-client/src/main/java/org/apache/fluss/client/table/scanner/log/RemoteLogDownloader.java \
  fluss-client/src/test/java/org/apache/fluss/client/table/scanner/log/DefaultCompletedFetchTest.java \
  fluss-client/src/test/java/org/apache/fluss/client/table/scanner/log/LogFetcherITCase.java \
  fluss-client/src/test/java/org/apache/fluss/client/table/scanner/log/RemoteCompletedFetchTest.java \
  fluss-client/src/test/java/org/apache/fluss/client/write/RecordAccumulatorTest.java \
  fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/sink/undo/RecoveryOffsetManagerTest.java \
  fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/source/enumerator/FlinkSourceEnumeratorTest.java \
  fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/utils/FlinkConversionsTest.java \
  fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/utils/FlinkTestBase.java \
  fluss-lake/fluss-lake-iceberg/src/test/java/org/apache/fluss/lake/iceberg/tiering/IcebergTieringTest.java \
  fluss-lake/fluss-lake-lance/src/test/java/org/apache/fluss/lake/lance/tiering/LanceTieringTest.java \
  fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/tiering/PaimonTieringTest.java

git add -A
git commit -m "[test] Add comprehensive integration tests for multiple remote directories

Move RemoteStorageCleaner to remote package and update it to handle
multiple directories. Add extensive IT tests to verify:
- Remote directory selection strategies
- KV snapshot with multiple directories
- Remote log tiering with multiple directories
- Remote storage cleanup across multiple directories

Also update client and connector tests to work with the new metadata."

# =====================================
# 应用最后的修复commit
# =====================================
echo ""
echo "========================================="
echo "应用最后的修复 commit"
echo "========================================="

git cherry-pick $LATEST_COMMIT

echo ""
echo "========================================="
echo "拆分完成!"
echo "========================================="
echo ""
echo "新分支: $SPLIT_BRANCH"
echo "原始commit已被拆分为 7 个小commit + 1 个修复commit"
echo ""
echo "查看拆分后的commits:"
echo "  git log --oneline $BASE_COMMIT..$SPLIT_BRANCH"
echo ""
echo "如果满意,可以将原分支指向新分支:"
echo "  git branch -f multiple-remote-paths $SPLIT_BRANCH"
echo "  git checkout multiple-remote-paths"
echo "  git branch -D $SPLIT_BRANCH"
echo ""
echo "或者推送新分支:"
echo "  git push -f my $SPLIT_BRANCH:multiple-remote-paths"
echo ""
