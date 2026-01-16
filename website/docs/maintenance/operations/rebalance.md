---
title: Rebalance
sidebar_position: 2
---
# Rebalance

## Overview

Fluss provides cluster rebalancing capabilities to redistribute buckets across TabletServers based on workload and cluster topology changes. Rebalancing helps maintain optimal resource utilization and ensures balanced load distribution across the cluster.

Rebalancing primarily occurs in the following scenarios:
- **Scaling Down**: Offline existing TabletServers from the cluster
- **Scaling Up**: Adding new TabletServers to the cluster
- **Load Balancing**: Routine adjustments for load imbalance across TabletServers

## When to Rebalance

You should consider triggering a rebalance operation in these situations:

1. **Server Decommissioning**: Before removing a TabletServer from the cluster permanently
2. **Server Maintenance**: Before temporarily taking a TabletServer offline for upgrades or maintenance
3. **Cluster Expansion**: After adding new TabletServers to distribute load to the new nodes
4. **Load Imbalance**: When monitoring shows uneven distribution of replicas or leaders across TabletServers

## Rebalance Workflow

A typical rebalance workflow consists of the following steps:

### 1. Tag Servers (Optional but Recommended)

Before rebalancing, tag TabletServers that need special handling:

```java
import org.apache.fluss.cluster.rebalance.ServerTag;

// Mark a server for permanent decommissioning
admin.addServerTag(Collections.singletonList(0), ServerTag.PERMANENT_OFFLINE).get();

// Mark servers for temporary maintenance
admin.addServerTag(Arrays.asList(1, 2), ServerTag.TEMPORARY_OFFLINE).get();
```

Server tags help the rebalance algorithm make informed decisions:
- **PERMANENT_OFFLINE**: All buckets on these servers will be migrated away
- **TEMPORARY_OFFLINE**: Buckets may be temporarily migrated, with possibility to return after maintenance

### 2. Trigger Rebalance

Initiate a rebalance operation with specified optimization goals:

```java
import org.apache.fluss.cluster.rebalance.GoalType;

// Trigger rebalance with replica distribution goal
List<GoalType> goals = Collections.singletonList(GoalType.REPLICA_DISTRIBUTION);
String rebalanceId = admin.rebalance(goals).get();
System.out.println("Rebalance started with ID: " + rebalanceId);

// Trigger rebalance with multiple goals in priority order
List<GoalType> multipleGoals = Arrays.asList(
    GoalType.REPLICA_DISTRIBUTION,
    GoalType.LEADER_DISTRIBUTION
);
String rebalanceId = admin.rebalance(multipleGoals).get();
```

Available rebalance goals:
- **REPLICA_DISTRIBUTION**: Ensures the number of replicas on each TabletServer is near balanced
- **LEADER_DISTRIBUTION**: Ensures the number of leader replicas on each TabletServer is near balanced

### 3. Monitor Progress

Track the rebalance operation using the returned rebalance ID:

```java
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;

// Query specific rebalance progress
Optional<RebalanceProgress> progress = admin.listRebalanceProgress(rebalanceId).get();

if (progress.isPresent()) {
    RebalanceProgress p = progress.get();
    System.out.println("Rebalance ID: " + p.rebalanceId());
    System.out.println("Status: " + p.status());
    System.out.println("Progress: " + (p.progress() * 100) + "%");
    
    // Check if rebalance is complete
    if (p.status() == RebalanceStatus.COMPLETED) {
        System.out.println("Rebalance completed successfully!");
    }
}

// Query the most recent rebalance progress (if rebalanceId is not provided)
Optional<RebalanceProgress> latestProgress = admin.listRebalanceProgress(null).get();
```

Rebalance statuses:
- **NOT_STARTED**: The rebalance has been created but not yet started
- **REBALANCING**: The rebalance is currently in progress
- **COMPLETED**: The rebalance has successfully completed
- **FAILED**: The rebalance has failed
- **CANCELED**: The rebalance has been canceled

### 4. Cancel Rebalance (If Needed)

Cancel an ongoing rebalance operation if necessary:

```java
// Cancel a specific rebalance
admin.cancelRebalance(rebalanceId).get();

// Cancel the most recent rebalance
admin.cancelRebalance(null).get();
```

**Important Notes:**
- Only rebalance operations in `NOT_STARTED` or `REBALANCING` status can be canceled
- Already completed bucket migrations will not be rolled back
- After cancellation, the rebalance status will change to `CANCELED`

### 5. Remove Server Tags (After Completion)

After rebalance completes and maintenance is done, remove server tags to restore normal operation:

```java
// Remove tags from servers that are back online
admin.removeServerTag(Arrays.asList(1, 2), ServerTag.TEMPORARY_OFFLINE).get();
```

## Using Java Client

Here is a complete example demonstrating the rebalance workflow using the Java Client:

```java
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.cluster.rebalance.GoalType;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.cluster.rebalance.ServerTag;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class RebalanceExample {
    public static void main(String[] args) throws Exception {
        Admin admin = // ... create admin client
        
        try {
            // Step 1: Tag servers for decommissioning
            System.out.println("Tagging server 0 for permanent offline...");
            admin.addServerTag(Collections.singletonList(0), ServerTag.PERMANENT_OFFLINE).get();
            
            // Step 2: Trigger rebalance
            System.out.println("Triggering rebalance...");
            List<GoalType> goals = Arrays.asList(
                GoalType.REPLICA_DISTRIBUTION,
                GoalType.LEADER_DISTRIBUTION
            );
            String rebalanceId = admin.rebalance(goals).get();
            System.out.println("Rebalance started with ID: " + rebalanceId);
            
            // Step 3: Monitor progress
            boolean isComplete = false;
            while (!isComplete) {
                Thread.sleep(5000); // Poll every 5 seconds
                
                Optional<RebalanceProgress> progress = admin.listRebalanceProgress(rebalanceId).get();
                if (progress.isPresent()) {
                    RebalanceProgress p = progress.get();
                    System.out.printf("Status: %s, Progress: %.2f%%\n", 
                        p.status(), p.progress() * 100);
                    
                    if (RebalanceStatus.FINAL_STATUSES.contains(p.status())) {
                        isComplete = true;
                        System.out.println("Rebalance finished with status: " + p.status());
                    }
                }
            }
            
            // Step 4: Verify completion
            Optional<RebalanceProgress> finalProgress = admin.listRebalanceProgress(rebalanceId).get();
            if (finalProgress.isPresent() && 
                finalProgress.get().status() == RebalanceStatus.COMPLETED) {
                System.out.println("Rebalance completed successfully!");
            }
            
        } catch (Exception e) {
            System.err.println("Rebalance failed: " + e.getMessage());
            // Optionally cancel the rebalance on error
            // admin.cancelRebalance(rebalanceId).get();
        } finally {
            admin.close();
        }
    }
}
```

## Using Flink Stored Procedures

For rebalancing operations, Fluss provides convenient Flink stored procedures that can be called directly from Flink SQL. See [Rebalance Procedures](/docs/engine-flink/procedures.md#rebalance-procedures) for detailed documentation on using the following procedures:

- **add_server_tag**: Tag servers before rebalancing
- **remove_server_tag**: Remove tags after rebalancing
- **rebalance**: Trigger rebalance operation
- **list_rebalance**: Monitor rebalance progress
- **cancel_rebalance**: Cancel ongoing rebalance

Example using Flink SQL:

```sql
-- Tag a server for permanent offline
CALL sys.add_server_tag('0', 'PERMANENT_OFFLINE');

-- Trigger rebalance
CALL sys.rebalance('REPLICA_DISTRIBUTION,LEADER_DISTRIBUTION');

-- Monitor progress
CALL sys.list_rebalance();

-- Cancel if needed
CALL sys.cancel_rebalance();
```

## Best Practices

1. **Plan Ahead**: Tag servers appropriately before triggering rebalance to guide the algorithm
2. **Monitor Progress**: Regularly check rebalance status to ensure smooth operation
3. **Off-Peak Hours**: Schedule rebalance operations during off-peak hours to minimize impact
4. **Single Rebalance**: Fluss supports only one active rebalance task at a time in the cluster
5. **Backup First**: For production environments, ensure data is backed up before major topology changes
6. **Goal Priority**: Order rebalance goals by priority - the system attempts to achieve them in order
7. **Server Tags**: Use `TEMPORARY_OFFLINE` for maintenance scenarios to allow buckets to return after maintenance

## Troubleshooting

### Rebalance Fails to Start

If rebalance fails to start:
- Check if another rebalance is already in progress
- Verify that all TabletServers are healthy and reachable
- Ensure the specified server IDs exist in the cluster

### Rebalance Takes Too Long

If rebalance is taking longer than expected:
- Check network bandwidth between TabletServers
- Verify disk I/O performance on TabletServers
- Monitor cluster load and resource utilization
- Consider canceling and retrying with fewer goals

### Rebalance Stuck in REBALANCING Status

If rebalance appears stuck:
- Check TabletServer logs for errors
- Verify network connectivity between servers
- Use `list_rebalance` to check detailed bucket progress
- If necessary, cancel and restart the rebalance operation
