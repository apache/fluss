---
sidebar_label: Monitor Metrics
sidebar_position: 5
---

# Monitor Metrics

Fluss has built a metrics system to measure the behaviours of cluster and table, like the active CoordinatorServer, 
the number of table, the bytes written, the number of records written, etc.

Fluss supports different metric types: **Counters**, **Gauges**, **Histograms**, and **Meters**.

- `Gauge`: Provides a value of any type at a point in time.
- `Counter`: Used to count values by incrementing and decrementing.
- `Histogram`: Measure the statistical distribution of a set of values including the min, max, mean, standard deviation and percentile.
- `Meter`: The gauge exports the meter's rate.

Fluss client also has supported built-in metrics to measure operations of **write to**, **read from** fluss cluster, 
which can be bridged to Flink use Flink connector standard metrics.

## Scope

Every metric is assigned an identifier and a set of key-value pairs under which the metric will be reported.

The identifier is delimited by `metrics.scope.delimiter`. Currently, the `metrics.scope.delimiter` is not configurable, 
it determined by the metric reporter. Take prometheus as example, the scope will delimited by `_`, so the scope like `A_B_C`, 
while Fluss metrics will always begin with `fluss`, as `fluss_A_B_C`.

The key-value pairs are called **variables** and are used to filter metrics. There are no restrictions on the 
number of order of variables. Variables are case-sensitive.

## Reporter

For information on how to set up Fluss's metric reporters please take a look at the [Metric Reporters](./metric-reporters.md) page.

## Metrics List

By default, Fluss provides **cluster state** metrics, **table state** metrics, and bridging to
**Flink connector** standard metrics. This section is a reference of all these metrics.

The tables below generally feature 5 columns:

* The "Scope" column describes which scope format is used to generate the system scope.
  For example, if the cell contains `tabletserver` then the scope format for `fluss_tabletserver` is used.
  If the cell contains multiple values, separated by a slash, then the metrics are reported multiple
  times for different entities, like for both `tabletserver` and `coordinator`.

* The (optional)"Infix" column describes which infix is appended to the scope.

* The "Metrics" column lists the names of all metrics that are registered for the given scope and infix.

* The "Description" column provides information as to what a given metric is measuring.

* The "Type" column describes which metric type is used for the measurement.

Thus, in order to infer the metric identifier:

1. Take the "fluss_" first.
2. Take the scope-format based on the "Scope" column
3. Append the value in the "Infix" column if present, and account for the `metrics.scope.delimiter` setting
4. Append metric name.
5. One metric for prometheus will be like `fluss_tabletserver_status_JVM_CPU_load`

### CPU

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style={{width: '30pt'}}>Scope</th>
      <th class="text-left" style={{width: '150pt'}}>Infix</th>
      <th class="text-left" style={{width: '80pt'}}>Metrics</th>
      <th class="text-left" style={{width: '300pt'}}>Description</th>
      <th class="text-left" style={{width: '40pt'}}>Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2"><strong>coordinator/tabletserver</strong></th>
      <td rowspan="2">status_JVM_CPU</td>
      <td>load</td>
      <td>The recent CPU usage of the JVM.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>time</td>
      <td>The CPU time used by the JVM.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### Memory

The memory-related metrics require Oracle's memory management (also included in OpenJDK's Hotspot implementation) to be in place.
Some metrics might not be exposed when using other JVM implementations (e.g. IBM's J9).

<table class="table table-bordered">                               
  <thead>                                                          
    <tr>                                                           
      <th class="text-left">Scope</th>
      <th class="text-left">Infix</th>
      <th class="text-left">Metrics</th>
      <th class="text-left">Description</th>
      <th class="text-left">Type</th>               
    </tr>                                                          
  </thead>                                                         
  <tbody>                                                          
    <tr>                                                           
      <th rowspan="17"><strong>coordinator/tabletserver</strong></th>
      <td rowspan="15">status_JVM_memory</td>
      <td>heap_used</td>
      <td>The amount of heap memory currently used (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>heap_committed</td>
      <td>The amount of heap memory guaranteed to be available to the JVM (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>heap_max</td>
      <td>The maximum amount of heap memory that can be used for memory management (in bytes). <br/>
      This value might not be necessarily equal to the maximum value specified through -Xmx or
      the equivalent Fluss configuration parameter. Some GC algorithms allocate heap memory that won't
      be available to the user code and, therefore, not being exposed through the heap metrics.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>nonHeap_used</td>
      <td>The amount of non-heap memory currently used (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>nonHeap_committed</td>
      <td>The amount of non-heap memory guaranteed to be available to the JVM (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>nonHeap_max</td>
      <td>The maximum amount of non-heap memory that can be used for memory management (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>metaspace_used</td>
      <td>The amount of memory currently used in the Metaspace memory pool (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>metaspace_committed</td>
      <td>The amount of memory guaranteed to be available to the JVM in the Metaspace memory pool (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>metaspace_max</td>
      <td>The maximum amount of memory that can be used in the Metaspace memory pool (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>direct_count</td>
      <td>The number of buffers in the direct buffer pool.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>direct_memoryUsed</td>
      <td>The amount of memory used by the JVM for the direct buffer pool (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>direct_totalCapacity</td>
      <td>The total capacity of all buffers in the direct buffer pool (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>mapped_count</td>
      <td>The number of buffers in the mapped buffer pool.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>mapped_memoryUsed</td>
      <td>The amount of memory used by the JVM for the mapped buffer pool (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>mapped_totalCapacity</td>
      <td>The number of buffers in the mapped buffer pool (in bytes).</td>
      <td>Gauge</td>
    </tr>
  </tbody>                                                         
</table>

### Threads
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style={{width: '30pt'}}>Scope</th>
      <th class="text-left" style={{width: '150pt'}}>Infix</th>
      <th class="text-left" style={{width: '80pt'}}>Metrics</th>
      <th class="text-left" style={{width: '300pt'}}>Description</th>
      <th class="text-left" style={{width: '40pt'}}>Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="1"><strong>coordinator/tabletserver</strong></th>
      <td rowspan="1">status_JVM_threads</td>
      <td>count</td>
      <td>The total number of live threads.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### GarbageCollection
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style={{width: '30pt'}}>Scope</th>
      <th class="text-left" style={{width: '150pt'}}>Infix</th>
      <th class="text-left" style={{width: '80pt'}}>Metrics</th>
      <th class="text-left" style={{width: '300pt'}}>Description</th>
      <th class="text-left" style={{width: '40pt'}}>Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="3"><strong>coordinator/tabletserver</strong></th>
      <td rowspan="3">status_JVM_GC</td>
      <td>&lt;Collector/all&gt;_count</td>
      <td>The total number of collections that have occurred for the given (or all) collector.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>&lt;Collector/all&gt;_time</td>
      <td>The total time spent performing garbage collection for the given (or all) collector.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>&lt;Collector/all&gt;_timeMsPerSecond</td>
      <td>The time (in milliseconds) spent garbage collecting per second for the given (or all) collector.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### Coordinator Server

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style={{width: '30pt'}}>Scope</th>
      <th class="text-left" style={{width: '150pt'}}>Infix</th>
      <th class="text-left" style={{width: '80pt'}}>Metrics</th>
      <th class="text-left" style={{width: '300pt'}}>Description</th>
      <th class="text-left" style={{width: '40pt'}}>Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="5"><strong>coordinator</strong></th>
      <td style={{textAlign: 'center', verticalAlign: 'middle' }} rowspan="5">-</td>
      <td>activeCoordinatorCount</td>
      <td>The number of active CoordinatorServer in this cluster.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>activeTabletServerCount</td>
      <td>The number of active TabletServer in this cluster.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>offlineBucketCount</td>
      <td>The total number of offline buckets in this cluster.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>tableCount</td>
      <td>The total number of tables in this cluster.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>bucketCount</td>
      <td>The total number of buckets in this cluster.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### Tablet Server

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style={{width: '30pt'}}>Scope</th>
      <th class="text-left" style={{width: '150pt'}}>Infix</th>
      <th class="text-left" style={{width: '80pt'}}>Metrics</th>
      <th class="text-left" style={{width: '300pt'}}>Description</th>
      <th class="text-left" style={{width: '40pt'}}>Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="10"><strong>tabletserver</strong></th>
      <td style={{textAlign: 'center', verticalAlign: 'middle' }} rowspan="6">-</td>
      <td>replicationBytesInPerSecond</td>
      <td>The bytes of data write into follower replica for data sync.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>replicationBytesOutPerSecond</td>
      <td>The bytes of data read from leader replica for data sync.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>leaderCount</td>
      <td>The total number of leader replicas in this TabletServer.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>replicaCount</td>
      <td>The total number of replicas (include follower replicas) in this TabletServer.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>writerIdCount</td>
      <td>The writer id count</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>delayedWriteCount</td>
      <td>The delayed write count in this TabletServer.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>delayedFetchCount</td>
      <td>The delayed fetch log operation count in this TabletServer.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>delayedWriteExpiresPerSecond</td>
      <td>The delayed write operation expire count per second in this TabletServer.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>delayedFetchFromFollowerExpiresPerSecond</td>
      <td>The delayed fetch log operation from follower expire count per second in this TabletServer.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>delayedFetchFromClientExpiresPerSecond</td>
      <td>The delayed fetch log operation from client expire count per second in this TabletServer.</td>
      <td>Meter</td>
    </tr>
  </tbody>
</table>

### Request

<table class="table table-bordered">
  <thead>
    <tr>
     <th class="text-left" style={{width: '30pt'}}>Scope</th>
      <th class="text-left" style={{width: '150pt'}}>Infix</th>
      <th class="text-left" style={{width: '80pt'}}>Metrics</th>
      <th class="text-left" style={{width: '300pt'}}>Description</th>
      <th class="text-left" style={{width: '40pt'}}>Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="1"><strong>coordinator</strong></th>
      <td rowspan="1">request</td>
      <td>requestQueueSize</td>
      <td>The CoordinatorServer node network waiting queue size.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="6">tabletserver</th>
      <td rowspan="6">request</td>
      <td>requestQueueSize</td>
      <td>The TabletServer node network waiting queue size.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>requestPerSecond</td>
      <td>The total number of requests processed per second by the TabletServer node.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>errorsPerSecond</td>
      <td>The total number of error requests processed per second by the TabletServer node.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>totalTimeMs</td>
      <td>The total time it takes for the current TabletServer node to process a request.</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>requestProcessTimeMs</td>
      <td>The time the current TabletServer node spends to process a request.</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>requestQueueTimeMs</td>
      <td>The wait time spent by the request in the network waiting queue in this TabletServer node.</td>
      <td>Histogram</td>
    </tr>
     <tr>
      <th rowspan="6">client</th>
      <td rowspan="6">request</td>
      <td>bytesInPerSecond</td>
      <td>The data bytes return from another server per second.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>bytesOutPerSecond</td>
      <td>The data bytes send from client to another server per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>requestsPerSecond</td>
      <td>The requests count send from this client to another server per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>responsesPerSecond</td>
      <td>The responses count return from another server per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>requestLatencyMs</td>
      <td>The request latency.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>requestsInFlight</td>
      <td>The in flight requests count send from client to another server.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### Table/Bucket

<table class="table table-bordered">
  <thead>
    <tr>
     <th class="text-left" style={{width: '30pt'}}>Scope</th>
      <th class="text-left" style={{width: '150pt'}}>Infix</th>
      <th class="text-left" style={{width: '80pt'}}>Metrics</th>
      <th class="text-left" style={{width: '300pt'}}>Description</th>
      <th class="text-left" style={{width: '40pt'}}>Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="39"><strong>tabletserver</strong></th>
      <td rowspan="20">table</td>
      <td>messagesInPerSecond</td>
      <td>The number of messages written per second to this table</td>
      <td>Meter</td>
    </tr>
     <tr>
      <td>bytesInPerSecond</td>
      <td>The number of bytes written per second to this table.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>bytesOutPerSecond</td>
      <td>The number of bytes read per second from this table.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>totalProduceLogRequestsPerSecond</td>
      <td>The number of produce log requests to write log to this table per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>failedProduceLogRequestsPerSecond</td>
      <td>The number of failed produce log requests to write log to this table per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>totalFetchLogRequestsPerSecond</td>
      <td>The number of fetch log requests to read log from this table per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>failedFetchLogRequestsPerSecond</td>
      <td>The number of failed fetch log requests to read log from this table per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>totalPutKvRequestsPerSecond</td>
      <td>The number of put kv requests to put kv to this table per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>failedPutKvRequestsPerSecond</td>
      <td>The number of failed put kv requests to put kv to this table per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>totalLookupRequestsPerSecond</td>
      <td>The number of lookup requests to lookup value by key from this table per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>failedLookupRequestsPerSecond</td>
      <td>The number of failed lookup requests to lookup value by key from this table per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>totalLimitScanRequestsPerSecond</td>
      <td>The number of limit scan requests to scan records with limit from this table per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>failedLimitScanRequestsPerSecond</td>
      <td>The number of failed limit scan requests to scan records with limit from this table per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>totalPrefixLookupRequestsPerSecond</td>
      <td>The number of prefix lookup requests to lookup value by prefix key from this table per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>failedPrefixLookupRequestsPerSecond</td>
      <td>The number of failed prefix lookup requests to lookup value by prefix key from this table per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>remoteLogCopyBytesPerSecond</td>
      <td>The bytes of log data copied to remote per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>remoteLogCopyRequestsPerSecond</td>
      <td>The number of remote log copy requests to copy local log to remote per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>remoteLogCopyErrorPerSecond</td>
      <td>The number of error remote log copy requests to copy local log to remote per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>remoteLogDeleteRequestsPerSecond</td>
      <td>The number of delete remote log requests to delete remote log after log ttl per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>remoteLogDeleteErrorPerSecond</td>
      <td>The number of failed delete remote log requests to delete remote log after log ttl per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td rowspan="6">table_bucket</td>
      <td>inSyncReplicasCount</td>
      <td>The inSync replicas count of this table bucket.</td>
      <td>Gauge</td>
    </tr>
     <tr>
      <td>underMinIsr</td>
      <td>If this bucket is under min isr, this value is 1, otherwise 0.</td>
      <td>Gauge</td>
    </tr>
     <tr>
      <td>atMinIsr</td>
      <td>If this bucket is at min isr, this value is 1, otherwise 0.</td>
      <td>Gauge</td>
    </tr>
     <tr>
      <td>isrExpandsPerSecond</td>
      <td>The number of isr expands per second.</td>
      <td>Meter</td>
    </tr>
     <tr>
      <td>isrShrinksPerSecond</td>
      <td>The number of isr shrinks per second.</td>
      <td>Meter</td>
    </tr>
     <tr>
      <td>failedIsrUpdatesPerSecond</td>
      <td>The failed isr updates per second.</td>
      <td>Meter</td>
    </tr>
     <tr>
      <td rowspan="5">table_bucket_log</td>
      <td>numSegments</td>
      <td>The number of segments in local storage for this table bucket.</td>
      <td>Gauge</td>
    </tr>
     <tr>
      <td>endOffset</td>
      <td>The end offset in local storage for this table bucket.</td>
      <td>Gauge</td>
    </tr>
     <tr>
      <td>size</td>
      <td>The total log sizes in local storage for this table bucket.</td>
      <td>Gauge</td>
    </tr>
     <tr>
      <td>flushPerSecond</td>
      <td>The log flush count per second.</td>
      <td>Meter</td>
    </tr>
     <tr>
      <td>flushLatencyMs</td>
      <td>The log flush latency in ms.</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td rowspan="3">table_bucket_remoteLog</td>
      <td>numSegments</td>
      <td>The number of segments in remote storage for this table bucket.</td>
      <td>Gauge</td>
    </tr>
     <tr>
      <td>endOffset</td>
      <td>The end offset in remote storage for this table bucket.</td>
      <td>Gauge</td>
    </tr>
     <tr>
      <td>size</td>
      <td>The number of bytes written per second to this table.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="4">table_bucket_kv</td>
      <td>preWriteBufferFlushPerSecond</td>
      <td>The kv pre-write buffer flush count per second.</td>
      <td>Meter</td>
    </tr>
     <tr>
      <td>preWriteBufferFlushLatencyMs</td>
      <td>The kv pre-write buffer latency in ms.</td>
      <td>Histogram</td>
    </tr>
     <tr>
      <td>preWriteBufferTruncateAsDuplicatedPerSecond</td>
      <td>The number of kv pre-write buffer truncate due to the batch duplicated per second.</td>
      <td>Meter</td>
    </tr>
     <tr>
      <td>preWriteBufferTruncateAsErrorPerSecond</td>
      <td>The number of kv pre-write buffer truncate due to the error happened when writing cdc to log per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td rowspan="1">table_bucket_kv_snapshot</td>
      <td>latestSnapshotSize</td>
      <td>The latest kv snapshot size in bytes for this table bucket.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>


### Flink connector standard metrics

When using Flink to read and write, Fluss has implemented some key standard Flink connector metrics 
to measure the source latency and output of sink, see [FLIP-33: Standardize Connector Metrics](https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics). 
Flink source / sink metrics implemented are listed here.

How to use flink metrics, you can see [flink metrics](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/metrics/#system-metrics) for more details.

#### Source Metrics

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style={{width: '225pt'}}>Metrics Name</th>
      <th class="text-left" style={{width: '165pt'}}>Level</th>
      <th class="text-left" style={{width: '300pt'}}>Description</th>
      <th class="text-left" style={{width: '70pt'}}>Type</th>
    </tr>
    </thead>
    <tbody>
        <tr>
            <td>currentEmitEventTimeLag</td>
            <td>Flink Source Operator</td>
            <td>Time difference between sending the record out of source and file creation.</td>
            <td>Gauge</td>
        </tr>
        <tr>
            <td>currentFetchEventTimeLag</td>
            <td>Flink Source Operator</td>
            <td>Time difference between reading the data file and file creation.</td>
            <td>Gauge</td>
        </tr>
    </tbody>
</table>

#### Sink Metrics

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style={{width: '225pt'}}>Metrics Name</th>
      <th class="text-left" style={{width: '165pt'}}>Level</th>
      <th class="text-left" style={{width: '300pt'}}>Description</th>
      <th class="text-left" style={{width: '70pt'}}>Type</th>
    </tr>
    </thead>
    <tbody>
        <tr>
            <td>numBytesOut</td>
            <td>Table</td>
            <td>The total number of output bytes.</td>
            <td>Counter</td>
        </tr>
        <tr>
            <td>numBytesOutPerSecond</td>
            <td>Table</td>
            <td>The output bytes per second.</td>
            <td>Meter</td>
        </tr>
        <tr>
            <td>numRecordsOut</td>
            <td>Table</td>
            <td>The total number of output records.</td>
            <td>Counter</td>
        </tr>
        <tr>
            <td>numRecordsOutPerSecond</td>
            <td>Table</td>
            <td>The output records per second.</td>
            <td>Meter</td>
        </tr>  
    </tbody>
</table>

## Observability (Prometheus + Grafana)

We provide a minimal quickstart configuration for application observability with Prometheus and
Grafana [here](../assets/fluss-quickstart-observability.zip). The quickstart configuration comes with 2 dashboards.

- `Fluss – overview`: Selected metrics to observe the overall cluster status
- `Fluss – detail`: Majority of metrics listed in [metrics list](#metrics-list)


### Quickstart

Based on the [Flink quickstart guide](/docs/quickstart/flink), you can add observability capabilities as follows.

1. Download the [observability quickstart configuration](../assets/fluss-quickstart-observability.zip) and extract the ZIP archive in your working directory.
After extracting the archive, the contents of the working directory should be as follows.

```
├── docker-compose.yml              # docker compose manifest from quickstart guide
└── fluss-quickstart-observability  # downloaded and extracted ZIP archive
    ├── grafana
    │   ├── grafana.ini
    │   └── provisioning
    │       ├── dashboards
    │       │   ├── default.yml
    │       │   └── fluss
    │       │       └── ...
    │       └── datatsources
    │           └── default.yml
    └── prometheus
        └── prometheus.yml
```


2. Next, you need to adapt the `docker-compose.yml` manifest and

- add containers for Prometheus and Grafana and mount the corresponding configuration directories, and
- configure Fluss to expose metrics via Prometheus
```
metrics.reporters: prometheus
metrics.reporter.prometheus.port: 9250
```
- configure Flink to expose metrics via Prometheus
```
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9250
```

You can simply copy the manifest below into your `docker-compose.yml`

<!-- TODO: based on manifest in Flink quickstart guide + additions (see enumeration above) -->
```yaml
services:
  #begin Flink cluster
  coordinator-server:
    image: fluss/fluss:0.5.0
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        coordinator.host: coordinator-server
        remote.data.dir: /tmp/fluss/remote-data
        lakehouse.storage: paimon
        paimon.catalog.metastore: filesystem
        paimon.catalog.warehouse: /tmp/paimon
        metrics.reporters: prometheus
        metrics.reporter.prometheus.port: 9250
  tablet-server:
    image: fluss/fluss:0.5.0
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        tablet-server.host: tablet-server
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
        kv.snapshot.interval: 0s
        lakehouse.storage: paimon
        paimon.catalog.metastore: filesystem
        paimon.catalog.warehouse: /tmp/paimon
        metrics.reporters: prometheus
        metrics.reporter.prometheus.port: 9250
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  #end
  #begin Flink cluster
  jobmanager:
    image: fluss/quickstart-flink:1.20-0.5
    ports:
      - "8083:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
        metrics.reporter.prom.port: 9250
    volumes:
      - shared-tmpfs:/tmp/paimon
  taskmanager:
    image: fluss/quickstart-flink:1.20-0.5
    depends_on:
      - jobmanager
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 10
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.framework.off-heap.size: 256m
        metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
        metrics.reporter.prom.port: 9250
    volumes:
      - shared-tmpfs:/tmp/paimon
  #end
  #begin observability
  prometheus:
    image: bitnami/prometheus:2.55.1-debian-12-r0
    ports:
      - 9092:9090
    volumes:
      - ./fluss-quickstart-observability/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
  grafana:
    image:
      grafana/grafana:11.4.0
    ports:
      - 3002:3000
    depends_on:
      - prometheus
    volumes:
      - ./fluss-quickstart-observability/grafana:/etc/grafana:ro
  #end

volumes:
  shared-tmpfs:
    driver: local
    driver_opts:
      type: "tmpfs"
      device: "tmpfs"
```

and run

```shell
docker compose up -d
```

to apply the changes.

:::warning
This recreates `shared-tmpfs` and all data is lost (created tables, running jobs, etc.)
:::

Make sure that the Prometheus and Grafana container are up and running using

```shell
docker ps
```

3. Now you are all set! You can visit

- [Grafana](http://localhost:3002/dashboards) to observe the cluster status of the Fluss and Flink cluster with the provided dashboards, or
- the [Prometheus Web UI](http://localhost:9092) to directly query Prometheus with [PromQL](https://prometheus.io/docs/prometheus/2.55/getting_started/).