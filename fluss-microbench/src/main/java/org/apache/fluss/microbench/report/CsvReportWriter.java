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

package org.apache.fluss.microbench.report;

import org.apache.fluss.microbench.sampling.ProcessSnapshot;
import org.apache.fluss.microbench.sampling.ResourceSnapshot;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.fluss.microbench.report.ReportFormatUtils.formatOneDecimal;
import static org.apache.fluss.microbench.report.ReportFormatUtils.toMB;
import static org.apache.fluss.microbench.report.ReportFormatUtils.toMBOrNotAvailable;

/** Writes the time-series section of a {@link PerfReport} as a CSV file. */
class CsvReportWriter {

    private CsvReportWriter() {}

    static void write(PerfReport report, Path outputDir) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "timestamp,"
                        + "clientRssMB,clientVszMB,clientCpuPercent,clientThreadCount,"
                        + "clientDiskReadBytes,clientDiskWriteBytes,"
                        + "clientDiskReadBytesPerSec,clientDiskWriteBytesPerSec,"
                        + "clientOpenFiles,clientCtxSwitches,clientMinorFaults,"
                        + "clientHeapMB,clientHeapMaxMB,clientNonHeapMB,clientDirectMB,"
                        + "clientJvmCpuPercent,clientGcCount,clientGcTimeMs,"
                        + "clientBytesSendTotal,clientRecordsSendTotal,"
                        + "clientSendLatencyMs,clientBatchQueueTimeMs,"
                        + "clientRecordsRetryTotal,clientBytesPerBatchMean,clientRecordsPerBatchMean,"
                        + "clientRpcRequestRate,clientRpcResponseRate,"
                        + "clientRpcBytesInRate,clientRpcBytesOutRate,"
                        + "clientRpcLatencyAvg,clientRpcLatencyMax,clientRpcInflight,"
                        + "serverRssMB,serverVszMB,serverCpuPercent,serverThreadCount,"
                        + "serverDiskReadBytes,serverDiskWriteBytes,"
                        + "serverDiskReadBytesPerSec,serverDiskWriteBytesPerSec,"
                        + "serverOpenFiles,serverCtxSwitches,serverMinorFaults,"
                        + "serverNmtHeapMB,serverNmtClassMB,serverNmtThreadStacksMB,"
                        + "serverNmtCodeCacheMB,serverNmtGcMB,serverNmtInternalMB,"
                        + "serverRocksdbMemtableMB,serverRocksdbBlockCacheMB,"
                        + "serverRocksdbTableReadersMB,"
                        + "serverNettyDirectMB,serverNativeUntrackedMB");
        sb.append('\n');

        for (ResourceSnapshot snap : report.snapshots()) {
            ProcessSnapshot client = snap.client;
            ProcessSnapshot server = snap.server;

            sb.append(snap.timestamp).append(',');
            sb.append(toMB(client != null ? client.rssBytes : 0)).append(',');
            sb.append(toMB(client != null ? client.vszBytes : 0)).append(',');
            sb.append(formatOneDecimal(client != null ? client.cpuPercent : 0)).append(',');
            sb.append(client != null ? client.threadCount : 0).append(',');
            sb.append(client != null ? client.diskReadBytes : 0).append(',');
            sb.append(client != null ? client.diskWriteBytes : 0).append(',');
            sb.append(snap.clientDiskReadBytesPerSec).append(',');
            sb.append(snap.clientDiskWriteBytesPerSec).append(',');
            sb.append(client != null ? client.openFileCount : 0).append(',');
            sb.append(client != null ? client.contextSwitches : 0).append(',');
            sb.append(client != null ? client.minorFaults : 0).append(',');
            sb.append(toMB(snap.clientHeapUsed)).append(',');
            sb.append(toMB(snap.clientHeapMax)).append(',');
            sb.append(toMB(snap.clientNonHeapUsed)).append(',');
            sb.append(toMB(snap.clientDirectMemoryUsed)).append(',');
            sb.append(formatOneDecimal(snap.clientJvmCpuLoad)).append(',');
            sb.append(snap.clientGcCount).append(',');
            sb.append(snap.clientGcTimeMs).append(',');
            sb.append(snap.clientBytesSendTotal).append(',');
            sb.append(snap.clientRecordsSendTotal).append(',');
            sb.append(formatOneDecimal(snap.clientSendLatencyMs)).append(',');
            sb.append(formatOneDecimal(snap.clientBatchQueueTimeMs)).append(',');
            sb.append(snap.clientRecordsRetryTotal).append(',');
            sb.append(formatOneDecimal(snap.clientBytesPerBatchMean)).append(',');
            sb.append(formatOneDecimal(snap.clientRecordsPerBatchMean)).append(',');
            sb.append(formatOneDecimal(snap.clientRpcRequestRate)).append(',');
            sb.append(formatOneDecimal(snap.clientRpcResponseRate)).append(',');
            sb.append(formatOneDecimal(snap.clientRpcBytesInRate)).append(',');
            sb.append(formatOneDecimal(snap.clientRpcBytesOutRate)).append(',');
            sb.append(formatOneDecimal(snap.clientRpcRequestLatencyAvg)).append(',');
            sb.append(formatOneDecimal(snap.clientRpcRequestLatencyMax)).append(',');
            sb.append(formatOneDecimal(snap.clientRpcInflight)).append(',');
            sb.append(toMB(server != null ? server.rssBytes : 0)).append(',');
            sb.append(toMB(server != null ? server.vszBytes : 0)).append(',');
            sb.append(formatOneDecimal(server != null ? server.cpuPercent : 0)).append(',');
            sb.append(server != null ? server.threadCount : 0).append(',');
            sb.append(server != null ? server.diskReadBytes : 0).append(',');
            sb.append(server != null ? server.diskWriteBytes : 0).append(',');
            sb.append(snap.serverDiskReadBytesPerSec).append(',');
            sb.append(snap.serverDiskWriteBytesPerSec).append(',');
            sb.append(server != null ? server.openFileCount : 0).append(',');
            sb.append(server != null ? server.contextSwitches : 0).append(',');
            sb.append(server != null ? server.minorFaults : 0).append(',');
            sb.append(toMBOrNotAvailable(snap.serverNmtHeap)).append(',');
            sb.append(toMBOrNotAvailable(snap.serverNmtClass)).append(',');
            sb.append(toMBOrNotAvailable(snap.serverNmtThreadStacks)).append(',');
            sb.append(toMBOrNotAvailable(snap.serverNmtCodeCache)).append(',');
            sb.append(toMBOrNotAvailable(snap.serverNmtGc)).append(',');
            sb.append(toMBOrNotAvailable(snap.serverNmtInternal)).append(',');
            sb.append(toMB(snap.serverRocksdbMemtable)).append(',');
            sb.append(toMB(snap.serverRocksdbBlockCache)).append(',');
            sb.append(toMB(snap.serverRocksdbTableReaders)).append(',');
            sb.append(toMB(snap.serverNettyDirectMemory)).append(',');
            sb.append(toMB(snap.serverNativeUntracked));
            sb.append('\n');
        }

        Files.write(
                outputDir.resolve("timeseries.csv"),
                sb.toString().getBytes(StandardCharsets.UTF_8));
    }
}
