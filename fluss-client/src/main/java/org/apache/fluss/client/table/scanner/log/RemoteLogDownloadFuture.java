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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.record.FileLogRecords;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** Represents the future of a remote log download request. */
public class RemoteLogDownloadFuture {

    private final CompletableFuture<File> logFileFuture;
    private final Runnable recycleCallback;

    public RemoteLogDownloadFuture(
            CompletableFuture<File> logFileFuture, Runnable recycleCallback) {
        this.logFileFuture = logFileFuture;
        this.recycleCallback = recycleCallback;
    }

    public boolean isDone() {
        return logFileFuture.isDone();
    }

    public FileLogRecords getFileLogRecords(int startPosition) {
        try {
            FileLogRecords fileLogRecords = FileLogRecords.open(logFileFuture.join(), false);
            if (startPosition > 0) {
                return fileLogRecords.slice(startPosition, Integer.MAX_VALUE);
            } else {
                return fileLogRecords;
            }
        } catch (IOException e) {
            throw new FlussRuntimeException(e);
        }
    }

    public Runnable getRecycleCallback() {
        return recycleCallback;
    }

    public void onComplete(Runnable callback) {
        logFileFuture.thenRun(callback);
    }
}
