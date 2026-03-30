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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

/**
 * A callback interface that the user can implement to allow code to execute when the write request
 * is complete. This callback will generally execute in the background I/O thread, so it should be
 * fast.
 */
@Internal
public interface WriteCallback {

    /**
     * Called when the write operation completes.
     *
     * @param bucket the bucket this record was written to, or null if not available (e.g., log
     *     tables)
     * @param logEndOffset the log end offset (LEO) after this write, i.e., the offset of the next
     *     record to be written, or -1 if not available
     * @param exception the exception if the write failed, or null if successful
     */
    void onCompletion(
            @Nullable TableBucket bucket, long logEndOffset, @Nullable Exception exception);
}
