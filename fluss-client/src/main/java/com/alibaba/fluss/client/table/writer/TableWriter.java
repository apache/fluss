/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.client.table.writer;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.config.ConfigOptions;

/**
 * A writer to write data to a Fluss table.
 *
 * <p>A base interface for {@link AppendWriter} and {@link UpsertWriter} to write data to table.
 *
 * @since 0.1
 */
@PublicEvolving
public interface TableWriter {

    /**
     * Flush data written that have not yet been sent to the server, forcing the client to send the
     * requests to server and blocks on the completion of the requests associated with these
     * records. A request is considered completed when it is successfully acknowledged according to
     * the {@link ConfigOptions#CLIENT_WRITER_ACKS} configuration you have specified or else it
     * results in an error.
     */
    void flush();
}
