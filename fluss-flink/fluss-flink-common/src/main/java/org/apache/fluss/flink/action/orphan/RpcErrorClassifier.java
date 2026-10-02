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

package org.apache.fluss.flink.action.orphan;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Classifies RPC exceptions raised during scope enumeration and per-target active-set fetch into a
 * small, audit-stable vocabulary. The category name is what surfaces as the {@code reason=} field
 * of {@code skip_log_target} / {@code skip_kv_target} audit events, so operators triage by exact
 * string and the enum must not be widened lightly.
 *
 * <ul>
 *   <li>{@link Category#NOT_FOUND} — legitimate "object does not exist"; the enumerator treats it
 *       as the target having disappeared concurrently and silently skips it without alarm.
 *   <li>{@link Category#TRANSIENT} — IO / timeout / ZK connection loss; the target is skipped this
 *       round and naturally retried in the next cleanup round.
 *   <li>{@link Category#SERVER_ERROR} — server-side failure; same skip, but audited at higher
 *       severity so an operator can investigate.
 *   <li>{@link Category#UNKNOWN} — anything not matched above; conservatively skipped + audited.
 * </ul>
 */
@Internal
public final class RpcErrorClassifier {

    private RpcErrorClassifier() {}

    /** Categories of RPC errors. */
    public enum Category {
        NOT_FOUND,
        TRANSIENT,
        SERVER_ERROR,
        UNKNOWN
    }

    /**
     * Classifies a thrown exception. Unwraps {@link CompletionException}/{@link
     * ExecutionException}.
     */
    public static Category classify(Throwable t) {
        Throwable cause = unwrap(t);
        if (cause instanceof TableNotExistException
                || cause instanceof PartitionNotExistException) {
            return Category.NOT_FOUND;
        }
        if (cause instanceof IOException || cause instanceof TimeoutException) {
            return Category.TRANSIENT;
        }
        if (cause instanceof FlussRuntimeException) {
            return Category.SERVER_ERROR;
        }
        return Category.UNKNOWN;
    }

    private static Throwable unwrap(Throwable t) {
        while (t instanceof CompletionException || t instanceof ExecutionException) {
            if (t.getCause() == null) {
                return t;
            }
            t = t.getCause();
        }
        return t;
    }
}
