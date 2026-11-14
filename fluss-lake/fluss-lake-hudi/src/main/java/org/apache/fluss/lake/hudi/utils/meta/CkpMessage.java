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

package org.apache.fluss.lake.hudi.utils.meta;

import org.apache.hadoop.fs.FileStatus;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkState;

/** A checkpoint message used to coordinate Hudi instant initialization across tiering writers. */
public class CkpMessage implements Serializable, Comparable<CkpMessage> {

    private static final long serialVersionUID = 1L;

    public static final Comparator<CkpMessage> COMPARATOR =
            Comparator.comparing(CkpMessage::getInstant).thenComparing(CkpMessage::getState);

    private final String instant;
    private final State state;

    public CkpMessage(String instant, String state) {
        this.instant = instant;
        this.state = State.valueOf(state);
    }

    public CkpMessage(FileStatus fileStatus) {
        String fileName = fileStatus.getPath().getName();
        String[] nameAndExt = fileName.split("\\.");
        checkState(nameAndExt.length == 2, "Invalid checkpoint metadata file: %s", fileName);
        this.instant = nameAndExt[0];
        this.state = State.valueOf(nameAndExt[1]);
    }

    public String getInstant() {
        return instant;
    }

    public State getState() {
        return state;
    }

    public boolean isComplete() {
        return State.COMPLETED == state;
    }

    public boolean isInflight() {
        return State.INFLIGHT == state;
    }

    public static String getFileName(String instant, State state) {
        return instant + "." + state.name();
    }

    public static List<String> getAllFileNames(String instant) {
        List<String> fileNames = new ArrayList<>();
        for (State state : State.values()) {
            fileNames.add(getFileName(instant, state));
        }
        return fileNames;
    }

    @Override
    public int compareTo(CkpMessage other) {
        return COMPARATOR.compare(this, other);
    }

    /** Hudi instant state tracked by checkpoint metadata. */
    public enum State {
        INFLIGHT,
        ABORTED,
        COMPLETED
    }

    @Override
    public String toString() {
        return "CkpMessage{" + "instant='" + instant + '\'' + ", state=" + state + '}';
    }
}
