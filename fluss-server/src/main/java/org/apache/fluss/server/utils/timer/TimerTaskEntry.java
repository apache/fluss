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

package org.apache.fluss.server.utils.timer;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** The entry to describe timer task and its location in link list. */
public class TimerTaskEntry implements Comparable<TimerTaskEntry> {
    private final TimerTask timerTask;
    private final long expirationMs;

    volatile TimerTaskList list;
    TimerTaskEntry next;
    TimerTaskEntry prev;

    TimerTaskEntry(TimerTask timerTask, long expirationMs) {
        this.timerTask = timerTask;
        this.expirationMs = expirationMs;
        this.next = null;
        this.prev = null;
        this.list = null;

        // if this timerTask is already held by an existing timer task entry, setTimerTaskEntry
        // will remove it.
        if (timerTask != null) {
            timerTask.setTimerTaskEntry(this);
        }
    }

    TimerTask getTimerTask() {
        return timerTask;
    }

    long getExpirationMs() {
        return expirationMs;
    }

    boolean isCancelled() {
        return timerTask.getTimerTaskEntry() != this;
    }

    void remove() {
        TimerTaskList currentList = list;
        // If remove is called when another thread is moving the entry from a task
        // entry list to another, this may fail to remove the entry due to the change of value
        // of list. Thus, we retry  until the list becomes null.  In a rare case, this thread
        // sees null and exits the loop, but the other thread insert the entry to another list
        // later.
        while (list != null) {
            currentList.remove(this);
            currentList = list;
        }
    }

    @Override
    public int compareTo(TimerTaskEntry that) {
        return Long.compare(this.expirationMs, that.expirationMs);
    }
}
