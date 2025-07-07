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

/**
 * Interface representing a timer service capable of scheduling tasks to be executed after a
 * specified delay.
 *
 * <p>A Timer provides a facility for threads to schedule tasks for future execution in a background
 * thread. Tasks may be scheduled for one-time execution or for repeated execution at regular
 * intervals.
 */
public interface Timer {

    /**
     * Add a new task to this executor. It will be executed after the task's delay (beginning from
     * the time of submission).
     *
     * @param timerTask the task to add
     */
    void add(TimerTask timerTask);

    /**
     * Advance the internal clock, executing any tasks whose expiration has been reached within the
     * duration of the passed wait time.
     *
     * @param waitMs the time in milliseconds to wait for tasks to be executed
     * @return whether ot not any tasks were executed
     */
    boolean advanceClock(long waitMs) throws InterruptedException;

    /**
     * Get the number of tasks pending execution.
     *
     * @return the number of tasks.
     */
    int numOfTimerTasks();

    /** Shutdown the timer service. leaving pending tasks un-executed. */
    void shutdown();
}
