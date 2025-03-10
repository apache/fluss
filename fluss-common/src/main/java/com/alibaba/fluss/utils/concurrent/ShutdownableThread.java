/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.utils.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/** An abstract thread that is shutdownable . */
public abstract class ShutdownableThread extends Thread {

    protected final Logger log;

    private final boolean isInterruptible;

    private final CountDownLatch shutdownInitiated = new CountDownLatch(1);
    private final CountDownLatch shutdownComplete = new CountDownLatch(1);

    private volatile boolean isStarted = false;

    public ShutdownableThread(String name) {
        this(name, true);
    }

    public ShutdownableThread(String name, boolean isInterruptible) {
        super(name);
        this.isInterruptible = isInterruptible;
        log = LoggerFactory.getLogger(this.getClass());
        this.setDaemon(false);
    }

    public void shutdown() throws InterruptedException {
        initiateShutdown();
        awaitShutdown();
    }

    public boolean isShutdownInitiated() {
        return shutdownInitiated.getCount() == 0;
    }

    public boolean initiateShutdown() {
        synchronized (this) {
            if (isRunning()) {
                log.info("Shutting down");
                shutdownInitiated.countDown();
                if (isInterruptible) {
                    interrupt();
                }
                return true;
            } else {
                return false;
            }
        }
    }

    /** After calling initiateShutdown(), use this API to wait until the shutdown is complete. */
    public void awaitShutdown() throws InterruptedException {
        if (!isShutdownInitiated()) {
            throw new IllegalStateException(
                    "initiateShutdown() was not called before awaitShutdown()");
        } else {
            if (isStarted) {
                shutdownComplete.await();
            }
            log.info("Shutdown completed");
        }
    }

    /**
     * This method is repeatedly invoked until the thread shuts down or this method throws an
     * exception.
     */
    public abstract void doWork() throws Exception;

    public void run() {
        isStarted = true;
        log.info("Starting");
        try {
            while (isRunning()) {
                doWork();
            }
        } catch (Error e) {
            shutdownInitiated.countDown();
            shutdownComplete.countDown();
            log.info("Stopped");
            System.exit(-1);
        } catch (Throwable e) {
            if (isRunning()) {
                log.error("Error due to", e);
            }
        } finally {
            shutdownComplete.countDown();
        }
        log.info("Stopped");
    }

    public boolean isRunning() {
        return !isShutdownInitiated();
    }
}
