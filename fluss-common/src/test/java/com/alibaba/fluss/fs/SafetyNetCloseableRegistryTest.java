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

package com.alibaba.fluss.fs;

import com.alibaba.fluss.fs.local.LocalFileSystem;
import com.alibaba.fluss.testutils.common.CheckedThread;
import com.alibaba.fluss.utils.AbstractAutoCloseableRegistry;
import com.alibaba.fluss.utils.AbstractAutoCloseableRegistryTest;
import com.alibaba.fluss.utils.ExceptionUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the {@link SafetyNetCloseableRegistry}. */
public class SafetyNetCloseableRegistryTest
        extends AbstractAutoCloseableRegistryTest<
                Closeable,
                WrappingProxyCloseable<? extends Closeable>,
                SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef> {

    @Override
    protected void registerCloseable(final Closeable closeable) throws IOException {
        final WrappingProxyCloseable<Closeable> wrappingProxyCloseable =
                new WrappingProxyCloseable<Closeable>() {

                    @Override
                    public void close() throws IOException {
                        closeable.close();
                    }

                    @Override
                    public Closeable getWrappedDelegate() {
                        return closeable;
                    }
                };
        closeableRegistry.registerCloseable(wrappingProxyCloseable);
    }

    @Override
    protected AbstractAutoCloseableRegistry<
                    Closeable,
                    WrappingProxyCloseable<? extends Closeable>,
                    SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef,
                    IOException>
            createRegistry() {
        // SafetyNetCloseableRegistry has a global reaper thread to reclaim leaking resources,
        // in normal cases, that thread will be interrupted in closing of last active registry
        // and then shutdown in background. But in testing codes, some assertions need leaking
        // resources reclaimed, so we override reaper thread to join itself on interrupt. Thus,
        // after close of last active registry, we can assert post-close-invariants safely.
        return new SafetyNetCloseableRegistry(JoinOnInterruptReaperThread::new);
    }

    @Override
    protected AbstractAutoCloseableRegistryTest.ProducerThread<
                    Closeable,
                    WrappingProxyCloseable<? extends Closeable>,
                    SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef>
            createProducerThread(
                    AbstractAutoCloseableRegistry<
                                    Closeable,
                                    WrappingProxyCloseable<? extends Closeable>,
                                    SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef,
                                    IOException>
                            registry,
                    AtomicInteger unclosedCounter,
                    int maxStreams) {

        return new AbstractAutoCloseableRegistryTest.ProducerThread<
                Closeable,
                WrappingProxyCloseable<? extends Closeable>,
                SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef>(
                registry, unclosedCounter, maxStreams) {

            int count = 0;

            @Override
            protected void createAndRegisterStream() throws IOException {
                String debug = Thread.currentThread().getName() + " " + count;
                TestFSDataInputStream testStream = new TestFSDataInputStream(refCount);

                // this method automatically registers the stream with the given registry.
                @SuppressWarnings("unused")
                ClosingFSDataInputStream pis =
                        ClosingFSDataInputStream.wrapSafe(
                                testStream,
                                (SafetyNetCloseableRegistry) registry,
                                debug); // reference dies here
                ++count;
            }
        };
    }

    @AfterEach
    void tearDown() {
        assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isFalse();
    }

    @Test
    public void testCorrectScopesForSafetyNet(@TempDir Path tempDir) throws Exception {
        CheckedThread t1 =
                new CheckedThread() {

                    @Override
                    public void go() throws Exception {
                        try {
                            FileSystem fs1 = FileSystem.get(LocalFileSystem.getLocalFsURI());
                            // ensure no safety net in place
                            assertThat(fs1).isNotInstanceOf(SafetyNetWrapperFileSystem.class);
                            FileSystemSafetyNet.initializeSafetyNetForThread();
                            fs1 = FileSystem.get(LocalFileSystem.getLocalFsURI());
                            // ensure safety net is in place now
                            assertThat(fs1).isInstanceOf(SafetyNetWrapperFileSystem.class);

                            FsPath tmp = new FsPath(tempDir.toString(), "test_file");

                            try (FSDataOutputStream stream =
                                    fs1.create(tmp, FileSystem.WriteMode.NO_OVERWRITE)) {
                                CheckedThread t2 =
                                        new CheckedThread() {
                                            @Override
                                            public void go() {
                                                try {
                                                    FileSystem fs2 =
                                                            FileSystem.get(
                                                                    LocalFileSystem
                                                                            .getLocalFsURI());
                                                    // ensure the safety net does not leak here
                                                    assertThat(fs2)
                                                            .isNotInstanceOf(
                                                                    SafetyNetWrapperFileSystem
                                                                            .class);
                                                    FileSystemSafetyNet
                                                            .initializeSafetyNetForThread();
                                                    fs2 =
                                                            FileSystem.get(
                                                                    LocalFileSystem
                                                                            .getLocalFsURI());
                                                    // ensure we can bring another safety net in
                                                    // place
                                                    assertThat(fs2)
                                                            .isInstanceOf(
                                                                    SafetyNetWrapperFileSystem
                                                                            .class);
                                                    FileSystemSafetyNet
                                                            .closeSafetyNetAndGuardedResourcesForThread();
                                                    fs2 =
                                                            FileSystem.get(
                                                                    LocalFileSystem
                                                                            .getLocalFsURI());
                                                    // and that we can remove it again
                                                    assertThat(fs2)
                                                            .isNotInstanceOf(
                                                                    SafetyNetWrapperFileSystem
                                                                            .class);
                                                } catch (Exception e) {
                                                    fail(ExceptionUtils.stringifyException(e));
                                                }
                                            }
                                        };

                                t2.start();
                                t2.sync();

                                // ensure stream is still open and was never closed by any
                                // interferences
                                stream.write(42);
                                FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();

                                // ensure leaking stream was closed
                                try {
                                    stream.write(43);
                                    fail("stream should be closed.");
                                } catch (IOException ignore) {

                                }
                                fs1 = FileSystem.get(LocalFileSystem.getLocalFsURI());
                                // ensure safety net was removed
                                assertThat(fs1).isNotInstanceOf(SafetyNetWrapperFileSystem.class);
                            } finally {
                                fs1.delete(tmp, false);
                            }
                        } catch (Exception e) {
                            fail(ExceptionUtils.stringifyException(e));
                        }
                    }
                };

        t1.start();
        t1.sync();
    }

    @Test
    void testSafetyNetClose() throws Exception {
        setup(20);
        startThreads();

        joinThreads();

        for (int i = 0; i < 5 && unclosedCounter.get() > 0; ++i) {
            System.gc();
            Thread.sleep(50);
        }

        Assertions.assertThat(unclosedCounter.get()).isEqualTo(0);
        closeableRegistry.close();
    }

    @Test
    void testReaperThreadSpawnAndStop() throws Exception {
        assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isFalse();

        try (SafetyNetCloseableRegistry ignored = new SafetyNetCloseableRegistry()) {
            assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isTrue();

            try (SafetyNetCloseableRegistry ignored2 = new SafetyNetCloseableRegistry()) {
                assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isTrue();
            }
            assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isTrue();
        }
        assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isFalse();
    }

    /**
     * Test whether failure to start thread in {@link SafetyNetCloseableRegistry} constructor can
     * lead to failure of subsequent state check.
     */
    @Test
    void testReaperThreadStartFailed() throws Exception {

        try {
            new SafetyNetCloseableRegistry(OutOfMemoryReaperThread::new);
        } catch (OutOfMemoryError ignored) {
        }
        assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isFalse();

        // the OOM error will not lead to failure of subsequent constructor call.
        SafetyNetCloseableRegistry closeableRegistry = new SafetyNetCloseableRegistry();
        assertThat(SafetyNetCloseableRegistry.isReaperThreadRunning()).isTrue();

        closeableRegistry.close();
    }

    private static class JoinOnInterruptReaperThread
            extends SafetyNetCloseableRegistry.CloseableReaperThread {
        @Override
        public void interrupt() {
            super.interrupt();
            try {
                join();
            } catch (InterruptedException ex) {
                currentThread().interrupt();
            }
        }
    }

    private static class OutOfMemoryReaperThread
            extends SafetyNetCloseableRegistry.CloseableReaperThread {

        @Override
        public synchronized void start() {
            throw new OutOfMemoryError();
        }
    }

    /** Testing stream which adds itself to a reference counter while not closed. */
    protected static final class TestFSDataInputStream extends FSDataInputStream {

        protected AtomicInteger refCount;

        public TestFSDataInputStream(AtomicInteger refCount) {
            this.refCount = refCount;
            refCount.incrementAndGet();
        }

        @Override
        public int read() throws IOException {
            return 0;
        }

        @Override
        public synchronized void close() throws IOException {
            refCount.decrementAndGet();
        }

        @Override
        public void seek(long desired) throws IOException {}

        @Override
        public long getPos() throws IOException {
            return 0;
        }
    }
}
