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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.RemoteStorageException;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.exception.CorruptIndexException;
import org.apache.fluss.server.log.OffsetIndex;
import org.apache.fluss.server.log.OffsetPosition;
import org.apache.fluss.server.log.StorageAction;
import org.apache.fluss.server.log.TimeIndex;
import org.apache.fluss.server.log.TimestampOffset;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.fluss.utils.FlussPaths.INDEX_FILE_SUFFIX;
import static org.apache.fluss.utils.FlussPaths.TIME_INDEX_FILE_SUFFIX;
import static org.apache.fluss.utils.FlussPaths.offsetFromRemoteIndexCacheFileName;
import static org.apache.fluss.utils.FlussPaths.remoteLogIndexCacheDir;
import static org.apache.fluss.utils.FlussPaths.remoteOffsetIndexCacheFile;
import static org.apache.fluss.utils.FlussPaths.remoteTimeIndexCacheFile;
import static org.apache.fluss.utils.FlussPaths.uuidFromRemoteIndexCacheFileName;
import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/**
 * This is a LFU (Least Frequently Used) cache which cache remote log index files of this
 * tabletServer, The remote log index files will be downloaded and storage in dir
 * `$logDir/remote-log-index-cache`, and the index files are named as
 * `$startOffset-$segmentId-.index`. The startOffset is the start offset of this segment and the
 * segmentId is the remote segment id (UUID) of this segment.
 *
 * <p>This is helpful to avoid re-fetching the index files like offset indexes from the remote log
 * storage for every fetch call. The cache is re-initialized from the index files on disk on
 * startup, if the index files are available.
 *
 * <p>Note that closing this cache does not delete the index files on disk. Note that the cache
 * eviction policy is based on the default implementation of Caffeine i.e. <a href="<a
 * href="https://github.com/ben-manes/caffeine/wiki/Efficiency">Window TinyLfu</a>. TinyLfu relies
 * on a frequency sketch to probabilistically estimate the historic usage of an entry.
 *
 * <p>This class is thread safe.
 */
@ThreadSafe
public class RemoteLogIndexCache implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteLogIndexCache.class);

    private static final String TMP_FILE_SUFFIX = ".tmp";
    public static final String REMOTE_LOG_INDEX_CACHE_CLEANER_THREAD =
            "remote-log-index-cache-cleaner";
    public static final String DIR_NAME = "remote-log-index-cache";

    /** Directory where the index files will be stored on disk. */
    private final File cacheDir;

    /** Represents if the cache is closed or not. Closing the cache is an irreversible operation. */
    private final AtomicBoolean isRemoteIndexCacheClosed = new AtomicBoolean(false);

    /**
     * Unbounded queue containing the removed entries from the cache which are waiting to be garbage
     * collected.
     */
    private final LinkedBlockingQueue<Entry> expiredIndexes = new LinkedBlockingQueue<>();

    /**
     * Lock used to synchronize close with other read operations. This ensures that when we close,
     * we don't have any other concurrent reads in-progress.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final RemoteLogStorage remoteLogStorage;
    private final ShutdownableThread cleanerThread;

    /**
     * Actual cache implementation that this file wraps around.
     *
     * <p>The requirements for this internal cache is as follows:
     *
     * <pre>
     *     1. Multiple threads should be able to read concurrently.
     *     2. Fetch for missing keys should not block read for available keys.
     *     3. Only one thread should fetch for a specific key.
     *     4. Should support LRU-like policy.
     * </pre>
     *
     * <p>We use {@link Caffeine} cache instead of implementing a thread safe LRU cache on our own.
     */
    private final Cache<UUID, Entry> internalCache;

    public RemoteLogIndexCache(long maxSize, RemoteLogStorage remoteLogStorage, File dataDir)
            throws IOException {
        this.remoteLogStorage = remoteLogStorage;
        this.cacheDir = remoteLogIndexCacheDir(dataDir);
        this.internalCache = initEmptyCache(maxSize);

        // init remote index cache by reading the index files.
        init();

        // Start cleaner thread that will clean the expired entries.
        cleanerThread = createCleanerThread();
        cleanerThread.start();
    }

    private ShutdownableThread createCleanerThread() {
        ShutdownableThread thread =
                new ShutdownableThread(REMOTE_LOG_INDEX_CACHE_CLEANER_THREAD) {
                    public void doWork() {
                        try {
                            Entry entry = expiredIndexes.take();
                            LOG.debug("Cleaning up index entry {}", entry);
                            entry.cleanup();
                        } catch (InterruptedException ie) {
                            // cleaner thread should only be interrupted when cache is being closed,
                            // else it's an error.
                            if (!isRemoteIndexCacheClosed.get()) {
                                LOG.error(
                                        "Cleaner thread received interruption but remote index cache is not closed",
                                        ie);
                                // propagate the InterruptedException outside to correctly close the
                                // thread.
                                throw new FlussRuntimeException(ie);
                            } else {
                                LOG.debug("Cleaner thread was interrupted on cache shutdown");
                            }
                        } catch (Exception ex) {
                            // do not exit for exceptions other than InterruptedException
                            LOG.error("Error occurred while cleaning up expired entry", ex);
                        }
                    }
                };
        thread.setDaemon(true);
        return thread;
    }

    /** Get the position of the given offset in the remote log segment. */
    public int lookupPosition(RemoteLogSegment remoteLogSegment, long offset) {
        return inReadLock(
                lock, () -> getIndexEntry(remoteLogSegment).lookupOffset(offset).getPosition());
    }

    /**
     * Get the base offset of the given timestamp and startingOffset in the remote log segment. If
     * the timestamp lower than the smallest commit timestamp in this segment, return the
     * remoteLogStartOffset.
     */
    public long lookupOffsetForTimestamp(RemoteLogSegment remoteLogSegment, long timestamp) {
        return inReadLock(
                lock, () -> getIndexEntry(remoteLogSegment).lookupTimestamp(timestamp).getOffset());
    }

    Entry getIndexEntry(RemoteLogSegment remoteLogSegment) {
        if (isRemoteIndexCacheClosed.get()) {
            throw new IllegalStateException(
                    "Unable to fetch index for "
                            + "remote-segment-id = "
                            + remoteLogSegment.remoteLogSegmentId()
                            + ". Instance is already closed.");
        }

        return inReadLock(
                lock,
                () -> {
                    // while this thread was waiting for lock, another thread may have changed the
                    // value of isRemoteIndexCacheClosed. Check for index close again.
                    if (isRemoteIndexCacheClosed.get()) {
                        throw new IllegalStateException(
                                "Unable to fetch index for remote-segment-id = "
                                        + remoteLogSegment.remoteLogSegmentId()
                                        + ". Index instance is already closed.");
                    }
                    return internalCache.get(
                            remoteLogSegment.remoteLogSegmentId(),
                            id -> createCacheEntry(remoteLogSegment));
                });
    }

    private Entry createCacheEntry(RemoteLogSegment remoteLogSegment) {
        long startOffset = remoteLogSegment.remoteLogStartOffset();
        try {
            File offsetIndexFile = remoteOffsetIndexCacheFile(cacheDir, remoteLogSegment);
            OffsetIndex offsetIndex =
                    loadIndexFile(
                            offsetIndexFile,
                            remoteLogSegment,
                            metadata -> {
                                try {
                                    return remoteLogStorage.fetchIndex(
                                            remoteLogSegment, RemoteLogStorage.IndexType.OFFSET);
                                } catch (RemoteStorageException e) {
                                    throw new FlussRuntimeException(e);
                                }
                            },
                            file -> {
                                try {
                                    OffsetIndex index =
                                            new OffsetIndex(
                                                    file, startOffset, Integer.MAX_VALUE, false);
                                    index.sanityCheck();
                                    return index;
                                } catch (IOException e) {
                                    throw new FlussRuntimeException(e);
                                }
                            });
            File timeIndexFile = remoteTimeIndexCacheFile(cacheDir, remoteLogSegment);
            TimeIndex timeIndex =
                    loadIndexFile(
                            timeIndexFile,
                            remoteLogSegment,
                            metadata -> {
                                try {
                                    return remoteLogStorage.fetchIndex(
                                            remoteLogSegment, RemoteLogStorage.IndexType.TIMESTAMP);
                                } catch (RemoteStorageException e) {
                                    throw new FlussRuntimeException(e);
                                }
                            },
                            file -> {
                                try {
                                    TimeIndex index =
                                            new TimeIndex(
                                                    file, startOffset, Integer.MAX_VALUE, false);
                                    index.sanityCheck();
                                    return index;
                                } catch (IOException e) {
                                    throw new FlussRuntimeException(e);
                                }
                            });
            return new Entry(offsetIndex, timeIndex);
        } catch (IOException e) {
            throw new FlussRuntimeException(e);
        }
    }

    public void remove(UUID remoteSegmentId) {
        inReadLock(
                lock,
                () -> {
                    internalCache
                            .asMap()
                            .computeIfPresent(
                                    remoteSegmentId,
                                    (k, v) -> {
                                        deleteEntry(k, v);
                                        // Returning null to remove the key from the cache.
                                        return null;
                                    });
                });
    }

    public void removeAll(List<UUID> remoteSegmentIds) {
        inReadLock(
                lock,
                () ->
                        remoteSegmentIds.forEach(
                                id ->
                                        internalCache
                                                .asMap()
                                                .computeIfPresent(
                                                        id,
                                                        (k, v) -> {
                                                            deleteEntry(k, v);
                                                            // Returning null to remove the key from
                                                            // the cache.
                                                            return null;
                                                        })));
    }

    private void deleteEntry(UUID remoteSegmentId, Entry entry) {
        try {
            entry.cleanup();
        } catch (IOException e) {
            LOG.error("Failed to delete entry for remote segment id {}.", remoteSegmentId, e);
        }
    }

    private Cache<UUID, Entry> initEmptyCache(long maxSize) {
        return Caffeine.newBuilder()
                .maximumWeight(maxSize)
                .weigher((UUID key, Entry entry) -> (int) entry.entrySizeBytes)
                // This listener is invoked each time an entry is being automatically removed due to
                // eviction. The cache will invoke this listener during the atomic operation to
                // remove the entry (refer: https://github.com/ben-manes/caffeine/wiki/Removal),
                // hence, care must be taken to ensure that this operation is not expensive. Note
                // that this listener is not invoked when RemovalCause from cache is EXPLICIT or
                // REPLACED (e.g. on Cache.invalidate(), Cache.put() etc.) For a complete list see:
                // https://github.com/ben-manes/caffeine/blob/0cef55168986e3816314e7fdba64cb0b996dd3cc/caffeine/src/main/java/com/github/benmanes/caffeine/cache/RemovalCause.java#L23
                // Hence, any operation required after removal from cache must be performed manually
                // for these scenarios.
                .evictionListener(
                        (UUID key, Entry entry, RemovalCause cause) -> {
                            // Mark the entries for cleanup and add them to the queue to be garbage
                            // collected later by the background thread.
                            if (entry != null) {
                                enqueueEntryForCleanup(entry, key);
                            } else {
                                LOG.error(
                                        "Received entry as null for key {} when it is removed from the cache.",
                                        key);
                            }
                        })
                .build();
    }

    private void init() throws IOException {
        long start = System.currentTimeMillis();

        try {
            Files.createDirectory(cacheDir.toPath());
            LOG.info("Created new file {} for RemoteIndexCache", cacheDir);
        } catch (FileAlreadyExistsException e) {
            LOG.info(
                    "RemoteIndexCache directory {} already exists. Re-using the same directory.",
                    cacheDir);
        } catch (Exception e) {
            LOG.error("Unable to create directory {} for RemoteIndexCache.", cacheDir, e);
            throw new FlussRuntimeException(e);
        }

        // Delete any .tmp files remained from the earlier run of the tablet server.
        try (Stream<Path> paths = Files.list(cacheDir.toPath())) {
            paths.forEach(
                    path -> {
                        if (path.endsWith(TMP_FILE_SUFFIX)) {
                            try {
                                if (Files.deleteIfExists(path)) {
                                    LOG.debug("Deleted file path {} on cache initialization", path);
                                }
                            } catch (IOException e) {
                                throw new FlussRuntimeException(e);
                            }
                        }
                    });
        }

        try (Stream<Path> paths = Files.list(cacheDir.toPath())) {
            Iterator<Path> iterator = paths.iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next();
                Path fileNamePath = path.getFileName();
                if (fileNamePath == null) {
                    throw new FlussRuntimeException(
                            "Empty file name in remote index cache directory: " + cacheDir);
                }

                String indexFileName = fileNamePath.toString();
                UUID remoteSegmentId = uuidFromRemoteIndexCacheFileName(indexFileName);

                // It is safe to update the internalCache non-atomically here since this function is
                // always called by a single thread only.
                if (!internalCache.asMap().containsKey(remoteSegmentId)) {
                    String fileNameWithoutSuffix =
                            indexFileName.substring(0, indexFileName.indexOf("."));
                    File offsetIndexFile =
                            new File(cacheDir, fileNameWithoutSuffix + INDEX_FILE_SUFFIX);
                    File timestampIndexFile =
                            new File(cacheDir, fileNameWithoutSuffix + TIME_INDEX_FILE_SUFFIX);

                    // Create entries for each path if all the index files exist.
                    if (Files.exists(offsetIndexFile.toPath())
                            && Files.exists(timestampIndexFile.toPath())) {
                        long offset = offsetFromRemoteIndexCacheFileName(indexFileName);
                        try {
                            OffsetIndex offsetIndex =
                                    new OffsetIndex(
                                            offsetIndexFile, offset, Integer.MAX_VALUE, false);
                            offsetIndex.sanityCheck();

                            TimeIndex timeIndex =
                                    new TimeIndex(
                                            timestampIndexFile, offset, Integer.MAX_VALUE, false);
                            timeIndex.sanityCheck();

                            Entry entry = new Entry(offsetIndex, timeIndex);
                            internalCache.put(remoteSegmentId, entry);
                        } catch (CorruptIndexException e) {
                            LOG.debug(
                                    "Remote offset/time log index is corrupt, delete corrupt index.",
                                    e);
                            // let's delete offset index & time index
                            Files.deleteIfExists(offsetIndexFile.toPath());
                            Files.deleteIfExists(timestampIndexFile.toPath());
                        }
                    } else {
                        // Delete all of them if any one of those indexes is not available for a
                        // specific segment id.
                        tryAll(
                                Arrays.asList(
                                        () -> {
                                            Files.deleteIfExists(offsetIndexFile.toPath());
                                            return null;
                                        },
                                        () -> {
                                            Files.deleteIfExists(timestampIndexFile.toPath());
                                            return null;
                                        }));
                    }
                }
            }
        }

        LOG.info("RemoteIndexCache starts up in {} ms.", System.currentTimeMillis() - start);
    }

    private void enqueueEntryForCleanup(Entry entry, UUID key) {
        try {
            entry.markForCleanup();
            if (!expiredIndexes.offer(entry)) {
                LOG.error(
                        "Error while inserting entry {} for key {} into the cleaner queue because queue is full.",
                        entry,
                        key);
            }
        } catch (IOException e) {
            throw new FlussRuntimeException(e);
        }
    }

    private <T> T loadIndexFile(
            File file,
            RemoteLogSegment remoteLogSegment,
            Function<RemoteLogSegment, InputStream> fetchRemoteIndex,
            Function<File, T> readIndex)
            throws IOException {
        File indexFile = new File(cacheDir, file.getName());
        T index = null;
        if (Files.exists(indexFile.toPath())) {
            try {
                index = readIndex.apply(indexFile);
            } catch (CorruptIndexException ex) {
                LOG.info(
                        "Error occurred while loading the stored index file {} for bucket {}",
                        indexFile.getPath(),
                        remoteLogSegment.tableBucket(),
                        ex);
            }
        }
        if (index == null) {
            File tmpIndexFile =
                    new File(indexFile.getParentFile(), indexFile.getName() + TMP_FILE_SUFFIX);
            long start = System.currentTimeMillis();
            try (InputStream inputStream = fetchRemoteIndex.apply(remoteLogSegment)) {
                Files.copy(inputStream, tmpIndexFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                LOG.debug(
                        "Fetched index file from remote path: {} in {} ms.",
                        tmpIndexFile.getPath(),
                        System.currentTimeMillis() - start);
            }
            FileUtils.atomicMoveWithFallback(tmpIndexFile.toPath(), indexFile.toPath(), false);
            index = readIndex.apply(indexFile);
        }
        return index;
    }

    @Override
    public void close() throws IOException {
        // Make close idempotent and ensure no more reads allowed from henceforth. The in-progress
        // reads will continue to completion (release the read lock) and then close will begin
        // executing. Cleaner thread will immediately stop work.
        if (!isRemoteIndexCacheClosed.getAndSet(true)) {
            inWriteLock(
                    lock,
                    () -> {
                        try {
                            LOG.info(
                                    "Close initiated for RemoteIndexCache. Cache stats={}. Cache entries pending delete={}",
                                    internalCache.stats(),
                                    expiredIndexes.size());
                            boolean shutdownRequired = cleanerThread.initiateShutdown();
                            // Close all the opened indexes to force unload mmap memory. This does
                            // not delete the index files from disk.
                            internalCache.asMap().forEach((uuid, entry) -> entry.close());
                            // wait for cleaner thread to shut down.
                            if (shutdownRequired) {
                                cleanerThread.awaitShutdown();
                            }
                            // Note that internal cache does not require explicit cleaning/closing.
                            // We don't want to invalidate or cleanup the cache as both would lead
                            // to triggering of removal listener.
                            LOG.info("Close completed for RemoteIndexCache");
                        } catch (InterruptedException e) {
                            throw new FlussRuntimeException(e);
                        }
                    });
        }
    }

    static class Entry implements AutoCloseable {

        private final OffsetIndex offsetIndex;
        private final TimeIndex timeIndex;

        // This lock is used to synchronize cleanup methods and read methods. This ensures that
        // cleanup (which changes the underlying files of the index) isn't performed while a read is
        // in-progress for the entry. This is required in addition to using the thread safe cache
        // because, while the thread safety of the cache ensures that we can read entries
        // concurrently, it does not ensure that we won't mutate underlying files belonging to an
        // entry.
        private final ReentrantReadWriteLock entryLock = new ReentrantReadWriteLock();

        private boolean markedForCleanup = false;

        private boolean cleanStarted = false;

        private final long entrySizeBytes;

        public Entry(OffsetIndex offsetIndex, TimeIndex timeIndex) {
            this.offsetIndex = offsetIndex;
            this.timeIndex = timeIndex;
            this.entrySizeBytes = estimatedEntrySize();
        }

        // Visible for testing
        public OffsetIndex offsetIndex() {
            return offsetIndex;
        }

        // Visible for testing
        public TimeIndex timeIndex() {
            return timeIndex;
        }

        private long estimatedEntrySize() {
            return inReadLock(entryLock, offsetIndex::sizeInBytes);
        }

        public OffsetPosition lookupOffset(long targetOffset) {
            return inReadLock(
                    entryLock,
                    () -> {
                        if (markedForCleanup) {
                            throw new IllegalStateException("This entry is marked for cleanup.");
                        } else {
                            return offsetIndex.lookup(targetOffset);
                        }
                    });
        }

        public OffsetPosition lookupTimestamp(long timestamp) {
            return inReadLock(
                    entryLock,
                    () -> {
                        if (markedForCleanup) {
                            throw new IllegalStateException("This entry is marked for cleanup.");
                        }

                        TimestampOffset timestampOffset = timeIndex.lookup(timestamp);
                        return offsetIndex.lookup(timestampOffset.offset);
                    });
        }

        public void markForCleanup() throws IOException {
            inWriteLock(
                    entryLock,
                    () -> {
                        if (!markedForCleanup) {
                            markedForCleanup = true;
                            offsetIndex.renameTo(
                                    new File(
                                            FileUtils.replaceSuffix(
                                                    offsetIndex.file().getPath(),
                                                    "",
                                                    FlussPaths.DELETED_FILE_SUFFIX)));
                        }
                    });
        }

        public void cleanup() throws IOException {
            inWriteLock(
                    entryLock,
                    () -> {
                        markForCleanup();
                        // no-op if clean is done already
                        if (!cleanStarted) {
                            cleanStarted = true;

                            List<StorageAction<Void, Exception>> actions =
                                    Arrays.asList(
                                            () -> {
                                                offsetIndex.deleteIfExists();
                                                return null;
                                            },
                                            () -> {
                                                timeIndex.deleteIfExists();
                                                return null;
                                            });

                            tryAll(actions);
                        }
                    });
        }

        @Override
        public void close() {
            inReadLock(
                    entryLock,
                    () -> {
                        IOUtils.closeQuietly(offsetIndex, "OffsetIndex");
                        IOUtils.closeQuietly(timeIndex, "TimeIndex");
                    });
        }

        @Override
        public String toString() {
            return "Entry{"
                    + "offsetIndex="
                    + offsetIndex.file().getName()
                    + ", timeIndex="
                    + timeIndex.file().getName()
                    + '}';
        }

        @VisibleForTesting
        public boolean isCleanStarted() {
            return inReadLock(entryLock, () -> cleanStarted);
        }
    }

    /**
     * Executes each entry in `actions` list even if one or more throws an exception. If any of them
     * throws an IOException, it will be rethrown and adds all others encountered exceptions as
     * suppressed to that IOException. Otherwise, it throws FlussRuntimeException wrapped with the
     * first exception and the remaining exceptions are added as suppressed to the
     * FlussRuntimeException.
     *
     * @param actions actions to be executes
     * @throws IOException Any IOException encountered while executing those actions.
     * @throws FlussRuntimeException Any other non IOExceptions are wrapped and thrown as
     *     FlussRuntimeException
     */
    private static void tryAll(List<StorageAction<Void, Exception>> actions) throws IOException {
        IOException firstIOException = null;
        List<Exception> exceptions = new ArrayList<>();
        for (StorageAction<Void, Exception> action : actions) {
            try {
                action.execute();
            } catch (IOException e) {
                if (firstIOException == null) {
                    firstIOException = e;
                } else {
                    exceptions.add(e);
                }
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        if (firstIOException != null) {
            exceptions.forEach(firstIOException::addSuppressed);
            throw firstIOException;
        } else if (!exceptions.isEmpty()) {
            Iterator<Exception> iterator = exceptions.iterator();
            FlussRuntimeException flussRuntimeException =
                    new FlussRuntimeException(iterator.next());
            while (iterator.hasNext()) {
                flussRuntimeException.addSuppressed(iterator.next());
            }
            throw flussRuntimeException;
        }
    }

    @VisibleForTesting
    Cache<UUID, Entry> getInternalCache() {
        return internalCache;
    }

    @VisibleForTesting
    File cacheDir() {
        return cacheDir;
    }

    @VisibleForTesting
    ShutdownableThread cleanerThread() {
        return cleanerThread;
    }
}
