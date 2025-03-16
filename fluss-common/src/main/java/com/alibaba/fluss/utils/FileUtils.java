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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.utils.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.stream.Stream;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This is a utility class to deal files and directories. Contains utilities for recursive deletion
 * and creation of temporary files.
 */
public class FileUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    // ------------------------------------------------------------------------
    //  IO Utilities
    // ------------------------------------------------------------------------

    /**
     * Read data from the channel to the given byte buffer until there are no bytes remaining in the
     * buffer or the end of the file has been reached.
     *
     * @param channel File channel containing the data to read from
     * @param destinationBuffer The buffer into which bytes are to be transferred
     * @param position The file position at which the transfer is to begin; it must be non-negative
     * @throws IllegalArgumentException If position is negative
     * @throws IOException If an I/O error occurs, see {@link FileChannel#read(ByteBuffer, long)}
     *     for details on the possible exceptions
     */
    public static void readFully(FileChannel channel, ByteBuffer destinationBuffer, long position)
            throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException(
                    "The file channel position cannot be negative, but it is " + position);
        }
        long currentPosition = position;
        int bytesRead;
        do {
            bytesRead = channel.read(destinationBuffer, currentPosition);
            currentPosition += bytesRead;
        } while (bytesRead != -1 && destinationBuffer.hasRemaining());
    }

    /**
     * Read data from the channel to the given byte buffer until there are no bytes remaining in the
     * buffer. If the end of the file is reached while there are bytes remaining in the buffer, an
     * EOFException is thrown.
     *
     * @param channel File channel containing the data to read from
     * @param destinationBuffer The buffer into which bytes are to be transferred
     * @param position The file position at which the transfer is to begin; it must be non-negative
     * @param description A description of what is being read, this will be included in the
     *     EOFException if it is thrown
     * @throws IllegalArgumentException If position is negative
     * @throws EOFException If the end of the file is reached while there are remaining bytes in the
     *     destination buffer
     * @throws IOException If an I/O error occurs, see {@link FileChannel#read(ByteBuffer, long)}
     *     for details on the possible exceptions
     */
    public static void readFullyOrFail(
            FileChannel channel, ByteBuffer destinationBuffer, long position, String description)
            throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException(
                    "The file channel position cannot be negative, but it is " + position);
        }
        int expectedReadBytes = destinationBuffer.remaining();
        readFully(channel, destinationBuffer, position);
        if (destinationBuffer.hasRemaining()) {
            throw new EOFException(
                    String.format(
                            "Failed to read `%s` from file channel `%s`. Expected to read %d bytes, "
                                    + "but reached end of file after reading %d bytes. Started read from position %d.",
                            description,
                            channel,
                            expectedReadBytes,
                            expectedReadBytes - destinationBuffer.remaining(),
                            position));
        }
    }

    /**
     * Load a byte buffer from the given file channel.
     *
     * @param channel File channel containing the data to read from
     * @param size The size of the byte buffer to load
     * @param position The file position at which the transfer is to begin; it must be non-negative
     * @param description A description of what is being read, this will be included in the
     *     EOFException if it is thrown
     * @throws IllegalArgumentException If position is negative
     * @throws EOFException If the end of the file is reached while there are remaining bytes in the
     *     destination buffer
     * @throws IOException If an I/O error occurs, see {@link FileChannel#read(ByteBuffer, long)}
     *     for details on the possible exceptions
     */
    public static ByteBuffer loadByteBufferFromFile(
            FileChannel channel, int size, int position, String description) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        FileUtils.readFullyOrFail(channel, buffer, position, description);
        buffer.rewind();
        return buffer;
    }

    // ------------------------------------------------------------------------
    //  Flushing directories
    // ------------------------------------------------------------------------

    /** Flushes dirty file with swallowing {@link NoSuchFileException}. */
    public static void flushFileIfExists(Path path) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            fileChannel.force(true);
        } catch (NoSuchFileException e) {
            LOG.warn("Failed to flush file {}", path, e);
        }
    }

    /**
     * Flushes dirty directories to guarantee crash consistency with swallowing {@link
     * NoSuchFileException}.
     */
    public static void flushDirIfExists(Path path) throws IOException {
        try {
            flushDir(path);
        } catch (NoSuchFileException e) {
            LOG.warn("Failed to flush directory {}", path);
        }
    }

    /**
     * Flushes dirty directories to guarantee crash consistency.
     *
     * @throws IOException if flushing the directory fails.
     */
    public static void flushDir(Path path) throws IOException {
        if (path != null && !OperatingSystem.isWindows()) {
            try (FileChannel dir = FileChannel.open(path, StandardOpenOption.READ)) {
                dir.force(true);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Move directories
    // ------------------------------------------------------------------------

    /**
     * Attempts to move source to target atomically and falls back to a non-atomic move if it fails.
     * This function allows callers to decide whether to flush the parent directory. This is needed
     * when a sequence of atomicMoveWithFallback is called for the same directory and we don't want
     * to repeatedly flush the same parent directory.
     *
     * @throws IOException if both atomic and non-atomic moves fail, or parent dir flush fails if
     *     needFlushParentDir is true.
     */
    public static void atomicMoveWithFallback(Path source, Path target, boolean needFlushParentDir)
            throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException outer) {
            try {
                LOG.warn(
                        "Failed atomic move of {} to {} retrying with a non-atomic move",
                        source,
                        target,
                        outer);
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
                LOG.debug(
                        "Non-atomic move of {} to {} succeeded after atomic move failed",
                        source,
                        target);
            } catch (IOException inner) {
                inner.addSuppressed(outer);
                throw inner;
            }
        } finally {
            if (needFlushParentDir) {
                flushDir(target.toAbsolutePath().normalize().getParent());
            }
        }
    }

    /**
     * Attempts to move source to target atomically and falls back to a non-atomic move if it fails.
     * This function also flushes the parent directory to guarantee crash consistency.
     *
     * @throws IOException if both atomic and non-atomic moves fail, or parent dir flush fails.
     */
    public static void atomicMoveWithFallback(Path source, Path target) throws IOException {
        atomicMoveWithFallback(source, target, true);
    }

    /**
     * Replace the given string suffix with the new suffix. If the string doesn't end with the given
     * suffix throw an exception.
     */
    public static String replaceSuffix(String str, String oldSuffix, String newSuffix) {
        if (!str.endsWith(oldSuffix)) {
            throw new IllegalArgumentException(
                    "Expected string to end with " + oldSuffix + " but string is " + str);
        }
        return str.substring(0, str.length() - oldSuffix.length()) + newSuffix;
    }

    // ------------------------------------------------------------------------
    //  Listing directories
    // ------------------------------------------------------------------------

    /** Lists the given directory in a resource-leak-safe way. */
    public static Path[] listDirectory(Path directory) throws IOException {
        try (Stream<Path> stream = Files.list(directory)) {
            return stream.toArray(Path[]::new);
        }
    }

    // ------------------------------------------------------------------------
    //  Deleting directories on standard File Systems
    // ------------------------------------------------------------------------

    /** Global lock to prevent concurrent directory deletes under Windows and MacOS. */
    private static final Object DELETE_LOCK = new Object();

    /**
     * Removes the given file or directory recursively.
     *
     * <p>If the file or directory does not exist, this does not throw an exception, but simply does
     * nothing. It considers the fact that a file-to-be-deleted is not present a success.
     *
     * <p>This method is safe against other concurrent deletion attempts.
     *
     * @param file The file or directory to delete.
     * @throws IOException Thrown if the directory could not be cleaned for some reason, for example
     *     due to missing access/write permissions.
     */
    public static void deleteFileOrDirectory(File file) throws IOException {
        checkNotNull(file, "file");

        guardIfNotThreadSafe(FileUtils::deleteFileOrDirectoryInternal, file);
    }

    /**
     * Deletes the given directory recursively, not reporting any I/O exceptions that occur.
     *
     * <p>This method is identical to {@link FileUtils#deleteDirectory(File)}, except that it
     * swallows all exceptions and may leave the job quietly incomplete.
     *
     * @param directory The directory to delete.
     */
    public static void deleteDirectoryQuietly(File directory) {
        if (directory == null) {
            return;
        }

        // delete and do not report if it fails
        try {
            deleteDirectory(directory);
        } catch (Exception ignored) {
        }
    }

    /**
     * Deletes the given directory recursively.
     *
     * <p>If the directory does not exist, this does not throw an exception, but simply does
     * nothing. It considers the fact that a directory-to-be-deleted is not present a success.
     *
     * <p>This method is safe against other concurrent deletion attempts.
     *
     * @param directory The directory to be deleted.
     * @throws IOException Thrown if the given file is not a directory, or if the directory could
     *     not be deleted for some reason, for example due to missing access/write permissions.
     */
    public static void deleteDirectory(File directory) throws IOException {
        checkNotNull(directory, "directory");

        guardIfNotThreadSafe(FileUtils::deleteDirectoryInternal, directory);
    }

    /**
     * Lists all directories in the given directory.
     *
     * @param directory The directory to list.
     * @return The directories in the given directory.
     */
    public static File[] listDirectories(File directory) {
        checkNotNull(directory, "directory should not be null");
        File[] files = directory.listFiles();
        files = files != null ? files : new File[0];
        return Arrays.stream(files).filter(File::isDirectory).toArray(File[]::new);
    }

    /**
     * Checks whether the given directory is empty.
     *
     * @param directory The directory to check.
     * @return True, if the directory is empty, false otherwise.
     */
    public static boolean isDirectoryEmpty(File directory) {
        checkNotNull(directory, "directory should not be null");
        File[] files = directory.listFiles();
        return files == null || files.length == 0;
    }

    /**
     * Checks whether the given directory is empty.
     *
     * @param directory The directory to check.
     * @return True, if the directory is empty, false otherwise.
     */
    public static boolean isDirectoryEmpty(Path directory) {
        if (Files.isDirectory(directory)) {
            try (Stream<Path> entries = Files.list(directory)) {
                return !entries.findFirst().isPresent();
            } catch (IOException e) {
                throw new FlussRuntimeException("Could not check if directory is empty.", e);
            }
        }
        return false;
    }

    private static void deleteFileOrDirectoryInternal(File file) throws IOException {
        if (file.isDirectory()) {
            // file exists and is directory
            deleteDirectoryInternal(file);
        } else {
            // if the file is already gone (concurrently), we don't mind
            Files.deleteIfExists(file.toPath());
        }
        // else: already deleted
    }

    private static void deleteDirectoryInternal(File directory) throws IOException {
        if (directory.isDirectory()) {
            // directory exists and is a directory

            // empty the directory first
            try {
                cleanDirectoryInternal(directory);
            } catch (FileNotFoundException ignored) {
                // someone concurrently deleted the directory, nothing to do for us
                return;
            }

            // delete the directory. this fails if the directory is not empty, meaning
            // if new files got concurrently created. we want to fail then.
            // if someone else deleted the empty directory concurrently, we don't mind
            // the result is the same for us, after all
            Files.deleteIfExists(directory.toPath());
        } else if (directory.exists()) {
            // exists but is file, not directory
            // either an error from the caller, or concurrently a file got created
            throw new IOException(directory + " is not a directory");
        }
        // else: does not exist, which is okay (as if deleted)
    }

    private static void cleanDirectoryInternal(File directory) throws IOException {
        if (Files.isSymbolicLink(directory.toPath())) {
            // the user directories which symbolic links point to should not be cleaned.
            return;
        }
        if (directory.isDirectory()) {
            final File[] files = directory.listFiles();

            if (files == null) {
                // directory does not exist any more or no permissions
                if (directory.exists()) {
                    throw new IOException("Failed to list contents of " + directory);
                } else {
                    throw new FileNotFoundException(directory.toString());
                }
            }

            // remove all files in the directory
            for (File file : files) {
                if (file != null) {
                    deleteFileOrDirectory(file);
                }
            }
        } else if (directory.exists()) {
            throw new IOException(directory + " is not a directory but a regular file");
        } else {
            // else does not exist at all
            throw new FileNotFoundException(directory.toString());
        }
    }

    private static void guardIfNotThreadSafe(ThrowingConsumer<File, IOException> toRun, File file)
            throws IOException {
        if (OperatingSystem.isMac()) {
            guardIfMac(toRun, file);
            return;
        }

        toRun.accept(file);
    }

    // Guard Mac for the same reason we guard windows. Refer to guardIfWindows for details.
    // The difference to guardIfWindows is that we don't swallow the AccessDeniedException because
    // doing that would lead to wrong behaviour.
    private static void guardIfMac(ThrowingConsumer<File, IOException> toRun, File file)
            throws IOException {
        synchronized (DELETE_LOCK) {
            toRun.accept(file);
            // briefly wait and fall through the loop
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // restore the interruption flag and error out of the method
                Thread.currentThread().interrupt();
                throw new IOException("operation interrupted");
            }
        }
    }

    public static File createDirectory(File parentDir, String dirName) throws IOException {
        String parentDirPath = parentDir.getAbsolutePath();
        File dir = new File(parentDirPath, dirName);
        Files.createDirectories(dir.toPath());
        return dir;
    }
}
