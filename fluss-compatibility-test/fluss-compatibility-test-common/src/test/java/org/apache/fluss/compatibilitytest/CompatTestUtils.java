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

package org.apache.fluss.compatibilitytest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ListImagesCmd;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Some utils method for compatibility test. */
public class CompatTestUtils {

    /**
     * Check whether the image exists in the docker client.
     *
     * @param dockerClient The docker client
     * @param imageName The image name
     * @return True if the image exists
     */
    public static boolean checkImageExists(DockerClient dockerClient, String imageName) {
        ListImagesCmd listImagesCmd = dockerClient.listImagesCmd();
        return listImagesCmd.exec().stream()
                .anyMatch(
                        image -> {
                            String[] tags = image.getRepoTags();
                            if (tags != null) {
                                for (String tag : tags) {
                                    if (tag.equals(imageName)) {
                                        return true;
                                    }
                                }
                            }
                            return false;
                        });
    }

    public static Port getAvailablePort() {
        for (int i = 0; i < 50; i++) {
            try (ServerSocket serverSocket = new ServerSocket(0)) {
                int port = serverSocket.getLocalPort();
                if (port != 0) {
                    FileLock fileLock = new FileLock(CompatTestUtils.class.getName() + port);
                    if (fileLock.tryLock()) {
                        return new Port(port, fileLock);
                    } else {
                        fileLock.unlockAndDestroy();
                    }
                }
            } catch (IOException ignored) {
            }
        }

        throw new RuntimeException("Could not find a free permitted port on the machine.");
    }

    /**
     * Port wrapper class which holds a {@link FileLock} until it releases. Used to avoid race
     * condition among multiple threads/processes.
     */
    public static class Port implements AutoCloseable {
        private final int port;
        private final FileLock fileLock;

        public Port(int port, FileLock fileLock) throws IOException {
            this.port = port;
            this.fileLock = fileLock;
        }

        public int getPort() {
            return port;
        }

        @Override
        public void close() throws Exception {
            fileLock.unlockAndDestroy();
        }
    }

    private static class FileLock {
        private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");
        private final File file;
        private FileOutputStream outputStream;
        private java.nio.channels.FileLock lock;

        /**
         * Initialize a FileLock using a file located at fullPath.
         *
         * @param fullPath The path of the locking file
         */
        public FileLock(String fullPath) {
            Path path = Paths.get(fullPath);
            String normalizedFileName = normalizeFileName(path.getFileName().toString());
            if (normalizedFileName.isEmpty()) {
                throw new IllegalArgumentException(
                        "There are no legal characters in the file name");
            }
            this.file =
                    path.getParent() == null
                            ? new File(TEMP_DIR, normalizedFileName)
                            : new File(path.getParent().toString(), normalizedFileName);
        }

        /**
         * Check whether the locking file exists in the file system. Create it if it does not exist.
         * Then create a FileOutputStream for it.
         *
         * @throws IOException If the file path is invalid or the parent dir does not exist
         */
        private void init() throws IOException {
            if (!this.file.exists()) {
                this.file.createNewFile();
            }
            outputStream = new FileOutputStream(this.file);
        }

        /**
         * Try to acquire a lock on the locking file. This method immediately returns whenever the
         * lock is acquired or not.
         *
         * @return True if successfully acquired the lock
         * @throws IOException If the file path is invalid
         */
        public boolean tryLock() throws IOException {
            if (outputStream == null) {
                init();
            }
            try {
                lock = outputStream.getChannel().tryLock();
            } catch (Exception e) {
                return false;
            }

            return lock != null;
        }

        /**
         * Release the file lock.
         *
         * @throws IOException If the FileChannel is closed
         */
        public void unlock() throws IOException {
            if (lock != null && lock.channel().isOpen()) {
                lock.release();
            }
        }

        /**
         * Release the file lock, close the fileChannel and FileOutputStream then try deleting the
         * locking file if other file lock does not need it, which means the lock will not be used
         * anymore.
         *
         * @throws IOException If an I/O error occurs
         */
        public void unlockAndDestroy() throws IOException {
            try {
                unlock();
                if (lock != null) {
                    lock.channel().close();
                    lock = null;
                }
                if (outputStream != null) {
                    outputStream.close();
                    outputStream = null;
                }

            } finally {
                this.file.delete();
            }
        }

        /**
         * Check whether a FileLock is actually holding the lock.
         *
         * @return True if it is actually holding the lock
         */
        public boolean isValid() {
            if (lock != null) {
                return lock.isValid();
            }
            return false;
        }

        /**
         * Normalize the file name, which only allows slash, backslash, digits and letters.
         *
         * @param fileName Original file name
         * @return File name with illegal characters stripped
         */
        private static String normalizeFileName(String fileName) {
            return fileName.replaceAll("[^\\w/\\\\]", "");
        }
    }
}
