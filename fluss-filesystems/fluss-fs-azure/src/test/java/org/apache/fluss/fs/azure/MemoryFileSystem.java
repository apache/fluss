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

package org.apache.fluss.fs.azure;

import org.apache.fluss.utils.MapUtils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Util file system abstraction. */
public class MemoryFileSystem extends FileSystem {

    private final URI uri;
    private final Map<Path, byte[]> files = MapUtils.newConcurrentHashMap();
    private final Set<Path> directories =
            Collections.newSetFromMap(MapUtils.newConcurrentHashMap());

    public MemoryFileSystem(URI uri) {
        this.uri = uri;
    }

    @Override
    public boolean exists(Path f) throws IOException {
        return files.containsKey(f) || directories.contains(f);
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        return open(f, -1);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        byte[] data = files.get(f);

        if (data == null) {
            throw new IOException(f.toString());
        }

        return new FSDataInputStream(
                new FSInputStream() {
                    private int pos = 0;

                    @Override
                    public void seek(long pos) {
                        this.pos = (int) pos;
                    }

                    @Override
                    public long getPos() {
                        return pos;
                    }

                    @Override
                    public boolean seekToNewSource(long targetPos) {
                        return false;
                    }

                    @Override
                    public int read() {
                        return pos < data.length ? (data[pos++] & 0xff) : -1;
                    }
                });
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
        return create(f, overwrite, -1, (short) -1, -1, null);
    }

    @Override
    public FSDataOutputStream create(
            Path f,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress)
            throws IOException {

        if (!overwrite && files.containsKey(f)) {
            throw new IOException("File exists: " + f);
        }

        directories.add(f.getParent());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final Path toRet = f;
        return new FSDataOutputStream(
                new FilterOutputStream(baos) {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        files.put(toRet, baos.toByteArray());
                    }
                },
                null);
    }

    @Override
    public FSDataOutputStream create(
            Path path,
            FsPermission fsPermission,
            boolean b,
            int i,
            short i1,
            long l,
            Progressable progressable)
            throws IOException {
        return create(path, b, i, i1, l, progressable);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        if (files.remove(f) != null) {
            return true;
        }

        if (!recursive) {
            boolean hasChildren =
                    files.keySet().stream()
                            .anyMatch(p -> p.getParent().toString().startsWith(f.toString()));
            if (hasChildren) {
                throw new IOException();
            }
        }

        directories.removeIf(d -> d.toString().startsWith(f.toString()));
        files.keySet().removeIf(p -> p.toString().startsWith(f.toString()));
        return true;
    }

    @Override
    public FileStatus[] listStatus(Path f) {
        if (files.containsKey(f)) {
            return new FileStatus[] {new FileStatus(files.get(f).length, false, 1, 1, 0, f)};
        }

        if (directories.contains(f)) {
            List<FileStatus> statusList = new ArrayList<>();

            for (Path p : files.keySet()) {
                if (p.getParent().equals(f)) {
                    statusList.add(new FileStatus(files.get(p).length, false, 1, 1, 0, p));
                }
            }

            for (Path d : directories) {
                if (d.getParent() != null && d.getParent().equals(f) && !d.equals(f)) {
                    statusList.add(new FileStatus(0, true, 1, 1, 0, d));
                }
            }

            return statusList.toArray(new FileStatus[0]);
        }

        return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path path) {}

    @Override
    public Path getWorkingDirectory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        Path parent = f;
        while (parent != null) {
            if (files.containsKey(parent)) {
                throw new IOException();
            }
            parent = parent.getParent();
        }

        directories.add(f);
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        if (files.containsKey(f)) {
            return new FileStatus(files.get(f).length, false, 1, 1, 0, f);
        }
        if (directories.contains(f)) {
            return new FileStatus(0, true, 1, 1, 0, f);
        }

        throw new IOException(f.toString());
    }
}
