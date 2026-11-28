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

package org.apache.fluss.fs.local;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemFailure;
import org.apache.fluss.fs.FileSystemOperationException;
import org.apache.fluss.fs.FileSystemPathNotFoundException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.utils.OperatingSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * The class {@code LocalFileSystem} is an implementation of the {@link FileSystem} interface for
 * the local file system of the machine where the JVM runs.
 */
@Internal
public class LocalFileSystem extends FileSystem {

    private static final ObtainedSecurityToken TOKEN =
            new ObtainedSecurityToken("file", new byte[0], null, Collections.emptyMap());

    /** The URI representing the local file system. */
    private static final URI LOCAL_URI =
            OperatingSystem.isWindows() ? URI.create("file:/") : URI.create("file:///");

    /** The shared instance of the local file system. */
    private static final LocalFileSystem INSTANCE = new LocalFileSystem();

    @Override
    public FileStatus getFileStatus(FsPath f) throws IOException {
        final File path = pathToFile(f);
        try {
            Files.readAttributes(path.toPath(), BasicFileAttributes.class);
            return new LocalFileStatus(path, this);
        } catch (IOException failure) {
            throw normalize(failure, "get_file_status", true);
        }
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        return TOKEN;
    }

    @Override
    public URI getUri() {
        return LOCAL_URI;
    }

    @Override
    public FSDataInputStream open(final FsPath f) throws IOException {
        final File file = pathToFile(f);
        try {
            return new LocalDataInputStream(file);
        } catch (FileNotFoundException failure) {
            try (SeekableByteChannel ignored =
                    Files.newByteChannel(file.toPath(), StandardOpenOption.READ)) {
                // A successful probe does not explain the earlier failure.
            } catch (NoSuchFileException missing) {
                failure.addSuppressed(missing);
                throw new FileSystemPathNotFoundException("open", null, null, failure);
            } catch (AccessDeniedException denied) {
                failure.addSuppressed(denied);
                throw new FileSystemOperationException(
                        FileSystemFailure.Kind.PERMISSION_DENIED,
                        FileSystemFailure.Resource.UNKNOWN,
                        "open",
                        false,
                        null,
                        null,
                        failure);
            } catch (IOException probeFailure) {
                failure.addSuppressed(probeFailure);
                throw normalize(failure, "open", false);
            }
            throw normalize(failure, "open", false);
        } catch (IOException failure) {
            throw normalize(failure, "open", false);
        }
    }

    @Override
    public boolean exists(FsPath f) throws IOException {
        return super.exists(f);
    }

    @Override
    public FileStatus[] listStatus(final FsPath f) throws IOException {

        final Path path = pathToFile(f).toPath();
        BasicFileAttributes attributes;
        try {
            attributes = Files.readAttributes(path, BasicFileAttributes.class);
        } catch (IOException failure) {
            throw normalize(failure, "list_status", true);
        }
        if (!attributes.isDirectory()) {
            return new FileStatus[] {new LocalFileStatus(path.toFile(), this)};
        }

        List<FileStatus> results = new ArrayList<FileStatus>();
        boolean childDisappeared = false;
        try (DirectoryStream<Path> children = Files.newDirectoryStream(path)) {
            for (Path child : children) {
                try {
                    Files.readAttributes(child, BasicFileAttributes.class);
                    results.add(new LocalFileStatus(child.toFile(), this));
                } catch (NoSuchFileException ignored) {
                    childDisappeared = true;
                }
            }
        } catch (DirectoryIteratorException failure) {
            throw normalize(failure.getCause(), "list_status", false);
        } catch (IOException failure) {
            throw normalize(failure, "list_status", true);
        }
        if (childDisappeared) {
            try {
                Files.readAttributes(path, BasicFileAttributes.class);
            } catch (IOException failure) {
                throw normalize(failure, "list_status", true);
            }
        }
        return results.toArray(new FileStatus[0]);
    }

    @Override
    public boolean delete(final FsPath f, final boolean recursive) throws IOException {
        final Path path = pathToFile(f).toPath();
        try {
            if (!recursive) {
                return Files.deleteIfExists(path);
            }
            try {
                Files.readAttributes(path, BasicFileAttributes.class);
            } catch (NoSuchFileException ignored) {
                return false;
            }
            Files.walkFileTree(
                    path,
                    new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
                                throws IOException {
                            Files.deleteIfExists(file);
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFileFailed(Path file, IOException failure)
                                throws IOException {
                            if (failure instanceof NoSuchFileException) {
                                return FileVisitResult.CONTINUE;
                            }
                            throw failure;
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(
                                Path directory, IOException failure) throws IOException {
                            if (failure != null) {
                                throw failure;
                            }
                            Files.deleteIfExists(directory);
                            return FileVisitResult.CONTINUE;
                        }
                    });
            return true;
        } catch (IOException failure) {
            throw normalize(failure, "delete", false);
        }
    }

    /**
     * Recursively creates the directory specified by the provided path.
     *
     * @return <code>true</code>if the directories either already existed or have been created
     *     successfully, <code>false</code> otherwise
     * @throws IOException thrown if an error occurred while creating the directory/directories
     */
    @Override
    public boolean mkdirs(final FsPath f) throws IOException {
        checkNotNull(f, "path is null");
        try {
            Files.createDirectories(pathToFile(f).toPath());
            return true;
        } catch (FileAlreadyExistsException failure) {
            throw failure;
        } catch (IOException failure) {
            throw normalize(failure, "mkdirs", false);
        }
    }

    @Override
    public FSDataOutputStream create(final FsPath filePath, final WriteMode overwrite)
            throws IOException {
        checkNotNull(filePath, "filePath");

        try {
            if (exists(filePath) && overwrite == WriteMode.NO_OVERWRITE) {
                throw new FileAlreadyExistsException("File already exists: " + filePath);
            }

            final FsPath parent = filePath.getParent();
            if (parent != null) {
                mkdirs(parent);
            }

            return new LocalDataOutputStream(pathToFile(filePath));
        } catch (FileAlreadyExistsException failure) {
            throw failure;
        } catch (IOException failure) {
            throw normalize(failure, "create", false);
        }
    }

    @Override
    public boolean rename(final FsPath src, final FsPath dst) throws IOException {
        final File srcFile = pathToFile(src);
        final File dstFile = pathToFile(dst);

        final File dstParent = dstFile.getParentFile();

        try {
            Files.createDirectories(dstParent.toPath());
            Files.move(srcFile.toPath(), dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (NoSuchFileException failure) {
            if (Files.notExists(srcFile.toPath())) {
                return false;
            }
            throw normalize(failure, "rename", false);
        } catch (DirectoryNotEmptyException failure) {
            return false;
        } catch (IOException failure) {
            throw normalize(failure, "rename", false);
        }
    }

    private static IOException normalize(
            IOException failure, String operation, boolean targetPath) {
        if (failure instanceof FileSystemFailure) {
            return failure;
        }
        if (targetPath && failure instanceof NoSuchFileException) {
            return new FileSystemPathNotFoundException(operation, null, null, failure);
        }
        FileSystemFailure.Kind kind =
                failure instanceof AccessDeniedException
                        ? FileSystemFailure.Kind.PERMISSION_DENIED
                        : failure instanceof NoSuchFileException
                                ? FileSystemFailure.Kind.NOT_FOUND
                                : FileSystemFailure.Kind.UNEXPECTED;
        return new FileSystemOperationException(
                kind, FileSystemFailure.Resource.UNKNOWN, operation, false, null, null, failure);
    }

    // ------------------------------------------------------------------------

    /**
     * Converts the given Path to a File for this file system. If the path is empty, we will return
     * <tt>new File(".")</tt> instead of <tt>new File("")</tt>, since the latter returns
     * <tt>false</tt> for <tt>isDirectory</tt> judgement (See issue
     * https://issues.apache.org/jira/browse/FLINK-18612).
     */
    public File pathToFile(FsPath path) {
        String localPath = path.getPath();
        checkState(localPath != null, "Cannot convert a null path to File");

        if (localPath.length() == 0) {
            return new File(".");
        }

        return new File(localPath);
    }

    // ------------------------------------------------------------------------

    /**
     * Gets the URI that represents the local file system. That URI is {@code "file:/"} on Windows
     * platforms and {@code "file:///"} on other UNIX family platforms.
     *
     * @return The URI that represents the local file system.
     */
    public static URI getLocalFsURI() {
        return LOCAL_URI;
    }

    /**
     * Gets the shared instance of this file system.
     *
     * @return The shared instance of this file system.
     */
    public static LocalFileSystem getSharedInstance() {
        return INSTANCE;
    }
}
