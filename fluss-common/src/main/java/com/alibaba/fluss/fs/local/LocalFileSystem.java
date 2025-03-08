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

package com.alibaba.fluss.fs.local;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.fs.FSDataInputStream;
import com.alibaba.fluss.fs.FSDataOutputStream;
import com.alibaba.fluss.fs.FileStatus;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import com.alibaba.fluss.utils.OperatingSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardCopyOption;
import java.util.Collections;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.Preconditions.checkState;

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
        if (path.exists()) {
            return new LocalFileStatus(path, this);
        } else {
            throw new FileNotFoundException(
                    "File "
                            + f
                            + " does not exist or the user running "
                            + "Fluss ('"
                            + System.getProperty("user.name")
                            + "') has insufficient permissions to access it.");
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
        return new LocalDataInputStream(file);
    }

    @Override
    public boolean exists(FsPath f) throws IOException {
        final File path = pathToFile(f);
        return path.exists();
    }

    @Override
    public FileStatus[] listStatus(final FsPath f) throws IOException {

        final File localf = pathToFile(f);
        FileStatus[] results;

        if (!localf.exists()) {
            return null;
        }
        if (localf.isFile()) {
            return new FileStatus[] {new LocalFileStatus(localf, this)};
        }

        final String[] names = localf.list();
        if (names == null) {
            return null;
        }
        results = new FileStatus[names.length];
        for (int i = 0; i < names.length; i++) {
            results[i] = getFileStatus(new FsPath(f, names[i]));
        }

        return results;
    }

    @Override
    public boolean delete(final FsPath f, final boolean recursive) throws IOException {

        final File file = pathToFile(f);
        if (file.isFile()) {
            return file.delete();
        } else if ((!recursive) && file.isDirectory()) {
            File[] containedFiles = file.listFiles();
            if (containedFiles == null) {
                throw new IOException(
                        "Directory " + file + " does not exist or an I/O error occurred");
            } else if (containedFiles.length != 0) {
                throw new IOException("Directory " + file + " is not empty");
            }
        }

        return delete(file);
    }

    /**
     * Deletes the given file or directory.
     *
     * @param f the file to be deleted
     * @return <code>true</code> if all files were deleted successfully, <code>false</code>
     *     otherwise
     * @throws IOException thrown if an error occurred while deleting the files/directories
     */
    private boolean delete(final File f) throws IOException {

        if (f.isDirectory()) {
            final File[] files = f.listFiles();
            if (files != null) {
                for (File file : files) {
                    final boolean del = delete(file);
                    if (!del) {
                        return false;
                    }
                }
            }
        } else {
            return f.delete();
        }

        // Now directory is empty
        return f.delete();
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
        return mkdirsInternal(pathToFile(f));
    }

    private boolean mkdirsInternal(File file) throws IOException {
        if (file.isDirectory()) {
            return true;
        } else if (file.exists() && !file.isDirectory()) {
            // Important: The 'exists()' check above must come before the 'isDirectory()' check to
            //            be safe when multiple parallel instances try to create the directory

            // exists and is not a directory -> is a regular file
            throw new FileAlreadyExistsException(file.getAbsolutePath());
        } else {
            File parent = file.getParentFile();
            return (parent == null || mkdirsInternal(parent))
                    && (file.mkdir() || file.isDirectory());
        }
    }

    @Override
    public FSDataOutputStream create(final FsPath filePath, final WriteMode overwrite)
            throws IOException {
        checkNotNull(filePath, "filePath");

        if (exists(filePath) && overwrite == WriteMode.NO_OVERWRITE) {
            throw new FileAlreadyExistsException("File already exists: " + filePath);
        }

        final FsPath parent = filePath.getParent();
        if (parent != null && !mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }

        final File file = pathToFile(filePath);
        return new LocalDataOutputStream(file);
    }

    @Override
    public boolean rename(final FsPath src, final FsPath dst) throws IOException {
        final File srcFile = pathToFile(src);
        final File dstFile = pathToFile(dst);

        final File dstParent = dstFile.getParentFile();

        // Files.move fails if the destination directory doesn't exist
        //noinspection ResultOfMethodCallIgnored -- we don't care if the directory existed or was
        // created
        dstParent.mkdirs();

        try {
            Files.move(srcFile.toPath(), dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (NoSuchFileException
                | AccessDeniedException
                | DirectoryNotEmptyException
                | SecurityException ex) {
            // catch the errors that are regular "move failed" exceptions and return false
            return false;
        }
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
