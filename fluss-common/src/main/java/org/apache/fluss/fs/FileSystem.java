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

package org.apache.fluss.fs;

import org.apache.fluss.annotation.PublicStable;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.fs.local.LocalFileSystemPlugin;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableMultimap;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Iterators;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Multimap;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.TemporaryClassLoaderContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Abstract base class of all file systems used by Fluss. This class may be extended to implement
 * distributed file systems, or local file systems. The abstraction by this file system is very
 * simple, and the set of available operations quite limited, to support the common denominator of a
 * wide range of file systems. For example, appending to or mutating existing files is not
 * supported.
 *
 * <p>Fluss implements and supports some file system types directly (for example the default
 * machine-local file system). Other file system types are accessed by an implementation that
 * bridges to the suite of file systems supported by Hadoop (such as for example HDFS).
 *
 * <h2>Scope and Purpose</h2>
 *
 * <p>The purpose of this abstraction is used to expose a common and well defined interface for
 * access to files. This abstraction is used both by Fluss's fault tolerance mechanism (recovery
 * data) in KV Store and tiered storage in Log Store .
 *
 * <h2>Data Persistence Contract</h2>
 *
 * <p>The FileSystem's {@link FSDataOutputStream output streams} are used to persistently store
 * data, both for checkpoints of KV Store and for tiered storage of Log Store. It is therefore
 * crucial that the persistence semantics of these streams are well defined.
 *
 * <h3>Definition of Persistence Guarantees</h3>
 *
 * <p>Data written to an output stream is considered persistent, if two requirements are met:
 *
 * <ol>
 *   <li><b>Visibility Requirement:</b> It must be guaranteed that all other processes, machines,
 *       virtual machines, containers, etc. that are able to access the file see the data
 *       consistently when given the absolute file path. This requirement is similar to the
 *       <i>close-to-open</i> semantics defined by POSIX, but restricted to the file itself (by its
 *       absolute path).
 *   <li><b>Durability Requirement:</b> The file system's specific durability/persistence
 *       requirements must be met. These are specific to the particular file system. For example the
 *       {@link LocalFileSystem} does not provide any durability guarantees for crashes of both
 *       hardware and operating system, while replicated distributed file systems (like HDFS)
 *       typically guarantee durability in the presence of at most <i>n</i> concurrent node
 *       failures, where <i>n</i> is the replication factor.
 * </ol>
 *
 * <p>Updates to the file's parent directory (such that the file shows up when listing the directory
 * contents) are not required to be complete for the data in the file stream to be considered
 * persistent. This relaxation is important for file systems where updates to directory contents are
 * only eventually consistent.
 *
 * <p>The {@link FSDataOutputStream} has to guarantee data persistence for the written bytes once
 * the call to {@link FSDataOutputStream#close()} returns.
 *
 * <h3>Examples</h3>
 *
 * <h4>Fault-tolerant distributed file systems</h4>
 *
 * <p>For <b>fault-tolerant distributed file systems</b>, data is considered persistent once it has
 * been received and acknowledged by the file system, typically by having been replicated to a
 * quorum of machines (<i>durability requirement</i>). In addition the absolute file path must be
 * visible to all other machines that will potentially access the file (<i>visibility
 * requirement</i>).
 *
 * <p>Whether data has hit non-volatile storage on the storage nodes depends on the specific
 * guarantees of the particular file system.
 *
 * <p>The metadata updates to the file's parent directory are not required to have reached a
 * consistent state. It is permissible that some machines see the file when listing the parent
 * directory's contents while others do not, as long as access to the file by its absolute path is
 * possible on all nodes.
 *
 * <h4>Local file systems</h4>
 *
 * <p>A <b>local file system</b> must support the POSIX <i>close-to-open</i> semantics. Because the
 * local file system does not have any fault tolerance guarantees, no further requirements exist.
 *
 * <p>The above implies specifically that data may still be in the OS cache when considered
 * persistent from the local file system's perspective. Crashes that cause the OS cache to lose data
 * are considered fatal to the local machine and are not covered by the local file system's
 * guarantees as defined by Fluss.
 *
 * <p>That means that checkpoints of KV Store that are written only to the local filesystem are not
 * guaranteed to be recoverable from the local machine's failure, making local file systems
 * unsuitable for production setups.
 *
 * <h2>Updating File Contents</h2>
 *
 * <p>Many file systems either do not support overwriting contents of existing files at all, or do
 * not support consistent visibility of the updated contents in that case. For that reason, Fluss's
 * FileSystem does not support appending to existing files, or seeking within output streams so that
 * previously written data could be overwritten.
 *
 * <h2>Overwriting Files</h2>
 *
 * <p>Overwriting files is in general possible. A file is overwritten by deleting it and creating a
 * new file. However, certain filesystems cannot make that change synchronously visible to all
 * parties that have access to the file. For example <a
 * href="https://aws.amazon.com/documentation/s3/">Amazon S3</a> guarantees only <i>eventual
 * consistency</i> in the visibility of the file replacement: Some machines may see the old file,
 * some machines may see the new file.
 *
 * <p>To avoid these consistency issues, the implementations of failure/recovery mechanisms in Fluss
 * strictly avoid writing to the same file path more than once.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>Implementations of {@code FileSystem} must be thread-safe: The same instance of FileSystem is
 * frequently shared across multiple threads in Fluss and must be able to concurrently create
 * input/output streams and list file metadata.
 *
 * <p>The {@link FSDataInputStream} and {@link FSDataOutputStream} implementations are strictly
 * <b>not thread-safe</b>. Instances of the streams should also not be passed between threads in
 * between read or write operations, because there are no guarantees about the visibility of
 * operations across threads (many operations do not create memory fences).
 *
 * <h2>Streams Safety Net</h2>
 *
 * <p>After call method {@link FileSystemSafetyNet#initializeSafetyNetForThread()}, and then obtains
 * a FileSystem (via {@link FileSystem#get(URI)} or via {@link FsPath#getFileSystem()}), the
 * FileSystem instantiates a safety net for that FileSystem. The safety net ensures that all streams
 * created from the FileSystem are tracked and can be closed via method {@link
 * FileSystemSafetyNet#closeSafetyNetAndGuardedResourcesForThread()}.
 *
 * <p>It also exposes a method {@link FileSystem#getUnguardedFileSystem(URI)} to provide a
 * FileSystem that does not use the safety net.
 *
 * @see FSDataInputStream
 * @see FSDataOutputStream
 * @since 0.1
 */
@PublicStable
public abstract class FileSystem {

    /**
     * The possible write modes. The write mode decides what happens if a file should be created,
     * but already exists.
     */
    public enum WriteMode {

        /**
         * Creates the target file only if no file exists at that path already. Does not overwrite
         * existing files and directories.
         */
        NO_OVERWRITE,

        /**
         * Creates a new target file regardless of any existing files or directories. Existing files
         * and directories will be deleted (recursively) automatically before creating the new file.
         */
        OVERWRITE
    }

    // ------------------------------------------------------------------------

    /** Logger for all FileSystem work. */
    private static final Logger LOG = LoggerFactory.getLogger(FileSystem.class);

    /** Object used to protect calls to specific methods. */
    private static final ReentrantLock LOCK = new ReentrantLock(true);

    /** Cache for file systems, by scheme + authority. */
    private static final HashMap<FSKey, FileSystem> CACHE = new HashMap<>();

    /**
     * Mapping of file system schemes to the corresponding factories, populated in {@link
     * FileSystem#initialize(Configuration, PluginManager)}.
     */
    private static final HashMap<String, FileSystemPlugin> FS_PLUGINS = new HashMap<>();

    /** All known plugins for a given scheme. */
    private static final Multimap<String, String> DIRECTLY_SUPPORTED_FILESYSTEM =
            ImmutableMultimap.<String, String>builder()
                    .put("hdfs", "fluss-fs-hadoop")
                    .put("oss", "fluss-fs-oss")
                    .build();

    /**
     * The configuration used to create a FileSystem via {@link FileSystemPlugin#create(URI,
     * Configuration)}. It will be set in method {@link #initialize(Configuration, PluginManager)}
     */
    private static Configuration configuration = new Configuration();

    private static void initializeWithoutPlugins(Configuration config)
            throws IllegalConfigurationException {
        initialize(config, null);
    }

    /**
     * Initializes the shared file system settings.
     *
     * <p>The given configuration is passed to each file system plugin to initialize the respective
     * file systems. Because the configuration of file systems may be different subsequent to the
     * call of this method, this method clears the file system instance cache.
     *
     * @param config the configuration from where to fetch the parameter.
     * @param pluginManager optional plugin manager that is used to initialized filesystems provided
     *     as plugins.
     */
    public static void initialize(Configuration config, @Nullable PluginManager pluginManager)
            throws IllegalConfigurationException {
        inLock(
                LOCK,
                () -> {
                    // make sure file systems are re-instantiated after re-configuration
                    CACHE.clear();
                    FS_PLUGINS.clear();

                    // set the configuration
                    configuration = config;

                    Collection<Supplier<Iterator<FileSystemPlugin>>> pluginSuppliers =
                            new ArrayList<>(2);
                    pluginSuppliers.add(
                            () ->
                                    ServiceLoader.load(
                                                    FileSystemPlugin.class,
                                                    FileSystem.class.getClassLoader())
                                            .iterator());

                    if (pluginManager != null) {
                        pluginSuppliers.add(
                                () ->
                                        Iterators.transform(
                                                pluginManager.load(FileSystemPlugin.class),
                                                PluginFileSystemWrapper::of));
                    }

                    final List<FileSystemPlugin> fileSystemFactories =
                            loadFileSystemPlugins(pluginSuppliers);

                    // cache all filesystem plugin
                    for (FileSystemPlugin plugin : fileSystemFactories) {
                        String scheme = plugin.getScheme();

                        FS_PLUGINS.put(scheme, plugin);
                    }
                });
    }

    /**
     * Returns a reference to the {@link FileSystem} instance for accessing the file system
     * identified by the given {@link URI}.
     *
     * @param uri the {@link URI} identifying the file system
     * @return a reference to the {@link FileSystem} instance for accessing the file system
     *     identified by the given {@link URI}.
     * @throws IOException thrown if a reference to the file system instance could not be obtained
     */
    public static FileSystem get(URI uri) throws IOException {
        return FileSystemSafetyNet.wrapWithSafetyNetWhenActivated(getUnguardedFileSystem(uri));
    }

    public static FileSystem getUnguardedFileSystem(final URI fsUri) throws IOException {
        checkNotNull(fsUri, "file system URI");
        return inLock(
                LOCK,
                () -> {
                    final URI uri;

                    if (fsUri.getScheme() != null) {
                        uri = fsUri;
                    } else {
                        // use local FileSystem urI as default
                        final URI defaultUri = LocalFileSystem.getLocalFsURI();
                        URI rewrittenUri = null;

                        try {
                            rewrittenUri =
                                    new URI(
                                            defaultUri.getScheme(),
                                            null,
                                            defaultUri.getHost(),
                                            defaultUri.getPort(),
                                            fsUri.getPath(),
                                            null,
                                            null);
                        } catch (URISyntaxException e) {
                            // for local URIs, we make one more try to repair the path by making it
                            // absolute
                            if (defaultUri.getScheme().equals("file")) {
                                try {
                                    rewrittenUri =
                                            new URI(
                                                    "file",
                                                    null,
                                                    new FsPath(
                                                                    new File(fsUri.getPath())
                                                                            .getAbsolutePath())
                                                            .toUri()
                                                            .getPath(),
                                                    null);
                                } catch (URISyntaxException ignored) {
                                    // could not help it...
                                }
                            }
                        }

                        if (rewrittenUri != null) {
                            uri = rewrittenUri;
                        } else {
                            throw new IOException(
                                    "The file system URI '"
                                            + fsUri
                                            + "' declares no scheme and cannot be interpreted relative to the default file system URI ("
                                            + defaultUri
                                            + ").");
                        }
                    }

                    // print a helpful pointer for malformed local URIs (happens a lot to new users)
                    if (uri.getScheme().equals("file")
                            && uri.getAuthority() != null
                            && !uri.getAuthority().isEmpty()) {
                        String supposedUri = "file:///" + uri.getAuthority() + uri.getPath();

                        throw new IOException(
                                "Found local file path with authority '"
                                        + uri.getAuthority()
                                        + "' in path '"
                                        + uri
                                        + "'. Hint: Did you forget a slash? (correct path would be '"
                                        + supposedUri
                                        + "')");
                    }

                    final FSKey key = new FSKey(uri.getScheme(), uri.getAuthority());

                    // See if there is a file system object in the cache
                    {
                        FileSystem cached = CACHE.get(key);
                        if (cached != null) {
                            return cached;
                        }
                    }

                    // this "default" initialization makes sure that the FileSystem class works
                    // even when not configured with an explicit Fluss configuration
                    if (FS_PLUGINS.isEmpty()) {
                        initializeWithoutPlugins(new Configuration());
                    }

                    // Try to create a new file system
                    final FileSystem fs;
                    final FileSystemPlugin fileSystemPlugin = FS_PLUGINS.get(uri.getScheme());

                    if (fileSystemPlugin != null) {
                        ClassLoader classLoader = fileSystemPlugin.getClassLoader();
                        try (TemporaryClassLoaderContext ignored =
                                TemporaryClassLoaderContext.of(classLoader)) {
                            fs = fileSystemPlugin.create(uri, configuration);
                        }
                    } else {
                        if (DIRECTLY_SUPPORTED_FILESYSTEM.containsKey(uri.getScheme())) {
                            final Collection<String> plugins =
                                    DIRECTLY_SUPPORTED_FILESYSTEM.get(uri.getScheme());
                            // todo: may need to add message like flink
                            // "See
                            // https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/plugins/ "
                            // for more information.",
                            throw new UnsupportedFileSystemSchemeException(
                                    String.format(
                                            "Could not find a file system implementation for scheme '%s'. File system schemes "
                                                    + "are supported by Fluss through the following plugin(s): %s. "
                                                    + "No file system to support this scheme could be loaded. Please ensure that each plugin is "
                                                    + "configured properly and resides within its own subfolder in the plugins directory. ",
                                            uri.getScheme(), String.join(", ", plugins)));
                        } else {
                            throw new UnsupportedFileSystemSchemeException(
                                    "Could not find a file system implementation for scheme '"
                                            + uri.getScheme()
                                            + "'. The scheme is not directly supported by Fluss.");
                        }
                    }
                    CACHE.put(key, fs);
                    return fs;
                });
    }

    /**
     * Obtain security token to access the files.
     *
     * @return a map contains the necessary info about the security token.
     */
    public abstract ObtainedSecurityToken obtainSecurityToken() throws IOException;

    /**
     * Returns a URI whose scheme and authority identify this file system.
     *
     * @return a URI whose scheme and authority identify this file system
     */
    public abstract URI getUri();

    /**
     * Return a file status object that represents the path.
     *
     * @param f The path we want information from
     * @return a FileStatus object
     * @throws FileNotFoundException when the path does not exist; IOException see specific
     *     implementation
     */
    public abstract FileStatus getFileStatus(FsPath f) throws IOException;

    /**
     * Opens an FSDataInputStream at the indicated Path.
     *
     * @param f the file to open
     */
    public abstract FSDataInputStream open(FsPath f) throws IOException;

    /**
     * List the statuses of the files/directories in the given path if the path is a directory.
     *
     * @param f given path
     * @return the statuses of the files/directories in the given path
     * @throws IOException
     */
    public abstract FileStatus[] listStatus(FsPath f) throws IOException;

    /**
     * Check if exists.
     *
     * @param f source file
     */
    public boolean exists(final FsPath f) throws IOException {
        try {
            return (getFileStatus(f) != null);
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    /**
     * Delete a file.
     *
     * @param f the path to delete
     * @param recursive if path is a directory and set to <code>true</code>, the directory is
     *     deleted else throws an exception. In case of a file the recursive can be set to either
     *     <code>true</code> or <code>false</code>
     * @return <code>true</code> if delete is successful, <code>false</code> otherwise
     * @throws IOException
     */
    public abstract boolean delete(FsPath f, boolean recursive) throws IOException;

    /**
     * Make the given file and all non-existent parents into directories. Has the semantics of Unix
     * 'mkdir -p'. Existence of the directory hierarchy is not an error.
     *
     * @param f the directory/directories to be created
     * @return <code>true</code> if at least one new directory has been created, <code>false</code>
     *     otherwise
     * @throws IOException thrown if an I/O error occurs while creating the directory
     */
    public abstract boolean mkdirs(FsPath f) throws IOException;

    /**
     * Opens an FSDataOutputStream to a new file at the given path.
     *
     * <p>If the file already exists, the behavior depends on the given {@code WriteMode}. If the
     * mode is set to {@link WriteMode#NO_OVERWRITE}, then this method fails with an exception.
     *
     * @param f The file path to write to
     * @param overwriteMode The action to take if a file or directory already exists at the given
     *     path.
     * @return The stream to the new file at the target path.
     * @throws IOException Thrown, if the stream could not be opened because of an I/O, or because a
     *     file already exists at that path and the write mode indicates to not overwrite the file.
     */
    public abstract FSDataOutputStream create(FsPath f, WriteMode overwriteMode) throws IOException;

    /**
     * Renames the file/directory src to dst.
     *
     * @param src the file/directory to rename
     * @param dst the new name of the file/directory
     * @return <code>true</code> if the renaming was successful, <code>false</code> otherwise
     * @throws IOException
     */
    public abstract boolean rename(FsPath src, FsPath dst) throws IOException;

    // ------------------------------------------------------------------------

    /**
     * Loads the plugins for the file systems directly supported by Fluss. Aside from the {@link
     * LocalFileSystem}, these file systems are loaded via Java's service framework.
     *
     * @return A map from the file system scheme to corresponding file system plugin.
     */
    private static List<FileSystemPlugin> loadFileSystemPlugins(
            Collection<Supplier<Iterator<FileSystemPlugin>>> pluginIteratorsSuppliers) {

        final ArrayList<FileSystemPlugin> list = new ArrayList<>();

        // by default, we always have the local file system plugin
        list.add(new LocalFileSystemPlugin());

        LOG.debug("Loading extension file systems via services");

        for (Supplier<Iterator<FileSystemPlugin>> pluginIteratorsSupplier :
                pluginIteratorsSuppliers) {
            try {
                addAllPluginsToList(pluginIteratorsSupplier.get(), list);
            } catch (Throwable t) {
                // catching Throwable here to handle various forms of class loading
                // and initialization errors
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                LOG.error("Failed to load additional file systems via services", t);
            }
        }

        return Collections.unmodifiableList(list);
    }

    private static void addAllPluginsToList(
            Iterator<FileSystemPlugin> iter, List<FileSystemPlugin> list) {
        // we explicitly use an iterator here (rather than for-each) because that way
        // we can catch errors in individual service instantiations

        while (iter.hasNext()) {
            try {
                FileSystemPlugin plugin = iter.next();
                list.add(plugin);
                LOG.debug(
                        "Added file system {}:{}",
                        plugin.getScheme(),
                        plugin.getClass().getSimpleName());
            } catch (Throwable t) {
                // catching Throwable here to handle various forms of class loading
                // and initialization errors
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                LOG.error("Failed to load a file system via services", t);
            }
        }
    }

    // ------------------------------------------------------------------------

    /** An identifier of a file system, via its scheme and its authority. */
    private static final class FSKey {

        /** The scheme of the file system. */
        private final String scheme;

        /** The authority of the file system. */
        @Nullable private final String authority;

        /**
         * Creates a file system key from a given scheme and an authority.
         *
         * @param scheme The scheme of the file system
         * @param authority The authority of the file system
         */
        public FSKey(String scheme, @Nullable String authority) {
            this.scheme = checkNotNull(scheme, "scheme");
            this.authority = authority;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            } else if (obj != null && obj.getClass() == FSKey.class) {
                final FSKey that = (FSKey) obj;
                return this.scheme.equals(that.scheme)
                        && (this.authority == null
                                ? that.authority == null
                                : (that.authority != null
                                        && this.authority.equals(that.authority)));
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return 31 * scheme.hashCode() + (authority == null ? 17 : authority.hashCode());
        }

        @Override
        public String toString() {
            return scheme + "://" + (authority != null ? authority : "");
        }
    }
}
