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

package com.alibaba.fluss.fs;

import com.alibaba.fluss.annotation.PublicStable;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Names a file or directory in a {@link FileSystem}. {@link FsPath} strings use slash as the
 * directory separator. A {@link FsPath} string is absolute if it begins with a slash.
 *
 * <p>Tailing slashes are removed from the path.
 *
 * @since 0.1
 */
@PublicStable
public class FsPath implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The directory separator, a slash. */
    public static final String SEPARATOR = "/";

    /** Character denoting the current directory. */
    public static final String CUR_DIR = ".";

    /** A pre-compiled regex/state-machine to match the windows drive pattern. */
    private static final Pattern WINDOWS_ROOT_DIR_REGEX = Pattern.compile("/\\p{Alpha}+:/");

    /** The internal representation of the path, a hierarchical URI. */
    private URI uri;

    /**
     * Constructs a path object from a given URI.
     *
     * @param uri the URI to construct the path object from
     */
    public FsPath(URI uri) {
        this.uri = uri;
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public FsPath(String parent, String child) {
        this(new FsPath(parent), new FsPath(child));
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public FsPath(FsPath parent, String child) {
        this(parent, new FsPath(child));
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public FsPath(String parent, FsPath child) {
        this(new FsPath(parent), child);
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public FsPath(FsPath parent, FsPath child) {
        // Add a slash to parent's path so resolution is compatible with URI's
        URI parentUri = parent.uri;
        final String parentPath = parentUri.getPath();
        if (!(parentPath.equals("/") || parentPath.equals(""))) {
            try {
                parentUri =
                        new URI(
                                parentUri.getScheme(),
                                parentUri.getAuthority(),
                                parentUri.getPath() + "/",
                                null,
                                null);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }

        if (child.uri.getPath().startsWith(FsPath.SEPARATOR)) {
            child =
                    new FsPath(
                            child.uri.getScheme(),
                            child.uri.getAuthority(),
                            child.uri.getPath().substring(1));
        }

        final URI resolved = parentUri.resolve(child.uri);
        initialize(resolved.getScheme(), resolved.getAuthority(), resolved.getPath());
    }

    /**
     * Checks if the provided path string is either null or has zero length and throws a {@link
     * IllegalArgumentException} if any of the two conditions apply.
     *
     * @param path the path string to be checked
     * @return The checked path.
     */
    private String checkPathArg(String path) {
        // disallow construction of a Path from an empty string
        if (path == null) {
            throw new IllegalArgumentException("Can not create a Path from a null string");
        }
        if (path.isEmpty()) {
            throw new IllegalArgumentException("Can not create a Path from an empty string");
        }
        return path;
    }

    /**
     * Construct a path from a String. Path strings are URIs, but with unescaped elements and some
     * additional normalization.
     *
     * @param pathString the string to construct a path from
     */
    public FsPath(String pathString) {
        pathString = checkPathArg(pathString);

        // We can't use 'new URI(String)' directly, since it assumes things are
        // escaped, which we don't require of Paths.

        // add a slash in front of paths with Windows drive letters
        if (hasWindowsDrive(pathString, false)) {
            pathString = "/" + pathString;
        }

        // parse uri components
        String scheme = null;
        String authority = null;

        int start = 0;

        // parse uri scheme, if any
        final int colon = pathString.indexOf(':');
        final int slash = pathString.indexOf('/');
        if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a
            // scheme
            scheme = pathString.substring(0, colon);
            start = colon + 1;
        }

        // parse uri authority, if any
        if (pathString.startsWith("//", start)
                && (pathString.length() - start > 2)) { // has authority
            final int nextSlash = pathString.indexOf('/', start + 2);
            final int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
            authority = pathString.substring(start + 2, authEnd);
            start = authEnd;
        }

        // uri path is the rest of the string -- query & fragment not supported
        final String path = pathString.substring(start);

        initialize(scheme, authority, path);
    }

    /**
     * Construct a Path from a scheme, an authority and a path string.
     *
     * @param scheme the scheme string
     * @param authority the authority string
     * @param path the path string
     */
    public FsPath(String scheme, String authority, String path) {
        path = checkPathArg(path);
        initialize(scheme, authority, path);
    }

    /**
     * Initializes a path object given the scheme, authority and path string.
     *
     * @param scheme the scheme string.
     * @param authority the authority string.
     * @param path the path string.
     */
    private void initialize(String scheme, String authority, String path) {
        try {
            this.uri = new URI(scheme, authority, normalizePath(path), null, null).normalize();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Normalizes a path string.
     *
     * @param path the path string to normalize
     * @return the normalized path string
     */
    private String normalizePath(String path) {
        // remove consecutive slashes & backslashes
        path = path.replace("\\", "/");
        path = path.replaceAll("/+", "/");

        // remove tailing separator
        if (path.endsWith(SEPARATOR)
                && !path.equals(SEPARATOR)
                && // UNIX root path
                !WINDOWS_ROOT_DIR_REGEX.matcher(path).matches()) { // Windows root path)

            // remove tailing slash
            path = path.substring(0, path.length() - SEPARATOR.length());
        }

        return path;
    }

    /**
     * Converts the path object to a {@link URI}.
     *
     * @return the {@link URI} object converted from the path object
     */
    public URI toUri() {
        return uri;
    }

    /**
     * Returns the FileSystem that owns this Path.
     *
     * @return the FileSystem that owns this Path
     * @throws IOException thrown if the file system could not be retrieved
     */
    public FileSystem getFileSystem() throws IOException {
        return FileSystem.get(this.toUri());
    }

    /**
     * Returns the final component of this path, i.e., everything that follows the last separator.
     *
     * @return the final component of the path
     */
    public String getName() {
        final String path = uri.getPath();
        final int slash = path.lastIndexOf(SEPARATOR);
        return path.substring(slash + 1);
    }

    /**
     * Return full path.
     *
     * @return full path
     */
    public String getPath() {
        return uri.getPath();
    }

    /**
     * Returns the parent of a path, i.e., everything that precedes the last separator or <code>null
     * </code> if at root.
     *
     * @return the parent of a path or <code>null</code> if at root.
     */
    public FsPath getParent() {
        final String path = uri.getPath();
        final int lastSlash = path.lastIndexOf('/');
        final int start = hasWindowsDrive(path, true) ? 3 : 0;
        if ((path.length() == start)
                || // empty path
                (lastSlash == start && path.length() == start + 1)) { // at root
            return null;
        }
        String parent;
        if (lastSlash == -1) {
            parent = CUR_DIR;
        } else {
            final int end = hasWindowsDrive(path, true) ? 3 : 0;
            parent = path.substring(0, lastSlash == end ? end + 1 : lastSlash);
        }
        return new FsPath(uri.getScheme(), uri.getAuthority(), parent);
    }

    @Override
    public String toString() {
        // we can't use uri.toString(), which escapes everything, because we want
        // illegal characters unescaped in the string, for glob processing, etc.
        final StringBuilder buffer = new StringBuilder();
        if (uri.getScheme() != null) {
            buffer.append(uri.getScheme());
            buffer.append(":");
        }
        if (uri.getAuthority() != null) {
            buffer.append("//");
            buffer.append(uri.getAuthority());
        }
        if (uri.getPath() != null) {
            String path = uri.getPath();
            if (path.indexOf('/') == 0
                    && hasWindowsDrive(path, true)
                    && // has windows drive
                    uri.getScheme() == null
                    && // but no scheme
                    uri.getAuthority() == null) { // or authority
                path = path.substring(1); // remove slash before drive
            }
            buffer.append(path);
        }
        return buffer.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FsPath)) {
            return false;
        }
        FsPath that = (FsPath) o;
        return this.uri.equals(that.uri);
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Checks if the provided path string contains a windows drive letter.
     *
     * @param path the path to check
     * @param slashed true to indicate the first character of the string is a slash, false otherwise
     * @return <code>true</code> if the path string contains a windows drive letter, false otherwise
     */
    private boolean hasWindowsDrive(String path, boolean slashed) {
        final int start = slashed ? 1 : 0;
        return path.length() >= start + 2
                && (!slashed || path.charAt(0) == '/')
                && path.charAt(start + 1) == ':'
                && ((path.charAt(start) >= 'A' && path.charAt(start) <= 'Z')
                        || (path.charAt(start) >= 'a' && path.charAt(start) <= 'z'));
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Creates a path for the given local file.
     *
     * <p>This method is useful to make sure the path creation for local files works seamlessly
     * across different operating systems. Especially Windows has slightly different rules for
     * slashes between schema and a local file path, making it sometimes tricky to produce
     * cross-platform URIs for local files.
     *
     * @param file The file that the path should represent.
     * @return A path representing the local file URI of the given file.
     */
    public static FsPath fromLocalFile(File file) {
        return new FsPath(file.toURI());
    }
}
