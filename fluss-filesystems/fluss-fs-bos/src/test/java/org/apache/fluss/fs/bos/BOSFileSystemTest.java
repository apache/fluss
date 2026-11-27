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

package org.apache.fluss.fs.bos;

import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem.WriteMode;
import org.apache.fluss.fs.FsPath;

import com.baidubce.services.bos.FakeBosClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BOSFileSystem} against an in-memory BOS client. */
class BOSFileSystemTest {
    private static final String BUCKET = "test-bucket";

    private FakeBosClient client;
    private BOSFileSystem fs;

    @BeforeEach
    void setUp() {
        client = new FakeBosClient();
        fs = new BOSFileSystem(BUCKET, Collections.emptyMap(), false, client);
    }

    private static FsPath p(String path) {
        return new FsPath("bos://" + BUCKET + path);
    }

    private void put(String key, int len) {
        client.objects.put(key, new byte[len]);
    }

    @Test
    void directoriesAreVirtualPrefixes() throws IOException {
        put("a/b/file", 3);
        assertThat(fs.getFileStatus(p("/a")).isDir()).isTrue();
        assertThat(fs.getFileStatus(p("/a/b")).isDir()).isTrue();
        assertThat(fs.getFileStatus(p("/a/b/file")).isDir()).isFalse();
        assertThat(fs.getFileStatus(p("/a/b/file")).getLen()).isEqualTo(3);
        assertThat(fs.getFileStatus(p("/")).isDir()).isTrue();
        assertThatThrownBy(() -> fs.getFileStatus(p("/a/missing")))
                .isInstanceOf(FileNotFoundException.class);
        assertThat(fs.exists(p("/a/missing"))).isFalse();
    }

    @Test
    void pathOfAnotherBucketIsRejected() {
        assertThatThrownBy(() -> fs.getFileStatus(new FsPath("bos://other-bucket/x")))
                .isInstanceOf(IOException.class);
    }

    @Test
    void listStatusSeparatesFilesAndDirsAndHidesTheDirectoryMarker() throws IOException {
        put("d/", 0); // marker object
        put("d/f1", 1);
        put("d/f2", 2);
        put("d/sub/f3", 3);
        FileStatus[] st = fs.listStatus(p("/d"));
        Set<String> names =
                Arrays.stream(st)
                        .map(s -> s.getPath().getName() + (s.isDir() ? "/" : ""))
                        .collect(Collectors.toSet());
        assertThat(names).isEqualTo(new HashSet<>(Arrays.asList("f1", "f2", "sub/")));
        assertThat(fs.listStatus(p("/d/f1")).length).isEqualTo(1);
        assertThatThrownBy(() -> fs.listStatus(p("/nope")))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void listStatusFollowsPagination() throws IOException {
        for (int i = 0; i < 2500; i++) {
            put(String.format("big/f%05d", i), 1);
        }
        FileStatus[] st = fs.listStatus(p("/big"));
        assertThat(st.length).isEqualTo(2500);
        assertThat(Arrays.stream(st).map(s -> s.getPath().toString()).distinct().count())
                .isEqualTo(2500);
    }

    @Test
    void phantomEntriesFromMaxKeysOneListingsDoNotMakeAnEmptyDirectoryExist() throws IOException {
        // BOS quirk: list(maxKeys=1) keeps returning an already deleted object
        client.phantoms.add("gone/old-file");
        assertThat(fs.exists(p("/gone"))).isFalse();
        assertThatThrownBy(() -> fs.listStatus(p("/gone")))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void phantomEntriesAreConfirmedWithAHeadEvenForLargerPages() throws IOException {
        client.phantomMaxKeys = 1000; // BOS also returned deleted entries for larger pages
        client.phantoms.add("gone/old-file");
        assertThat(fs.exists(p("/gone"))).isFalse();
        put("gone2/real", 1);
        client.phantoms.add("gone2/old");
        assertThat(fs.exists(p("/gone2"))).isTrue();
    }

    @Test
    void aDeletedDirectoryMarkerStillListedDoesNotKeepTheDirectoryAlive() throws IOException {
        // observed on BOS: right after a recursive delete the list still returns the marker
        client.phantomMaxKeys = 1000;
        client.phantoms.add("gone/");
        client.phantoms.add("gone/file-0");
        assertThat(fs.exists(p("/gone"))).isFalse();
    }

    @Test
    void aDeletedSubDirectoryStillListedAsPrefixDoesNotKeepTheParentAlive() throws IOException {
        client.phantomMaxKeys = 1000;
        client.phantoms.add("parent/sub/file");
        assertThat(fs.exists(p("/parent"))).isFalse();
        put("parent/real", 1);
        assertThat(fs.exists(p("/parent"))).isTrue();
    }

    @Test
    void createRejectsExistingFilesWhenAskedTo() throws IOException {
        put("x/file", 1);
        assertThatThrownBy(() -> fs.create(p("/x/file"), WriteMode.NO_OVERWRITE))
                .isInstanceOf(FileAlreadyExistsException.class);
        try (FSDataOutputStream out = fs.create(p("/x/file"), WriteMode.OVERWRITE)) {
            out.write(new byte[] {1, 2});
        }
        assertThat(client.objects.get("x/file").length).isEqualTo(2);
        assertThatThrownBy(() -> fs.create(p("/"), WriteMode.OVERWRITE))
                .isInstanceOf(IOException.class);
    }

    @Test
    void deleteSemantics() throws IOException {
        put("t/a", 1);
        put("t/sub/b", 1);
        assertThat(fs.delete(p("/t/none"), true)).isFalse();
        assertThatThrownBy(() -> fs.delete(p("/t"), false)).isInstanceOf(IOException.class);
        assertThat(client.objects.containsKey("t/a")).isTrue();
        assertThat(fs.delete(p("/t/a"), false)).isTrue();
        assertThat(fs.delete(p("/t"), true)).isTrue();
        assertThat(client.objects.isEmpty()).isTrue();
        assertThat(fs.exists(p("/t"))).isFalse();
        assertThatThrownBy(() -> fs.delete(p("/"), true)).isInstanceOf(IOException.class);
    }

    @Test
    void recursiveDeleteHandlesMoreThanOnePage() throws IOException {
        for (int i = 0; i < 2300; i++) {
            put(String.format("many/f%05d", i), 1);
        }
        assertThat(fs.delete(p("/many"), true)).isTrue();
        assertThat(client.objects.isEmpty()).isTrue();
    }

    @Test
    void renameFileAndDirectory() throws IOException {
        put("src/a", 1);
        put("src/sub/b", 2);
        put("other", 3);
        assertThat(fs.rename(p("/missing"), p("/dst"))).isFalse();
        assertThat(fs.rename(p("/src"), p("/other"))).as("destination exists").isFalse();
        assertThat(fs.rename(p("/other"), p("/other2"))).isTrue();
        assertThat(client.objects.containsKey("other")).isFalse();
        assertThat(fs.rename(p("/src"), p("/dst"))).isTrue();
        assertThat(client.objects.keySet())
                .isEqualTo(new HashSet<>(Arrays.asList("dst/a", "dst/sub/b", "other2")));
        assertThat(fs.exists(p("/src"))).isFalse();
    }

    @Test
    void getUri() {
        assertThat(fs.getUri().toString()).isEqualTo("bos://" + BUCKET);
    }

    @Test
    void mkdirsWritesAMarkerSoThatAnEmptyDirectoryExists() throws IOException {
        assertThat(fs.mkdirs(p("/a/b"))).isTrue();
        assertThat(client.objects).containsOnlyKeys("a/b/");
        assertThat(fs.exists(p("/a/b"))).isTrue();
        assertThat(fs.getFileStatus(p("/a/b")).isDir()).isTrue();
        assertThat(fs.listStatus(p("/a/b"))).isEmpty();
        // idempotent: no second marker, no error
        assertThat(fs.mkdirs(p("/a/b"))).isTrue();
        assertThat(client.objects).containsOnlyKeys("a/b/");
        assertThat(fs.mkdirs(p("/"))).isTrue();
    }

    @Test
    void mkdirsOnAnExistingDirectoryWithFilesWritesNoMarker() throws IOException {
        put("d/f", 1);
        assertThat(fs.mkdirs(p("/d"))).isTrue();
        assertThat(client.objects).containsOnlyKeys("d/f");
    }

    @Test
    void mkdirsFailsWhenThePathOrAnAncestorIsAFile() {
        put("file", 1);
        put("dir/file", 1);
        assertThatThrownBy(() -> fs.mkdirs(p("/file")))
                .isInstanceOf(FileAlreadyExistsException.class);
        assertThatThrownBy(() -> fs.mkdirs(p("/file/sub")))
                .isInstanceOf(FileAlreadyExistsException.class);
        assertThatThrownBy(() -> fs.mkdirs(p("/dir/file/x/y")))
                .isInstanceOf(FileAlreadyExistsException.class);
        assertThat(client.objects).containsOnlyKeys("file", "dir/file");
    }

    @Test
    void anEmptyDirectoryCanBeDeletedWithoutRecursion() throws IOException {
        fs.mkdirs(p("/e"));
        assertThat(fs.delete(p("/e"), false)).isTrue();
        assertThat(client.objects).isEmpty();
        assertThat(fs.exists(p("/e"))).isFalse();
    }

    @Test
    void aNonEmptyDirectoryWithAMarkerStillNeedsRecursion() throws IOException {
        fs.mkdirs(p("/n"));
        put("n/f", 1);
        assertThatThrownBy(() -> fs.delete(p("/n"), false)).isInstanceOf(IOException.class);
        assertThat(fs.delete(p("/n"), true)).isTrue();
        assertThat(client.objects).isEmpty();
    }

    @Test
    void renamingADirectoryMovesItsMarker() throws IOException {
        fs.mkdirs(p("/src"));
        put("src/f", 1);
        assertThat(fs.rename(p("/src"), p("/dst"))).isTrue();
        assertThat(client.objects).containsOnlyKeys("dst/", "dst/f");
    }
}
