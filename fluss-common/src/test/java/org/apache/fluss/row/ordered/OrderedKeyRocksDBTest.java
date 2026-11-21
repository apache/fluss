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

package org.apache.fluss.row.ordered;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.OrderedKeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksIterator;
import io.github.fluss_contrib.rocksdb.Slice;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies prefix and bounded seeks with RocksDB's default bytewise comparator. */
class OrderedKeyRocksDBTest {

    @TempDir private Path directory;

    @Test
    void testPrefixAndRangeSeeks() throws Exception {
        RocksDB.loadLibrary();
        RowType schema =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.INT()},
                        new String[] {"group", "time", "id"});
        OrderedKeyEncoder full = OrderedKeyEncoder.createKeyEncoder(schema, schema.getFieldNames());
        OrderedKeyEncoder prefix =
                OrderedKeyEncoder.createKeyEncoder(schema, Collections.singletonList("group"));
        OrderedKeyEncoder endpoint =
                OrderedKeyEncoder.createKeyEncoder(schema, Arrays.asList("group", "time"));
        BinaryString group = BinaryString.fromString("12345678");
        byte[] p = prefix.encodeKey(GenericRow.of(group, 0L, 0));
        byte[] lower = endpoint.encodeKey(GenericRow.of(group, 255L, 0));
        byte[] upper = endpoint.encodeKey(GenericRow.of(group, 256L, 0));
        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB db = RocksDB.open(options, directory.resolve("keys").toString())) {
            for (long time : new long[] {257, 256, 255, 254}) {
                for (int id : new int[] {1, 0, -1}) {
                    db.put(
                            full.encodeKey(GenericRow.of(group, time, id)),
                            new byte[] {(byte) (time - 254)});
                }
            }
            db.put(
                    full.encodeKey(GenericRow.of(BinaryString.fromString("123456789"), 255L, 0)),
                    new byte[] {99});
            try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                db.flush(flush);
            }
            assertThat(read(db, p, OrderedKeyCodec.prefixUpperBound(p)))
                    .containsExactly(0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3);
            assertThat(read(db, lower, upper)).containsExactly(1, 1, 1);
            assertThat(read(db, lower, OrderedKeyCodec.prefixUpperBound(upper)))
                    .containsExactly(1, 1, 1, 2, 2, 2);
            assertThat(read(db, OrderedKeyCodec.prefixUpperBound(lower), upper)).isEmpty();
            assertThat(
                            read(
                                    db,
                                    OrderedKeyCodec.prefixUpperBound(lower),
                                    OrderedKeyCodec.prefixUpperBound(upper)))
                    .containsExactly(2, 2, 2);
            assertThat(read(db, lower, OrderedKeyCodec.prefixUpperBound(lower)))
                    .containsExactly(1, 1, 1);
            assertThat(read(db, lower, OrderedKeyCodec.prefixUpperBound(p)))
                    .containsExactly(1, 1, 1, 2, 2, 2, 3, 3, 3);
        }
    }

    private static List<Integer> read(RocksDB db, byte[] lower, byte[] upper) throws Exception {
        List<Integer> values = new ArrayList<>();
        try (Slice bound = new Slice(upper);
                ReadOptions options = new ReadOptions().setIterateUpperBound(bound);
                RocksIterator iterator = db.newIterator(options)) {
            iterator.seek(lower);
            while (iterator.isValid()) {
                values.add((int) iterator.value()[0]);
                iterator.next();
            }
            iterator.status();
        }
        return values;
    }
}
