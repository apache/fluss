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

package com.alibaba.fluss.utils.log;

import com.alibaba.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.utils.log.FairBucketStatusMap}. */
final class FairBucketStatusMapTest {

    @Test
    void testSet() {
        FairBucketStatusMap<String> statuses = new FairBucketStatusMap<>();
        Map<TableBucket, String> map = createMap();
        statuses.set(map);

        Map<TableBucket, String> expected = new LinkedHashMap<>();
        expected.put(new TableBucket(15000L, 2), "t1 2");
        expected.put(new TableBucket(15000L, 0), "t1 0");
        expected.put(new TableBucket(15001L, 2), "t2 2");
        expected.put(new TableBucket(15001L, 1), "t2 1");
        expected.put(new TableBucket(15002L, 2), "t3 2");
        expected.put(new TableBucket(15002L, 3), "t3 3");
        checkStatus(statuses, expected);

        statuses.set(new LinkedHashMap<>());
        checkStatus(statuses, new LinkedHashMap<>());
    }

    @Test
    void testMoveToEnd() {
        FairBucketStatusMap<String> statuses = new FairBucketStatusMap<>();
        Map<TableBucket, String> map = createMap();
        statuses.set(map);

        statuses.moveToEnd(new TableBucket(15002L, 2));
        Map<TableBucket, String> expected = new LinkedHashMap<>();
        expected.put(new TableBucket(15000L, 2), "t1 2");
        expected.put(new TableBucket(15000L, 0), "t1 0");
        expected.put(new TableBucket(15001L, 2), "t2 2");
        expected.put(new TableBucket(15001L, 1), "t2 1");
        expected.put(new TableBucket(15002L, 2), "t3 2");
        expected.put(new TableBucket(15002L, 3), "t3 3");
        checkStatus(statuses, expected);

        statuses.moveToEnd(new TableBucket(15000L, 2));
        expected = new LinkedHashMap<>();
        expected.put(new TableBucket(15000L, 0), "t1 0");
        expected.put(new TableBucket(15001L, 2), "t2 2");
        expected.put(new TableBucket(15001L, 1), "t2 1");
        expected.put(new TableBucket(15002L, 2), "t3 2");
        expected.put(new TableBucket(15002L, 3), "t3 3");
        expected.put(new TableBucket(15000L, 2), "t1 2");
        checkStatus(statuses, expected);

        statuses.moveToEnd(new TableBucket(15000L, 2));
        checkStatus(statuses, expected);

        // bucket doesn't exist.
        statuses.moveToEnd(new TableBucket(15002L, 5));
        checkStatus(statuses, expected);

        // table doesn't exist.
        statuses.moveToEnd(new TableBucket(15003L, 0));
        checkStatus(statuses, expected);
    }

    @Test
    void testUpdateAndMoveToEnd() {
        FairBucketStatusMap<String> statuses = new FairBucketStatusMap<>();
        Map<TableBucket, String> map = createMap();
        statuses.set(map);

        statuses.updateAndMoveToEnd(new TableBucket(15000L, 0), "t1 0 updated");
        Map<TableBucket, String> expected = new LinkedHashMap<>();
        expected.put(new TableBucket(15000L, 2), "t1 2");
        expected.put(new TableBucket(15001L, 2), "t2 2");
        expected.put(new TableBucket(15001L, 1), "t2 1");
        expected.put(new TableBucket(15002L, 2), "t3 2");
        expected.put(new TableBucket(15002L, 3), "t3 3");
        expected.put(new TableBucket(15000L, 0), "t1 0 updated");
        checkStatus(statuses, expected);

        statuses.updateAndMoveToEnd(new TableBucket(15002L, 2), "t3 2 updated");
        expected = new LinkedHashMap<>();
        expected.put(new TableBucket(15000L, 2), "t1 2");
        expected.put(new TableBucket(15001L, 2), "t2 2");
        expected.put(new TableBucket(15001L, 1), "t2 1");
        expected.put(new TableBucket(15002L, 3), "t3 3");
        expected.put(new TableBucket(15000L, 0), "t1 0 updated");
        expected.put(new TableBucket(15002L, 2), "t3 2 updated");
        checkStatus(statuses, expected);

        // bucket doesn't exist.
        statuses.updateAndMoveToEnd(new TableBucket(15002L, 5), "t3 5 new");
        expected = new LinkedHashMap<>();
        expected.put(new TableBucket(15000L, 2), "t1 2");
        expected.put(new TableBucket(15001L, 2), "t2 2");
        expected.put(new TableBucket(15001L, 1), "t2 1");
        expected.put(new TableBucket(15002L, 3), "t3 3");
        expected.put(new TableBucket(15000L, 0), "t1 0 updated");
        expected.put(new TableBucket(15002L, 2), "t3 2 updated");
        expected.put(new TableBucket(15002L, 5), "t3 5 new");
        checkStatus(statuses, expected);

        // table doesn't exist.
        statuses.updateAndMoveToEnd(new TableBucket(15003L, 2), "t4 2 new");
        expected = new LinkedHashMap<>();
        expected.put(new TableBucket(15000L, 2), "t1 2");
        expected.put(new TableBucket(15001L, 2), "t2 2");
        expected.put(new TableBucket(15001L, 1), "t2 1");
        expected.put(new TableBucket(15002L, 3), "t3 3");
        expected.put(new TableBucket(15000L, 0), "t1 0 updated");
        expected.put(new TableBucket(15002L, 2), "t3 2 updated");
        expected.put(new TableBucket(15002L, 5), "t3 5 new");
        expected.put(new TableBucket(15003L, 2), "t4 2 new");
        checkStatus(statuses, expected);
    }

    @Test
    void testBucketValues() {
        FairBucketStatusMap<String> statuses = new FairBucketStatusMap<>();
        Map<TableBucket, String> map = createMap();
        statuses.set(map);

        List<String> expected = Arrays.asList("t1 2", "t1 0", "t2 2", "t2 1", "t3 2", "t3 3");
        assertThat(statuses.bucketStatusValues()).isEqualTo(expected);
    }

    @Test
    void testClear() {
        FairBucketStatusMap<String> statuses = new FairBucketStatusMap<>();
        Map<TableBucket, String> map = createMap();
        statuses.set(map);

        statuses.clear();
        checkStatus(statuses, new LinkedHashMap<>());
    }

    @Test
    void testRemove() {
        FairBucketStatusMap<String> statuses = new FairBucketStatusMap<>();
        Map<TableBucket, String> map = createMap();
        statuses.set(map);

        statuses.remove(new TableBucket(15000L, 2));
        Map<TableBucket, String> expected = new LinkedHashMap<>();
        expected.put(new TableBucket(15000L, 0), "t1 0");
        expected.put(new TableBucket(15001L, 2), "t2 2");
        expected.put(new TableBucket(15001L, 1), "t2 1");
        expected.put(new TableBucket(15002L, 2), "t3 2");
        expected.put(new TableBucket(15002L, 3), "t3 3");
        checkStatus(statuses, expected);

        statuses.remove(new TableBucket(15001L, 1));
        expected = new LinkedHashMap<>();
        expected.put(new TableBucket(15000L, 0), "t1 0");
        expected.put(new TableBucket(15001L, 2), "t2 2");
        expected.put(new TableBucket(15002L, 2), "t3 2");
        expected.put(new TableBucket(15002L, 3), "t3 3");
        checkStatus(statuses, expected);
    }

    private Map<TableBucket, String> createMap() {
        Map<TableBucket, String> map = new LinkedHashMap<>();
        map.put(new TableBucket(15000L, 2), "t1 2");
        map.put(new TableBucket(15001L, 2), "t2 2");
        map.put(new TableBucket(15001L, 1), "t2 1");
        map.put(new TableBucket(15002L, 2), "t3 2");
        map.put(new TableBucket(15000L, 0), "t1 0");
        map.put(new TableBucket(15002L, 3), "t3 3");
        return map;
    }

    private void checkStatus(FairBucketStatusMap<String> statuses, Map<TableBucket, String> map) {
        assertThat(statuses.bucketSet()).isEqualTo(map.keySet());
        assertThat(statuses.size()).isEqualTo(map.size());
        assertThat(statuses.bucketStatusMap()).isEqualTo(map);
    }
}
