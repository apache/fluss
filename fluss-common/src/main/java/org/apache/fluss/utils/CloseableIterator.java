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

package org.apache.fluss.utils;

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterators that need to be closed in order to release resources should implement this interface.
 *
 * <p>Warning: before implementing this interface, consider if there are better options. The chance
 * of misuse is a bit high since people are used to iterating without closing.
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {

    void close();

    static <R> CloseableIterator<R> wrap(Iterator<R> inner) {
        return new CloseableIterator<R>() {
            @Override
            public void close() {}

            @Override
            public boolean hasNext() {
                return inner.hasNext();
            }

            @Override
            public R next() {
                return inner.next();
            }

            @Override
            public void remove() {
                inner.remove();
            }
        };
    }

    // TODO Test case
    static <R> CloseableIterator<R> concatenate(List<CloseableIterator<R>> inners) {
        return new CloseableIterator<R>() {
            Iterator<CloseableIterator<R>> iterator = inners.stream().iterator();
            CloseableIterator<R> current;

            @Override
            public void close() {
                if (current != null) {
                    current.close();
                }

                while (iterator.hasNext()) {
                    current = iterator.next();

                    if (current != null) {
                        current.close();
                    }
                }
            }

            @Override
            public boolean hasNext() {
                while (current == null || !current.hasNext()) {
                    if (!iterator.hasNext()) {
                        return false;
                    }

                    current = iterator.next();
                }
                return true;
            }

            @Override
            public R next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                return current.next();
            }
        };
    }

    static <R> CloseableIterator<R> emptyIterator() {
        return wrap(Collections.emptyIterator());
    }
}
