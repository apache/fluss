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

package org.apache.fluss.client.utils;

import org.apache.fluss.utils.CloseableIterator;

/**
 * A {@link CloseableIterator} that wraps another iterator with a single element prepended at the
 * head.
 *
 * <p>This iterator first returns the single element, then delegates to the inner iterator for
 * subsequent elements. It can be reused by calling {@link #set(Object, CloseableIterator)} to
 * replace the head element and inner iterator.
 */
public class SingleElementHeadIterator<T> implements CloseableIterator<T> {
    private T singleElement;

    private CloseableIterator<T> inner;

    private boolean singleElementReturned;

    public SingleElementHeadIterator(T element, CloseableIterator<T> inner) {
        this.singleElement = element;
        this.inner = inner;
        this.singleElementReturned = false;
    }

    public static <T> SingleElementHeadIterator<T> addElementToHead(
            T firstElement, CloseableIterator<T> originElementIterator) {
        if (originElementIterator instanceof SingleElementHeadIterator) {
            SingleElementHeadIterator<T> singleElementHeadIterator =
                    (SingleElementHeadIterator<T>) originElementIterator;
            singleElementHeadIterator.set(firstElement, singleElementHeadIterator.inner);
            return singleElementHeadIterator;
        } else {
            return new SingleElementHeadIterator<>(firstElement, originElementIterator);
        }
    }

    public void set(T element, CloseableIterator<T> inner) {
        this.singleElement = element;
        this.inner = inner;
        this.singleElementReturned = false;
    }

    @Override
    public boolean hasNext() {
        return !singleElementReturned || inner.hasNext();
    }

    @Override
    public T next() {
        if (singleElementReturned) {
            return inner.next();
        }
        singleElementReturned = true;
        return singleElement;
    }

    public T peek() {
        if (singleElementReturned) {
            this.singleElement = inner.next();
            this.singleElementReturned = false;
            return this.singleElement;
        }
        return singleElement;
    }

    @Override
    public void close() {
        inner.close();
    }
}
