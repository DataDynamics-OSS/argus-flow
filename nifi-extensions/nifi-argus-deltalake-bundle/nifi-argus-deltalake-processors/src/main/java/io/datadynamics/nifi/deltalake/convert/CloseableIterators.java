/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.deltalake.convert;

import io.delta.kernel.utils.CloseableIterator;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/** Delta Kernel {@link CloseableIterator}로 인메모리 컬렉션을 감싸는 헬퍼. */
public final class CloseableIterators {

    private CloseableIterators() {
    }

    public static <T> CloseableIterator<T> singleton(final T element) {
        return new CloseableIterator<T>() {
            private boolean consumed = false;

            @Override
            public boolean hasNext() {
                return !consumed;
            }

            @Override
            public T next() {
                if (consumed) {
                    throw new NoSuchElementException();
                }
                consumed = true;
                return element;
            }

            @Override
            public void close() {
                // no-op
            }
        };
    }

    public static <T> CloseableIterator<T> fromList(final List<T> list) {
        final Iterator<T> delegate = list.iterator();
        return new CloseableIterator<T>() {
            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public T next() {
                return delegate.next();
            }

            @Override
            public void close() {
                // no-op
            }
        };
    }
}
