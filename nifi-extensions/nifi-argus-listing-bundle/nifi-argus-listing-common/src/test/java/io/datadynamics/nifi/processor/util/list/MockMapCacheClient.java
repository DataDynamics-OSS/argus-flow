/*
 * Copyright 2026 Data Dynamics Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.processor.util.list;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 테스트용 메모리 DistributedMapCacheClient. 키는 UTF-8 문자열로, 값은 직렬화한 바이트로 보관한다.
 */
class MockMapCacheClient extends AbstractControllerService implements DistributedMapCacheClient {

    final Map<String, byte[]> entries = new ConcurrentHashMap<>();

    private static <T> byte[] serialize(final T value, final Serializer<T> serializer) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.serialize(value, out);
        return out.toByteArray();
    }

    private static <K> String key(final K key, final Serializer<K> serializer) throws IOException {
        return new String(serialize(key, serializer), StandardCharsets.UTF_8);
    }

    @Override
    public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        return entries.putIfAbsent(key(key, keySerializer), serialize(value, valueSerializer)) == null;
    }

    @Override
    public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer,
                                      final Deserializer<V> valueDeserializer) throws IOException {
        final byte[] previous = entries.putIfAbsent(key(key, keySerializer), serialize(value, valueSerializer));
        return previous == null ? null : valueDeserializer.deserialize(previous);
    }

    @Override
    public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
        return entries.containsKey(key(key, keySerializer));
    }

    @Override
    public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        entries.put(key(key, keySerializer), serialize(value, valueSerializer));
    }

    @Override
    public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
        final byte[] value = entries.get(key(key, keySerializer));
        return value == null ? null : valueDeserializer.deserialize(value);
    }

    @Override
    public void close() {
    }

    @Override
    public <K> boolean remove(final K key, final Serializer<K> serializer) throws IOException {
        return entries.remove(key(key, serializer)) != null;
    }
}
