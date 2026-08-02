/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-extension-utils/nifi-hadoop-utils/src/main/java/org/apache/nifi/processors/hadoop/ExtendedConfiguration.java
 */
package io.datadynamics.nifi.processors.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.logging.ComponentLog;
import org.slf4j.Logger;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.BiConsumer;

/**
 * 클래스를 찾지 못한 경우에도 이를 캐싱하지 않도록 Hadoop Configuration을 확장한 클래스.
 * 사용자가 클래스패스에 추가 JAR을 나중에 추가할 수 있으므로, 이전에 찾지 못했던 클래스라도
 * JVM을 재시작하지 않고 이후 다시 로드를 시도할 수 있어야 한다. 이를 위해 클래스를 찾지
 * 못한 경우 결과를 캐시에 저장하지 않고 매번 다시 로드를 시도하도록 재정의한다.
 * <p>
 * Configuration의 원본 getClassByNameOrNull() 구현을 참고하여 작성되었다.
 */
public class ExtendedConfiguration extends Configuration {

    // 클래스 로드 실패 시 오류를 기록하기 위한 콜백. 생성자에서 전달받은 로거의 error 메서드를 참조한다.
    private final BiConsumer<String, Throwable> loggerMethod;
    // 클래스로더별로 이름 -> 클래스(약한 참조) 매핑을 캐시한다. 클래스로더가 GC될 수 있도록
    // WeakHashMap을 사용하여 클래스로더 자체에 대한 강한 참조를 갖지 않도록 한다.
    private final Map<ClassLoader, Map<String, WeakReference<Class<?>>>> CACHE_CLASSES = new WeakHashMap<>();

    public ExtendedConfiguration(final Logger logger) {
        this.loggerMethod = logger::error;
    }

    public ExtendedConfiguration(final ComponentLog logger) {
        this.loggerMethod = logger::error;
    }

    /**
     * 지정된 이름의 클래스를 로드한다. 원본 Hadoop Configuration 구현과 달리, 클래스를
     * 찾지 못했을 때의 결과(null)는 캐시에 저장하지 않는다. 따라서 클래스패스에 새로운 JAR이
     * 추가되어 이후 해당 클래스를 찾을 수 있게 되면 다음 호출에서 정상적으로 로드된다.
     */
    @Override
    public Class<?> getClassByNameOrNull(String name) {
        final ClassLoader classLoader = getClassLoader();

        Map<String, WeakReference<Class<?>>> map;
        synchronized (CACHE_CLASSES) {
            map = CACHE_CLASSES.get(classLoader);
            if (map == null) {
                map = Collections.synchronizedMap(new WeakHashMap<>());
                CACHE_CLASSES.put(classLoader, map);
            }
        }

        Class<?> clazz = null;
        WeakReference<Class<?>> ref = map.get(name);
        if (ref != null) {
            clazz = ref.get();
        }

        if (clazz == null) {
            try {
                clazz = Class.forName(name, true, classLoader);
            } catch (ClassNotFoundException e) {
                // 클래스를 찾지 못한 경우 캐시에 저장하지 않고 오류만 기록한다.
                loggerMethod.accept(e.getMessage(), e);
                return null;
            }
            // 두 스레드가 동시에 이 지점에 도달해 경쟁 상태가 발생할 수 있지만,
            // 결국 동일한 클래스를 캐시에 저장하게 되므로 문제되지 않는다.
            map.put(name, new WeakReference<>(clazz));
            return clazz;
        } else {
            // 캐시에서 클래스를 찾은 경우
            return clazz;
        }
    }

}
