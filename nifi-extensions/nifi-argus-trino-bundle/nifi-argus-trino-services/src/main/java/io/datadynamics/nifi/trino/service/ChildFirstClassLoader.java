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
package io.datadynamics.nifi.trino.service;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * 외부 JDBC 드라이버 jar 를 위한 child-first 클래스로더.
 * <p>
 * NiFi 인스턴스 클래스로더(dynamicallyModifiesClasspath)는 parent-first 라 서비스 NAR 에 번들된 {@code io.trino.jdbc.*}가
 * 외부 jar 의 같은 패키지 클래스를 가린다. 그래서 외부 jar 를 먼저 찾고, JDK·NiFi API·SLF4J 만 부모에서 먼저 찾는다.
 */
final class ChildFirstClassLoader extends URLClassLoader {

    private static final String[] PARENT_FIRST = {"java.", "javax.", "jdk.", "sun.", "com.sun.", "org.apache.nifi.", "org.slf4j."};

    static {
        ClassLoader.registerAsParallelCapable();
    }

    private volatile boolean closed;

    ChildFirstClassLoader(final URL[] urls, final ClassLoader parent) {
        super("trino-external-driver", urls, parent);
    }

    static boolean isParentFirst(final String name) {
        for (final String prefix : PARENT_FIRST) {
            if (name.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> loaded = findLoadedClass(name);
            if (loaded == null) {
                if (isParentFirst(name)) {
                    loaded = super.loadClass(name, false);
                } else {
                    try {
                        loaded = findClass(name);
                    } catch (final ClassNotFoundException e) {
                        loaded = getParent().loadClass(name);
                    }
                }
            }
            if (resolve) {
                resolveClass(loaded);
            }
            return loaded;
        }
    }

    @Override
    public URL getResource(final String name) {
        if (!isParentFirst(name.replace('/', '.'))) {
            final URL local = findResource(name);
            if (local != null) {
                return local;
            }
        }
        return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(final String name) throws IOException {
        final List<URL> urls = new ArrayList<>(Collections.list(findResources(name)));
        final ClassLoader parent = getParent();
        if (parent != null) {
            urls.addAll(Collections.list(parent.getResources(name)));
        }
        return Collections.enumeration(urls);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        super.close();
    }

    boolean isClosed() {
        return closed;
    }
}
