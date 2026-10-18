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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code jdbc:trino://host[:port][/catalog[/schema]][?k=v&...]} 형식의 URL 파싱·조립.
 * <p>
 * catalog/schema 는 URL 경로로만 지정한다(문서 기준). 포트를 생략하면 드라이버가 HTTP 80 / SSL=true 면 443 을 쓴다.
 */
public final class TrinoJdbcUrl {

    public static final String PREFIX = "jdbc:trino://";

    /** {@code jdbc:<sub>://} 접두사(계층형 URL) 또는 null(불투명 URL). */
    private final String prefix;
    /** 불투명 URL(예: jdbc:h2:mem:x) 원문. 계층형이면 null. */
    private final String opaque;
    private final String authority;
    private final String catalog;
    private final String schema;
    private final String rawQuery;
    private final Map<String, String> queryParameters;

    private TrinoJdbcUrl(final String prefix, final String opaque, final String authority, final String catalog, final String schema,
                         final String rawQuery, final Map<String, String> queryParameters) {
        this.prefix = prefix;
        this.opaque = opaque;
        this.authority = authority;
        this.catalog = catalog;
        this.schema = schema;
        this.rawQuery = rawQuery;
        this.queryParameters = queryParameters;
    }

    /**
     * 번들 Trino 드라이버용 엄격한 파싱({@code jdbc:trino://} 필수).
     *
     * @throws IllegalArgumentException 형식이 잘못된 경우(메시지는 사용자 표시용)
     */
    public static TrinoJdbcUrl parse(final String url) {
        return parse(url, true);
    }

    /**
     * @param trinoFormat true 면 {@code jdbc:trino://} 형식을 강제한다. false(외부 드라이버)면 {@code jdbc:} 접두사만 요구하고,
     *                    {@code jdbc:<sub>://host...} 형식이면 경로·쿼리를 해석하며 그 밖의 형식은 원문 그대로 둔다(쿼리 파라미터만 해석).
     * @throws IllegalArgumentException 형식이 잘못된 경우(메시지는 사용자 표시용)
     */
    public static TrinoJdbcUrl parse(final String url, final boolean trinoFormat) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("URL 이 비어 있다");
        }
        final String trimmed = url.trim();
        if (trinoFormat && !trimmed.startsWith(PREFIX)) {
            throw new IllegalArgumentException("URL 은 " + PREFIX + " 로 시작해야 한다");
        }
        if (!trimmed.startsWith("jdbc:")) {
            throw new IllegalArgumentException("URL 은 jdbc: 로 시작해야 한다");
        }
        final int schemeEnd = trimmed.indexOf("://");
        if (schemeEnd < 0 || trimmed.substring("jdbc:".length(), schemeEnd).contains(":")) {
            if (trinoFormat) {
                throw new IllegalArgumentException("URL 은 " + PREFIX + " 로 시작해야 한다");
            }
            final int q = trimmed.indexOf('?');
            return new TrinoJdbcUrl(null, trimmed, null, null, null, null,
                    q < 0 ? Map.of() : parseQuery(trimmed.substring(q + 1)));
        }
        final String prefix = trimmed.substring(0, schemeEnd + 3);
        final URI uri;
        try {
            uri = new URI(trimmed.substring("jdbc:".length()));
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("URL 형식 오류: " + e.getMessage());
        }
        if (uri.getHost() == null || uri.getHost().isEmpty()) {
            throw new IllegalArgumentException("URL 에 호스트가 없다");
        }
        if (uri.getRawUserInfo() != null) {
            throw new IllegalArgumentException("URL 에 사용자 정보(user@host)를 넣을 수 없다. User 프로퍼티를 사용");
        }
        if (uri.getRawFragment() != null) {
            throw new IllegalArgumentException("URL 에 fragment(#)를 넣을 수 없다");
        }

        String catalog = null;
        String schema = null;
        final String path = uri.getPath();
        if (path != null && !path.isEmpty() && !path.equals("/")) {
            final String trimmedPath = path.endsWith("/") ? path.substring(1, path.length() - 1) : path.substring(1);
            final String[] parts = trimmedPath.split("/", -1);
            final List<String> segments = new ArrayList<>();
            for (final String part : parts) {
                segments.add(part);
            }
            if (segments.size() > 2 || segments.stream().anyMatch(String::isEmpty)) {
                throw new IllegalArgumentException("URL 경로는 /catalog 또는 /catalog/schema 형식이어야 한다");
            }
            catalog = segments.get(0);
            schema = segments.size() > 1 ? segments.get(1) : null;
        }

        final String rawQuery = uri.getRawQuery();
        final Map<String, String> params = rawQuery == null || rawQuery.isEmpty() ? Map.of() : parseQuery(rawQuery);

        final String authority = uri.getPort() < 0 ? uri.getHost() : uri.getHost() + ":" + uri.getPort();
        return new TrinoJdbcUrl(prefix, null, authority, catalog, schema, rawQuery, params);
    }

    private static Map<String, String> parseQuery(final String rawQuery) {
        final Map<String, String> params = new LinkedHashMap<>();
        for (final String pair : rawQuery.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            final int eq = pair.indexOf('=');
            final String key = eq < 0 ? pair : pair.substring(0, eq);
            final String value = eq < 0 ? "" : pair.substring(eq + 1);
            if (params.put(key, value) != null) {
                throw new IllegalArgumentException("URL 쿼리 파라미터가 중복된다: " + key);
            }
        }
        return Collections.unmodifiableMap(params);
    }

    /**
     * URL 경로에 catalog/schema 가 없을 때 기본값을 경로로 붙인다. URL 에 이미 있는 값이 우선한다.
     *
     * @throws IllegalArgumentException catalog 없이 schema 만 지정된 경우
     */
    public TrinoJdbcUrl withDefaults(final String defaultCatalog, final String defaultSchema) {
        if (opaque != null) {
            if (notBlank(defaultCatalog) || notBlank(defaultSchema)) {
                throw new IllegalArgumentException("jdbc:<sub>://host 형식이 아닌 URL 에는 Default Catalog/Schema 를 쓸 수 없다");
            }
            return this;
        }
        String newCatalog = catalog;
        String newSchema = schema;
        if (newCatalog == null && notBlank(defaultCatalog)) {
            newCatalog = defaultCatalog.trim();
        }
        if (newSchema == null && notBlank(defaultSchema)) {
            if (newCatalog == null) {
                throw new IllegalArgumentException("Default Schema 를 쓰려면 URL 경로나 Default Catalog 로 catalog 를 지정해야 한다");
            }
            newSchema = defaultSchema.trim();
        }
        for (final String segment : new String[]{newCatalog, newSchema}) {
            if (segment != null && (segment.contains("/") || segment.contains("?") || segment.contains("#"))) {
                throw new IllegalArgumentException("catalog/schema 에 '/', '?', '#' 를 쓸 수 없다: " + segment);
            }
        }
        return new TrinoJdbcUrl(prefix, null, authority, newCatalog, newSchema, rawQuery, queryParameters);
    }

    public String getCatalog() {
        return catalog;
    }

    public String getSchema() {
        return schema;
    }

    public Map<String, String> getQueryParameters() {
        return queryParameters;
    }

    /**
     * @return 드라이버에 넘길 URL 문자열
     */
    public String toUrl() {
        if (opaque != null) {
            return opaque;
        }
        final StringBuilder sb = new StringBuilder(prefix).append(authority);
        if (catalog != null) {
            sb.append('/').append(catalog);
            if (schema != null) {
                sb.append('/').append(schema);
            }
        }
        if (rawQuery != null && !rawQuery.isEmpty()) {
            sb.append('?').append(rawQuery);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toUrl();
    }

    private static boolean notBlank(final String value) {
        return value != null && !value.isBlank();
    }
}
