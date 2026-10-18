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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TrinoJdbcUrlTest {

    @Test
    void parsesHostPortCatalogSchemaAndQuery() {
        final TrinoJdbcUrl url = TrinoJdbcUrl.parse("jdbc:trino://trino.example.com:8443/iceberg/sales?SSL=true&clientInfo=x");
        assertEquals("iceberg", url.getCatalog());
        assertEquals("sales", url.getSchema());
        assertEquals(Map.of("SSL", "true", "clientInfo", "x"), url.getQueryParameters());
        assertEquals("jdbc:trino://trino.example.com:8443/iceberg/sales?SSL=true&clientInfo=x", url.toUrl());
    }

    @Test
    void portIsOptional() {
        final TrinoJdbcUrl url = TrinoJdbcUrl.parse("jdbc:trino://trino.example.com");
        assertNull(url.getCatalog());
        assertEquals("jdbc:trino://trino.example.com", url.toUrl());
        assertEquals("jdbc:trino://trino.example.com/tpch", TrinoJdbcUrl.parse("jdbc:trino://trino.example.com/tpch/").toUrl());
    }

    @Test
    void defaultsAreAppendedAsPathOnlyWhenMissing() {
        assertEquals("jdbc:trino://h:8080/iceberg/raw",
                TrinoJdbcUrl.parse("jdbc:trino://h:8080").withDefaults("iceberg", "raw").toUrl());
        assertEquals("jdbc:trino://h:8080/hive/raw?SSL=true",
                TrinoJdbcUrl.parse("jdbc:trino://h:8080/hive?SSL=true").withDefaults("iceberg", "raw").toUrl());
        assertEquals("jdbc:trino://h:8080/hive/s1",
                TrinoJdbcUrl.parse("jdbc:trino://h:8080/hive/s1").withDefaults("iceberg", "raw").toUrl());
    }

    @Test
    void schemaWithoutCatalogIsRejected() {
        final TrinoJdbcUrl url = TrinoJdbcUrl.parse("jdbc:trino://h:8080");
        assertThrows(IllegalArgumentException.class, () -> url.withDefaults(null, "raw"));
    }

    @Test
    void malformedUrlsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> TrinoJdbcUrl.parse("jdbc:presto://h:8080"));
        assertThrows(IllegalArgumentException.class, () -> TrinoJdbcUrl.parse("jdbc:trino://"));
        assertThrows(IllegalArgumentException.class, () -> TrinoJdbcUrl.parse("jdbc:trino://h:8080/a/b/c"));
        assertThrows(IllegalArgumentException.class, () -> TrinoJdbcUrl.parse("jdbc:trino://u:p@h:8080"));
        assertThrows(IllegalArgumentException.class, () -> TrinoJdbcUrl.parse("jdbc:trino://h:8080?user=a&user=b"));
    }

    @Test
    void lenientParsingForExternalDrivers() {
        final TrinoJdbcUrl hierarchical = TrinoJdbcUrl.parse("jdbc:other://h:443/cat?SSL=true", false);
        assertEquals("cat", hierarchical.getCatalog());
        assertEquals("jdbc:other://h:443/cat/raw?SSL=true", hierarchical.withDefaults(null, "raw").toUrl());

        final TrinoJdbcUrl opaque = TrinoJdbcUrl.parse("jdbc:h2:mem:x;DB_CLOSE_DELAY=-1", false);
        assertEquals("jdbc:h2:mem:x;DB_CLOSE_DELAY=-1", opaque.withDefaults(null, null).toUrl());
        assertThrows(IllegalArgumentException.class, () -> opaque.withDefaults("c", null));
        assertEquals(Map.of("password", "x"), TrinoJdbcUrl.parse("jdbc:foo:bar?password=x", false).getQueryParameters());

        assertThrows(IllegalArgumentException.class, () -> TrinoJdbcUrl.parse("jdbc:h2:mem:x", true));
        assertThrows(IllegalArgumentException.class, () -> TrinoJdbcUrl.parse("mysql://h", false));
    }

    @Test
    void credentialRulesStillApplyToExternalDriverUrls() {
        final TrinoConnectionSettings settings = new TrinoConnectionSettings().trinoUrlFormat(false).user("u")
                .url("jdbc:other://h:8080?accessToken=abc");
        org.junit.jupiter.api.Assertions.assertTrue(settings.validate().stream().anyMatch(e -> e.contains("자격 증명")));
        org.junit.jupiter.api.Assertions.assertTrue(new TrinoConnectionSettings().trinoUrlFormat(false).user("u")
                .url("jdbc:other://h:8080?user=u").validate().stream().anyMatch(e -> e.contains("양쪽")));
    }
}
