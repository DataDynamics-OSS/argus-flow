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
package io.datadynamics.nifi.processors.db.putdatabaserecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * DB 방언(Adapter)별 SQL 생성 로직 단위 테스트.
 *
 * 어댑터는 SELECT 페이징·UPSERT·INSERT IGNORE 문을 방언에 맞게 문자열로 생성하는 순수 로직이므로
 * 실제 DB 없이 문자열 단언만으로 검증한다. name/quote/supports 메타데이터, 방언별 구문(MySQL의
 * ON DUPLICATE KEY, PostgreSQL의 ON CONFLICT, Oracle의 ROWNUM, Oracle 12+의 OFFSET/FETCH·MERGE),
 * 인자 검증 예외까지 커버한다.
 */
class DatabaseAdaptersTest {

    private final GenericDatabaseAdapter generic = new GenericDatabaseAdapter();
    private final MySQLDatabaseAdapter mysql = new MySQLDatabaseAdapter();
    private final OracleDatabaseAdapter oracle = new OracleDatabaseAdapter();
    private final Oracle12DatabaseAdapter oracle12 = new Oracle12DatabaseAdapter();
    private final PostgreSQLDatabaseAdapter postgres = new PostgreSQLDatabaseAdapter();

    private static final List<String> COLS = List.of("id", "name", "val");
    private static final List<String> KEYS = List.of("id");

    // ---- 메타데이터 ----
    @Test
    void names() {
        assertEquals("Generic", generic.getName());
        assertEquals("MySQL", mysql.getName());
        assertEquals("Oracle", oracle.getName());
        assertEquals("Oracle 12+", oracle12.getName());
        assertEquals("PostgreSQL", postgres.getName());
    }

    @Test
    void supportsUpsertFlags() {
        assertFalse(generic.supportsUpsert());
        assertFalse(oracle.supportsUpsert());
        assertTrue(mysql.supportsUpsert());
        assertTrue(oracle12.supportsUpsert());
        assertTrue(postgres.supportsUpsert());
    }

    @Test
    void supportsInsertIgnoreFlags() {
        assertFalse(generic.supportsInsertIgnore());
        assertTrue(mysql.supportsInsertIgnore());
        assertTrue(postgres.supportsInsertIgnore());
    }

    @Test
    void mysqlUsesBacktickQuoting() {
        assertEquals("`", mysql.getTableQuoteString());
        assertEquals("`", mysql.getColumnQuoteString());
    }

    // ---- UPSERT ----
    @Test
    void mysqlUpsertStatement() {
        assertEquals(
                "INSERT INTO users(id, name, val) VALUES (?, ?, ?) "
                        + "ON DUPLICATE KEY UPDATE id = ?, name = ?, val = ?",
                mysql.getUpsertStatement("users", COLS, KEYS));
    }

    @Test
    void mysqlInsertIgnoreStatement() {
        assertEquals(
                "INSERT IGNORE INTO users(id, name, val) VALUES (?, ?, ?)",
                mysql.getInsertIgnoreStatement("users", COLS, KEYS));
    }

    @Test
    void postgresUpsertStatement() {
        assertEquals(
                "INSERT INTO users(id, name, val) VALUES (?, ?, ?) "
                        + "ON CONFLICT (id) DO UPDATE SET (id, name, val) "
                        + "= (EXCLUDED.id, EXCLUDED.name, EXCLUDED.val)",
                postgres.getUpsertStatement("users", COLS, KEYS));
    }

    @Test
    void postgresInsertIgnoreStatement() {
        final String sql = postgres.getInsertIgnoreStatement("users", COLS, KEYS);
        assertTrue(sql.startsWith("INSERT INTO users(id, name, val) VALUES (?, ?, ?)"), sql);
        assertTrue(sql.contains("ON CONFLICT (id)"), sql);
        assertTrue(sql.contains("DO NOTHING"), sql);
    }

    @Test
    void oracle12UpsertUsesMerge() {
        final String sql = oracle12.getUpsertStatement("users", COLS, KEYS);
        assertTrue(sql.startsWith("MERGE INTO users USING"), sql);
        assertTrue(sql.contains("WHEN MATCHED"), sql);
        assertTrue(sql.contains("WHEN NOT MATCHED"), sql);
    }

    @Test
    void upsertRejectsBlankArguments() {
        assertThrows(IllegalArgumentException.class, () -> mysql.getUpsertStatement("", COLS, KEYS));
        assertThrows(IllegalArgumentException.class, () -> mysql.getUpsertStatement("users", List.of(), KEYS));
        assertThrows(IllegalArgumentException.class, () -> mysql.getUpsertStatement("users", COLS, List.of()));
        assertThrows(IllegalArgumentException.class, () -> postgres.getUpsertStatement(null, COLS, KEYS));
    }

    // ---- SELECT 페이징 ----
    @Test
    void genericSelectBasic() {
        assertEquals("SELECT * FROM users",
                generic.getSelectStatement("users", "*", null, null, null, null));
        assertEquals("SELECT id, name FROM users",
                generic.getSelectStatement("users", "id, name", null, null, null, null));
        assertEquals("SELECT * FROM users WHERE age > 5",
                generic.getSelectStatement("users", "*", "age > 5", null, null, null));
    }

    @Test
    void genericSelectLimitOffset() {
        assertEquals("SELECT * FROM users ORDER BY name LIMIT 100 OFFSET 10",
                generic.getSelectStatement("users", "*", null, "name", 100L, 10L));
        // offset 0 이면 OFFSET 절을 붙이지 않는다
        assertEquals("SELECT * FROM users LIMIT 50",
                generic.getSelectStatement("users", "*", null, null, 50L, 0L));
    }

    @Test
    void genericSelectPartitioningUsesRangePredicate() {
        // columnForPartitioning 지정 시 LIMIT/OFFSET 대신 컬럼 값 범위(>= offset, < offset+limit)로 페이징
        assertEquals("SELECT * FROM users WHERE age > 5 AND id >= 10 AND id < 110",
                generic.getSelectStatement("users", "*", "age > 5", "name", 100L, 10L, "id"));
    }

    @Test
    void genericSelectRejectsBlankTable() {
        assertThrows(IllegalArgumentException.class,
                () -> generic.getSelectStatement("", "*", null, null, null, null));
    }

    @Test
    void mysqlInheritsGenericLimitOffsetPaging() {
        assertEquals("SELECT * FROM users LIMIT 100",
                mysql.getSelectStatement("users", "*", null, null, 100L, null));
    }

    @Test
    void oracleSelectUsesRownum() {
        final String sql = oracle.getSelectStatement("users", "*", null, "id", 100L, 10L);
        assertTrue(sql.contains("ROWNUM"), sql);
    }

    @Test
    void oracle12SelectUsesOffsetFetch() {
        final String sql = oracle12.getSelectStatement("users", "*", null, "id", 100L, 10L);
        assertTrue(sql.contains("OFFSET"), sql);
        assertTrue(sql.contains("FETCH NEXT"), sql);
        assertTrue(sql.contains("ROWS ONLY"), sql);
    }
}
