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
package io.datadynamics.nifi.processors.hive.compaction;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 세션 설정 저장·복원. 응답 모양은 apache/hive 3.1.3·4.0.1 의 {@code SET key} 결과(컬럼 set, "key=value" / "key is undefined")를 따른다.
 */
class SessionOverridesTest {

    /** 기록하는 가짜 세션. */
    static final class FakeSession implements SessionOverrides.SqlRunner {
        final Map<String, String> values = new HashMap<>();
        final List<String> log = new ArrayList<>();
        String failOn;

        @Override
        public List<String> queryFirstColumn(final String sql) {
            log.add(sql);
            final String key = sql.substring(4);
            return List.of(values.containsKey(key) ? key + "=" + values.get(key) : key + " is undefined");
        }

        @Override
        public void execute(final String sql) throws SQLException {
            log.add(sql);
            if (sql.equals(failOn)) {
                throw new SQLException("거부: " + sql);
            }
            if (sql.startsWith("RESET ")) {
                values.remove(sql.substring(6));
            } else {
                final String body = sql.substring(4);
                values.put(body.substring(0, body.indexOf('=')), body.substring(body.indexOf('=') + 1));
            }
        }
    }

    @Test
    void readsOriginalOnceAndRestoresInReverseOrder() throws Exception {
        final FakeSession session = new FakeSession();
        session.values.put("mapreduce.job.reduces", "-1");
        session.values.put("hive.compute.query.using.stats", "true");
        final SessionOverrides overrides = new SessionOverrides();
        overrides.set(session, "hive.compute.query.using.stats", "false");
        overrides.set(session, "mapreduce.job.reduces", "4");
        overrides.set(session, "mapreduce.job.reduces", "5");
        overrides.set(session, "no.such.key", "x");
        assertEquals(List.of("SET hive.compute.query.using.stats", "SET hive.compute.query.using.stats=false",
                "SET mapreduce.job.reduces", "SET mapreduce.job.reduces=4", "SET mapreduce.job.reduces=5",
                "SET no.such.key", "SET no.such.key=x"), session.log);
        assertEquals(List.of("RESET no.such.key", "SET mapreduce.job.reduces=-1", "SET hive.compute.query.using.stats=true"),
                overrides.restoreStatements());
        overrides.restore(session);
        assertEquals(Map.of("mapreduce.job.reduces", "-1", "hive.compute.query.using.stats", "true"), session.values);
        assertTrue(overrides.isEmpty());
    }

    @Test
    void restoreContinuesAfterFailureAndRethrows() throws Exception {
        final FakeSession session = new FakeSession();
        session.values.put("a.b", "1");
        session.values.put("c.d", "2");
        final SessionOverrides overrides = new SessionOverrides();
        overrides.set(session, "a.b", "9");
        overrides.set(session, "c.d", "9");
        session.failOn = "SET c.d=2";
        assertThrows(SQLException.class, () -> overrides.restore(session));
        assertEquals("1", session.values.get("a.b"));
    }

    @Test
    void parsesSetRowsAndRejectsUnsafeInput() throws Exception {
        assertEquals("false", SessionOverrides.parseSetRow("hive.merge.tezfiles=false", "hive.merge.tezfiles"));
        assertEquals("a=b", SessionOverrides.parseSetRow("k=a=b", "k"));
        assertNull(SessionOverrides.parseSetRow("no.such.key is undefined", "no.such.key"));
        assertNull(SessionOverrides.parseSetRow("other=1", "k"));
        final SessionOverrides overrides = new SessionOverrides();
        final FakeSession session = new FakeSession();
        assertThrows(IllegalArgumentException.class, () -> overrides.set(session, "a;drop", "1"));
        assertThrows(IllegalArgumentException.class, () -> overrides.set(session, "a.b", "1; drop table x"));
        assertTrue(session.log.isEmpty());
    }
}
