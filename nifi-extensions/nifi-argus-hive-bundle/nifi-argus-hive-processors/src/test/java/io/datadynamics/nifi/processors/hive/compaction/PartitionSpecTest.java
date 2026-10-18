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

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionSpecTest {

    @Test
    void parsesKeyValuePairsWithWhitespaceAndCase() {
        final PartitionSpec spec = PartitionSpec.parse("  DT = '2026-10-01' , hour='03'");
        assertEquals(Map.of("dt", "2026-10-01", "hour", "03"), spec.values());
        assertEquals("dt='2026-10-01',hour='03'", spec.toCanonicalString());
        assertEquals("PARTITION (`dt`='2026-10-01', `hour`='03')", spec.toPartitionClause());
        assertEquals("WHERE `dt` = '2026-10-01' AND `hour` = '03'", spec.toWhereClause());
    }

    @Test
    void emptySpec() {
        assertTrue(PartitionSpec.parse(null).isEmpty());
        assertTrue(PartitionSpec.parse("   ").isEmpty());
        assertEquals("", PartitionSpec.empty().toPartitionClause());
        assertEquals("", PartitionSpec.empty().toWhereClause());
        assertEquals("", PartitionSpec.empty().toCanonicalString());
    }

    @Test
    void escapesQuotesAndBackslashes() {
        final PartitionSpec spec = PartitionSpec.parse("name='o\\'brien',path='a\\\\b',comma='x,y',eq='a=b'");
        assertEquals("o'brien", spec.values().get("name"));
        assertEquals("a\\b", spec.values().get("path"));
        assertEquals("x,y", spec.values().get("comma"));
        assertEquals("a=b", spec.values().get("eq"));
        assertEquals("PARTITION (`name`='o\\'brien', `path`='a\\\\b', `comma`='x,y', `eq`='a=b')", spec.toPartitionClause());
        // 정규화 문자열은 다시 같은 값으로 읽힌다
        assertEquals(spec, PartitionSpec.parse(spec.toCanonicalString()));
    }

    @Test
    void rejectsMalformedSpecs() {
        for (final String bad : List.of("dt=2026", "dt='2026", "='a'", "dt 'a'", "dt='a' hour='b'", "dt='a',", "d-t='a'",
                "dt='a',DT='b'", "`dt`='a'", "dt='a';drop")) {
            assertThrows(IllegalArgumentException.class, () -> PartitionSpec.parse(bad), bad);
        }
    }

    @Test
    void matchesHivePartitionNamesIgnoringOrderAndPathEscaping() {
        final PartitionSpec spec = PartitionSpec.parse("hour='03',dt='2026-10-01'");
        assertTrue(spec.matchesPartitionName("dt=2026-10-01/hour=03"));
        assertFalse(spec.matchesPartitionName("dt=2026-10-01/hour=04"));
        assertFalse(spec.matchesPartitionName("dt=2026-10-01"));
        assertFalse(spec.matchesPartitionName(null));
        assertTrue(PartitionSpec.parse("p='a:b/c'").matchesPartitionName("p=a%3Ab%2Fc"));
        assertTrue(PartitionSpec.parse("p='한글'").matchesPartitionName("p=한글"));
        assertTrue(PartitionSpec.empty().matchesPartitionName(null));
        assertTrue(PartitionSpec.empty().matchesPartitionName(" --- "));
        assertFalse(PartitionSpec.empty().matchesPartitionName("dt=a"));
    }

    @Test
    void comparesKeysWithPartitionColumns() {
        final PartitionSpec spec = PartitionSpec.parse("hour='03',dt='x'");
        assertTrue(spec.hasSameKeys(List.of("dt", "HOUR")));
        assertFalse(spec.hasSameKeys(List.of("dt")));
        assertFalse(spec.hasSameKeys(List.of("dt", "hour", "minute")));
    }
}
