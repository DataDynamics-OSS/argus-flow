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
package io.datadynamics.nifi.trino.processors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlStatementsTest {

    @Test
    void singleStatementIsTrimmed() {
        assertEquals("SELECT 1", SqlStatements.requireSingleStatement("  SELECT 1  "));
        assertEquals("SELECT 1", SqlStatements.requireSingleStatement("SELECT 1;"));
        assertEquals("SELECT 1", SqlStatements.requireSingleStatement("SELECT 1 ;  \n"));
        assertEquals("SELECT 1", SqlStatements.requireSingleStatement("SELECT 1; -- 끝 주석\n/* 블록 */"));
    }

    @Test
    void semicolonsInsideQuotesAndCommentsAreIgnored() {
        assertEquals("SELECT 'a;b', \"c;d\" FROM t -- x;y\nWHERE s = 'it''s;'",
                SqlStatements.requireSingleStatement("SELECT 'a;b', \"c;d\" FROM t -- x;y\nWHERE s = 'it''s;'"));
        assertEquals("SELECT /* ; */ 1", SqlStatements.requireSingleStatement("SELECT /* ; */ 1;"));
    }

    @Test
    void multipleStatementsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> SqlStatements.requireSingleStatement("SELECT 1; SELECT 2"));
        assertThrows(IllegalArgumentException.class, () -> SqlStatements.requireSingleStatement("SELECT 1;;"));
        assertThrows(IllegalArgumentException.class, () -> SqlStatements.requireSingleStatement("DELETE FROM t; DROP TABLE t;"));
    }

    @Test
    void emptyIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> SqlStatements.requireSingleStatement(" "));
        assertThrows(IllegalArgumentException.class, () -> SqlStatements.requireSingleStatement(";"));
        assertThrows(IllegalArgumentException.class, () -> SqlStatements.requireSingleStatement(null));
    }
}
