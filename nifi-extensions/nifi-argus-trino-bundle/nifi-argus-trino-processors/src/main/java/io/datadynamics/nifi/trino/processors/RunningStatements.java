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

import org.apache.nifi.logging.ComponentLog;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 실행 중인 {@link Statement}를 추적해 프로세서 중지 시 취소한다. Trino 에서는 쿼리가 USER_CANCELED 로 끝난다.
 */
final class RunningStatements {

    private final Set<Statement> statements = ConcurrentHashMap.newKeySet();

    void add(final Statement statement) {
        if (statement != null) {
            statements.add(statement);
        }
    }

    void remove(final Statement statement) {
        if (statement != null) {
            statements.remove(statement);
        }
    }

    int size() {
        return statements.size();
    }

    int cancelAll(final ComponentLog logger) {
        int canceled = 0;
        for (final Statement statement : statements) {
            try {
                statement.cancel();
                canceled++;
            } catch (final SQLException | RuntimeException e) {
                logger.warn("실행 중인 Trino 문장 취소 실패", e);
            }
        }
        if (canceled > 0) {
            logger.info("프로세서 중지: 실행 중인 Trino 문장 {}건 취소", canceled);
        }
        return canceled;
    }
}
