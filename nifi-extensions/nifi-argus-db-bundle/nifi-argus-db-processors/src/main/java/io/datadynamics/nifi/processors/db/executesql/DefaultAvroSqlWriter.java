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
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/sql/DefaultAvroSqlWriter.java
 */
package io.datadynamics.nifi.processors.db.executesql;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static io.datadynamics.nifi.processors.db.executesql.JdbcCommon.AvroConversionOptions;
import static io.datadynamics.nifi.processors.db.executesql.JdbcCommon.ResultSetRowCallback;

/**
 * JDBC ResultSet을 Avro 바이너리 형식으로 직접 변환하여 출력하는 SqlWriter의 기본 구현체.
 * ExecuteSQL 프로세서가 사용하며, RecordSqlWriter와 달리 Record Writer를 거치지 않고
 * 항상 Avro 포맷으로 결과를 기록한다.
 */
public class DefaultAvroSqlWriter implements SqlWriter {

    private final AvroConversionOptions options;

    private final Map<String, String> attributesToAdd = new HashMap<String, String>() {{
        put(CoreAttributes.MIME_TYPE.key(), JdbcCommon.MIME_TYPE_AVRO_BINARY);
    }};

    public DefaultAvroSqlWriter(AvroConversionOptions options) {
        this.options = options;
    }

    /**
     * ResultSet을 JdbcCommon.convertToAvroStream을 통해 Avro 바이너리 스트림으로 변환하여
     * outputStream에 기록하고, 기록된 행의 개수를 반환한다.
     */
    @Override
    public long writeResultSet(ResultSet resultSet, OutputStream outputStream, ComponentLog logger, ResultSetRowCallback callback) throws Exception {
        try {
            return JdbcCommon.convertToAvroStream(resultSet, outputStream, options, callback);
        } catch (SQLException e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public Map<String, String> getAttributesToAdd() {
        return attributesToAdd;
    }

    @Override
    public void writeEmptyResultSet(OutputStream outputStream, ComponentLog logger) throws IOException {
        JdbcCommon.createEmptyAvroStream(outputStream);
    }

    @Override
    public String getMimeType() {
        return JdbcCommon.MIME_TYPE_AVRO_BINARY;
    }
}