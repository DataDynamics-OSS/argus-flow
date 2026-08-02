/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-processors/src/main/java/org/apache/nifi/processors/iceberg/writer/IcebergTaskWriterFactory.java
 */
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
package org.apache.nifi.processors.iceberg.writer;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.util.PropertyUtil;

/**
 * {@link Table}의 속성(파티션 여부 등)에 따라 적합한 {@link TaskWriter}를 생성해 주는 팩토리 클래스.
 * 파티션이 설정되지 않은 테이블에는 {@link UnpartitionedWriter}를, 파티션 테이블에는
 * {@link IcebergPartitionedWriter}를 사용하도록 분기 처리한다.
 */
public class IcebergTaskWriterFactory {

    private final Schema schema;
    private final PartitionSpec spec;
    private final FileIO io;
    private final long targetFileSize;
    private final FileFormat fileFormat;
    private final FileAppenderFactory<Record> appenderFactory;
    private final OutputFileFactory outputFileFactory;

    /**
     * 테이블 정보를 바탕으로 writer 생성에 필요한 스키마, 파티션 스펙, 파일 I/O, 대상 파일 크기 등을
     * 미리 계산하여 준비한다.
     *
     * @param table          대상 Iceberg 테이블
     * @param taskId         출력 파일명을 구분하기 위한 태스크(작업) 식별자
     * @param fileFormat     기록할 데이터 파일 포맷(Parquet, ORC, Avro 등)
     * @param targetFileSize 목표 파일 크기(바이트). null이면 테이블 속성에 설정된 기본값을 사용
     */
    public IcebergTaskWriterFactory(Table table, long taskId, FileFormat fileFormat, String targetFileSize) {
        this.schema = table.schema();
        this.spec = table.spec();
        this.io = table.io();
        this.fileFormat = fileFormat;

        this.targetFileSize = targetFileSize != null ? Long.parseLong(targetFileSize) :
                PropertyUtil.propertyAsLong(table.properties(), TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

        this.outputFileFactory = OutputFileFactory.builderFor(table, table.spec().specId(), taskId).format(fileFormat).build();
        this.appenderFactory = new GenericAppenderFactory(schema, spec);
    }

    /**
     * 테이블의 파티션 스펙에 따라 파티션 미적용 writer 또는 파티션 writer를 생성하여 반환한다.
     */
    public TaskWriter<Record> create() {
        if (spec.isUnpartitioned()) {
            return new UnpartitionedWriter<>(spec, fileFormat, appenderFactory, outputFileFactory, io, targetFileSize);
        } else {
            return new IcebergPartitionedWriter(spec, fileFormat, appenderFactory, outputFileFactory, io, targetFileSize, schema);
        }
    }
}
