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
 *   nifi-nar-bundles/nifi-parquet-bundle/nifi-parquet-processors/src/main/java/org/apache/nifi/parquet/record/WriteParquetResult.java
 */
package io.datadynamics.nifi.services.record.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parquet.stream.NifiParquetOutputFile;
import org.apache.nifi.parquet.utils.ParquetConfig;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;

import static org.apache.nifi.parquet.utils.ParquetUtils.applyCommonConfig;

/**
 * NiFi의 Record를 Avro GenericRecord로 변환한 뒤 Parquet 파일로 기록하는 RecordSetWriter 구현체.
 * 내부적으로 AvroParquetWriter를 사용하여 지정된 OutputStream(FlowFile 컨텐츠)에 직접 Parquet
 * 포맷으로 데이터를 기록하며, 레코드 변환 시 타임스탬프 포맷 속성명과 시간 보정값(addHours)을
 * 함께 사용하여 시간대 관련 값을 보정한다.
 */
public class WriteParquetResult extends AbstractRecordSetWriter {

    // Parquet 기록에 사용되는 Avro 스키마
    private final Schema schema;
    // 실제로 GenericRecord를 Parquet 파일에 기록하는 writer
    private final ParquetWriter<GenericRecord> parquetWriter;
    private final ComponentLog componentLogger;
    // 레코드의 타임스탬프 필드를 Avro 레코드로 변환할 때 사용할 타임스탬프 포맷 속성의 키 이름
    private final String timestampFormatPropertyKeyName; // FIXED
    // 스키마 정보를 FlowFile 속성 등으로 기록하기 위한 접근자
    private final SchemaAccessWriter accessWriter;
    private final RecordSchema recordSchema;
    // 타임스탬프 값 변환 시 더해줄 시간(시 단위) 보정값
    private final int addHours; // FIXED

    public WriteParquetResult(final Schema avroSchema, final RecordSchema recordSchema, final SchemaAccessWriter accessWriter, final OutputStream out,
                              final ParquetConfig parquetConfig, final ComponentLog componentLogger, String timestampFormatPropertyKeyName, int addHours) throws IOException { // FIXED
        super(out);
        this.schema = avroSchema;
        this.componentLogger = componentLogger;
        this.accessWriter = accessWriter;
        this.recordSchema = recordSchema;
        this.timestampFormatPropertyKeyName = timestampFormatPropertyKeyName; // FIXED
        this.addHours = addHours; // FIXED

        final Configuration conf = new Configuration();
        final OutputFile outputFile = new NifiParquetOutputFile(out);

        final AvroParquetWriter.Builder<GenericRecord> writerBuilder = AvroParquetWriter.<GenericRecord>builder(outputFile).withSchema(avroSchema);
        applyCommonConfig(writerBuilder, conf, parquetConfig);
        parquetWriter = writerBuilder.build();

        if (componentLogger.isDebugEnabled()) {
            componentLogger.debug("[DFM] WriteParquetResult : Schema = {}", schema);
            componentLogger.debug("[DFM] WriteParquetResult : timestampFormatPropertyKeyName = {}, addHours = {}", this.timestampFormatPropertyKeyName, this.addHours);
        }
    }

    /**
     * 단일 레코드를 Avro GenericRecord로 변환한 뒤 Parquet writer에 기록한다.
     * 타임스탬프 관련 필드는 timestampFormatPropertyKeyName과 addHours를 이용해 변환된다.
     */
    @Override
    protected Map<String, String> writeRecord(final Record record) throws IOException {
        if (componentLogger.isDebugEnabled()) {
            this.componentLogger.debug("[DFM] [Write] Record = {}", record);
        }

        final GenericRecord genericRecord = AvroTypeUtil.createAvroRecord(record, schema, timestampFormatPropertyKeyName, addHours);
        parquetWriter.write(genericRecord);
        return Collections.emptyMap();
    }

    // 레코드셋 기록이 끝난 후 스키마 접근 정보를 FlowFile 속성 형태로 반환한다.
    @Override
    protected Map<String, String> onFinishRecordSet() {
        return accessWriter.getAttributes(recordSchema);
    }

    @Override
    public void close() throws IOException {
        try {
            parquetWriter.close();
        } finally {
            // 출력 스트림이 항상 닫히도록 보장한다.
            super.close();
        }
    }

    // 이 writer가 생성하는 콘텐츠의 MIME 타입을 반환한다.
    @Override
    public String getMimeType() {
        return "application/parquet";
    }

}
