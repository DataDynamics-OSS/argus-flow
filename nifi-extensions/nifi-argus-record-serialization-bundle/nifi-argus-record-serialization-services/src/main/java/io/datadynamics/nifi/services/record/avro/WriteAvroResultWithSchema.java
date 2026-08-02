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
 *   nifi-nar-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services/src/main/java/org/apache/nifi/avro/WriteAvroResultWithSchema.java
 */
package io.datadynamics.nifi.services.record.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;

/**
 * Avro 스키마를 콘텐츠에 직접 내장(embed)하여 기록하는 RecordSetWriter 구현체.
 *
 * <p>Avro의 표준 {@link DataFileWriter}를 사용하며, 파일 헤더에 스키마와 압축 코덱 정보가 함께
 * 저장되므로 별도의 외부 스키마 접근 전략이 필요 없다. Timestamp 관련 필드 변환 시에는
 * {@link TimestampFormatAvroTypeUtil}에 위임하여, 지정된 형식 Property 이름과 시간(hour) 오프셋을
 * 적용한다.</p>
 */
public class WriteAvroResultWithSchema extends AbstractRecordSetWriter {

    private final DataFileWriter<GenericRecord> dataFileWriter;
    private final Schema schema;
    // Timestamp 패턴이 정의된 Avro 스키마 Property의 이름
    private final String timestampFormatPropertyKeyName;
    // Timestamp 값에 더할 시간(hour) 오프셋
    private final int addHours;

    /**
     * 지정된 압축 코덱으로 Avro DataFileWriter를 생성하고, 스키마를 출력 스트림에 기록한다.
     */
    public WriteAvroResultWithSchema(final Schema schema, final OutputStream out, final CodecFactory codec,
                                     final String timestampFormatPropertyKeyName, final int addHours) throws IOException {
        super(out);
        this.schema = schema;
        this.timestampFormatPropertyKeyName = timestampFormatPropertyKeyName;
        this.addHours = addHours;

        final GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.setCodec(codec);
        dataFileWriter.create(schema, out);
    }

    @Override
    public void close() throws IOException {
        dataFileWriter.close();
    }

    @Override
    public void flush() throws IOException {
        dataFileWriter.flush();
    }

    /**
     * 레코드 한 건을 Timestamp 형식이 적용된 Avro GenericRecord로 변환한 후 DataFileWriter에 추가한다.
     * 스키마가 파일에 내장되므로 별도의 FlowFile 속성은 반환하지 않는다.
     */
    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        final GenericRecord rec = TimestampFormatAvroTypeUtil.createAvroRecord(record, schema, timestampFormatPropertyKeyName, addHours);
        dataFileWriter.append(rec);
        return Collections.emptyMap();
    }

    @Override
    public String getMimeType() {
        return "application/avro-binary";
    }
}
