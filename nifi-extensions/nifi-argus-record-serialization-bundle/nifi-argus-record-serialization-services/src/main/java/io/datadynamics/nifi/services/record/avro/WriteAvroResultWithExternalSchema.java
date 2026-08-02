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
 *   nifi-nar-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services/src/main/java/org/apache/nifi/avro/WriteAvroResultWithExternalSchema.java
 */
package io.datadynamics.nifi.services.record.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Avro 스키마를 콘텐츠에 내장하지 않고, 외부 스키마 접근 전략({@link SchemaAccessWriter})을 통해
 * 스키마 정보를 헤더나 FlowFile 속성으로 기록하는 RecordSetWriter 구현체.
 *
 * <p>Timestamp 관련 필드 변환 시에는 {@link TimestampFormatAvroTypeUtil}에 위임하여, 지정된 형식
 * Property 이름과 시간(hour) 오프셋을 적용한다. BinaryEncoder는 상위 컨트롤러 서비스가 관리하는
 * 재사용 풀({@code recycleQueue})에서 빌려오고, close 시점에 다시 반납한다.</p>
 */
public class WriteAvroResultWithExternalSchema extends AbstractRecordSetWriter {
    private final SchemaAccessWriter schemaAccessWriter;
    private final RecordSchema recordSchema;
    private final Schema avroSchema;
    private final BinaryEncoder encoder;
    private final OutputStream buffered;
    private final DatumWriter<GenericRecord> datumWriter;
    // Encoder를 반납할 재사용 풀 (컨트롤러 서비스가 소유하며, 여러 Writer 인스턴스가 공유)
    private final BlockingQueue<BinaryEncoder> recycleQueue;
    // Timestamp 패턴이 정의된 Avro 스키마 Property의 이름
    private final String timestampFormatPropertyKeyName;
    // Timestamp 값에 더할 시간(hour) 오프셋
    private final int addHours;
    private boolean closed = false;

    /**
     * 재사용 풀에서 BinaryEncoder를 획득(없으면 새로 생성)하고, 외부 스키마 접근 방식에 필요한
     * DatumWriter를 초기화한다.
     */
    public WriteAvroResultWithExternalSchema(final Schema avroSchema, final RecordSchema recordSchema, final SchemaAccessWriter schemaAccessWriter,
                                             final OutputStream out, final BlockingQueue<BinaryEncoder> recycleQueue, final ComponentLog logger,
                                             final String timestampFormatPropertyKeyName, final int addHours) {
        super(out);
        this.recordSchema = recordSchema;
        this.schemaAccessWriter = schemaAccessWriter;
        this.avroSchema = avroSchema;
        this.buffered = new BufferedOutputStream(out);
        this.recycleQueue = recycleQueue;
        this.timestampFormatPropertyKeyName = timestampFormatPropertyKeyName;
        this.addHours = addHours;

        // 재사용 풀에서 기존 Encoder를 꺼내오고, 없으면 null을 전달하여 새로 생성하도록 한다.
        BinaryEncoder reusableEncoder = recycleQueue.poll();
        if (reusableEncoder == null) {
            logger.debug("Was not able to obtain a BinaryEncoder from reuse pool. This is normal for the first X number of iterations (where X is equal to the max size of the pool), " +
                    "but if this continues, it indicates that increasing the size of the pool will likely yield better performance for this Avro Writer.");
        }

        encoder = EncoderFactory.get().blockingBinaryEncoder(buffered, reusableEncoder);

        datumWriter = new GenericDatumWriter<>(avroSchema);
    }

    /**
     * 레코드 세트 기록을 시작할 때 외부 스키마 접근 전략에 따라 스키마 헤더 정보를 먼저 기록한다.
     */
    @Override
    protected void onBeginRecordSet() throws IOException {
        schemaAccessWriter.writeHeader(recordSchema, buffered);
    }

    /**
     * 레코드 세트 기록을 마칠 때 버퍼를 flush하고, 스키마 접근 전략이 제공하는 FlowFile 속성을 반환한다.
     */
    @Override
    protected Map<String, String> onFinishRecordSet() throws IOException {
        flush();
        return schemaAccessWriter.getAttributes(recordSchema);
    }

    /**
     * 레코드 한 건을 Avro GenericRecord로 변환한 후 인코딩하여 기록한다.
     * Timestamp 관련 필드 변환은 {@link TimestampFormatAvroTypeUtil}에 위임한다.
     */
    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        // 활성화된 레코드 세트를 기록 중이 아니라면(단건 기록 모드), 스키마 정보를 먼저 기록해야 한다.
        if (!isActiveRecordSet()) {
            flush();
            schemaAccessWriter.writeHeader(recordSchema, getOutputStream());
        }

        final GenericRecord rec = TimestampFormatAvroTypeUtil.createAvroRecord(record, avroSchema, timestampFormatPropertyKeyName, addHours);
        datumWriter.write(rec, encoder);
        return schemaAccessWriter.getAttributes(recordSchema);
    }

    @Override
    public void flush() throws IOException {
        encoder.flush();
        buffered.flush();
    }

    @Override
    public String getMimeType() {
        return "application/avro-binary";
    }

    /**
     * Writer를 닫으면서 남은 데이터를 flush하고, 사용한 BinaryEncoder를 재사용 풀에 반납한다.
     * 중복 호출에 대비해 closed 플래그로 멱등성을 보장한다.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        if (encoder != null) {
            flush();
            recycleQueue.offer(encoder);
        }

        super.close();
    }
}
