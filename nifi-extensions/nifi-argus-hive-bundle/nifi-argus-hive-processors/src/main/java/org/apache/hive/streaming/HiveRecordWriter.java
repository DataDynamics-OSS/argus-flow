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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/hive/streaming/HiveRecordWriter.java
 */
/*
 * NOTE: This class intentionally remains in the org.apache.hive.streaming package (not io.datadynamics.*).
 * It relies on package-private members of the Hive Streaming library (hive-streaming 3.1.3), most notably the
 * package-private constructor SerializationError(String, Exception), which is only accessible from within
 * the org.apache.hive.streaming package.
 */
package org.apache.hive.streaming;

import com.google.common.base.Joiner;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * NiFi의 RecordReader를 통해 읽어들인 레코드를 Hive Streaming API(AbstractRecordWriter)로
 * 전달하기 위한 어댑터 구현체. 바이트 배열 기반 API는 지원하지 않고, InputStream을 통해
 * recordReader가 직접 레코드를 순회하도록 위임하는 방식만 지원한다.
 */
public class HiveRecordWriter extends AbstractRecordWriter {

    private final RecordReader recordReader;
    private final ComponentLog log;
    // 하나의 트랜잭션에서 기록할 최대 레코드 수. 0이면 제한 없음(스트림이 끝날 때까지 기록)
    private final int recordsPerTransaction;
    private NiFiRecordSerDe serde;
    // 현재 트랜잭션에서 지금까지 기록한 레코드 수
    private int currentRecordsWritten;

    public HiveRecordWriter(RecordReader recordReader, ComponentLog log, final int recordsPerTransaction) {
        super(null);
        this.recordReader = recordReader;
        this.log = log;
        this.recordsPerTransaction = recordsPerTransaction;
    }

    // 대상 Hive 테이블의 컬럼명/타입 정보를 SerDe 속성에 반영하여 NiFiRecordSerDe를 초기화한다.
    @Override
    public AbstractSerDe createSerde() throws SerializationError {
        try {
            Properties tableProps = table.getMetadata();
            tableProps.setProperty(serdeConstants.LIST_COLUMNS, Joiner.on(",").join(inputColumns));
            tableProps.setProperty(serdeConstants.LIST_COLUMN_TYPES, Joiner.on(":").join(inputTypes));
            NiFiRecordSerDe serde = new NiFiRecordSerDe(recordReader, log);
            SerDeUtils.initializeSerDe(serde, conf, tableProps, null);
            this.serde = serde;
            return serde;
        } catch (SerDeException e) {
            throw new SerializationError("Error initializing serde " + NiFiRecordSerDe.class.getName(), e);
        }
    }

    // 바이트 배열 기반 인코딩은 지원하지 않음 - 이 구현체는 InputStream을 통한 레코드 순회만 지원한다.
    @Override
    public Object encode(byte[] bytes) {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support encoding of records via bytes, only via an InputStream");
    }

    // 바이트 배열 기반 기록은 지원하지 않음 - InputStream 기반 write(long, InputStream)을 사용해야 한다.
    @Override
    public void write(long writeId, byte[] record) {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support writing of records via bytes, only via an InputStream");
    }

    // InputStream은 이미 recordReader에서 사용 중이므로, 별도 파싱 없이 레코드를 순회하며 기록한다.
    @Override
    public void write(long writeId, InputStream inputStream) throws StreamingException {
        // inputStream은 recordReader가 이미 참조하고 있으므로, 여기서는 레코드를 순회하기만 하면 된다
        try {
            Record record = null;
            while ((++currentRecordsWritten <= recordsPerTransaction || recordsPerTransaction == 0)
                    && (record = recordReader.nextRecord()) != null) {
                write(writeId, record);
            }
            // 더 이상 레코드가 없으면 입력 스트림이 소진되었음을 알리기 위해 RecordsEOFException을 던진다
            if (record == null) {
                throw new RecordsEOFException("End of transaction", new Exception());
            }
            currentRecordsWritten = 0;
        } catch (MalformedRecordException | IOException e) {
            throw new StreamingException(e.getLocalizedMessage(), e);
        }
    }

    // NiFi Record를 ObjectWritable로 감싼 뒤 SerDe를 통해 Hive가 이해할 수 있는 객체로 역직렬화한다.
    public Object encode(Record record) throws SerializationError {
        try {
            ObjectWritable blob = new ObjectWritable(record);
            return serde.deserialize(blob);
        } catch (SerDeException e) {
            throw new SerializationError("Unable to convert Record into Object", e);
        }
    }

    // 레코드를 인코딩하고, 버킷 및 파티션 값을 계산하여 해당 RecordUpdater에 삽입한다.
    private void write(long writeId, Record record) throws StreamingException {
        checkAutoFlush();
        try {
            Object encodedRow = encode(record);
            int bucket = getBucket(encodedRow);
            List<String> partitionValues = getPartitionValues(encodedRow);
            getRecordUpdater(partitionValues, bucket).insert(writeId, encodedRow);
            conn.getConnectionStats().incrementRecordsWritten();
        } catch (IOException e) {
            throw new StreamingIOFailure("Error writing record in transaction write id (" + writeId + ")", e);
        }
    }
}
