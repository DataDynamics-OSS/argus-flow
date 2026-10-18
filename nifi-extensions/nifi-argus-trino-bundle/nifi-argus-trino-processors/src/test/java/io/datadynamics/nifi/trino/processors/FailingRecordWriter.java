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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * N 번째 레코드(모든 writer 누적)에서 IOException 을 던지는 Record Writer.
 */
public class FailingRecordWriter extends AbstractControllerService implements RecordSetWriterFactory {

    private final int failAt;
    private final AtomicInteger written = new AtomicInteger();

    public FailingRecordWriter(final int failAt) {
        this.failAt = failAt;
    }

    @Override
    public RecordSchema getSchema(final Map<String, String> variables, final RecordSchema readSchema) {
        return readSchema;
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out,
                                        final Map<String, String> variables) {
        return new RecordSetWriter() {
            private int count;

            @Override
            public WriteResult write(final RecordSet recordSet) throws IOException {
                Record record;
                while ((record = recordSet.next()) != null) {
                    write(record);
                }
                return WriteResult.of(count, Map.of());
            }

            @Override
            public void beginRecordSet() {
            }

            @Override
            public WriteResult finishRecordSet() {
                return WriteResult.of(count, Map.of());
            }

            @Override
            public WriteResult write(final Record record) throws IOException {
                if (written.incrementAndGet() >= failAt) {
                    throw new IOException("의도한 쓰기 실패");
                }
                out.write('x');
                count++;
                return WriteResult.of(1, Map.of());
            }

            @Override
            public String getMimeType() {
                return "text/plain";
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
    }
}
