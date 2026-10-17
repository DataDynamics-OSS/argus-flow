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
package io.datadynamics.nifi.processor.util.list;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.list.ListableEntity;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * {@link ListableEntity} 를 업스트림 AbstractListProcessor 와 같은 방식으로 쓴다 — Record Writer 가 있으면
 * {@link ListableEntity#toRecord()} 레코드 FlowFile 하나, 없으면 엔티티마다 속성 FlowFile 하나.
 * 업스트림과 달리 자식 FlowFile 은 입력 FlowFile 에서 만든다 (계보·속성 상속).
 */
public final class StandardListingEmitter<T extends ListableEntity> implements TriggeredListingSupport.ListingEmitter<T> {

    private final ProcessContext context;
    private final ComponentLog logger;
    private final RecordSchema recordSchema;
    private final Function<T, Map<String, String>> attributesFactory;

    public StandardListingEmitter(final ProcessContext context, final ComponentLog logger, final RecordSchema recordSchema,
                                  final Function<T, Map<String, String>> attributesFactory) {
        this.context = context;
        this.logger = logger;
        this.recordSchema = recordSchema;
        this.attributesFactory = attributesFactory;
    }

    @Override
    public void emit(final ProcessSession session, final FlowFile parent, final List<T> entities, final List<FlowFile> children)
            throws IOException, SchemaNotFoundException {
        if (entities.isEmpty()) {
            return;
        }

        if (context.getProperty(TriggeredListing.RECORD_WRITER).isSet()) {
            emitRecords(session, parent, entities, children);
        } else {
            for (final T entity : entities) {
                FlowFile child = session.create(parent);
                children.add(child);
                child = session.putAllAttributes(child, attributesFactory.apply(entity));
                children.set(children.size() - 1, child);
            }
        }
    }

    private void emitRecords(final ProcessSession session, final FlowFile parent, final List<T> entities, final List<FlowFile> children)
            throws IOException, SchemaNotFoundException {
        final RecordSetWriterFactory writerFactory = context.getProperty(TriggeredListing.RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        FlowFile child = session.create(parent);
        children.add(child);
        final WriteResult writeResult;
        final String mimeType;
        try (OutputStream out = session.write(child);
             RecordSetWriter writer = writerFactory.createWriter(logger, recordSchema, out, parent.getAttributes())) {
            mimeType = writer.getMimeType();
            writer.beginRecordSet();
            for (final T entity : entities) {
                writer.write(entity.toRecord());
            }
            writeResult = writer.finishRecordSet();
        }

        final Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
        attributes.put(CoreAttributes.MIME_TYPE.key(), mimeType);
        child = session.putAllAttributes(child, attributes);
        children.set(children.size() - 1, child);
    }
}
