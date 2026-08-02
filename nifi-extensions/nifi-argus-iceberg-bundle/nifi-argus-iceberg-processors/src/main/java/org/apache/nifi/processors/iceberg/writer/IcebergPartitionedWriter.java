/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-processors/src/main/java/org/apache/nifi/processors/iceberg/writer/IcebergPartitionedWriter.java
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
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;

/**
 * 파티션 기반 쓰기를 위해 {@link Record}를 어댑팅하는 클래스.
 * 레코드마다 파티션 값을 계산하여, 값이 같은 레코드는 같은 파티션(파일)에 기록되도록
 * {@link PartitionedFanoutWriter}의 파티션 라우팅 로직을 구현한다.
 */
public class IcebergPartitionedWriter extends PartitionedFanoutWriter<Record> {

    // 현재 레코드로부터 계산된 파티션 키를 재사용하기 위한 인스턴스(레코드마다 값만 갱신됨)
    private final PartitionKey partitionKey;
    // Iceberg 내부 Record를 StructLike로 감싸 파티션 값 계산에 사용할 수 있게 해주는 래퍼
    private final InternalRecordWrapper wrapper;

    public IcebergPartitionedWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<Record> appenderFactory, OutputFileFactory fileFactory,
                             FileIO io, long targetFileSize, Schema schema) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
        this.partitionKey = new PartitionKey(spec, schema);
        this.wrapper = new InternalRecordWrapper(schema.asStruct());
    }

    /**
     * 주어진 레코드를 파티션 스펙에 맞게 래핑하여 해당 레코드가 속할 파티션 키를 계산해 반환한다.
     * 반환되는 partitionKey 인스턴스는 호출마다 재사용되므로, 상위 writer가 즉시 파티션을 판별하는 데만 사용해야 한다.
     */
    @Override
    protected PartitionKey partition(Record record) {
        partitionKey.partition(wrapper.wrap(record));
        return partitionKey;
    }
}
