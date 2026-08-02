/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.deltalake.convert;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import java.util.List;

/**
 * NiFi 레코드 목록을 Delta 논리 스키마(StructType)에 맞춰 컬럼 지향 배치로 노출한다.
 *
 * <p>배치의 스키마는 반드시 테이블의 논리 스키마와 동일해야 한다(파티션 컬럼 포함).
 * 파티션 컬럼 제거는 Delta Kernel의 {@code transformLogicalData}가 담당한다.</p>
 */
public final class RecordColumnarBatch implements ColumnarBatch {

    private final StructType schema;
    private final ColumnVector[] columns;
    private final int size;

    private RecordColumnarBatch(final StructType schema, final ColumnVector[] columns, final int size) {
        this.schema = schema;
        this.columns = columns;
        this.size = size;
    }

    /**
     * 주어진 레코드들을 {@code schema}의 각 필드에 대해 컬럼 벡터로 구성한다.
     * 각 값은 필드명으로 레코드에서 꺼내 Delta 물리 표현으로 정규화한다(없으면 null).
     */
    public static RecordColumnarBatch of(final StructType schema, final List<Object[]> rowsByColumn, final int size) {
        final List<StructField> fields = schema.fields();
        final ColumnVector[] vectors = new ColumnVector[fields.size()];
        for (int col = 0; col < fields.size(); col++) {
            final DataType type = fields.get(col).getDataType();
            vectors[col] = new RecordColumnVector(type, rowsByColumn.get(col));
        }
        return new RecordColumnarBatch(schema, vectors, size);
    }

    @Override
    public StructType getSchema() {
        return schema;
    }

    @Override
    public ColumnVector getColumnVector(final int ordinal) {
        return columns[ordinal];
    }

    @Override
    public int getSize() {
        return size;
    }
}
