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
import io.delta.kernel.types.DataType;

import java.math.BigDecimal;

/**
 * 한 컬럼의 값을 이미 {@link DeltaTypeMapper#coerce} 로 정규화해 담고 있는 인메모리 컬럼 벡터.
 *
 * <p>값은 Delta 물리 표현으로 미리 변환되어 있다(DATE=Integer 일수, TIMESTAMP=Long 마이크로초).
 * 따라서 각 getter는 단순 캐스팅만 수행한다.</p>
 */
final class RecordColumnVector implements ColumnVector {

    private final DataType dataType;
    private final Object[] values;

    RecordColumnVector(final DataType dataType, final Object[] values) {
        this.dataType = dataType;
        this.values = values;
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public int getSize() {
        return values.length;
    }

    @Override
    public void close() {
        // 인메모리 배열이라 해제할 자원 없음.
    }

    @Override
    public boolean isNullAt(final int rowId) {
        return values[rowId] == null;
    }

    @Override
    public boolean getBoolean(final int rowId) {
        return (Boolean) values[rowId];
    }

    @Override
    public byte getByte(final int rowId) {
        return (Byte) values[rowId];
    }

    @Override
    public short getShort(final int rowId) {
        return (Short) values[rowId];
    }

    @Override
    public int getInt(final int rowId) {
        return ((Number) values[rowId]).intValue();
    }

    @Override
    public long getLong(final int rowId) {
        return ((Number) values[rowId]).longValue();
    }

    @Override
    public float getFloat(final int rowId) {
        return ((Number) values[rowId]).floatValue();
    }

    @Override
    public double getDouble(final int rowId) {
        return ((Number) values[rowId]).doubleValue();
    }

    @Override
    public BigDecimal getDecimal(final int rowId) {
        return (BigDecimal) values[rowId];
    }

    @Override
    public byte[] getBinary(final int rowId) {
        return (byte[]) values[rowId];
    }

    @Override
    public String getString(final int rowId) {
        return (String) values[rowId];
    }
}
