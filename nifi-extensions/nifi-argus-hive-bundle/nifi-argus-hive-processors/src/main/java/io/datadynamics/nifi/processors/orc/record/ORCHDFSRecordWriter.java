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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/nifi/processors/orc/record/ORCHDFSRecordWriter.java
 */
package io.datadynamics.nifi.processors.orc.record;

import org.apache.hadoop.hive.ql.io.orc.NiFiOrcUtils;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datadynamics.nifi.processors.orc.PutORC.HIVE_DDL_ATTRIBUTE;

/**
 * NiFi의 Record API를 스키마 표현으로 사용하여 ORC 파일을 작성하는 HDFSRecordWriter 구현체.
 * 레코드 하나하나를 ORC 오브젝트로 변환한 뒤 내부 ORC Writer에 행(row)으로 추가하는 역할을 담당한다.
 */

public class ORCHDFSRecordWriter implements HDFSRecordWriter {

    // 원본 FlowFile 레코드의 스키마 (Hive DDL 생성 시에도 재사용됨)
    private final RecordSchema recordSchema;
    // recordSchema로부터 변환된 ORC(Hive) 타입 정보. ORC Struct를 생성할 때 필요하다.
    private final TypeInfo orcSchema;
    // 실제로 ORC 파일에 데이터를 기록하는 하위 Writer
    private final Writer orcWriter;
    // hive.ddl 속성 생성에 사용될 테이블명
    private final String hiveTableName;
    // true면 Hive 규칙에 맞춰 필드명을 정규화(소문자화 등)한다
    private final boolean hiveFieldNames;
    private final List<RecordField> recordFields;
    private final int numRecordFields;
    // 매 레코드마다 새로 배열을 할당하지 않도록 재사용하는 작업용 행(row) 버퍼
    private final Object[] workingRow;

    public ORCHDFSRecordWriter(final Writer orcWriter, final RecordSchema recordSchema, final String hiveTableName, final boolean hiveFieldNames) {
        this.recordSchema = recordSchema;
        this.orcWriter = orcWriter;
        this.hiveFieldNames = hiveFieldNames;
        this.orcSchema = NiFiOrcUtils.getOrcSchema(recordSchema, this.hiveFieldNames);
        this.hiveTableName = hiveTableName;
        this.recordFields = recordSchema != null ? recordSchema.getFields() : null;
        this.numRecordFields = recordFields != null ? recordFields.size() : -1;
        // 행 객체를 재사용하여 매 레코드마다 배열을 새로 생성하는 오버헤드를 줄인다
        this.workingRow = numRecordFields > -1 ? new Object[numRecordFields] : null;
    }

    // 레코드 하나를 ORC 오브젝트 배열로 변환한 뒤, ORC Struct로 감싸서 Writer에 추가한다.
    @Override
    public void write(final org.apache.nifi.serialization.record.Record record) throws IOException {
        if (recordFields != null) {
            for (int i = 0; i < numRecordFields; i++) {
                final RecordField field = recordFields.get(i);
                final DataType fieldType = field.getDataType();
                final String fieldName = field.getFieldName();
                Object o = record.getValue(field);
                try {
                    workingRow[i] = NiFiOrcUtils.convertToORCObject(NiFiOrcUtils.getOrcField(fieldType, hiveFieldNames), o, hiveFieldNames);
                } catch (ArrayIndexOutOfBoundsException aioobe) {
                    final String errorMsg = "Index out of bounds for column " + i + ", type " + fieldName + ", and object " + o.toString();
                    throw new IOException(errorMsg, aioobe);
                }
            }

            orcWriter.addRow(NiFiOrcUtils.createOrcStruct(orcSchema, workingRow));
        }
    }

    /**
     * RecordSet에 포함된 모든 레코드를 순회하며 ORC 파일에 기록하고, 완료 후 Hive DDL 속성을 포함한 결과를 반환한다.
     *
     * @param recordSet 기록할 RecordSet
     * @return 기록 결과(레코드 수 및 속성 맵)
     * @throws IOException RecordSet 읽기 또는 Record 기록 중 I/O 오류가 발생한 경우
     */
    public WriteResult write(final RecordSet recordSet) throws IOException {
        int recordCount = 0;

        org.apache.nifi.serialization.record.Record record;
        while ((record = recordSet.next()) != null) {
            write(record);
            recordCount++;
        }

        // Hive DDL 속성 추가 (다운스트림에서 외부 테이블 생성 DDL로 활용 가능)
        String hiveDDL = NiFiOrcUtils.generateHiveDDL(recordSchema, hiveTableName, hiveFieldNames);
        Map<String, String> attributes = new HashMap<String, String>() {{
            put(HIVE_DDL_ATTRIBUTE, hiveDDL);
        }};

        return WriteResult.of(recordCount, attributes);
    }

    // 내부 ORC Writer를 닫아 파일에 남은 데이터를 플러시하고 자원을 해제한다.
    @Override
    public void close() throws IOException {
        orcWriter.close();
    }

}

