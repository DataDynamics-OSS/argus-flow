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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/hive/streaming/NiFiRecordSerDe.java
 */
/*
 * NOTE: This class intentionally remains in the org.apache.hive.streaming package (not io.datadynamics.*).
 * It relies on package-private members of the Hive Streaming library (hive-streaming 3.1.3), most notably the
 * package-private constructor SerializationError(String, Exception), which is only accessible from within
 * the org.apache.hive.streaming package.
 */
package org.apache.hive.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.TimestampParser;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.field.StandardFieldConverterRegistry;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * NiFi의 Record 모델(org.apache.nifi.serialization.record.Record)을 Hive Streaming API가 요구하는
 * Writable/ObjectInspector 형태로 변환해주는 SerDe(직렬화/역직렬화) 구현체.
 * Hive Streaming 쪽에서 데이터를 쓸 때(deserialize 방향) 이 클래스를 거쳐 NiFi Record가
 * Hive의 컬럼 타입 체계(TypeInfo)에 맞는 자바 객체 목록으로 변환된다.
 * 이 SerDe는 쓰기(적재) 전용이며 serialize()는 지원하지 않는다(UnsupportedOperationException).
 */
public class NiFiRecordSerDe extends AbstractSerDe {

    // Hive 내부적으로 컬럼 이름을 명시적으로 지정하지 않았을 때 사용하는 "_col0", "_col1" 형태의 컬럼명을 매칭하는 정규식
    private final static Pattern INTERNAL_PATTERN = Pattern.compile("_col([0-9]+)");
    protected RecordReader recordReader;
    protected ComponentLog log;
    protected List<String> columnNames;
    // Hive 테이블 스키마(컬럼명과 컬럼 타입 정보)를 담는 구조체 타입 정보
    protected StructTypeInfo schema;
    protected SerDeStats stats;
    protected StandardStructObjectInspector cachedObjectInspector;
    // Hive의 다양한 타임스탬프 포맷 문자열을 파싱하기 위한 파서
    protected TimestampParser tsParser;

    public NiFiRecordSerDe(RecordReader recordReader, ComponentLog log) {
        this.recordReader = recordReader;
        this.log = log;
    }

    // Hive 테이블 속성(Properties)으로부터 컬럼 이름/타입 목록을 읽어 스키마와 ObjectInspector를 준비한다.
    // Hive Streaming 커넥션이 시작될 때 한 번 호출된다.
    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        List<TypeInfo> columnTypes;
        StructTypeInfo rowTypeInfo;

        log.debug("Initializing NiFiRecordSerDe: {}", tbl.entrySet().toArray());

        // 테이블 속성에서 컬럼 이름 및 컬럼 타입 문자열을 가져온다
        String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        final String columnNameDelimiter = tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? tbl
                .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
        // 테이블의 전체 컬럼 이름 목록
        if (columnNameProperty.isEmpty()) {
            columnNames = new ArrayList<>(0);
        } else {
            columnNames = new ArrayList<>(Arrays.asList(columnNameProperty.split(columnNameDelimiter)));
        }

        // 전체 컬럼 타입 목록
        if (columnTypeProperty.isEmpty()) {
            columnTypes = new ArrayList<>(0);
        } else {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        }

        log.debug("columns: {}, {}", columnNameProperty, columnNames);
        log.debug("types: {}, {} ", columnTypeProperty, columnTypes);

        assert (columnNames.size() == columnTypes.size());

        rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        schema = rowTypeInfo;
        log.debug("schema : {}", schema);
        cachedObjectInspector = (StandardStructObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
        tsParser = new TimestampParser(HiveStringUtils.splitAndUnEscape(tbl.getProperty(serdeConstants.TIMESTAMP_FORMATS)));
        stats = new SerDeStats();
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return ObjectWritable.class;
    }

    // 이 SerDe는 Hive Streaming을 통한 적재(쓰기) 전용으로만 사용되므로 직렬화는 지원하지 않는다.
    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        throw new UnsupportedOperationException("This SerDe only supports deserialization");
    }

    @Override
    public SerDeStats getSerDeStats() {
        return stats;
    }

    // ObjectWritable로 감싸진 NiFi Record를 꺼내어 Hive 컬럼 순서에 맞는 값 목록으로 변환한다.
    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        ObjectWritable t = (ObjectWritable) writable;
        org.apache.nifi.serialization.record.Record record = (org.apache.nifi.serialization.record.Record) t.get();

        List<Object> result = deserialize(record, schema);

        stats.setRowCount(stats.getRowCount() + 1);

        return result;
    }

    // 레코드의 각 필드를 순회하며 대상 스키마(schema)의 컬럼 위치에 맞게 값을 채운 리스트를 만든다. STRUCT 타입의 경우 재귀 호출된다.
    private List<Object> deserialize(org.apache.nifi.serialization.record.Record record, StructTypeInfo schema) throws SerDeException {
        List<Object> result = new ArrayList<>(Collections.nCopies(schema.getAllStructFieldNames().size(), null));

        try {
            RecordSchema recordSchema = record.getSchema();
            for (RecordField field : recordSchema.getFields()) {
                populateRecord(result, record.getValue(field), field, schema);
            }
        } catch (SerDeException se) {
            log.error("Error [{}] parsing Record [{}].", se.toString(), record, se);
            throw se;
        } catch (Exception e) {
            log.error("Error [{}] parsing Record [{}].", e.toString(), record, e);
            throw new SerDeException(e);
        }

        return result;
    }

    // 하나의 필드 값을 NiFi의 DataType/값 표현에서 Hive의 TypeInfo에 대응하는 자바 타입(Hive 전용 타입 포함)으로 변환한다.
    // PRIMITIVE/LIST/MAP/STRUCT 각 카테고리별로 변환 방식이 다르며, LIST/MAP/STRUCT는 재귀적으로 자신을 호출한다.
    @SuppressWarnings("unchecked")
    private Object extractCurrentField(final Object fieldValue, final String fieldName, final DataType fieldDataType, final TypeInfo fieldTypeInfo) throws SerDeException {
        if (fieldValue == null) {
            return null;
        }

        Object val;
        switch (fieldTypeInfo.getCategory()) {
            case PRIMITIVE:
                PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = PrimitiveObjectInspector.PrimitiveCategory.UNKNOWN;
                if (fieldTypeInfo instanceof PrimitiveTypeInfo) {
                    primitiveCategory = ((PrimitiveTypeInfo) fieldTypeInfo).getPrimitiveCategory();
                }
                switch (primitiveCategory) {
                    case BYTE:
                        Integer bIntValue = DataTypeUtils.toInteger(fieldValue, fieldName);
                        val = bIntValue.byteValue();
                        break;
                    case SHORT:
                        Integer sIntValue = DataTypeUtils.toInteger(fieldValue, fieldName);
                        val = sIntValue.shortValue();
                        break;
                    case INT:
                        val = DataTypeUtils.toInteger(fieldValue, fieldName);
                        break;
                    case LONG:
                        val = DataTypeUtils.toLong(fieldValue, fieldName);
                        break;
                    case BOOLEAN:
                        val = DataTypeUtils.toBoolean(fieldValue, fieldName);
                        break;
                    case FLOAT:
                        val = DataTypeUtils.toFloat(fieldValue, fieldName);
                        break;
                    case DOUBLE:
                        val = DataTypeUtils.toDouble(fieldValue, fieldName);
                        break;
                    case STRING:
                    case VARCHAR:
                    case CHAR:
                        val = DataTypeUtils.toString(fieldValue, fieldName);
                        break;
                    case BINARY:
                        final ArrayDataType arrayDataType;
                        if (fieldValue instanceof String) {
                            // 문자열로 들어온 경우 바이트 배열로 간주하여 처리한다
                            arrayDataType = (ArrayDataType) RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
                        } else {
                            arrayDataType = (ArrayDataType) fieldDataType;
                        }
                        Object[] array = DataTypeUtils.toArray(fieldValue, fieldName, arrayDataType.getElementType());
                        val = AvroTypeUtil.convertByteArray(array).array();
                        break;
                    case DATE:
                        final LocalDate localDate = StandardFieldConverterRegistry.getRegistry().getFieldConverter(LocalDate.class)
                                .convertField(fieldValue, Optional.ofNullable(fieldDataType.getFormat()), fieldName);
                        val = org.apache.hadoop.hive.common.type.Date.ofEpochDay((int) localDate.toEpochDay());
                        break;
                    // ORC는 현재 TIMESTAMPLOCALTZ를 지원하지 않는다
                    case TIMESTAMP:
                        Timestamp ts = StandardFieldConverterRegistry.getRegistry().getFieldConverter(Timestamp.class)
                                .convertField(fieldValue, Optional.ofNullable(fieldDataType.getFormat()), fieldName);
                        // Hive의 Timestamp 타입으로 변환한다
                        org.apache.hadoop.hive.common.type.Timestamp hivetimestamp = new org.apache.hadoop.hive.common.type.Timestamp();
                        hivetimestamp.setTimeInMillis(ts.getTime(), ts.getNanos());
                        val = hivetimestamp;
                        break;
                    case DECIMAL:
                        if (fieldValue instanceof BigDecimal) {
                            val = HiveDecimal.create((BigDecimal) fieldValue);
                        } else if (fieldValue instanceof Number) {
                            val = HiveDecimal.create(((Number) fieldValue).doubleValue());
                        } else {
                            val = HiveDecimal.create(DataTypeUtils.toDouble(fieldValue, fieldDataType.getFormat()));
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Field " + fieldName + " cannot be converted to type: " + primitiveCategory.name());
                }
                break;
            case LIST:
                Object[] value = (Object[]) fieldValue;
                ListTypeInfo listTypeInfo = (ListTypeInfo) fieldTypeInfo;
                TypeInfo nestedType = listTypeInfo.getListElementTypeInfo();
                List<Object> converted = new ArrayList<>(value.length);
                for (Object o : value) {
                    converted.add(extractCurrentField(o, fieldName, ((ArrayDataType) fieldDataType).getElementType(), nestedType));
                }
                val = converted;
                break;
            case MAP:
                // NiFi에서 모든 맵의 키는 항상 String이므로 이를 그대로 활용한다
                Map<String, Object> valueMap = (Map<String, Object>) fieldValue;
                MapTypeInfo mapTypeInfo = (MapTypeInfo) fieldTypeInfo;
                Map<Object, Object> convertedMap = new HashMap<>(valueMap.size());
                // 맵의 키/값에 대응하는 레코드 필드가 없으므로, 키와 값 각각에 대해 새로운(합성) 필드 타입 정보를 만들어 변환한다
                for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
                    convertedMap.put(
                            extractCurrentField(entry.getKey(), fieldName + ".key", RecordFieldType.STRING.getDataType(), mapTypeInfo.getMapKeyTypeInfo()),
                            extractCurrentField(entry.getValue(), fieldName + ".value", ((MapDataType) fieldDataType).getValueType(), mapTypeInfo.getMapValueTypeInfo())
                    );
                }
                val = convertedMap;
                break;
            case STRUCT:
                org.apache.nifi.serialization.record.Record nestedRecord = (org.apache.nifi.serialization.record.Record) fieldValue;
                StructTypeInfo s = (StructTypeInfo) fieldTypeInfo;
                val = deserialize(nestedRecord, s);
                break;
            default:
                log.error("Unknown type found: " + fieldTypeInfo + "for field of type: " + fieldDataType.toString());
                return null;
        }
        return val;
    }


    @Override
    public ObjectInspector getObjectInspector() {
        return cachedObjectInspector;
    }


    // 레코드 필드 하나를 대상 스키마(typeInfo)에서 이름으로 찾아 알맞은 위치(fpos)에 변환된 값을 채워 넣는다.
    // 이름으로 찾지 못하면 "_col0" 같은 Hive 내부 컬럼명 패턴인지 확인하여 위치를 추정한다.
    private void populateRecord(List<Object> r, Object value, RecordField field, StructTypeInfo typeInfo) throws SerDeException {

        String fieldName = field.getFieldName();
        String normalizedFieldName = fieldName.toLowerCase();

        // 구조체 필드 이름들을 정규화(소문자화)한 뒤, 지정된(정규화된) 필드 이름을 검색한다
        int fpos = typeInfo.getAllStructFieldNames().stream().map((s) -> s == null ? null : s.toLowerCase()).collect(Collectors.toList()).indexOf(normalizedFieldName);
        if (fpos == -1) {
            Matcher m = INTERNAL_PATTERN.matcher(fieldName);
            fpos = m.matches() ? Integer.parseInt(m.group(1)) : -1;

            log.debug("NPE finding position for field [{}] in schema [{}],"
                    + " attempting to check if it is an internal column name like _col0", fieldName, typeInfo);
            if (fpos == -1) {
                // 알 수 없는 필드이므로 그냥 반환하고 다음 필드로 넘어간다. 파티션 컬럼은 "알 수 없는 필드"로 나타날 수 있으므로 debug 레벨로만 로깅한다.
                log.debug("Field {} is not found in the target table, ignoring...", field.getFieldName());
                return;
            }
            // 여기까지 왔다면 컬럼 이름이 _col0 등 Hive의 내부 컬럼명 패턴과 일치했다는 의미이므로,
            // 반드시 해당 위치의 스키마 컬럼과 일치해야 한다.
            // 즉, 사용자가 임의로 _col0 같은 이름을 컬럼명으로 사용하면서 이를 무시해주길 기대할 수는 없다.
            if (!fieldName.equalsIgnoreCase(HiveConf.getColumnInternalName(fpos))) {
                log.error("Hive internal column name {} and position "
                        + "encoding {} for the column name are at odds", fieldName, fpos);
                throw new SerDeException("Hive internal column name (" + fieldName
                        + ") and position encoding (" + fpos
                        + ") for the column name are at odds");
            }
            // 여기까지 도달했다면 대체 내부 컬럼 매핑을 성공적으로 찾은 것이므로 계속 진행한다.
        }
        Object currField = extractCurrentField(value, fieldName, field.getDataType(), typeInfo.getStructFieldTypeInfo(normalizedFieldName));
        r.set(fpos, currField);
    }

}
