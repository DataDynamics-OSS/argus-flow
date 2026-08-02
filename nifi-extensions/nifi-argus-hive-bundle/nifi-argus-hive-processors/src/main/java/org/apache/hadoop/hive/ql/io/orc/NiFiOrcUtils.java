/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/hadoop/hive/ql/io/orc/NiFiOrcUtils.java
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
/*
 * 참고: 이 클래스는 의도적으로 io.datadynamics.* 가 아닌 org.apache.hadoop.hive.ql.io.orc 패키지에 그대로 남아 있다.
 * hive-exec의 ORC 관련 클래스 중 package-private로 선언된 멤버들 — OrcStruct(int) 생성자,
 * OrcStruct.setFieldValue/createObjectInspector, 그리고 package-private 클래스인 OrcUnion — 을
 * 사용해야 하는데, 이들은 org.apache.hadoop.hive.ql.io.orc 패키지 내부에서만 접근 가능하기 때문이다.
 */
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.*;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.serialization.record.type.*;
import org.apache.orc.MemoryManager;
import org.apache.orc.OrcConf;
import org.apache.orc.impl.MemoryManagerImpl;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * ORC 지원을 위한 유틸리티 메서드 모음 (예: Avro/NiFi 레코드 값을 ORC(Writable) 객체로 변환하거나,
 * NiFi RecordSchema를 Hive의 TypeInfo/DDL 문자열로 변환하는 기능 등).
 * PutORC 계열 프로세서가 NiFi 레코드를 ORC 파일로 기록할 때 스키마 변환과 값 변환에 사용한다.
 */
public class NiFiOrcUtils {

    // ORC Writer가 공유하는 메모리 매니저. writer 생성 시 지연 초기화(lazy init)되어 재사용된다.
    private static MemoryManager memoryManager = null;

    /**
     * 임의의 Java 객체(o)를 지정된 ORC TypeInfo에 대응하는 Writable/ORC 객체로 변환한다.
     * 기본형 래퍼(Integer, Boolean, Long 등)는 대응하는 Writable로, BigDecimal은 HiveDecimalWritable로,
     * Timestamp/Date는 각각 TimestampWritableV2/DateWritableV2로 변환한다. 배열은 리스트로, NiFi Record는
     * OrcStruct로, Map은 재귀적으로 키/값을 변환한 Map으로 변환한다. Union 타입인 경우 실제 객체의 타입과
     * 일치하는 union 분기를 찾아 OrcUnion으로 감싼다. 대응하는 변환 규칙이 없으면 IllegalArgumentException을 던진다.
     *
     * @param typeInfo      변환 대상 값의 ORC 타입 정보
     * @param o             변환할 원본 객체 (null이면 그대로 null 반환)
     * @param hiveFieldNames Hive 필드 이름 규칙(소문자화 등)을 적용할지 여부 - 중첩된 record/구조체 변환 시 전달됨
     * @return ORC(Writable) 형태로 변환된 객체
     */
    public static Object convertToORCObject(TypeInfo typeInfo, Object o, final boolean hiveFieldNames) {
        if (o != null) {
            if (typeInfo instanceof UnionTypeInfo) {
                OrcUnion union = new OrcUnion();
                // 원본 객체(primitive object)가 union 타입 목록 중 어느 것에 해당하는지 찾아야 한다.
                TypeInfo objectTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(
                        ObjectInspectorFactory.getReflectionObjectInspector(o.getClass(), ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
                List<TypeInfo> unionTypeInfos = ((UnionTypeInfo) typeInfo).getAllUnionObjectTypeInfos();

                int index = 0;
                while (index < unionTypeInfos.size() && !unionTypeInfos.get(index).equals(objectTypeInfo)) {
                    index++;
                }
                if (index < unionTypeInfos.size()) {
                    union.set((byte) index, convertToORCObject(objectTypeInfo, o, hiveFieldNames));
                } else {
                    throw new IllegalArgumentException("Object Type for class " + o.getClass().getName() + " not in Union declaration");
                }
                return union;
            }
            if (o instanceof Integer) {
                return new IntWritable((int) o);
            }
            if (o instanceof Boolean) {
                return new BooleanWritable((boolean) o);
            }
            if (o instanceof Long) {
                return new LongWritable((long) o);
            }
            if (o instanceof Float) {
                return new FloatWritable((float) o);
            }
            if (o instanceof Double) {
                return new DoubleWritable((double) o);
            }
            if (o instanceof BigDecimal) {
                return new HiveDecimalWritable(HiveDecimal.create((BigDecimal) o));
            }
            if (o instanceof String) {
                return new Text(o.toString());
            }
            if (o instanceof ByteBuffer) {
                return new BytesWritable(((ByteBuffer) o).array());
            }
            if (o instanceof Timestamp) {
                Timestamp t = (Timestamp) o;
                org.apache.hadoop.hive.common.type.Timestamp timestamp = new org.apache.hadoop.hive.common.type.Timestamp();
                timestamp.setTimeInMillis(t.getTime(), t.getNanos());
                return new TimestampWritableV2(timestamp);
            }
            if (o instanceof Date) {
                Date d = (Date) o;
                org.apache.hadoop.hive.common.type.Date date = new org.apache.hadoop.hive.common.type.Date();
                date.setTimeInMillis(d.getTime());
                return new DateWritableV2(date);
            }
            if (o instanceof Object[]) {
                Object[] objArray = (Object[]) o;
                if (TypeInfoFactory.binaryTypeInfo.equals(typeInfo)) {
                    byte[] dest = new byte[objArray.length];
                    for (int i = 0; i < objArray.length; i++) {
                        dest[i] = (byte) objArray[i];
                    }
                    return new BytesWritable(dest);
                } else {
                    // 바이너리 타입이 아니면 객체 리스트로 간주하고 처리한다.
                    TypeInfo listTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
                    return Arrays.stream(objArray)
                            .map(o1 -> convertToORCObject(listTypeInfo, o1, hiveFieldNames))
                            .collect(Collectors.toList());
                }
            }
            if (o instanceof int[]) {
                int[] intArray = (int[]) o;
                return Arrays.stream(intArray)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("int"), element, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof long[]) {
                long[] longArray = (long[]) o;
                return Arrays.stream(longArray)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("bigint"), element, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof float[]) {
                float[] floatArray = (float[]) o;
                return IntStream.range(0, floatArray.length)
                        .mapToDouble(i -> floatArray[i])
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("float"), (float) element, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof double[]) {
                double[] doubleArray = (double[]) o;
                return Arrays.stream(doubleArray)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("double"), element, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof boolean[]) {
                boolean[] booleanArray = (boolean[]) o;
                return IntStream.range(0, booleanArray.length)
                        .map(i -> booleanArray[i] ? 1 : 0)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("boolean"), element == 1, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof List) {
                return o;
            }
            if (o instanceof org.apache.nifi.serialization.record.Record) {
                org.apache.nifi.serialization.record.Record record = (org.apache.nifi.serialization.record.Record) o;
                TypeInfo recordSchema = NiFiOrcUtils.getOrcSchema(record.getSchema(), hiveFieldNames);
                List<RecordField> recordFields = record.getSchema().getFields();
                if (recordFields != null) {
                    Object[] fieldObjects = new Object[recordFields.size()];
                    for (int i = 0; i < recordFields.size(); i++) {
                        RecordField field = recordFields.get(i);
                        DataType dataType = field.getDataType();
                        Object fieldObject = record.getValue(field);
                        fieldObjects[i] = convertToORCObject(NiFiOrcUtils.getOrcField(dataType, hiveFieldNames), fieldObject, hiveFieldNames);
                    }
                    return NiFiOrcUtils.createOrcStruct(recordSchema, fieldObjects);
                }
                return null;
            }
            if (o instanceof Map) {
                Map map = new HashMap();
                MapTypeInfo mapTypeInfo = ((MapTypeInfo) typeInfo);
                TypeInfo keyInfo = mapTypeInfo.getMapKeyTypeInfo();
                TypeInfo valueInfo = mapTypeInfo.getMapValueTypeInfo();
                // Map의 키/값 타입으로는 Union이 허용되지 않으므로, key와 value 객체를 변환하면
                // 항상 Writable 객체가 반환되어야 한다.
                ((Map) o).forEach((key, value) -> {
                    Object keyObject = convertToORCObject(keyInfo, key, hiveFieldNames);
                    Object valueObject = convertToORCObject(valueInfo, value, hiveFieldNames);
                    if (keyObject == null) {
                        throw new IllegalArgumentException("Maps' key cannot be null");
                    }
                    map.put(keyObject, valueObject);
                });
                return map;
            }
            throw new IllegalArgumentException("Error converting object of type " + o.getClass().getName() + " to ORC type " + typeInfo.getTypeName());
        } else {
            return null;
        }
    }

    /**
     * TypeInfo(ORC 레코드 스키마)와 필드 값 목록으로부터 OrcStruct 객체를 생성한다.
     * package-private인 OrcStruct.createObjectInspector와 setStructFieldData를 사용해야 하므로
     * 이 클래스가 org.apache.hadoop.hive.ql.io.orc 패키지에 위치해야 하는 이유이기도 하다.
     *
     * @param typeInfo ORC 레코드 스키마를 표현하는 TypeInfo 객체
     * @param objs     ORC 객체/Writable 값 목록 (필드 순서와 일치해야 함)
     * @return 지정한 스키마에 맞춰 값이 채워진 OrcStruct
     */
    @SuppressWarnings("unchecked")
    public static OrcStruct createOrcStruct(TypeInfo typeInfo, Object... objs) {
        SettableStructObjectInspector oi = (SettableStructObjectInspector) OrcStruct
                .createObjectInspector(typeInfo);
        List<StructField> fields = (List<StructField>) oi.getAllStructFieldRefs();
        OrcStruct result = (OrcStruct) oi.create();
        result.setNumFields(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            oi.setStructFieldData(result, fields.get(i), objs[i]);
        }
        return result;
    }

    // Hive 테이블 이름에 사용할 수 없는 마침표와 공백을 밑줄로 치환하여 이름을 정규화한다.
    public static String normalizeHiveTableName(String name) {
        return name.replaceAll("[\\. ]", "_");
    }

    /**
     * NiFi RecordSchema로부터 ORC 저장 형식의 외부 테이블(EXTERNAL TABLE)을 생성하는 Hive DDL 문을 만든다.
     * 테이블 이름은 마침표 단위로 분리해 각 구간을 백틱(`)으로 감싸며(예: DB.TABLE), 각 필드는
     * 이름과 getHiveTypeFromFieldType으로 변환한 Hive 타입을 나열한다.
     *
     * @param recordSchema   변환할 NiFi 레코드 스키마
     * @param tableName      생성할 테이블 이름 (DB.TABLE 형식 가능)
     * @param hiveFieldNames true이면 필드 이름을 소문자로 변환하여 Hive 명명 규칙을 따르게 함
     * @return "CREATE EXTERNAL TABLE IF NOT EXISTS ..." 형태의 DDL 문자열
     */
    public static String generateHiveDDL(RecordSchema recordSchema, String tableName, boolean hiveFieldNames) {
        StringBuilder sb = new StringBuilder("CREATE EXTERNAL TABLE IF NOT EXISTS ");
        String[] tableSections = tableName.split("\\.");
        String quotedTableName = Arrays.stream(tableSections).map((section) -> "`" + section + "`").collect(Collectors.joining("."));
        sb.append(quotedTableName);
        sb.append(" (");
        List<String> hiveColumns = new ArrayList<>();
        List<RecordField> fields = recordSchema.getFields();
        if (fields != null) {
            hiveColumns.addAll(
                    fields.stream().map(field -> "`" + (hiveFieldNames ? field.getFieldName().toLowerCase() : field.getFieldName()) + "` "
                            + getHiveTypeFromFieldType(field.getDataType(), hiveFieldNames)).collect(Collectors.toList()));
        }
        sb.append(StringUtils.join(hiveColumns, ", "));
        sb.append(") STORED AS ORC");
        return sb.toString();

    }

    /**
     * NiFi RecordSchema 전체를 ORC의 struct TypeInfo로 변환한다. 각 필드 이름과 getOrcField로 변환한
     * 필드 타입을 모아 TypeInfoFactory.getStructTypeInfo로 구조체 타입 정보를 생성한다.
     */
    public static TypeInfo getOrcSchema(RecordSchema recordSchema, boolean hiveFieldNames) throws IllegalArgumentException {
        List<RecordField> recordFields = recordSchema.getFields();
        if (recordFields != null) {
            List<String> orcFieldNames = new ArrayList<>(recordFields.size());
            List<TypeInfo> orcFields = new ArrayList<>(recordFields.size());
            recordFields.forEach(recordField -> {
                String fieldName = hiveFieldNames ? recordField.getFieldName().toLowerCase() : recordField.getFieldName();
                orcFieldNames.add(fieldName);
                orcFields.add(getOrcField(recordField.getDataType(), hiveFieldNames));
            });
            return TypeInfoFactory.getStructTypeInfo(orcFieldNames, orcFields);
        }
        return null;
    }

    /**
     * NiFi의 DataType(RecordFieldType) 하나를 그에 대응하는 ORC TypeInfo로 변환한다. 기본형(INT, LONG,
     * BOOLEAN, DOUBLE, FLOAT, STRING, ENUM)은 getPrimitiveOrcTypeFromPrimitiveFieldType에 위임하고,
     * DECIMAL/DATE/TIME/TIMESTAMP는 각각 대응하는 Hive 타입으로, ARRAY는 리스트(바이트 배열은 binary)로,
     * CHOICE(Union)는 하위 타입들을 변환한 뒤 하나만 남으면 평탄화하고 그렇지 않으면 union 타입으로,
     * MAP은 키를 문자열로 고정한 맵 타입으로, RECORD는 재귀적으로 struct 타입으로 변환한다.
     * 대응되는 ORC 타입이 없으면 IllegalArgumentException을 던진다.
     */
    public static TypeInfo getOrcField(DataType dataType, boolean hiveFieldNames) throws IllegalArgumentException {
        if (dataType == null) {
            return null;
        }

        RecordFieldType fieldType = dataType.getFieldType();
        if (RecordFieldType.INT.equals(fieldType)
                || RecordFieldType.LONG.equals(fieldType)
                || RecordFieldType.BOOLEAN.equals(fieldType)
                || RecordFieldType.DOUBLE.equals(fieldType)
                || RecordFieldType.FLOAT.equals(fieldType)
                || RecordFieldType.STRING.equals(fieldType)
                || RecordFieldType.ENUM.equals(fieldType)) {
            return getPrimitiveOrcTypeFromPrimitiveFieldType(dataType);
        }

        if (RecordFieldType.DECIMAL.equals(fieldType)) {
            DecimalDataType decimalDataType = (DecimalDataType) dataType;
            return TypeInfoFactory.getDecimalTypeInfo(decimalDataType.getPrecision(), decimalDataType.getScale());
        }
        if (RecordFieldType.DATE.equals(fieldType)) {
            return TypeInfoFactory.dateTypeInfo;
        }
        if (RecordFieldType.TIME.equals(fieldType)) {
            return TypeInfoFactory.intTypeInfo;
        }
        if (RecordFieldType.TIMESTAMP.equals(fieldType)) {
            return TypeInfoFactory.timestampTypeInfo;
        }
        if (RecordFieldType.ARRAY.equals(fieldType)) {
            ArrayDataType arrayDataType = (ArrayDataType) dataType;
            if (RecordFieldType.BYTE.getDataType().equals(arrayDataType.getElementType())) {
                return TypeInfoFactory.getPrimitiveTypeInfo("binary");
            }
            return TypeInfoFactory.getListTypeInfo(getOrcField(arrayDataType.getElementType(), hiveFieldNames));
        }
        if (RecordFieldType.CHOICE.equals(fieldType)) {
            ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
            List<DataType> unionFieldSchemas = choiceDataType.getPossibleSubTypes();

            if (unionFieldSchemas != null) {
                // union 내의 null 타입은 무시한다.
                List<TypeInfo> orcFields = unionFieldSchemas.stream()
                        .map((it) -> NiFiOrcUtils.getOrcField(it, hiveFieldNames))
                        .collect(Collectors.toList());

                // union에 null이 아닌 요소가 하나뿐이면 굳이 union으로 두지 않고 해당 타입으로 평탄화한다.
                if (orcFields.size() == 1) {
                    return orcFields.get(0);
                } else {
                    return TypeInfoFactory.getUnionTypeInfo(orcFields);
                }
            }
            return null;
        }
        if (RecordFieldType.MAP.equals(fieldType)) {
            MapDataType mapDataType = (MapDataType) dataType;
            return TypeInfoFactory.getMapTypeInfo(
                    getPrimitiveOrcTypeFromPrimitiveFieldType(RecordFieldType.STRING.getDataType()),
                    getOrcField(mapDataType.getValueType(), hiveFieldNames));
        }
        if (RecordFieldType.RECORD.equals(fieldType)) {
            RecordDataType recordDataType = (RecordDataType) dataType;
            List<RecordField> recordFields = recordDataType.getChildSchema().getFields();
            if (recordFields != null) {
                List<String> orcFieldNames = new ArrayList<>(recordFields.size());
                List<TypeInfo> orcFields = new ArrayList<>(recordFields.size());
                recordFields.forEach(recordField -> {
                    String fieldName = hiveFieldNames ? recordField.getFieldName().toLowerCase() : recordField.getFieldName();
                    orcFieldNames.add(fieldName);
                    orcFields.add(getOrcField(recordField.getDataType(), hiveFieldNames));
                });
                return TypeInfoFactory.getStructTypeInfo(orcFieldNames, orcFields);
            }
            return null;
        }

        throw new IllegalArgumentException("Did not recognize field type " + fieldType.name());
    }

    /**
     * NiFi의 기본형(primitive) RecordFieldType을 대응하는 ORC 기본형 TypeInfo로 변환한다.
     * 기본형이 아닌 타입이 전달되면 IllegalArgumentException을 던진다.
     */
    public static TypeInfo getPrimitiveOrcTypeFromPrimitiveFieldType(DataType rawDataType) throws IllegalArgumentException {
        if (rawDataType == null) {
            throw new IllegalArgumentException("Avro type is null");
        }
        RecordFieldType fieldType = rawDataType.getFieldType();
        if (RecordFieldType.INT.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("int");
        }
        if (RecordFieldType.LONG.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("bigint");
        }
        if (RecordFieldType.BOOLEAN.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("boolean");
        }
        if (RecordFieldType.DOUBLE.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("double");
        }
        if (RecordFieldType.FLOAT.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("float");
        }
        if (RecordFieldType.STRING.equals(fieldType) || RecordFieldType.ENUM.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("string");
        }

        throw new IllegalArgumentException("Field type " + fieldType.name() + " is not a primitive type");
    }

    /**
     * NiFi RecordSchema를 "STRUCT&lt;field1:TYPE1, field2:TYPE2, ...&gt;" 형태의 Hive 스키마 문자열로 변환한다.
     */
    public static String getHiveSchema(RecordSchema recordSchema, boolean hiveFieldNames) throws IllegalArgumentException {
        List<RecordField> recordFields = recordSchema.getFields();
        if (recordFields != null) {
            List<String> hiveFields = new ArrayList<>(recordFields.size());
            recordFields.forEach(recordField -> {
                hiveFields.add((hiveFieldNames ? recordField.getFieldName().toLowerCase() : recordField.getFieldName())
                        + ":" + getHiveTypeFromFieldType(recordField.getDataType(), hiveFieldNames));
            });
            return "STRUCT<" + StringUtils.join(hiveFields, ", ") + ">";
        }
        return null;
    }

    /**
     * NiFi DataType을 그에 대응하는 Hive DDL 타입 이름(문자열)으로 변환한다. 기본형은 대문자 Hive 타입명으로,
     * ARRAY/MAP/CHOICE(UNIONTYPE)/RECORD(STRUCT)는 재귀적으로 구성 타입을 변환하여 복합 타입 문자열을 만든다.
     * generateHiveDDL과 getHiveSchema에서 컬럼/필드 타입 표현에 사용된다.
     */
    public static String getHiveTypeFromFieldType(DataType rawDataType, boolean hiveFieldNames) {
        if (rawDataType == null) {
            throw new IllegalArgumentException("Field type is null");
        }
        RecordFieldType dataType = rawDataType.getFieldType();

        if (RecordFieldType.INT.equals(dataType)) {
            return "INT";
        }
        if (RecordFieldType.LONG.equals(dataType)) {
            return "BIGINT";
        }
        if (RecordFieldType.BOOLEAN.equals(dataType)) {
            return "BOOLEAN";
        }
        if (RecordFieldType.DOUBLE.equals(dataType)) {
            return "DOUBLE";
        }
        if (RecordFieldType.FLOAT.equals(dataType)) {
            return "FLOAT";
        }
        if (RecordFieldType.DECIMAL.equals(dataType)) {
            return "DECIMAL";
        }
        if (RecordFieldType.STRING.equals(dataType) || RecordFieldType.ENUM.equals(dataType)) {
            return "STRING";
        }
        if (RecordFieldType.DATE.equals(dataType)) {
            return "DATE";
        }
        if (RecordFieldType.TIME.equals(dataType)) {
            return "INT";
        }
        if (RecordFieldType.TIMESTAMP.equals(dataType)) {
            return "TIMESTAMP";
        }
        if (RecordFieldType.ARRAY.equals(dataType)) {
            ArrayDataType arrayDataType = (ArrayDataType) rawDataType;
            if (RecordFieldType.BYTE.getDataType().equals(arrayDataType.getElementType())) {
                return "BINARY";
            }
            return "ARRAY<" + getHiveTypeFromFieldType(arrayDataType.getElementType(), hiveFieldNames) + ">";
        }
        if (RecordFieldType.MAP.equals(dataType)) {
            MapDataType mapDataType = (MapDataType) rawDataType;
            return "MAP<STRING, " + getHiveTypeFromFieldType(mapDataType.getValueType(), hiveFieldNames) + ">";
        }
        if (RecordFieldType.CHOICE.equals(dataType)) {
            ChoiceDataType choiceDataType = (ChoiceDataType) rawDataType;
            List<DataType> unionFieldSchemas = choiceDataType.getPossibleSubTypes();

            if (unionFieldSchemas != null) {
                // union 내의 null 타입은 무시한다.
                List<String> hiveFields = unionFieldSchemas.stream()
                        .map((it) -> getHiveTypeFromFieldType(it, hiveFieldNames))
                        .collect(Collectors.toList());

                // union에 null이 아닌 요소가 하나뿐이면 UNIONTYPE으로 감싸지 않고 해당 타입으로 평탄화한다.
                return (hiveFields.size() == 1)
                        ? hiveFields.get(0)
                        : "UNIONTYPE<" + StringUtils.join(hiveFields, ", ") + ">";
            }
            return null;
        }

        if (RecordFieldType.RECORD.equals(dataType)) {
            RecordDataType recordDataType = (RecordDataType) rawDataType;
            List<RecordField> recordFields = recordDataType.getChildSchema().getFields();
            if (recordFields != null) {
                List<String> hiveFields = recordFields.stream().map(
                        recordField -> ("`" + (hiveFieldNames ? recordField.getFieldName().toLowerCase() : recordField.getFieldName()) + "`:"
                                + getHiveTypeFromFieldType(recordField.getDataType(), hiveFieldNames))).collect(Collectors.toList());
                return "STRUCT<" + StringUtils.join(hiveFields, ", ") + ">";
            }
            return null;
        }

        throw new IllegalArgumentException("Error converting Avro type " + dataType.name() + " to Hive type");
    }

    /**
     * 주어진 경로와 스키마(orcSchema)로 ORC Writer를 생성한다. Hadoop Configuration에 설정된
     * OrcConf 값들(row index stride, block padding, write format 버전, encoding strategy,
     * block padding tolerance, block size, bloom filter false-positive 확률 등)을 읽어 WriterOptions에
     * 반영하고, 공유 MemoryManager를 사용해 여러 Writer 간 메모리 사용량을 조율한다.
     *
     * @param path       ORC 파일을 생성할 경로
     * @param conf       ORC 관련 설정값을 담은 Hadoop Configuration
     * @param orcSchema  ORC 레코드 스키마
     * @param stripeSize 스트라이프(stripe) 크기
     * @param compress   압축 방식
     * @param bufferSize 버퍼 크기
     * @return 생성된 ORC Writer
     */
    public static Writer createWriter(
            Path path,
            Configuration conf,
            TypeInfo orcSchema,
            long stripeSize,
            CompressionKind compress,
            int bufferSize) throws IOException {

        int rowIndexStride = (int) OrcConf.ROW_INDEX_STRIDE.getLong(conf);

        boolean addBlockPadding = OrcConf.BLOCK_PADDING.getBoolean(conf);

        String versionName = OrcConf.WRITE_FORMAT.getString(conf);
        OrcFile.Version versionValue = (versionName == null)
                ? OrcFile.Version.CURRENT
                : OrcFile.Version.byName(versionName);

        OrcFile.EncodingStrategy encodingStrategy;
        String enString = OrcConf.ENCODING_STRATEGY.getString(conf);
        if (enString == null) {
            encodingStrategy = OrcFile.EncodingStrategy.SPEED;
        } else {
            encodingStrategy = OrcFile.EncodingStrategy.valueOf(enString);
        }

        final double paddingTolerance = OrcConf.BLOCK_PADDING_TOLERANCE.getDouble(conf);

        long blockSizeValue = OrcConf.BLOCK_SIZE.getLong(conf);

        double bloomFilterFpp = OrcConf.BLOOM_FILTER_FPP.getDouble(conf);

        ObjectInspector inspector = OrcStruct.createObjectInspector(orcSchema);

        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
                .rowIndexStride(rowIndexStride)
                .blockPadding(addBlockPadding)
                .version(versionValue)
                .encodingStrategy(encodingStrategy)
                .paddingTolerance(paddingTolerance)
                .blockSize(blockSizeValue)
                .bloomFilterFpp(bloomFilterFpp)
                .memory(getMemoryManager(conf))
                .inspector(inspector)
                .stripeSize(stripeSize)
                .bufferSize(bufferSize)
                .compress(compress);

        return OrcFile.createWriter(path, writerOptions);
    }

    // MemoryManager를 지연 초기화하여 여러 ORC Writer가 하나의 인스턴스를 공유하도록 한다.
    private static synchronized MemoryManager getMemoryManager(Configuration conf) {
        if (memoryManager == null) {
            memoryManager = new MemoryManagerImpl(conf);
        }
        return memoryManager;
    }
}