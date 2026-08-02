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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/nifi/processors/orc/PutORC.java
 */
package io.datadynamics.nifi.processors.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.NiFiOrcUtils;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.AbstractPutHDFSRecord;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import io.datadynamics.nifi.processors.orc.record.ORCHDFSRecordWriter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * FlowFile로부터 레코드를 읽어 ORC(Optimized Row Columnar) 포맷으로 HDFS(또는 로컬 파일시스템)에 기록하는 프로세서.
 * 실제 ORC 파일 작성 로직은 {@link ORCHDFSRecordWriter}에 위임하고, 이 클래스는 스트라이프 크기, 버퍼 크기,
 * 압축 방식, Hive 테이블명/필드명 정규화 여부 등 ORC 관련 프로퍼티 정의 및 Writer 생성을 담당한다.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"cloudera", "put", "ORC", "hadoop", "HDFS", "filesystem", "restricted", "record"})
@CapabilityDescription("제공된 Record Reader를 사용하여 유입되는 FlowFile로부터 레코드를 읽고, "
        + "설정에 지정된 위치/파일시스템의 ORC 파일에 해당 레코드를 기록한다.")
@ReadsAttribute(attribute = "filename", description = "기록할 파일의 이름은 이 속성 값으로부터 가져온다.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "파일의 이름이 이 속성에 저장된다."),
        @WritesAttribute(attribute = "absolute.hdfs.path", description = "파일의 절대 경로가 이 속성에 저장된다."),
        @WritesAttribute(attribute = "hadoop.file.url", description = "파일의 hadoop URL이 이 속성에 저장된다."),
        @WritesAttribute(attribute = "record.count", description = "ORC 파일에 기록된 레코드 수"),
        @WritesAttribute(attribute = "hive.ddl", description = "대상 폴더로부터 Hive에 외부 테이블(external table)을 생성하기 위한 "
                + "부분적인 Hive DDL 구문을 생성한다. ReplaceText에서 콘텐츠를 이 DDL로 설정하는 데 사용할 수 있다. "
                + "유효한 DDL로 만들려면 \"LOCATION '<path_to_orc_file_in_hdfs>'\"를 추가해야 하며, 여기서 경로는 HDFS 상에서 "
                + "이 ORC 파일이 위치한 디렉토리이다. 예를 들어 이 프로세서는 FlowFile을 다운스트림의 ReplaceText로 보내 "
                + "콘텐츠를 이 DDL(및 위에서 설명한 LOCATION 절)로 설정한 뒤, PutHiveQL 프로세서로 보내 테이블이 없을 경우 생성하도록 할 수 있다.")
})
@Restricted(restrictions = {
        @Restriction(
                requiredPermission = RequiredPermission.WRITE_DISTRIBUTED_FILESYSTEM,
                explanation = "운영자가 NiFi가 접근 가능한 HDFS 또는 로컬 파일시스템 내 모든 파일에 쓸 수 있는 권한을 부여한다.")
})
public class PutORC extends AbstractPutHDFSRecord {

    public static final String HIVE_DDL_ATTRIBUTE = "hive.ddl";

    public static final PropertyDescriptor ORC_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("putorc-config-resources")
            .displayName("ORC 설정 리소스")
            .description("ORC 설정(예: hive-site.xml)을 담고 있는 파일 또는 콤마로 구분된 파일 목록. 이 값을 지정하지 않으면 "
                    + "Hadoop이 classpath에서 'hive-site.xml' 파일을 찾거나 기본 설정으로 대체한다. 자세한 내용은 ORC 문서를 참고한다.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .build();

    public static final PropertyDescriptor STRIPE_SIZE = new PropertyDescriptor.Builder()
            .name("putorc-stripe-size")
            .displayName("스트라이프 크기")
            .description("ORC 파일에 스트라이프(stripe)를 기록하기 위한 메모리 버퍼의 크기(바이트 단위)")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("64 MB")
            .build();

    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("putorc-buffer-size")
            .displayName("버퍼 크기")
            .description("스트라이프를 압축하여 메모리에 저장하는 데 사용되는 메모리 버퍼의 최대 크기(바이트 단위). 이는 ORC writer에 대한 힌트일 뿐이며, "
                    + "효율적인 스트라이프 작성과 메모리 사용을 위해 스트라이프 크기와 컬럼 수에 따라 더 작은 버퍼 크기를 선택할 수도 있다.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("10 KB")
            .build();
    public static final List<AllowableValue> COMPRESSION_TYPES;
    static final PropertyDescriptor HIVE_TABLE_NAME = new PropertyDescriptor.Builder()
            .name("putorc-hive-table-name")
            .displayName("Hive 테이블명")
            .description("hive.ddl 속성에 삽입할 테이블명(선택 사항). 생성된 DDL은 (보통 PutHDFS 프로세서 이후에 실행되는) "
                    + "PutHive3QL 프로세서가 변환된 ORC 파일을 기반으로 하는 테이블을 생성하는 데 사용할 수 있다. "
                    + "이 프로퍼티를 지정하지 않으면, 유입된 Avro 레코드의 전체 이름(네임스페이스 포함)을 정규화하여 "
                    + "테이블명으로 사용한다.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    static final PropertyDescriptor HIVE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("putorc-hive-field-names")
            .displayName("Hive를 위한 필드명 정규화")
            .description("Hive를 위해 필드명을 정규화할지 여부(예: 강제로 소문자화). ORC 파일이 Hive 테이블의 일부로 "
                    + "사용될 예정이라면 이 프로퍼티를 true로 설정해야 한다. 스키마의 원래 필드명을 그대로 유지하려면 "
                    + "이 프로퍼티를 false로 설정해야 한다.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    // ORC가 지원하는 압축 방식 목록. NONE(압축 안 함)을 기본값으로 사용한다.
    static {
        final List<AllowableValue> compressionTypes = new ArrayList<>();
        compressionTypes.add(new AllowableValue("NONE", "NONE", "압축하지 않음"));
        compressionTypes.add(new AllowableValue("ZLIB", "ZLIB", "ZLIB 압축"));
        compressionTypes.add(new AllowableValue("SNAPPY", "SNAPPY", "Snappy 압축"));
        compressionTypes.add(new AllowableValue("LZO", "LZO", "LZO 압축"));
        COMPRESSION_TYPES = Collections.unmodifiableList(compressionTypes);
    }

    @Override
    public List<AllowableValue> getCompressionTypes(final ProcessorInitializationContext context) {
        return COMPRESSION_TYPES;
    }

    @Override
    public String getDefaultCompressionType(final ProcessorInitializationContext context) {
        return "NONE";
    }

    // 부모 클래스(AbstractPutHDFSRecord)의 공통 프로퍼티에 더해, ORC 파일 작성에 필요한 프로퍼티들을 추가로 노출한다.
    @Override
    public List<PropertyDescriptor> getAdditionalProperties() {
        final List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(ORC_CONFIGURATION_RESOURCES);
        _propertyDescriptors.add(STRIPE_SIZE);
        _propertyDescriptors.add(BUFFER_SIZE);
        _propertyDescriptors.add(HIVE_TABLE_NAME);
        _propertyDescriptors.add(HIVE_FIELD_NAMES);
        return Collections.unmodifiableList(_propertyDescriptors);
    }

    // 설정된 프로퍼티(스트라이프/버퍼 크기, 압축 방식, 필드명 정규화 여부 등)를 바탕으로
    // 실제 ORC 파일을 기록할 ORCHDFSRecordWriter를 생성한다.
    // Hive 테이블명이 명시적으로 설정되지 않은 경우, 스키마 식별자로부터 이름을 유추하여 정규화한다.
    @Override
    public HDFSRecordWriter createHDFSRecordWriter(final ProcessContext context, final FlowFile flowFile, final Configuration conf, final Path path, final RecordSchema schema)
            throws IOException, SchemaNotFoundException {

        final long stripeSize = context.getProperty(STRIPE_SIZE).asDataSize(DataUnit.B).longValue();
        final int bufferSize = context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final CompressionKind compressionType = CompressionKind.valueOf(context.getProperty(COMPRESSION_TYPE).getValue());
        final boolean normalizeForHive = context.getProperty(HIVE_FIELD_NAMES).asBoolean();
        TypeInfo orcSchema = NiFiOrcUtils.getOrcSchema(schema, normalizeForHive);
        final Writer orcWriter = NiFiOrcUtils.createWriter(path, conf, orcSchema, stripeSize, compressionType, bufferSize);
        final String hiveTableName = context.getProperty(HIVE_TABLE_NAME).isSet()
                ? context.getProperty(HIVE_TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue()
                : NiFiOrcUtils.normalizeHiveTableName(schema.getIdentifier().getName().orElse("unknown"));
        final boolean hiveFieldNames = context.getProperty(HIVE_FIELD_NAMES).asBoolean();

        return new ORCHDFSRecordWriter(orcWriter, schema, hiveTableName, hiveFieldNames);
    }
}
