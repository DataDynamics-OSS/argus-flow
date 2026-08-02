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
 *   nifi-nar-bundles/nifi-extension-utils/nifi-database-utils/src/main/java/org/apache/nifi/util/db/AvroUtil.java
 */
package io.datadynamics.nifi.processors.db.executesql;


import org.apache.avro.file.CodecFactory;

/**
 * Avro 압축 코덱 관련 유틸리티. COMPRESSION_FORMAT 속성 값(문자열)을 실제 Avro CodecFactory
 * 인스턴스로 변환하는 역할을 한다.
 */
public class AvroUtil {

    /**
     * 속성 값으로 지정된 코덱 이름을 실제 Avro CodecFactory로 변환한다.
     * 지원하지 않거나 NONE인 경우 압축을 사용하지 않는 nullCodec을 반환한다.
     */
    public static CodecFactory getCodecFactory(String property) {
        CodecType type = CodecType.valueOf(property);
        switch (type) {
            case BZIP2:
                return CodecFactory.bzip2Codec();
            case DEFLATE:
                return CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
            case LZO:
                return CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL);
            case SNAPPY:
                return CodecFactory.snappyCodec();
            case NONE:
            default:
                return CodecFactory.nullCodec();
        }
    }

    public static enum CodecType {
        BZIP2,
        DEFLATE,
        NONE,
        SNAPPY,
        LZO
    }

}