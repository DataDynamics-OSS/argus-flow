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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/nifi/util/hive/CsvOutputOptions.java
 */
package io.datadynamics.nifi.processors.hive.util;

/**
 * Hive 쿼리 결과 등을 CSV로 변환할 때 사용하는 출력 형식 옵션을 담는 값 객체(value object).
 * 헤더 포함 여부, 구분자, 인용/이스케이프 처리, FlowFile당 최대 행 수 등
 * 프로세서의 프로퍼티 값을 하나로 묶어 CSV 작성 로직에 전달하기 위해 사용한다.
 */
public class CsvOutputOptions {

    // CSV 첫 줄에 컬럼명을 헤더로 출력할지 여부.
    private boolean header = true;
    // header가 true이면서 컬럼명 대신 사용할 대체 헤더 문자열. null이면 실제 컬럼명을 사용한다.
    private String altHeader = null;
    // 각 컬럼 값을 구분하는 구분자.
    private String delimiter = ",";
    // 컬럼 값을 인용부호로 감쌀지 여부.
    private boolean quote = false;
    // 값에 포함된 구분자/인용부호 등의 특수문자를 이스케이프 처리할지 여부.
    private boolean escape = true;

    // 하나의 FlowFile에 담을 최대 행(row) 수. 0이면 제한 없이 전체 결과를 하나의 FlowFile에 담는다.
    private int maxRowsPerFlowFile = 0;

    public CsvOutputOptions(boolean header, String altHeader, String delimiter, boolean quote, boolean escape, int maxRowsPerFlowFile) {
        this.header = header;
        this.altHeader = altHeader;
        this.delimiter = delimiter;
        this.quote = quote;
        this.escape = escape;
        this.maxRowsPerFlowFile = maxRowsPerFlowFile;
    }

    public boolean isHeader() {
        return header;
    }

    public String getAltHeader() {
        return altHeader;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public boolean isQuote() {
        return quote;
    }

    public boolean isEscape() {
        return escape;
    }

    public int getMaxRowsPerFlowFile() {
        return maxRowsPerFlowFile;
    }
}
