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
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/util/ArgumentUtils.java
 */
package io.datadynamics.nifi.processors.command;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 명령행 인자 문자열을 구분자(delimiter) 기준으로 분리하는 유틸리티 클래스.
 * 탭, 캐리지 리턴, 줄바꿈 문자와 사용자가 지정한 구분자를 기준으로 인자를 나눈다.
 */
public class ArgumentUtils {
    // 따옴표 문자. 과거에는 따옴표로 감싼 구간을 하나의 인자로 처리하는 데 사용했으나
    // 현재는 아래 splitArgs()에서 관련 로직이 주석 처리되어 있어 실제로는 사용되지 않는다.
    private final static char QUOTE = '"';
    // 인자를 구분하는 기본 구분 문자 목록(탭, 캐리지 리턴, 줄바꿈)
    private final static List<Character> DELIMITING_CHARACTERS = new ArrayList<>(3);

    static {
        DELIMITING_CHARACTERS.add('\t');
        DELIMITING_CHARACTERS.add('\r');
        DELIMITING_CHARACTERS.add('\n');
    }

    /**
     * 입력 문자열을 지정된 구분자와 기본 구분 문자(탭/CR/LF) 기준으로 분리하여
     * 인자 목록을 반환한다.
     *
     * @param input            분리할 원본 문자열. null인 경우 빈 목록을 반환한다.
     * @param definedDelimiter 사용자가 지정한 추가 구분 문자
     * @return 분리된 인자들의 목록
     */
    public static List<String> splitArgs(final String input, final char definedDelimiter) {
        if (input == null) {
            return Collections.emptyList();
        }

        final List<String> args = new ArrayList<>();

        // 과거 따옴표 처리 로직의 흔적으로, 현재는 항상 false로 유지된다(아래 주석 처리된 블록 참고).
        boolean inQuotes = false;
        final StringBuilder sb = new StringBuilder();

        for (int i = 0; i < input.length(); i++) {
            final char c = input.charAt(i);

            // 구분 문자를 만나면 지금까지 누적된 문자열을 하나의 인자로 확정하고 버퍼를 초기화한다.
            if (DELIMITING_CHARACTERS.contains(c) || c == definedDelimiter) {
                if (inQuotes) {
                    sb.append(c);
                } else {
                    final String arg = sb.toString();
                    args.add(arg);
                    sb.setLength(0);
                }
                continue;
            }

/* Processor의 인자에 "" 으로 감싸면 이 코드때문에 ""이 날라간다.
            if (c == QUOTE) {
                inQuotes = !inQuotes;
                continue;
            }
*/

            sb.append(c);
        }

        // 마지막 구분자 이후 남아있는 문자열도 하나의 인자로 추가한다.
        args.add(sb.toString());

        return args;
    }
}
