/*
 * Copyright 2026 Data Dynamics Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.processors.command;

import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * {@link ArgumentUtils}의 인자 분리(splitArgs) 동작을 검증하는 테스트 클래스.
 */
public class ArgumentUtilsTest {

    /**
     * 세미콜론(;)을 구분자로 사용하여 JDBC 접속 문자열이 포함된 명령행 인자를
     * 올바르게 분리하는지 확인한다.
     */
    @Test
    public void splitArgs() {
        List<String> strings = ArgumentUtils.splitArgs("-u;\"jdbc:hive2://hive.example.com:10000/default;principal=hive@EXAMPLE.COM\";-e;\"select 1\"", ';');
        System.out.println(strings);
    }

}