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
package io.datadynamics.nifi.processors.csv;

import java.util.List;

/**
 * CSV 파싱 과정에서 한 행(row)을 후처리하기 위한 콜백 인터페이스.
 * 구현체는 파싱된 행을 원하는 형태로 변환하거나 필터링할 수 있다.
 */
public interface RowProcessor {

    /**
     * 파싱된 1개 행(List<String>)을 가공하여 반환.
     * 반환된 결과가 즉시 출력 파일에 기록됩니다.
     */
    List<String> process(List<String> row);

}