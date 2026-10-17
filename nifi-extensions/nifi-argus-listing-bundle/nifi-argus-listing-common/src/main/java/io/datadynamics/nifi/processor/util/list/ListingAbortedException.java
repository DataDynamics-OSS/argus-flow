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
package io.datadynamics.nifi.processor.util.list;

/**
 * List 를 끝까지 하지 못했지만 입력 FlowFile 의 잘못은 아닌 경우(예: 프로세서 중지).
 * failure 로 보내지 않고 세션을 롤백해 입력 FlowFile 을 큐로 되돌린다.
 */
public class ListingAbortedException extends RuntimeException {

    public ListingAbortedException(final String message) {
        super(message);
    }
}
