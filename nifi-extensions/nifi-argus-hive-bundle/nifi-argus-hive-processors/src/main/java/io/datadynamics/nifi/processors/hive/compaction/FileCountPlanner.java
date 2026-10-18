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
package io.datadynamics.nifi.processors.hive.compaction;

/**
 * 출력 파일 수 N 계산.
 */
public final class FileCountPlanner {

    private FileCountPlanner() {
    }

    /**
     * {@code N = max(1, ceil(partitionBytes / targetFileBytes))}.
     *
     * @throws IllegalArgumentException targetFileBytes 가 0 이하이거나 partitionBytes 가 음수
     */
    public static int fromSize(final long partitionBytes, final long targetFileBytes) {
        if (targetFileBytes <= 0) {
            throw new IllegalArgumentException("Target File Size 는 0 보다 커야 한다: " + targetFileBytes);
        }
        if (partitionBytes < 0) {
            throw new IllegalArgumentException("파티션 크기를 알 수 없다(" + partitionBytes + ")");
        }
        final long files = (partitionBytes + targetFileBytes - 1) / targetFileBytes;
        if (files > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("파일 수가 너무 크다: " + files);
        }
        return (int) Math.max(1L, files);
    }
}
