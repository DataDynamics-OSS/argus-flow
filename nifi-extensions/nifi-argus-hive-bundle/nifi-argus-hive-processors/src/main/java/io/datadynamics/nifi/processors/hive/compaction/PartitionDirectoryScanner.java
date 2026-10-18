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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 파티션(또는 비파티션 테이블) 디렉토리를 Hadoop FileSystem 으로 훑어 compaction 판단에 쓰는 수치를 모은다.
 * <p>
 * ACID: 최상위의 {@code base_<W>[_v<V>]}, {@code delta_<min>_<max>[_<stmt>|_v<V>]}, {@code delete_delta_...} 디렉토리를 본다.
 * Hive 의 {@code AcidUtils.getAcidState} 처럼 가장 큰 write id 의 base 하나를 현재 base 로 보고, 그 write id 이하에서 끝나는
 * delta 는 이미 합쳐진(Cleaner 가 아직 지우지 않은) 옛 디렉토리로 빼고 센다. 열린·중단된 트랜잭션은 보지 않는다(근사).
 * <p>
 * Non-ACID: 디렉토리 아래(재귀) 데이터 파일 수와 크기를 센다.
 * <p>
 * 이름이 {@code .} 또는 {@code _} 로 시작하는 파일·디렉토리({@code _orc_acid_version}, {@code .hive-staging_*}, {@code .crc} 등)는 뺀다.
 */
public final class PartitionDirectoryScanner {

    private static final Pattern BASE = Pattern.compile("base_(\\d+)(?:_v\\d+)?");
    private static final Pattern DELTA = Pattern.compile("(delete_)?delta_(\\d+)_(\\d+)(?:_\\d+)?(?:_v\\d+)?");

    private PartitionDirectoryScanner() {
    }

    /**
     * 집계 결과.
     *
     * @param location         디렉토리
     * @param acid             ACID 로 해석했는지
     * @param baseName         현재 base 디렉토리 이름(없으면 null)
     * @param baseBytes        현재 base 크기
     * @param deltaCount       현재 delta 디렉토리 수
     * @param deleteDeltaCount 현재 delete_delta 디렉토리 수
     * @param deltaBytes       현재 delta + delete_delta 크기
     * @param obsoleteDirCount 이미 합쳐졌지만 남아 있는 base/delta 디렉토리 수
     * @param fileCount        현재 데이터 파일 수(ACID 는 현재 base·delta 안, Non-ACID 는 전체)
     * @param totalBytes       현재 데이터 크기
     * @param lastModified     디렉토리와 그 아래 항목의 가장 최근 수정 시각(epoch ms)
     */
    public record Stats(String location, boolean acid, String baseName, long baseBytes, int deltaCount, int deleteDeltaCount,
                        long deltaBytes, int obsoleteDirCount, long fileCount, long totalBytes, long lastModified) {

        public int totalDeltaCount() {
            return deltaCount + deleteDeltaCount;
        }

        public long averageFileBytes() {
            return fileCount == 0 ? 0 : totalBytes / fileCount;
        }
    }

    private static final class Totals {
        long files;
        long bytes;
        long lastModified;
    }

    /**
     * @throws FileNotFoundException 디렉토리가 없음
     */
    public static Stats scan(final FileSystem fileSystem, final Path location, final boolean acid) throws IOException {
        final FileStatus root = fileSystem.getFileStatus(location);
        if (!root.isDirectory()) {
            throw new FileNotFoundException(location + " 은(는) 디렉토리가 아니다");
        }
        long lastModified = root.getModificationTime();
        if (!acid) {
            final Totals totals = sum(fileSystem, location);
            return new Stats(location.toString(), false, null, 0, 0, 0, 0, 0, totals.files, totals.bytes,
                    Math.max(lastModified, totals.lastModified));
        }

        final List<FileStatus> bases = new ArrayList<>();
        final List<FileStatus> deltas = new ArrayList<>();
        long baseWriteId = -1;
        FileStatus currentBase = null;
        for (final FileStatus child : fileSystem.listStatus(location)) {
            final String name = child.getPath().getName();
            if (isHidden(name) || !child.isDirectory()) {
                continue;
            }
            lastModified = Math.max(lastModified, child.getModificationTime());
            final Matcher base = BASE.matcher(name);
            if (base.matches()) {
                bases.add(child);
                final long writeId = Long.parseLong(base.group(1));
                if (writeId > baseWriteId) {
                    baseWriteId = writeId;
                    currentBase = child;
                }
            } else if (DELTA.matcher(name).matches()) {
                deltas.add(child);
            }
        }

        int obsolete = bases.size() - (currentBase == null ? 0 : 1);
        long baseBytes = 0;
        long fileCount = 0;
        if (currentBase != null) {
            final Totals totals = sum(fileSystem, currentBase.getPath());
            baseBytes = totals.bytes;
            fileCount += totals.files;
            lastModified = Math.max(lastModified, totals.lastModified);
        }
        int deltaCount = 0;
        int deleteDeltaCount = 0;
        long deltaBytes = 0;
        for (final FileStatus delta : deltas) {
            final Matcher matcher = DELTA.matcher(delta.getPath().getName());
            matcher.matches();
            final long maxWriteId = Long.parseLong(matcher.group(3));
            if (maxWriteId <= baseWriteId) {
                obsolete++;
                continue;
            }
            final Totals totals = sum(fileSystem, delta.getPath());
            deltaBytes += totals.bytes;
            fileCount += totals.files;
            lastModified = Math.max(lastModified, totals.lastModified);
            if (matcher.group(1) != null) {
                deleteDeltaCount++;
            } else {
                deltaCount++;
            }
        }
        return new Stats(location.toString(), true, currentBase == null ? null : currentBase.getPath().getName(), baseBytes,
                deltaCount, deleteDeltaCount, deltaBytes, obsolete, fileCount, baseBytes + deltaBytes, lastModified);
    }

    static boolean isHidden(final String name) {
        return name.startsWith(".") || name.startsWith("_");
    }

    private static Totals sum(final FileSystem fileSystem, final Path directory) throws IOException {
        final Totals totals = new Totals();
        final RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(directory, true);
        while (files.hasNext()) {
            final LocatedFileStatus file = files.next();
            if (hasHiddenComponent(directory, file.getPath())) {
                continue;
            }
            totals.files++;
            totals.bytes += file.getLen();
            totals.lastModified = Math.max(totals.lastModified, file.getModificationTime());
        }
        return totals;
    }

    /** directory 아래 상대 경로에 숨김 이름이 있는지. */
    static boolean hasHiddenComponent(final Path directory, final Path file) {
        final String base = Path.getPathWithoutSchemeAndAuthority(directory).toString();
        final String full = Path.getPathWithoutSchemeAndAuthority(file).toString();
        final String relative = full.startsWith(base) ? full.substring(base.length()) : file.getName();
        for (final String component : relative.split("/")) {
            if (!component.isEmpty() && isHidden(component)) {
                return true;
            }
        }
        return false;
    }
}
