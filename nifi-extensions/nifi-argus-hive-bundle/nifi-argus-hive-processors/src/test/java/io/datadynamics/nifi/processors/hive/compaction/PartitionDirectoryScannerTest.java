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

import io.datadynamics.nifi.processors.hive.compaction.CandidateEvaluator.Recommendation;
import io.datadynamics.nifi.processors.hive.compaction.CandidateEvaluator.Thresholds;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 로컬 파일시스템(@TempDir)에 Hive 3.1·4.0 이 만드는 이름 그대로의 base/delta 구조를 만들어 센다.
 */
class PartitionDirectoryScannerTest {

    private static FileSystem fs;

    @TempDir
    java.nio.file.Path dir;

    @BeforeAll
    static void fileSystem() throws IOException {
        fs = FileSystem.getLocal(new Configuration());
    }

    private void file(final String relative, final int bytes) throws IOException {
        final java.nio.file.Path path = dir.resolve(relative);
        Files.createDirectories(path.getParent());
        Files.write(path, new byte[bytes]);
    }

    private PartitionDirectoryScanner.Stats scan(final boolean acid) throws IOException {
        return PartitionDirectoryScanner.scan(fs, new Path(dir.toUri()), acid);
    }

    private static final Thresholds DEFAULTS = new Thresholds(10, 0.1, 10, 64L * 1024 * 1024);

    @Test
    void acidWithoutBaseCountsAllDeltasAndSkipsHiddenFiles() throws IOException {
        // Hive 3.1 INSERT ... UNION ALL 은 같은 write id 에 statement id 별 delta 를 만든다
        file("delta_0000001_0000001_0001/bucket_00000", 100);
        file("delta_0000001_0000001_0001/_orc_acid_version", 1);
        file("delta_0000001_0000001_0002/bucket_00000", 100);
        file("delta_0000002_0000002_0000/bucket_00000", 50);
        file("delta_0000002_0000002_0000/.bucket_00000.crc", 8);
        file("delete_delta_0000003_0000003_0000/bucket_00000", 10);
        file(".hive-staging_hive_2026/x", 999);
        final PartitionDirectoryScanner.Stats stats = scan(true);
        assertNull(stats.baseName());
        assertEquals(3, stats.deltaCount());
        assertEquals(1, stats.deleteDeltaCount());
        assertEquals(260, stats.deltaBytes());
        assertEquals(4, stats.fileCount());
        assertEquals(260, stats.totalBytes());
        assertEquals(0, stats.obsoleteDirCount());
        assertEquals(Optional.of(Recommendation.MAJOR), CandidateEvaluator.evaluate(stats, false, DEFAULTS));
    }

    @Test
    void currentBaseHidesObsoleteDirectories() throws IOException {
        // Hive 4 이름(_v<visibility txn>): MAJOR 뒤 Cleaner 가 아직 옛 delta 를 지우지 않은 상태 + 새 delta 하나
        file("base_0000003_v0000015/bucket_00000", 1000);
        file("base_0000001/bucket_00000", 400);
        file("delta_0000001_0000001_0000/bucket_00000_0", 10);
        file("delta_0000002_0000003_v0000012/bucket_00000", 10);
        file("delta_0000004_0000004_0000/bucket_00000_0", 20);
        final PartitionDirectoryScanner.Stats stats = scan(true);
        assertEquals("base_0000003_v0000015", stats.baseName());
        assertEquals(1000, stats.baseBytes());
        assertEquals(1, stats.deltaCount());
        assertEquals(20, stats.deltaBytes());
        assertEquals(3, stats.obsoleteDirCount());
        assertEquals(2, stats.fileCount());
        // 20/1000 = 0.02 <= 0.1, delta 1 <= 10
        assertEquals(Optional.empty(), CandidateEvaluator.evaluate(stats, false, DEFAULTS));
        assertEquals(Optional.of(Recommendation.MAJOR), CandidateEvaluator.evaluate(stats, false, new Thresholds(10, 0.01, 10, 1)));
        assertEquals(Optional.of(Recommendation.MINOR), CandidateEvaluator.evaluate(stats, false, new Thresholds(0, 0.5, 10, 1)));
    }

    @Test
    void thresholdsAreStrictlyGreaterThan() throws IOException {
        file("base_0000010/bucket_00000", 100);
        for (int i = 11; i <= 20; i++) {
            file(String.format("delta_%07d_%07d_0000/bucket_00000", i, i), 1);
        }
        final PartitionDirectoryScanner.Stats stats = scan(true);
        assertEquals(10, stats.deltaCount());
        // 10/100 = 0.1 (> 0.1 아님), 10 (> 10 아님)
        assertEquals(Optional.empty(), CandidateEvaluator.evaluate(stats, false, DEFAULTS));
        assertEquals(Optional.of(Recommendation.MINOR), CandidateEvaluator.evaluate(stats, false, new Thresholds(9, 0.1, 10, 1)));
        assertEquals(Optional.of(Recommendation.MAJOR), CandidateEvaluator.evaluate(stats, false, new Thresholds(10, 0.09, 10, 1)));
    }

    @Test
    void acidWithOnlyBaseIsNotCandidate() throws IOException {
        file("base_0000005/bucket_00000", 100);
        file("delta_0000004_0000005/bucket_00000", 100);
        final PartitionDirectoryScanner.Stats stats = scan(true);
        assertEquals(0, stats.totalDeltaCount());
        assertEquals(1, stats.obsoleteDirCount());
        assertFalse(CandidateEvaluator.evaluate(stats, false, DEFAULTS).isPresent());
    }

    @Test
    void nonAcidCountsFilesRecursively() throws IOException {
        for (int i = 0; i < 11; i++) {
            file("00000" + i + "_0" + (i == 0 ? "" : "_copy_" + i), 100);
        }
        file("sub/part-1", 100);
        file("_SUCCESS", 0);
        file(".000000_0.crc", 4);
        final PartitionDirectoryScanner.Stats stats = scan(false);
        assertFalse(stats.acid());
        assertEquals(12, stats.fileCount());
        assertEquals(1200, stats.totalBytes());
        assertEquals(100, stats.averageFileBytes());
        assertEquals(Optional.of(Recommendation.CONCATENATE), CandidateEvaluator.evaluate(stats, true, DEFAULTS));
        assertEquals(Optional.of(Recommendation.REWRITE), CandidateEvaluator.evaluate(stats, false, DEFAULTS));
        // 평균 100 byte >= Small File Size 100 → 후보 아님
        assertEquals(Optional.empty(), CandidateEvaluator.evaluate(stats, true, new Thresholds(10, 0.1, 10, 100)));
        assertEquals(Optional.empty(), CandidateEvaluator.evaluate(stats, true, new Thresholds(10, 0.1, 12, 1000)));
    }

    @Test
    void lastModifiedCoversNewestFile() throws IOException {
        file("delta_0000001_0000001_0000/bucket_00000", 1);
        final java.nio.file.Path bucket = dir.resolve("delta_0000001_0000001_0000/bucket_00000");
        assertTrue(bucket.toFile().setLastModified(1_000_000L));
        assertTrue(dir.resolve("delta_0000001_0000001_0000").toFile().setLastModified(1_000_000L));
        assertTrue(dir.toFile().setLastModified(2_000_000L));
        assertEquals(2_000_000L, scan(true).lastModified());
        assertTrue(bucket.toFile().setLastModified(3_000_000L));
        assertEquals(3_000_000L, scan(true).lastModified());
    }

    @Test
    void missingDirectoryFails() {
        assertThrows(FileNotFoundException.class, () -> PartitionDirectoryScanner.scan(fs, new Path(dir.resolve("nope").toUri()), true));
    }
}
