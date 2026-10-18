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
package io.datadynamics.nifi.processors.parquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * MergeParquet 프로세서 단위 테스트.
 *
 * 로컬 파일 시스템(file://, Hadoop Configuration 미지정 시 기본값)에서 소스 디렉터리의 여러 Parquet
 * 파일을 하나로 병합하는 커스텀 로직을 검증한다. parquet-avro로 입력 파일을 만들고, 병합 결과 파일을
 * 다시 읽어 레코드 수·속성을 확인한다.
 */
class MergeParquetTest {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"},"
                    + "{\"name\":\"name\",\"type\":\"string\"}]}");

    private void writeParquet(File file, int startId, int count) throws Exception {
        try (ParquetWriter<GenericRecord> writer =
                     AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.toURI()))
                             .withSchema(SCHEMA)
                             .withConf(new Configuration())
                             .build()) {
            for (int i = 0; i < count; i++) {
                final GenericRecord r = new GenericData.Record(SCHEMA);
                r.put("id", (long) (startId + i));
                r.put("name", "name-" + (startId + i));
                writer.write(r);
            }
        }
    }

    /** 로컬 파일 시스템(file://)을 기본 FS로 지정하는 최소 core-site.xml을 만들어 경로를 반환한다. */
    private File writeLocalHadoopConfig(Path dir) throws Exception {
        final File conf = dir.resolve("core-site.xml").toFile();
        java.nio.file.Files.writeString(conf.toPath(),
                "<?xml version=\"1.0\"?>\n<configuration>\n"
                        + "  <property><name>fs.defaultFS</name><value>file:///</value></property>\n"
                        + "</configuration>\n");
        return conf;
    }

    private long countRecords(File parquet) throws Exception {
        long n = 0;
        try (ParquetReader<GenericRecord> reader =
                     AvroParquetReader.<GenericRecord>builder(new org.apache.hadoop.fs.Path(parquet.toURI()))
                             .withConf(new Configuration())
                             .build()) {
            while (reader.read() != null) {
                n++;
            }
        }
        return n;
    }

    @Test
    void mergesAllParquetFilesInSourceDir(@TempDir Path tmp) throws Exception {
        final File sourceDir = tmp.resolve("source").toFile();
        final File targetDir = tmp.resolve("target").toFile();
        assertTrue(sourceDir.mkdirs());
        assertTrue(targetDir.mkdirs());

        writeParquet(new File(sourceDir, "part-1.parquet"), 0, 2);
        writeParquet(new File(sourceDir, "part-2.parquet"), 100, 3);

        final TestRunner runner = TestRunners.newTestRunner(new MergeParquet());
        runner.setProperty(MergeParquet.PROP_HADOOP_CONFIGURATION_RESOURCES, writeLocalHadoopConfig(tmp).getAbsolutePath());
        // 소스는 glob 패턴이다(ParquetUtils가 fs.globStatus로 매칭). 디렉터리 경로만 주면 매칭되지 않는다.
        runner.setProperty(MergeParquet.PROP_HDFS_SOURCE_PATH, sourceDir.getAbsolutePath() + "/*.parquet");
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_DIR, targetDir.getAbsolutePath());
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_FILENAME, "merged.parquet");
        runner.setProperty(MergeParquet.PROP_KEEP_SOURCE_FILE, "true");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(MergeParquet.REL_SUCCESS).get(0);
        out.assertAttributeEquals("merge.hdfs.source.files.count", "2");
        out.assertAttributeEquals("merge.target.filename", "merged.parquet");

        final File merged = new File(targetDir, "merged.parquet");
        assertTrue(merged.exists(), "병합 결과 파일이 생성되어야 한다");
        // 2 + 3 = 5개 레코드가 하나의 파일로 병합되어야 한다
        assertEquals(5L, countRecords(merged));
    }

    @Test
    void keepSourceFileTruePreservesInputs(@TempDir Path tmp) throws Exception {
        final File sourceDir = tmp.resolve("src").toFile();
        final File targetDir = tmp.resolve("tgt").toFile();
        assertTrue(sourceDir.mkdirs());
        assertTrue(targetDir.mkdirs());
        final File input = new File(sourceDir, "only.parquet");
        writeParquet(input, 0, 4);

        final TestRunner runner = TestRunners.newTestRunner(new MergeParquet());
        runner.setProperty(MergeParquet.PROP_HADOOP_CONFIGURATION_RESOURCES, writeLocalHadoopConfig(tmp).getAbsolutePath());
        // 소스는 glob 패턴이다(ParquetUtils가 fs.globStatus로 매칭). 디렉터리 경로만 주면 매칭되지 않는다.
        runner.setProperty(MergeParquet.PROP_HDFS_SOURCE_PATH, sourceDir.getAbsolutePath() + "/*.parquet");
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_DIR, targetDir.getAbsolutePath());
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_FILENAME, "merged.parquet");
        runner.setProperty(MergeParquet.PROP_KEEP_SOURCE_FILE, "true");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        assertTrue(input.exists(), "원본 유지 옵션이 true면 소스 파일이 남아 있어야 한다");
        assertEquals(4L, countRecords(new File(targetDir, "merged.parquet")));
    }

    /* -------------------- 공통 헬퍼 -------------------- */

    private static void writeRecords(File file, Schema schema, Configuration conf, List<GenericRecord> records) throws Exception {
        try (ParquetWriter<GenericRecord> writer =
                     AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.toURI()))
                             .withSchema(schema)
                             .withConf(conf)
                             .build()) {
            for (GenericRecord r : records) {
                writer.write(r);
            }
        }
    }

    private static List<GenericRecord> readAll(File parquet) throws Exception {
        final List<GenericRecord> out = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader =
                     AvroParquetReader.<GenericRecord>builder(new org.apache.hadoop.fs.Path(parquet.toURI()))
                             .withConf(new Configuration())
                             .build()) {
            GenericRecord r;
            while ((r = reader.read()) != null) {
                out.add(r);
            }
        }
        return out;
    }

    private TestRunner newRunner(Path tmp, String sourceGlob, File targetDir, String filename, boolean keep) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new MergeParquet());
        runner.setProperty(MergeParquet.PROP_HADOOP_CONFIGURATION_RESOURCES, writeLocalHadoopConfig(tmp).getAbsolutePath());
        runner.setProperty(MergeParquet.PROP_HDFS_SOURCE_PATH, sourceGlob);
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_DIR, targetDir.getAbsolutePath());
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_FILENAME, filename);
        runner.setProperty(MergeParquet.PROP_KEEP_SOURCE_FILE, String.valueOf(keep));
        return runner;
    }

    private static File mkdir(Path tmp, String name) {
        final File d = tmp.resolve(name).toFile();
        assertTrue(d.mkdirs());
        return d;
    }

    /* -------------------- 정상 동작 / 설정 / EL / 실패 라우팅 -------------------- */

    /** 원본 유지=false이면 커밋 후 원본이 삭제되고, 레코드 값과 순서(glob 정렬 순)가 보존되어야 한다. */
    @Test
    void keepSourceFalse_deletesSourcesAfterCommit_andPreservesValues(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        final File a = new File(src, "a.parquet");
        final File b = new File(src, "b.parquet");
        writeParquet(a, 0, 2);
        writeParquet(b, 10, 3);

        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", false);
        runner.enqueue(new byte[0], Map.of("keep.me", "1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(MergeParquet.REL_SUCCESS).get(0);
        out.assertAttributeEquals("keep.me", "1");
        out.assertAttributeEquals("merge.hdfs.source.dir", src.getAbsolutePath() + "/*.parquet");
        out.assertAttributeEquals("merge.hdfs.source.files.count", "2");
        out.assertAttributeEquals("merge.hdfs.target.dir", tgt.getAbsolutePath());
        out.assertAttributeEquals("merge.target.qualifiedPath", "file:" + tgt.getAbsolutePath() + "/merged.parquet");

        assertFalse(a.exists(), "원본 유지=false면 커밋 후 원본이 삭제되어야 한다");
        assertFalse(b.exists());
        final List<GenericRecord> merged = readAll(new File(tgt, "merged.parquet"));
        assertEquals(5, merged.size());
        final long[] ids = merged.stream().mapToLong(r -> (Long) r.get("id")).toArray();
        org.junit.jupiter.api.Assertions.assertArrayEquals(new long[]{0, 1, 10, 11, 12}, ids);
        assertEquals("name-11", merged.get(3).get("name").toString());
    }

    /** 소스/대상 디렉터리/파일명을 FlowFile 속성 기반 EL로 지정할 수 있어야 한다. */
    @Test
    void expressionLanguage_pathsFromFlowFileAttributes(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "p.parquet"), 0, 3);

        final TestRunner runner = TestRunners.newTestRunner(new MergeParquet());
        runner.setProperty(MergeParquet.PROP_HADOOP_CONFIGURATION_RESOURCES, writeLocalHadoopConfig(tmp).getAbsolutePath());
        runner.setProperty(MergeParquet.PROP_HDFS_SOURCE_PATH, "${src.dir}/*.parquet");
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_DIR, "${tgt.dir}");
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_FILENAME, "${tgt.name}.parquet");
        runner.assertValid();
        runner.enqueue(new byte[0], Map.of("src.dir", src.getAbsolutePath(), "tgt.dir", tgt.getAbsolutePath(), "tgt.name", "day1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(MergeParquet.REL_SUCCESS).get(0).assertAttributeEquals("merge.target.filename", "day1.parquet");
        assertEquals(3L, countRecords(new File(tgt, "day1.parquet")));
    }

    /** 필수 속성 누락/허용되지 않은 값은 invalid여야 한다. */
    @Test
    void configValidation(@TempDir Path tmp) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new MergeParquet());
        runner.assertNotValid();
        runner.setProperty(MergeParquet.PROP_HDFS_SOURCE_PATH, "/a/*.parquet");
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_DIR, "/b");
        runner.assertNotValid();
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_FILENAME, "m.parquet");
        runner.assertValid(); // Hadoop 설정 파일은 선택, 원본 유지는 기본값 true
        runner.setProperty(MergeParquet.PROP_KEEP_SOURCE_FILE, "maybe");
        runner.assertNotValid();
        runner.setProperty(MergeParquet.PROP_KEEP_SOURCE_FILE, "false");
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_FILENAME, "");
        runner.assertNotValid();
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_FILENAME, "m.parquet");
        runner.setProperty(MergeParquet.PROP_HADOOP_CONFIGURATION_RESOURCES, tmp.resolve("missing-core-site.xml").toString());
        runner.assertNotValid(); // 존재하지 않는 외부 리소스
    }

    /** 손상된(Parquet가 아닌) 소스 파일이 섞이면 failure로 라우팅되고, 원본 유지=false여도 원본은 삭제되지 않아야 한다. */
    @Test
    void corruptSourceFile_routesToFailure_andKeepsSources(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        final File a = new File(src, "a.parquet");
        final File b = new File(src, "b.parquet");
        writeParquet(a, 0, 2);
        Files.writeString(b.toPath(), "this is not a parquet file");

        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", false);
        runner.enqueue("payload", Map.of("k", "v"));
        runner.run();

        runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 1);
        final MockFlowFile ff = runner.getFlowFilesForRelationship(MergeParquet.REL_FAILURE).get(0);
        ff.assertContentEquals("payload");
        ff.assertAttributeEquals("k", "v");
        ff.assertAttributeNotExists("merge.target.filename");
        assertTrue(a.exists(), "실패 시 원본은 삭제되면 안 된다");
        assertTrue(b.exists());
    }

    /** 존재하지 않는(glob이 아닌) 소스 경로는 failure로 라우팅되어야 한다. */
    @Test
    void nonexistentSourcePath_routesToFailure(@TempDir Path tmp) throws Exception {
        final File tgt = mkdir(tmp, "tgt");
        final TestRunner runner = newRunner(tmp, tmp.resolve("nope").toString(), tgt, "merged.parquet", false);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 1);
    }

    /** 대상 파일이 이미 있으면 성공 시 새 병합 결과로 덮어쓴다(현재 설계). */
    @Test
    void existingTarget_isOverwrittenOnSuccess(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "a.parquet"), 0, 2);
        final File merged = new File(tgt, "merged.parquet");
        writeParquet(merged, 1000, 7);

        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", true);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        assertEquals(2L, countRecords(merged));
    }

    /** 여러 FlowFile을 순차 처리해도 각자의 EL 경로로 독립적으로 병합되어야 한다. */
    @Test
    void multipleFlowFiles_independentMerges(@TempDir Path tmp) throws Exception {
        final File s1 = mkdir(tmp, "s1");
        final File s2 = mkdir(tmp, "s2");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(s1, "x.parquet"), 0, 1);
        writeParquet(new File(s2, "y.parquet"), 0, 4);

        final TestRunner runner = newRunner(tmp, "${dir}/*.parquet", tgt, "${out}", true);
        runner.enqueue(new byte[0], Map.of("dir", s1.getAbsolutePath(), "out", "m1.parquet"));
        runner.enqueue(new byte[0], Map.of("dir", s2.getAbsolutePath(), "out", "m2.parquet"));
        runner.run(2);
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 2);
        assertEquals(1L, countRecords(new File(tgt, "m1.parquet")));
        assertEquals(4L, countRecords(new File(tgt, "m2.parquet")));
    }

    /* -------------------- 확인된 결함(BUG) 재현 테스트 -------------------- */

    /** 병합 시작 전에 기존 대상 파일을 먼저 삭제하므로, 병합이 실패하면 이전 병합 결과가 유실된다. */
    @Test
    void bug_failedMerge_destroysExistingTarget(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        Files.writeString(new File(src, "bad.parquet").toPath(), "garbage");
        final File merged = new File(tgt, "merged.parquet");
        writeParquet(merged, 0, 7);

        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", true);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 1);
        assertTrue(merged.exists(), "실패한 병합이 기존 대상 파일을 지우면 안 된다");
        assertEquals(7L, countRecords(merged));
    }

    /** 병합 도중 실패하면 Writer가 닫히지 않은 불완전한(footer 없는) 파일이 최종 경로에 남는다(임시 파일 + rename 미사용). */
    @Test
    void bug_failedMerge_leavesPartialTargetFile(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "a.parquet"), 0, 3);
        Files.writeString(new File(src, "b.parquet").toPath(), "garbage");

        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", true);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 1);
        assertFalse(new File(tgt, "merged.parquet").exists(), "실패 시 최종 경로에 불완전한 파일이 남으면 안 된다");
    }

    /**
     * 대상 파일이 소스 glob에 포함되면(같은 디렉터리 + *.parquet) 재실행 시 이전 병합 결과가 원본으로 취급되어 유실/중복되므로,
     * 원본 유지 옵션과 무관하게 첫 실행부터 failure로 라우팅하고 파일 시스템을 변경하지 않아야 한다(P-1).
     */
    @Test
    void targetInsideSourceGlob_routesToFailure(@TempDir Path tmp) throws Exception {
        for (boolean keep : new boolean[]{true, false}) {
            final File dir = mkdir(tmp, "data-" + keep);
            final File a = new File(dir, "a.parquet");
            final File b = new File(dir, "b.parquet");
            writeParquet(a, 0, 2);
            writeParquet(b, 10, 3);
            final List<String> before = List.of(dir.list());

            final TestRunner runner = newRunner(tmp, dir.getAbsolutePath() + "/*.parquet", dir, "merged.parquet", keep);
            runner.enqueue(new byte[0]);
            runner.run();
            runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 1);
            runner.getFlowFilesForRelationship(MergeParquet.REL_FAILURE).get(0).assertAttributeNotExists("merge.target.filename");
            assertTrue(a.exists() && b.exists(), "keep=" + keep + ": 원본은 삭제되면 안 된다");
            assertFalse(new File(dir, "merged.parquet").exists(), "keep=" + keep + ": 대상 파일을 만들면 안 된다");
            assertEquals(new TreeSet<>(before), new TreeSet<>(List.of(dir.list())), "keep=" + keep + ": 디렉터리 내용이 바뀌면 안 된다");
        }
    }

    /** 이미 존재하는 대상 파일이 소스 glob에 포함되어도 failure로 라우팅되고 대상 파일과 원본은 그대로 남아야 한다. */
    @Test
    void targetInsideSourceGlob_existingTargetAndSourcesUntouched(@TempDir Path tmp) throws Exception {
        for (boolean keep : new boolean[]{true, false}) {
            final File dir = mkdir(tmp, "data-" + keep);
            final File merged = new File(dir, "merged.parquet");
            writeParquet(merged, 0, 4);
            final byte[] mergedBytes = Files.readAllBytes(merged.toPath());
            final File a = new File(dir, "a.parquet");
            writeParquet(a, 100, 1);
            final List<String> before = List.of(dir.list());

            final TestRunner runner = newRunner(tmp, dir.getAbsolutePath() + "/*.parquet", dir, "merged.parquet", keep);
            runner.enqueue(new byte[0]);
            runner.run();
            runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 1);
            assertTrue(a.exists(), "keep=" + keep + ": 원본은 삭제되면 안 된다");
            assertArrayEquals(mergedBytes, Files.readAllBytes(merged.toPath()), "keep=" + keep + ": 기존 대상 파일이 바뀌면 안 된다");
            assertEquals(new TreeSet<>(before), new TreeSet<>(List.of(dir.list())), "keep=" + keep + ": 디렉터리 내용이 바뀌면 안 된다");
        }
    }

    /** 중괄호/와일드카드 디렉터리 glob도 대상 포함 여부를 판단하며, 하위 디렉터리처럼 glob에 걸리지 않는 대상은 정상 병합되어야 한다. */
    @Test
    void targetInsideSourceGlob_patternMatchedByPathComponents(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "a.parquet"), 0, 2);

        // {src,tgt}/*.parquet 는 tgt/merged.parquet 를 포함한다(대상 파일이 아직 없어도)
        TestRunner runner = newRunner(tmp, tmp.toAbsolutePath() + "/{src,tgt}/*.parquet", tgt, "merged.parquet", false);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 1);
        assertTrue(new File(src, "a.parquet").exists());

        // */*.parquet 도 tgt/merged.parquet 를 포함한다
        runner = newRunner(tmp, tmp.toAbsolutePath() + "/*/*.parquet", tgt, "merged.parquet", false);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 1);
        assertTrue(new File(src, "a.parquet").exists());
        assertEquals(0, tgt.list().length);

        // src/*.parquet 는 src/sub/merged.parquet 를 포함하지 않는다(* 는 / 를 넘지 않음)
        final File sub = mkdir(tmp, "src/sub");
        runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", sub, "merged.parquet", true);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        assertEquals(2L, countRecords(new File(sub, "merged.parquet")));
    }

    /** 성공 시 대상 디렉터리에 임시 파일(.<name>.tmp-*)이나 백업 파일이 남지 않아야 한다(기존 대상 교체 포함). */
    @Test
    void noTempFilesRemain_afterSuccess(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "a.parquet"), 0, 2);
        writeParquet(new File(tgt, "merged.parquet"), 1000, 7);
        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", true);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        assertEquals(2L, countRecords(new File(tgt, "merged.parquet")));
        assertTrue(noTempFiles(tgt), "남은 파일: " + List.of(tgt.list()));
    }

    /** 병합 도중 실패해도 대상 디렉터리에 임시 파일이 남지 않아야 한다. */
    @Test
    void noTempFilesRemain_afterFailure(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "a.parquet"), 0, 3);
        Files.writeString(new File(src, "b.parquet").toPath(), "garbage");
        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", true);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 1);
        assertEquals(0, tgt.list().length, "남은 파일: " + List.of(tgt.list()));
    }

    /** 대상 경로가 디렉터리이면 failure로 라우팅하고 디렉터리를 삭제하지 않아야 한다. */
    @Test
    void targetPathIsDirectory_routesToFailure(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "a.parquet"), 0, 1);
        final File dirTarget = new File(tgt, "merged.parquet");
        assertTrue(dirTarget.mkdirs());
        final File inner = new File(dirTarget, "keep.txt");
        Files.writeString(inner.toPath(), "x");
        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", false);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 1);
        assertTrue(inner.exists());
        assertTrue(new File(src, "a.parquet").exists());
    }

    /** 파일명이 ".", ".."이거나 "/"를 포함하면 failure로 라우팅되어야 한다. */
    @Test
    void invalidTargetFilename_routesToFailure(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "a.parquet"), 0, 1);
        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "${name}", true);
        runner.enqueue(new byte[0], Map.of("name", "."));
        runner.enqueue(new byte[0], Map.of("name", ".."));
        runner.enqueue(new byte[0], Map.of("name", "../escaped.parquet"));
        runner.run(3);
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 3);
        assertFalse(new File(tmp.toFile(), "escaped.parquet").exists());
        assertEquals(0, tgt.list().length);
    }

    private static boolean noTempFiles(File dir) {
        final String[] names = dir.list();
        for (String n : names) {
            if (n.contains(ParquetUtils.TEMP_FILE_INFIX) || n.contains(ParquetUtils.BACKUP_FILE_INFIX)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 첫 레코드의 스키마로 Writer를 만들고 위치(index) 기반으로 기록하므로, 필드 순서만 다른 파일을 병합하면 값이 뒤바뀐다(P-3).
     * 기록 전에 Footer 스키마를 비교해 failure로 라우팅하고, 아무것도 기록/삭제하지 않아야 한다.
     */
    @Test
    void bug_schemaMismatch_reorderedFields_silentlySwapped(@TempDir Path tmp) throws Exception {
        final Schema ab = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"string\"}]}");
        final Schema ba = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"b\",\"type\":\"string\"},{\"name\":\"a\",\"type\":\"string\"}]}");
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        final GenericRecord r1 = new GenericData.Record(ab);
        r1.put("a", "A1");
        r1.put("b", "B1");
        final GenericRecord r2 = new GenericData.Record(ba);
        r2.put("a", "A2");
        r2.put("b", "B2");
        writeRecords(new File(src, "1.parquet"), ab, new Configuration(), List.of(r1));
        writeRecords(new File(src, "2.parquet"), ba, new Configuration(), List.of(r2));

        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", false);
        runner.enqueue(new byte[0]);
        runner.run();
        assertSchemaMismatchFailure(runner, tgt, new File(src, "1.parquet"), new File(src, "2.parquet"));
    }

    /** 이후 파일에 컬럼이 추가되어 있으면 추가 컬럼이 조용히 버려지므로(P-3), failure로 라우팅하고 아무것도 기록/삭제하지 않아야 한다. */
    @Test
    void bug_schemaMismatch_extraColumnSilentlyDropped(@TempDir Path tmp) throws Exception {
        final Schema wide = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},"
                        + "{\"name\":\"extra\",\"type\":\"string\"}]}");
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "1.parquet"), 0, 1);
        final GenericRecord r = new GenericData.Record(wide);
        r.put("id", 5L);
        r.put("name", "n");
        r.put("extra", "important");
        writeRecords(new File(src, "2.parquet"), wide, new Configuration(), List.of(r));

        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", false);
        runner.enqueue(new byte[0]);
        runner.run();
        assertSchemaMismatchFailure(runner, tgt, new File(src, "1.parquet"), new File(src, "2.parquet"));
    }

    /** 같은 이름의 컬럼이라도 타입이나 nullable 여부가 다르면 failure로 라우팅되고, 기존 대상 파일은 그대로 남아야 한다. */
    @Test
    void schemaMismatch_typeOrRepetition_routesToFailure(@TempDir Path tmp) throws Exception {
        final Schema intId = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}");
        final Schema nullableName = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":[\"null\",\"string\"]}]}");
        for (Schema other : List.of(intId, nullableName)) {
            final File src = mkdir(tmp, "src-" + other.getField("id").schema().getType() + "-" + other.getField("name").schema().getType());
            final File tgt = mkdir(tmp, "tgt-" + src.getName());
            writeParquet(new File(src, "1.parquet"), 0, 1);
            final GenericRecord r = new GenericData.Record(other);
            r.put("id", other == intId ? (Object) 5 : (Object) 5L);
            r.put("name", "n");
            writeRecords(new File(src, "2.parquet"), other, new Configuration(), List.of(r));
            final File merged = new File(tgt, "merged.parquet");
            writeParquet(merged, 1000, 3);

            final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", false);
            runner.enqueue(new byte[0]);
            runner.run();
            runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 1);
            assertTrue(new File(src, "1.parquet").exists() && new File(src, "2.parquet").exists());
            assertEquals(3L, countRecords(merged), "기존 대상 파일은 그대로 남아야 한다");
            assertTrue(noTempFiles(tgt));
        }
    }

    /** 레코드 이름/namespace/doc 같은 메타데이터만 다르고 구조가 같으면 정상 병합되어야 한다. */
    @Test
    void schemaMetadataOnlyDifference_mergesSuccessfully(@TempDir Path tmp) throws Exception {
        final Schema renamed = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"Other\",\"namespace\":\"x.y\",\"doc\":\"d\",\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"long\",\"doc\":\"id doc\"},{\"name\":\"name\",\"type\":\"string\"}]}");
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "1.parquet"), 0, 2);
        final GenericRecord r = new GenericData.Record(renamed);
        r.put("id", 7L);
        r.put("name", "name-7");
        writeRecords(new File(src, "2.parquet"), renamed, new Configuration(), List.of(r));

        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", true);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        final List<GenericRecord> merged = readAll(new File(tgt, "merged.parquet"));
        assertEquals(3, merged.size());
        assertEquals(7L, merged.get(2).get("id"));
        assertEquals("name-7", merged.get(2).get("name").toString());
    }

    private static void assertSchemaMismatchFailure(TestRunner runner, File tgt, File... sources) {
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(MergeParquet.REL_FAILURE).get(0).assertAttributeNotExists("merge.target.filename");
        for (File s : sources) {
            assertTrue(s.exists(), "스키마 불일치 시 원본이 삭제되면 안 된다: " + s);
        }
        assertEquals(0, tgt.list().length, "스키마 불일치 시 대상 디렉터리에 아무것도 기록되면 안 된다: " + List.of(tgt.list()));
    }

    /** 파일명이 "."(EL로 유입 가능)이면 대상 경로가 대상 디렉터리 자체가 되어 fs.delete(target, recursive=true)로 디렉터리 전체가 삭제된다. */
    @Test
    void bug_dotFilename_recursivelyDeletesTargetDir(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "a.parquet"), 0, 1);
        final File precious = new File(tgt, "precious.txt");
        Files.writeString(precious.toPath(), "do not delete");

        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "${name}", true);
        runner.enqueue(new byte[0], Map.of("name", "."));
        runner.run();
        assertTrue(precious.exists(), "대상 디렉터리의 다른 파일이 재귀 삭제되면 안 된다");
    }

    /** REQUIRED INT96 컬럼도 병합 후 INT96 타입이 유지되어야 한다(P-7: 기존에는 UNION 필드만 감지해 FIXED_LEN_BYTE_ARRAY(12)로 바뀜). */
    @Test
    void bug_requiredInt96Column_typeLostAfterMerge(@TempDir Path tmp) throws Exception {
        final Schema schema = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"ts\",\"type\":"
                        + "{\"type\":\"fixed\",\"name\":\"INT96\",\"size\":12}}]}");
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        final Configuration conf = new Configuration();
        conf.setStrings(AvroWriteSupport.WRITE_FIXED_AS_INT96, "ts");
        final GenericRecord r = new GenericData.Record(schema);
        r.put("ts", new GenericData.Fixed(schema.getField("ts").schema(), new byte[12]));
        final File in = new File(src, "a.parquet");
        writeRecords(in, schema, conf, List.of(r));
        assertEquals(PrimitiveType.PrimitiveTypeName.INT96, parquetType(in, "ts"), "전제: 입력은 INT96");

        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", true);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        assertEquals(PrimitiveType.PrimitiveTypeName.INT96, parquetType(new File(tgt, "merged.parquet"), "ts"));
    }

    /** 중첩 Record 안의 INT96 컬럼(REQUIRED/OPTIONAL)도 병합 후 INT96 타입과 값이 유지되어야 한다(P-7). */
    @Test
    void nestedAndOptionalInt96Columns_typePreservedAfterMerge(@TempDir Path tmp) throws Exception {
        final String int96 = "{\"type\":\"fixed\",\"name\":\"INT96\",\"size\":12}";
        final Schema schema = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
                        + "{\"name\":\"ts\",\"type\":" + int96 + "},"
                        + "{\"name\":\"opt_ts\",\"type\":[\"null\",\"INT96\"],\"default\":null},"
                        + "{\"name\":\"event\",\"type\":{\"type\":\"record\",\"name\":\"E\",\"fields\":["
                        + "{\"name\":\"ts\",\"type\":\"INT96\"},{\"name\":\"code\",\"type\":\"string\"}]}},"
                        + "{\"name\":\"opt_event\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"O\",\"fields\":["
                        + "{\"name\":\"ts\",\"type\":[\"null\",\"INT96\"],\"default\":null}]}],\"default\":null}]}");
        final Schema fixed = schema.getField("ts").schema();
        final Configuration conf = new Configuration();
        conf.setStrings(AvroWriteSupport.WRITE_FIXED_AS_INT96, "ts", "opt_ts", "event.ts", "opt_event.ts");
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        for (int f = 0; f < 2; f++) {
            final byte[] bytes = new byte[12];
            bytes[0] = (byte) (f + 1);
            final GenericRecord e = new GenericData.Record(schema.getField("event").schema());
            e.put("ts", new GenericData.Fixed(fixed, bytes));
            e.put("code", "c" + f);
            final GenericRecord o = new GenericData.Record(schema.getField("opt_event").schema().getTypes().get(1));
            o.put("ts", new GenericData.Fixed(fixed, bytes));
            final GenericRecord r = new GenericData.Record(schema);
            r.put("ts", new GenericData.Fixed(fixed, bytes));
            r.put("opt_ts", new GenericData.Fixed(fixed, bytes));
            r.put("event", e);
            r.put("opt_event", o);
            writeRecords(new File(src, f + ".parquet"), schema, conf, List.of(r));
        }
        assertEquals(PrimitiveType.PrimitiveTypeName.INT96, parquetType(new File(src, "0.parquet"), "event", "ts"), "전제: 입력은 INT96");

        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", true);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        final File merged = new File(tgt, "merged.parquet");
        for (String[] column : List.of(new String[]{"ts"}, new String[]{"opt_ts"}, new String[]{"event", "ts"}, new String[]{"opt_event", "ts"})) {
            assertEquals(PrimitiveType.PrimitiveTypeName.INT96, parquetType(merged, column), String.join(".", column) + " 컬럼은 INT96이어야 한다");
        }
        final List<GenericRecord> records = readAll(merged);
        assertEquals(2, records.size());
        assertEquals(2, ((GenericData.Fixed) ((GenericRecord) records.get(1).get("event")).get("ts")).bytes()[0], "INT96 값이 보존되어야 한다");
        assertEquals("c1", ((GenericRecord) records.get(1).get("event")).get("code").toString());
    }

    private static PrimitiveType.PrimitiveTypeName parquetType(File file, String... column) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(
                HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(file.toURI()), new Configuration()))) {
            final MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            return schema.getType(column).asPrimitiveType().getPrimitiveTypeName();
        }
    }

    /** 원본 경로에 Glob 문자가 없는 디렉토리를 지정하면 <디렉토리>/*.parquet로 해석해 Parquet 파일만 병합해야 한다(P-10). */
    @Test
    void plainDirectorySourcePath_mergesParquetFilesInDir(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "a.parquet"), 0, 2);
        writeParquet(new File(src, "b.parquet"), 10, 1);
        Files.writeString(new File(src, "notes.txt").toPath(), "not parquet");
        final TestRunner runner = newRunner(tmp, src.getAbsolutePath(), tgt, "merged.parquet", false);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(MergeParquet.REL_SUCCESS).get(0);
        out.assertAttributeEquals("merge.hdfs.source.dir", src.getAbsolutePath());
        out.assertAttributeEquals("merge.hdfs.source.files.count", "2");
        assertEquals(3L, countRecords(new File(tgt, "merged.parquet")));
        assertTrue(new File(src, "notes.txt").exists(), "*.parquet가 아닌 파일은 원본으로 취급하면 안 된다");
        assertFalse(new File(src, "a.parquet").exists());
    }

    /** 디렉토리로 해석한 원본 패턴(<디렉토리>/*.parquet)에 대상 파일이 포함되어도 P-1 검사로 failure가 되고 아무것도 바뀌지 않아야 한다. */
    @Test
    void plainDirectorySourcePath_targetInsideResolvedPattern_routesToFailure(@TempDir Path tmp) throws Exception {
        for (boolean keep : new boolean[]{true, false}) {
            final File dir = mkdir(tmp, "data-" + keep);
            writeParquet(new File(dir, "a.parquet"), 0, 2);
            final List<String> before = List.of(dir.list());
            final TestRunner runner = newRunner(tmp, dir.getAbsolutePath(), dir, "merged.parquet", keep);
            runner.enqueue(new byte[0]);
            runner.run();
            runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 1);
            assertEquals(new TreeSet<>(before), new TreeSet<>(List.of(dir.list())), "keep=" + keep + ": 디렉터리 내용이 바뀌면 안 된다");
        }
        // 대상 파일명이 *.parquet가 아니면 디렉토리 패턴에 포함되지 않으므로 정상 병합된다
        final File dir = mkdir(tmp, "data-other");
        writeParquet(new File(dir, "a.parquet"), 0, 2);
        final TestRunner runner = newRunner(tmp, dir.getAbsolutePath(), dir, "merged.parq", true);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        assertEquals(2L, countRecords(new File(dir, "merged.parq")));
    }

    /** Glob 문자가 없는 파일 경로를 지정하면 그 파일 하나만 병합해야 한다. */
    @Test
    void plainFileSourcePath_mergesSingleFile(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "a.parquet"), 0, 2);
        writeParquet(new File(src, "b.parquet"), 10, 5);
        final TestRunner runner = newRunner(tmp, new File(src, "b.parquet").getAbsolutePath(), tgt, "merged.parquet", true);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(MergeParquet.REL_SUCCESS).get(0).assertAttributeEquals("merge.hdfs.source.files.count", "1");
        assertEquals(5L, countRecords(new File(tgt, "merged.parquet")));
    }

    /** Glob 문자가 없는 원본 경로가 존재하지 않으면 NPE가 아니라 명확한 메시지로 failure가 되어야 한다. */
    @Test
    void nonexistentPlainSourcePath_failsWithClearMessage(@TempDir Path tmp) throws Exception {
        final File tgt = mkdir(tmp, "tgt");
        final TestRunner runner = newRunner(tmp, tmp.resolve("nope").toString(), tgt, "merged.parquet", false);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_FAILURE, 1);
        final List<org.apache.nifi.util.LogMessage> warns = runner.getLogger().getWarnMessages();
        assertEquals(1, warns.size());
        final Throwable cause = warns.get(0).getThrowable();
        assertTrue(cause instanceof IOException, "NPE가 아니라 IOException이어야 한다: " + cause);
        assertTrue(cause.getMessage().contains("원본 경로가 존재하지 않습니다"), cause.getMessage());
        assertEquals(0, tgt.list().length);
    }

    /** Glob에 일치하는 파일이 없으면 success로 보내되 대상 관련 속성은 쓰지 않고 원본 파일 수 0만 기록해야 한다(P-10). */
    @Test
    void noMatchingFiles_successWithoutTargetAttributes(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", false);
        runner.enqueue(new byte[0], Map.of("k", "v"));
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(MergeParquet.REL_SUCCESS).get(0);
        out.assertAttributeEquals("k", "v");
        out.assertAttributeEquals("merge.hdfs.source.dir", src.getAbsolutePath() + "/*.parquet");
        out.assertAttributeEquals("merge.hdfs.source.files.count", "0");
        out.assertAttributeNotExists("merge.hdfs.target.dir");
        out.assertAttributeNotExists("merge.target.filename");
        out.assertAttributeNotExists("merge.target.qualifiedPath");
        assertFalse(new File(tgt, "merged.parquet").exists());
        assertEquals(0, tgt.list().length);
    }

    /** 대상 디렉토리 끝에 "/"가 있어도 merge.target.qualifiedPath는 스킴을 포함한 정규화된 경로여야 한다(P-10). */
    @Test
    void qualifiedPathAttribute_isFullyQualified(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "a.parquet"), 0, 1);
        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", true);
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_DIR, tgt.getAbsolutePath() + "/");
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        final String qualified = runner.getFlowFilesForRelationship(MergeParquet.REL_SUCCESS).get(0).getAttribute("merge.target.qualifiedPath");
        assertEquals("file:" + tgt.getAbsolutePath() + "/merged.parquet", qualified);
        assertFalse(qualified.substring("file:".length()).contains("//"), qualified);
    }

    /** 동시 태스크가 같은 대상 파일에 기록하지 않도록 @TriggerSerially가 지정되어 있어야 한다(P-10). */
    @Test
    void processorIsTriggerSerially() {
        assertTrue(MergeParquet.class.isAnnotationPresent(org.apache.nifi.annotation.behavior.TriggerSerially.class));
    }

    /**
     * 기본 FS 스킴의 FileSystem 캐시를 비활성화하므로 HadoopInputFile/HadoopOutputFile.fromPath를 쓰면 입력 파일마다, Writer마다
     * 새 FileSystem 인스턴스가 생성되고 close되지 않는다(P-5, HDFS에서는 DFSClient 누수). 기존 FileSystem만 재사용해야 한다.
     */
    @Test
    void bug_fileSystemInstancesLeakedPerFile(@TempDir Path tmp) throws Exception {
        final File src = mkdir(tmp, "src");
        final File tgt = mkdir(tmp, "tgt");
        writeParquet(new File(src, "a.parquet"), 0, 1);
        writeParquet(new File(src, "b.parquet"), 0, 1);
        writeParquet(new File(src, "c.parquet"), 0, 1);
        final File conf = tmp.resolve("counting-core-site.xml").toFile();
        Files.writeString(conf.toPath(), "<?xml version=\"1.0\"?>\n<configuration>\n"
                + "  <property><name>fs.defaultFS</name><value>countfs:///</value></property>\n"
                + "  <property><name>fs.countfs.impl</name><value>" + CountingFileSystem.class.getName() + "</value></property>\n"
                + "</configuration>\n");
        CountingFileSystem.CREATED.set(0);
        CountingFileSystem.CLOSED.set(0);

        final TestRunner runner = newRunner(tmp, src.getAbsolutePath() + "/*.parquet", tgt, "merged.parquet", true);
        runner.setProperty(MergeParquet.PROP_HADOOP_CONFIGURATION_RESOURCES, conf.getAbsolutePath());
        runner.enqueue(new byte[0]);
        runner.run(); // run() 종료 시 @OnStopped 호출 → 캐싱된 FileSystem은 close됨
        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        assertEquals(3L, countRecords(new File(tgt, "merged.parquet")));
        assertTrue(CountingFileSystem.CREATED.get() > 0, "countfs FileSystem이 실제로 사용되어야 한다");
        assertEquals(CountingFileSystem.CREATED.get(), CountingFileSystem.CLOSED.get(),
                "생성된 FileSystem 수(" + CountingFileSystem.CREATED.get() + ")와 close된 수(" + CountingFileSystem.CLOSED.get() + ")가 같아야 한다");
    }

    /** 생성/close 횟수를 세는 로컬 파일 시스템(스킴: countfs). */
    public static class CountingFileSystem extends RawLocalFileSystem {
        static final AtomicInteger CREATED = new AtomicInteger();
        static final AtomicInteger CLOSED = new AtomicInteger();

        public CountingFileSystem() {
            CREATED.incrementAndGet();
        }

        @Override
        public URI getUri() {
            return URI.create("countfs:///");
        }

        @Override
        public String getScheme() {
            return "countfs";
        }

        @Override
        public void close() throws IOException {
            CLOSED.incrementAndGet();
            super.close();
        }
    }
}
