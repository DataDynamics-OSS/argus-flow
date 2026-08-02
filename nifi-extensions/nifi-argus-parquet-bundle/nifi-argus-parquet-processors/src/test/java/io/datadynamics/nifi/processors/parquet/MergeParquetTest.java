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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Path;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
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
}
