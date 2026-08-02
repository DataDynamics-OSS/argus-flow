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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.logging.ComponentLog;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.parquet.avro.AvroWriteSupport.WRITE_FIXED_AS_INT96;

/**
 * Parquet 파일의 처리를 위한 유틸리티.
 */
public class ParquetUtils {

    /**
     * 지정한 HDFS 디렉토리에 Glob Pattern에 일치하는 파일 목록을 반환한다.
     * <pre>
     * +--------+-----------------------------------------------------------------------------------------------------------+
     * | Glob   | Matches                                                                                                   |
     * +--------+-----------------------------------------------------------------------------------------------------------+
     * | *      | Matches zero or more characters                                                                           |
     * | ?      | Matches a single character                                                                                |
     * | [ab]   | Matches a single character in the set {a, b}                                                              |
     * | [^ab]  | Matches a single character not in the set {a, b}                                                          |
     * | [a-b]  | Matches a single character in the range [a, b] where a is lexicographically less than or equal to b       |
     * | [^a-b] | Matches a single character not in the range [a, b] where a is lexicographically less than or equal to b   |
     * | {a,b}  | Matches either expression a or b                                                                          |
     * | \c     | Matches character c when it is a metacharacter                                                            |
     * +--------+-----------------------------------------------------------------------------------------------------------+
     * </pre>
     *
     * @param path HDFS 디렉토리
     * @param fs   FileSystem
     * @return 파일 목록
     * @throws IOException 파일 시스템에 접근할 수 없는 경우
     */
    public static List<FileStatus> listParquetFilesInDir(Path path, FileSystem fs) throws IOException {
        FileStatus[] files = fs.globStatus(path);
        return Arrays.asList(files);
    }

    /**
     * 지정한 경로의 파일을 모두 삭제한다.
     *
     * @param logger NiFi Component Logger
     * @param files  삭제할 파일 목록
     * @param fs     FileSystem
     */
    public static void deleteAll(ComponentLog logger, List<String> files, FileSystem fs) {
        if (!files.isEmpty()) {
            logger.info("HDFS의 파일을 삭제합니다. 삭제할 파일의 개수는 {}개입니다.", files.size());
            for (String file : files) {
                try {
                    fs.delete(new Path(file), false);
                    logger.info("HDFS에서 파일을 삭제했습니다. 삭제한 파일 : {}", file);
                } catch (Exception e) {
                    logger.warn("HDFS에서 파일을 삭제할 수 없습니다. 삭제할 파일 : {}", file, e);
                }
            }
        } else {
            logger.info("HDFS에서 삭제할 파일이 없습니다.");
        }
    }

    /**
     * HDFS 디렉토리의 Parquet 파일을 Merge한다.
     *
     * @param fs             FileSystem
     * @param sourcePath     HDFS의 Parquet 파일이 저장되어 있는 원본 HDFS 디렉토리
     * @param targetPath     Merge한 Parquet 파일을 저장할 HDFS  디렉토리
     * @param targetFilename Merge한 Parquet 파일명
     * @param logger         NiFi Component Logger
     * @param filesToDelete  Merge를 완료한 경우 삭제할 파일 목록(Session.commitAsync()에서 커밋이 완료된 후 이 파일목록의 파일을 모두 삭제)
     * @throws IOException 파일 시스템에 접근할 수 없는 경우
     */
    public static void merge(FileSystem fs, String sourcePath, String targetPath, String targetFilename, ComponentLog logger, List<String> filesToDelete) throws IOException {

        // Avro Schema
        Schema schema = null;

        // 소스 경로
        Path sourceDir = new Path(sourcePath);

        // 타겟 경로가 존재한다면 삭제
        Path targetFile = new Path(targetPath, targetFilename);
        if (fs.exists(targetFile)) {
            logger.warn("Parquet 파일을 저장할 HDFS 파일의 경로가 존재하므로 강제로 삭제합니다. 경로 : {}", targetFile.getName());
            fs.delete(targetFile, true);
        }

        logger.info("HDFS의 Parquet 파일을 Merge하려고 합니다. 경로 : {}", sourcePath);

        ParquetWriter<Object> writer = null;
        List<FileStatus> files = listParquetFilesInDir(sourceDir, fs); // Glob 패턴을 지원하므로 입력한 문자열이 디렉토리 명인지 파악이 불가능함.
        logger.info("Merge할 Parquet 파일은 {}개입니다.", files.size());
        logger.info("Merge할 Parquet 파일 목록은 다음과 같습니다.\n{}", getParquetFilesAsString(files));
        long rowIndex = 0;
        int fileIndex = 0;
        for (FileStatus file : files) {
            ParquetReader<GenericRecord> reader = getAvroParquetReader(fs.getConf(), file.getPath());
            logger.info("[{}] Merge할 Parquet 파일을 로딩합니다. 경로 : {}", fileIndex, file.getPath().toString());
            fileIndex++;
            GenericRecord record = reader.read();
            while (record != null) {
                if (schema == null) {
                    schema = record.getSchema();
                    String avroSchemaString = schema.toString(true);
                    logger.info("Parquet 파일을 생성하기 위해서 Avro Schema를 로딩할 원본 파일에서 추출했습니다. Avro Schema : \n{}", avroSchemaString);
                }

                if (writer == null) {
                    logger.info("Parquet 파일을 Merge하고 저장할 Writer를 생성했습니다.");
                    writer = getParquetWriter(schema, fs.getConf(), targetFile, logger);
                }

                writer.write(record);
                rowIndex++;

                record = reader.read();
            }

            logger.info("[{}] Merge할 Parquet 파일을 처리를 종료합니다. 경로 : {}", fileIndex, file.getPath().toString());
            filesToDelete.add(file.getPath().toString());
            reader.close();
        }

        if (writer != null) {
            logger.info("Merge한 Parquet 파일의 저장을 완료했습니다. 경로 : {}", targetFile);
            writer.close();
        }
    }

    /**
     * HDFS의 Parquet 목록을 경로만 추출해서 문자열 배열로 구성한다.
     *
     * @param files HDFS 파일 목록
     * @return 파일의 절대 경로 목록
     */
    private static String getParquetFilesAsString(List<FileStatus> files) {
        List<String> paths = new ArrayList<>();
        for (FileStatus file : files) {
            paths.add(file.getPath().toString());
        }
        return String.join("\n", paths);
    }

    /**
     * Parquet 파일을 읽기 위한 Parquet Reader를 생성한다.
     * Parquet Reader는 Avro Parquet Reader로 INT96을 읽기 위해서 추가로 옵션을 지정해야 한다.
     *
     * @param conf Hadoop Configuration
     * @param path 로딩할 HDFS의 Parquet 파일 경로
     * @return Parquet Reader
     * @throws IOException Parquet Reader를 생성할 수 없는 경우
     */
    public static ParquetReader<GenericRecord> getAvroParquetReader(Configuration conf, Path path) throws IOException {
        return AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(path, conf))
                .withConf(conf)
                .set("parquet.avro.readInt96AsFixed", "true")
                .build();
    }

    /**
     * Parquet 파일을 기록하기 위한 Parquet Writer를 생성한다.
     *
     * @param schema        원본 Parquet 파일의 Avro Schema
     * @param configuration Hadoop Configuration
     * @param path          저장할 Parquet 파일의 Fully Qualified Path
     * @param logger        NiFi Component Logger
     * @return Avro Parquet Writer
     * @throws IOException Writer를 생성할 수 없는 경우
     */
    public static ParquetWriter<Object> getParquetWriter(Schema schema, Configuration configuration, Path path, ComponentLog logger) throws IOException {
        Configuration conf = new Configuration(configuration);

        // Int96 형식의 Timestamp 컬럼을 별도 처리하지 않으면 FIXED, Byte Array로 저장한다.
        // 따라서 Parquet Writer에 Int96 형식의 Timestamp 컬럼명을 지정하여 Timestamp 형식으로 처리하도록 해야 한다.
        String[] timestampInt96Columns = getTimestampInt96Columns(schema);
        logger.info("Parquet 파일의 INT96 Timestamp 컬럼명 : {}", String.join(", ", timestampInt96Columns));
        if (timestampInt96Columns.length > 0) {
            conf.setStrings(WRITE_FIXED_AS_INT96, timestampInt96Columns);
        }

        return AvroParquetWriter.builder(HadoopOutputFile.fromPath(path, conf))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize((long) ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withConf(conf)
                .withValidation(false)
                .withDictionaryEncoding(true)
                .build();
    }

    /**
     * Avro Schema에서 INT96인 Timestamp 컬럼만 추출한다.
     * Timestamp 컬럼의 경우 INT96이지만 INT96을 지원하지 않으므로
     * Timestamp 컬럼의 값을 읽고 그대로 기록하면 원본의 Timestamp INT96 컬럼이 Fixed로 스키마가 구성이 된다.
     * 따라서 INT96인 컬럼을 Parquet 저장시 알려주기 위해서 컬럼 목록을 확인하고 Parquet Writer에 지정해야 한다.
     *
     * @param schema 원본 Parquet 파일의 Avro Schema
     * @return Avro Schema에서 INT96 컬럼 목록
     */
    public static String[] getTimestampInt96Columns(Schema schema) {
        List<String> list = new ArrayList<>();
        schema.getFields().forEach(field -> {
            if (field.schema().getType() == Schema.Type.UNION) {
                List<Schema> types = field.schema().getTypes();
                for (Schema type : types) {
                    if (type.getType() == Schema.Type.FIXED && type.getName().equals("INT96")) {
                        list.add(field.name());
                    }
                }
            }
        });
        return list.toArray(new String[0]);
    }
}
