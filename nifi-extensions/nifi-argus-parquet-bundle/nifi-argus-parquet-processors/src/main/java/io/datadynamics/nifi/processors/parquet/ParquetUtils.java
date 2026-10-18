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
import org.apache.hadoop.fs.GlobExpander;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.logging.ComponentLog;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.apache.parquet.avro.AvroWriteSupport.WRITE_FIXED_AS_INT96;

/**
 * Parquet 파일의 처리를 위한 유틸리티.
 */
public class ParquetUtils {

    /**
     * Merge 임시 파일명에 사용하는 구분자.
     */
    static final String TEMP_FILE_INFIX = ".tmp-";

    /**
     * 기존 대상 파일 교체 시 사용하는 백업 파일명의 구분자.
     */
    static final String BACKUP_FILE_INFIX = ".bak-";

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
        // Glob이 아닌 경로가 존재하지 않으면 globStatus는 null을 반환합니다.
        return files == null ? new ArrayList<>() : Arrays.asList(files);
    }

    /**
     * 원본 경로를 Glob 패턴으로 해석한다.
     * Glob 문자(<code>* ? [ ] { } \</code>)를 포함하면 그대로 사용하고, 포함하지 않으면서 디렉토리이면
     * <code>&lt;디렉토리&gt;/*.parquet</code>로 해석한다. Glob 문자를 포함하지 않는 파일 경로는 그대로 사용한다.
     *
     * @param fs         FileSystem
     * @param sourcePath 원본 경로(Glob 패턴 또는 디렉토리 또는 파일)
     * @return Glob 패턴
     * @throws IOException Glob 문자를 포함하지 않는 경로가 존재하지 않는 경우
     */
    static String resolveSourcePattern(FileSystem fs, String sourcePath) throws IOException {
        if (hasGlobCharacters(sourcePath)) {
            return sourcePath;
        }
        Path path = new Path(sourcePath);
        if (!fs.exists(path)) {
            throw new IOException(String.format("Merge할 Parquet 파일의 원본 경로가 존재하지 않습니다. 경로 : %s", sourcePath));
        }
        if (fs.getFileStatus(path).isDirectory()) {
            return new Path(path, "*.parquet").toString();
        }
        return sourcePath;
    }

    private static boolean hasGlobCharacters(String path) {
        for (char c : new char[]{'*', '?', '[', ']', '{', '}', '\\'}) {
            if (path.indexOf(c) >= 0) {
                return true;
            }
        }
        return false;
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
                    if (!fs.delete(new Path(file), false)) {
                        logger.warn("HDFS에서 파일을 삭제하지 못했습니다(파일이 없거나 삭제가 거부됨). 삭제할 파일 : {}", file);
                        continue;
                    }
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
     * <p>
     * Merge 결과는 대상 디렉토리의 임시 파일(<code>.&lt;파일명&gt;.tmp-&lt;UUID&gt;</code>)에 먼저 기록하고,
     * Writer를 정상적으로 close한 후에만 대상 파일을 교체한다. 따라서 Merge가 실패하면 기존 대상 파일은 그대로 유지되며
     * 최종 경로에 불완전한 파일이 남지 않는다(임시 파일은 실패 시 삭제한다).
     * <p>
     * 대상 파일이 원본 Glob 패턴에 포함되면(대상 파일의 존재 여부와 무관) 재실행 시 이전 Merge 결과가 원본으로 취급되어
     * 삭제되거나 중복될 수 있으므로, 파일 시스템을 변경하지 않고(읽기/쓰기/삭제 없음) 예외를 발생시킨다.
     *
     * 원본 경로는 Glob 패턴이며, Glob 문자가 없는 디렉토리 경로는 <code>&lt;디렉토리&gt;/*.parquet</code>로 해석한다.
     *
     * @param fs             FileSystem
     * @param sourcePath     HDFS의 Parquet 파일이 저장되어 있는 원본 HDFS 디렉토리 또는 Glob 패턴
     * @param targetPath     Merge한 Parquet 파일을 저장할 HDFS  디렉토리
     * @param targetFilename Merge한 Parquet 파일명
     * @param logger         NiFi Component Logger
     * @param filesToDelete  Merge를 완료한 경우 삭제할 파일 목록(Session.commitAsync()에서 커밋이 완료된 후 이 파일목록의 파일을 모두 삭제)
     * @return 대상 파일을 기록(교체)한 경우 대상 파일의 Fully Qualified Path. Merge할 파일이나 레코드가 없어 대상 파일을 기록하지 않은 경우 null
     * @throws IOException 파일 시스템에 접근할 수 없거나 원본 경로가 없거나 대상 파일이 원본 Glob 패턴에 포함되는 경우
     */
    public static Path merge(FileSystem fs, String sourcePath, String targetPath, String targetFilename, ComponentLog logger, List<String> filesToDelete) throws IOException {

        // 대상 파일명을 검증합니다. ".", ".."이나 경로 구분자가 포함되면 대상 디렉토리 자체 또는 다른 경로를 가리키게 됩니다.
        validateTargetFilename(targetFilename);

        // 소스 경로(Glob 문자가 없는 디렉토리는 <디렉토리>/*.parquet로 해석)
        String sourcePattern = resolveSourcePattern(fs, sourcePath);
        Path sourceDir = new Path(sourcePattern);

        // 대상 경로(비교를 위해 Fully Qualified Path로 변환)
        Path targetFile = fs.makeQualified(new Path(targetPath, targetFilename));

        // 대상 파일이 원본 Glob 패턴에 포함되면 Merge하지 않습니다(이전 Merge 결과의 유실/중복 방지).
        if (isTargetInsideSourceGlob(fs, sourcePattern, targetFile)) {
            throw new IOException(targetInsideSourceGlobMessage(targetFile, sourcePattern));
        }

        if (fs.exists(targetFile) && fs.getFileStatus(targetFile).isDirectory()) {
            throw new IOException(String.format("Parquet 파일을 저장할 HDFS 경로가 디렉토리이므로 Merge할 수 없습니다. 경로 : %s", targetFile));
        }

        logger.info("HDFS의 Parquet 파일을 Merge하려고 합니다. 경로 : {}", sourcePattern);

        // 대상 파일의 임시 파일과 백업 파일은 원본 파일 목록에서 제외합니다.
        List<FileStatus> files = new ArrayList<>();
        for (FileStatus file : listParquetFilesInDir(sourceDir, fs)) {
            Path path = fs.makeQualified(file.getPath());
            if (path.equals(targetFile)) {
                // 패턴 검사에서 걸러지지 않은 경우를 위한 방어 코드
                throw new IOException(targetInsideSourceGlobMessage(targetFile, sourcePattern));
            }
            if (isTempFileOf(path, targetFile)) {
                logger.info("Merge 임시 파일 또는 백업 파일은 원본 파일 목록에서 제외합니다. 경로 : {}", path);
                continue;
            }
            files.add(file);
        }
        logger.info("Merge할 Parquet 파일은 {}개입니다.", files.size());
        logger.info("Merge할 Parquet 파일 목록은 다음과 같습니다.\n{}", getParquetFilesAsString(files));

        if (files.isEmpty()) {
            // Merge할 원본 파일이 없으면 기존 대상 파일을 변경하지 않습니다.
            logger.info("Merge할 Parquet 파일이 없으므로 대상 파일을 변경하지 않습니다. 원본 경로 : {}, 대상 경로 : {}", sourcePattern, targetFile);
            return null;
        }

        List<Path> inputs = new ArrayList<>();
        for (FileStatus file : files) {
            inputs.add(file.getPath());
        }

        // 대상 디렉토리에 숨김 임시 파일을 생성하여 기록합니다(*.parquet Glob에 일치하지 않음).
        Path tempFile = new Path(targetFile.getParent(), "." + targetFilename + TEMP_FILE_INFIX + UUID.randomUUID());
        Path writtenFile = null;
        try {
            long rowIndex = writeMergedFile(fs, inputs, tempFile, logger);
            if (rowIndex < 0) {
                // 레코드가 하나도 없으면 Writer를 생성하지 않으므로 대상 파일을 변경하지 않습니다.
                logger.info("Merge할 레코드가 없으므로 대상 파일을 변경하지 않습니다. 경로 : {}", targetFile);
            } else {
                replaceTarget(fs, tempFile, targetFile, logger);
                writtenFile = targetFile;
                logger.info("Merge한 Parquet 파일의 저장을 완료했습니다. 경로 : {}, 레코드 수 : {}", targetFile, rowIndex);
            }
        } finally {
            if (fs.exists(tempFile)) {
                try {
                    fs.delete(tempFile, false);
                    logger.info("Merge 임시 파일을 삭제했습니다. 경로 : {}", tempFile);
                } catch (IOException e) {
                    logger.warn("Merge 임시 파일을 삭제할 수 없습니다. 경로 : {}", tempFile, e);
                }
            }
        }

        // 대상 파일이 교체된 후에만 삭제할 원본 파일 목록을 구성합니다(대상 파일은 포함하지 않음).
        for (FileStatus file : files) {
            filesToDelete.add(file.getPath().toString());
        }
        return writtenFile;
    }

    /**
     * 대상 파일명을 검증한다. 비어 있거나 ".", ".."이거나 경로 구분자("/")를 포함하면 허용하지 않는다.
     *
     * @param targetFilename Merge한 Parquet 파일명
     */
    static void validateTargetFilename(String targetFilename) {
        if (targetFilename == null || targetFilename.trim().isEmpty()
                || ".".equals(targetFilename) || "..".equals(targetFilename) || targetFilename.contains("/")) {
            throw new IllegalArgumentException(String.format("Merge한 Parquet 파일명이 유효하지 않습니다. 파일명 : '%s'", targetFilename));
        }
    }

    /**
     * 지정한 경로가 대상 파일의 Merge 임시 파일(<code>.&lt;파일명&gt;.tmp-*</code>) 또는
     * 백업 파일(<code>.&lt;파일명&gt;.bak-*</code>)인지 확인한다.
     */
    private static boolean isTempFileOf(Path path, Path targetFile) {
        String prefix = "." + targetFile.getName();
        return path.getParent() != null && path.getParent().equals(targetFile.getParent())
                && (path.getName().startsWith(prefix + TEMP_FILE_INFIX) || path.getName().startsWith(prefix + BACKUP_FILE_INFIX));
    }

    /**
     * 대상 파일 경로가 원본 Glob 패턴에 포함되는지 확인한다. 대상 파일이 아직 존재하지 않아도 패턴으로 판단한다.
     * Hadoop Globber와 같이 중괄호({a,b})를 먼저 전개한 후 경로 구성요소(/) 단위로 Glob 패턴을 비교한다.
     *
     * @param fs         FileSystem
     * @param sourcePath 원본 Glob 패턴
     * @param targetFile Fully Qualified 대상 파일 경로
     * @return 포함되면 true
     * @throws IOException Glob 패턴을 해석할 수 없는 경우
     */
    static boolean isTargetInsideSourceGlob(FileSystem fs, String sourcePath, Path targetFile) throws IOException {
        Path qualifiedTarget = fs.makeQualified(targetFile);
        String[] targetComponents = qualifiedTarget.toUri().getPath().split("/");
        for (String expanded : GlobExpander.expand(sourcePath)) {
            String[] globComponents = fs.makeQualified(new Path(expanded)).toUri().getPath().split("/");
            if (globComponents.length != targetComponents.length) {
                continue;
            }
            boolean matched = true;
            for (int i = 0; i < globComponents.length && matched; i++) {
                matched = globComponents[i].equals(targetComponents[i]) || new GlobPattern(globComponents[i]).matches(targetComponents[i]);
            }
            if (matched) {
                return true;
            }
        }
        return false;
    }

    private static String targetInsideSourceGlobMessage(Path targetFile, String sourcePath) {
        return String.format("대상 파일 '%s'이 원본 경로 패턴 '%s'에 포함됩니다. 대상 디렉토리 또는 파일명을 원본 패턴 밖으로 지정하세요.", targetFile, sourcePath);
    }

    /**
     * 입력 Parquet 파일의 레코드를 순서대로 읽어 지정한 파일에 기록한다.
     * 기록하기 전에 모든 입력 파일의 Footer 스키마가 첫 번째 파일과 같은지 먼저 검사하며, 다르면 아무것도 기록하지 않고 예외를 발생시킨다.
     * Reader/Writer는 예외가 발생해도 반드시 close한다.
     *
     * @return 기록한 레코드 수. 레코드가 없어 Writer를 생성하지 않은 경우 -1
     */
    private static long writeMergedFile(FileSystem fs, List<Path> inputs, Path outputFile, ComponentLog logger) throws IOException {
        // 입력 파일의 스키마가 모두 같은지 확인합니다(Writer는 위치 기반으로 기록하므로 스키마가 다르면 값이 뒤바뀌거나 컬럼이 누락됨).
        MessageType parquetSchema = validateSameSchema(fs, inputs, logger);

        // INT96 컬럼은 Footer의 Parquet 스키마에서 모든 경로(REQUIRED/OPTIONAL, 중첩 Group 포함)를 수집합니다.
        String[] timestampInt96Columns = getTimestampInt96Columns(parquetSchema);

        // Avro Schema
        Schema schema = null;
        ParquetWriter<Object> writer = null;
        long rowIndex = 0;
        int fileIndex = 0;
        try {
            for (Path input : inputs) {
                logger.info("[{}] Merge할 Parquet 파일을 로딩합니다. 경로 : {}", fileIndex, input.toString());
                try (ParquetReader<GenericRecord> reader = getAvroParquetReader(fs, input)) {
                    GenericRecord record = reader.read();
                    while (record != null) {
                        if (schema == null) {
                            schema = record.getSchema();
                            String avroSchemaString = schema.toString(true);
                            logger.info("Parquet 파일을 생성하기 위해서 Avro Schema를 로딩할 원본 파일에서 추출했습니다. Avro Schema : \n{}", avroSchemaString);
                        }

                        if (writer == null) {
                            logger.info("Parquet 파일을 Merge하고 저장할 Writer를 생성했습니다. 임시 파일 : {}", outputFile);
                            writer = getParquetWriter(schema, timestampInt96Columns, fs, outputFile, logger);
                        }

                        writer.write(record);
                        rowIndex++;

                        record = reader.read();
                    }
                }
                logger.info("[{}] Merge할 Parquet 파일을 처리를 종료합니다. 경로 : {}", fileIndex, input.toString());
                fileIndex++;
            }
        } catch (IOException | RuntimeException e) {
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception closeException) {
                    e.addSuppressed(closeException);
                }
            }
            throw e;
        }

        if (writer == null) {
            return -1;
        }
        // Writer를 close해야 footer가 기록되어 완전한 Parquet 파일이 됩니다.
        writer.close();
        return rowIndex;
    }

    /**
     * 임시 파일로 대상 파일을 교체한다. 대상 파일이 존재하면 백업 파일로 rename한 후 임시 파일을 대상 파일로 rename하고,
     * 실패하면 백업 파일을 원래 경로로 복구한다. 모든 삭제는 재귀 삭제를 사용하지 않는다.
     */
    private static void replaceTarget(FileSystem fs, Path tempFile, Path targetFile, ComponentLog logger) throws IOException {
        Path backupFile = null;
        if (fs.exists(targetFile)) {
            backupFile = new Path(targetFile.getParent(), "." + targetFile.getName() + BACKUP_FILE_INFIX + UUID.randomUUID());
            if (!fs.rename(targetFile, backupFile)) {
                throw new IOException(String.format("기존 대상 파일을 백업할 수 없습니다. 경로 : %s", targetFile));
            }
            logger.info("기존 대상 파일을 백업했습니다. 경로 : {} -> {}", targetFile, backupFile);
        }

        boolean renamed;
        try {
            renamed = fs.rename(tempFile, targetFile);
        } catch (IOException e) {
            restoreBackup(fs, backupFile, targetFile, logger);
            throw e;
        }
        if (!renamed) {
            restoreBackup(fs, backupFile, targetFile, logger);
            throw new IOException(String.format("Merge 임시 파일을 대상 파일로 교체할 수 없습니다. 경로 : %s -> %s", tempFile, targetFile));
        }

        if (backupFile != null) {
            try {
                fs.delete(backupFile, false);
            } catch (IOException e) {
                logger.warn("기존 대상 파일의 백업을 삭제할 수 없습니다. 경로 : {}", backupFile, e);
            }
        }
    }

    /**
     * 백업한 기존 대상 파일을 원래 경로로 복구한다.
     */
    private static void restoreBackup(FileSystem fs, Path backupFile, Path targetFile, ComponentLog logger) {
        if (backupFile == null) {
            return;
        }
        try {
            if (!fs.rename(backupFile, targetFile)) {
                logger.error("기존 대상 파일을 복구할 수 없습니다. 백업 파일 : {}", backupFile);
            }
        } catch (IOException e) {
            logger.error("기존 대상 파일을 복구할 수 없습니다. 백업 파일 : {}", backupFile, e);
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
     * 파일은 전달받은 FileSystem으로 직접 열어 FileSystem 인스턴스를 새로 생성하지 않는다.
     *
     * @param fs   FileSystem
     * @param path 로딩할 HDFS의 Parquet 파일 경로
     * @return Parquet Reader
     * @throws IOException Parquet Reader를 생성할 수 없는 경우
     */
    public static ParquetReader<GenericRecord> getAvroParquetReader(FileSystem fs, Path path) throws IOException {
        return AvroParquetReader.<GenericRecord>builder(new FileSystemInputFile(fs, path))
                .withConf(fs.getConf())
                .set("parquet.avro.readInt96AsFixed", "true")
                .build();
    }

    /**
     * Parquet 파일을 기록하기 위한 Parquet Writer를 생성한다.
     *
     * @param schema                원본 Parquet 파일의 Avro Schema
     * @param timestampInt96Columns INT96으로 기록할 컬럼의 경로 목록(Avro 필드명을 "."으로 연결한 경로, {@link #getTimestampInt96Columns(MessageType)})
     * @param fs            FileSystem(파일은 이 FileSystem으로 직접 생성하며 FileSystem 인스턴스를 새로 생성하지 않음)
     * @param path          저장할 Parquet 파일의 Fully Qualified Path
     * @param logger        NiFi Component Logger
     * @return Avro Parquet Writer
     * @throws IOException Writer를 생성할 수 없는 경우
     */
    public static ParquetWriter<Object> getParquetWriter(Schema schema, String[] timestampInt96Columns, FileSystem fs, Path path, ComponentLog logger) throws IOException {
        Configuration conf = new Configuration(fs.getConf());

        // Int96 형식의 Timestamp 컬럼을 별도 처리하지 않으면 FIXED, Byte Array로 저장한다.
        // 따라서 Parquet Writer에 Int96 형식의 Timestamp 컬럼 경로를 지정하여 Timestamp 형식으로 처리하도록 해야 한다.
        logger.info("Parquet 파일의 INT96 Timestamp 컬럼명 : {}", String.join(", ", timestampInt96Columns));
        if (timestampInt96Columns.length > 0) {
            conf.setStrings(WRITE_FIXED_AS_INT96, timestampInt96Columns);
        }

        return AvroParquetWriter.builder(new FileSystemOutputFile(fs, path))
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
     * 이 메소드는 최상위 UNION(nullable) 필드만 검사하므로 REQUIRED/중첩 INT96 컬럼은 찾지 못한다.
     * Merge 시에는 Footer의 Parquet 스키마를 사용하는 {@link #getTimestampInt96Columns(MessageType)}를 사용한다.
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

    /**
     * 모든 입력 Parquet 파일의 Footer 스키마가 첫 번째 파일의 스키마와 구조적으로 같은지 검사한다.
     * 필드명, 필드 순서, Primitive 타입(FIXED 길이 포함), Repetition(REQUIRED/OPTIONAL/REPEATED), Logical Type을 비교한다.
     * 최상위 Message 이름(Avro Record 이름), Field ID, Key-Value 메타데이터(Avro Schema의 doc/namespace 등), created_by는
     * 기록되는 값에 영향을 주지 않으므로 비교하지 않는다.
     *
     * @param fs     FileSystem
     * @param inputs 입력 Parquet 파일 목록
     * @param logger NiFi Component Logger
     * @throws IOException Footer를 읽을 수 없거나 스키마가 다른 경우
     */
    static MessageType validateSameSchema(FileSystem fs, List<Path> inputs, ComponentLog logger) throws IOException {
        MessageType baseSchema = null;
        Path basePath = null;
        for (Path input : inputs) {
            MessageType schema;
            try (ParquetFileReader reader = ParquetFileReader.open(new FileSystemInputFile(fs, input))) {
                schema = reader.getFooter().getFileMetaData().getSchema();
            }
            if (baseSchema == null) {
                baseSchema = schema;
                basePath = input;
                continue;
            }
            String difference = describeDifference(baseSchema, schema, "");
            if (difference != null) {
                throw new IOException(String.format("Parquet 파일 '%s'의 스키마가 첫 번째 파일 '%s'의 스키마와 다르므로 Merge할 수 없습니다. 차이 : %s",
                        input, basePath, difference));
            }
        }
        logger.info("Merge할 Parquet 파일 {}개의 스키마가 모두 같습니다.", inputs.size());
        return baseSchema;
    }

    /**
     * Parquet 스키마(Footer)에서 INT96 Primitive 컬럼을 모든 경로에서 수집한다.
     * REQUIRED/OPTIONAL 여부와 무관하며 중첩 Group(Record), List, Map 내부의 컬럼도 포함한다.
     * <p>
     * 반환하는 경로는 AvroWriteSupport의 <code>parquet.avro.writeFixedAsInt96</code>가 비교하는 형식으로,
     * Avro 필드명을 "."으로 연결한 경로이다(예 : <code>event.ts</code>). List/Map의 Parquet 래퍼 Group
     * (<code>list</code>/<code>element</code>/<code>array</code>/<code>key_value</code>/<code>value</code>)은
     * Avro 경로에 포함되지 않으므로 건너뛴다.
     *
     * @param schema Parquet 파일의 Footer 스키마
     * @return INT96 컬럼의 경로 목록
     */
    public static String[] getTimestampInt96Columns(MessageType schema) {
        List<String> list = new ArrayList<>();
        collectInt96Fields(schema, "", list);
        return list.toArray(new String[0]);
    }

    /**
     * Record에 해당하는 Group의 모든 하위 필드에서 INT96 컬럼을 수집한다.
     */
    private static void collectInt96Fields(GroupType group, String path, List<String> out) {
        for (Type field : group.getFields()) {
            collectInt96Type(field, path.isEmpty() ? field.getName() : path + "." + field.getName(), out);
        }
    }

    /**
     * 지정한 필드(경로가 이미 계산된 필드)에서 INT96 컬럼을 수집한다.
     */
    private static void collectInt96Type(Type type, String path, List<String> out) {
        if (type.isPrimitive()) {
            if (type.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96 && !out.contains(path)) {
                out.add(path);
            }
            return;
        }
        GroupType group = type.asGroupType();
        LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();
        boolean isMap = annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation
                || annotation instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation;
        boolean isList = annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation;
        if ((isMap || isList) && group.getFieldCount() == 1 && group.getType(0).isRepetition(Type.Repetition.REPEATED)) {
            Type repeated = group.getType(0);
            if (repeated.isPrimitive()) {
                // 2-level List의 Primitive 원소 : Avro 경로는 List 필드의 경로와 같음
                collectInt96Type(repeated, path, out);
            } else if (isMap) {
                // Map : repeated group key_value { key; value } - Avro 경로는 Map 필드의 경로와 같음
                GroupType keyValue = repeated.asGroupType();
                if (keyValue.getFieldCount() == 2) {
                    collectInt96Type(keyValue.getType(1), path, out);
                }
            } else {
                GroupType repeatedGroup = repeated.asGroupType();
                boolean elementIsRepeatedGroup = repeatedGroup.getFieldCount() != 1
                        || "array".equals(repeatedGroup.getName()) || repeatedGroup.getName().endsWith("_tuple");
                if (elementIsRepeatedGroup) {
                    // 2-level List의 Record 원소 : repeated group 자체가 원소
                    collectInt96Fields(repeatedGroup, path, out);
                } else {
                    // 3-level List : repeated group list { element } - 원소의 Avro 경로는 List 필드의 경로와 같음
                    collectInt96Type(repeatedGroup.getType(0), path, out);
                }
            }
            return;
        }
        // 중첩 Record(또는 다중 타입 UNION의 member Group)
        collectInt96Fields(group, path, out);
    }

    /**
     * 두 Group 타입의 하위 필드를 구조적으로 비교한다.
     *
     * @return 차이 설명. 같으면 null
     */
    private static String describeDifference(GroupType expected, GroupType actual, String parent) {
        List<Type> expectedFields = expected.getFields();
        List<Type> actualFields = actual.getFields();
        int count = Math.min(expectedFields.size(), actualFields.size());
        for (int i = 0; i < count; i++) {
            Type e = expectedFields.get(i);
            Type a = actualFields.get(i);
            String path = parent.isEmpty() ? e.getName() : parent + "." + e.getName();
            if (!e.getName().equals(a.getName())) {
                return String.format("%d번째 필드명이 다릅니다(기준 : %s, 대상 : %s)", i + 1, path, parent.isEmpty() ? a.getName() : parent + "." + a.getName());
            }
            if (e.getRepetition() != a.getRepetition()) {
                return String.format("필드 '%s'의 Repetition이 다릅니다(기준 : %s, 대상 : %s)", path, e.getRepetition(), a.getRepetition());
            }
            if (e.isPrimitive() != a.isPrimitive()) {
                return String.format("필드 '%s'의 타입 종류가 다릅니다(기준 : %s, 대상 : %s)", path, e.isPrimitive() ? "primitive" : "group", a.isPrimitive() ? "primitive" : "group");
            }
            if (!Objects.equals(e.getLogicalTypeAnnotation(), a.getLogicalTypeAnnotation())) {
                return String.format("필드 '%s'의 Logical Type이 다릅니다(기준 : %s, 대상 : %s)", path, e.getLogicalTypeAnnotation(), a.getLogicalTypeAnnotation());
            }
            if (e.isPrimitive()) {
                PrimitiveType ep = e.asPrimitiveType();
                PrimitiveType ap = a.asPrimitiveType();
                if (ep.getPrimitiveTypeName() != ap.getPrimitiveTypeName() || ep.getTypeLength() != ap.getTypeLength()) {
                    return String.format("필드 '%s'의 타입이 다릅니다(기준 : %s(%d), 대상 : %s(%d))", path,
                            ep.getPrimitiveTypeName(), ep.getTypeLength(), ap.getPrimitiveTypeName(), ap.getTypeLength());
                }
            } else {
                String nested = describeDifference(e.asGroupType(), a.asGroupType(), path);
                if (nested != null) {
                    return nested;
                }
            }
        }
        if (expectedFields.size() != actualFields.size()) {
            return String.format("%s필드 개수가 다릅니다(기준 : %d개, 대상 : %d개)", parent.isEmpty() ? "" : "'" + parent + "'의 ",
                    expectedFields.size(), actualFields.size());
        }
        return null;
    }

    /**
     * 기존 FileSystem 인스턴스로 파일을 여는 Parquet InputFile.
     * HadoopInputFile.fromPath/fromStatus는 Path.getFileSystem(conf)으로 FileSystem을 다시 조회하므로
     * FileSystem 캐시가 비활성화된 환경에서는 파일마다 close되지 않는 FileSystem 인스턴스가 생성된다.
     */
    static final class FileSystemInputFile implements InputFile {

        private final FileSystem fs;

        private final Path path;

        private Long length;

        FileSystemInputFile(FileSystem fs, Path path) {
            this.fs = fs;
            this.path = path;
        }

        @Override
        public long getLength() throws IOException {
            if (length == null) {
                length = fs.getFileStatus(path).getLen();
            }
            return length;
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            return HadoopStreams.wrap(fs.open(path));
        }

        @Override
        public String toString() {
            return path.toString();
        }
    }

    /**
     * 기존 FileSystem 인스턴스로 파일을 생성하는 Parquet OutputFile.
     * HadoopOutputFile.fromPath는 Path.getFileSystem(conf)으로 FileSystem을 다시 조회하므로 사용하지 않는다.
     */
    static final class FileSystemOutputFile implements OutputFile {

        private final FileSystem fs;

        private final Path path;

        FileSystemOutputFile(FileSystem fs, Path path) {
            this.fs = fs;
            this.path = path;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            return HadoopStreams.wrap(fs.create(path, false, bufferSize(), fs.getDefaultReplication(path), blockSize(blockSizeHint)));
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            return HadoopStreams.wrap(fs.create(path, true, bufferSize(), fs.getDefaultReplication(path), blockSize(blockSizeHint)));
        }

        @Override
        public boolean supportsBlockSize() {
            return true;
        }

        @Override
        public long defaultBlockSize() {
            return fs.getDefaultBlockSize(path);
        }

        @Override
        public String getPath() {
            return path.toString();
        }

        private int bufferSize() {
            return fs.getConf().getInt("io.file.buffer.size", 4096);
        }

        private long blockSize(long blockSizeHint) {
            return Math.max(fs.getDefaultBlockSize(path), blockSizeHint);
        }

        @Override
        public String toString() {
            return path.toString();
        }
    }
}
