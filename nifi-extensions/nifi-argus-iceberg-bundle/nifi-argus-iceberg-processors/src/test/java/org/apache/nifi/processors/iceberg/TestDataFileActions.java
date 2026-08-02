/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-processors/src/test/java/org/apache/nifi/processors/iceberg/TestDataFileActions.java
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software

 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.iceberg;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.iceberg.catalog.IcebergCatalogFactory;
import org.apache.nifi.processors.iceberg.catalog.TestHadoopCatalogService;
import org.apache.nifi.processors.iceberg.converter.IcebergRecordConverter;
import org.apache.nifi.processors.iceberg.writer.IcebergTaskWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.nifi.processors.iceberg.PutIceberg.MAXIMUM_COMMIT_DURATION;
import static org.apache.nifi.processors.iceberg.PutIceberg.MAXIMUM_COMMIT_WAIT_TIME;
import static org.apache.nifi.processors.iceberg.PutIceberg.MINIMUM_COMMIT_WAIT_TIME;
import static org.apache.nifi.processors.iceberg.PutIceberg.NUMBER_OF_COMMIT_RETRIES;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.condition.OS.WINDOWS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * PutIceberg 프로세서의 데이터 파일 관련 동작(작성 실패 시 롤백/abort, 커밋 재시도 로직)을 검증하는 테스트 클래스.
 * TaskWriter로 실제 데이터 파일을 기록한 뒤 abort 처리 시 파일이 정상적으로 삭제되는지,
 * 그리고 커밋 실패 시 설정된 재시도 횟수/최대 지속 시간 조건에 따라 재시도가 올바르게 이루어지는지 확인한다.
 */
public class TestDataFileActions {

    private static final Namespace NAMESPACE = Namespace.of("default");
    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "abort");

    private static final Schema ABORT_SCHEMA = new Schema(
            Types.NestedField.required(0, "id", Types.IntegerType.get())
    );

    private PutIceberg icebergProcessor;
    private ComponentLog logger;

    @BeforeEach
    public void setUp() {
        icebergProcessor = new PutIceberg();
        logger = new MockComponentLog("mock", new Object());
    }

    // 레코드를 TaskWriter로 기록해 데이터 파일들을 실제로 생성한 뒤,
    // icebergProcessor.abort()를 호출했을 때 방금 기록된(아직 커밋되지 않은) 데이터 파일들이
    // 파일시스템에서 정상적으로 삭제되는지 검증한다.
    @DisabledOnOs(WINDOWS)
    @Test
    public void testAbortUncommittedFiles() throws IOException {
        Table table = initCatalog();

        List<RecordField> recordFields = Collections.singletonList(new RecordField("id", RecordFieldType.INT.getDataType()));
        RecordSchema abortSchema = new SimpleRecordSchema(recordFields);

        List<MapRecord> recordList = new ArrayList<>();
        recordList.add(new MapRecord(abortSchema, Collections.singletonMap("id", 1)));
        recordList.add(new MapRecord(abortSchema, Collections.singletonMap("id", 2)));
        recordList.add(new MapRecord(abortSchema, Collections.singletonMap("id", 3)));
        recordList.add(new MapRecord(abortSchema, Collections.singletonMap("id", 4)));
        recordList.add(new MapRecord(abortSchema, Collections.singletonMap("id", 5)));

        IcebergTaskWriterFactory taskWriterFactory = new IcebergTaskWriterFactory(table, new Random().nextLong(), FileFormat.PARQUET, null);
        TaskWriter<Record> taskWriter = taskWriterFactory.create();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(table.schema(), abortSchema, FileFormat.PARQUET, UnmatchedColumnBehavior.IGNORE_UNMATCHED_COLUMN, logger);

        for (MapRecord record : recordList) {
            taskWriter.write(recordConverter.convert(record));
        }

        DataFile[] dataFiles = taskWriter.dataFiles();

        // taskWriter가 기록한 데이터 파일들이 실제로 존재해야 함 - DataFiles written by the taskWriter should exist
        for (DataFile dataFile : dataFiles) {
            Assertions.assertTrue(Files.exists(Paths.get(dataFile.path().toString())));
        }

        icebergProcessor.abort(taskWriter.dataFiles(), table);

        // abort 이후에는 데이터 파일들이 존재하지 않아야 함 - DataFiles shouldn't exist after aborting them
        for (DataFile dataFile : dataFiles) {
            Assertions.assertFalse(Files.exists(Paths.get(dataFile.path().toString())));
        }
    }

    // 커밋이 매번 CommitFailedException으로 실패하는 상황에서, 설정된 재시도 횟수(3회)를 모두 소진한 뒤에도
    // 커밋이 계속 실패하면 최종적으로 예외가 전파되어야 하고, commit()은 최초 시도 1회 + 재시도 3회 = 총 4회 호출되어야 한다.
    @Test
    public void testAppenderCommitRetryExceeded() {
        ProcessContext context = Mockito.mock(ProcessContext.class);
        when(context.getProperty(NUMBER_OF_COMMIT_RETRIES)).thenReturn(new MockPropertyValue("3", null));
        when(context.getProperty(MINIMUM_COMMIT_WAIT_TIME)).thenReturn(new MockPropertyValue("1 ms", null));
        when(context.getProperty(MAXIMUM_COMMIT_WAIT_TIME)).thenReturn(new MockPropertyValue("1 ms", null));
        when(context.getProperty(MAXIMUM_COMMIT_DURATION)).thenReturn(new MockPropertyValue("1 min", null));

        FlowFile mockFlowFile = new MockFlowFile(1234567890L);
        AppendFiles appender = Mockito.mock(AppendFiles.class);
        doThrow(CommitFailedException.class).when(appender).commit();

        Table table = Mockito.mock(Table.class);
        when(table.newAppend()).thenReturn(appender);

        // 재시도 횟수를 모두 초과하면 결국 커밋 액션이 실패해야 함 - assert the commit action eventually fails after exceeding the number of retries
        assertThrows(CommitFailedException.class, () -> icebergProcessor.appendDataFiles(context, mockFlowFile, table, WriteResult.builder().build()));

        // 설정된 횟수만큼 커밋 액션이 호출되었는지 검증 - verify the commit action was called the configured number of times
        verify(appender, times(4)).commit();
    }

    // 커밋이 처음 2번은 CommitFailedException으로 실패하고 3번째 시도에서 성공하는 상황을 시뮬레이션한다.
    // 설정된 재시도 횟수(3회)가 실패 횟수(2회)보다 많으므로 예외 없이 정상적으로 완료되어야 하고,
    // commit()은 정확히 3회(실패 2회 + 성공 1회) 호출되어야 한다.
    @SuppressWarnings("unchecked")
    @Test
    public void testAppenderCommitSucceeded() {
        ProcessContext context = Mockito.mock(ProcessContext.class);
        when(context.getProperty(NUMBER_OF_COMMIT_RETRIES)).thenReturn(new MockPropertyValue("3", null));
        when(context.getProperty(MINIMUM_COMMIT_WAIT_TIME)).thenReturn(new MockPropertyValue("1 ms", null));
        when(context.getProperty(MAXIMUM_COMMIT_WAIT_TIME)).thenReturn(new MockPropertyValue("1 ms", null));
        when(context.getProperty(MAXIMUM_COMMIT_DURATION)).thenReturn(new MockPropertyValue("1 min", null));

        FlowFile mockFlowFile = new MockFlowFile(1234567890L);
        AppendFiles appender = Mockito.mock(AppendFiles.class);
        // 커밋 액션은 성공하기 전까지 2번 예외를 던지도록 설정 - the commit action should throw exception 2 times before succeeding
        doThrow(CommitFailedException.class, CommitFailedException.class).doNothing().when(appender).commit();

        Table table = Mockito.mock(Table.class);
        when(table.newAppend()).thenReturn(appender);

        // 설정된 재시도 횟수가 실패한 커밋 액션 횟수보다 많으므로 예외가 발생하지 않아야 함
        // the method call shouldn't throw exception since the configured number of retries is higher than the number of failed commit actions
        icebergProcessor.appendDataFiles(context, mockFlowFile, table, WriteResult.builder().build());

        // 커밋 액션이 올바른 횟수만큼 호출되었는지 검증 - verify the proper number of commit action was called
        verify(appender, times(3)).commit();
    }

    // 커밋이 계속 실패하는 상황에서, 설정된 최대 커밋 지속 시간(1ms)이 재시도 대기 시간 누적으로 인해
    // 재시도 횟수(5회)에 도달하기 전에 먼저 초과되는 경우를 검증한다.
    // 이 경우 재시도는 설정된 5회가 아니라 시간 제한에 걸려 더 적은 횟수(2회)만 수행되고 예외가 발생해야 한다.
    @Test
    public void testMaxCommitDurationExceeded() {
        ProcessContext context = Mockito.mock(ProcessContext.class);
        when(context.getProperty(NUMBER_OF_COMMIT_RETRIES)).thenReturn(new MockPropertyValue("5", null));
        when(context.getProperty(MINIMUM_COMMIT_WAIT_TIME)).thenReturn(new MockPropertyValue("2 ms", null));
        when(context.getProperty(MAXIMUM_COMMIT_WAIT_TIME)).thenReturn(new MockPropertyValue("2 ms", null));
        when(context.getProperty(MAXIMUM_COMMIT_DURATION)).thenReturn(new MockPropertyValue("1 ms", null));

        FlowFile mockFlowFile = new MockFlowFile(1234567890L);
        AppendFiles appender = Mockito.mock(AppendFiles.class);
        doThrow(CommitFailedException.class).when(appender).commit();

        Table table = Mockito.mock(Table.class);
        when(table.newAppend()).thenReturn(appender);

        // 최대 재시도 지속 시간을 초과하면 결국 커밋 액션이 실패해야 함 - assert the commit action eventually fails after exceeding duration of maximum retries
        assertThrows(CommitFailedException.class, () -> icebergProcessor.appendDataFiles(context, mockFlowFile, table, WriteResult.builder().build()));

        // 설정된 5회가 아니라 시간 제한으로 인해 2회만 커밋 액션이 호출되었는지 검증 - verify the commit action was called only 2 times instead of the configured 5
        verify(appender, times(2)).commit();
    }

    // 테스트용 Hadoop 카탈로그 서비스를 이용해 카탈로그를 생성하고, ABORT_SCHEMA로 비파티션 테이블을 만들어 반환한다.
    private Table initCatalog() throws IOException {
        TestHadoopCatalogService catalogService = new TestHadoopCatalogService();
        IcebergCatalogFactory catalogFactory = new IcebergCatalogFactory(catalogService);
        Catalog catalog = catalogFactory.create();

        return catalog.createTable(TABLE_IDENTIFIER, ABORT_SCHEMA, PartitionSpec.unpartitioned());
    }
}
