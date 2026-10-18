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
package io.datadynamics.nifi.trino.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Execution Mode=BACKGROUND 동작: 즉시 반환, 완료 후 전송, 가득 차면 yield, 중지 시 cancel + rollback.
 */
class BackgroundExecutionTest {

    private TestRunner runner;
    private ScriptedTrinoConnectionService service;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(ExecuteTrinoStatement.class);
        service = new ScriptedTrinoConnectionService();
        runner.addControllerService("trino", service);
        runner.enableControllerService(service);
        runner.setProperty(TrinoProcessorProperties.TRINO_CONNECTION_SERVICE, "trino");
        runner.setProperty(ExecuteTrinoStatement.SQL_STATEMENT, "ALTER TABLE t EXECUTE optimize");
        runner.setProperty(TrinoProcessorProperties.EXECUTION_MODE, "BACKGROUND");
        runner.setProperty(TrinoProcessorProperties.MAX_BACKGROUND_QUERIES, "1");
    }

    @Test
    void triggerReturnsImmediatelyAndTransfersAfterCompletion() throws Exception {
        final CountDownLatch gate = new CountDownLatch(1);
        service.gate = gate;
        runner.enqueue("", Map.of("id", "1"));

        final long start = System.nanoTime();
        runner.run(1, false, true);
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 5_000, "트리거는 문장 완료를 기다리지 않는다");
        assertTrue(service.started.tryAcquire(10, TimeUnit.SECONDS), "백그라운드 스레드가 문장을 시작해야 한다");
        assertEquals(0, runner.getQueueSize().getObjectCount(), "FlowFile 은 세션에 잡혀 있다");
        assertEquals(0, runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).size());

        gate.countDown();
        await(() -> runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).size() == 1);
        runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).get(0).assertAttributeEquals(TrinoAttributes.UPDATE_COUNT, "7");
        runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).get(0).assertAttributeEquals("id", "1");
        runner.stop();
    }

    @Test
    void yieldsWhenAllBackgroundSlotsAreBusy() throws Exception {
        final CountDownLatch gate = new CountDownLatch(1);
        service.gate = gate;
        runner.enqueue("first");
        runner.enqueue("second");

        runner.run(1, false, true);
        assertTrue(service.started.tryAcquire(10, TimeUnit.SECONDS));
        assertFalse(runner.isYieldCalled());
        runner.run(1, false, false);
        assertTrue(runner.isYieldCalled(), "슬롯이 가득 차면 yield 한다");
        assertEquals(1, runner.getQueueSize().getObjectCount(), "가득 찼을 때는 FlowFile 을 꺼내지 않는다");
        assertEquals(1, service.executed.size());

        gate.countDown();
        await(() -> runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).size() == 1);
        runner.run(1, false, false);
        await(() -> runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).size() == 2);
        runner.stop();
    }

    @Test
    void stopCancelsRunningStatementAndRollsBack() throws Exception {
        service.gate = new CountDownLatch(1);
        runner.enqueue("payload");

        runner.run(1, false, true);
        assertTrue(service.started.tryAcquire(10, TimeUnit.SECONDS));
        runner.unSchedule();
        runner.stop();

        assertEquals(1, service.cancels.get(), "실행 중인 문장을 취소한다");
        await(() -> runner.getQueueSize().getObjectCount() == 1);
        assertEquals(0, runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).size());
        assertEquals(0, runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_FAILURE).size());
    }

    @Test
    void worksWithoutInput() throws Exception {
        runner.setIncomingConnection(false);
        runner.run(1, false, true);
        await(() -> runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).size() == 1);
        runner.stop();
    }

    private static void await(final BooleanSupplier condition) throws InterruptedException {
        final long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("조건이 10초 안에 충족되지 않았다");
            }
            Thread.sleep(20);
        }
    }
}
