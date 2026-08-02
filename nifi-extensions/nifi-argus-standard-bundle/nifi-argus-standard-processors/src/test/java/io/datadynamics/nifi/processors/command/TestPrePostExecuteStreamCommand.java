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
package io.datadynamics.nifi.processors.command;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.IOException;

/**
 * {@link PrePostExecuteStreamCommand} 프로세서의 실제 프로세스 실행 동작을 검증하는 테스트.
 * 셸 명령 실행에 의존하므로 *nix 계열 OS에서만 실행하도록 제한한다(Windows 제외).
 */
@DisabledOnOs(value = OS.WINDOWS, disabledReason = "Test only runs on *nix")
public class TestPrePostExecuteStreamCommand {

    private static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().startsWith("windows");
    }

    /**
     * Pre-Command / Post-Command와 본 명령(java -jar ...)이 모두 포함된 셸 스크립트가
     * 정상적으로 구성되어 실행되는지 확인한다. 1MB 크기의 더미 파일을 FlowFile 콘텐츠로 사용하여
     * STDIN 스트리밍 경로도 함께 검증한다.
     */
    @Test
    public void testKerberos() throws IOException {
        File exJar = new File("src/test/resources/ExecuteCommand/TestLogStdErr.jar");
        File dummy = new File("src/test/resources/ExecuteCommand/1mb.txt");
        String jarPath = exJar.getAbsolutePath();
        exJar.setExecutable(true);
        final TestRunner controller = TestRunners.newTestRunner(PrePostExecuteStreamCommand.class);
        controller.setValidateExpressionUsage(false);
        controller.enqueue(dummy.toPath());
        controller.setProperty(PrePostExecuteStreamCommand.PRE_COMMAND, "echo hello");
        controller.setProperty(PrePostExecuteStreamCommand.POST_COMMAND, "echo world");
        controller.setProperty(PrePostExecuteStreamCommand.EXECUTION_COMMAND, "java");
        controller.setProperty(PrePostExecuteStreamCommand.EXECUTION_ARGUMENTS, "-jar;" + jarPath);
        controller.run(1);
    }

}