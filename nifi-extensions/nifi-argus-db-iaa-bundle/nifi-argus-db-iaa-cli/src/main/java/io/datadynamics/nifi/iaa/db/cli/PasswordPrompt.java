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
package io.datadynamics.nifi.iaa.db.cli;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 비밀번호 입력.
 *
 * <p><strong>인자로 받지 않는다.</strong> 명령행 인자는 {@code ps} 에 노출되고 셸 히스토리에
 * 남는다(NiFi 의 {@code set-single-user-credentials <user> <password>} 가 가진 약점이다).
 * 대화형으로는 에코 없이 입력받고, 자동화에는 stdin 을 쓴다.
 */
public final class PasswordPrompt {

    /** NiFi single-user 프로바이더와 같은 기준. */
    public static final int MINIMUM_LENGTH = 12;

    private PasswordPrompt() {
    }

    /** 확인 입력까지 받아 일치를 검증한다. */
    public static char[] readInteractive() {
        final Console console = System.console();
        if (console == null) {
            throw new CliException("터미널이 아니라 비밀번호를 입력받을 수 없습니다. "
                    + "--password-stdin 을 사용하십시오.");
        }
        final char[] first = console.readPassword("Password: ");
        final char[] second = console.readPassword("Confirm:  ");
        try {
            if (first == null || second == null || !Arrays.equals(first, second)) {
                throw new CliException("두 번 입력한 비밀번호가 다릅니다.");
            }
            validate(first);
            return Arrays.copyOf(first, first.length);
        } finally {
            if (first != null) {
                Arrays.fill(first, '\0');
            }
            if (second != null) {
                Arrays.fill(second, '\0');
            }
        }
    }

    /** stdin 첫 줄을 비밀번호로 읽는다. */
    public static char[] readFromStdin() {
        try {
            final BufferedReader reader =
                    new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            final String line = reader.readLine();
            if (line == null) {
                throw new CliException("stdin 에서 비밀번호를 읽지 못했습니다.");
            }
            final char[] password = line.toCharArray();
            validate(password);
            return password;
        } catch (final IOException e) {
            throw new CliException("stdin 을 읽을 수 없습니다: " + e.getMessage(), e);
        }
    }

    static void validate(final char[] password) {
        if (password.length < MINIMUM_LENGTH) {
            throw new CliException(
                    "비밀번호는 " + MINIMUM_LENGTH + "자 이상이어야 합니다.");
        }
    }
}
