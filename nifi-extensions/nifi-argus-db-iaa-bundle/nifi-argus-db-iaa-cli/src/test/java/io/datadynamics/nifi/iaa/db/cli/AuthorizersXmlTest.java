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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AuthorizersXmlTest {

    @TempDir
    Path tempDir;

    private File write(final String xml) throws IOException {
        final Path file = tempDir.resolve("authorizers.xml");
        Files.writeString(file, xml);
        return file.toFile();
    }

    private static final String VALID = """
            <authorizers>
              <userGroupProvider>
                <identifier>file-user-group-provider</identifier>
                <class>org.apache.nifi.authorization.FileUserGroupProvider</class>
                <property name="Users File">./conf/users.xml</property>
              </userGroupProvider>
              <userGroupProvider>
                <identifier>db-user-group-provider</identifier>
                <class>io.datadynamics.nifi.iaa.db.DbUserGroupProvider</class>
                <property name="Database URL">jdbc:postgresql://db:5432/nifi</property>
                <property name="Database User">nifi</property>
                <property name="Cache Duration">1 min</property>
              </userGroupProvider>
            </authorizers>
            """;

    @Test
    void 클래스로_DB_프로바이더를_찾는다() throws Exception {
        final Map<String, String> properties = AuthorizersXml.readUserGroupProvider(write(VALID), null);

        assertEquals("jdbc:postgresql://db:5432/nifi", properties.get("Database URL"));
        assertEquals("nifi", properties.get("Database User"));
        assertEquals("1 min", properties.get("Cache Duration"));
        // 다른 프로바이더의 property 가 섞이면 안 된다
        assertTrue(!properties.containsKey("Users File"));
    }

    @Test
    void identifier_로도_찾을_수_있다() throws Exception {
        final Map<String, String> properties =
                AuthorizersXml.readUserGroupProvider(write(VALID), "db-user-group-provider");
        assertEquals("jdbc:postgresql://db:5432/nifi", properties.get("Database URL"));
    }

    @Test
    void 없는_identifier_는_알기_쉬운_오류() throws Exception {
        final File file = write(VALID);
        final CliException e = assertThrows(CliException.class,
                () -> AuthorizersXml.readUserGroupProvider(file, "없는-프로바이더"));
        assertTrue(e.getMessage().contains("없는-프로바이더"), e.getMessage());
    }

    @Test
    void DB_프로바이더가_없으면_주석_해제를_안내한다() throws Exception {
        final File file = write("""
                <authorizers>
                  <userGroupProvider>
                    <identifier>file-user-group-provider</identifier>
                    <class>org.apache.nifi.authorization.FileUserGroupProvider</class>
                  </userGroupProvider>
                </authorizers>
                """);
        final CliException e = assertThrows(CliException.class,
                () -> AuthorizersXml.readUserGroupProvider(file, null));
        assertTrue(e.getMessage().contains("주석"), e.getMessage());
    }

    @Test
    void 주석_처리된_블록은_찾지_못한다() throws Exception {
        // 배포 기본값은 주석 상태다. 이때는 "주석을 해제하라"는 안내가 나와야 한다.
        final File file = write("""
                <authorizers>
                  <!--
                  <userGroupProvider>
                    <identifier>db-user-group-provider</identifier>
                    <class>io.datadynamics.nifi.iaa.db.DbUserGroupProvider</class>
                  </userGroupProvider>
                  -->
                </authorizers>
                """);
        assertThrows(CliException.class, () -> AuthorizersXml.readUserGroupProvider(file, null));
    }

    @Test
    void 파일이_없으면_경로와_대안을_알려준다() {
        final File missing = tempDir.resolve("없음.xml").toFile();
        final CliException e = assertThrows(CliException.class,
                () -> AuthorizersXml.readUserGroupProvider(missing, null));
        assertTrue(e.getMessage().contains("--conf"), e.getMessage());
    }

    @Test
    void DOCTYPE_이_있으면_거부한다() throws Exception {
        // 자격증명을 담은 파일이므로 XXE 를 차단한다
        final File file = write("""
                <!DOCTYPE authorizers [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
                <authorizers/>
                """);
        assertThrows(CliException.class, () -> AuthorizersXml.readUserGroupProvider(file, null));
    }
}
