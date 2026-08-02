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

import com.zaxxer.hikari.HikariDataSource;
import io.datadynamics.nifi.iaa.db.DataSourceFactory;
import io.datadynamics.nifi.iaa.db.Dialect;
import io.datadynamics.nifi.iaa.db.ProviderConfig;
import io.datadynamics.nifi.iaa.db.dao.GroupRecord;
import io.datadynamics.nifi.iaa.db.dao.UserRecord;
import java.io.File;
import java.io.PrintStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code bin/argus-user.sh} 의 진입점.
 *
 * <p>접속 정보는 {@code conf/authorizers.xml} 에서 읽는다 — JDBC URL 을 인자로 또 받으면
 * 설정이 두 곳에 생겨 어긋난다.
 *
 * <p>NiFi 가 떠 있는 상태에서 써도 된다. 프로바이더는 DB 를 조회하므로 변경이 반영된다
 * (인가 쪽은 Cache Duration 만큼 지연될 수 있다). 예외는 {@code schema-init} 으로, DDL 중
 * 프로바이더 조회가 실패할 수 있어 NiFi 정지 상태를 권한다.
 */
public final class ArgusUserCommand {

    private static final String USAGE = """
            사용법: argus-user.sh [전역옵션] <명령> [인자]

            전역옵션
              --conf <경로>        authorizers.xml 경로 (기본: $NIFI_HOME/conf/authorizers.xml)
              --provider <id>      userGroupProvider 의 identifier (기본: DbUserGroupProvider 자동 탐색)

            스키마
              schema-init          스키마를 적용한다. NiFi 정지 상태에서 실행할 것

            사용자
              list                 사용자 목록
              show <identity>      상세(그룹·잠금 상태)
              add <identity> [--no-password] [--disabled]
              passwd <identity>
              rename <identity> <새-identity>
              enable <identity>  |  disable <identity>
              unlock <identity>    실패 횟수와 잠금 해제
              delete <identity>

            그룹
              group-list
              group-add <이름>  |  group-delete <이름>
              group-member <이름> --add <identity> | --remove <identity>

            비밀번호는 인자로 받지 않는다(ps 노출·셸 히스토리). 대화형 프롬프트를 쓰거나
            자동화에는 --password-stdin 을 사용한다:
              echo "$PW" | argus-user.sh add alice --password-stdin
            """;

    private ArgusUserCommand() {
    }

    public static void main(final String[] args) {
        try {
            System.exit(run(args, System.out));
        } catch (final CliException e) {
            System.err.println("오류: " + e.getMessage());
            System.exit(1);
        } catch (final Exception e) {
            System.err.println("오류: " + e);
            System.exit(2);
        }
    }

    /** 테스트에서 종료 없이 호출하기 위한 진입점. */
    static int run(final String[] args, final PrintStream out) throws Exception {
        final List<String> rest = new ArrayList<>();
        String conf = null;
        String provider = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--conf" -> conf = requireValue(args, ++i, "--conf");
                case "--provider" -> provider = requireValue(args, ++i, "--provider");
                case "-h", "--help" -> {
                    out.print(USAGE);
                    return 0;
                }
                default -> rest.add(args[i]);
            }
        }
        if (rest.isEmpty()) {
            out.print(USAGE);
            return 1;
        }

        final File confFile = conf != null ? new File(conf) : defaultConf();
        final Map<String, String> properties =
                AuthorizersXml.readUserGroupProvider(confFile, provider);
        final ProviderConfig config = new ProviderConfig(properties);
        final Dialect dialect = Dialect.fromJdbcUrl(config.getRequired(DataSourceFactory.PROP_URL));

        try (HikariDataSource dataSource = DataSourceFactory.create(config, "cli")) {
            return dispatch(new UserService(dataSource, dialect), rest, out);
        }
    }

    private static int dispatch(final UserService service, final List<String> args,
                                final PrintStream out) throws Exception {
        final String command = args.get(0);
        final List<String> rest = args.subList(1, args.size());

        switch (command) {
            case "schema-init" -> {
                service.initSchema();
                out.println("스키마를 적용했습니다.");
            }
            case "list" -> printUsers(service.listUsers(), out);
            case "show" -> show(service, arg(rest, 0, "identity"), out);
            case "add" -> {
                final String identity = arg(rest, 0, "identity");
                final boolean noPassword = rest.contains("--no-password");
                final boolean disabled = rest.contains("--disabled");
                final char[] password = noPassword ? null : readPassword(rest);
                service.addUser(identity, password, !disabled);
                out.println("사용자를 추가했습니다: " + identity
                        + (noPassword ? " (비밀번호 없음 — 비밀번호 인증 불가)" : ""));
            }
            case "passwd" -> {
                final String identity = arg(rest, 0, "identity");
                service.setPassword(identity, readPassword(rest));
                out.println("비밀번호를 변경했습니다: " + identity);
            }
            case "rename" -> {
                service.rename(arg(rest, 0, "identity"), arg(rest, 1, "새-identity"));
                out.println("identity 를 변경했습니다. 접근 권한은 유지됩니다.");
            }
            case "enable", "disable" -> {
                final String identity = arg(rest, 0, "identity");
                service.setEnabled(identity, "enable".equals(command));
                out.println(("enable".equals(command) ? "활성화" : "비활성화") + "했습니다: " + identity);
            }
            case "unlock" -> {
                service.unlock(arg(rest, 0, "identity"));
                out.println("잠금을 해제했습니다: " + rest.get(0));
            }
            case "delete" -> {
                service.deleteUser(arg(rest, 0, "identity"));
                out.println("삭제했습니다: " + rest.get(0));
            }
            case "group-list" -> {
                for (final GroupRecord group : service.listGroups()) {
                    out.printf("%-30s 소속 %d명%n", group.name(), group.userIds().size());
                }
            }
            case "group-add" -> {
                service.addGroup(arg(rest, 0, "이름"));
                out.println("그룹을 추가했습니다: " + rest.get(0));
            }
            case "group-delete" -> {
                service.deleteGroup(arg(rest, 0, "이름"));
                out.println("그룹을 삭제했습니다: " + rest.get(0));
            }
            case "group-member" -> {
                final String group = arg(rest, 0, "이름");
                final int addAt = rest.indexOf("--add");
                final int removeAt = rest.indexOf("--remove");
                if (addAt < 0 && removeAt < 0) {
                    throw new CliException("--add 또는 --remove 와 identity 가 필요합니다.");
                }
                final boolean adding = addAt >= 0;
                final int at = adding ? addAt : removeAt;
                if (at + 1 >= rest.size()) {
                    throw new CliException("identity 가 필요합니다.");
                }
                service.setMembership(group, rest.get(at + 1), adding);
                out.println((adding ? "추가" : "제거") + "했습니다: " + rest.get(at + 1) + " → " + group);
            }
            default -> throw new CliException("알 수 없는 명령입니다: " + command
                    + " (--help 로 사용법을 확인하십시오)");
        }
        return 0;
    }

    private static void show(final UserService service, final String identity, final PrintStream out)
            throws Exception {
        final UserRecord user = service.requireUser(identity);
        final Map<String, String> fields = new LinkedHashMap<>();
        fields.put("identity", user.identity());
        fields.put("식별자", user.id());
        fields.put("활성", String.valueOf(user.enabled()));
        fields.put("비밀번호", user.passwordHash() == null ? "없음 (비밀번호 인증 불가)" : "설정됨");
        fields.put("연속 실패", String.valueOf(user.failedCount()));
        fields.put("잠금", user.isLockedAt(Instant.now())
                ? "잠김 (해제 " + user.lockedUntil() + ")" : "아님");
        fields.put("그룹", String.join(", ",
                service.groupsOf(user.id()).stream().map(GroupRecord::name).toList()));
        fields.forEach((k, v) -> out.printf("%-10s %s%n", k, v));
    }

    private static void printUsers(final List<UserRecord> users, final PrintStream out) {
        out.printf("%-30s %-6s %-10s %s%n", "IDENTITY", "활성", "비밀번호", "상태");
        final Instant now = Instant.now();
        for (final UserRecord user : users) {
            out.printf("%-30s %-6s %-10s %s%n",
                    user.identity(),
                    user.enabled() ? "예" : "아니오",
                    user.passwordHash() == null ? "없음" : "설정됨",
                    user.isLockedAt(now) ? "잠김" : "정상");
        }
    }

    private static char[] readPassword(final List<String> args) {
        return args.contains("--password-stdin")
                ? PasswordPrompt.readFromStdin()
                : PasswordPrompt.readInteractive();
    }

    private static String arg(final List<String> args, final int index, final String name) {
        if (index >= args.size() || args.get(index).startsWith("--")) {
            throw new CliException(name + " 이(가) 필요합니다. --help 로 사용법을 확인하십시오.");
        }
        return args.get(index);
    }

    private static String requireValue(final String[] args, final int index, final String option) {
        if (index >= args.length) {
            throw new CliException(option + " 에 값이 필요합니다.");
        }
        return args[index];
    }

    private static File defaultConf() {
        final String home = System.getProperty("nifi.home", System.getenv("NIFI_HOME"));
        if (home == null) {
            throw new CliException("NIFI_HOME 을 알 수 없습니다. --conf 로 authorizers.xml 경로를 "
                    + "지정하거나 bin/argus-user.sh 로 실행하십시오.");
        }
        return new File(home, "conf/authorizers.xml");
    }

    static String usage() {
        return USAGE;
    }
}
