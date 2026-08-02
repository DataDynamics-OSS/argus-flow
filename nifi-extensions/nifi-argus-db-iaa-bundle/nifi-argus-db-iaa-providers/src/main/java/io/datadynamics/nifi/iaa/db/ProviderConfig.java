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
package io.datadynamics.nifi.iaa.db;

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 프로바이더 설정값 읽기.
 *
 * <p>값에 {@code ${VAR}} 가 있으면 환경변수로 치환한다. NiFi 는 login-identity-providers.xml
 * 과 authorizers.xml 에 민감 속성 암호화를 지원하지 않으므로(nifi.sensitive.props.key 는
 * 플로우 전용), DB 비밀번호를 XML 에 평문으로 남기지 않는 유일한 수단이다.
 */
public final class ProviderConfig {

    private static final Pattern ENV_REF = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)}");
    private static final Pattern DURATION = Pattern.compile("(\\d+)\\s*([a-zA-Z]+)");

    private final Map<String, String> properties;
    private final UnaryOperator<String> environment;

    public ProviderConfig(final Map<String, String> properties) {
        this(properties, System::getenv);
    }

    /** 테스트에서 환경변수 조회를 대체하기 위한 생성자. */
    ProviderConfig(final Map<String, String> properties, final UnaryOperator<String> environment) {
        this.properties = properties;
        this.environment = environment;
    }

    /** 값이 없거나 비어 있으면 {@code defaultValue}. 환경변수 참조는 치환된다. */
    public String getString(final String name, final String defaultValue) {
        final String raw = properties.get(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return expand(raw.trim());
    }

    /** 필수 값. 없으면 예외. */
    public String getRequired(final String name) {
        final String value = getString(name, null);
        if (value == null) {
            throw new IllegalArgumentException("필수 설정이 비어 있습니다: " + name);
        }
        return value;
    }

    public int getInt(final String name, final int defaultValue) {
        final String value = getString(name, null);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("정수가 아닙니다: " + name + "=" + value, e);
        }
    }

    public boolean getBoolean(final String name, final boolean defaultValue) {
        final String value = getString(name, null);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    /**
     * NiFi 표기의 기간을 읽는다 — {@code "12 hours"}, {@code "15 mins"}, {@code "30 secs"}.
     *
     * <p>NiFi 의 FormatUtils 를 쓰지 않는 이유: nifi-utils 를 끌어오면 인증 프로바이더 NAR 에
     * 불필요한 전이 의존성이 붙는다. 여기서 필요한 것은 이 표기의 부분집합뿐이다.
     */
    public Duration getDuration(final String name, final Duration defaultValue) {
        final String value = getString(name, null);
        if (value == null) {
            return defaultValue;
        }
        final Matcher m = DURATION.matcher(value.trim());
        if (!m.matches()) {
            throw new IllegalArgumentException(
                    "기간 표기가 올바르지 않습니다: " + name + "=" + value + " (예: \"12 hours\")");
        }
        final long amount = Long.parseLong(m.group(1));
        final String unit = m.group(2).toLowerCase(Locale.ROOT);
        return switch (unit) {
            case "ms", "milli", "millis", "millisecond", "milliseconds" -> Duration.ofMillis(amount);
            case "s", "sec", "secs", "second", "seconds" -> Duration.ofSeconds(amount);
            case "m", "min", "mins", "minute", "minutes" -> Duration.ofMinutes(amount);
            case "h", "hr", "hrs", "hour", "hours" -> Duration.ofHours(amount);
            case "d", "day", "days" -> Duration.ofDays(amount);
            default -> throw new IllegalArgumentException(
                    "알 수 없는 시간 단위입니다: " + name + "=" + value);
        };
    }

    private String expand(final String value) {
        final Matcher m = ENV_REF.matcher(value);
        final StringBuilder out = new StringBuilder();
        while (m.find()) {
            final String var = m.group(1);
            final String resolved = environment.apply(var);
            if (resolved == null) {
                throw new IllegalArgumentException("환경변수가 설정되지 않았습니다: " + var);
            }
            m.appendReplacement(out, Matcher.quoteReplacement(resolved));
        }
        m.appendTail(out);
        return out.toString();
    }
}
