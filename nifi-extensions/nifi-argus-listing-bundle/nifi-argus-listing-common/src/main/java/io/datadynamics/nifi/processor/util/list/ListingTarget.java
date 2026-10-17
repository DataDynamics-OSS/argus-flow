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
package io.datadynamics.nifi.processor.util.list;

import org.apache.nifi.components.PropertyDependency;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.ProcessContext;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Set;

/**
 * List 대상 하나 (예: {@code Remote Directory [/in] on [sftp.example.com:22]}).
 *
 * <p>추적 상태는 대상별로 분리해 저장하고, {@link #key()} 가 그 키다. 키는 대상 설명과 함께 결과에 영향을 주는
 * 설정(필터·재귀·파일 나이·크기 등)의 값으로 만든다. 그래서 같은 경로라도 그런 설정이 바뀌면 새 대상이 되어 처음부터
 * 다시 List 한다 — 업스트림이 해당 속성 변경 시 상태를 지우는 것과 같은 효과다. 필터를 넓혔는데 이전 상태 때문에
 * 오래된 파일이 영영 나오지 않는 일을 막는다.</p>
 *
 * @param description   사람이 읽을 수 있는 대상 설명 — FlowFile 속성 {@code argus.list.target} 으로도 기록된다
 * @param configuration 결과에 영향을 주는 설정의 정규화된 값 (키에만 쓴다)
 */
public record ListingTarget(String description, String configuration) {

    public ListingTarget {
        Objects.requireNonNull(description, "description");
        Objects.requireNonNull(configuration, "configuration");
    }

    public ListingTarget(final String description) {
        this(description, "");
    }

    /**
     * @param context    입력 FlowFile 에 묶인 컨텍스트 — FlowFile 속성을 쓰는 필터는 FlowFile 마다 평가된 값이 키에 들어간다
     * @param properties 결과에 영향을 주는 설정
     */
    public static ListingTarget of(final String description, final ProcessContext context, final Collection<PropertyDescriptor> properties) {
        final StringBuilder configuration = new StringBuilder();
        properties.stream()
                .filter(property -> isDependencySatisfied(context, property))
                .sorted(Comparator.comparing(PropertyDescriptor::getName))
                .forEach(property -> {
                    final PropertyValue value = context.getProperty(property);
                    final String evaluated = value == null ? null
                            : property.isExpressionLanguageSupported() ? value.evaluateAttributeExpressions().getValue() : value.getValue();
                    configuration.append(property.getName()).append('=').append(evaluated).append('\n');
                });
        return new ListingTarget(description, configuration.toString());
    }

    /**
     * 의존 조건이 맞지 않는 속성은 사용자에게 보이지 않으므로 값을 읽지 않는다 (NIFI-14400).
     */
    private static boolean isDependencySatisfied(final ProcessContext context, final PropertyDescriptor property) {
        for (final PropertyDependency dependency : property.getDependencies()) {
            final PropertyValue dependencyValue = context.getProperty(dependency.getPropertyName());
            final String value = dependencyValue == null ? null : dependencyValue.getValue();
            if (value == null) {
                return false;
            }
            final Set<String> dependentValues = dependency.getDependentValues();
            if (dependentValues != null && !dependentValues.contains(value)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return 대상 설명과 설정의 SHA-256 앞 16자리(hex)
     */
    public String key() {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(description.getBytes(StandardCharsets.UTF_8));
            if (!configuration.isEmpty()) {
                digest.update((byte) 0);
                digest.update(configuration.getBytes(StandardCharsets.UTF_8));
            }
            return HexFormat.of().formatHex(digest.digest(), 0, 8);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    @Override
    public String toString() {
        return description;
    }
}
