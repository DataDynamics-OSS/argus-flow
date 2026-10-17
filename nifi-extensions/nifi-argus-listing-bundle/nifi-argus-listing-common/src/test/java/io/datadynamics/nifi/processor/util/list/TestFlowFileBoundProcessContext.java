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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class TestFlowFileBoundProcessContext {

    private static final PropertyDescriptor PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("Per FlowFile")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final PropertyDescriptor ENVIRONMENT = new PropertyDescriptor.Builder()
            .name("Environment")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
                return List.of(PER_FLOWFILE, ENVIRONMENT);
            }

            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) {
            }
        });
        runner.setProperty(PER_FLOWFILE, "${host}:${port}");
        runner.setProperty(ENVIRONMENT, "static");
    }

    @Test
    void evaluatesFlowFileScopedPropertiesWithBoundAttributes() {
        final ProcessContext bound = FlowFileBoundProcessContext.bind(runner.getProcessContext(), Map.of("host", "h1", "port", "22"));

        assertEquals("h1:22", bound.getProperty(PER_FLOWFILE).evaluateAttributeExpressions().getValue());
        assertEquals("h1:22", bound.getProperty(PER_FLOWFILE.getName()).evaluateAttributeExpressions().getValue());
        assertEquals("h1:22", bound.getProperty(PER_FLOWFILE).evaluateAttributeExpressions((FlowFile) null).getValue());
        assertEquals("static", bound.getProperty(ENVIRONMENT).evaluateAttributeExpressions().getValue());
    }

    @Test
    void explicitAttributesAreOverlaidOnBoundAttributes() {
        final ProcessContext bound = FlowFileBoundProcessContext.bind(runner.getProcessContext(), Map.of("host", "h1", "port", "22"));

        // 업스트림 전송 계층은 FlowFile 이 없으면 빈 맵을 넘긴다 — 묶인 속성이 사라지면 안 된다
        assertEquals("h1:22", bound.getProperty(PER_FLOWFILE).evaluateAttributeExpressions(Map.of()).getValue());
        assertEquals("h1:2222", bound.getProperty(PER_FLOWFILE).evaluateAttributeExpressions(Map.of("port", "2222")).getValue());
    }

    @Test
    void rebindingUnwrapsThePreviousBinding() {
        final ProcessContext original = runner.getProcessContext();
        final FlowFileBoundProcessContext first = FlowFileBoundProcessContext.bind(original, Map.of("host", "a", "port", "1"));
        final FlowFileBoundProcessContext second = FlowFileBoundProcessContext.bind(first, Map.of("host", "b", "port", "2"));

        assertEquals("b:2", second.getProperty(PER_FLOWFILE).evaluateAttributeExpressions().getValue());
        assertSame(original.getStateManager(), second.getStateManager());
    }
}
