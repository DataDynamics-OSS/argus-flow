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

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.exception.ProcessException;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 입력 FlowFile 의 속성에 묶인 PropertyValue.
 *
 * <p>업스트림 List 코드는 대상 속성을 {@code evaluateAttributeExpressions()} (FlowFile 없이) 로 평가한다.
 * 이 래퍼는 그 호출을 묶인 속성으로 평가해, 포크한 코드의 평가 지점을 하나하나 고치지 않고도
 * FlowFile 기준으로 동작하게 한다. 호출자가 FlowFile 을 직접 넘기면 그것으로 평가하고, 속성 맵을 넘기면 묶인 속성 위에
 * 덮어써 평가한다 — 업스트림 전송 계층은 FlowFile 이 없을 때 빈 맵을 넘기므로, 맵만으로 평가하면 묶인 속성이 사라진다.</p>
 */
final class FlowFileBoundPropertyValue implements PropertyValue {

    private final PropertyValue delegate;
    private final Map<String, String> attributes;

    FlowFileBoundPropertyValue(final PropertyValue delegate, final Map<String, String> attributes) {
        this.delegate = delegate;
        this.attributes = attributes;
    }

    @Override
    public PropertyValue evaluateAttributeExpressions() throws ProcessException {
        return delegate.evaluateAttributeExpressions(attributes);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final Map<String, String> attributes) throws ProcessException {
        return delegate.evaluateAttributeExpressions(merge(attributes));
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final Map<String, String> attributes, final AttributeValueDecorator decorator) throws ProcessException {
        return delegate.evaluateAttributeExpressions(merge(attributes), decorator);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile) throws ProcessException {
        return flowFile == null ? delegate.evaluateAttributeExpressions(attributes) : delegate.evaluateAttributeExpressions(flowFile);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile, final Map<String, String> additionalAttributes) throws ProcessException {
        return flowFile == null ? delegate.evaluateAttributeExpressions(merge(additionalAttributes))
                : delegate.evaluateAttributeExpressions(flowFile, additionalAttributes);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile, final Map<String, String> additionalAttributes,
                                                      final AttributeValueDecorator decorator) throws ProcessException {
        return flowFile == null ? delegate.evaluateAttributeExpressions(merge(additionalAttributes), decorator)
                : delegate.evaluateAttributeExpressions(flowFile, additionalAttributes, decorator);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile, final Map<String, String> additionalAttributes,
                                                      final AttributeValueDecorator decorator, final Map<String, String> stateValues) throws ProcessException {
        return flowFile == null ? delegate.evaluateAttributeExpressions(merge(additionalAttributes), decorator)
                : delegate.evaluateAttributeExpressions(flowFile, additionalAttributes, decorator, stateValues);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final AttributeValueDecorator decorator) throws ProcessException {
        return delegate.evaluateAttributeExpressions(attributes, decorator);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile, final AttributeValueDecorator decorator) throws ProcessException {
        return flowFile == null ? delegate.evaluateAttributeExpressions(attributes, decorator) : delegate.evaluateAttributeExpressions(flowFile, decorator);
    }

    private Map<String, String> merge(final Map<String, String> additionalAttributes) {
        if (additionalAttributes == null || additionalAttributes.isEmpty()) {
            return attributes;
        }
        final Map<String, String> merged = new HashMap<>(attributes);
        merged.putAll(additionalAttributes);
        return merged;
    }

    @Override
    public String getValue() {
        return delegate.getValue();
    }

    @Override
    public Integer asInteger() {
        return delegate.asInteger();
    }

    @Override
    public Long asLong() {
        return delegate.asLong();
    }

    @Override
    public Boolean asBoolean() {
        return delegate.asBoolean();
    }

    @Override
    public Float asFloat() {
        return delegate.asFloat();
    }

    @Override
    public Double asDouble() {
        return delegate.asDouble();
    }

    @Override
    public Long asTimePeriod(final TimeUnit timeUnit) {
        return delegate.asTimePeriod(timeUnit);
    }

    @Override
    public Duration asDuration() {
        return delegate.asDuration();
    }

    @Override
    public Double asDataSize(final DataUnit dataUnit) {
        return delegate.asDataSize(dataUnit);
    }

    @Override
    public ControllerService asControllerService() {
        return delegate.asControllerService();
    }

    @Override
    public <T extends ControllerService> T asControllerService(final Class<T> serviceType) throws IllegalArgumentException {
        return delegate.asControllerService(serviceType);
    }

    @Override
    public ResourceReference asResource() {
        return delegate.asResource();
    }

    @Override
    public ResourceReferences asResources() {
        return delegate.asResources();
    }

    @Override
    public <E extends Enum<E>> E asAllowableValue(final Class<E> enumType) throws IllegalArgumentException {
        return delegate.asAllowableValue(enumType);
    }

    @Override
    public boolean isSet() {
        return delegate.isSet();
    }

    @Override
    public boolean isExpressionLanguagePresent() {
        return delegate.isExpressionLanguagePresent();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
