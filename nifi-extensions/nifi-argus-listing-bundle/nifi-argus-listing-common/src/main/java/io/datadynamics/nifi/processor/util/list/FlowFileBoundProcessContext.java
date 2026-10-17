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
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.scheduling.ExecutionNode;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 입력 FlowFile 에 묶인 ProcessContext.
 *
 * <p>{@code FLOWFILE_ATTRIBUTES} 범위로 선언된 속성의 {@code evaluateAttributeExpressions()} 를 묶인
 * FlowFile 속성으로 평가한다. 포크한 업스트림 코드(전송 계층 SFTPTransfer/FTPTransfer, ListS3 의
 * 버킷 리스터, ListHDFS 의 경로 해석 등)가 ProcessContext/PropertyContext 만 받아 속성을 평가하므로,
 * 이 컨텍스트를 넘기는 것만으로 FlowFile 기준 List 가 된다 — 업스트림 코드와의 diff 를 최소로 유지하는 장치.</p>
 *
 * <p>그 밖의 범위(ENVIRONMENT, NONE)의 속성과 나머지 메서드는 원래 컨텍스트에 그대로 위임한다.</p>
 */
public final class FlowFileBoundProcessContext implements ProcessContext {

    private final ProcessContext delegate;
    private final Map<String, String> attributes;
    private final Set<String> flowFileScopedProperties;

    private FlowFileBoundProcessContext(final ProcessContext delegate, final Map<String, String> attributes) {
        this.delegate = delegate;
        this.attributes = Map.copyOf(attributes);
        this.flowFileScopedProperties = delegate.getProperties().keySet().stream()
                .filter(descriptor -> descriptor.getExpressionLanguageScope() == ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .map(PropertyDescriptor::getName)
                .collect(Collectors.toUnmodifiableSet());
    }

    public static FlowFileBoundProcessContext bind(final ProcessContext context, final FlowFile flowFile) {
        return bind(context, flowFile.getAttributes());
    }

    public static FlowFileBoundProcessContext bind(final ProcessContext context, final Map<String, String> attributes) {
        final ProcessContext unwrapped = context instanceof FlowFileBoundProcessContext bound ? bound.delegate : context;
        return new FlowFileBoundProcessContext(unwrapped, attributes == null ? Map.of() : attributes);
    }

    /**
     * @return 이 컨텍스트가 묶인 FlowFile 속성
     */
    public Map<String, String> getAttributes() {
        return attributes;
    }

    private PropertyValue bindValue(final String propertyName, final PropertyValue value) {
        if (value == null || !flowFileScopedProperties.contains(propertyName)) {
            return value;
        }
        return new FlowFileBoundPropertyValue(value, attributes);
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor descriptor) {
        return bindValue(descriptor.getName(), delegate.getProperty(descriptor));
    }

    @Override
    public PropertyValue getProperty(final String propertyName) {
        return bindValue(propertyName, delegate.getProperty(propertyName));
    }

    @Override
    public Map<String, String> getAllProperties() {
        return delegate.getAllProperties();
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return delegate.newPropertyValue(rawValue);
    }

    @Override
    public void yield() {
        delegate.yield();
    }

    @Override
    public int getMaxConcurrentTasks() {
        return delegate.getMaxConcurrentTasks();
    }

    @Override
    public ExecutionNode getExecutionNode() {
        return delegate.getExecutionNode();
    }

    @Override
    public String getAnnotationData() {
        return delegate.getAnnotationData();
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return delegate.getProperties();
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return delegate.getControllerServiceLookup();
    }

    @Override
    public Set<Relationship> getAvailableRelationships() {
        return delegate.getAvailableRelationships();
    }

    @Override
    public boolean isAutoTerminated(final Relationship relationship) {
        return delegate.isAutoTerminated(relationship);
    }

    @Override
    public boolean hasIncomingConnection() {
        return delegate.hasIncomingConnection();
    }

    @Override
    public boolean hasNonLoopConnection() {
        return delegate.hasNonLoopConnection();
    }

    @Override
    public boolean hasConnection(final Relationship relationship) {
        return delegate.hasConnection(relationship);
    }

    @Override
    public boolean isExpressionLanguagePresent(final PropertyDescriptor property) {
        return delegate.isExpressionLanguagePresent(property);
    }

    @Override
    public StateManager getStateManager() {
        return delegate.getStateManager();
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public boolean isRelationshipRetried(final Relationship relationship) {
        return delegate.isRelationshipRetried(relationship);
    }

    @Override
    public int getRetryCount() {
        return delegate.getRetryCount();
    }

    @Override
    public boolean isConnectedToCluster() {
        return delegate.isConnectedToCluster();
    }
}
