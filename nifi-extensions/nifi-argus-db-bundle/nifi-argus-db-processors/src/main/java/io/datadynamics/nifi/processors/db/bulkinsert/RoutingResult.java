/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-extension-utils/nifi-put-pattern/src/main/java/org/apache/nifi/processor/util/pattern/RoutingResult.java
 */
package io.datadynamics.nifi.processors.db.bulkinsert;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 하나의 처리(onTrigger 등) 도중 어떤 FlowFile이 어떤 관계(Relationship)로 라우팅되어야 하는지를
 * 누적하여 기록하는 결과 객체. {@link ExceptionHandler}, {@link PartialFunctions}, {@link RollbackOnFailure}가
 * 실제 session.transfer() 호출을 나중으로 미루고, 그 전에 라우팅 결과를 검토/보정할 수 있도록 하기 위해 사용한다.
 */
public class RoutingResult {

    // 관계별로 라우팅될 FlowFile 목록을 누적하는 맵
    private final Map<Relationship, List<FlowFile>> routedFlowFiles = new HashMap<>();

    /**
     * FlowFile 1개를 지정한 관계로 라우팅하도록 기록한다.
     */
    public void routeTo(final FlowFile flowFile, final Relationship relationship) {
        routedFlowFiles.computeIfAbsent(relationship, r -> new ArrayList<>()).add(flowFile);
    }

    /**
     * 여러 FlowFile을 지정한 관계로 라우팅하도록 기록한다.
     */
    public void routeTo(final List<FlowFile> flowFiles, final Relationship relationship) {
        routedFlowFiles.computeIfAbsent(relationship, r -> new ArrayList<>()).addAll(flowFiles);
    }

    /**
     * 다른 RoutingResult에 기록된 라우팅 내용을 이 결과에 합친다. (예: 그룹 단위 처리 결과를 전체 결과에 반영할 때 사용)
     */
    public void merge(final RoutingResult r) {
        r.getRoutedFlowFiles().forEach((relationship, routedFlowFiles) -> routeTo(routedFlowFiles, relationship));
    }

    public Map<Relationship, List<FlowFile>> getRoutedFlowFiles() {
        return routedFlowFiles;
    }

    /**
     * 지정한 관계로 라우팅된 FlowFile이 하나라도 있는지 확인한다. (예: RollbackOnFailure가 failure 관계 존재 여부를 검사할 때 사용)
     */
    public boolean contains(Relationship relationship) {
        return routedFlowFiles.containsKey(relationship) && !routedFlowFiles.get(relationship).isEmpty();
    }
}