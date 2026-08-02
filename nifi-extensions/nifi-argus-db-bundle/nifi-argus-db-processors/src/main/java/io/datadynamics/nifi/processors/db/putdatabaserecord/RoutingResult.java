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
package io.datadynamics.nifi.processors.db.putdatabaserecord;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 처리 도중 각 FlowFile이 어느 관계(Relationship)로 라우팅되어야 하는지를 누적해서 기록하는 결과 객체.
 * onTrigger 실행 중에는 즉시 session.transfer()를 호출하지 않고 이 객체에 라우팅 결과를 모아두었다가,
 * 처리 로직이 끝난 뒤 한꺼번에 전송함으로써 부분 실패 시에도 일관된 라우팅 결정을 내릴 수 있게 한다.
 */
public class RoutingResult {

    private final Map<Relationship, List<FlowFile>> routedFlowFiles = new HashMap<>();

    /**
     * 단일 FlowFile을 지정한 관계로 라우팅하도록 기록한다.
     */
    public void routeTo(final FlowFile flowFile, final Relationship relationship) {
        routedFlowFiles.computeIfAbsent(relationship, r -> new ArrayList<>()).add(flowFile);
    }

    /**
     * 여러 FlowFile을 한 번에 지정한 관계로 라우팅하도록 기록한다.
     */
    public void routeTo(final List<FlowFile> flowFiles, final Relationship relationship) {
        routedFlowFiles.computeIfAbsent(relationship, r -> new ArrayList<>()).addAll(flowFiles);
    }

    /**
     * 다른 RoutingResult에 누적된 라우팅 정보를 현재 결과에 병합한다.
     * 배치 처리 등에서 여러 하위 결과를 하나로 합칠 때 사용한다.
     */
    public void merge(final RoutingResult r) {
        r.getRoutedFlowFiles().forEach((relationship, routedFlowFiles) -> routeTo(routedFlowFiles, relationship));
    }

    public Map<Relationship, List<FlowFile>> getRoutedFlowFiles() {
        return routedFlowFiles;
    }

    /**
     * 지정한 관계로 라우팅된 FlowFile이 하나라도 존재하는지 확인한다.
     * 예: failure 관계로 보내진 FlowFile이 있는지 검사하여 세션 롤백 여부를 판단할 때 사용.
     */
    public boolean contains(Relationship relationship) {
        return routedFlowFiles.containsKey(relationship) && !routedFlowFiles.get(relationship).isEmpty();
    }
}