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
package io.datadynamics.nifi.flowanalysis;

import static io.datadynamics.nifi.flowanalysis.RuleTestSupport.connection;
import static io.datadynamics.nifi.flowanalysis.RuleTestSupport.funnel;
import static io.datadynamics.nifi.flowanalysis.RuleTestSupport.listProcessor;
import static io.datadynamics.nifi.flowanalysis.RuleTestSupport.processor;
import static io.datadynamics.nifi.flowanalysis.RuleTestSupport.setOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.Collections;

import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flowanalysis.ComponentAnalysisResult;
import org.apache.nifi.flowanalysis.GroupAnalysisResult;
import org.junit.jupiter.api.Test;

import io.datadynamics.nifi.flowanalysis.RuleTestSupport.TestContext;

class FlowAnalysisRulesTest {

    private VersionedProcessGroup rootGroup() {
        final VersionedProcessGroup pg = new VersionedProcessGroup();
        pg.setGroupIdentifier(null); // 루트
        pg.setProcessors(Collections.emptySet());
        pg.setConnections(Collections.emptySet());
        pg.setFunnels(Collections.emptySet());
        return pg;
    }

    // ---- TimerThreadPoolCeilingRule ----
    @Test
    void threadPoolCeiling_violatesWhenAboveCoresTimesMultiplier() {
        final TimerThreadPoolCeilingRule rule = new TimerThreadPoolCeilingRule() {
            @Override
            protected int availableProcessors() {
                return 2; // 상한 = 2 * 4 = 8
            }
        };
        final TestContext ctx = new TestContext().maxTimerDrivenThreads(9);
        assertEquals(1, rule.analyzeProcessGroup(rootGroup(), ctx).size());
    }

    @Test
    void threadPoolCeiling_okAtLimit() {
        final TimerThreadPoolCeilingRule rule = new TimerThreadPoolCeilingRule() {
            @Override
            protected int availableProcessors() {
                return 2;
            }
        };
        final TestContext ctx = new TestContext().maxTimerDrivenThreads(8); // 상한과 동일 → OK
        assertTrue(rule.analyzeProcessGroup(rootGroup(), ctx).isEmpty());
    }

    @Test
    void threadPoolCeiling_skipsNonRootGroup() {
        final VersionedProcessGroup child = rootGroup();
        child.setGroupIdentifier("parent-id");
        final TimerThreadPoolCeilingRule rule = new TimerThreadPoolCeilingRule() {
            @Override
            protected int availableProcessors() {
                return 1;
            }
        };
        assertTrue(rule.analyzeProcessGroup(child, new TestContext().maxTimerDrivenThreads(100)).isEmpty());
    }

    // ---- ProcessorThreadShareLimitRule ----
    @Test
    void threadShare_violatesWhenProcessorTakesTooMuch() {
        final ProcessorThreadShareLimitRule rule = new ProcessorThreadShareLimitRule();
        final TestContext ctx = new TestContext().maxTimerDrivenThreads(10); // 50% = 5
        final Collection<ComponentAnalysisResult> r =
                rule.analyzeComponent(processor("p1", "com.x.PutBig", 6), ctx);
        assertEquals(1, r.size());
    }

    @Test
    void threadShare_okWithinShare() {
        final ProcessorThreadShareLimitRule rule = new ProcessorThreadShareLimitRule();
        final TestContext ctx = new TestContext().maxTimerDrivenThreads(10);
        assertTrue(rule.analyzeComponent(processor("p1", "com.x.PutBig", 5), ctx).isEmpty());
    }

    // ---- ListingScheduleGuardRule ----
    @Test
    void listingGuard_violatesForZeroTimerSchedule() {
        final ListingScheduleGuardRule rule = new ListingScheduleGuardRule();
        final Collection<ComponentAnalysisResult> r = rule.analyzeComponent(
                listProcessor("l1", "org.apache.nifi.processors.standard.ListFile", "TIMER_DRIVEN", "0 sec"),
                new TestContext());
        assertEquals(1, r.size());
    }

    @Test
    void listingGuard_okForNonZeroSchedule() {
        final ListingScheduleGuardRule rule = new ListingScheduleGuardRule();
        assertTrue(rule.analyzeComponent(
                listProcessor("l1", "org.apache.nifi.processors.standard.ListFile", "TIMER_DRIVEN", "1 min"),
                new TestContext()).isEmpty());
    }

    @Test
    void listingGuard_ignoresNonListingProcessor() {
        final ListingScheduleGuardRule rule = new ListingScheduleGuardRule();
        assertTrue(rule.analyzeComponent(
                listProcessor("g1", "org.apache.nifi.processors.standard.GenerateFlowFile", "TIMER_DRIVEN", "0 sec"),
                new TestContext()).isEmpty());
    }

    // ---- DeadEndFunnelRule ----
    @Test
    void deadEnd_violatesForFunnelWithIncomingNoOutgoing() {
        final VersionedProcessGroup pg = rootGroup();
        pg.setFunnels(setOf(funnel("f1")));
        pg.setConnections(setOf(connection("p1", "f1"))); // f1으로 유입만 있음
        final Collection<GroupAnalysisResult> r = new DeadEndFunnelRule().analyzeProcessGroup(pg, new TestContext());
        assertEquals(1, r.size());
    }

    @Test
    void deadEnd_okWhenFunnelHasOutgoing() {
        final VersionedProcessGroup pg = rootGroup();
        pg.setFunnels(setOf(funnel("f1")));
        pg.setConnections(setOf(connection("p1", "f1"), connection("f1", "p2"))); // 유입+유출
        assertTrue(new DeadEndFunnelRule().analyzeProcessGroup(pg, new TestContext()).isEmpty());
    }

    // ---- ProcessorConcurrencyCapRule ----
    @Test
    void concurrencyCap_violatesAboveCapForMatchingType() {
        final ProcessorConcurrencyCapRule rule = new ProcessorConcurrencyCapRule();
        final TestContext ctx = new TestContext()
                .set(ProcessorConcurrencyCapRule.TARGET_TYPE, ".*PutDatabaseRecord");
        final Collection<ComponentAnalysisResult> r =
                rule.analyzeComponent(processor("d1", "com.x.PutDatabaseRecord", 4), ctx);
        assertEquals(1, r.size());
    }

    @Test
    void concurrencyCap_ignoresNonMatchingType() {
        final ProcessorConcurrencyCapRule rule = new ProcessorConcurrencyCapRule();
        final TestContext ctx = new TestContext()
                .set(ProcessorConcurrencyCapRule.TARGET_TYPE, ".*PutDatabaseRecord");
        assertTrue(rule.analyzeComponent(processor("o1", "com.x.PutFile", 10), ctx).isEmpty());
    }

    // ---- IcebergSinkMergeRule ----
    @Test
    void icebergMerge_violatesWhenNoUpstreamMerge() {
        final VersionedProcessGroup pg = rootGroup();
        pg.setProcessors(setOf(
                processor("src", "com.x.GenerateFlowFile", 1),
                processor("sink", "io.datadynamics.nifi.processors.iceberg.PutIceberg", 1)));
        pg.setConnections(setOf(connection("src", "sink")));
        final Collection<GroupAnalysisResult> r = new IcebergSinkMergeRule().analyzeProcessGroup(pg, new TestContext());
        assertEquals(1, r.size());
    }

    @Test
    void icebergMerge_okWithUpstreamMergeWithinDistance() {
        final VersionedProcessGroup pg = rootGroup();
        pg.setProcessors(setOf(
                processor("src", "com.x.GenerateFlowFile", 1),
                processor("merge", "org.apache.nifi.processors.standard.MergeContent", 1),
                processor("sink", "io.datadynamics.nifi.processors.iceberg.PutIceberg", 1)));
        pg.setConnections(setOf(connection("src", "merge"), connection("merge", "sink")));
        assertTrue(new IcebergSinkMergeRule().analyzeProcessGroup(pg, new TestContext()).isEmpty());
    }

    @Test
    void icebergMerge_violatesWhenMergeBeyondMaxDistance() {
        final VersionedProcessGroup pg = rootGroup();
        // merge -> a -> b -> c -> sink : merge는 4홉 밖, 기본 max-distance=3
        pg.setProcessors(setOf(
                processor("merge", "org.apache.nifi.processors.standard.MergeContent", 1),
                processor("a", "com.x.A", 1),
                processor("b", "com.x.B", 1),
                processor("c", "com.x.C", 1),
                processor("sink", "io.datadynamics.nifi.processors.iceberg.PutIceberg", 1)));
        pg.setConnections(setOf(
                connection("merge", "a"), connection("a", "b"),
                connection("b", "c"), connection("c", "sink")));
        assertEquals(1, new IcebergSinkMergeRule().analyzeProcessGroup(pg, new TestContext()).size());
    }
}
