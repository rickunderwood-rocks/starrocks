// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.qe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.thrift.TWorkGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * CelerData Query SLA Manager
 *
 * Provides enterprise-grade SLA enforcement for query performance guarantees.
 * This is a critical feature for enterprise sales as customers need contractual
 * performance guarantees (e.g., "99th percentile query latency < 2 seconds").
 *
 * Key Features:
 * - Percentile-based latency tracking (P50, P95, P99)
 * - Per-resource-group SLA definitions
 * - Real-time SLA violation detection and alerting
 * - Query prioritization based on SLA risk
 * - Historical SLA compliance reporting
 *
 * Usage:
 *   // Define SLA for a resource group
 *   QuerySLAManager.getInstance().defineSLA("analytics_team",
 *       new SLADefinition(2000, 5000, 10000)); // P50=2s, P95=5s, P99=10s
 *
 *   // Record query completion
 *   QuerySLAManager.getInstance().recordQueryLatency("analytics_team", 1500);
 *
 *   // Check SLA compliance
 *   SLAStatus status = QuerySLAManager.getInstance().getSLAStatus("analytics_team");
 */
public class QuerySLAManager {
    private static final Logger LOG = LogManager.getLogger(QuerySLAManager.class);

    // Singleton instance
    private static volatile QuerySLAManager INSTANCE;

    // SLA definitions per resource group
    private final Map<String, SLADefinition> slaDefinitions = new ConcurrentHashMap<>();

    // Latency samples for percentile calculation (circular buffer per group)
    private final Map<String, LatencyTracker> latencyTrackers = new ConcurrentHashMap<>();

    // SLA violation counters
    private final Map<String, SLAViolationCounter> violationCounters = new ConcurrentHashMap<>();

    // Listeners for SLA violations (for alerting)
    private final List<SLAViolationListener> violationListeners = Collections.synchronizedList(new ArrayList<>());

    // Background thread for SLA reporting
    private final ScheduledExecutorService scheduler;

    // Global statistics
    private final AtomicLong totalQueries = new AtomicLong(0);
    private final AtomicLong totalViolations = new AtomicLong(0);

    private QuerySLAManager() {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "QuerySLAManager-Reporter");
            t.setDaemon(true);
            return t;
        });

        // Schedule periodic SLA status reporting
        if (Config.query_sla_reporting_enabled) {
            scheduler.scheduleAtFixedRate(
                this::reportSLAStatus,
                Config.query_sla_reporting_interval_seconds,
                Config.query_sla_reporting_interval_seconds,
                TimeUnit.SECONDS
            );
        }

        LOG.info("CelerData QuerySLAManager initialized");
    }

    public static QuerySLAManager getInstance() {
        if (INSTANCE == null) {
            synchronized (QuerySLAManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new QuerySLAManager();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Define SLA targets for a resource group
     */
    public void defineSLA(String resourceGroup, SLADefinition definition) {
        slaDefinitions.put(resourceGroup, definition);
        latencyTrackers.putIfAbsent(resourceGroup, new LatencyTracker(
            Config.query_sla_sample_window_size));
        violationCounters.putIfAbsent(resourceGroup, new SLAViolationCounter());
        LOG.info("CelerData SLA defined for resource group '{}': P50={}ms, P95={}ms, P99={}ms",
            resourceGroup, definition.p50TargetMs, definition.p95TargetMs, definition.p99TargetMs);
    }

    /**
     * Record query latency and check for SLA violations
     */
    public void recordQueryLatency(String resourceGroup, long latencyMs) {
        totalQueries.incrementAndGet();

        LatencyTracker tracker = latencyTrackers.get(resourceGroup);
        if (tracker != null) {
            tracker.record(latencyMs);
        }

        SLADefinition sla = slaDefinitions.get(resourceGroup);
        if (sla != null) {
            checkAndRecordViolation(resourceGroup, sla, latencyMs);
        }
    }

    /**
     * Record query latency using TWorkGroup
     */
    public void recordQueryLatency(TWorkGroup workGroup, long latencyMs) {
        if (workGroup != null && workGroup.getName() != null) {
            recordQueryLatency(workGroup.getName(), latencyMs);
        }
    }

    private void checkAndRecordViolation(String resourceGroup, SLADefinition sla, long latencyMs) {
        SLAViolationType violationType = null;

        if (latencyMs > sla.p99TargetMs) {
            violationType = SLAViolationType.P99_EXCEEDED;
        } else if (latencyMs > sla.p95TargetMs) {
            violationType = SLAViolationType.P95_EXCEEDED;
        } else if (latencyMs > sla.p50TargetMs) {
            violationType = SLAViolationType.P50_EXCEEDED;
        }

        if (violationType != null) {
            totalViolations.incrementAndGet();

            SLAViolationCounter counter = violationCounters.get(resourceGroup);
            if (counter != null) {
                counter.record(violationType);
            }

            // Notify listeners
            SLAViolationEvent event = new SLAViolationEvent(
                resourceGroup, violationType, latencyMs, sla);
            notifyViolationListeners(event);

            if (violationType == SLAViolationType.P99_EXCEEDED) {
                LOG.warn("CelerData SLA VIOLATION [P99]: resource_group={}, latency={}ms, target={}ms",
                    resourceGroup, latencyMs, sla.p99TargetMs);
            }
        }
    }

    /**
     * Get current SLA status for a resource group
     */
    public SLAStatus getSLAStatus(String resourceGroup) {
        LatencyTracker tracker = latencyTrackers.get(resourceGroup);
        SLADefinition sla = slaDefinitions.get(resourceGroup);
        SLAViolationCounter counter = violationCounters.get(resourceGroup);

        if (tracker == null || sla == null) {
            return null;
        }

        LatencyPercentiles percentiles = tracker.getPercentiles();

        return new SLAStatus(
            resourceGroup,
            percentiles,
            sla,
            counter != null ? counter.getSnapshot() : new ViolationSnapshot(0, 0, 0),
            tracker.getSampleCount()
        );
    }

    /**
     * Get query priority boost based on SLA risk
     * Returns a priority multiplier (1.0 = normal, >1.0 = higher priority)
     */
    public double getSLAPriorityBoost(String resourceGroup) {
        SLAStatus status = getSLAStatus(resourceGroup);
        if (status == null) {
            return 1.0;
        }

        // Boost priority if we're approaching SLA violations
        double p99Ratio = (double) status.currentPercentiles.p99 / status.slaDefinition.p99TargetMs;

        if (p99Ratio > 0.9) {
            // Critical: P99 is at 90%+ of target, boost significantly
            return 2.0;
        } else if (p99Ratio > 0.75) {
            // Warning: P99 is at 75%+ of target, moderate boost
            return 1.5;
        } else if (p99Ratio > 0.5) {
            // Caution: P99 is at 50%+ of target, slight boost
            return 1.2;
        }

        return 1.0;
    }

    /**
     * Check if a resource group is at risk of SLA violation
     */
    public boolean isAtSLARisk(String resourceGroup) {
        return getSLAPriorityBoost(resourceGroup) > 1.0;
    }

    /**
     * Get overall SLA compliance percentage
     */
    public double getOverallCompliance() {
        long total = totalQueries.get();
        long violations = totalViolations.get();

        if (total == 0) {
            return 100.0;
        }

        return ((double) (total - violations) / total) * 100.0;
    }

    /**
     * Register a listener for SLA violations
     */
    public void addViolationListener(SLAViolationListener listener) {
        violationListeners.add(listener);
    }

    public void removeViolationListener(SLAViolationListener listener) {
        violationListeners.remove(listener);
    }

    private void notifyViolationListeners(SLAViolationEvent event) {
        for (SLAViolationListener listener : violationListeners) {
            try {
                listener.onViolation(event);
            } catch (Exception e) {
                LOG.warn("Error notifying SLA violation listener", e);
            }
        }
    }

    private void reportSLAStatus() {
        try {
            for (String group : slaDefinitions.keySet()) {
                SLAStatus status = getSLAStatus(group);
                if (status != null) {
                    LOG.info("CelerData SLA Report [{}]: P50={}ms (target={}ms), P95={}ms (target={}ms), " +
                            "P99={}ms (target={}ms), samples={}, compliance={:.2f}%",
                        group,
                        status.currentPercentiles.p50, status.slaDefinition.p50TargetMs,
                        status.currentPercentiles.p95, status.slaDefinition.p95TargetMs,
                        status.currentPercentiles.p99, status.slaDefinition.p99TargetMs,
                        status.sampleCount,
                        status.getCompliancePercentage());
                }
            }
        } catch (Exception e) {
            LOG.warn("Error reporting SLA status", e);
        }
    }

    @VisibleForTesting
    public void reset() {
        slaDefinitions.clear();
        latencyTrackers.clear();
        violationCounters.clear();
        totalQueries.set(0);
        totalViolations.set(0);
    }

    // ==================== Inner Classes ====================

    /**
     * SLA Definition with percentile targets
     */
    public static class SLADefinition {
        public final long p50TargetMs;
        public final long p95TargetMs;
        public final long p99TargetMs;

        public SLADefinition(long p50TargetMs, long p95TargetMs, long p99TargetMs) {
            this.p50TargetMs = p50TargetMs;
            this.p95TargetMs = p95TargetMs;
            this.p99TargetMs = p99TargetMs;
        }
    }

    /**
     * Current latency percentiles
     */
    public static class LatencyPercentiles {
        public final long p50;
        public final long p95;
        public final long p99;
        public final long max;
        public final long min;
        public final double avg;

        public LatencyPercentiles(long p50, long p95, long p99, long max, long min, double avg) {
            this.p50 = p50;
            this.p95 = p95;
            this.p99 = p99;
            this.max = max;
            this.min = min;
            this.avg = avg;
        }
    }

    /**
     * SLA Status summary
     */
    public static class SLAStatus {
        public final String resourceGroup;
        public final LatencyPercentiles currentPercentiles;
        public final SLADefinition slaDefinition;
        public final ViolationSnapshot violations;
        public final long sampleCount;

        public SLAStatus(String resourceGroup, LatencyPercentiles currentPercentiles,
                        SLADefinition slaDefinition, ViolationSnapshot violations, long sampleCount) {
            this.resourceGroup = resourceGroup;
            this.currentPercentiles = currentPercentiles;
            this.slaDefinition = slaDefinition;
            this.violations = violations;
            this.sampleCount = sampleCount;
        }

        public double getCompliancePercentage() {
            // Fix: Count only the highest violated percentile, not sum all violations
            // A query that exceeds P99 should not be counted as P99 + P95 + P50
            long highestViolationCount = Math.max(violations.p99Violations,
                    Math.max(violations.p95Violations, violations.p50Violations));

            if (sampleCount == 0 && highestViolationCount == 0) {
                return 100.0;
            }

            long totalQueries = sampleCount + highestViolationCount;
            if (totalQueries == 0) return 100.0;

            return ((double) (sampleCount) / totalQueries) * 100.0;
        }

        public boolean isP50Compliant() {
            return currentPercentiles.p50 <= slaDefinition.p50TargetMs;
        }

        public boolean isP95Compliant() {
            return currentPercentiles.p95 <= slaDefinition.p95TargetMs;
        }

        public boolean isP99Compliant() {
            return currentPercentiles.p99 <= slaDefinition.p99TargetMs;
        }

        public boolean isFullyCompliant() {
            return isP50Compliant() && isP95Compliant() && isP99Compliant();
        }
    }

    public enum SLAViolationType {
        P50_EXCEEDED,
        P95_EXCEEDED,
        P99_EXCEEDED
    }

    /**
     * SLA Violation Event for listeners
     */
    public static class SLAViolationEvent {
        public final String resourceGroup;
        public final SLAViolationType type;
        public final long actualLatencyMs;
        public final SLADefinition slaDefinition;
        public final long timestamp;

        public SLAViolationEvent(String resourceGroup, SLAViolationType type,
                                long actualLatencyMs, SLADefinition slaDefinition) {
            this.resourceGroup = resourceGroup;
            this.type = type;
            this.actualLatencyMs = actualLatencyMs;
            this.slaDefinition = slaDefinition;
            this.timestamp = System.currentTimeMillis();
        }
    }

    /**
     * Interface for SLA violation listeners
     */
    public interface SLAViolationListener {
        void onViolation(SLAViolationEvent event);
    }

    /**
     * Violation counter snapshot
     */
    public static class ViolationSnapshot {
        public final long p50Violations;
        public final long p95Violations;
        public final long p99Violations;

        public ViolationSnapshot(long p50, long p95, long p99) {
            this.p50Violations = p50;
            this.p95Violations = p95;
            this.p99Violations = p99;
        }
    }

    /**
     * Thread-safe latency tracker with circular buffer
     */
    private static class LatencyTracker {
        private final long[] samples;
        private final int maxSamples;
        private int writeIndex = 0;
        private int sampleCount = 0;
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        public LatencyTracker(int maxSamples) {
            this.maxSamples = maxSamples;
            this.samples = new long[maxSamples];
        }

        public void record(long latencyMs) {
            lock.writeLock().lock();
            try {
                samples[writeIndex] = latencyMs;
                writeIndex = (writeIndex + 1) % maxSamples;
                if (sampleCount < maxSamples) {
                    sampleCount++;
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        public int getSampleCount() {
            lock.readLock().lock();
            try {
                return sampleCount;
            } finally {
                lock.readLock().unlock();
            }
        }

        public LatencyPercentiles getPercentiles() {
            lock.readLock().lock();
            try {
                if (sampleCount == 0) {
                    return new LatencyPercentiles(0, 0, 0, 0, 0, 0);
                }

                // Copy samples for sorting
                long[] sorted = new long[sampleCount];
                System.arraycopy(samples, 0, sorted, 0, sampleCount);
                java.util.Arrays.sort(sorted);

                long sum = 0;
                for (int i = 0; i < sampleCount; i++) {
                    sum += sorted[i];
                }

                // Fix: Ensure consistent percentile calculation with proper bounds checking
                int p50Index = Math.min((int) (sampleCount * 0.50) - 1, sampleCount - 1);
                int p95Index = Math.min((int) (sampleCount * 0.95) - 1, sampleCount - 1);
                int p99Index = Math.min((int) (sampleCount * 0.99) - 1, sampleCount - 1);

                p50Index = Math.max(0, p50Index);
                p95Index = Math.max(0, p95Index);
                p99Index = Math.max(0, p99Index);

                return new LatencyPercentiles(
                    sorted[p50Index],
                    sorted[p95Index],
                    sorted[p99Index],
                    sorted[sampleCount - 1],
                    sorted[0],
                    (double) sum / sampleCount
                );
            } finally {
                lock.readLock().unlock();
            }
        }
    }

    /**
     * Thread-safe violation counter
     */
    private static class SLAViolationCounter {
        private final AtomicLong p50Violations = new AtomicLong(0);
        private final AtomicLong p95Violations = new AtomicLong(0);
        private final AtomicLong p99Violations = new AtomicLong(0);

        public void record(SLAViolationType type) {
            switch (type) {
                case P50_EXCEEDED:
                    p50Violations.incrementAndGet();
                    break;
                case P95_EXCEEDED:
                    p95Violations.incrementAndGet();
                    break;
                case P99_EXCEEDED:
                    p99Violations.incrementAndGet();
                    break;
            }
        }

        public ViolationSnapshot getSnapshot() {
            return new ViolationSnapshot(
                p50Violations.get(),
                p95Violations.get(),
                p99Violations.get()
            );
        }
    }
}
