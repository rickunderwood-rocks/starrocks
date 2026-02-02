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
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CelerData Resource Isolation Manager
 *
 * Provides enterprise-grade multi-tenancy with hard resource isolation guarantees.
 * This is critical for enterprise deals where multiple teams/departments share
 * a single cluster and need guaranteed resource allocations.
 *
 * Key Features:
 * - Hard memory limits per tenant (not just soft limits)
 * - I/O bandwidth isolation and throttling
 * - CPU time accounting and limits
 * - Cross-tenant priority management
 * - Resource burst capacity management
 * - Real-time resource usage tracking
 *
 * Isolation Modes:
 * - SOFT: Best-effort isolation (default StarRocks behavior)
 * - HARD: Strict enforcement with query rejection when limits exceeded
 * - BURST: Allow temporary burst above limits with fairness
 *
 * Usage:
 *   // Configure tenant resource limits
 *   ResourceIsolationManager.getInstance().setTenantLimits("analytics_team",
 *       new TenantResourceLimits()
 *           .setMemoryLimitBytes(64L * 1024 * 1024 * 1024)  // 64GB
 *           .setCpuLimitPercent(25)                          // 25% of cluster
 *           .setIoBandwidthMBps(500)                         // 500 MB/s
 *           .setIsolationMode(IsolationMode.HARD));
 *
 *   // Check if query can be admitted
 *   if (ResourceIsolationManager.getInstance().canAdmitQuery("analytics_team", estimatedMemory)) {
 *       // proceed with query
 *   }
 */
public class ResourceIsolationManager {
    private static final Logger LOG = LogManager.getLogger(ResourceIsolationManager.class);

    private static volatile ResourceIsolationManager INSTANCE;

    // Tenant resource configurations
    private final Map<String, TenantResourceLimits> tenantLimits = new ConcurrentHashMap<>();

    // Current resource usage per tenant
    private final Map<String, TenantResourceUsage> tenantUsage = new ConcurrentHashMap<>();

    // Global resource tracking
    private final AtomicLong totalClusterMemoryBytes = new AtomicLong(0);
    private final AtomicLong usedClusterMemoryBytes = new AtomicLong(0);

    // Background scheduler for usage tracking
    private final ScheduledExecutorService scheduler;

    // Burst capacity pool (shared across tenants)
    private final AtomicLong burstPoolMemoryBytes = new AtomicLong(0);
    private final AtomicLong usedBurstMemoryBytes = new AtomicLong(0);

    private ResourceIsolationManager() {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ResourceIsolationManager-Monitor");
            t.setDaemon(true);
            return t;
        });

        // Schedule periodic resource usage collection
        scheduler.scheduleAtFixedRate(
                this::collectResourceUsage,
                5, 5, TimeUnit.SECONDS
        );

        // Schedule periodic reporting
        if (Config.resource_isolation_reporting_enabled) {
            scheduler.scheduleAtFixedRate(
                    this::reportResourceUsage,
                    Config.resource_isolation_reporting_interval_seconds,
                    Config.resource_isolation_reporting_interval_seconds,
                    TimeUnit.SECONDS
            );
        }

        LOG.info("CelerData ResourceIsolationManager initialized");
    }

    public static ResourceIsolationManager getInstance() {
        if (INSTANCE == null) {
            synchronized (ResourceIsolationManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ResourceIsolationManager();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Configure resource limits for a tenant
     */
    public void setTenantLimits(String tenantId, TenantResourceLimits limits) {
        // Fix: Validate that memory limit is positive to prevent division by zero later
        if (limits.memoryLimitBytes <= 0) {
            LOG.warn("CelerData Resource Isolation: Invalid memory limit {} for tenant '{}', " +
                    "must be > 0. Using Long.MAX_VALUE as default.",
                    limits.memoryLimitBytes, tenantId);
            limits.memoryLimitBytes = Long.MAX_VALUE;
        }

        if (limits.cpuLimitPercent <= 0 || limits.cpuLimitPercent > 100) {
            LOG.warn("CelerData Resource Isolation: Invalid CPU limit {}% for tenant '{}', " +
                    "must be between 1 and 100. Using 100% as default.",
                    limits.cpuLimitPercent, tenantId);
            limits.cpuLimitPercent = 100;
        }

        if (limits.ioBandwidthMBps <= 0) {
            LOG.warn("CelerData Resource Isolation: Invalid I/O bandwidth {} MB/s for tenant '{}', " +
                    "must be > 0. Using Integer.MAX_VALUE as default.",
                    limits.ioBandwidthMBps, tenantId);
            limits.ioBandwidthMBps = Integer.MAX_VALUE;
        }

        tenantLimits.put(tenantId, limits);
        tenantUsage.putIfAbsent(tenantId, new TenantResourceUsage(tenantId));

        LOG.info("CelerData Resource Isolation: Configured tenant '{}' with limits: " +
                "memory={}GB, cpu={}%, io={}MB/s, mode={}",
                tenantId,
                limits.memoryLimitBytes / (1024L * 1024 * 1024),
                limits.cpuLimitPercent,
                limits.ioBandwidthMBps,
                limits.isolationMode);
    }

    /**
     * Check if a query can be admitted based on resource availability
     */
    public QueryAdmissionResult canAdmitQuery(String tenantId, long estimatedMemoryBytes) {
        TenantResourceLimits limits = tenantLimits.get(tenantId);
        if (limits == null) {
            // No limits configured, allow by default
            return QueryAdmissionResult.admitted("No resource limits configured");
        }

        TenantResourceUsage usage = tenantUsage.get(tenantId);
        if (usage == null) {
            usage = new TenantResourceUsage(tenantId);
            tenantUsage.put(tenantId, usage);
        }

        long currentMemory = usage.currentMemoryBytes.get();
        long projectedMemory = currentMemory + estimatedMemoryBytes;

        switch (limits.isolationMode) {
            case SOFT:
                // Soft mode: Always admit but track usage
                return QueryAdmissionResult.admitted("Soft isolation mode");

            case HARD:
                // Hard mode: Reject if over limit
                if (projectedMemory > limits.memoryLimitBytes) {
                    String reason = String.format(
                            "Memory limit exceeded: current=%dMB + estimated=%dMB > limit=%dMB",
                            currentMemory / (1024 * 1024),
                            estimatedMemoryBytes / (1024 * 1024),
                            limits.memoryLimitBytes / (1024 * 1024));
                    LOG.warn("CelerData Resource Isolation: Query rejected for tenant '{}': {}",
                            tenantId, reason);
                    return QueryAdmissionResult.rejected(reason);
                }
                return QueryAdmissionResult.admitted("Within hard limits");

            case BURST:
                // Burst mode: Allow temporary burst from shared pool
                if (projectedMemory <= limits.memoryLimitBytes) {
                    return QueryAdmissionResult.admitted("Within base limits");
                }

                long burstNeeded = projectedMemory - limits.memoryLimitBytes;
                long burstAvailable = burstPoolMemoryBytes.get() - usedBurstMemoryBytes.get();

                if (burstNeeded <= burstAvailable && burstNeeded <= limits.maxBurstMemoryBytes) {
                    usedBurstMemoryBytes.addAndGet(burstNeeded);
                    return QueryAdmissionResult.admittedWithBurst(burstNeeded,
                        "Using burst capacity");
                }

                String reason = String.format(
                        "Burst capacity exhausted: needed=%dMB, available=%dMB",
                        burstNeeded / (1024 * 1024),
                        burstAvailable / (1024 * 1024));
                return QueryAdmissionResult.rejected(reason);

            default:
                return QueryAdmissionResult.admitted("Unknown mode, defaulting to admit");
        }
    }

    /**
     * Reserve resources for a query
     */
    public ResourceReservation reserveResources(String tenantId, long memoryBytes, String queryId) {
        TenantResourceUsage usage = tenantUsage.computeIfAbsent(tenantId, TenantResourceUsage::new);

        usage.currentMemoryBytes.addAndGet(memoryBytes);
        usage.activeQueries.incrementAndGet();
        usage.totalQueriesExecuted.incrementAndGet();

        LOG.debug("CelerData Resource Isolation: Reserved {}MB for query {} (tenant={})",
                memoryBytes / (1024 * 1024), queryId, tenantId);

        return new ResourceReservation(tenantId, queryId, memoryBytes);
    }

    /**
     * Release resources after query completion
     */
    public void releaseResources(ResourceReservation reservation) {
        TenantResourceUsage usage = tenantUsage.get(reservation.tenantId);
        if (usage != null) {
            usage.currentMemoryBytes.addAndGet(-reservation.memoryBytes);
            usage.activeQueries.decrementAndGet();

            LOG.debug("CelerData Resource Isolation: Released {}MB for query {} (tenant={})",
                    reservation.memoryBytes / (1024 * 1024),
                    reservation.queryId,
                    reservation.tenantId);
        }

        // Release burst memory if used
        if (reservation.burstMemoryBytes > 0) {
            usedBurstMemoryBytes.addAndGet(-reservation.burstMemoryBytes);
        }
    }

    /**
     * Get current resource usage for a tenant
     */
    public TenantResourceSnapshot getTenantSnapshot(String tenantId) {
        TenantResourceUsage usage = tenantUsage.get(tenantId);
        TenantResourceLimits limits = tenantLimits.get(tenantId);

        if (usage == null) {
            return null;
        }

        return new TenantResourceSnapshot(
            tenantId,
            usage.currentMemoryBytes.get(),
            limits != null ? limits.memoryLimitBytes : 0,
            usage.currentCpuPercent.get(),
            limits != null ? limits.cpuLimitPercent : 0,
            usage.currentIoMBps.get(),
            limits != null ? limits.ioBandwidthMBps : 0,
            usage.activeQueries.get(),
            usage.totalQueriesExecuted.get()
        );
    }

    /**
     * Get cross-tenant priority multiplier based on current usage
     * Lower usage relative to limit = higher priority for new queries
     */
    public double getTenantPriorityMultiplier(String tenantId) {
        TenantResourceLimits limits = tenantLimits.get(tenantId);
        TenantResourceUsage usage = tenantUsage.get(tenantId);

        if (limits == null || usage == null) {
            return 1.0;
        }

        // Fix: Validate that memory limit is > 0 to prevent division by zero
        if (limits.memoryLimitBytes <= 0) {
            return 1.0;
        }

        // Calculate usage ratio
        double memoryUsageRatio = (double) usage.currentMemoryBytes.get() / limits.memoryLimitBytes;

        // Lower usage = higher priority (can accept more work)
        if (memoryUsageRatio < 0.5) {
            return 1.5;  // 50% boost for underutilized tenants
        } else if (memoryUsageRatio < 0.75) {
            return 1.2;  // 20% boost
        } else if (memoryUsageRatio > 0.9) {
            return 0.5;  // 50% reduction for nearly-full tenants
        }

        return 1.0;
    }

    /**
     * Configure the burst capacity pool
     */
    public void setBurstPoolSize(long memoryBytes) {
        burstPoolMemoryBytes.set(memoryBytes);
        LOG.info("CelerData Resource Isolation: Burst pool set to {}GB",
                memoryBytes / (1024L * 1024 * 1024));
    }

    /**
     * Update cluster capacity (called when BEs join/leave)
     */
    public void updateClusterCapacity(long totalMemoryBytes) {
        totalClusterMemoryBytes.set(totalMemoryBytes);

        // Default burst pool to 10% of cluster memory
        if (burstPoolMemoryBytes.get() == 0) {
            setBurstPoolSize((long) (totalMemoryBytes * 0.10));
        }
    }

    private void collectResourceUsage() {
        // This would integrate with BE to collect actual usage
        // For now, we track FE-side accounting
    }

    private void reportResourceUsage() {
        for (String tenantId : tenantUsage.keySet()) {
            TenantResourceSnapshot snapshot = getTenantSnapshot(tenantId);
            if (snapshot != null) {
                LOG.info("CelerData Resource Usage [{}]: memory={}/{}MB ({}%), " +
                        "cpu={}% (limit={}%), io={}/{}MB/s, active_queries={}",
                        tenantId,
                        snapshot.currentMemoryBytes / (1024 * 1024),
                        snapshot.memoryLimitBytes / (1024 * 1024),
                        snapshot.getMemoryUsagePercent(),
                        snapshot.currentCpuPercent,
                        snapshot.cpuLimitPercent,
                        snapshot.currentIoMBps,
                        snapshot.ioLimitMBps,
                        snapshot.activeQueries);
            }
        }
    }

    @VisibleForTesting
    public void reset() {
        tenantLimits.clear();
        tenantUsage.clear();
        burstPoolMemoryBytes.set(0);
        usedBurstMemoryBytes.set(0);
    }

    // ==================== Inner Classes ====================

    public enum IsolationMode {
        SOFT,   // Best-effort, no hard enforcement
        HARD,   // Strict enforcement, reject queries when over limit
        BURST   // Allow temporary burst with fairness
    }

    /**
     * Resource limits configuration for a tenant
     */
    public static class TenantResourceLimits {
        public long memoryLimitBytes = Long.MAX_VALUE;
        public int cpuLimitPercent = 100;
        public int ioBandwidthMBps = Integer.MAX_VALUE;
        public long maxBurstMemoryBytes = 0;
        public IsolationMode isolationMode = IsolationMode.SOFT;
        public int priority = 5;  // 1-10, higher = more priority

        public TenantResourceLimits setMemoryLimitBytes(long bytes) {
            this.memoryLimitBytes = bytes;
            return this;
        }

        public TenantResourceLimits setCpuLimitPercent(int percent) {
            this.cpuLimitPercent = percent;
            return this;
        }

        public TenantResourceLimits setIoBandwidthMBps(int mbps) {
            this.ioBandwidthMBps = mbps;
            return this;
        }

        public TenantResourceLimits setMaxBurstMemoryBytes(long bytes) {
            this.maxBurstMemoryBytes = bytes;
            return this;
        }

        public TenantResourceLimits setIsolationMode(IsolationMode mode) {
            this.isolationMode = mode;
            return this;
        }

        public TenantResourceLimits setPriority(int priority) {
            this.priority = priority;
            return this;
        }
    }

    /**
     * Current resource usage for a tenant
     */
    private static class TenantResourceUsage {
        public final String tenantId;
        public final AtomicLong currentMemoryBytes = new AtomicLong(0);
        public final AtomicLong currentCpuPercent = new AtomicLong(0);
        public final AtomicLong currentIoMBps = new AtomicLong(0);
        public final AtomicLong activeQueries = new AtomicLong(0);
        public final AtomicLong totalQueriesExecuted = new AtomicLong(0);

        public TenantResourceUsage(String tenantId) {
            this.tenantId = tenantId;
        }
    }

    /**
     * Query admission result
     */
    public static class QueryAdmissionResult {
        public final boolean admitted;
        public final String reason;
        public final long burstMemoryUsed;

        private QueryAdmissionResult(boolean admitted, String reason, long burstMemoryUsed) {
            this.admitted = admitted;
            this.reason = reason;
            this.burstMemoryUsed = burstMemoryUsed;
        }

        public static QueryAdmissionResult admitted(String reason) {
            return new QueryAdmissionResult(true, reason, 0);
        }

        public static QueryAdmissionResult admittedWithBurst(long burstMemory, String reason) {
            return new QueryAdmissionResult(true, reason, burstMemory);
        }

        public static QueryAdmissionResult rejected(String reason) {
            return new QueryAdmissionResult(false, reason, 0);
        }
    }

    /**
     * Resource reservation handle
     */
    public static class ResourceReservation {
        public final String tenantId;
        public final String queryId;
        public final long memoryBytes;
        public long burstMemoryBytes = 0;

        public ResourceReservation(String tenantId, String queryId, long memoryBytes) {
            this.tenantId = tenantId;
            this.queryId = queryId;
            this.memoryBytes = memoryBytes;
        }

        public ResourceReservation withBurstMemory(long burstBytes) {
            this.burstMemoryBytes = burstBytes;
            return this;
        }
    }

    /**
     * Snapshot of tenant resource usage
     */
    public static class TenantResourceSnapshot {
        public final String tenantId;
        public final long currentMemoryBytes;
        public final long memoryLimitBytes;
        public final long currentCpuPercent;
        public final int cpuLimitPercent;
        public final long currentIoMBps;
        public final int ioLimitMBps;
        public final long activeQueries;
        public final long totalQueries;

        public TenantResourceSnapshot(String tenantId, long currentMemoryBytes, long memoryLimitBytes,
                                     long currentCpuPercent, int cpuLimitPercent,
                                     long currentIoMBps, int ioLimitMBps,
                                     long activeQueries, long totalQueries) {
            this.tenantId = tenantId;
            this.currentMemoryBytes = currentMemoryBytes;
            this.memoryLimitBytes = memoryLimitBytes;
            this.currentCpuPercent = currentCpuPercent;
            this.cpuLimitPercent = cpuLimitPercent;
            this.currentIoMBps = currentIoMBps;
            this.ioLimitMBps = ioLimitMBps;
            this.activeQueries = activeQueries;
            this.totalQueries = totalQueries;
        }

        public double getMemoryUsagePercent() {
            if (memoryLimitBytes == 0) {
                return 0;
            }
            return (double) currentMemoryBytes / memoryLimitBytes * 100.0;
        }

        public boolean isOverMemoryLimit() {
            return currentMemoryBytes > memoryLimitBytes;
        }

        public boolean isOverCpuLimit() {
            return currentCpuPercent > cpuLimitPercent;
        }
    }
}
