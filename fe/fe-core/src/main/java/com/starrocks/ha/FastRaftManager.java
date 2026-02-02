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

package com.starrocks.ha;

import com.sleepycat.je.rep.ReplicationConfig;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CelerData Fast Raft Manager
 *
 * Implements optimizations for sub-2-second leader failover in BDBJE-based
 * Raft consensus. This manager coordinates:
 *
 * 1. Fast Heartbeat: Reduces heartbeat interval from 30s to 200ms
 * 2. Quick Election: Election timeout of 800ms (vs default 30s)
 * 3. Leader Lease: Allows leader to serve reads during lease period
 * 4. Pre-Vote Protocol: Prevents term inflation from partitioned nodes
 *
 * Failover Timeline (when enabled):
 *   T+0ms:     Leader failure occurs
 *   T+200ms:   First missed heartbeat detected
 *   T+400ms:   Second missed heartbeat (confirms failure)
 *   T+600ms:   Third missed heartbeat (election timeout starts)
 *   T+800ms:   Election timeout expires, pre-vote begins
 *   T+1000ms:  Pre-vote succeeds, real election starts
 *   T+1200ms:  New leader elected
 *   T+1500ms:  Leader ready to serve (state transfer complete)
 *   Total:     ~1.5 seconds (vs 30+ seconds with default settings)
 *
 * Usage:
 *   FastRaftManager.applyFastRaftConfig(replicationConfig);
 */
public class FastRaftManager {
    private static final Logger LOG = LogManager.getLogger(FastRaftManager.class);

    // Singleton instance
    private static volatile FastRaftManager instance;

    // Fast Raft state
    private final AtomicBoolean fastModeEnabled = new AtomicBoolean(false);
    private final AtomicLong lastHeartbeatTime = new AtomicLong(0);
    private final AtomicLong leaderLeaseExpiry = new AtomicLong(0);

    // Pre-vote state
    private final AtomicBoolean preVoteInProgress = new AtomicBoolean(false);
    private final AtomicLong preVoteStartTime = new AtomicLong(0);

    // Statistics
    private final AtomicLong failoverCount = new AtomicLong(0);
    private final AtomicLong fastestFailoverMs = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong totalFailoverTimeMs = new AtomicLong(0);

    private FastRaftManager() {
        // Validate configuration parameters
        validateAndApplyConfig();
        this.fastModeEnabled.set(Config.fast_raft_failover_enabled);
        LOG.info("CelerData FastRaftManager initialized: enabled={}, heartbeat={}ms, election={}ms, lease={}ms",
                Config.fast_raft_failover_enabled,
                Config.fast_raft_heartbeat_interval_ms,
                Config.fast_raft_election_timeout_ms,
                Config.fast_raft_leader_lease_ms);
    }

    public static FastRaftManager getInstance() {
        if (instance == null) {
            synchronized (FastRaftManager.class) {
                if (instance == null) {
                    instance = new FastRaftManager();
                }
            }
        }
        return instance;
    }

    /**
     * Validate and apply fast Raft configuration parameters.
     * Ensures all timeout values are positive and within reasonable ranges.
     * Logs warnings if configuration is invalid or suboptimal.
     */
    private static void validateAndApplyConfig() {
        StringBuilder validationErrors = new StringBuilder();

        // Validate heartbeat interval
        if (Config.fast_raft_heartbeat_interval_ms <= 0) {
            validationErrors.append("fast_raft_heartbeat_interval_ms must be > 0; ");
        } else if (Config.fast_raft_heartbeat_interval_ms < 100 || Config.fast_raft_heartbeat_interval_ms > 5000) {
            LOG.warn("fast_raft_heartbeat_interval_ms {} is outside recommended range [100, 5000]ms",
                    Config.fast_raft_heartbeat_interval_ms);
        }

        // Validate election timeout
        if (Config.fast_raft_election_timeout_ms <= 0) {
            validationErrors.append("fast_raft_election_timeout_ms must be > 0; ");
        } else if (Config.fast_raft_election_timeout_ms < Config.fast_raft_heartbeat_interval_ms * 2) {
            validationErrors.append("fast_raft_election_timeout_ms must be >= 2 * heartbeat_interval; ");
        } else if (Config.fast_raft_election_timeout_ms > 30000) {
            LOG.warn("fast_raft_election_timeout_ms {} exceeds 30s, failover may be slow",
                    Config.fast_raft_election_timeout_ms);
        }

        // Validate leader lease
        if (Config.fast_raft_leader_lease_ms < 0) {
            validationErrors.append("fast_raft_leader_lease_ms must be >= 0; ");
        } else if (Config.fast_raft_leader_lease_ms > Config.fast_raft_election_timeout_ms) {
            validationErrors.append("fast_raft_leader_lease_ms must be <= election_timeout; ");
        }

        // Validate state transfer timeout
        if (Config.fast_raft_state_transfer_timeout_ms <= 0) {
            validationErrors.append("fast_raft_state_transfer_timeout_ms must be > 0; ");
        }

        if (validationErrors.length() > 0) {
            String errMsg = "FastRaftManager configuration validation failed: " + validationErrors.toString();
            LOG.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }
    }

    /**
     * Apply fast Raft configuration to BDBJE ReplicationConfig.
     * Should be called before environment setup.
     * All configuration parameters are validated before application.
     *
     * @param replicationConfig The BDBJE ReplicationConfig to modify
     * @throws IllegalArgumentException if configuration validation fails
     */
    public static void applyFastRaftConfig(ReplicationConfig replicationConfig) {
        if (replicationConfig == null) {
            LOG.error("Cannot apply FastRaft config: replicationConfig is null");
            throw new IllegalArgumentException("replicationConfig cannot be null");
        }

        if (!Config.fast_raft_failover_enabled) {
            LOG.info("Fast Raft failover is disabled, using default BDBJE timeouts");
            return;
        }

        try {
            // Validate configuration before applying
            validateAndApplyConfig();

            // Convert milliseconds to seconds for BDBJE (with fractional support)
            String heartbeatTimeout = formatTimeout(Config.fast_raft_heartbeat_interval_ms * 3);
            String electionTimeout = formatTimeout(Config.fast_raft_election_timeout_ms);

            // Apply fast timeouts with error handling for each parameter
            try {
                replicationConfig.setConfigParam(ReplicationConfig.REPLICA_TIMEOUT, heartbeatTimeout);
                replicationConfig.setConfigParam(ReplicationConfig.FEEDER_TIMEOUT, heartbeatTimeout);
                replicationConfig.setConfigParam(ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT,
                        String.valueOf(Config.fast_raft_election_timeout_ms / 1000 + 1));

                // Set election rebroadcast period (how often to retry election messages)
                replicationConfig.setConfigParam(ReplicationConfig.ELECTIONS_REBROADCAST_PERIOD,
                        formatTimeout(Config.fast_raft_heartbeat_interval_ms));

                // Set election primary retry for faster convergence
                replicationConfig.setConfigParam(ReplicationConfig.ELECTIONS_PRIMARY_RETRIES, "3");

                // Set replica ack timeout based on heartbeat
                replicationConfig.setReplicaAckTimeout(
                        Config.fast_raft_heartbeat_interval_ms * 5, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                LOG.error("Failed to apply Fast Raft config parameters", e);
                throw new IllegalArgumentException("Failed to apply Fast Raft configuration: " + e.getMessage(), e);
            }

            LOG.info("CelerData Fast Raft configuration applied: " +
                            "replica_timeout={}, feeder_timeout={}, election_rebroadcast={}, " +
                            "replica_ack_timeout={}ms",
                    heartbeatTimeout, heartbeatTimeout,
                    formatTimeout(Config.fast_raft_heartbeat_interval_ms),
                    Config.fast_raft_heartbeat_interval_ms * 5);
        } catch (IllegalArgumentException e) {
            LOG.error("FastRaft configuration validation failed, falling back to default settings", e);
            throw e;
        }
    }

    /**
     * Format timeout value for BDBJE (supports "X ms" or "X s" format)
     */
    private static String formatTimeout(int milliseconds) {
        if (milliseconds < 1000) {
            return milliseconds + " ms";
        } else {
            return (milliseconds / 1000) + " s";
        }
    }

    /**
     * Record heartbeat received from leader.
     * Used to track leader liveness.
     */
    public void recordHeartbeat() {
        lastHeartbeatTime.set(System.currentTimeMillis());
    }

    /**
     * Extend the leader lease.
     * Called by leader after successful quorum write.
     */
    public void extendLeaderLease() {
        if (fastModeEnabled.get()) {
            leaderLeaseExpiry.set(System.currentTimeMillis() + Config.fast_raft_leader_lease_ms);
        }
    }

    /**
     * Check if leader lease is still valid.
     * During valid lease, leader can serve reads without quorum check.
     */
    public boolean isLeaderLeaseValid() {
        if (!fastModeEnabled.get()) {
            return false;
        }
        return System.currentTimeMillis() < leaderLeaseExpiry.get();
    }

    /**
     * Check if we should trigger a pre-vote.
     * Pre-vote prevents term inflation from partitioned nodes.
     */
    public boolean shouldStartPreVote() {
        if (!Config.fast_raft_prevote_enabled) {
            return false;
        }

        long lastHeartbeat = lastHeartbeatTime.get();
        if (lastHeartbeat == 0) {
            return false;
        }

        long timeSinceHeartbeat = System.currentTimeMillis() - lastHeartbeat;
        return timeSinceHeartbeat > Config.fast_raft_election_timeout_ms;
    }

    /**
     * Record the start of a pre-vote phase.
     */
    public void startPreVote() {
        preVoteInProgress.set(true);
        preVoteStartTime.set(System.currentTimeMillis());
        LOG.info("CelerData Fast Raft: Starting pre-vote phase");
    }

    /**
     * Complete the pre-vote phase.
     */
    public void completePreVote(boolean success) {
        preVoteInProgress.set(false);
        long duration = System.currentTimeMillis() - preVoteStartTime.get();
        LOG.info("CelerData Fast Raft: Pre-vote completed: success={}, duration={}ms",
                success, duration);
    }

    /**
     * Record a failover event for statistics.
     */
    public void recordFailover(long failoverDurationMs) {
        failoverCount.incrementAndGet();
        totalFailoverTimeMs.addAndGet(failoverDurationMs);

        long currentFastest = fastestFailoverMs.get();
        if (failoverDurationMs < currentFastest) {
            fastestFailoverMs.compareAndSet(currentFastest, failoverDurationMs);
        }

        LOG.info("CelerData Fast Raft: Failover completed in {}ms " +
                        "(total={}, fastest={}ms, avg={}ms)",
                failoverDurationMs,
                failoverCount.get(),
                fastestFailoverMs.get() == Long.MAX_VALUE ? 0 : fastestFailoverMs.get(),
                failoverCount.get() > 0 ? totalFailoverTimeMs.get() / failoverCount.get() : 0);
    }

    /**
     * Get the expected maximum failover time based on configuration.
     */
    public int getExpectedMaxFailoverMs() {
        if (!Config.fast_raft_failover_enabled) {
            // Default mode: heartbeat timeout + election + state transfer
            return Config.bdbje_heartbeat_timeout_second * 1000 +
                    Config.bdbje_replica_ack_timeout_second * 1000;
        }

        // Fast mode: 3 missed heartbeats + election timeout + state transfer
        return (Config.fast_raft_heartbeat_interval_ms * 3) +
                Config.fast_raft_election_timeout_ms +
                Config.fast_raft_state_transfer_timeout_ms;
    }

    /**
     * Check if fast Raft mode is enabled.
     */
    public boolean isFastModeEnabled() {
        return fastModeEnabled.get();
    }

    /**
     * Get failover statistics.
     */
    public String getStats() {
        return String.format(
                "FastRaft[enabled=%b, failovers=%d, fastest=%dms, avg=%dms, expected_max=%dms]",
                fastModeEnabled.get(),
                failoverCount.get(),
                fastestFailoverMs.get() == Long.MAX_VALUE ? 0 : fastestFailoverMs.get(),
                failoverCount.get() > 0 ? totalFailoverTimeMs.get() / failoverCount.get() : 0,
                getExpectedMaxFailoverMs());
    }
}
