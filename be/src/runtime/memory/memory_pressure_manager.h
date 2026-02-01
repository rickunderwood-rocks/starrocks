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

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace starrocks {

/**
 * CelerData Memory Pressure Manager
 *
 * Implements graceful degradation under memory pressure for improved stability.
 * Instead of OOM kills, the system progressively reduces functionality to
 * maintain availability.
 *
 * Pressure Levels:
 *   - NORMAL (0-70%):   Full functionality, no restrictions
 *   - LOW (70-80%):     Warning level, start rejecting new large queries
 *   - MEDIUM (80-90%):  Moderate pressure, reject medium queries, reduce caches
 *   - HIGH (90-95%):    High pressure, only small queries, aggressive cache eviction
 *   - CRITICAL (95%+):  Emergency mode, reject all new queries, preserve running ones
 *
 * Usage:
 *   auto& mgr = MemoryPressureManager::instance();
 *   if (mgr.should_admit_query(estimated_memory)) {
 *       // Execute query
 *   } else {
 *       // Return memory pressure error to client
 *   }
 */
class MemoryPressureManager {
public:
    // Memory pressure levels
    enum class PressureLevel {
        NORMAL = 0,     // 0-70% memory usage
        LOW = 1,        // 70-80% memory usage
        MEDIUM = 2,     // 80-90% memory usage
        HIGH = 3,       // 90-95% memory usage
        CRITICAL = 4    // 95%+ memory usage
    };

    // Query size categories
    enum class QuerySize {
        SMALL,    // < 100MB estimated
        MEDIUM,   // 100MB - 1GB estimated
        LARGE,    // 1GB - 10GB estimated
        HUGE      // > 10GB estimated
    };

    // Pressure level thresholds (memory usage percentage)
    static constexpr double THRESHOLD_LOW = 0.70;
    static constexpr double THRESHOLD_MEDIUM = 0.80;
    static constexpr double THRESHOLD_HIGH = 0.90;
    static constexpr double THRESHOLD_CRITICAL = 0.95;

    // Query size thresholds (in bytes)
    static constexpr int64_t SMALL_QUERY_THRESHOLD = 100 * 1024 * 1024;       // 100MB
    static constexpr int64_t MEDIUM_QUERY_THRESHOLD = 1024 * 1024 * 1024LL;   // 1GB
    static constexpr int64_t LARGE_QUERY_THRESHOLD = 10 * 1024 * 1024 * 1024LL; // 10GB

    // Callback type for pressure level changes
    using PressureCallback = std::function<void(PressureLevel, PressureLevel)>;

    static MemoryPressureManager& instance();

    // Get current pressure level
    PressureLevel get_pressure_level() const { return _current_level.load(); }

    // Get current memory usage ratio (0.0 - 1.0)
    double get_memory_usage_ratio() const { return _memory_usage_ratio.load(); }

    // Check if a query should be admitted based on estimated memory
    bool should_admit_query(int64_t estimated_memory_bytes);

    // Get query size category
    static QuerySize categorize_query_size(int64_t estimated_memory_bytes);

    // Update memory usage (called periodically by monitoring thread)
    void update_memory_usage(int64_t used_bytes, int64_t total_bytes);

    // Register callback for pressure level changes
    void register_pressure_callback(PressureCallback callback);

    // Get string representation of pressure level
    static const char* pressure_level_to_string(PressureLevel level);

    // Check if we should evict caches
    bool should_evict_caches() const;

    // Get recommended cache eviction aggressiveness (0.0 - 1.0)
    double get_cache_eviction_ratio() const;

    // Statistics
    int64_t get_rejected_queries() const { return _rejected_queries.load(); }
    int64_t get_admitted_queries() const { return _admitted_queries.load(); }

private:
    MemoryPressureManager();
    ~MemoryPressureManager() = default;

    // Singleton - no copy/move
    MemoryPressureManager(const MemoryPressureManager&) = delete;
    MemoryPressureManager& operator=(const MemoryPressureManager&) = delete;

    // Calculate pressure level from usage ratio
    static PressureLevel calculate_pressure_level(double usage_ratio);

    // Notify registered callbacks of pressure change
    void notify_pressure_change(PressureLevel old_level, PressureLevel new_level);

    std::atomic<PressureLevel> _current_level{PressureLevel::NORMAL};
    std::atomic<double> _memory_usage_ratio{0.0};

    std::mutex _callback_mutex;
    std::vector<PressureCallback> _callbacks;

    // Statistics
    std::atomic<int64_t> _rejected_queries{0};
    std::atomic<int64_t> _admitted_queries{0};

    // Last pressure change time
    std::chrono::steady_clock::time_point _last_pressure_change;
};

} // namespace starrocks
