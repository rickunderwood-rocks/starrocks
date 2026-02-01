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

#include "runtime/memory/memory_pressure_manager.h"

#include "common/logging.h"

namespace starrocks {

MemoryPressureManager& MemoryPressureManager::instance() {
    static MemoryPressureManager instance;
    return instance;
}

MemoryPressureManager::MemoryPressureManager()
    : _last_pressure_change(std::chrono::steady_clock::now()) {
    LOG(INFO) << "CelerData MemoryPressureManager initialized with thresholds: "
              << "LOW=" << (THRESHOLD_LOW * 100) << "%, "
              << "MEDIUM=" << (THRESHOLD_MEDIUM * 100) << "%, "
              << "HIGH=" << (THRESHOLD_HIGH * 100) << "%, "
              << "CRITICAL=" << (THRESHOLD_CRITICAL * 100) << "%";
}

bool MemoryPressureManager::should_admit_query(int64_t estimated_memory_bytes) {
    PressureLevel level = _current_level.load();
    QuerySize query_size = categorize_query_size(estimated_memory_bytes);

    bool should_admit = false;

    switch (level) {
        case PressureLevel::NORMAL:
            // Accept all queries
            should_admit = true;
            break;

        case PressureLevel::LOW:
            // Reject huge queries only
            should_admit = (query_size != QuerySize::HUGE);
            break;

        case PressureLevel::MEDIUM:
            // Reject large and huge queries
            should_admit = (query_size == QuerySize::SMALL || query_size == QuerySize::MEDIUM);
            break;

        case PressureLevel::HIGH:
            // Only accept small queries
            should_admit = (query_size == QuerySize::SMALL);
            break;

        case PressureLevel::CRITICAL:
            // Reject all new queries
            should_admit = false;
            break;
    }

    if (should_admit) {
        _admitted_queries.fetch_add(1, std::memory_order_relaxed);
    } else {
        _rejected_queries.fetch_add(1, std::memory_order_relaxed);
        LOG(WARNING) << "CelerData MemoryPressureManager rejecting query: "
                     << "pressure_level=" << pressure_level_to_string(level)
                     << ", estimated_memory=" << (estimated_memory_bytes / (1024 * 1024)) << "MB"
                     << ", query_size=" << static_cast<int>(query_size);
    }

    return should_admit;
}

MemoryPressureManager::QuerySize MemoryPressureManager::categorize_query_size(int64_t estimated_memory_bytes) {
    if (estimated_memory_bytes < SMALL_QUERY_THRESHOLD) {
        return QuerySize::SMALL;
    } else if (estimated_memory_bytes < MEDIUM_QUERY_THRESHOLD) {
        return QuerySize::MEDIUM;
    } else if (estimated_memory_bytes < LARGE_QUERY_THRESHOLD) {
        return QuerySize::LARGE;
    } else {
        return QuerySize::HUGE;
    }
}

void MemoryPressureManager::update_memory_usage(int64_t used_bytes, int64_t total_bytes) {
    if (total_bytes <= 0) {
        return;
    }

    double ratio = static_cast<double>(used_bytes) / static_cast<double>(total_bytes);
    _memory_usage_ratio.store(ratio);

    PressureLevel new_level = calculate_pressure_level(ratio);
    PressureLevel old_level = _current_level.exchange(new_level);

    if (old_level != new_level) {
        _last_pressure_change = std::chrono::steady_clock::now();

        LOG(INFO) << "CelerData MemoryPressureManager: pressure level changed from "
                  << pressure_level_to_string(old_level) << " to "
                  << pressure_level_to_string(new_level)
                  << " (usage=" << (ratio * 100) << "%)";

        notify_pressure_change(old_level, new_level);
    }
}

MemoryPressureManager::PressureLevel MemoryPressureManager::calculate_pressure_level(double usage_ratio) {
    if (usage_ratio >= THRESHOLD_CRITICAL) {
        return PressureLevel::CRITICAL;
    } else if (usage_ratio >= THRESHOLD_HIGH) {
        return PressureLevel::HIGH;
    } else if (usage_ratio >= THRESHOLD_MEDIUM) {
        return PressureLevel::MEDIUM;
    } else if (usage_ratio >= THRESHOLD_LOW) {
        return PressureLevel::LOW;
    } else {
        return PressureLevel::NORMAL;
    }
}

void MemoryPressureManager::register_pressure_callback(PressureCallback callback) {
    std::lock_guard<std::mutex> lock(_callback_mutex);
    _callbacks.push_back(std::move(callback));
}

void MemoryPressureManager::notify_pressure_change(PressureLevel old_level, PressureLevel new_level) {
    std::lock_guard<std::mutex> lock(_callback_mutex);
    for (const auto& callback : _callbacks) {
        try {
            callback(old_level, new_level);
        } catch (const std::exception& e) {
            LOG(WARNING) << "CelerData MemoryPressureManager: callback exception: " << e.what();
        }
    }
}

const char* MemoryPressureManager::pressure_level_to_string(PressureLevel level) {
    switch (level) {
        case PressureLevel::NORMAL:
            return "NORMAL";
        case PressureLevel::LOW:
            return "LOW";
        case PressureLevel::MEDIUM:
            return "MEDIUM";
        case PressureLevel::HIGH:
            return "HIGH";
        case PressureLevel::CRITICAL:
            return "CRITICAL";
        default:
            return "UNKNOWN";
    }
}

bool MemoryPressureManager::should_evict_caches() const {
    PressureLevel level = _current_level.load();
    return level >= PressureLevel::MEDIUM;
}

double MemoryPressureManager::get_cache_eviction_ratio() const {
    PressureLevel level = _current_level.load();

    switch (level) {
        case PressureLevel::NORMAL:
            return 0.0;  // No eviction needed
        case PressureLevel::LOW:
            return 0.1;  // Evict 10% of caches
        case PressureLevel::MEDIUM:
            return 0.3;  // Evict 30% of caches
        case PressureLevel::HIGH:
            return 0.5;  // Evict 50% of caches
        case PressureLevel::CRITICAL:
            return 0.8;  // Evict 80% of caches
        default:
            return 0.0;
    }
}

} // namespace starrocks
