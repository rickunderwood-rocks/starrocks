// Copyright 2021-present StarRocks, Inc.
// Copyright 2026 CelerData, Inc. - Enterprise Performance Optimizations
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

#include "runtime/memory/adaptive_memory_manager.h"
#include "util/logging.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <numeric>

#ifdef __linux__
#include <sys/sysinfo.h>
#endif

namespace starrocks {

AdaptiveMemoryManager& AdaptiveMemoryManager::instance() {
    static AdaptiveMemoryManager instance;
    return instance;
}

AdaptiveMemoryManager::AdaptiveMemoryManager() {
    _total_memory = detect_total_memory();

    // Initialize default budgets
    Config default_config;
    configure(default_config);
}

AdaptiveMemoryManager::~AdaptiveMemoryManager() {
    stop_adaptation_thread();
}

size_t AdaptiveMemoryManager::detect_total_memory() {
#ifdef __linux__
    struct sysinfo info;
    if (sysinfo(&info) == 0) {
        return info.totalram * info.mem_unit;
    }
#endif
    // Fallback: assume 16GB
    return 16ULL * 1024 * 1024 * 1024;
}

void AdaptiveMemoryManager::configure(const Config& config) {
    std::lock_guard<std::mutex> lock(_mutex);

    _config = config;

    if (_config.total_memory_limit == 0) {
        _config.total_memory_limit = _total_memory;
    }

    // Create default budgets
    _budgets["query"] = MemoryBudget{
        .limit = static_cast<size_t>(_config.total_memory_limit * _config.query_memory_ratio),
        .name = "query"
    };
    _budgets["query"].soft_limit = static_cast<size_t>(_budgets["query"].limit * 0.8);

    _budgets["cache"] = MemoryBudget{
        .limit = static_cast<size_t>(_config.total_memory_limit * _config.cache_memory_ratio),
        .name = "cache"
    };

    _budgets["compaction"] = MemoryBudget{
        .limit = static_cast<size_t>(_config.total_memory_limit * _config.compaction_memory_ratio),
        .name = "compaction"
    };

    _budgets["reserve"] = MemoryBudget{
        .limit = static_cast<size_t>(_config.total_memory_limit * _config.reserve_ratio),
        .name = "reserve"
    };

    LOG(INFO) << "AdaptiveMemoryManager configured: total=" << _config.total_memory_limit
              << ", query=" << _budgets["query"].limit
              << ", cache=" << _budgets["cache"].limit
              << ", compaction=" << _budgets["compaction"].limit;

    start_adaptation_thread();
}

AllocationResult AdaptiveMemoryManager::request_memory(const AllocationRequest& request) {
    std::lock_guard<std::mutex> lock(_mutex);

    _stats.allocation_requests++;

    AllocationResult result;

    // Determine which budget to use
    std::string budget_name = "query";
    switch (request.type) {
        case AllocationRequest::Type::CACHE:
            budget_name = "cache";
            break;
        case AllocationRequest::Type::COMPACTION:
            budget_name = "compaction";
            break;
        default:
            budget_name = "query";
            break;
    }

    auto& budget = _budgets[budget_name];

    // Check if allocation is possible
    if (budget.allocated + request.bytes_requested <= budget.limit) {
        // Direct allocation
        budget.allocated += request.bytes_requested;
        budget.peak = std::max(budget.peak, budget.allocated);

        _stats.total_allocated += request.bytes_requested;
        _stats.peak_memory = std::max(
            _stats.peak_memory.load(),
            _stats.total_allocated.load()
        );

        result.success = true;
        result.bytes_allocated = request.bytes_requested;
        result.recommended_action = AllocationResult::Action::ALLOCATED;

        // Learn from allocation
        if (_config.enable_workload_learning) {
            auto& pattern = _workload_patterns[request.type];
            pattern.recent_allocations.push_back(request.bytes_requested);
            if (pattern.recent_allocations.size() > 100) {
                pattern.recent_allocations.erase(pattern.recent_allocations.begin());
            }
        }

        return result;
    }

    // Allocation would exceed budget
    _stats.allocation_denials++;

    result.bytes_available = budget.available();

    // Determine recommended action
    if (request.can_wait) {
        result.recommended_action = AllocationResult::Action::WAIT_AND_RETRY;
        result.denial_reason = "Memory budget full, waiting for release";
    } else if (request.can_spill && request.bytes_requested > result.bytes_available) {
        result.recommended_action = AllocationResult::Action::SPILL_TO_DISK;
        result.denial_reason = "Insufficient memory, spill recommended";
    } else if (result.bytes_available >= request.bytes_requested / 2) {
        result.recommended_action = AllocationResult::Action::REDUCE_REQUEST;
        result.denial_reason = "Reduce request to fit available memory";
    } else {
        result.recommended_action = AllocationResult::Action::CANCEL_QUERY;
        result.denial_reason = "Insufficient memory, query cancellation recommended";
    }

    // Try to reclaim memory
    if (_current_pressure >= MemoryPressureLevel::HIGH) {
        size_t reclaimed = invoke_pressure_callbacks(request.bytes_requested);
        if (reclaimed >= request.bytes_requested) {
            // Retry allocation
            return request_memory(request);
        }
    }

    return result;
}

void AdaptiveMemoryManager::release_memory(const std::string& requester, size_t bytes) {
    std::lock_guard<std::mutex> lock(_mutex);

    // Find the budget for this requester
    for (auto& [name, budget] : _budgets) {
        if (budget.allocated >= bytes) {
            budget.allocated -= bytes;
            _stats.total_released += bytes;
            return;
        }
    }

    LOG(WARNING) << "Memory release for unknown allocation: " << bytes << " bytes from " << requester;
}

MemoryBudget AdaptiveMemoryManager::get_budget(const std::string& name) const {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _budgets.find(name);
    return it != _budgets.end() ? it->second : MemoryBudget{};
}

void AdaptiveMemoryManager::set_budget_limit(const std::string& name, size_t limit) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_budgets.count(name)) {
        _budgets[name].limit = limit;
        _budgets[name].soft_limit = static_cast<size_t>(limit * 0.8);
    }
}

std::vector<MemoryBudget> AdaptiveMemoryManager::get_all_budgets() const {
    std::lock_guard<std::mutex> lock(_mutex);
    std::vector<MemoryBudget> result;
    for (const auto& [name, budget] : _budgets) {
        result.push_back(budget);
    }
    return result;
}

size_t AdaptiveMemoryManager::get_query_memory_limit(const std::string& query_id) const {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _query_allocations.find(query_id);
    return it != _query_allocations.end() ? it->second : 0;
}

void AdaptiveMemoryManager::register_query(const std::string& query_id, size_t estimated_memory) {
    std::lock_guard<std::mutex> lock(_mutex);

    size_t query_limit = estimated_memory;

    // Apply predictive adjustment if enabled
    if (_config.enable_predictive_allocation) {
        auto& pattern = _workload_patterns[AllocationRequest::Type::QUERY_EXECUTION];
        if (!pattern.recent_allocations.empty()) {
            // Use historical data to adjust estimate
            double avg = static_cast<double>(pattern.average_allocation);
            if (estimated_memory < avg * 0.5) {
                // Estimate seems too low, adjust up
                query_limit = static_cast<size_t>(avg * 0.8);
            } else if (estimated_memory > avg * 2) {
                // Estimate seems high, be cautious
                query_limit = static_cast<size_t>(estimated_memory * 0.9);
            }
        }
    }

    _query_allocations[query_id] = query_limit;
    LOG(INFO) << "Registered query " << query_id << " with memory limit " << query_limit;
}

void AdaptiveMemoryManager::unregister_query(const std::string& query_id) {
    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _query_allocations.find(query_id);
    if (it != _query_allocations.end()) {
        _query_allocations.erase(it);
    }
}

void AdaptiveMemoryManager::adapt_to_workload() {
    std::lock_guard<std::mutex> lock(_mutex);

    _stats.adaptations++;

    // Update workload patterns
    for (auto& [type, pattern] : _workload_patterns) {
        if (!pattern.recent_allocations.empty()) {
            // Calculate average
            pattern.average_allocation = std::accumulate(
                pattern.recent_allocations.begin(),
                pattern.recent_allocations.end(),
                0ULL
            ) / pattern.recent_allocations.size();

            // Calculate peak
            pattern.peak_allocation = *std::max_element(
                pattern.recent_allocations.begin(),
                pattern.recent_allocations.end()
            );

            // Calculate variance
            double mean = static_cast<double>(pattern.average_allocation);
            double variance = 0;
            for (size_t val : pattern.recent_allocations) {
                variance += (val - mean) * (val - mean);
            }
            pattern.allocation_variance = std::sqrt(variance / pattern.recent_allocations.size());
        }
    }

    // Adjust budgets based on utilization
    double total_utilization = 0;
    for (const auto& [name, budget] : _budgets) {
        total_utilization += budget.utilization();
    }
    total_utilization /= _budgets.size();

    // If overall utilization is low, we can be more generous
    if (total_utilization < 0.5) {
        // System is underutilized, increase query budget
        auto& query_budget = _budgets["query"];
        size_t additional = static_cast<size_t>(_config.total_memory_limit * 0.1);
        query_budget.limit = std::min(
            query_budget.limit + additional,
            static_cast<size_t>(_config.total_memory_limit * 0.8)
        );
    } else if (total_utilization > 0.8) {
        // System under pressure, be more conservative
        auto& cache_budget = _budgets["cache"];
        size_t reduction = static_cast<size_t>(cache_budget.limit * 0.1);
        cache_budget.limit = std::max(
            cache_budget.limit - reduction,
            static_cast<size_t>(_config.total_memory_limit * 0.1)
        );
    }

    update_pressure_level();

    LOG(INFO) << "AdaptiveMemoryManager adapted: utilization=" << (total_utilization * 100)
              << "%, pressure=" << static_cast<int>(_current_pressure.load());
}

void AdaptiveMemoryManager::reclaim_memory(size_t target_bytes) {
    std::lock_guard<std::mutex> lock(_mutex);

    _stats.forced_reclamations++;
    invoke_pressure_callbacks(target_bytes);
}

void AdaptiveMemoryManager::register_pressure_callback(
    const std::string& name, PressureCallback callback) {

    std::lock_guard<std::mutex> lock(_mutex);
    _pressure_callbacks[name] = callback;
}

void AdaptiveMemoryManager::unregister_pressure_callback(const std::string& name) {
    std::lock_guard<std::mutex> lock(_mutex);
    _pressure_callbacks.erase(name);
}

void AdaptiveMemoryManager::start_adaptation_thread() {
    if (_running.exchange(true)) {
        return;  // Already running
    }

    _adaptation_thread = std::thread([this]() {
        adaptation_loop();
    });
}

void AdaptiveMemoryManager::stop_adaptation_thread() {
    _running = false;
    if (_adaptation_thread.joinable()) {
        _adaptation_thread.join();
    }
}

void AdaptiveMemoryManager::adaptation_loop() {
    while (_running) {
        std::this_thread::sleep_for(_config.adaptation_interval);

        if (!_running) break;

        adapt_to_workload();

        // Check for memory pressure
        if (_current_pressure >= MemoryPressureLevel::HIGH) {
            reclaim_memory(_config.min_reclaim_bytes);
        }
    }
}

void AdaptiveMemoryManager::update_pressure_level() {
    size_t total_allocated = 0;
    for (const auto& [name, budget] : _budgets) {
        total_allocated += budget.allocated;
    }

    double utilization = static_cast<double>(total_allocated) / _config.total_memory_limit;

    if (utilization >= _config.emergency_threshold) {
        _current_pressure = MemoryPressureLevel::CRITICAL;
    } else if (utilization >= _config.critical_memory_threshold) {
        _current_pressure = MemoryPressureLevel::HIGH;
    } else if (utilization >= _config.low_memory_threshold) {
        _current_pressure = MemoryPressureLevel::MEDIUM;
    } else {
        _current_pressure = MemoryPressureLevel::NORMAL;
    }

    _stats.average_utilization = utilization;
}

size_t AdaptiveMemoryManager::invoke_pressure_callbacks(size_t target_bytes) {
    size_t total_reclaimed = 0;

    for (const auto& [name, callback] : _pressure_callbacks) {
        if (total_reclaimed >= target_bytes) break;

        size_t remaining = target_bytes - total_reclaimed;
        size_t reclaimed = callback(remaining);
        total_reclaimed += reclaimed;

        LOG(INFO) << "Pressure callback '" << name << "' reclaimed " << reclaimed << " bytes";
    }

    return total_reclaimed;
}

size_t AdaptiveMemoryManager::predict_query_memory(const AllocationRequest& request) {
    auto& pattern = _workload_patterns[request.type];

    if (pattern.recent_allocations.empty()) {
        return request.bytes_requested;
    }

    // Predict based on historical pattern + safety margin
    double predicted = pattern.average_allocation + pattern.allocation_variance;
    return static_cast<size_t>(std::max(predicted, static_cast<double>(request.bytes_requested)));
}

void AdaptiveMemoryManager::learn_from_query(const std::string& query_id, size_t actual_memory) {
    std::lock_guard<std::mutex> lock(_mutex);

    auto& pattern = _workload_patterns[AllocationRequest::Type::QUERY_EXECUTION];
    pattern.recent_allocations.push_back(actual_memory);

    if (pattern.recent_allocations.size() > 1000) {
        pattern.recent_allocations.erase(pattern.recent_allocations.begin());
    }
}

// MemoryTracker implementation

MemoryTracker::MemoryTracker(const std::string& name, MemoryTracker* parent)
    : _name(name), _parent(parent) {

    if (_parent) {
        _parent->add_child(this);
    }
}

MemoryTracker::~MemoryTracker() {
    if (_parent) {
        _parent->remove_child(this);
    }
}

bool MemoryTracker::try_allocate(size_t bytes) {
    size_t current = _consumption.load();
    if (current + bytes > _limit) {
        return false;
    }

    // Check parent
    if (_parent && !_parent->try_allocate(bytes)) {
        return false;
    }

    _consumption += bytes;
    size_t peak = _peak_consumption.load();
    while (current + bytes > peak) {
        _peak_consumption.compare_exchange_weak(peak, current + bytes);
    }

    return true;
}

void MemoryTracker::allocate(size_t bytes) {
    if (_parent) {
        _parent->allocate(bytes);
    }

    _consumption += bytes;
    size_t current = _consumption.load();
    size_t peak = _peak_consumption.load();
    while (current > peak) {
        _peak_consumption.compare_exchange_weak(peak, current);
    }
}

void MemoryTracker::release(size_t bytes) {
    _consumption -= bytes;
    if (_parent) {
        _parent->release(bytes);
    }
}

void MemoryTracker::set_limit(size_t limit) {
    _limit = limit;
}

void MemoryTracker::add_child(MemoryTracker* child) {
    std::lock_guard<std::mutex> lock(_mutex);
    _children.push_back(child);
}

void MemoryTracker::remove_child(MemoryTracker* child) {
    std::lock_guard<std::mutex> lock(_mutex);
    _children.erase(
        std::remove(_children.begin(), _children.end(), child),
        _children.end()
    );
}

void MemoryTracker::set_spill_callback(std::function<size_t(size_t)> callback) {
    _spill_callback = callback;
}

size_t MemoryTracker::try_spill(size_t target_bytes) {
    if (_spill_callback) {
        return _spill_callback(target_bytes);
    }
    return 0;
}

double MemoryTracker::utilization() const {
    return _limit > 0 ? static_cast<double>(_consumption.load()) / _limit : 0;
}

// ScopedMemoryTracker implementation

ScopedMemoryTracker::ScopedMemoryTracker(MemoryTracker* tracker, size_t bytes)
    : _tracker(tracker), _bytes(bytes), _released(false) {
    if (_tracker) {
        _tracker->allocate(_bytes);
    }
}

ScopedMemoryTracker::~ScopedMemoryTracker() {
    release();
}

ScopedMemoryTracker::ScopedMemoryTracker(ScopedMemoryTracker&& other) noexcept
    : _tracker(other._tracker), _bytes(other._bytes), _released(other._released) {
    other._released = true;
}

ScopedMemoryTracker& ScopedMemoryTracker::operator=(ScopedMemoryTracker&& other) noexcept {
    if (this != &other) {
        release();
        _tracker = other._tracker;
        _bytes = other._bytes;
        _released = other._released;
        other._released = true;
    }
    return *this;
}

void ScopedMemoryTracker::release() {
    if (!_released && _tracker) {
        _tracker->release(_bytes);
        _released = true;
    }
}

// AdaptiveBufferPool implementation

AdaptiveBufferPool::AdaptiveBufferPool(const Config& config) : _config(config) {
    // Initialize size classes (powers of 2 from initial to max)
    size_t size = _config.initial_buffer_size;
    while (size <= _config.max_buffer_size) {
        _size_classes.push_back(SizeClass{
            .buffer_size = size,
            .acquisitions = 0,
            .releases = 0
        });
        size *= 2;
    }
}

AdaptiveBufferPool::~AdaptiveBufferPool() {
    std::lock_guard<std::mutex> lock(_mutex);
    for (auto& sc : _size_classes) {
        while (!sc.free_buffers.empty()) {
            free(sc.free_buffers.front());
            sc.free_buffers.pop();
        }
    }
}

void* AdaptiveBufferPool::acquire(size_t size) {
    std::lock_guard<std::mutex> lock(_mutex);

    size_t idx = get_size_class_index(size);
    auto& sc = _size_classes[idx];
    sc.acquisitions++;

    if (!sc.free_buffers.empty()) {
        void* buffer = sc.free_buffers.front();
        sc.free_buffers.pop();
        _total_pooled_bytes -= sc.buffer_size;
        return buffer;
    }

    // Allocate new buffer
    return malloc(sc.buffer_size);
}

void AdaptiveBufferPool::release(void* buffer, size_t size) {
    std::lock_guard<std::mutex> lock(_mutex);

    size_t idx = get_size_class_index(size);
    auto& sc = _size_classes[idx];
    sc.releases++;

    if (_total_pooled_bytes + sc.buffer_size <= _config.max_pool_size) {
        sc.free_buffers.push(buffer);
        _total_pooled_bytes += sc.buffer_size;
    } else {
        free(buffer);
    }
}

void AdaptiveBufferPool::adapt() {
    std::lock_guard<std::mutex> lock(_mutex);

    for (auto& sc : _size_classes) {
        double utilization = sc.acquisitions > 0 ?
            static_cast<double>(sc.acquisitions - sc.free_buffers.size()) / sc.acquisitions : 0;

        if (utilization < _config.shrink_threshold && !sc.free_buffers.empty()) {
            // Low utilization, free some buffers
            size_t to_free = sc.free_buffers.size() / 2;
            for (size_t i = 0; i < to_free; ++i) {
                free(sc.free_buffers.front());
                sc.free_buffers.pop();
                _total_pooled_bytes -= sc.buffer_size;
            }
        }

        // Reset counters for next period
        sc.acquisitions = 0;
        sc.releases = 0;
    }
}

size_t AdaptiveBufferPool::get_size_class_index(size_t size) const {
    for (size_t i = 0; i < _size_classes.size(); ++i) {
        if (size <= _size_classes[i].buffer_size) {
            return i;
        }
    }
    return _size_classes.size() - 1;
}

AdaptiveBufferPool::Stats AdaptiveBufferPool::get_stats() const {
    std::lock_guard<std::mutex> lock(_mutex);

    Stats stats;
    stats.current_pool_size = _total_pooled_bytes;

    for (const auto& sc : _size_classes) {
        stats.total_acquisitions += sc.acquisitions;
        stats.pool_hits += std::min(sc.acquisitions, static_cast<size_t>(sc.free_buffers.size()));
        stats.buffers_in_use += sc.acquisitions - sc.free_buffers.size();
    }

    stats.pool_misses = stats.total_acquisitions - stats.pool_hits;

    return stats;
}

// QueryMemoryController implementation

QueryMemoryController::QueryMemoryController(const std::string& query_id, size_t memory_limit)
    : _query_id(query_id), _memory_limit(memory_limit) {}

QueryMemoryController::~QueryMemoryController() {
    AdaptiveMemoryManager::instance().unregister_query(_query_id);
}

bool QueryMemoryController::try_allocate(size_t bytes) {
    size_t current = _memory_used.load();
    if (current + bytes > _memory_limit) {
        std::lock_guard<std::mutex> lock(_mutex);
        _stats.denial_count++;
        return false;
    }

    _memory_used += bytes;

    {
        std::lock_guard<std::mutex> lock(_mutex);
        _stats.allocation_count++;
    }

    // Update peak
    size_t peak = _peak_memory.load();
    while (current + bytes > peak) {
        _peak_memory.compare_exchange_weak(peak, current + bytes);
    }

    return true;
}

void QueryMemoryController::release(size_t bytes) {
    _memory_used -= bytes;
}

size_t QueryMemoryController::memory_available() const {
    size_t used = _memory_used.load();
    return _memory_limit > used ? _memory_limit - used : 0;
}

void QueryMemoryController::set_spill_threshold(double ratio) {
    _spill_threshold = ratio;
}

bool QueryMemoryController::should_spill() const {
    return static_cast<double>(_memory_used.load()) / _memory_limit >= _spill_threshold;
}

void QueryMemoryController::mark_spilled(size_t bytes) {
    _bytes_spilled += bytes;
    std::lock_guard<std::mutex> lock(_mutex);
    _stats.bytes_spilled = _bytes_spilled.load();
}

QueryMemoryController::Stats QueryMemoryController::get_stats() const {
    std::lock_guard<std::mutex> lock(_mutex);
    Stats stats = _stats;
    stats.peak_memory = _peak_memory.load();
    return stats;
}

} // namespace starrocks
