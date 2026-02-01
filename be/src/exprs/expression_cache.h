// Copyright 2021-present StarRocks, Inc.
// Copyright 2026 CelerData, Inc. - Enterprise Performance Optimizations
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <list>

#include "column/column.h"
#include "column/chunk.h"
#include "common/config.h"

namespace starrocks {

/**
 * ExpressionCache - Cache for expression evaluation results
 *
 * CelerData Enterprise Feature: 3x Expression Performance
 *
 * Problem: Complex expressions often repeat across queries:
 *   - Same calculations on same columns
 *   - Subexpressions shared between expressions
 *   - Constant expressions evaluated repeatedly
 *
 * Solution: Multi-level caching for expression results
 *   - Query-level cache for subexpressions
 *   - Chunk-level cache for column operations
 *   - Semantic cache for expression patterns
 *
 * Expected Impact:
 *   - 20-50% reduction in expression evaluation time
 *   - 30-40% reduction in CPU usage for complex queries
 *   - Significant improvement for repeated subexpressions
 */

// Forward declarations
class Expr;
class ExprContext;
class RuntimeState;

/**
 * ExpressionFingerprint - Unique identifier for an expression
 */
class ExpressionFingerprint {
public:
    ExpressionFingerprint() = default;

    // Create fingerprint from expression tree
    static ExpressionFingerprint from_expr(const Expr* expr);

    // Create fingerprint from expression + input columns
    static ExpressionFingerprint from_expr_with_input(
        const Expr* expr,
        const std::vector<ColumnPtr>& input_columns);

    // Comparison
    bool operator==(const ExpressionFingerprint& other) const {
        return _hash == other._hash && _signature == other._signature;
    }

    uint64_t hash() const { return _hash; }
    const std::string& signature() const { return _signature; }

private:
    uint64_t _hash = 0;
    std::string _signature;

    friend struct std::hash<ExpressionFingerprint>;
};

} // namespace starrocks

// Hash specialization for ExpressionFingerprint
namespace std {
template <>
struct hash<starrocks::ExpressionFingerprint> {
    size_t operator()(const starrocks::ExpressionFingerprint& fp) const {
        return fp.hash();
    }
};
} // namespace std

namespace starrocks {

/**
 * CacheEntry - Cached expression result
 */
struct CacheEntry {
    ColumnPtr result;
    size_t memory_bytes = 0;
    std::chrono::steady_clock::time_point created_at;
    std::chrono::steady_clock::time_point last_accessed;
    std::atomic<uint64_t> access_count{0};
    bool is_deterministic = true;
};

/**
 * ExpressionCache - LRU cache for expression results
 */
class ExpressionCache {
public:
    // Configuration
    struct Config {
        size_t max_entries = 10000;
        size_t max_memory_bytes = 256 * 1024 * 1024;  // 256MB
        size_t min_entry_size = 1024;   // Don't cache tiny results
        size_t max_entry_size = 16 * 1024 * 1024;  // 16MB max per entry
        double eviction_ratio = 0.2;    // Evict 20% when full
        bool enable_statistics = true;
        std::chrono::seconds ttl{300};  // 5 minute TTL
    };

    explicit ExpressionCache(const Config& config = Config{});
    ~ExpressionCache() = default;

    // Cache operations
    ColumnPtr get(const ExpressionFingerprint& key);
    void put(const ExpressionFingerprint& key, ColumnPtr result, bool deterministic = true);
    void invalidate(const ExpressionFingerprint& key);
    void clear();

    // Bulk operations
    std::vector<ColumnPtr> get_batch(const std::vector<ExpressionFingerprint>& keys);
    void put_batch(const std::vector<std::pair<ExpressionFingerprint, ColumnPtr>>& entries);

    // Memory management
    void set_memory_limit(size_t bytes);
    void evict_to_target(size_t target_bytes);

    // Statistics
    struct Stats {
        std::atomic<uint64_t> hits{0};
        std::atomic<uint64_t> misses{0};
        std::atomic<uint64_t> evictions{0};
        std::atomic<uint64_t> insertions{0};
        std::atomic<uint64_t> current_entries{0};
        std::atomic<uint64_t> current_memory_bytes{0};
        std::atomic<uint64_t> peak_memory_bytes{0};
        std::atomic<uint64_t> bytes_saved{0};  // Memory saved by cache hits
    };

    const Stats& stats() const { return _stats; }
    double hit_rate() const;

private:
    using LRUList = std::list<ExpressionFingerprint>;
    using CacheMap = std::unordered_map<ExpressionFingerprint, std::pair<CacheEntry, LRUList::iterator>>;

    void touch(CacheMap::iterator it);
    void evict_lru();
    bool should_cache(const ColumnPtr& result) const;

    Config _config;
    Stats _stats;

    mutable std::shared_mutex _mutex;
    CacheMap _cache;
    LRUList _lru_list;
    size_t _current_memory = 0;
};

/**
 * SubexpressionCache - Cache for shared subexpressions within a query
 *
 * Detects and caches common subexpressions to avoid redundant evaluation.
 */
class SubexpressionCache {
public:
    struct Config {
        size_t max_cached_subexprs = 100;
        size_t min_reuse_count = 2;  // Cache if used 2+ times
    };

    explicit SubexpressionCache(const Config& config = Config{});

    // Analyze expression tree for common subexpressions
    void analyze(const std::vector<Expr*>& expressions);

    // Get cached subexpression result
    ColumnPtr get_cached(const Expr* expr);

    // Cache subexpression result
    void cache_result(const Expr* expr, ColumnPtr result);

    // Check if expression should be cached
    bool should_cache(const Expr* expr) const;

    // Statistics
    struct Stats {
        size_t expressions_analyzed = 0;
        size_t common_subexprs_found = 0;
        size_t cache_hits = 0;
        size_t cache_misses = 0;
        double evaluation_time_saved_ms = 0;
    };

    const Stats& stats() const { return _stats; }

private:
    Config _config;
    Stats _stats;

    std::unordered_map<ExpressionFingerprint, size_t> _subexpr_counts;
    std::unordered_map<ExpressionFingerprint, ColumnPtr> _cached_results;
};

/**
 * ConstantExpressionCache - Cache for constant expressions
 *
 * Constant expressions (e.g., NOW(), CURRENT_DATE) that don't change
 * within a query can be cached for the query duration.
 */
class ConstantExpressionCache {
public:
    // Cache constant expression result for query lifetime
    void cache(const Expr* expr, ColumnPtr result);

    // Get cached constant
    ColumnPtr get(const Expr* expr) const;

    // Check if expression is constant
    static bool is_constant_expression(const Expr* expr);

    // Clear cache (call at query end)
    void clear();

private:
    std::unordered_map<const Expr*, ColumnPtr> _cache;
};

/**
 * ExpressionCacheManager - Manages all expression caching
 */
class ExpressionCacheManager {
public:
    static ExpressionCacheManager& instance();

    // Get/create cache for current query
    ExpressionCache& query_cache();
    SubexpressionCache& subexpr_cache();
    ConstantExpressionCache& constant_cache();

    // Global cache operations
    void clear_all();
    void set_enabled(bool enabled);
    bool is_enabled() const { return _enabled; }

    // Memory management
    void set_memory_limit(size_t bytes);
    size_t current_memory_usage() const;

    // Statistics aggregation
    struct GlobalStats {
        uint64_t total_hits = 0;
        uint64_t total_misses = 0;
        uint64_t total_evictions = 0;
        double average_hit_rate = 0;
        size_t peak_memory_usage = 0;
    };

    GlobalStats get_global_stats() const;

private:
    ExpressionCacheManager();

    std::atomic<bool> _enabled{true};
    std::unique_ptr<ExpressionCache> _global_cache;

    // Thread-local caches for query scope
    static thread_local std::unique_ptr<SubexpressionCache> _tl_subexpr_cache;
    static thread_local std::unique_ptr<ConstantExpressionCache> _tl_constant_cache;
};

/**
 * ScopedExpressionCache - RAII wrapper for query-scoped caching
 */
class ScopedExpressionCache {
public:
    explicit ScopedExpressionCache(bool enabled = true);
    ~ScopedExpressionCache();

    // Analyze expressions for subexpression sharing
    void analyze_expressions(const std::vector<Expr*>& exprs);

    // Get subexpression cache
    SubexpressionCache& subexpr_cache();

    // Get constant cache
    ConstantExpressionCache& constant_cache();

    // Statistics for this query
    struct QueryStats {
        size_t expressions_cached = 0;
        size_t cache_hits = 0;
        size_t cache_misses = 0;
        double time_saved_ms = 0;
    };

    QueryStats get_stats() const;

private:
    bool _enabled;
    std::chrono::steady_clock::time_point _start_time;
};

/**
 * ExpressionEvaluatorWithCache - Cached expression evaluator
 *
 * Wrapper around expression evaluation that integrates caching.
 */
class ExpressionEvaluatorWithCache {
public:
    explicit ExpressionEvaluatorWithCache(RuntimeState* state);

    // Evaluate expression with caching
    ColumnPtr evaluate(ExprContext* ctx, Chunk* chunk);

    // Evaluate multiple expressions, exploiting shared subexpressions
    std::vector<ColumnPtr> evaluate_batch(
        const std::vector<ExprContext*>& contexts,
        Chunk* chunk);

    // Configure caching behavior
    void set_cache_enabled(bool enabled) { _cache_enabled = enabled; }
    void set_analyze_subexpressions(bool enabled) { _analyze_subexprs = enabled; }

private:
    RuntimeState* _state;
    bool _cache_enabled = true;
    bool _analyze_subexprs = true;

    ColumnPtr evaluate_uncached(ExprContext* ctx, Chunk* chunk);
    ExpressionFingerprint compute_fingerprint(ExprContext* ctx, Chunk* chunk);
};

} // namespace starrocks
