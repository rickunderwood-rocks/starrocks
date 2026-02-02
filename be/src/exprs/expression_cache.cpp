// Copyright 2021-present StarRocks, Inc.
// Copyright 2026 CelerData, Inc. - Enterprise Performance Optimizations
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

#include "exprs/expression_cache.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"
#include "util/logging.h"

#include <algorithm>
#include <sstream>

namespace starrocks {

// Thread-local caches
thread_local std::unique_ptr<SubexpressionCache> ExpressionCacheManager::_tl_subexpr_cache;
thread_local std::unique_ptr<ConstantExpressionCache> ExpressionCacheManager::_tl_constant_cache;

// ExpressionFingerprint implementation

ExpressionFingerprint ExpressionFingerprint::from_expr(const Expr* expr) {
    ExpressionFingerprint fp;

    if (!expr) {
        return fp;
    }

    // Build signature from expression tree structure
    std::ostringstream oss;
    oss << static_cast<int>(expr->type().type);
    oss << ":" << expr->node_type();

    // Include children
    for (size_t i = 0; i < expr->get_num_children(); ++i) {
        auto child_fp = from_expr(expr->get_child(i));
        oss << "[" << child_fp.signature() << "]";
    }

    fp._signature = oss.str();

    // Compute hash using FNV-1a
    uint64_t hash = 14695981039346656037ULL;
    for (char c : fp._signature) {
        hash ^= static_cast<uint64_t>(c);
        hash *= 1099511628211ULL;
    }
    fp._hash = hash;

    return fp;
}

ExpressionFingerprint ExpressionFingerprint::from_expr_with_input(
    const Expr* expr,
    const std::vector<ColumnPtr>& input_columns) {

    auto fp = from_expr(expr);

    // Extend fingerprint with input column hashes
    std::ostringstream oss;
    oss << fp._signature;

    for (const auto& col : input_columns) {
        if (col) {
            // Use column address as part of fingerprint (column identity)
            oss << ":" << reinterpret_cast<uintptr_t>(col.get());
            oss << ":" << col->size();
        }
    }

    fp._signature = oss.str();

    // Recompute hash
    uint64_t hash = 14695981039346656037ULL;
    for (char c : fp._signature) {
        hash ^= static_cast<uint64_t>(c);
        hash *= 1099511628211ULL;
    }
    fp._hash = hash;

    return fp;
}

// ExpressionCache implementation

ExpressionCache::ExpressionCache(const Config& config)
    : _config(config) {}

ColumnPtr ExpressionCache::get(const ExpressionFingerprint& key) {
    // First check with shared lock - only read operations
    {
        std::shared_lock<std::shared_mutex> lock(_mutex);

        auto it = _cache.find(key);
        if (it == _cache.end()) {
            _stats.misses.fetch_add(1, std::memory_order_relaxed);
            return nullptr;
        }

        auto& entry = it->second.first;

        // Check TTL
        auto now = std::chrono::steady_clock::now();
        if (now - entry.created_at > _config.ttl) {
            _stats.misses.fetch_add(1, std::memory_order_relaxed);
            // Don't evict here (holding shared lock), just return miss
            return nullptr;
        }

        // Valid entry - return result with atomic stats updates only
        auto result = entry.result;
        _stats.hits.fetch_add(1, std::memory_order_relaxed);
        _stats.bytes_saved.fetch_add(entry.memory_bytes, std::memory_order_relaxed);

        return result;
    }
    // Note: entry.last_accessed and access_count updates deferred for performance
    // Consider using exclusive lock if precise tracking is required
}

void ExpressionCache::put(const ExpressionFingerprint& key, ColumnPtr result, bool deterministic) {
    if (!should_cache(result)) {
        return;
    }

    std::unique_lock<std::shared_mutex> lock(_mutex);

    // Check if already cached
    auto it = _cache.find(key);
    if (it != _cache.end()) {
        return;  // Already cached
    }

    size_t entry_size = result->memory_usage();

    // Evict if necessary
    while (_current_memory + entry_size > _config.max_memory_bytes ||
           _cache.size() >= _config.max_entries) {
        if (_cache.empty()) break;
        evict_lru();
    }

    // Insert new entry
    CacheEntry entry;
    entry.result = result;
    entry.memory_bytes = entry_size;
    entry.created_at = std::chrono::steady_clock::now();
    entry.last_accessed = entry.created_at;
    entry.is_deterministic = deterministic;

    _lru_list.push_front(key);
    _cache[key] = {std::move(entry), _lru_list.begin()};

    _current_memory += entry_size;
    _stats.insertions++;
    _stats.current_entries = _cache.size();
    _stats.current_memory_bytes = _current_memory;

    if (_current_memory > _stats.peak_memory_bytes) {
        _stats.peak_memory_bytes.store(_current_memory);
    }
}

void ExpressionCache::invalidate(const ExpressionFingerprint& key) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    auto it = _cache.find(key);
    if (it != _cache.end()) {
        _current_memory -= it->second.first.memory_bytes;
        _lru_list.erase(it->second.second);
        _cache.erase(it);
        _stats.current_entries = _cache.size();
        _stats.current_memory_bytes = _current_memory;
    }
}

void ExpressionCache::clear() {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    _cache.clear();
    _lru_list.clear();
    _current_memory = 0;
    _stats.current_entries = 0;
    _stats.current_memory_bytes = 0;
}

std::vector<ColumnPtr> ExpressionCache::get_batch(const std::vector<ExpressionFingerprint>& keys) {
    std::vector<ColumnPtr> results;
    results.reserve(keys.size());

    std::shared_lock<std::shared_mutex> lock(_mutex);

    for (const auto& key : keys) {
        auto it = _cache.find(key);
        if (it != _cache.end()) {
            auto& entry = it->second.first;
            auto now = std::chrono::steady_clock::now();

            if (now - entry.created_at <= _config.ttl) {
                entry.last_accessed = now;
                entry.access_count++;
                _stats.hits++;
                _stats.bytes_saved += entry.memory_bytes;
                results.push_back(entry.result);
                continue;
            }
        }
        _stats.misses++;
        results.push_back(nullptr);
    }

    return results;
}

void ExpressionCache::put_batch(
    const std::vector<std::pair<ExpressionFingerprint, ColumnPtr>>& entries) {

    std::unique_lock<std::shared_mutex> lock(_mutex);

    for (const auto& [key, result] : entries) {
        if (!should_cache(result) || _cache.count(key)) {
            continue;
        }

        size_t entry_size = result->memory_usage();

        while (_current_memory + entry_size > _config.max_memory_bytes ||
               _cache.size() >= _config.max_entries) {
            if (_cache.empty()) break;
            evict_lru();
        }

        CacheEntry entry;
        entry.result = result;
        entry.memory_bytes = entry_size;
        entry.created_at = std::chrono::steady_clock::now();
        entry.last_accessed = entry.created_at;

        _lru_list.push_front(key);
        _cache[key] = {std::move(entry), _lru_list.begin()};
        _current_memory += entry_size;
        _stats.insertions++;
    }

    _stats.current_entries = _cache.size();
    _stats.current_memory_bytes = _current_memory;
}

void ExpressionCache::set_memory_limit(size_t bytes) {
    _config.max_memory_bytes = bytes;
    evict_to_target(bytes);
}

void ExpressionCache::evict_to_target(size_t target_bytes) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    while (_current_memory > target_bytes && !_cache.empty()) {
        evict_lru();
    }
}

bool ExpressionCache::should_cache(const ColumnPtr& result) const {
    if (!result) return false;

    size_t size = result->memory_usage();
    return size >= _config.min_entry_size && size <= _config.max_entry_size;
}

void ExpressionCache::touch(CacheMap::iterator it) {
    // Move to front of LRU list
    _lru_list.erase(it->second.second);
    _lru_list.push_front(it->first);
    it->second.second = _lru_list.begin();
}

void ExpressionCache::evict_lru() {
    if (_lru_list.empty()) return;

    auto key = _lru_list.back();
    _lru_list.pop_back();

    auto it = _cache.find(key);
    if (it != _cache.end()) {
        _current_memory -= it->second.first.memory_bytes;
        _cache.erase(it);
        _stats.evictions++;
    }
}

void ExpressionCache::evict_expired_entries() {
    // Called periodically to remove stale TTL entries
    std::unique_lock<std::shared_mutex> lock(_mutex);

    auto now = std::chrono::steady_clock::now();
    std::vector<ExpressionFingerprint> expired_keys;

    for (auto it = _cache.begin(); it != _cache.end(); ++it) {
        const auto& entry = it->second.first;
        if (now - entry.created_at > _config.ttl) {
            expired_keys.push_back(it->first);
        }
    }

    for (const auto& key : expired_keys) {
        auto it = _cache.find(key);
        if (it != _cache.end()) {
            _current_memory -= it->second.first.memory_bytes;
            _lru_list.erase(it->second.second);
            _cache.erase(it);
            _stats.evictions++;
        }
    }

    _stats.current_entries = _cache.size();
    _stats.current_memory_bytes = _current_memory;
}

double ExpressionCache::hit_rate() const {
    uint64_t hits = _stats.hits.load();
    uint64_t total = hits + _stats.misses.load();
    return total > 0 ? static_cast<double>(hits) / total : 0.0;
}

// SubexpressionCache implementation

SubexpressionCache::SubexpressionCache(const Config& config)
    : _config(config) {}

void SubexpressionCache::analyze(const std::vector<Expr*>& expressions) {
    _subexpr_counts.clear();
    _stats.expressions_analyzed = expressions.size();

    // Count subexpressions
    std::function<void(const Expr*)> count_subexprs = [&](const Expr* expr) {
        if (!expr) return;

        auto fp = ExpressionFingerprint::from_expr(expr);
        _subexpr_counts[fp]++;

        for (size_t i = 0; i < expr->get_num_children(); ++i) {
            count_subexprs(expr->get_child(i));
        }
    };

    for (const auto* expr : expressions) {
        count_subexprs(expr);
    }

    // Count common subexpressions
    for (const auto& [fp, count] : _subexpr_counts) {
        if (count >= _config.min_reuse_count) {
            _stats.common_subexprs_found++;
        }
    }

    LOG(INFO) << "SubexpressionCache analyzed " << expressions.size()
              << " expressions, found " << _stats.common_subexprs_found
              << " common subexpressions";
}

ColumnPtr SubexpressionCache::get_cached(const Expr* expr) {
    auto fp = ExpressionFingerprint::from_expr(expr);
    auto it = _cached_results.find(fp);

    if (it != _cached_results.end()) {
        _stats.cache_hits++;
        return it->second;
    }

    _stats.cache_misses++;
    return nullptr;
}

void SubexpressionCache::cache_result(const Expr* expr, ColumnPtr result) {
    if (_cached_results.size() >= _config.max_cached_subexprs) {
        return;
    }

    auto fp = ExpressionFingerprint::from_expr(expr);
    _cached_results[fp] = result;
}

bool SubexpressionCache::should_cache(const Expr* expr) const {
    auto fp = ExpressionFingerprint::from_expr(expr);
    auto it = _subexpr_counts.find(fp);
    return it != _subexpr_counts.end() && it->second >= _config.min_reuse_count;
}

// ConstantExpressionCache implementation

void ConstantExpressionCache::cache(const Expr* expr, ColumnPtr result) {
    _cache[expr] = result;
}

ColumnPtr ConstantExpressionCache::get(const Expr* expr) const {
    auto it = _cache.find(expr);
    return it != _cache.end() ? it->second : nullptr;
}

bool ConstantExpressionCache::is_constant_expression(const Expr* expr) {
    if (!expr) return false;
    return expr->is_constant();
}

void ConstantExpressionCache::clear() {
    _cache.clear();
}

// ExpressionCacheManager implementation

ExpressionCacheManager& ExpressionCacheManager::instance() {
    static ExpressionCacheManager instance;
    return instance;
}

ExpressionCacheManager::ExpressionCacheManager() {
    ExpressionCache::Config config;
    _global_cache = std::make_unique<ExpressionCache>(config);
}

ExpressionCache& ExpressionCacheManager::query_cache() {
    return *_global_cache;
}

SubexpressionCache& ExpressionCacheManager::subexpr_cache() {
    if (!_tl_subexpr_cache) {
        _tl_subexpr_cache = std::make_unique<SubexpressionCache>();
    }
    return *_tl_subexpr_cache;
}

ConstantExpressionCache& ExpressionCacheManager::constant_cache() {
    if (!_tl_constant_cache) {
        _tl_constant_cache = std::make_unique<ConstantExpressionCache>();
    }
    return *_tl_constant_cache;
}

void ExpressionCacheManager::clear_all() {
    _global_cache->clear();
    if (_tl_subexpr_cache) {
        _tl_subexpr_cache = std::make_unique<SubexpressionCache>();
    }
    if (_tl_constant_cache) {
        _tl_constant_cache = std::make_unique<ConstantExpressionCache>();
    }
}

void ExpressionCacheManager::set_enabled(bool enabled) {
    _enabled = enabled;
}

void ExpressionCacheManager::set_memory_limit(size_t bytes) {
    _global_cache->set_memory_limit(bytes);
}

size_t ExpressionCacheManager::current_memory_usage() const {
    return _global_cache->stats().current_memory_bytes.load();
}

ExpressionCacheManager::GlobalStats ExpressionCacheManager::get_global_stats() const {
    GlobalStats stats;
    const auto& cache_stats = _global_cache->stats();

    stats.total_hits = cache_stats.hits.load();
    stats.total_misses = cache_stats.misses.load();
    stats.total_evictions = cache_stats.evictions.load();
    stats.average_hit_rate = _global_cache->hit_rate();
    stats.peak_memory_usage = cache_stats.peak_memory_bytes.load();

    return stats;
}

// ScopedExpressionCache implementation

ScopedExpressionCache::ScopedExpressionCache(bool enabled)
    : _enabled(enabled), _start_time(std::chrono::steady_clock::now()) {

    if (_enabled) {
        // Initialize thread-local caches
        ExpressionCacheManager::instance().subexpr_cache();
        ExpressionCacheManager::instance().constant_cache();
    }
}

ScopedExpressionCache::~ScopedExpressionCache() {
    if (_enabled) {
        // Clear thread-local caches
        ExpressionCacheManager::_tl_subexpr_cache.reset();
        ExpressionCacheManager::_tl_constant_cache.reset();
    }
}

void ScopedExpressionCache::analyze_expressions(const std::vector<Expr*>& exprs) {
    if (_enabled) {
        ExpressionCacheManager::instance().subexpr_cache().analyze(exprs);
    }
}

SubexpressionCache& ScopedExpressionCache::subexpr_cache() {
    return ExpressionCacheManager::instance().subexpr_cache();
}

ConstantExpressionCache& ScopedExpressionCache::constant_cache() {
    return ExpressionCacheManager::instance().constant_cache();
}

ScopedExpressionCache::QueryStats ScopedExpressionCache::get_stats() const {
    QueryStats stats;

    if (_enabled) {
        const auto& subexpr_stats = ExpressionCacheManager::instance().subexpr_cache().stats();
        stats.cache_hits = subexpr_stats.cache_hits;
        stats.cache_misses = subexpr_stats.cache_misses;
    }

    return stats;
}

// ExpressionEvaluatorWithCache implementation

ExpressionEvaluatorWithCache::ExpressionEvaluatorWithCache(RuntimeState* state)
    : _state(state) {}

ColumnPtr ExpressionEvaluatorWithCache::evaluate(ExprContext* ctx, Chunk* chunk) {
    if (!_cache_enabled || !ExpressionCacheManager::instance().is_enabled()) {
        return evaluate_uncached(ctx, chunk);
    }

    // Check constant cache first
    if (ConstantExpressionCache::is_constant_expression(ctx->root())) {
        auto cached = ExpressionCacheManager::instance().constant_cache().get(ctx->root());
        if (cached) {
            return cached;
        }
    }

    // Compute fingerprint
    auto fp = compute_fingerprint(ctx, chunk);

    // Check query cache
    auto cached = ExpressionCacheManager::instance().query_cache().get(fp);
    if (cached) {
        return cached;
    }

    // Check subexpression cache
    auto& subexpr_cache = ExpressionCacheManager::instance().subexpr_cache();
    cached = subexpr_cache.get_cached(ctx->root());
    if (cached) {
        return cached;
    }

    // Evaluate
    auto result = evaluate_uncached(ctx, chunk);

    // Cache result
    if (result) {
        if (ConstantExpressionCache::is_constant_expression(ctx->root())) {
            ExpressionCacheManager::instance().constant_cache().cache(ctx->root(), result);
        } else if (subexpr_cache.should_cache(ctx->root())) {
            subexpr_cache.cache_result(ctx->root(), result);
        }

        ExpressionCacheManager::instance().query_cache().put(fp, result);
    }

    return result;
}

std::vector<ColumnPtr> ExpressionEvaluatorWithCache::evaluate_batch(
    const std::vector<ExprContext*>& contexts,
    Chunk* chunk) {

    std::vector<ColumnPtr> results;
    results.reserve(contexts.size());

    // Analyze for shared subexpressions
    if (_analyze_subexprs && _cache_enabled) {
        std::vector<Expr*> exprs;
        exprs.reserve(contexts.size());
        for (auto* ctx : contexts) {
            exprs.push_back(ctx->root());
        }
        ExpressionCacheManager::instance().subexpr_cache().analyze(exprs);
    }

    // Evaluate each expression
    for (auto* ctx : contexts) {
        results.push_back(evaluate(ctx, chunk));
    }

    return results;
}

ColumnPtr ExpressionEvaluatorWithCache::evaluate_uncached(ExprContext* ctx, Chunk* chunk) {
    // Standard evaluation - this would call the actual expression evaluation
    // For now, return the result of evaluating the expression context
    return ctx->evaluate(chunk);
}

ExpressionFingerprint ExpressionEvaluatorWithCache::compute_fingerprint(
    ExprContext* ctx, Chunk* chunk) {

    std::vector<ColumnPtr> inputs;

    // Collect input columns referenced by expression
    // This is a simplified version - full implementation would traverse the expression tree
    for (size_t i = 0; i < chunk->num_columns(); ++i) {
        inputs.push_back(chunk->get_column_by_index(i));
    }

    return ExpressionFingerprint::from_expr_with_input(ctx->root(), inputs);
}

} // namespace starrocks
