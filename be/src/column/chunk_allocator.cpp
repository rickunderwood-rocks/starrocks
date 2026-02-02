// Copyright 2021-present StarRocks, Inc.
// Copyright 2026 CelerData, Inc. - Enterprise Performance Optimizations
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

#include "column/chunk_allocator.h"
#include "runtime/memory/memory_pressure_manager.h"
#include "util/logging.h"

namespace starrocks {

// Thread-local cache definition
thread_local ChunkAllocator::ThreadLocalCache ChunkAllocator::_tl_cache;

ChunkAllocator& ChunkAllocator::instance() {
    static ChunkAllocator instance;
    return instance;
}

ChunkAllocator::ChunkAllocator() {
    // Initialize with default config
    configure(Config{});
}

ChunkAllocator::~ChunkAllocator() {
    clear();
}

void ChunkAllocator::configure(const Config& config) {
    _config = config;
    _tl_cache.max_size = config.thread_local_cache_size;
}

size_t ChunkAllocator::get_size_class(size_t row_capacity) const {
    for (size_t i = 0; i < 8; ++i) {
        if (row_capacity <= _config.chunk_size_classes[i]) {
            return i;
        }
    }
    return 7;  // Largest class for oversized requests
}

size_t ChunkAllocator::size_class_capacity(size_t size_class) const {
    return _config.chunk_size_classes[std::min(size_class, size_t(7))];
}

ChunkPtr ChunkAllocator::try_allocate_from_tl_cache(size_t size_class) {
    if (!_config.enable_thread_local_cache) {
        return nullptr;
    }

    for (auto it = _tl_cache.chunks.begin(); it != _tl_cache.chunks.end(); ++it) {
        if (it->first == size_class && it->second) {
            ChunkPtr chunk = std::move(it->second);
            _tl_cache.chunks.erase(it);
            return chunk;
        }
    }
    return nullptr;
}

void ChunkAllocator::try_return_to_tl_cache(ChunkPtr chunk, size_t size_class) {
    if (!_config.enable_thread_local_cache) {
        return;
    }

    if (_tl_cache.chunks.size() < _tl_cache.max_size) {
        _tl_cache.chunks.emplace_back(size_class, std::move(chunk));
    }
}

ChunkAllocator::PooledChunk ChunkAllocator::allocate(size_t row_capacity) {
    _stats.allocations++;
    size_t size_class = get_size_class(row_capacity);
    size_t actual_capacity = size_class_capacity(size_class);

    // Fast path: thread-local cache
    ChunkPtr chunk = try_allocate_from_tl_cache(size_class);
    if (chunk) {
        _stats.pool_hits++;
        chunk->reset();  // Clear data but keep capacity
        return PooledChunk(std::move(chunk), this, size_class);
    }

    // Medium path: global pool
    {
        auto& pool = _pools[size_class];
        std::lock_guard<std::mutex> lock(pool.mutex);
        if (!pool.chunks.empty()) {
            chunk = std::move(pool.chunks.front());
            pool.chunks.pop();
            pool.count--;
            size_t chunk_bytes = chunk->memory_usage();
            pool.total_bytes -= chunk_bytes;
            _stats.current_pool_size_bytes -= chunk_bytes;
            _stats.pool_hits++;
            _stats.bytes_saved += chunk_bytes;
            chunk->reset();
            return PooledChunk(std::move(chunk), this, size_class);
        }
    }

    // Slow path: allocate new chunk
    _stats.pool_misses++;
    chunk = std::make_shared<Chunk>();
    chunk->reserve(actual_capacity);
    return PooledChunk(std::move(chunk), this, size_class);
}

ChunkAllocator::PooledChunk ChunkAllocator::allocate(size_t row_capacity, const Schema& schema) {
    auto pooled = allocate(row_capacity);

    // Initialize columns according to schema
    Chunk* chunk = pooled.get();
    if (!chunk) {
        LOG(ERROR) << "Failed to allocate chunk with row capacity: " << row_capacity;
        return pooled;  // Return empty PooledChunk
    }

    for (size_t i = 0; i < schema.num_fields(); ++i) {
        auto field = schema.field(i);
        auto column = ColumnHelper::create_column(field->type(), field->is_nullable());
        if (!column) {
            LOG(ERROR) << "Failed to create column for field: " << field->id();
            continue;  // Skip this column and continue
        }
        column->reserve(row_capacity);
        chunk->append_column(std::move(column), field->id());
    }

    return pooled;
}

void ChunkAllocator::deallocate(ChunkPtr chunk, size_t size_class) {
    if (!chunk) return;

    _stats.deallocations++;

    // Check memory pressure before pooling
    check_memory_pressure();

    size_t chunk_bytes = chunk->memory_usage();

    // Try thread-local cache first
    if (_config.enable_thread_local_cache &&
        _tl_cache.chunks.size() < _tl_cache.max_size) {
        chunk->reset();
        try {
            _tl_cache.chunks.emplace_back(size_class, std::move(chunk));
            return;
        } catch (...) {
            // If thread-local cache insert fails, fall through to global pool
            LOG(WARNING) << "Failed to insert chunk into thread-local cache";
        }
    }

    // Check if pool is at capacity
    auto& pool = _pools[size_class];
    if (pool.count >= _config.max_cached_chunks_per_class ||
        _stats.current_pool_size_bytes + chunk_bytes > _config.max_pool_size_bytes) {
        // Let chunk deallocate naturally
        return;
    }

    // Add to global pool
    {
        std::lock_guard<std::mutex> lock(pool.mutex);
        if (pool.count < _config.max_cached_chunks_per_class) {
            chunk->reset();
            pool.chunks.push(std::move(chunk));
            pool.count++;
            pool.total_bytes += chunk_bytes;
            _stats.current_pool_size_bytes += chunk_bytes;

            // Track peak
            uint64_t current = _stats.current_pool_size_bytes.load();
            uint64_t peak = _stats.peak_pool_size_bytes.load();
            while (current > peak &&
                   !_stats.peak_pool_size_bytes.compare_exchange_weak(peak, current)) {
                // Retry until successful
            }
        }
    }
}

void ChunkAllocator::clear() {
    // Clear thread-local caches (current thread only)
    // Explicitly destructing chunks to free their memory
    for (auto& pair : _tl_cache.chunks) {
        pair.second.reset();  // Release shared_ptr ownership
    }
    _tl_cache.chunks.clear();

    // Clear global pools
    for (auto& pool : _pools) {
        std::lock_guard<std::mutex> lock(pool.mutex);
        while (!pool.chunks.empty()) {
            pool.chunks.pop();
        }
        pool.count = 0;
        pool.total_bytes = 0;
    }

    _stats.current_pool_size_bytes = 0;
}

void ChunkAllocator::reclaim(double target_ratio) {
    if (_reclaiming.exchange(true)) {
        return;  // Already reclaiming
    }

    _stats.reclaim_count++;
    size_t target_bytes = static_cast<size_t>(_stats.current_pool_size_bytes * target_ratio);
    size_t reclaimed = 0;

    for (auto& pool : _pools) {
        std::lock_guard<std::mutex> lock(pool.mutex);
        while (!pool.chunks.empty() &&
               _stats.current_pool_size_bytes - reclaimed > target_bytes) {
            auto& chunk = pool.chunks.front();
            size_t chunk_bytes = chunk->memory_usage();
            pool.chunks.pop();
            pool.count--;
            pool.total_bytes -= chunk_bytes;
            reclaimed += chunk_bytes;
        }
    }

    _stats.current_pool_size_bytes -= reclaimed;
    _reclaiming = false;

    LOG(INFO) << "ChunkAllocator reclaimed " << reclaimed << " bytes, "
              << "pool now at " << _stats.current_pool_size_bytes << " bytes";
}

void ChunkAllocator::check_memory_pressure() {
    if (_stats.current_pool_size_bytes >
        _config.max_pool_size_bytes * _config.reclaim_threshold) {
        reclaim(0.5);
    }
}

double ChunkAllocator::hit_rate() const {
    uint64_t hits = _stats.pool_hits.load();
    uint64_t total = hits + _stats.pool_misses.load();
    return total > 0 ? static_cast<double>(hits) / total : 0.0;
}

// PooledChunk implementation
ChunkAllocator::PooledChunk::PooledChunk(ChunkPtr chunk, ChunkAllocator* allocator,
                                          size_t size_class)
    : _chunk(std::move(chunk)), _allocator(allocator), _size_class(size_class) {}

ChunkAllocator::PooledChunk::~PooledChunk() {
    if (_chunk && _allocator) {
        _allocator->deallocate(std::move(_chunk), _size_class);
    }
}

ChunkAllocator::PooledChunk::PooledChunk(PooledChunk&& other) noexcept
    : _chunk(std::move(other._chunk)),
      _allocator(other._allocator),
      _size_class(other._size_class) {
    // Null out the source allocator to prevent double-deallocation
    // The moved chunk is now owned by 'this', not by 'other'
    other._allocator = nullptr;
}

ChunkAllocator::PooledChunk& ChunkAllocator::PooledChunk::operator=(PooledChunk&& other) noexcept {
    if (this != &other) {
        // Deallocate our current chunk if we own it
        if (_chunk && _allocator) {
            _allocator->deallocate(std::move(_chunk), _size_class);
        }
        // Take ownership of other's chunk
        _chunk = std::move(other._chunk);
        _allocator = other._allocator;
        _size_class = other._size_class;
        // Clear other's allocator to prevent it from deallocating the chunk
        other._allocator = nullptr;
    }
    return *this;
}

ChunkPtr ChunkAllocator::PooledChunk::release() {
    _allocator = nullptr;
    return std::move(_chunk);
}

// ChunkRecycler implementation
ChunkRecycler::ChunkRecycler(ChunkAllocator& allocator) : _allocator(allocator) {}

ChunkRecycler::~ChunkRecycler() {
    recycle_all();
}

void ChunkRecycler::track(PooledChunkPtr chunk) {
    _tracked_chunks.push_back(std::move(chunk));
}

void ChunkRecycler::recycle_all() {
    _tracked_chunks.clear();  // Destructors return chunks to pool
}

void ChunkRecycler::release_all() {
    for (auto& chunk : _tracked_chunks) {
        chunk.release();
    }
    _tracked_chunks.clear();
}

// ScopedChunkPool implementation
ScopedChunkPool::ScopedChunkPool(size_t max_size_bytes)
    : _max_size_bytes(max_size_bytes) {}

ScopedChunkPool::~ScopedChunkPool() {
    _chunks.clear();  // Return all chunks to global pool
}

PooledChunkPtr ScopedChunkPool::allocate(size_t row_capacity) {
    auto chunk = ChunkAllocator::instance().allocate(row_capacity);

    // Estimate memory (rough approximation before columns added)
    size_t estimated_bytes = row_capacity * 64;  // ~64 bytes per row estimate

    if (_bytes_used + estimated_bytes > _max_size_bytes) {
        LOG(WARNING) << "ScopedChunkPool exceeded limit: " << _bytes_used
                     << " + " << estimated_bytes << " > " << _max_size_bytes;
    }

    _bytes_used += estimated_bytes;
    return chunk;
}

} // namespace starrocks
