// Copyright 2021-present StarRocks, Inc.
// Copyright 2026 CelerData, Inc. - Enterprise Performance Optimizations
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

#include "exec/vectorized_hash_join.h"
#include "column/nullable_column.h"
#include "util/logging.h"

#include <algorithm>
#include <chrono>
#include <cmath>

namespace starrocks {

// VectorizedHashJoinProbe implementation

VectorizedHashJoinProbe::VectorizedHashJoinProbe(const Config& config)
    : _config(config) {}

template <typename KeyType, typename ValueType>
void VectorizedHashJoinProbe::build(
    const Column* key_column,
    const std::vector<ValueType>& row_ids,
    VectorizedHashTable<KeyType, ValueType>& hash_table) {

    auto start = std::chrono::high_resolution_clock::now();

    size_t num_rows = key_column->size();
    _stats.build_rows = num_rows;

    // Handle nullable columns
    const uint8_t* null_data = nullptr;
    const Column* data_column = key_column;

    if (key_column->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(key_column);
        null_data = nullable->null_column()->raw_data();
        data_column = nullable->data_column().get();
    }

    // Get raw key data
    const KeyType* keys = nullptr;
    if constexpr (std::is_same_v<KeyType, int32_t>) {
        auto* typed = down_cast<const FixedLengthColumn<int32_t>*>(data_column);
        keys = typed->raw_data();
    } else if constexpr (std::is_same_v<KeyType, int64_t>) {
        auto* typed = down_cast<const FixedLengthColumn<int64_t>*>(data_column);
        keys = typed->raw_data();
    }

    if (!keys) {
        LOG(WARNING) << "Unsupported key type for vectorized hash join";
        return;
    }

    // Reserve space
    hash_table.reserve(num_rows);

    // Build in batches
    size_t batch_start = 0;
    while (batch_start < num_rows) {
        size_t batch_end = std::min(batch_start + _config.batch_size, num_rows);
        size_t batch_size = batch_end - batch_start;

        // Collect non-null keys for this batch
        std::vector<KeyType> batch_keys;
        std::vector<ValueType> batch_values;
        batch_keys.reserve(batch_size);
        batch_values.reserve(batch_size);

        for (size_t i = batch_start; i < batch_end; ++i) {
            if (null_data == nullptr || !null_data[i]) {
                batch_keys.push_back(keys[i]);
                batch_values.push_back(row_ids[i]);
            }
        }

        // Batch insert
        if (!batch_keys.empty()) {
            hash_table.insert_batch(
                batch_keys.data(),
                batch_values.data(),
                batch_keys.size()
            );
        }

        batch_start = batch_end;
    }

    auto end = std::chrono::high_resolution_clock::now();
    _stats.build_time_ms = std::chrono::duration<double, std::milli>(end - start).count();

    LOG(INFO) << "VectorizedHashJoin build: " << num_rows << " rows in "
              << _stats.build_time_ms << "ms, hash table size: "
              << hash_table.size() << ", load factor: "
              << hash_table.load_factor();
}

template <typename KeyType, typename ValueType>
void VectorizedHashJoinProbe::probe(
    const Column* probe_keys,
    const VectorizedHashTable<KeyType, ValueType>& hash_table,
    std::vector<ValueType>& matched_build_rows,
    std::vector<uint32_t>& matched_probe_rows,
    std::vector<uint32_t>& unmatched_probe_rows) {

    auto start = std::chrono::high_resolution_clock::now();

    size_t num_rows = probe_keys->size();
    _stats.probe_rows = num_rows;

    // Handle nullable columns
    const uint8_t* null_data = nullptr;
    const Column* data_column = probe_keys;

    if (probe_keys->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(probe_keys);
        null_data = nullable->null_column()->raw_data();
        data_column = nullable->data_column().get();
    }

    // Get raw key data
    const KeyType* keys = nullptr;
    if constexpr (std::is_same_v<KeyType, int32_t>) {
        auto* typed = down_cast<const FixedLengthColumn<int32_t>*>(data_column);
        keys = typed->raw_data();
    } else if constexpr (std::is_same_v<KeyType, int64_t>) {
        auto* typed = down_cast<const FixedLengthColumn<int64_t>*>(data_column);
        keys = typed->raw_data();
    }

    if (!keys) {
        LOG(WARNING) << "Unsupported key type for vectorized hash join probe";
        return;
    }

    // Pre-allocate results
    matched_build_rows.reserve(num_rows);
    matched_probe_rows.reserve(num_rows);
    unmatched_probe_rows.reserve(num_rows / 10);  // Assume ~10% unmatched

    // Probe in batches
    std::vector<ValueType> batch_results(_config.batch_size);
    std::vector<uint8_t> batch_found(_config.batch_size);
    size_t simd_processed = 0;

    size_t batch_start = 0;
    while (batch_start < num_rows) {
        size_t batch_end = std::min(batch_start + _config.batch_size, num_rows);
        size_t batch_size = batch_end - batch_start;

        // Collect non-null keys for probing
        std::vector<KeyType> batch_keys;
        std::vector<uint32_t> batch_indices;
        batch_keys.reserve(batch_size);
        batch_indices.reserve(batch_size);

        for (size_t i = batch_start; i < batch_end; ++i) {
            if (null_data == nullptr || !null_data[i]) {
                batch_keys.push_back(keys[i]);
                batch_indices.push_back(static_cast<uint32_t>(i));
            } else {
                // Null keys are always unmatched
                unmatched_probe_rows.push_back(static_cast<uint32_t>(i));
            }
        }

        if (!batch_keys.empty()) {
            // Batch probe
            const_cast<VectorizedHashTable<KeyType, ValueType>&>(hash_table)
                .probe_batch(
                    batch_keys.data(),
                    batch_keys.size(),
                    batch_results.data(),
                    batch_found.data()
                );

            simd_processed += batch_keys.size();

            // Process results
            for (size_t i = 0; i < batch_keys.size(); ++i) {
                if (batch_found[i]) {
                    matched_build_rows.push_back(batch_results[i]);
                    matched_probe_rows.push_back(batch_indices[i]);
                } else {
                    unmatched_probe_rows.push_back(batch_indices[i]);
                }
            }
        }

        batch_start = batch_end;
    }

    auto end = std::chrono::high_resolution_clock::now();
    _stats.probe_time_ms = std::chrono::duration<double, std::milli>(end - start).count();
    _stats.matched_rows = matched_probe_rows.size();
    _stats.unmatched_rows = unmatched_probe_rows.size();
    _stats.simd_utilization = static_cast<double>(simd_processed) / num_rows;

    LOG(INFO) << "VectorizedHashJoin probe: " << num_rows << " rows in "
              << _stats.probe_time_ms << "ms, matched: " << _stats.matched_rows
              << ", unmatched: " << _stats.unmatched_rows
              << ", SIMD utilization: " << (_stats.simd_utilization * 100) << "%";
}

// HashJoinSkewDetector implementation

HashJoinSkewDetector::HashJoinSkewDetector(const Config& config)
    : _config(config) {}

template <typename KeyType>
void HashJoinSkewDetector::analyze(const Column* key_column) {
    _key_counts.clear();
    _hot_key_hashes.clear();

    size_t num_rows = key_column->size();
    size_t sample_size = std::min(_config.sample_size, num_rows);

    // Sample and count keys
    const uint8_t* null_data = nullptr;
    const Column* data_column = key_column;

    if (key_column->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(key_column);
        null_data = nullable->null_column()->raw_data();
        data_column = nullable->data_column().get();
    }

    const KeyType* keys = nullptr;
    if constexpr (std::is_same_v<KeyType, int32_t>) {
        auto* typed = down_cast<const FixedLengthColumn<int32_t>*>(data_column);
        keys = typed->raw_data();
    } else if constexpr (std::is_same_v<KeyType, int64_t>) {
        auto* typed = down_cast<const FixedLengthColumn<int64_t>*>(data_column);
        keys = typed->raw_data();
    }

    if (!keys) return;

    // Sample uniformly
    size_t step = num_rows / sample_size;
    if (step == 0) step = 1;

    for (size_t i = 0; i < num_rows; i += step) {
        if (null_data && null_data[i]) continue;

        // Hash the key
        uint64_t h = static_cast<uint64_t>(keys[i]);
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdULL;
        h ^= h >> 33;

        _key_counts[h]++;
    }

    _stats.total_keys = sample_size;
    _stats.unique_keys = _key_counts.size();

    // Find hot keys
    std::vector<std::pair<uint64_t, uint64_t>> sorted_counts;
    sorted_counts.reserve(_key_counts.size());
    for (const auto& [hash, count] : _key_counts) {
        sorted_counts.emplace_back(hash, count);
    }

    std::sort(sorted_counts.begin(), sorted_counts.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    // Identify hot keys
    size_t hot_threshold = static_cast<size_t>(_config.skew_threshold * sample_size);
    for (const auto& [hash, count] : sorted_counts) {
        if (count >= hot_threshold && _hot_key_hashes.size() < _config.max_hot_keys) {
            _hot_key_hashes.push_back(hash);
        }
    }

    _stats.hot_keys = _hot_key_hashes.size();
    _stats.max_key_frequency = sorted_counts.empty() ? 0 :
        static_cast<double>(sorted_counts[0].second) / sample_size;

    // Calculate Gini coefficient for skew factor
    if (!sorted_counts.empty()) {
        double sum = 0;
        double cumulative = 0;
        for (size_t i = 0; i < sorted_counts.size(); ++i) {
            cumulative += sorted_counts[i].second;
            sum += (sorted_counts.size() - i) * sorted_counts[i].second;
        }
        double n = sorted_counts.size();
        _stats.skew_factor = (2.0 * sum) / (n * cumulative) - (n + 1.0) / n;
    }

    LOG(INFO) << "HashJoinSkewDetector: " << _stats.unique_keys << " unique keys, "
              << _stats.hot_keys << " hot keys, skew factor: " << _stats.skew_factor
              << ", max frequency: " << (_stats.max_key_frequency * 100) << "%";
}

template <typename KeyType>
bool HashJoinSkewDetector::is_hot_key(const KeyType& key) const {
    // Hash the key
    uint64_t h = static_cast<uint64_t>(key);
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;

    return std::find(_hot_key_hashes.begin(), _hot_key_hashes.end(), h)
           != _hot_key_hashes.end();
}

HashJoinSkewDetector::Strategy HashJoinSkewDetector::recommended_strategy() const {
    if (_stats.hot_keys == 0) {
        return Strategy::NORMAL;
    }

    if (_stats.skew_factor > 0.5) {
        // Severe skew
        if (_config.enable_broadcast && _stats.hot_keys <= 10) {
            return Strategy::BROADCAST_HOT;
        } else if (_config.enable_salting) {
            return Strategy::SALT_HOT;
        } else {
            return Strategy::SKEW_JOIN;
        }
    } else if (_stats.skew_factor > 0.3) {
        // Moderate skew
        if (_config.enable_salting) {
            return Strategy::SALT_HOT;
        }
    }

    return Strategy::NORMAL;
}

// Explicit template instantiations
template class VectorizedHashTable<int32_t, uint32_t>;
template class VectorizedHashTable<int64_t, uint32_t>;
template class VectorizedHashTable<int32_t, int64_t>;
template class VectorizedHashTable<int64_t, int64_t>;

template void VectorizedHashJoinProbe::build<int32_t, uint32_t>(
    const Column*, const std::vector<uint32_t>&,
    VectorizedHashTable<int32_t, uint32_t>&);
template void VectorizedHashJoinProbe::build<int64_t, uint32_t>(
    const Column*, const std::vector<uint32_t>&,
    VectorizedHashTable<int64_t, uint32_t>&);

template void VectorizedHashJoinProbe::probe<int32_t, uint32_t>(
    const Column*, const VectorizedHashTable<int32_t, uint32_t>&,
    std::vector<uint32_t>&, std::vector<uint32_t>&, std::vector<uint32_t>&);
template void VectorizedHashJoinProbe::probe<int64_t, uint32_t>(
    const Column*, const VectorizedHashTable<int64_t, uint32_t>&,
    std::vector<uint32_t>&, std::vector<uint32_t>&, std::vector<uint32_t>&);

template void HashJoinSkewDetector::analyze<int32_t>(const Column*);
template void HashJoinSkewDetector::analyze<int64_t>(const Column*);

template bool HashJoinSkewDetector::is_hot_key<int32_t>(const int32_t&) const;
template bool HashJoinSkewDetector::is_hot_key<int64_t>(const int64_t&) const;

} // namespace starrocks
