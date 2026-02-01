// Copyright 2021-present StarRocks, Inc.
// Copyright 2026 CelerData, Inc. - Enterprise Performance Optimizations
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

#include "exprs/simd_predicate.h"
#include "util/logging.h"

#include <chrono>

namespace starrocks {

SIMDPredicate::SIMDLevel SIMDPredicate::detect_simd_level() {
#if defined(__AVX512F__)
    return SIMDLevel::AVX512;
#elif defined(__AVX2__)
    return SIMDLevel::AVX2;
#elif defined(__SSE4_2__)
    return SIMDLevel::SSE42;
#else
    return SIMDLevel::NONE;
#endif
}

const char* SIMDPredicate::simd_level_name(SIMDLevel level) {
    switch (level) {
        case SIMDLevel::AVX512: return "AVX-512";
        case SIMDLevel::AVX2: return "AVX2";
        case SIMDLevel::SSE42: return "SSE4.2";
        default: return "Scalar";
    }
}

void SIMDPredicate::evaluate(
    const Column* column,
    CompareOp op,
    const void* value,
    uint8_t* result,
    size_t num_rows
) {
    // Handle nullable columns
    if (column->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(column);
        const auto& data_column = nullable->data_column();
        const auto& null_column = nullable->null_column();

        // Evaluate on data column
        evaluate(data_column.get(), op, value, result, num_rows);

        // Apply null mask
        apply_null_mask(null_column->raw_data(), result, num_rows, false);
        return;
    }

    // Dispatch based on column type
    LogicalType ltype = column->type().type;

    switch (ltype) {
        case TYPE_TINYINT:
        case TYPE_BOOLEAN: {
            auto* typed = down_cast<const FixedLengthColumn<int8_t>*>(column);
            evaluate_fixed<int8_t>(typed->raw_data(), op,
                                   *static_cast<const int8_t*>(value),
                                   result, num_rows);
            break;
        }
        case TYPE_SMALLINT: {
            auto* typed = down_cast<const FixedLengthColumn<int16_t>*>(column);
            evaluate_fixed<int16_t>(typed->raw_data(), op,
                                    *static_cast<const int16_t*>(value),
                                    result, num_rows);
            break;
        }
        case TYPE_INT:
        case TYPE_DATE: {
            auto* typed = down_cast<const FixedLengthColumn<int32_t>*>(column);
            evaluate_fixed<int32_t>(typed->raw_data(), op,
                                    *static_cast<const int32_t*>(value),
                                    result, num_rows);
            break;
        }
        case TYPE_BIGINT:
        case TYPE_DATETIME:
        case TYPE_TIME: {
            auto* typed = down_cast<const FixedLengthColumn<int64_t>*>(column);
            evaluate_fixed<int64_t>(typed->raw_data(), op,
                                    *static_cast<const int64_t*>(value),
                                    result, num_rows);
            break;
        }
        case TYPE_FLOAT: {
            auto* typed = down_cast<const FixedLengthColumn<float>*>(column);
            evaluate_fixed<float>(typed->raw_data(), op,
                                  *static_cast<const float*>(value),
                                  result, num_rows);
            break;
        }
        case TYPE_DOUBLE: {
            auto* typed = down_cast<const FixedLengthColumn<double>*>(column);
            evaluate_fixed<double>(typed->raw_data(), op,
                                   *static_cast<const double*>(value),
                                   result, num_rows);
            break;
        }
        default:
            // Fallback for unsupported types - use scalar
            LOG(WARNING) << "SIMD predicate not supported for type "
                         << static_cast<int>(ltype) << ", using scalar";
            std::memset(result, 1, num_rows);  // Default to all pass
            break;
    }
}

template <typename T>
void SIMDPredicate::evaluate_fixed(
    const T* data,
    CompareOp op,
    T value,
    uint8_t* result,
    size_t num_rows
) {
    SIMDLevel level = detect_simd_level();

#if defined(__AVX512F__)
    if (level == SIMDLevel::AVX512) {
        evaluate_avx512<T>(data, op, value, result, num_rows);
        return;
    }
#endif

#if defined(__AVX2__)
    if (level >= SIMDLevel::AVX2) {
        evaluate_avx2<T>(data, op, value, result, num_rows);
        return;
    }
#endif

#if defined(__SSE4_2__)
    if (level >= SIMDLevel::SSE42) {
        evaluate_sse42<T>(data, op, value, result, num_rows);
        return;
    }
#endif

    evaluate_scalar<T>(data, op, value, result, num_rows);
}

// Scalar fallback implementations
template <typename T>
void SIMDPredicate::evaluate_scalar(
    const T* data,
    CompareOp op,
    T value,
    uint8_t* result,
    size_t num_rows
) {
    for (size_t i = 0; i < num_rows; ++i) {
        result[i] = compare(data[i], op, value) ? 1 : 0;
    }
}

void SIMDPredicate::combine_and_scalar(
    const uint8_t* left,
    const uint8_t* right,
    uint8_t* result,
    size_t num_bytes
) {
    for (size_t i = 0; i < num_bytes; ++i) {
        result[i] = left[i] & right[i];
    }
}

void SIMDPredicate::combine_or_scalar(
    const uint8_t* left,
    const uint8_t* right,
    uint8_t* result,
    size_t num_bytes
) {
    for (size_t i = 0; i < num_bytes; ++i) {
        result[i] = left[i] | right[i];
    }
}

size_t SIMDPredicate::popcount_scalar(const uint8_t* bitmap, size_t num_bytes) {
    size_t count = 0;
    for (size_t i = 0; i < num_bytes; ++i) {
        count += __builtin_popcount(bitmap[i]);
    }
    return count;
}

// SSE4.2 implementations
#if defined(__SSE4_2__)
template <typename T>
void SIMDPredicate::evaluate_sse42(
    const T* data,
    CompareOp op,
    T value,
    uint8_t* result,
    size_t num_rows
) {
    constexpr size_t VECTOR_SIZE = 16 / sizeof(T);
    size_t vector_end = (num_rows / VECTOR_SIZE) * VECTOR_SIZE;

    if constexpr (sizeof(T) == 4) {  // int32_t, float
        __m128i val_vec = _mm_set1_epi32(*reinterpret_cast<const int32_t*>(&value));

        for (size_t i = 0; i < vector_end; i += VECTOR_SIZE) {
            __m128i data_vec = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(data + i));

            __m128i cmp_result;
            switch (op) {
                case CompareOp::EQ:
                    cmp_result = _mm_cmpeq_epi32(data_vec, val_vec);
                    break;
                case CompareOp::GT:
                    cmp_result = _mm_cmpgt_epi32(data_vec, val_vec);
                    break;
                case CompareOp::LT:
                    cmp_result = _mm_cmplt_epi32(data_vec, val_vec);
                    break;
                default:
                    // For other ops, use scalar for remaining
                    evaluate_scalar<T>(data + i, op, value, result + i, VECTOR_SIZE);
                    continue;
            }

            // Extract comparison results to bytes
            int mask = _mm_movemask_ps(_mm_castsi128_ps(cmp_result));
            for (size_t j = 0; j < VECTOR_SIZE; ++j) {
                result[i + j] = (mask >> j) & 1;
            }
        }
    } else if constexpr (sizeof(T) == 8) {  // int64_t, double
        __m128i val_vec = _mm_set1_epi64x(*reinterpret_cast<const int64_t*>(&value));

        for (size_t i = 0; i < vector_end; i += VECTOR_SIZE) {
            __m128i data_vec = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(data + i));

            __m128i cmp_result;
            switch (op) {
                case CompareOp::EQ:
                    cmp_result = _mm_cmpeq_epi64(data_vec, val_vec);
                    break;
                case CompareOp::GT:
                    cmp_result = _mm_cmpgt_epi64(data_vec, val_vec);
                    break;
                default:
                    evaluate_scalar<T>(data + i, op, value, result + i, VECTOR_SIZE);
                    continue;
            }

            int mask = _mm_movemask_pd(_mm_castsi128_pd(cmp_result));
            for (size_t j = 0; j < VECTOR_SIZE; ++j) {
                result[i + j] = (mask >> j) & 1;
            }
        }
    }

    // Handle remaining elements with scalar
    for (size_t i = vector_end; i < num_rows; ++i) {
        result[i] = compare(data[i], op, value) ? 1 : 0;
    }
}

void SIMDPredicate::combine_and_sse42(
    const uint8_t* left,
    const uint8_t* right,
    uint8_t* result,
    size_t num_bytes
) {
    size_t vector_end = (num_bytes / 16) * 16;

    for (size_t i = 0; i < vector_end; i += 16) {
        __m128i l = _mm_loadu_si128(reinterpret_cast<const __m128i*>(left + i));
        __m128i r = _mm_loadu_si128(reinterpret_cast<const __m128i*>(right + i));
        __m128i res = _mm_and_si128(l, r);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(result + i), res);
    }

    for (size_t i = vector_end; i < num_bytes; ++i) {
        result[i] = left[i] & right[i];
    }
}

void SIMDPredicate::combine_or_sse42(
    const uint8_t* left,
    const uint8_t* right,
    uint8_t* result,
    size_t num_bytes
) {
    size_t vector_end = (num_bytes / 16) * 16;

    for (size_t i = 0; i < vector_end; i += 16) {
        __m128i l = _mm_loadu_si128(reinterpret_cast<const __m128i*>(left + i));
        __m128i r = _mm_loadu_si128(reinterpret_cast<const __m128i*>(right + i));
        __m128i res = _mm_or_si128(l, r);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(result + i), res);
    }

    for (size_t i = vector_end; i < num_bytes; ++i) {
        result[i] = left[i] | right[i];
    }
}

size_t SIMDPredicate::popcount_sse42(const uint8_t* bitmap, size_t num_bytes) {
    size_t count = 0;
    size_t i = 0;

    // Process 8 bytes at a time with popcnt
    for (; i + 8 <= num_bytes; i += 8) {
        uint64_t val = *reinterpret_cast<const uint64_t*>(bitmap + i);
        count += _mm_popcnt_u64(val);
    }

    // Handle remaining bytes
    for (; i < num_bytes; ++i) {
        count += __builtin_popcount(bitmap[i]);
    }

    return count;
}
#endif  // __SSE4_2__

// AVX2 implementations
#if defined(__AVX2__)
template <typename T>
void SIMDPredicate::evaluate_avx2(
    const T* data,
    CompareOp op,
    T value,
    uint8_t* result,
    size_t num_rows
) {
    constexpr size_t VECTOR_SIZE = 32 / sizeof(T);
    size_t vector_end = (num_rows / VECTOR_SIZE) * VECTOR_SIZE;

    if constexpr (sizeof(T) == 4) {  // int32_t, float
        __m256i val_vec = _mm256_set1_epi32(*reinterpret_cast<const int32_t*>(&value));

        for (size_t i = 0; i < vector_end; i += VECTOR_SIZE) {
            __m256i data_vec = _mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(data + i));

            __m256i cmp_result;
            switch (op) {
                case CompareOp::EQ:
                    cmp_result = _mm256_cmpeq_epi32(data_vec, val_vec);
                    break;
                case CompareOp::GT:
                    cmp_result = _mm256_cmpgt_epi32(data_vec, val_vec);
                    break;
                default:
                    evaluate_scalar<T>(data + i, op, value, result + i, VECTOR_SIZE);
                    continue;
            }

            int mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp_result));
            for (size_t j = 0; j < VECTOR_SIZE; ++j) {
                result[i + j] = (mask >> j) & 1;
            }
        }
    } else if constexpr (sizeof(T) == 8) {  // int64_t, double
        __m256i val_vec = _mm256_set1_epi64x(*reinterpret_cast<const int64_t*>(&value));

        for (size_t i = 0; i < vector_end; i += VECTOR_SIZE) {
            __m256i data_vec = _mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(data + i));

            __m256i cmp_result;
            switch (op) {
                case CompareOp::EQ:
                    cmp_result = _mm256_cmpeq_epi64(data_vec, val_vec);
                    break;
                case CompareOp::GT:
                    cmp_result = _mm256_cmpgt_epi64(data_vec, val_vec);
                    break;
                default:
                    evaluate_scalar<T>(data + i, op, value, result + i, VECTOR_SIZE);
                    continue;
            }

            int mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp_result));
            for (size_t j = 0; j < VECTOR_SIZE; ++j) {
                result[i + j] = (mask >> j) & 1;
            }
        }
    }

    // Handle remaining elements
    for (size_t i = vector_end; i < num_rows; ++i) {
        result[i] = compare(data[i], op, value) ? 1 : 0;
    }
}

void SIMDPredicate::combine_and_avx2(
    const uint8_t* left,
    const uint8_t* right,
    uint8_t* result,
    size_t num_bytes
) {
    size_t vector_end = (num_bytes / 32) * 32;

    for (size_t i = 0; i < vector_end; i += 32) {
        __m256i l = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(left + i));
        __m256i r = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(right + i));
        __m256i res = _mm256_and_si256(l, r);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(result + i), res);
    }

    // Handle remaining bytes with SSE or scalar
    for (size_t i = vector_end; i < num_bytes; ++i) {
        result[i] = left[i] & right[i];
    }
}

void SIMDPredicate::combine_or_avx2(
    const uint8_t* left,
    const uint8_t* right,
    uint8_t* result,
    size_t num_bytes
) {
    size_t vector_end = (num_bytes / 32) * 32;

    for (size_t i = 0; i < vector_end; i += 32) {
        __m256i l = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(left + i));
        __m256i r = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(right + i));
        __m256i res = _mm256_or_si256(l, r);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(result + i), res);
    }

    for (size_t i = vector_end; i < num_bytes; ++i) {
        result[i] = left[i] | right[i];
    }
}

size_t SIMDPredicate::popcount_avx2(const uint8_t* bitmap, size_t num_bytes) {
    size_t count = 0;
    size_t i = 0;

    // Process 32 bytes at a time
    for (; i + 32 <= num_bytes; i += 32) {
        __m256i vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(bitmap + i));

        // Use lookup table for popcount
        const __m256i lookup = _mm256_setr_epi8(
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4);
        const __m256i low_mask = _mm256_set1_epi8(0x0f);

        __m256i lo = _mm256_and_si256(vec, low_mask);
        __m256i hi = _mm256_and_si256(_mm256_srli_epi16(vec, 4), low_mask);
        __m256i popcnt_lo = _mm256_shuffle_epi8(lookup, lo);
        __m256i popcnt_hi = _mm256_shuffle_epi8(lookup, hi);
        __m256i popcnt = _mm256_add_epi8(popcnt_lo, popcnt_hi);

        // Sum all bytes
        __m256i sum = _mm256_sad_epu8(popcnt, _mm256_setzero_si256());
        count += _mm256_extract_epi64(sum, 0) + _mm256_extract_epi64(sum, 1) +
                 _mm256_extract_epi64(sum, 2) + _mm256_extract_epi64(sum, 3);
    }

    // Handle remaining bytes
    for (; i < num_bytes; ++i) {
        count += __builtin_popcount(bitmap[i]);
    }

    return count;
}
#endif  // __AVX2__

// AVX-512 implementations
#if defined(__AVX512F__)
template <typename T>
void SIMDPredicate::evaluate_avx512(
    const T* data,
    CompareOp op,
    T value,
    uint8_t* result,
    size_t num_rows
) {
    constexpr size_t VECTOR_SIZE = 64 / sizeof(T);
    size_t vector_end = (num_rows / VECTOR_SIZE) * VECTOR_SIZE;

    if constexpr (sizeof(T) == 4) {  // int32_t, float
        __m512i val_vec = _mm512_set1_epi32(*reinterpret_cast<const int32_t*>(&value));

        for (size_t i = 0; i < vector_end; i += VECTOR_SIZE) {
            __m512i data_vec = _mm512_loadu_si512(data + i);

            __mmask16 mask;
            switch (op) {
                case CompareOp::EQ:
                    mask = _mm512_cmpeq_epi32_mask(data_vec, val_vec);
                    break;
                case CompareOp::NE:
                    mask = _mm512_cmpneq_epi32_mask(data_vec, val_vec);
                    break;
                case CompareOp::GT:
                    mask = _mm512_cmpgt_epi32_mask(data_vec, val_vec);
                    break;
                case CompareOp::GE:
                    mask = _mm512_cmpge_epi32_mask(data_vec, val_vec);
                    break;
                case CompareOp::LT:
                    mask = _mm512_cmplt_epi32_mask(data_vec, val_vec);
                    break;
                case CompareOp::LE:
                    mask = _mm512_cmple_epi32_mask(data_vec, val_vec);
                    break;
            }

            // Expand mask to bytes
            for (size_t j = 0; j < VECTOR_SIZE; ++j) {
                result[i + j] = (mask >> j) & 1;
            }
        }
    } else if constexpr (sizeof(T) == 8) {  // int64_t, double
        __m512i val_vec = _mm512_set1_epi64(*reinterpret_cast<const int64_t*>(&value));

        for (size_t i = 0; i < vector_end; i += VECTOR_SIZE) {
            __m512i data_vec = _mm512_loadu_si512(data + i);

            __mmask8 mask;
            switch (op) {
                case CompareOp::EQ:
                    mask = _mm512_cmpeq_epi64_mask(data_vec, val_vec);
                    break;
                case CompareOp::NE:
                    mask = _mm512_cmpneq_epi64_mask(data_vec, val_vec);
                    break;
                case CompareOp::GT:
                    mask = _mm512_cmpgt_epi64_mask(data_vec, val_vec);
                    break;
                case CompareOp::GE:
                    mask = _mm512_cmpge_epi64_mask(data_vec, val_vec);
                    break;
                case CompareOp::LT:
                    mask = _mm512_cmplt_epi64_mask(data_vec, val_vec);
                    break;
                case CompareOp::LE:
                    mask = _mm512_cmple_epi64_mask(data_vec, val_vec);
                    break;
            }

            for (size_t j = 0; j < VECTOR_SIZE; ++j) {
                result[i + j] = (mask >> j) & 1;
            }
        }
    }

    // Handle remaining elements
    for (size_t i = vector_end; i < num_rows; ++i) {
        result[i] = compare(data[i], op, value) ? 1 : 0;
    }
}

void SIMDPredicate::combine_and_avx512(
    const uint8_t* left,
    const uint8_t* right,
    uint8_t* result,
    size_t num_bytes
) {
    size_t vector_end = (num_bytes / 64) * 64;

    for (size_t i = 0; i < vector_end; i += 64) {
        __m512i l = _mm512_loadu_si512(left + i);
        __m512i r = _mm512_loadu_si512(right + i);
        __m512i res = _mm512_and_si512(l, r);
        _mm512_storeu_si512(result + i, res);
    }

    for (size_t i = vector_end; i < num_bytes; ++i) {
        result[i] = left[i] & right[i];
    }
}

void SIMDPredicate::combine_or_avx512(
    const uint8_t* left,
    const uint8_t* right,
    uint8_t* result,
    size_t num_bytes
) {
    size_t vector_end = (num_bytes / 64) * 64;

    for (size_t i = 0; i < vector_end; i += 64) {
        __m512i l = _mm512_loadu_si512(left + i);
        __m512i r = _mm512_loadu_si512(right + i);
        __m512i res = _mm512_or_si512(l, r);
        _mm512_storeu_si512(result + i, res);
    }

    for (size_t i = vector_end; i < num_bytes; ++i) {
        result[i] = left[i] | right[i];
    }
}

size_t SIMDPredicate::popcount_avx512(const uint8_t* bitmap, size_t num_bytes) {
    size_t count = 0;
    size_t i = 0;

#if defined(__AVX512VPOPCNTDQ__)
    // Direct hardware popcount if available
    for (; i + 64 <= num_bytes; i += 64) {
        __m512i vec = _mm512_loadu_si512(bitmap + i);
        __m512i popcnt = _mm512_popcnt_epi64(vec);
        count += _mm512_reduce_add_epi64(popcnt);
    }
#else
    // Fallback to lookup table method
    for (; i + 64 <= num_bytes; i += 64) {
        __m512i vec = _mm512_loadu_si512(bitmap + i);

        const __m512i lookup = _mm512_setr_epi8(
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4);
        const __m512i low_mask = _mm512_set1_epi8(0x0f);

        __m512i lo = _mm512_and_si512(vec, low_mask);
        __m512i hi = _mm512_and_si512(_mm512_srli_epi16(vec, 4), low_mask);
        __m512i popcnt_lo = _mm512_shuffle_epi8(lookup, lo);
        __m512i popcnt_hi = _mm512_shuffle_epi8(lookup, hi);
        __m512i popcnt = _mm512_add_epi8(popcnt_lo, popcnt_hi);

        __m512i sum = _mm512_sad_epu8(popcnt, _mm512_setzero_si512());
        count += _mm512_reduce_add_epi64(sum);
    }
#endif

    // Handle remaining bytes
    for (; i < num_bytes; ++i) {
        count += __builtin_popcount(bitmap[i]);
    }

    return count;
}
#endif  // __AVX512F__

void SIMDPredicate::apply_null_mask(
    const uint8_t* null_data,
    uint8_t* result,
    size_t num_rows,
    bool null_as_true
) {
    SIMDLevel level = detect_simd_level();

    if (null_as_true) {
        // OR with null mask: result | null
#if defined(__AVX512F__)
        if (level == SIMDLevel::AVX512) {
            combine_or_avx512(result, null_data, result, num_rows);
            return;
        }
#endif
#if defined(__AVX2__)
        if (level >= SIMDLevel::AVX2) {
            combine_or_avx2(result, null_data, result, num_rows);
            return;
        }
#endif
#if defined(__SSE4_2__)
        if (level >= SIMDLevel::SSE42) {
            combine_or_sse42(result, null_data, result, num_rows);
            return;
        }
#endif
        combine_or_scalar(result, null_data, result, num_rows);
    } else {
        // AND with NOT null mask: result & ~null
        // First invert null mask, then AND
        std::vector<uint8_t> not_null(num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            not_null[i] = null_data[i] ? 0 : 1;
        }

#if defined(__AVX512F__)
        if (level == SIMDLevel::AVX512) {
            combine_and_avx512(result, not_null.data(), result, num_rows);
            return;
        }
#endif
#if defined(__AVX2__)
        if (level >= SIMDLevel::AVX2) {
            combine_and_avx2(result, not_null.data(), result, num_rows);
            return;
        }
#endif
#if defined(__SSE4_2__)
        if (level >= SIMDLevel::SSE42) {
            combine_and_sse42(result, not_null.data(), result, num_rows);
            return;
        }
#endif
        combine_and_scalar(result, not_null.data(), result, num_rows);
    }
}

void SIMDPredicate::combine_and(
    const uint8_t* left,
    const uint8_t* right,
    uint8_t* result,
    size_t num_bytes
) {
    SIMDLevel level = detect_simd_level();

#if defined(__AVX512F__)
    if (level == SIMDLevel::AVX512) {
        combine_and_avx512(left, right, result, num_bytes);
        return;
    }
#endif
#if defined(__AVX2__)
    if (level >= SIMDLevel::AVX2) {
        combine_and_avx2(left, right, result, num_bytes);
        return;
    }
#endif
#if defined(__SSE4_2__)
    if (level >= SIMDLevel::SSE42) {
        combine_and_sse42(left, right, result, num_bytes);
        return;
    }
#endif
    combine_and_scalar(left, right, result, num_bytes);
}

void SIMDPredicate::combine_or(
    const uint8_t* left,
    const uint8_t* right,
    uint8_t* result,
    size_t num_bytes
) {
    SIMDLevel level = detect_simd_level();

#if defined(__AVX512F__)
    if (level == SIMDLevel::AVX512) {
        combine_or_avx512(left, right, result, num_bytes);
        return;
    }
#endif
#if defined(__AVX2__)
    if (level >= SIMDLevel::AVX2) {
        combine_or_avx2(left, right, result, num_bytes);
        return;
    }
#endif
#if defined(__SSE4_2__)
    if (level >= SIMDLevel::SSE42) {
        combine_or_sse42(left, right, result, num_bytes);
        return;
    }
#endif
    combine_or_scalar(left, right, result, num_bytes);
}

size_t SIMDPredicate::count_set_bits(const uint8_t* bitmap, size_t num_rows) {
    SIMDLevel level = detect_simd_level();

#if defined(__AVX512F__)
    if (level == SIMDLevel::AVX512) {
        return popcount_avx512(bitmap, num_rows);
    }
#endif
#if defined(__AVX2__)
    if (level >= SIMDLevel::AVX2) {
        return popcount_avx2(bitmap, num_rows);
    }
#endif
#if defined(__SSE4_2__)
    if (level >= SIMDLevel::SSE42) {
        return popcount_sse42(bitmap, num_rows);
    }
#endif
    return popcount_scalar(bitmap, num_rows);
}

// Explicit template instantiations
template void SIMDPredicate::evaluate_fixed<int8_t>(
    const int8_t*, CompareOp, int8_t, uint8_t*, size_t);
template void SIMDPredicate::evaluate_fixed<int16_t>(
    const int16_t*, CompareOp, int16_t, uint8_t*, size_t);
template void SIMDPredicate::evaluate_fixed<int32_t>(
    const int32_t*, CompareOp, int32_t, uint8_t*, size_t);
template void SIMDPredicate::evaluate_fixed<int64_t>(
    const int64_t*, CompareOp, int64_t, uint8_t*, size_t);
template void SIMDPredicate::evaluate_fixed<float>(
    const float*, CompareOp, float, uint8_t*, size_t);
template void SIMDPredicate::evaluate_fixed<double>(
    const double*, CompareOp, double, uint8_t*, size_t);

// BatchPredicateEvaluator implementation
size_t BatchPredicateEvaluator::add_predicate(
    const Column* column,
    SIMDPredicate::CompareOp op,
    const void* value,
    size_t value_size,
    LogicalType type
) {
    Predicate pred;
    pred.column = column;
    pred.op = op;
    pred.value_bytes.resize(value_size);
    std::memcpy(pred.value_bytes.data(), value, value_size);
    pred.type = type;

    _predicates.push_back(std::move(pred));
    return _predicates.size() - 1;
}

size_t BatchPredicateEvaluator::add_and(size_t left_idx, size_t right_idx) {
    CombineOp op;
    op.type = CombineOp::AND;
    op.left_idx = left_idx;
    op.right_idx = right_idx;

    _combines.push_back(op);
    return _predicates.size() + _combines.size() - 1;
}

size_t BatchPredicateEvaluator::add_or(size_t left_idx, size_t right_idx) {
    CombineOp op;
    op.type = CombineOp::OR;
    op.left_idx = left_idx;
    op.right_idx = right_idx;

    _combines.push_back(op);
    return _predicates.size() + _combines.size() - 1;
}

std::vector<uint8_t> BatchPredicateEvaluator::evaluate(size_t root_idx, size_t num_rows) {
    auto start = std::chrono::high_resolution_clock::now();

    // Allocate result storage for all nodes
    _intermediate_results.clear();
    _intermediate_results.resize(_predicates.size() + _combines.size());

    for (auto& result : _intermediate_results) {
        result.resize(num_rows);
    }

    // Evaluate all predicates first
    for (size_t i = 0; i < _predicates.size(); ++i) {
        const auto& pred = _predicates[i];
        SIMDPredicate::evaluate(
            pred.column,
            pred.op,
            pred.value_bytes.data(),
            _intermediate_results[i].data(),
            num_rows
        );
        _stats.predicates_evaluated++;
    }

    // Apply combine operations in order
    for (size_t i = 0; i < _combines.size(); ++i) {
        const auto& combine = _combines[i];
        size_t result_idx = _predicates.size() + i;

        const uint8_t* left = _intermediate_results[combine.left_idx].data();
        const uint8_t* right = _intermediate_results[combine.right_idx].data();
        uint8_t* result = _intermediate_results[result_idx].data();

        if (combine.type == CombineOp::AND) {
            SIMDPredicate::combine_and(left, right, result, num_rows);
        } else {
            SIMDPredicate::combine_or(left, right, result, num_rows);
        }
        _stats.simd_operations++;
    }

    auto end = std::chrono::high_resolution_clock::now();
    _stats.total_time_us += std::chrono::duration<double, std::micro>(end - start).count();

    return std::move(_intermediate_results[root_idx]);
}

} // namespace starrocks
