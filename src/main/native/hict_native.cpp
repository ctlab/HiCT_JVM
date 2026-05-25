/*
 * MIT License
 *
 * Copyright (c) 2021-2026. Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis and Computer Technologies Laboratory ITMO University team.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include <jni.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <new>
#include <vector>

#if defined(HICT_NATIVE_AVX512) && defined(__AVX512F__)
#include <immintrin.h>
#endif

namespace {

#ifndef HICT_NATIVE_VARIANT
#define HICT_NATIVE_VARIANT "baseline"
#endif

constexpr const char* HICT_NATIVE_VERSION = "hict-native-processing/0.4-" HICT_NATIVE_VARIANT;
constexpr std::int64_t PARALLEL_THRESHOLD = 131072;

struct NativeBackendSession {
  std::atomic<jlong> operation_count{0};
  std::atomic<jlong> failed_operation_count{0};
  bool hdf5_backend_available{false};
};

NativeBackendSession* session_from_handle(const jlong session_handle) {
  if (session_handle == 0) {
    return nullptr;
  }
  return reinterpret_cast<NativeBackendSession*>(session_handle);
}

jboolean native_result(const jlong session_handle, const bool ok) {
  auto* session = session_from_handle(session_handle);
  if (session == nullptr) {
    return JNI_FALSE;
  }
  session->operation_count.fetch_add(1, std::memory_order_relaxed);
  if (!ok) {
    session->failed_operation_count.fetch_add(1, std::memory_order_relaxed);
  }
  return ok ? JNI_TRUE : JNI_FALSE;
}

jboolean native_failure(const jlong session_handle) {
  return native_result(session_handle, false);
}

bool valid_extent(const jint rows, const jint columns, const jsize element_count) {
  if (rows < 0 || columns < 0) {
    return false;
  }
  const auto expected = static_cast<std::int64_t>(rows) * static_cast<std::int64_t>(columns);
  return expected >= 0 && expected <= element_count;
}

double sanitize_signal(double signal) {
  if (!std::isfinite(signal) || signal < 0.0) {
    return 0.0;
  }
  return signal;
}

double sanitize_positive_signal(double signal) {
  if (!std::isfinite(signal) || signal <= 0.0) {
    return 0.0;
  }
  return signal;
}

double transform_signal(double signal,
                        const double row_weight,
                        const double column_weight,
                        const double ln_pre_log_base,
                        const double resolution_scaling_coeff,
                        const double resolution_linear_scaling_coeff,
                        const bool apply_resolution_scaling,
                        const bool apply_resolution_linear_scaling,
                        const bool apply_cooler_weights) {
  signal = sanitize_signal(signal);
  if (ln_pre_log_base > 0.0) {
    signal = std::log1p(signal) / ln_pre_log_base;
  }
  if (apply_resolution_scaling) {
    signal *= resolution_scaling_coeff;
  }
  if (apply_resolution_linear_scaling) {
    signal *= resolution_linear_scaling_coeff;
  }
  if (apply_cooler_weights) {
    signal *= row_weight * column_weight;
  }
  return std::isfinite(signal) ? signal : 0.0;
}

template <typename InputElement>
bool compute_base_signal(const InputElement* input,
                         const double* row_weights,
                         const double* column_weights,
                         const jint rows,
                         const jint columns,
                         const double ln_pre_log_base,
                         const double resolution_scaling_coeff,
                         const double resolution_linear_scaling_coeff,
                         const bool apply_resolution_scaling,
                         const bool apply_resolution_linear_scaling,
                         const bool apply_cooler_weights,
                         double* output) {
#pragma omp parallel for schedule(static) if(static_cast<std::int64_t>(rows) * columns >= PARALLEL_THRESHOLD)
  for (jint row = 0; row < rows; ++row) {
    const auto row_weight = row_weights == nullptr ? 1.0 : row_weights[row];
    const auto row_offset = static_cast<std::int64_t>(row) * columns;
    for (jint column = 0; column < columns; ++column) {
      const auto column_weight = column_weights == nullptr ? 1.0 : column_weights[column];
      const auto offset = row_offset + column;
      output[offset] = transform_signal(
        static_cast<double>(input[offset]),
        row_weight,
        column_weight,
        ln_pre_log_base,
        resolution_scaling_coeff,
        resolution_linear_scaling_coeff,
        apply_resolution_scaling,
        apply_resolution_linear_scaling,
        apply_cooler_weights
      );
    }
  }
  return true;
}

bool apply_post_log(double* values, const jsize length, const double ln_post_log_base) {
  if (values == nullptr || !std::isfinite(ln_post_log_base) || ln_post_log_base <= 0.0) {
    return false;
  }
#pragma omp parallel for schedule(static) if(length >= PARALLEL_THRESHOLD)
  for (jsize i = 0; i < length; ++i) {
    const auto value = values[i];
    values[i] = std::isfinite(value) && value > 0.0 ? (std::log1p(value) / ln_post_log_base) : 0.0;
  }
  return true;
}

std::uint8_t to_u8(double value) {
  value = std::clamp(value, 0.0, 1.0);
  return static_cast<std::uint8_t>(std::lround(value * 255.0));
}

#if defined(HICT_NATIVE_AVX512) && defined(__AVX512F__)
bool map_linear_gradient_rgba_avx512(const double* signal,
                                     const std::int64_t element_count,
                                     const float* start_rgba,
                                     const double* delta,
                                     const double min_signal,
                                     const double signal_range,
                                     jbyte* output_rgba) {
  const auto min_v = _mm512_set1_pd(min_signal);
  const auto inv_range_v = _mm512_set1_pd(1.0 / signal_range);
  const auto zero_v = _mm512_setzero_pd();
  const auto one_v = _mm512_set1_pd(1.0);
  const auto block_count = element_count / 8;

#pragma omp parallel for schedule(static) if(element_count >= PARALLEL_THRESHOLD)
  for (std::int64_t block = 0; block < block_count; ++block) {
    const auto offset = block * 8;
    auto standardized_v = _mm512_mul_pd(_mm512_sub_pd(_mm512_loadu_pd(signal + offset), min_v), inv_range_v);
    standardized_v = _mm512_min_pd(_mm512_max_pd(standardized_v, zero_v), one_v);

    alignas(64) double standardized[8];
    _mm512_store_pd(standardized, standardized_v);
    for (int lane = 0; lane < 8; ++lane) {
      const auto output_offset = (offset + lane) * 4;
      const auto t = standardized[lane];
      for (int component = 0; component < 4; ++component) {
        const auto value = static_cast<double>(start_rgba[component]) + delta[component] * t;
        output_rgba[output_offset + component] = static_cast<jbyte>(to_u8(value));
      }
    }
  }

  for (std::int64_t offset = block_count * 8; offset < element_count; ++offset) {
    const auto standardized = std::clamp((signal[offset] - min_signal) / signal_range, 0.0, 1.0);
    const auto output_offset = offset * 4;
    for (int component = 0; component < 4; ++component) {
      const auto value = static_cast<double>(start_rgba[component]) + delta[component] * standardized;
      output_rgba[output_offset + component] = static_cast<jbyte>(to_u8(value));
    }
  }
  return true;
}
#endif

bool map_linear_gradient_rgba(const double* signal,
                              const jint rows,
                              const jint columns,
                              const float* start_rgba,
                              const float* end_rgba,
                              const double min_signal,
                              const double max_signal,
                              jbyte* output_rgba) {
  const auto signal_range = max_signal - min_signal;
  if (!std::isfinite(signal_range) || signal_range <= 0.0) {
    return false;
  }

  double delta[4]{};
  for (int component = 0; component < 4; ++component) {
    delta[component] = static_cast<double>(end_rgba[component]) - static_cast<double>(start_rgba[component]);
  }

  const auto element_count = static_cast<std::int64_t>(rows) * columns;
#if defined(HICT_NATIVE_AVX512) && defined(__AVX512F__)
  if (element_count >= 8) {
    return map_linear_gradient_rgba_avx512(signal, element_count, start_rgba, delta, min_signal, signal_range, output_rgba);
  }
#endif
#pragma omp parallel for schedule(static) if(element_count >= PARALLEL_THRESHOLD)
  for (std::int64_t offset = 0; offset < element_count; ++offset) {
    const auto standardized = std::clamp((signal[offset] - min_signal) / signal_range, 0.0, 1.0);
    const auto output_offset = offset * 4;
    for (int component = 0; component < 4; ++component) {
      const auto value = static_cast<double>(start_rgba[component]) + delta[component] * standardized;
      output_rgba[output_offset + component] = static_cast<jbyte>(to_u8(value));
    }
  }
  return true;
}

bool count_stripe_blocks(const jlong* column_bins,
                         const jsize column_count,
                         const jint stripe_count,
                         const jint submatrix_size,
                         const jint dense_threshold,
                         jlong* output_sparse_dense_counts) {
  if (column_bins == nullptr || output_sparse_dense_counts == nullptr || stripe_count <= 0 || submatrix_size <= 0 || dense_threshold <= 0) {
    return false;
  }

  std::vector<int> counts(static_cast<std::size_t>(stripe_count), 0);
  std::vector<int> touched;
  touched.reserve(static_cast<std::size_t>(std::min(stripe_count, column_count)));

  for (jsize i = 0; i < column_count; ++i) {
    const auto column_bin = column_bins[i];
    if (column_bin < 0) {
      return false;
    }
    const auto col_stripe = static_cast<std::int64_t>(column_bin / submatrix_size);
    if (col_stripe < 0 || col_stripe >= stripe_count) {
      return false;
    }
    auto& count = counts[static_cast<std::size_t>(col_stripe)];
    if (count++ == 0) {
      touched.push_back(static_cast<int>(col_stripe));
    }
  }

  std::sort(touched.begin(), touched.end());
  std::int64_t sparse = 0;
  std::int64_t dense = 0;
  for (const auto col_stripe : touched) {
    const auto count = counts[static_cast<std::size_t>(col_stripe)];
    if (count >= dense_threshold) {
      ++dense;
    } else {
      sparse += count;
    }
  }
  output_sparse_dense_counts[0] = static_cast<jlong>(sparse);
  output_sparse_dense_counts[1] = static_cast<jlong>(dense);
  return true;
}

bool aggregate_precomputed_series(const double* values,
                                  const jlong* support,
                                  const jsize series_length,
                                  const jlong query_start_px,
                                  const jlong query_end_px,
                                  const jint bucket_count,
                                  const jint strategy_code,
                                  double* output_values,
                                  jlong* output_support) {
  if (values == nullptr || support == nullptr || output_values == nullptr || output_support == nullptr) {
    return false;
  }
  if (series_length <= 0 || bucket_count <= 0 || query_end_px <= query_start_px) {
    return false;
  }
  if (strategy_code < 1 || strategy_code > 4) {
    return false;
  }
  const auto span = static_cast<double>(std::max<jlong>(1, query_end_px - query_start_px));
  const auto bucket_span = std::max(1.0, span / static_cast<double>(bucket_count));

#pragma omp parallel for schedule(static) if(bucket_count >= 1024)
  for (jint bucket = 0; bucket < bucket_count; ++bucket) {
    const auto start_px = query_start_px + static_cast<jlong>(std::floor(bucket * bucket_span));
    const auto end_px = std::min<jlong>(query_end_px, query_start_px + static_cast<jlong>(std::ceil((bucket + 1) * bucket_span)));
    const auto safe_end_px = std::max<jlong>(start_px + 1, end_px);
    const auto from = static_cast<jsize>(std::max<jlong>(0, std::min<jlong>(start_px, series_length - 1)));
    const auto to = static_cast<jsize>(std::max<jlong>(from + 1, std::min<jlong>(safe_end_px, series_length)));

    double max_value = 0.0;
    double sum_value = 0.0;
    jlong support_sum = 0;
    jlong support_count = 0;
    for (jsize idx = from; idx < to; ++idx) {
      const auto value = values[idx];
      const auto current_support = support[idx];
      max_value = std::max(max_value, value);
      sum_value += value;
      support_sum += current_support;
      if (current_support > 0) {
        ++support_count;
      }
    }

    double result = 0.0;
    switch (strategy_code) {
      case 1:
        result = max_value;
        break;
      case 2:
        result = sum_value / std::max(1.0, static_cast<double>(to - from));
        break;
      case 3:
        result = support_count > 0 ? (sum_value / static_cast<double>(support_count)) : 0.0;
        break;
      case 4:
        result = sum_value;
        break;
      default:
        result = 0.0;
        break;
    }
    output_values[bucket] = std::isfinite(result) ? result : 0.0;
    output_support[bucket] = support_sum;
  }
  return true;
}

bool aggregate_intervals(const jlong* starts,
                         const jlong* ends,
                         const double* values,
                         const jsize feature_count,
                         const jlong query_start_px,
                         const jlong query_end_px,
                         const jint bucket_count,
                         const jint mode_code,
                         double* output_values,
                         jlong* output_counts) {
  if (starts == nullptr || ends == nullptr || output_values == nullptr || output_counts == nullptr) {
    return false;
  }
  if (feature_count < 0 || bucket_count <= 0 || query_end_px <= query_start_px) {
    return false;
  }
  if (mode_code < 1 || mode_code > 5) {
    return false;
  }
  if ((mode_code == 1 || mode_code == 2 || mode_code == 3) && values == nullptr) {
    return false;
  }

  std::fill(output_values, output_values + bucket_count, 0.0);
  std::fill(output_counts, output_counts + bucket_count, 0);

  const auto span = static_cast<double>(std::max<jlong>(1, query_end_px - query_start_px));
  const auto bucket_span = std::max(1.0, span / static_cast<double>(bucket_count));
  std::vector<double> weighted_sums;
  std::vector<double> overlap_sums;
  if (mode_code == 2 || mode_code == 3) {
    weighted_sums.assign(static_cast<std::size_t>(bucket_count), 0.0);
    overlap_sums.assign(static_cast<std::size_t>(bucket_count), 0.0);
  }

  for (jsize feature = 0; feature < feature_count; ++feature) {
    const auto feature_start = starts[feature];
    const auto feature_end = ends[feature];
    if (feature_end <= query_start_px || feature_start >= query_end_px || feature_end <= feature_start) {
      continue;
    }

    if (mode_code == 5) {
      const auto center = feature_start + ((feature_end - feature_start) >> 1);
      auto bucket = static_cast<jint>(std::floor((center - query_start_px) / bucket_span));
      bucket = std::max<jint>(0, std::min<jint>(bucket, bucket_count - 1));
      output_values[bucket] += 1.0;
      output_counts[bucket] += 1;
      continue;
    }

    auto left = static_cast<jint>(std::floor((feature_start - query_start_px) / bucket_span));
    auto right = static_cast<jint>(std::ceil((feature_end - query_start_px) / bucket_span)) - 1;
    left = std::max<jint>(0, std::min<jint>(left, bucket_count - 1));
    right = std::max<jint>(0, std::min<jint>(right, bucket_count - 1));
    const auto feature_value = values == nullptr ? 1.0 : values[feature];
    for (jint bucket = left; bucket <= right; ++bucket) {
      const auto bucket_start = query_start_px + static_cast<jlong>(std::floor(bucket * bucket_span));
      const auto bucket_end = std::min<jlong>(query_end_px, query_start_px + static_cast<jlong>(std::ceil((bucket + 1) * bucket_span)));
      const auto overlap = std::min<jlong>(feature_end, bucket_end) - std::max<jlong>(feature_start, bucket_start);
      if (overlap <= 0) {
        continue;
      }

      switch (mode_code) {
        case 1:
          output_values[bucket] = std::max(output_values[bucket], feature_value);
          break;
        case 2:
        case 3:
          weighted_sums[static_cast<std::size_t>(bucket)] += feature_value * static_cast<double>(overlap);
          overlap_sums[static_cast<std::size_t>(bucket)] += static_cast<double>(overlap);
          break;
        case 4:
          output_values[bucket] += static_cast<double>(overlap) / std::max(1.0, static_cast<double>(bucket_end - bucket_start));
          break;
        default:
          break;
      }
      output_counts[bucket] += 1;
    }
  }

  if (mode_code == 2 || mode_code == 3) {
    for (jint bucket = 0; bucket < bucket_count; ++bucket) {
      if (output_counts[bucket] <= 0) {
        continue;
      }
      const auto start_px = query_start_px + static_cast<jlong>(std::floor(bucket * bucket_span));
      const auto end_px = std::min<jlong>(query_end_px, query_start_px + static_cast<jlong>(std::ceil((bucket + 1) * bucket_span)));
      const auto bucket_width = std::max(1.0, static_cast<double>(std::max<jlong>(start_px + 1, end_px) - start_px));
      if (mode_code == 2) {
        output_values[bucket] = overlap_sums[static_cast<std::size_t>(bucket)] > 0.0
          ? weighted_sums[static_cast<std::size_t>(bucket)] / overlap_sums[static_cast<std::size_t>(bucket)]
          : 0.0;
      } else {
        output_values[bucket] = weighted_sums[static_cast<std::size_t>(bucket)] / bucket_width;
      }
    }
  }
  return true;
}

std::uint8_t complement_ascii(const std::uint8_t base) {
  switch (base) {
    case 'A':
    case 'a':
      return 'T';
    case 'T':
    case 't':
      return 'A';
    case 'C':
    case 'c':
      return 'G';
    case 'G':
    case 'g':
      return 'C';
    case 'N':
    case 'n':
      return 'N';
    case 'R':
    case 'r':
      return 'Y';
    case 'Y':
    case 'y':
      return 'R';
    case 'S':
    case 's':
      return 'S';
    case 'W':
    case 'w':
      return 'W';
    case 'K':
    case 'k':
      return 'M';
    case 'M':
    case 'm':
      return 'K';
    case 'B':
    case 'b':
      return 'V';
    case 'D':
    case 'd':
      return 'H';
    case 'H':
    case 'h':
      return 'D';
    case 'V':
    case 'v':
      return 'B';
    default:
      return 'N';
  }
}

bool reverse_complement_ascii(const jbyte* input, const jsize length, jbyte* output) {
  if (input == nullptr || output == nullptr || length < 0) {
    return false;
  }
#pragma omp parallel for schedule(static) if(length >= PARALLEL_THRESHOLD)
  for (jsize i = 0; i < length; ++i) {
    const auto src = static_cast<std::uint8_t>(input[length - i - 1]);
    output[i] = static_cast<jbyte>(complement_ascii(src));
  }
  return true;
}

template <typename ValueElement>
bool sort_sparse_block_row_major(jlong* rows,
                                 jlong* columns,
                                 ValueElement* values,
                                 const jsize length,
                                 const jint submatrix_size) {
  if (rows == nullptr || columns == nullptr || values == nullptr || length < 0 || submatrix_size <= 0) {
    return false;
  }
  if (length <= 1) {
    return true;
  }
  const auto bucket_count_64 = static_cast<std::int64_t>(submatrix_size) * static_cast<std::int64_t>(submatrix_size);
  if (bucket_count_64 <= 0 || bucket_count_64 > 1'048'576) {
    return false;
  }
  const auto bucket_count = static_cast<std::size_t>(bucket_count_64);
  std::vector<int> counts(bucket_count, 0);
  std::vector<int> keys(static_cast<std::size_t>(length));
  for (jsize i = 0; i < length; ++i) {
    if (rows[i] < 0 || rows[i] >= submatrix_size || columns[i] < 0 || columns[i] >= submatrix_size) {
      return false;
    }
    const auto key = static_cast<int>(rows[i] * submatrix_size + columns[i]);
    keys[static_cast<std::size_t>(i)] = key;
    counts[static_cast<std::size_t>(key)]++;
  }

  int prefix = 0;
  for (auto& count : counts) {
    const auto current = count;
    count = prefix;
    prefix += current;
  }

  std::vector<jlong> sorted_rows(static_cast<std::size_t>(length));
  std::vector<jlong> sorted_columns(static_cast<std::size_t>(length));
  std::vector<ValueElement> sorted_values(static_cast<std::size_t>(length));
  for (jsize i = 0; i < length; ++i) {
    const auto key = keys[static_cast<std::size_t>(i)];
    const auto position = counts[static_cast<std::size_t>(key)]++;
    sorted_rows[static_cast<std::size_t>(position)] = rows[i];
    sorted_columns[static_cast<std::size_t>(position)] = columns[i];
    sorted_values[static_cast<std::size_t>(position)] = values[i];
  }

  std::copy(sorted_rows.begin(), sorted_rows.end(), rows);
  std::copy(sorted_columns.begin(), sorted_columns.end(), columns);
  std::copy(sorted_values.begin(), sorted_values.end(), values);
  return true;
}

bool transform_expected_signal(const double* signal,
                               const jint rows,
                               const jint columns,
                               const jlong start_row_px,
                               const jlong start_col_px,
                               const jint display_mode_code,
                               const jlong min_diagonal,
                               const double* diagonal_means,
                               const jsize diagonal_mean_count,
                               double* output) {
  if (signal == nullptr || diagonal_means == nullptr || output == nullptr) {
    return false;
  }
  if (display_mode_code != 1 && display_mode_code != 2) {
    return false;
  }

#pragma omp parallel for schedule(static) if(static_cast<std::int64_t>(rows) * columns >= PARALLEL_THRESHOLD)
  for (jint row = 0; row < rows; ++row) {
    const auto absolute_row_px = start_row_px + row;
    const auto row_to_col_delta = start_col_px - absolute_row_px;
    const auto row_offset = static_cast<std::int64_t>(row) * columns;
    for (jint column = 0; column < columns; ++column) {
      const auto delta = row_to_col_delta + column;
      const auto diagonal = delta >= 0 ? delta : -delta;
      const auto diagonal_index = diagonal - min_diagonal;
      double expected = 0.0;
      if (diagonal_index >= 0 && diagonal_index < diagonal_mean_count) {
        expected = diagonal_means[diagonal_index];
      }

      const auto offset = row_offset + column;
      if (display_mode_code == 1) {
        output[offset] = expected;
      } else {
        const auto observed = sanitize_positive_signal(signal[offset]);
        output[offset] = expected > 1.0e-12 && std::isfinite(expected) ? observed / expected : 0.0;
      }
    }
  }
  return true;
}

} // namespace

extern "C" JNIEXPORT jstring JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeVersion(JNIEnv* env, jclass) {
  return env->NewStringUTF(HICT_NATIVE_VERSION);
}

extern "C" JNIEXPORT jlong JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeOpenSession(JNIEnv*, jclass) {
  auto* session = new (std::nothrow) NativeBackendSession();
  return reinterpret_cast<jlong>(session);
}

extern "C" JNIEXPORT jlong JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeSessionOperationCount(
  JNIEnv*,
  jclass,
  jlong session_handle
) {
  auto* session = session_from_handle(session_handle);
  return session == nullptr ? 0 : session->operation_count.load(std::memory_order_relaxed);
}

extern "C" JNIEXPORT jlong JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeSessionFailedOperationCount(
  JNIEnv*,
  jclass,
  jlong session_handle
) {
  auto* session = session_from_handle(session_handle);
  return session == nullptr ? 0 : session->failed_operation_count.load(std::memory_order_relaxed);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeSessionHdf5Available(
  JNIEnv*,
  jclass,
  jlong session_handle
) {
  auto* session = session_from_handle(session_handle);
  return session != nullptr && session->hdf5_backend_available ? JNI_TRUE : JNI_FALSE;
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeComputeBaseSignalDouble(
  JNIEnv* env,
  jclass,
  jlong session_handle,
  jdoubleArray input_array,
  jdoubleArray row_weights_array,
  jdoubleArray column_weights_array,
  jint rows,
  jint columns,
  jdouble ln_pre_log_base,
  jdouble resolution_scaling_coeff,
  jdouble resolution_linear_scaling_coeff,
  jboolean apply_resolution_scaling,
  jboolean apply_resolution_linear_scaling,
  jboolean apply_cooler_weights,
  jdoubleArray output_array
) {
  if (session_from_handle(session_handle) == nullptr) {
    return JNI_FALSE;
  }
  if (input_array == nullptr || output_array == nullptr) {
    return native_failure(session_handle);
  }
  const auto input_length = env->GetArrayLength(input_array);
  const auto output_length = env->GetArrayLength(output_array);
  if (!valid_extent(rows, columns, input_length) || !valid_extent(rows, columns, output_length)) {
    return native_failure(session_handle);
  }
  if (row_weights_array != nullptr && env->GetArrayLength(row_weights_array) < rows) {
    return native_failure(session_handle);
  }
  if (column_weights_array != nullptr && env->GetArrayLength(column_weights_array) < columns) {
    return native_failure(session_handle);
  }

  auto* input = env->GetDoubleArrayElements(input_array, nullptr);
  auto* row_weights = row_weights_array == nullptr ? nullptr : env->GetDoubleArrayElements(row_weights_array, nullptr);
  auto* column_weights = column_weights_array == nullptr ? nullptr : env->GetDoubleArrayElements(column_weights_array, nullptr);
  auto* output = env->GetDoubleArrayElements(output_array, nullptr);
  if (input == nullptr || output == nullptr) {
    if (input != nullptr) {
      env->ReleaseDoubleArrayElements(input_array, input, JNI_ABORT);
    }
    if (row_weights != nullptr) {
      env->ReleaseDoubleArrayElements(row_weights_array, row_weights, JNI_ABORT);
    }
    if (column_weights != nullptr) {
      env->ReleaseDoubleArrayElements(column_weights_array, column_weights, JNI_ABORT);
    }
    if (output != nullptr) {
      env->ReleaseDoubleArrayElements(output_array, output, JNI_ABORT);
    }
    return native_failure(session_handle);
  }

  const auto ok = compute_base_signal(
    input,
    row_weights,
    column_weights,
    rows,
    columns,
    ln_pre_log_base,
    resolution_scaling_coeff,
    resolution_linear_scaling_coeff,
    apply_resolution_scaling == JNI_TRUE,
    apply_resolution_linear_scaling == JNI_TRUE,
    apply_cooler_weights == JNI_TRUE,
    output
  );

  env->ReleaseDoubleArrayElements(input_array, input, JNI_ABORT);
  if (row_weights != nullptr) {
    env->ReleaseDoubleArrayElements(row_weights_array, row_weights, JNI_ABORT);
  }
  if (column_weights != nullptr) {
    env->ReleaseDoubleArrayElements(column_weights_array, column_weights, JNI_ABORT);
  }
  env->ReleaseDoubleArrayElements(output_array, output, ok ? 0 : JNI_ABORT);
  return native_result(session_handle, ok);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeComputeBaseSignalLong(
  JNIEnv* env,
  jclass,
  jlong session_handle,
  jlongArray input_array,
  jdoubleArray row_weights_array,
  jdoubleArray column_weights_array,
  jint rows,
  jint columns,
  jdouble ln_pre_log_base,
  jdouble resolution_scaling_coeff,
  jdouble resolution_linear_scaling_coeff,
  jboolean apply_resolution_scaling,
  jboolean apply_resolution_linear_scaling,
  jboolean apply_cooler_weights,
  jdoubleArray output_array
) {
  if (session_from_handle(session_handle) == nullptr) {
    return JNI_FALSE;
  }
  if (input_array == nullptr || output_array == nullptr) {
    return native_failure(session_handle);
  }
  const auto input_length = env->GetArrayLength(input_array);
  const auto output_length = env->GetArrayLength(output_array);
  if (!valid_extent(rows, columns, input_length) || !valid_extent(rows, columns, output_length)) {
    return native_failure(session_handle);
  }
  if (row_weights_array != nullptr && env->GetArrayLength(row_weights_array) < rows) {
    return native_failure(session_handle);
  }
  if (column_weights_array != nullptr && env->GetArrayLength(column_weights_array) < columns) {
    return native_failure(session_handle);
  }

  auto* input = env->GetLongArrayElements(input_array, nullptr);
  auto* row_weights = row_weights_array == nullptr ? nullptr : env->GetDoubleArrayElements(row_weights_array, nullptr);
  auto* column_weights = column_weights_array == nullptr ? nullptr : env->GetDoubleArrayElements(column_weights_array, nullptr);
  auto* output = env->GetDoubleArrayElements(output_array, nullptr);
  if (input == nullptr || output == nullptr) {
    if (input != nullptr) {
      env->ReleaseLongArrayElements(input_array, input, JNI_ABORT);
    }
    if (row_weights != nullptr) {
      env->ReleaseDoubleArrayElements(row_weights_array, row_weights, JNI_ABORT);
    }
    if (column_weights != nullptr) {
      env->ReleaseDoubleArrayElements(column_weights_array, column_weights, JNI_ABORT);
    }
    if (output != nullptr) {
      env->ReleaseDoubleArrayElements(output_array, output, JNI_ABORT);
    }
    return native_failure(session_handle);
  }

  const auto ok = compute_base_signal(
    input,
    row_weights,
    column_weights,
    rows,
    columns,
    ln_pre_log_base,
    resolution_scaling_coeff,
    resolution_linear_scaling_coeff,
    apply_resolution_scaling == JNI_TRUE,
    apply_resolution_linear_scaling == JNI_TRUE,
    apply_cooler_weights == JNI_TRUE,
    output
  );

  env->ReleaseLongArrayElements(input_array, input, JNI_ABORT);
  if (row_weights != nullptr) {
    env->ReleaseDoubleArrayElements(row_weights_array, row_weights, JNI_ABORT);
  }
  if (column_weights != nullptr) {
    env->ReleaseDoubleArrayElements(column_weights_array, column_weights, JNI_ABORT);
  }
  env->ReleaseDoubleArrayElements(output_array, output, ok ? 0 : JNI_ABORT);
  return native_result(session_handle, ok);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeMapLinearGradientRgba(
  JNIEnv* env,
  jclass,
  jlong session_handle,
  jdoubleArray signal_array,
  jint rows,
  jint columns,
  jfloatArray start_rgba_array,
  jfloatArray end_rgba_array,
  jdouble min_signal,
  jdouble max_signal,
  jbyteArray output_rgba_array
) {
  if (session_from_handle(session_handle) == nullptr) {
    return JNI_FALSE;
  }
  if (signal_array == nullptr || start_rgba_array == nullptr || end_rgba_array == nullptr || output_rgba_array == nullptr) {
    return native_failure(session_handle);
  }
  const auto signal_length = env->GetArrayLength(signal_array);
  const auto output_length = env->GetArrayLength(output_rgba_array);
  if (!valid_extent(rows, columns, signal_length)) {
    return native_failure(session_handle);
  }
  const auto element_count = static_cast<std::int64_t>(rows) * columns;
  if (output_length < element_count * 4 || env->GetArrayLength(start_rgba_array) < 4 || env->GetArrayLength(end_rgba_array) < 4) {
    return native_failure(session_handle);
  }

  auto* signal = env->GetDoubleArrayElements(signal_array, nullptr);
  auto* start_rgba = env->GetFloatArrayElements(start_rgba_array, nullptr);
  auto* end_rgba = env->GetFloatArrayElements(end_rgba_array, nullptr);
  auto* output_rgba = env->GetByteArrayElements(output_rgba_array, nullptr);
  if (signal == nullptr || start_rgba == nullptr || end_rgba == nullptr || output_rgba == nullptr) {
    if (signal != nullptr) {
      env->ReleaseDoubleArrayElements(signal_array, signal, JNI_ABORT);
    }
    if (start_rgba != nullptr) {
      env->ReleaseFloatArrayElements(start_rgba_array, start_rgba, JNI_ABORT);
    }
    if (end_rgba != nullptr) {
      env->ReleaseFloatArrayElements(end_rgba_array, end_rgba, JNI_ABORT);
    }
    if (output_rgba != nullptr) {
      env->ReleaseByteArrayElements(output_rgba_array, output_rgba, JNI_ABORT);
    }
    return native_failure(session_handle);
  }

  const auto ok = map_linear_gradient_rgba(signal, rows, columns, start_rgba, end_rgba, min_signal, max_signal, output_rgba);

  env->ReleaseDoubleArrayElements(signal_array, signal, JNI_ABORT);
  env->ReleaseFloatArrayElements(start_rgba_array, start_rgba, JNI_ABORT);
  env->ReleaseFloatArrayElements(end_rgba_array, end_rgba, JNI_ABORT);
  env->ReleaseByteArrayElements(output_rgba_array, output_rgba, ok ? 0 : JNI_ABORT);
  return native_result(session_handle, ok);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeApplyPostLog(
  JNIEnv* env,
  jclass,
  jlong session_handle,
  jdoubleArray values_array,
  jdouble ln_post_log_base
) {
  if (session_from_handle(session_handle) == nullptr) {
    return JNI_FALSE;
  }
  if (values_array == nullptr) {
    return native_failure(session_handle);
  }
  auto* values = static_cast<jdouble*>(env->GetPrimitiveArrayCritical(values_array, nullptr));
  if (values == nullptr) {
    return native_failure(session_handle);
  }
  const auto ok = apply_post_log(values, env->GetArrayLength(values_array), ln_post_log_base);
  env->ReleasePrimitiveArrayCritical(values_array, values, ok ? 0 : JNI_ABORT);
  return native_result(session_handle, ok);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeCountStripeBlocks(
  JNIEnv* env,
  jclass,
  jlong session_handle,
  jlongArray column_bins_array,
  jint stripe_count,
  jint submatrix_size,
  jint dense_threshold,
  jlongArray output_sparse_dense_counts_array
) {
  if (session_from_handle(session_handle) == nullptr) {
    return JNI_FALSE;
  }
  if (column_bins_array == nullptr || output_sparse_dense_counts_array == nullptr) {
    return native_failure(session_handle);
  }
  if (env->GetArrayLength(output_sparse_dense_counts_array) < 2) {
    return native_failure(session_handle);
  }

  const auto column_count = env->GetArrayLength(column_bins_array);
  auto* column_bins = env->GetLongArrayElements(column_bins_array, nullptr);
  auto* output_sparse_dense_counts = env->GetLongArrayElements(output_sparse_dense_counts_array, nullptr);
  if (column_bins == nullptr || output_sparse_dense_counts == nullptr) {
    if (column_bins != nullptr) {
      env->ReleaseLongArrayElements(column_bins_array, column_bins, JNI_ABORT);
    }
    if (output_sparse_dense_counts != nullptr) {
      env->ReleaseLongArrayElements(output_sparse_dense_counts_array, output_sparse_dense_counts, JNI_ABORT);
    }
    return native_failure(session_handle);
  }

  const auto ok = count_stripe_blocks(
    column_bins,
    column_count,
    stripe_count,
    submatrix_size,
    dense_threshold,
    output_sparse_dense_counts
  );

  env->ReleaseLongArrayElements(column_bins_array, column_bins, JNI_ABORT);
  env->ReleaseLongArrayElements(output_sparse_dense_counts_array, output_sparse_dense_counts, ok ? 0 : JNI_ABORT);
  return native_result(session_handle, ok);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeAggregatePrecomputedSeries(
  JNIEnv* env,
  jclass,
  jlong session_handle,
  jdoubleArray values_array,
  jlongArray support_array,
  jlong query_start_px,
  jlong query_end_px,
  jint bucket_count,
  jint strategy_code,
  jdoubleArray output_values_array,
  jlongArray output_support_array
) {
  if (session_from_handle(session_handle) == nullptr) {
    return JNI_FALSE;
  }
  if (values_array == nullptr || support_array == nullptr || output_values_array == nullptr || output_support_array == nullptr) {
    return native_failure(session_handle);
  }
  const auto series_length = env->GetArrayLength(values_array);
  if (env->GetArrayLength(support_array) < series_length ||
      env->GetArrayLength(output_values_array) < bucket_count ||
      env->GetArrayLength(output_support_array) < bucket_count) {
    return native_failure(session_handle);
  }

  auto* values = env->GetDoubleArrayElements(values_array, nullptr);
  auto* support = env->GetLongArrayElements(support_array, nullptr);
  auto* output_values = env->GetDoubleArrayElements(output_values_array, nullptr);
  auto* output_support = env->GetLongArrayElements(output_support_array, nullptr);
  if (values == nullptr || support == nullptr || output_values == nullptr || output_support == nullptr) {
    if (values != nullptr) {
      env->ReleaseDoubleArrayElements(values_array, values, JNI_ABORT);
    }
    if (support != nullptr) {
      env->ReleaseLongArrayElements(support_array, support, JNI_ABORT);
    }
    if (output_values != nullptr) {
      env->ReleaseDoubleArrayElements(output_values_array, output_values, JNI_ABORT);
    }
    if (output_support != nullptr) {
      env->ReleaseLongArrayElements(output_support_array, output_support, JNI_ABORT);
    }
    return native_failure(session_handle);
  }

  const auto ok = aggregate_precomputed_series(
    values,
    support,
    series_length,
    query_start_px,
    query_end_px,
    bucket_count,
    strategy_code,
    output_values,
    output_support
  );

  env->ReleaseDoubleArrayElements(values_array, values, JNI_ABORT);
  env->ReleaseLongArrayElements(support_array, support, JNI_ABORT);
  env->ReleaseDoubleArrayElements(output_values_array, output_values, ok ? 0 : JNI_ABORT);
  env->ReleaseLongArrayElements(output_support_array, output_support, ok ? 0 : JNI_ABORT);
  return native_result(session_handle, ok);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeAggregateIntervals(
  JNIEnv* env,
  jclass,
  jlong session_handle,
  jlongArray starts_array,
  jlongArray ends_array,
  jdoubleArray values_array,
  jlong query_start_px,
  jlong query_end_px,
  jint bucket_count,
  jint mode_code,
  jdoubleArray output_values_array,
  jlongArray output_counts_array
) {
  if (session_from_handle(session_handle) == nullptr) {
    return JNI_FALSE;
  }
  if (starts_array == nullptr || ends_array == nullptr || output_values_array == nullptr || output_counts_array == nullptr) {
    return native_failure(session_handle);
  }
  const auto feature_count = env->GetArrayLength(starts_array);
  if (env->GetArrayLength(ends_array) < feature_count ||
      (values_array != nullptr && env->GetArrayLength(values_array) < feature_count) ||
      env->GetArrayLength(output_values_array) < bucket_count ||
      env->GetArrayLength(output_counts_array) < bucket_count) {
    return native_failure(session_handle);
  }

  auto* starts = env->GetLongArrayElements(starts_array, nullptr);
  auto* ends = env->GetLongArrayElements(ends_array, nullptr);
  auto* values = values_array == nullptr ? nullptr : env->GetDoubleArrayElements(values_array, nullptr);
  auto* output_values = env->GetDoubleArrayElements(output_values_array, nullptr);
  auto* output_counts = env->GetLongArrayElements(output_counts_array, nullptr);
  if (starts == nullptr || ends == nullptr || output_values == nullptr || output_counts == nullptr || (values_array != nullptr && values == nullptr)) {
    if (starts != nullptr) {
      env->ReleaseLongArrayElements(starts_array, starts, JNI_ABORT);
    }
    if (ends != nullptr) {
      env->ReleaseLongArrayElements(ends_array, ends, JNI_ABORT);
    }
    if (values != nullptr) {
      env->ReleaseDoubleArrayElements(values_array, values, JNI_ABORT);
    }
    if (output_values != nullptr) {
      env->ReleaseDoubleArrayElements(output_values_array, output_values, JNI_ABORT);
    }
    if (output_counts != nullptr) {
      env->ReleaseLongArrayElements(output_counts_array, output_counts, JNI_ABORT);
    }
    return native_failure(session_handle);
  }

  const auto ok = aggregate_intervals(
    starts,
    ends,
    values,
    feature_count,
    query_start_px,
    query_end_px,
    bucket_count,
    mode_code,
    output_values,
    output_counts
  );

  env->ReleaseLongArrayElements(starts_array, starts, JNI_ABORT);
  env->ReleaseLongArrayElements(ends_array, ends, JNI_ABORT);
  if (values != nullptr) {
    env->ReleaseDoubleArrayElements(values_array, values, JNI_ABORT);
  }
  env->ReleaseDoubleArrayElements(output_values_array, output_values, ok ? 0 : JNI_ABORT);
  env->ReleaseLongArrayElements(output_counts_array, output_counts, ok ? 0 : JNI_ABORT);
  return native_result(session_handle, ok);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeReverseComplementAscii(
  JNIEnv* env,
  jclass,
  jlong session_handle,
  jbyteArray input_array,
  jbyteArray output_array
) {
  if (session_from_handle(session_handle) == nullptr) {
    return JNI_FALSE;
  }
  if (input_array == nullptr || output_array == nullptr) {
    return native_failure(session_handle);
  }
  const auto length = env->GetArrayLength(input_array);
  if (env->GetArrayLength(output_array) < length) {
    return native_failure(session_handle);
  }

  auto* input = static_cast<jbyte*>(env->GetPrimitiveArrayCritical(input_array, nullptr));
  auto* output = static_cast<jbyte*>(env->GetPrimitiveArrayCritical(output_array, nullptr));
  if (input == nullptr || output == nullptr) {
    if (input != nullptr) {
      env->ReleasePrimitiveArrayCritical(input_array, input, JNI_ABORT);
    }
    if (output != nullptr) {
      env->ReleasePrimitiveArrayCritical(output_array, output, JNI_ABORT);
    }
    return native_failure(session_handle);
  }
  const auto ok = reverse_complement_ascii(input, length, output);
  env->ReleasePrimitiveArrayCritical(input_array, input, JNI_ABORT);
  env->ReleasePrimitiveArrayCritical(output_array, output, ok ? 0 : JNI_ABORT);
  return native_result(session_handle, ok);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeSortSparseBlockDouble(
  JNIEnv* env,
  jclass,
  jlong session_handle,
  jlongArray rows_array,
  jlongArray columns_array,
  jdoubleArray values_array,
  jint submatrix_size
) {
  if (session_from_handle(session_handle) == nullptr) {
    return JNI_FALSE;
  }
  if (rows_array == nullptr || columns_array == nullptr || values_array == nullptr) {
    return native_failure(session_handle);
  }
  const auto length = env->GetArrayLength(rows_array);
  if (env->GetArrayLength(columns_array) < length || env->GetArrayLength(values_array) < length) {
    return native_failure(session_handle);
  }

  auto* rows = env->GetLongArrayElements(rows_array, nullptr);
  auto* columns = env->GetLongArrayElements(columns_array, nullptr);
  auto* values = env->GetDoubleArrayElements(values_array, nullptr);
  if (rows == nullptr || columns == nullptr || values == nullptr) {
    if (rows != nullptr) {
      env->ReleaseLongArrayElements(rows_array, rows, JNI_ABORT);
    }
    if (columns != nullptr) {
      env->ReleaseLongArrayElements(columns_array, columns, JNI_ABORT);
    }
    if (values != nullptr) {
      env->ReleaseDoubleArrayElements(values_array, values, JNI_ABORT);
    }
    return native_failure(session_handle);
  }

  const auto ok = sort_sparse_block_row_major(rows, columns, values, length, submatrix_size);
  env->ReleaseLongArrayElements(rows_array, rows, ok ? 0 : JNI_ABORT);
  env->ReleaseLongArrayElements(columns_array, columns, ok ? 0 : JNI_ABORT);
  env->ReleaseDoubleArrayElements(values_array, values, ok ? 0 : JNI_ABORT);
  return native_result(session_handle, ok);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeSortSparseBlockLong(
  JNIEnv* env,
  jclass,
  jlong session_handle,
  jlongArray rows_array,
  jlongArray columns_array,
  jlongArray values_array,
  jint submatrix_size
) {
  if (session_from_handle(session_handle) == nullptr) {
    return JNI_FALSE;
  }
  if (rows_array == nullptr || columns_array == nullptr || values_array == nullptr) {
    return native_failure(session_handle);
  }
  const auto length = env->GetArrayLength(rows_array);
  if (env->GetArrayLength(columns_array) < length || env->GetArrayLength(values_array) < length) {
    return native_failure(session_handle);
  }

  auto* rows = env->GetLongArrayElements(rows_array, nullptr);
  auto* columns = env->GetLongArrayElements(columns_array, nullptr);
  auto* values = env->GetLongArrayElements(values_array, nullptr);
  if (rows == nullptr || columns == nullptr || values == nullptr) {
    if (rows != nullptr) {
      env->ReleaseLongArrayElements(rows_array, rows, JNI_ABORT);
    }
    if (columns != nullptr) {
      env->ReleaseLongArrayElements(columns_array, columns, JNI_ABORT);
    }
    if (values != nullptr) {
      env->ReleaseLongArrayElements(values_array, values, JNI_ABORT);
    }
    return native_failure(session_handle);
  }

  const auto ok = sort_sparse_block_row_major(rows, columns, values, length, submatrix_size);
  env->ReleaseLongArrayElements(rows_array, rows, ok ? 0 : JNI_ABORT);
  env->ReleaseLongArrayElements(columns_array, columns, ok ? 0 : JNI_ABORT);
  env->ReleaseLongArrayElements(values_array, values, ok ? 0 : JNI_ABORT);
  return native_result(session_handle, ok);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeTransformExpectedSignal(
  JNIEnv* env,
  jclass,
  jlong session_handle,
  jdoubleArray signal_array,
  jint rows,
  jint columns,
  jlong start_row_px,
  jlong start_col_px,
  jint display_mode_code,
  jlong min_diagonal,
  jdoubleArray diagonal_means_array,
  jdoubleArray output_array
) {
  if (session_from_handle(session_handle) == nullptr) {
    return JNI_FALSE;
  }
  if (signal_array == nullptr || diagonal_means_array == nullptr || output_array == nullptr) {
    return native_failure(session_handle);
  }
  const auto signal_length = env->GetArrayLength(signal_array);
  const auto output_length = env->GetArrayLength(output_array);
  if (!valid_extent(rows, columns, signal_length) || !valid_extent(rows, columns, output_length)) {
    return native_failure(session_handle);
  }

  auto* signal = static_cast<jdouble*>(env->GetPrimitiveArrayCritical(signal_array, nullptr));
  auto* diagonal_means = static_cast<jdouble*>(env->GetPrimitiveArrayCritical(diagonal_means_array, nullptr));
  auto* output = static_cast<jdouble*>(env->GetPrimitiveArrayCritical(output_array, nullptr));
  if (signal == nullptr || diagonal_means == nullptr || output == nullptr) {
    if (signal != nullptr) {
      env->ReleasePrimitiveArrayCritical(signal_array, signal, JNI_ABORT);
    }
    if (diagonal_means != nullptr) {
      env->ReleasePrimitiveArrayCritical(diagonal_means_array, diagonal_means, JNI_ABORT);
    }
    if (output != nullptr) {
      env->ReleasePrimitiveArrayCritical(output_array, output, JNI_ABORT);
    }
    return native_failure(session_handle);
  }

  const auto ok = transform_expected_signal(
    signal,
    rows,
    columns,
    start_row_px,
    start_col_px,
    display_mode_code,
    min_diagonal,
    diagonal_means,
    env->GetArrayLength(diagonal_means_array),
    output
  );

  env->ReleasePrimitiveArrayCritical(signal_array, signal, JNI_ABORT);
  env->ReleasePrimitiveArrayCritical(diagonal_means_array, diagonal_means, JNI_ABORT);
  env->ReleasePrimitiveArrayCritical(output_array, output, ok ? 0 : JNI_ABORT);
  return native_result(session_handle, ok);
}
