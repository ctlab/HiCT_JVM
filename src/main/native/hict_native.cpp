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
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <vector>

namespace {

#ifndef HICT_NATIVE_VARIANT
#define HICT_NATIVE_VARIANT "baseline"
#endif

constexpr const char* HICT_NATIVE_VERSION = "hict-native-processing/0.2-" HICT_NATIVE_VARIANT;

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

std::uint8_t to_u8(double value) {
  value = std::clamp(value, 0.0, 1.0);
  return static_cast<std::uint8_t>(std::lround(value * 255.0));
}

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

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeComputeBaseSignalDouble(
  JNIEnv* env,
  jclass,
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
  if (input_array == nullptr || output_array == nullptr) {
    return JNI_FALSE;
  }
  const auto input_length = env->GetArrayLength(input_array);
  const auto output_length = env->GetArrayLength(output_array);
  if (!valid_extent(rows, columns, input_length) || !valid_extent(rows, columns, output_length)) {
    return JNI_FALSE;
  }
  if (row_weights_array != nullptr && env->GetArrayLength(row_weights_array) < rows) {
    return JNI_FALSE;
  }
  if (column_weights_array != nullptr && env->GetArrayLength(column_weights_array) < columns) {
    return JNI_FALSE;
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
    return JNI_FALSE;
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
  return ok ? JNI_TRUE : JNI_FALSE;
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeComputeBaseSignalLong(
  JNIEnv* env,
  jclass,
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
  if (input_array == nullptr || output_array == nullptr) {
    return JNI_FALSE;
  }
  const auto input_length = env->GetArrayLength(input_array);
  const auto output_length = env->GetArrayLength(output_array);
  if (!valid_extent(rows, columns, input_length) || !valid_extent(rows, columns, output_length)) {
    return JNI_FALSE;
  }
  if (row_weights_array != nullptr && env->GetArrayLength(row_weights_array) < rows) {
    return JNI_FALSE;
  }
  if (column_weights_array != nullptr && env->GetArrayLength(column_weights_array) < columns) {
    return JNI_FALSE;
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
    return JNI_FALSE;
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
  return ok ? JNI_TRUE : JNI_FALSE;
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeMapLinearGradientRgba(
  JNIEnv* env,
  jclass,
  jdoubleArray signal_array,
  jint rows,
  jint columns,
  jfloatArray start_rgba_array,
  jfloatArray end_rgba_array,
  jdouble min_signal,
  jdouble max_signal,
  jbyteArray output_rgba_array
) {
  if (signal_array == nullptr || start_rgba_array == nullptr || end_rgba_array == nullptr || output_rgba_array == nullptr) {
    return JNI_FALSE;
  }
  const auto signal_length = env->GetArrayLength(signal_array);
  const auto output_length = env->GetArrayLength(output_rgba_array);
  if (!valid_extent(rows, columns, signal_length)) {
    return JNI_FALSE;
  }
  const auto element_count = static_cast<std::int64_t>(rows) * columns;
  if (output_length < element_count * 4 || env->GetArrayLength(start_rgba_array) < 4 || env->GetArrayLength(end_rgba_array) < 4) {
    return JNI_FALSE;
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
    return JNI_FALSE;
  }

  const auto ok = map_linear_gradient_rgba(signal, rows, columns, start_rgba, end_rgba, min_signal, max_signal, output_rgba);

  env->ReleaseDoubleArrayElements(signal_array, signal, JNI_ABORT);
  env->ReleaseFloatArrayElements(start_rgba_array, start_rgba, JNI_ABORT);
  env->ReleaseFloatArrayElements(end_rgba_array, end_rgba, JNI_ABORT);
  env->ReleaseByteArrayElements(output_rgba_array, output_rgba, ok ? 0 : JNI_ABORT);
  return ok ? JNI_TRUE : JNI_FALSE;
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeCountStripeBlocks(
  JNIEnv* env,
  jclass,
  jlongArray column_bins_array,
  jint stripe_count,
  jint submatrix_size,
  jint dense_threshold,
  jlongArray output_sparse_dense_counts_array
) {
  if (column_bins_array == nullptr || output_sparse_dense_counts_array == nullptr) {
    return JNI_FALSE;
  }
  if (env->GetArrayLength(output_sparse_dense_counts_array) < 2) {
    return JNI_FALSE;
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
    return JNI_FALSE;
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
  return ok ? JNI_TRUE : JNI_FALSE;
}

extern "C" JNIEXPORT jboolean JNICALL
Java_ru_itmo_ctlab_hict_hict_1library_nativeprocessing_NativeTileProcessor_nativeTransformExpectedSignal(
  JNIEnv* env,
  jclass,
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
  if (signal_array == nullptr || diagonal_means_array == nullptr || output_array == nullptr) {
    return JNI_FALSE;
  }
  const auto signal_length = env->GetArrayLength(signal_array);
  const auto output_length = env->GetArrayLength(output_array);
  if (!valid_extent(rows, columns, signal_length) || !valid_extent(rows, columns, output_length)) {
    return JNI_FALSE;
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
    return JNI_FALSE;
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
  return ok ? JNI_TRUE : JNI_FALSE;
}
