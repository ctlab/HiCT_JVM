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
#include <cstdint>

namespace {

constexpr const char* HICT_NATIVE_VERSION = "hict-native-processing/0.1";

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
