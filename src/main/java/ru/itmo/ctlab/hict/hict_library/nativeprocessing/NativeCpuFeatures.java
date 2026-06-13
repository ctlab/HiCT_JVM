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

package ru.itmo.ctlab.hict.hict_library.nativeprocessing;

import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.PlatformManagedObject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Slf4j
public final class NativeCpuFeatures {
  private static final String NATIVE_VARIANT_PROPERTY = "hict.native.variant";
  private static final String NATIVE_VARIANT_ENV = "HICT_NATIVE_VARIANT";
  private static volatile boolean hotSpotUseAvxQueried = false;
  private static volatile @Nullable Integer hotSpotUseAvxLevel = null;

  private NativeCpuFeatures() {
  }

  public static boolean supportsSse2Core() {
    final var arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
    return arch.equals("amd64") || arch.equals("x86_64") || arch.equals("x64");
  }

  public static boolean supportsAvx2Core() {
    if (isTruthy(firstNonBlank(
      System.getProperty("hict.native.disableAvx2"),
      System.getenv("HICT_NATIVE_DISABLE_AVX2")
    ))) {
      return false;
    }
    if (isTruthy(firstNonBlank(
      System.getProperty("hict.native.forceAvx2"),
      System.getenv("HICT_NATIVE_FORCE_AVX2")
    ))) {
      return true;
    }
    return supportsAvx2FromProcCpuInfo() || supportsAvx2FromHotSpot();
  }

  public static boolean supportsAvx512Core() {
    if (isTruthy(firstNonBlank(
      System.getProperty("hict.native.disableAvx512"),
      System.getenv("HICT_NATIVE_DISABLE_AVX512")
    ))) {
      return false;
    }
    if (isTruthy(firstNonBlank(
      System.getProperty("hict.native.forceAvx512"),
      System.getenv("HICT_NATIVE_FORCE_AVX512")
    ))) {
      return true;
    }
    return supportsAvx2Core() && (supportsAvx512FromHotSpot() || supportsAvx512FromProcCpuInfo());
  }

  public static boolean isTruthy(final @Nullable String value) {
    if (value == null || value.isBlank()) {
      return false;
    }
    final var normalized = value.trim().toLowerCase(Locale.ROOT);
    return normalized.equals("1")
      || normalized.equals("true")
      || normalized.equals("yes")
      || normalized.equals("on");
  }

  public static @Nullable String firstNonBlank(final @Nullable String... values) {
    for (final var value : values) {
      if (value != null && !value.isBlank()) {
        return value.trim();
      }
    }
    return null;
  }

  public static @NotNull String requestedNativeVariantLimit() {
    return normalizeNativeVariantLimit(firstNonBlank(
      System.getProperty(NATIVE_VARIANT_PROPERTY),
      System.getenv(NATIVE_VARIANT_ENV)
    ));
  }

  public static @NotNull String normalizeNativeVariantLimit(final @Nullable String raw) {
    if (raw == null || raw.isBlank()) {
      return "auto";
    }
    final var normalized = raw.trim().toLowerCase(Locale.ROOT).replace('_', '-');
    return switch (normalized) {
      case "", "default", "auto", "best" -> "auto";
      case "generic", "baseline", "sse2", "x86-64-v2", "x86_64-v2", "x64-v2" -> "generic";
      case "avx2", "x86-64-v3", "x86_64-v3", "x64-v3" -> "avx2";
      case "avx512", "avx-512", "x86-64-v4", "x86_64-v4", "x64-v4" -> "avx512";
      default -> "auto";
    };
  }

  public static @NotNull List<String> preferredNativeVariantOrder() {
    return preferredNativeVariantOrder(requestedNativeVariantLimit());
  }

  public static @NotNull List<String> preferredNativeVariantOrder(final @Nullable String rawLimit) {
    final var limit = normalizeNativeVariantLimit(rawLimit);
    final var result = new ArrayList<String>(3);
    final var allowAvx512 = limit.equals("auto") || limit.equals("avx512");
    final var allowAvx2 = allowAvx512 || limit.equals("avx2");

    if (allowAvx512 && supportsAvx512Core()) {
      result.add("avx512");
    }
    if (allowAvx2 && supportsAvx2Core()) {
      result.add("avx2");
    }
    if (supportsSse2Core()) {
      result.add("generic");
    }
    return List.copyOf(result);
  }

  private static boolean supportsAvx512FromHotSpot() {
    final var useAvx = hotSpotUseAvxLevel();
    return useAvx != null && useAvx >= 3;
  }

  private static boolean supportsAvx2FromHotSpot() {
    final var useAvx = hotSpotUseAvxLevel();
    return useAvx != null && useAvx >= 2;
  }

  private static @Nullable Integer hotSpotUseAvxLevel() {
    if (hotSpotUseAvxQueried) {
      return hotSpotUseAvxLevel;
    }
    synchronized (NativeCpuFeatures.class) {
      if (hotSpotUseAvxQueried) {
        return hotSpotUseAvxLevel;
      }
      hotSpotUseAvxLevel = queryHotSpotUseAvxLevel();
      hotSpotUseAvxQueried = true;
      return hotSpotUseAvxLevel;
    }
  }

  private static @Nullable Integer queryHotSpotUseAvxLevel() {
    try {
      final var beanType = Class
        .forName("com.sun.management.HotSpotDiagnosticMXBean")
        .asSubclass(PlatformManagedObject.class);
      final var bean = ManagementFactory.getPlatformMXBean(beanType);
      if (bean == null) {
        return null;
      }
      final var option = beanType.getMethod("getVMOption", String.class).invoke(bean, "UseAVX");
      if (option == null) {
        return null;
      }
      final var value = option.getClass().getMethod("getValue").invoke(option);
      return value == null ? null : Integer.parseInt(value.toString());
    } catch (final ClassNotFoundException | NoClassDefFoundError err) {
      log.debug("HotSpot diagnostic MXBean is unavailable; skipping HotSpot UseAVX feature detection.");
      return null;
    } catch (final Throwable err) {
      log.debug("Could not query HotSpot UseAVX support: {}", err.toString());
      return null;
    }
  }

  private static boolean supportsAvx2FromProcCpuInfo() {
    final var cpuInfo = linuxCpuInfo();
    if (cpuInfo == null) {
      return false;
    }
    return cpuInfo.contains("avx2")
      && cpuInfo.contains("fma")
      && cpuInfo.contains("sse4_2")
      && (cpuInfo.contains("bmi1") || cpuInfo.contains(" bmi "))
      && cpuInfo.contains("bmi2");
  }

  private static boolean supportsAvx512FromProcCpuInfo() {
    final var cpuInfo = linuxCpuInfo();
    if (cpuInfo == null) {
      return false;
    }
    return cpuInfo.contains("avx512f")
      && cpuInfo.contains("avx512dq")
      && cpuInfo.contains("avx512bw")
      && cpuInfo.contains("avx512vl");
  }

  private static @Nullable String linuxCpuInfo() {
    final var os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (!os.contains("linux")) {
      return null;
    }
    try {
      return Files.readString(Path.of("/proc/cpuinfo")).toLowerCase(Locale.ROOT);
    } catch (final IOException err) {
      log.debug("Could not inspect /proc/cpuinfo for CPU feature support", err);
      return null;
    }
  }
}
