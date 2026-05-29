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
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.PlatformManagedObject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

@Slf4j
public final class NativeCpuFeatures {
  private NativeCpuFeatures() {
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
    return supportsAvx512FromHotSpot() || supportsAvx512FromProcCpuInfo();
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

  private static boolean supportsAvx512FromHotSpot() {
    try {
      final var beanType = Class
        .forName("com.sun.management.HotSpotDiagnosticMXBean")
        .asSubclass(PlatformManagedObject.class);
      final var bean = ManagementFactory.getPlatformMXBean(beanType);
      if (bean == null) {
        return false;
      }
      final var option = beanType.getMethod("getVMOption", String.class).invoke(bean, "UseAVX");
      if (option == null) {
        return false;
      }
      final var value = option.getClass().getMethod("getValue").invoke(option);
      return value != null && Integer.parseInt(value.toString()) >= 3;
    } catch (final ClassNotFoundException | NoClassDefFoundError err) {
      log.debug("HotSpot diagnostic MXBean is unavailable; skipping HotSpot UseAVX AVX-512 detection.");
      return false;
    } catch (final Throwable err) {
      log.debug("Could not query HotSpot UseAVX for AVX-512 support: {}", err.toString());
      return false;
    }
  }

  private static boolean supportsAvx512FromProcCpuInfo() {
    final var os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (!os.contains("linux")) {
      return false;
    }
    try {
      final var cpuInfo = Files.readString(Path.of("/proc/cpuinfo")).toLowerCase(Locale.ROOT);
      return cpuInfo.contains("avx512f")
        && cpuInfo.contains("avx512dq")
        && cpuInfo.contains("avx512bw")
        && cpuInfo.contains("avx512vl");
    } catch (final IOException err) {
      log.debug("Could not inspect /proc/cpuinfo for AVX-512 support", err);
      return false;
    }
  }
}
