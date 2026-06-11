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

package ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.exceptions.HDF5LibraryException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.scijava.nativelib.JniExtractor;
import org.scijava.nativelib.NativeLibraryUtil;
import org.scijava.nativelib.NativeLoader;
import ru.itmo.ctlab.hict.hict_library.nativeprocessing.NativeCpuFeatures;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.jar.JarFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Locale;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


@Slf4j
@Getter
public class HDF5LibraryInitializer {
  private static final String[] BUNDLED_JHDF5_LIBRARY_ROOTS = {"native/jhdf5", "libs/native/jhdf5"};
  private static final AtomicBoolean hdf5LibraryInitialized = new AtomicBoolean(false);
  private static final LinkedHashMap<String, String> libraryNames = new LinkedHashMap<>();
  private static final LinkedHashMap<String, Path> extractedJhdf5NativeDirectories = new LinkedHashMap<>();
  private static final JniExtractor defaultJNIExtractor = NativeLoader.getJniExtractor();
  private static final PathCollectingJNIExtractor jniExtractor = new PathCollectingJNIExtractor(defaultJNIExtractor);

  static {
    //libraryNames.put("jhdf5", "jHDF5");
    // SiS JHDF5 dependencies:
    // libraryNames.put("libnativedata", "NativeData (Linux-style naming)");
    // libraryNames.put("nativedata", "NativeData (Windows-style naming)");
    // libraryNames.put("libunix", "libunix.so (Linux-style naming), not needed on Windows");
    // libraryNames.put("unix", "libunix.so (Windows-style naming), not needed on Windows");
    // Main HDF5 library, rebuilt from sources:
//    libraryNames.put("libhdf5", "HDF5 (Linux-style naming)");
//    libraryNames.put("hdf5", "HDF5 (Windows-style naming)");
    // HDF5_java library (from source on Linux, from SiS-modified jni/ folder JHDF5 source on Windows):
    // libraryNames.put("libhdf5_java", "HDF5 (Linux-style naming)");
    // libraryNames.put("hdf5_java", "HDF5 (Windows-style naming)");
    // This library might be in dependencies:
    // libraryNames.put("hdf5_tools", "HDF5_tools");
    // The most important HDF5 filter plugins for HiCT (bitshuffle and LZF compression):
    libraryNames.put("libh5bshuf", "HDF5 Shuffle filter plugin (Linux-style naming)");
    libraryNames.put("h5bshuf", "HDF5 Shuffle filter plugin (Windows-style naming)");
    libraryNames.put("h5lzf", "HDF5 LZF filter plugin (Windows-style naming)");
    libraryNames.put("libh5lzf", "HDF5 LZF filter plugin (Linux-style naming)");
    // SiS-modified jni/ folder source linked to libhdf5.a on Linux, SiS-modified version of hdf5_java.dll on Windows
    // libraryNames.put("jhdf5", "jHDF5");
    // Other general compression plugins:
    libraryNames.put("libh5bz2", "HDF5 BZ2 filter plugin (Linux-style naming)");
    libraryNames.put("h5bz2", "HDF5 BZ2 filter plugin (Windows-style naming)");
    libraryNames.put("libh5lz4", "HDF5 LZ4 filter plugin (Linux-style naming)");
    libraryNames.put("h5lz4", "HDF5 LZ4 filter plugin (Windows-style naming)");
    libraryNames.put("libh5zfp", "HDF5 ZFP filter plugin (Linux-style naming)");
    libraryNames.put("h5zfp", "HDF5 ZFP filter plugin (Windows-style naming)");
    libraryNames.put("libh5zstd", "HDF5 zSTD filter plugin (Linux-style naming)");
    libraryNames.put("h5zstd", "HDF5 zSTD filter plugin (Windows-style naming)");
    // Lossy compression plugins currently not used by HiCT:
//    libraryNames.put("libh5blosc", "HDF5 BLOSC filter plugin (Linux-style naming)");
//    libraryNames.put("h5blosc", "HDF5 BLOSC filter plugin (Windows-style naming)");
    /*
    libraryNames.put("hdf5", "HDF5 (Windows-style naming)");
    libraryNames.put("h5bshuf", "HDF5 Shuffle filter plugin (Windows-style naming)");
    libraryNames.put("h5lzf", "HDF5 LZF filter plugin (Windows-style naming)");
    libraryNames.put("jhdf5", "jHDF5");
    */
    initializeHDF5Library();
  }

  public static synchronized void initializeHDF5Library() {
    if (hdf5LibraryInitialized.get()) {
      log.debug("HDF5 library is already initialized");
      return;
    }

    try {
      prepareBundledJhdf5NativeLibraryPath().ifPresent(path ->
        log.info("Using bundled JHDF5 native library {}", path)
      );
    } catch (final IOException err) {
      log.debug("Failed to prepare bundled JHDF5 native library path", err);
    }

    try {
      if (!loadBundledNativeLibrary("hdf5")) {
        log.warn("Failed to load HDF5 with custom JNI Extractor, will try fallback method.");
        NativeLoader.loadLibrary("hdf5");
        log.warn("Fallback method succeeded but the library path won't be added to the H5 plugins search registry.");
      }
      log.info("Loaded HDF5");
    } catch (final IOException err) {
      log.warn("Failed to load HDF5 due to IOException", err);
    } catch (final UnsatisfiedLinkError unsatisfiedLinkError) {
      log.error("Failed to load HDF5 due to unsatisfied link error", unsatisfiedLinkError);
    }
//    NativeLibraryUtil.loadNativeLibrary(jniExtractor, "jhdf5", "resources/", "resources/libs/", "resources/libs/natives/", "/resources/", "/resources/libs/", "/resources/libs/natives/");
//    log.info("Loaded JHDF5");

    for (int i = H5.H5PLsize() - 1; i >= 0; --i) {
      final String path;
      try {
        path = H5.H5PLget(i);
      } catch (final HDF5LibraryException e) {
        log.error("Failed to get plugin path with index " + i);
        continue;
      }

      try {
        H5.H5PLremove(i);
        log.info("Removed pre-existing path with index " + i + " that was " + path);
      } catch (final HDF5LibraryException e) {
        log.error("Failed to remove plugin path with index " + i + " that is " + path);
        continue;
      }
    }

//    for (final var libPath : jniExtractor.getFullPathsCollection()) {
//      try {
//        log.info("Prepending " + libPath + " to the plugin path registry of H5 library");
//        H5.H5PLprepend(libPath);
//        log.info("Appending " + libPath + " to the plugin path registry of H5 library");
//        H5.H5PLappend(libPath);
//      } catch (final HDF5LibraryException e) {
//        log.error("Failed to append " + libPath + " to the plugin registry", e);
//      }
//    }

//    for (final var libPath : jniExtractor.getAbsolutePathsCollection()) {
//      try {
//        log.info("Prepending " + libPath + " to the plugin path registry of H5 library");
//        H5.H5PLprepend(libPath);
//        log.info("Appending " + libPath + " to the plugin path registry of H5 library");
//        H5.H5PLappend(libPath);
//      } catch (final HDF5LibraryException e) {
//        log.error("Failed to append " + libPath + " to the plugin registry", e);
//      }
//    }


    for (final var e : libraryNames.entrySet()) {
      final var lib = e.getKey();
      final var name = e.getValue();
      log.info("Loading " + name + " library");
      try {
        if (!loadBundledNativeLibrary(lib)) {
          log.warn("Failed to load library " + lib + " with custom JNI Extractor, will try fallback method.");
          NativeLoader.loadLibrary(lib);
          log.warn("Fallback method succeeded but the library path won't be added to the H5 plugins search registry.");
        }
        log.info("Successfully loaded library " + lib + " using NativeLoader");
      } catch (final IOException err) {
        log.warn("Failed to load native library " + name + " by NativeLoader due to IOException", err);
//        log.warn("Failed to load native library due to IOException");
//        throw new RuntimeException("Failed to load native library " + name + " by NativeLoader", err);
      } catch (UnsatisfiedLinkError unsatisfiedLinkError) {
        log.error("Failed to load native library " + name + " by NativeLoader due to unsatisfied link error", unsatisfiedLinkError);
//        log.error("Failed to load native library due to UnsatisfiedLinkError");
//        throw new RuntimeException("Failed to load native library " + name + " by NativeLoader due to unsatisfied link error", unsatisfiedLinkError);
      }
    }

//
//    for (int i = H5.H5PLsize() - 1; i >= 0; --i) {
//      final String path;
//      try {
//        path = H5.H5PLget(i);
//      } catch (final HDF5LibraryException e) {
//        log.error("Failed to get plugin path with index " + i);
//        continue;
//      }
//
//      try {
//        H5.H5PLremove(i);
//        log.info("Removed pre-existing path with index " + i + " that was " + path);
//      } catch (final HDF5LibraryException e) {
//        log.error("Failed to remove plugin path with index " + i + " that is " + path);
//        continue;
//      }
//    }
//
//    for (final var libPath : jniExtractor.getFullPathsCollection()) {
//      try {
//        log.info("Prepending " + libPath + " to the plugin path registry of H5 library");
//        H5.H5PLprepend(libPath);
//        log.info("Appending " + libPath + " to the plugin path registry of H5 library");
//        H5.H5PLappend(libPath);
//      } catch (final HDF5LibraryException e) {
//        log.error("Failed to append " + libPath + " to the plugin registry", e);
//      }
//    }

    for (final var libPath : jniExtractor.getAbsolutePathsCollection()) {
      try {
        log.info("Prepending " + libPath + " to the plugin path registry of H5 library");
        H5.H5PLprepend(libPath);
        log.info("Appending " + libPath + " to the plugin path registry of H5 library");
        H5.H5PLappend(libPath);
      } catch (final HDF5LibraryException e) {
        log.error("Failed to append " + libPath + " to the plugin registry", e);
      }
    }

    try {
      H5.loadH5Lib();
      log.info("Loaded HDF5 library");
    } catch (final Throwable uoe) {
      log.error("Caught an Unsupported Operation Exception while initializing HDF5 Library, if it complains about library version, you can simply ignore that", uoe);
    }

    for (final var libPath : jniExtractor.getAbsolutePathsCollection()) {
      try {
        log.info("Prepending " + libPath + " to the plugin path registry of H5 library");
        H5.H5PLprepend(libPath);
        log.info("Appending " + libPath + " to the plugin path registry of H5 library");
        H5.H5PLappend(libPath);
      } catch (final HDF5LibraryException e) {
        log.error("Failed to append " + libPath + " to the plugin registry", e);
      }
    }

    hdf5LibraryInitialized.set(true);
  }


  private static boolean loadBundledNativeLibrary(final String libraryBaseName) throws IOException {
    if (loadBundledJhdf5NativeLibrary(libraryBaseName)) {
      return true;
    }
    if (loadBundledLegacyNativeLibrary(libraryBaseName)) {
      return true;
    }
    return false;
  }

  private static boolean loadBundledJhdf5NativeLibrary(final String libraryBaseName) throws IOException {
    final var candidatePaths = new LinkedHashSet<String>();
    for (final var variantDirectory : bundledVariantDirectories()) {
      for (final var jhdf5PlatformDirectory : bundledJhdf5PlatformDirectories(variantDirectory)) {
        candidatePaths.add(jhdf5PlatformDirectory);
      }
    }
    candidatePaths.addAll(bundledJhdf5PlatformDirectories(""));

    final var candidatePathList = new ArrayList<String>(candidatePaths.size() * 2 * BUNDLED_JHDF5_LIBRARY_ROOTS.length);
    for (final var jhdf5PlatformDirectory : candidatePaths) {
      addBundledJhdf5LibrarySearchPaths(candidatePathList, jhdf5PlatformDirectory);
    }
    if (candidatePathList.isEmpty()) {
      return false;
    }

    for (final String jhdf5LibraryPath : candidatePathList) {
      if (loadBundledJhdf5NativeLibrary(jniExtractor, libraryBaseName, jhdf5LibraryPath)) {
        return true;
      }
    }

    return false;
  }

  private static boolean loadBundledJhdf5NativeLibrary(final @NotNull JniExtractor libraryExtractor,
                                                     final @NotNull String libraryBaseName,
                                                     final @NotNull String jhdf5LibraryPath) {
    final var mappedLibraryNames = mappedJhdf5LibraryNames(libraryBaseName);
    final var normalizedDirectory = normalizeJhdf5ResourceDirectory(jhdf5LibraryPath);
    for (final String mappedName : mappedLibraryNames) {
      for (final String resourcePath : listBundledJhdf5CandidateLibraries(normalizedDirectory, mappedName)) {
        try {
          if (loadBundledJhdf5LibraryFromResource(libraryExtractor, resourcePath, mappedName)) {
            return true;
          }
        } catch (final IOException err) {
          log.debug("Failed to extract and load bundled Jhdf5 library resource {}", resourcePath, err);
        }
      }
    }

    final var absoluteDirectory = normalizedDirectory;
    try {
      final File extractedLibrary = libraryExtractor.extractJni(absoluteDirectory, libraryBaseName);
      if (extractedLibrary == null) {
        return false;
      }

      try {
        System.load(extractedLibrary.getAbsolutePath());
        return true;
      }
      catch (final UnsatisfiedLinkError unsatisfiedLinkError) {
        final String message = unsatisfiedLinkError.getMessage();
        if (message != null && message.contains("already loaded")) {
          log.debug("Bundled JHDF5 library {} is already loaded from {}", libraryBaseName, extractedLibrary.getAbsolutePath());
          return true;
        }
        log.debug("Failed to load bundled JHDF5 library {} from {}", libraryBaseName, extractedLibrary.getAbsolutePath(), unsatisfiedLinkError);
      }
    }
    catch (final IOException err) {
      log.debug("Failed to extract bundled JHDF5 library {} from {}", libraryBaseName, jhdf5LibraryPath, err);
    }

    return false;
  }

  private static List<String> mappedJhdf5LibraryNames(final @NotNull String libraryBaseName) {
    final var mappedName = System.mapLibraryName(libraryBaseName);
    final var result = new LinkedHashSet<String>(3);

    if (libraryBaseName.startsWith("lib")) {
      final var baseWithoutLibPrefix = libraryBaseName.substring("lib".length());
      if (!baseWithoutLibPrefix.isEmpty()) {
        result.add(System.mapLibraryName(baseWithoutLibPrefix));
      }
      if (result.isEmpty()) {
        result.add(mappedName);
      }
    } else {
      result.add(mappedName);
      if (!mappedName.startsWith("lib") && System.mapLibraryName("lib" + libraryBaseName).equals("lib" + mappedName)) {
        result.add("lib" + mappedName);
      }
    }
    if (mappedName.startsWith("lib") && !libraryBaseName.startsWith("lib")) {
      final var withoutLibPrefix = mappedName.substring("lib".length());
      result.add(withoutLibPrefix);
    }
    if (mappedName.endsWith(".dylib")) {
      result.add(mappedName.substring(0, mappedName.length() - ".dylib".length()) + ".jnilib");
    }

    return new ArrayList<>(result);
  }

  private static List<String> listBundledJhdf5CandidateLibraries(final @NotNull String resourceDirectory,
                                                                final @NotNull String mappedName) {
    final var result = new ArrayList<String>();
    final var allEntries = listJarOrResourceEntries(resourceDirectory);
    for (final var entry : allEntries) {
      final var fileName = entry.substring(entry.lastIndexOf('/') + 1);
      if (matchesBundledLibraryFile(fileName, mappedName)) {
        result.add(entry);
      }
    }

    if (result.isEmpty()) {
      final var normalized = resourceDirectory.endsWith("/") ? resourceDirectory : resourceDirectory + "/";
      final var directResource = normalized + mappedName;
      try {
        if (HDF5LibraryInitializer.class.getResource("/" + directResource) != null) {
          result.add(directResource);
        }
      } catch (final Exception err) {
        // Ignore and treat as unavailable resource.
      }
    }
    return result;
  }

  private static boolean matchesBundledLibraryFile(final @NotNull String fileName, final @NotNull String mappedName) {
    if (fileName.equals(mappedName) || fileName.startsWith(mappedName + ".")) {
      return true;
    }

    final var extensionIndex = mappedName.lastIndexOf('.');
    if (extensionIndex <= 0) {
      return false;
    }
    final var baseName = mappedName.substring(0, extensionIndex);
    final var extension = mappedName.substring(extensionIndex);
    return fileName.startsWith(baseName + ".") && fileName.endsWith(extension);
  }

  private static boolean loadBundledJhdf5LibraryFromResource(final @NotNull JniExtractor libraryExtractor,
                                                             final @NotNull String resourcePath,
                                                             final @NotNull String mappedLibraryName) throws IOException {
    final var normalizedResourcePath = resourcePath.startsWith("/") ? resourcePath.substring(1) : resourcePath;
    final var separatorIndex = normalizedResourcePath.lastIndexOf('/');
    if (separatorIndex < 0) {
      return false;
    }

    final var resourceDirectory = normalizedResourcePath.substring(0, separatorIndex + 1);
    final var extractionDirectory = extractBundledJhdf5NativeDirectory(resourceDirectory);
    if (extractionDirectory.isEmpty()) {
      return false;
    }

    final var extractionFileName = normalizedResourcePath.substring(separatorIndex + 1);
    final var extractedLibrary = extractionDirectory.get().resolve(extractionFileName);
    if (!Files.isRegularFile(extractedLibrary)) {
      return false;
    }

    final File extractedLibraryFile = extractionLibraryPath(mappedLibraryName, extractedLibrary);
    try {
      System.load(extractedLibraryFile.getAbsolutePath());
      if (libraryExtractor instanceof PathCollectingJNIExtractor pathCollectingExtractor) {
        pathCollectingExtractor.pathsCollection.add(resourceDirectory);
        pathCollectingExtractor.namesCollection.add(mappedLibraryName);
        pathCollectingExtractor.absolutePathsCollection.add(extractedLibraryFile.getParent());
        pathCollectingExtractor.fullPathsCollection.add(extractedLibraryFile.getAbsolutePath());
      }
      return true;
    } catch (final UnsatisfiedLinkError unsatisfiedLinkError) {
      final String message = unsatisfiedLinkError.getMessage();
      if (message != null && message.contains("already loaded")) {
        return true;
      }
      throw unsatisfiedLinkError;
    }
  }

  private static @NotNull File extractionLibraryPath(final @NotNull String libraryName, final @NotNull Path extractedLibrary) {
    return extractedLibrary.toFile();
  }

  private static @NotNull List<String> listJarOrResourceEntries(final @NotNull String resourceDirectory) {
    final var directory = normalizeJhdf5ResourceDirectory(resourceDirectory);
    try {
      final var codeSource = HDF5LibraryInitializer.class.getProtectionDomain().getCodeSource();
      if (codeSource == null || codeSource.getLocation() == null) {
        return List.of();
      }
      final var location = codeSource.getLocation().toURI();
      final var sourceFile = new File(location);
      if (!sourceFile.isFile() || !sourceFile.getName().endsWith(".jar")) {
        return listFileSystemResourceEntries(resourceDirectory);
      }
      try (var jar = new JarFile(sourceFile)) {
        final var result = new ArrayList<String>();
        final var entries = jar.entries();
        while (entries.hasMoreElements()) {
          final var entry = entries.nextElement().getName();
          if (entry.startsWith(directory)) {
            final var child = entry.substring(directory.length());
            if (child.isEmpty() || child.endsWith("/")) {
              continue;
            }
            result.add(directory + child);
          }
        }
        return result;
      }
    }
    catch (final IOException | URISyntaxException err) {
      return List.of();
    }
  }

  private static @NotNull List<String> listFileSystemResourceEntries(final @NotNull String resourceDirectory) {
    final var directory = normalizeJhdf5ResourceDirectory(resourceDirectory);
    final var root = normalizeClasspathDirectoryForFileSystem(directory);
    if (!root.toFile().exists()) {
      return List.of();
    }
    final var files = root.toFile().listFiles();
    if (files == null) {
      return List.of();
    }
    final var result = new ArrayList<String>(files.length);
    for (final var file : files) {
      if (file.isFile()) {
        result.add(directory + file.getName());
      }
    }
    return result;
  }

  private static @NotNull String normalizeJhdf5ResourceDirectory(final @NotNull String resourceDirectory) {
    var normalized = resourceDirectory.startsWith("/") ? resourceDirectory.substring(1) : resourceDirectory;
    if (!normalized.isEmpty() && !normalized.endsWith("/")) {
      normalized = normalized + "/";
    }
    return normalized;
  }

  private static @NotNull java.nio.file.Path normalizeClasspathDirectoryForFileSystem(final @NotNull String resourceDirectory) {
    final var root = HDF5LibraryInitializer.class.getResource("/" + resourceDirectory);
    if (root == null) {
      return java.nio.file.Path.of("");
    }
    try {
      final var uri = root.toURI();
      return java.nio.file.Path.of(uri);
    }
    catch (final URISyntaxException err) {
      return java.nio.file.Path.of("");
    }
  }

  private static void addBundledJhdf5LibrarySearchPaths(final @NotNull Collection<String> searchPaths,
                                                       final @NotNull String jhdf5PlatformDirectory) {
    for (final var rootDirectory : BUNDLED_JHDF5_LIBRARY_ROOTS) {
      final var candidatePath = rootDirectory + "/" + jhdf5PlatformDirectory + "/";
      searchPaths.add(candidatePath);
      searchPaths.add("/" + candidatePath);
    }
  }

  private static @NotNull Optional<Path> prepareBundledJhdf5NativeLibraryPath() throws IOException {
    for (final var resourceDirectory : bundledJhdf5NativeResourceDirectories()) {
      final var jhdf5ResourcePath = firstBundledJhdf5LibraryResource(resourceDirectory, "jhdf5");
      if (jhdf5ResourcePath.isEmpty()) {
        continue;
      }
      final var extractionDirectory = extractBundledJhdf5NativeDirectory(resourceDirectory);
      if (extractionDirectory.isEmpty()) {
        continue;
      }
      final var fileName = jhdf5ResourcePath.get().substring(jhdf5ResourcePath.get().lastIndexOf('/') + 1);
      final var extractedJhdf5 = extractionDirectory.get().resolve(fileName);
      if (!Files.isRegularFile(extractedJhdf5)) {
        continue;
      }

      setNativeLibraryPropertyIfUnset("jhdf5", extractedJhdf5);
      firstBundledJhdf5LibraryResource(resourceDirectory, "hdf5")
        .map(path -> path.substring(path.lastIndexOf('/') + 1))
        .map(extractionDirectory.get()::resolve)
        .filter(Files::isRegularFile)
        .ifPresent(path -> setNativeLibraryPropertyIfUnset("hdf5", path));
      jniExtractor.pathsCollection.add(resourceDirectory);
      jniExtractor.namesCollection.add("jhdf5");
      jniExtractor.absolutePathsCollection.add(extractionDirectory.get().toString());
      jniExtractor.fullPathsCollection.add(extractedJhdf5.toString());
      return Optional.of(extractedJhdf5);
    }
    return Optional.empty();
  }

  private static void setNativeLibraryPropertyIfUnset(final @NotNull String libraryBaseName, final @NotNull Path libraryPath) {
    final var propertyName = "native.libpath." + libraryBaseName;
    if (System.getProperty(propertyName) == null) {
      System.setProperty(propertyName, libraryPath.toAbsolutePath().normalize().toString());
    }
  }

  private static @NotNull Optional<String> firstBundledJhdf5LibraryResource(final @NotNull String resourceDirectory,
                                                                            final @NotNull String libraryBaseName) {
    final var normalizedDirectory = normalizeJhdf5ResourceDirectory(resourceDirectory);
    for (final var mappedName : mappedJhdf5LibraryNames(libraryBaseName)) {
      final var candidates = listBundledJhdf5CandidateLibraries(normalizedDirectory, mappedName);
      if (!candidates.isEmpty()) {
        return Optional.of(candidates.get(0));
      }
    }
    return Optional.empty();
  }

  private static @NotNull List<String> bundledJhdf5NativeResourceDirectories() {
    final var candidatePaths = new LinkedHashSet<String>();
    for (final var variantDirectory : bundledVariantDirectories()) {
      candidatePaths.addAll(bundledJhdf5PlatformDirectories(variantDirectory));
    }
    candidatePaths.addAll(bundledJhdf5PlatformDirectories(""));

    final var result = new ArrayList<String>(candidatePaths.size() * BUNDLED_JHDF5_LIBRARY_ROOTS.length);
    for (final var jhdf5PlatformDirectory : candidatePaths) {
      for (final var rootDirectory : BUNDLED_JHDF5_LIBRARY_ROOTS) {
        result.add(normalizeJhdf5ResourceDirectory(rootDirectory + "/" + jhdf5PlatformDirectory));
      }
    }
    return result;
  }

  private static @NotNull Optional<Path> extractBundledJhdf5NativeDirectory(final @NotNull String resourceDirectory) throws IOException {
    final var normalizedDirectory = normalizeJhdf5ResourceDirectory(resourceDirectory);
    if (extractedJhdf5NativeDirectories.containsKey(normalizedDirectory)) {
      return Optional.of(extractedJhdf5NativeDirectories.get(normalizedDirectory));
    }

    final var entries = listJarOrResourceEntries(normalizedDirectory);
    if (entries.isEmpty()) {
      return Optional.empty();
    }

    final var extractionDirectory = Files.createTempDirectory("hict-jhdf5-native-");
    extractionDirectory.toFile().deleteOnExit();
    for (final var entry : entries) {
      final var normalizedEntry = entry.startsWith("/") ? entry.substring(1) : entry;
      if (!normalizedEntry.startsWith(normalizedDirectory)) {
        continue;
      }
      final var relativeEntry = normalizedEntry.substring(normalizedDirectory.length());
      if (relativeEntry.isBlank() || relativeEntry.endsWith("/")) {
        continue;
      }
      final var target = extractionDirectory.resolve(relativeEntry).normalize();
      if (!target.startsWith(extractionDirectory)) {
        throw new IOException("Refusing to extract native resource outside temporary directory: " + entry);
      }
      final var parent = target.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
        parent.toFile().deleteOnExit();
      }
      try (InputStream stream = HDF5LibraryInitializer.class.getResourceAsStream("/" + normalizedEntry)) {
        if (stream == null) {
          continue;
        }
        Files.copy(stream, target, StandardCopyOption.REPLACE_EXISTING);
        target.toFile().deleteOnExit();
      }
    }

    extractedJhdf5NativeDirectories.put(normalizedDirectory, extractionDirectory);
    return Optional.of(extractionDirectory);
  }

  private static boolean loadBundledLegacyNativeLibrary(final String libraryBaseName) throws IOException {
    final var platformDirectories = bundledPlatformDirectories();
    if (platformDirectories.isEmpty()) {
      return false;
    }
    for (final var platformDirectory : platformDirectories) {
      for (final var variantDirectory : bundledVariantDirectories()) {
        if (NativeLibraryUtil.loadNativeLibrary(
          jniExtractor,
          libraryBaseName,
          "resources/libs/natives/" + platformDirectory + "/" + variantDirectory + "/native/",
          "/resources/libs/natives/" + platformDirectory + "/" + variantDirectory + "/native/",
          "resources/libs/" + platformDirectory + "/" + variantDirectory + "/native/",
          "/resources/libs/" + platformDirectory + "/" + variantDirectory + "/native/",
          "resources/libs/natives/" + platformDirectory + "/",
          "/resources/libs/natives/" + platformDirectory + "/",
          "resources/libs/" + platformDirectory + "/",
          "/resources/libs/" + platformDirectory + "/"
        )) {
          return true;
        }
      }
      if (NativeLibraryUtil.loadNativeLibrary(
        jniExtractor,
        libraryBaseName,
        "resources/libs/natives/" + platformDirectory + "/",
        "/resources/libs/natives/" + platformDirectory + "/",
        "resources/libs/" + platformDirectory + "/",
        "/resources/libs/" + platformDirectory + "/"
      )) {
        return true;
      }
    }
    if (NativeLibraryUtil.loadNativeLibrary(
      jniExtractor,
      libraryBaseName,
      "resources/",
      "resources/libs/",
      "resources/libs/natives/",
      "/resources/",
      "/resources/libs/",
      "/resources/libs/natives/"
    )) {
      return true;
    }
    return false;
  }

  private static @NotNull List<String> bundledJhdf5PlatformDirectories(final @NotNull String variantDirectory) {
    final var os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    final var arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
    final var is64Bit = arch.contains("64") || arch.equals("amd64") || arch.equals("x86_64");
    final var isArm64 = arch.equals("aarch64") || arch.equals("arm64");
    final var isAmd64 = arch.equals("amd64") || arch.equals("x86_64");
    if (!is64Bit) {
      return List.of();
    }

    final var candidateSet = new LinkedHashSet<String>(8);
    if (os.contains("linux")) {
      final var linuxBase = isAmd64 ? "amd64-Linux" : "arm64-Linux";
      if (!variantDirectory.isBlank()) {
        candidateSet.add(linuxBase + "-" + variantDirectory);
      }
      candidateSet.add(linuxBase);
      return new ArrayList<>(candidateSet);
    }
    if (os.contains("win")) {
      if (!variantDirectory.isBlank()) {
        candidateSet.add("amd64-Windows-" + variantDirectory);
      }
      candidateSet.add("amd64-Windows");
      return new ArrayList<>(candidateSet);
    }
    if (os.contains("mac") || os.contains("darwin")) {
      candidateSet.add(isArm64 ? "aarch64-Mac OS X" : "x86_64-Mac OS X");
      return new ArrayList<>(candidateSet);
    }
    return List.of();
  }

  private static @NotNull List<String> bundledVariantDirectories() {
    final var variants = new ArrayList<String>(4);
    if (NativeCpuFeatures.supportsAvx512Core()) {
      variants.add("avx512");
    }
    if (NativeCpuFeatures.supportsAvx2Core()) {
      variants.add("avx2");
    }
    variants.add("generic");
    variants.add("sse2");
    return variants;
  }

  @RequiredArgsConstructor
  @Getter
  private static class PathCollectingJNIExtractor implements JniExtractor {
    private final JniExtractor defaultExtractor;


    private final Set<String> pathsCollection = new LinkedHashSet<>();
    private final Set<String> absolutePathsCollection = new LinkedHashSet<>();
    private final Set<String> namesCollection = new LinkedHashSet<>();
    private final Set<String> fullPathsCollection = new LinkedHashSet<>();

    @Override
    public File extractJni(String libPath, String libname) throws IOException {
      final var result = this.defaultExtractor.extractJni(libPath, libname);
      if (result != null) {
        pathsCollection.add(libPath);
        namesCollection.add(libname);
        Optional.ofNullable(result.getAbsoluteFile().getParent()).ifPresent(absolutePathsCollection::add);
        fullPathsCollection.add(result.getAbsolutePath());
      }
      return result;
    }

    @Override
    public void extractRegistered() throws IOException {
      this.defaultExtractor.extractRegistered();
    }
  }

  private static @NotNull List<String> bundledPlatformDirectories() {
    final var os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    final var arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
    final var is64Bit = arch.contains("64") || arch.equals("amd64") || arch.equals("x86_64");
    final var isArm64 = arch.equals("aarch64") || arch.equals("arm64");
    final var isAmd64 = arch.equals("amd64") || arch.equals("x86_64");
    if (!is64Bit) {
      return List.of();
    }
    if (os.contains("linux")) {
      return List.of("linux_64");
    }
    if (os.contains("win")) {
      return List.of("windows_64");
    }
    if (os.contains("mac") || os.contains("darwin")) {
      final var isArmBuild = isArm64 && !isAmd64;
      final var macPlatform = isArmBuild ? "darwin_arm64" : "darwin_x86_64";
      final var modernPlatform = isArmBuild ? "osx_arm64" : "osx_64";
      return List.of("macos_64", modernPlatform, macPlatform);
    }
    return List.of();
  }
}
