package ru.itmo.ctlab.hict.hict_library.chunkedfile.hdf5;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.exceptions.HDF5LibraryException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.scijava.nativelib.JniExtractor;
import org.scijava.nativelib.NativeLibraryUtil;
import org.scijava.nativelib.NativeLoader;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


@Slf4j
@Getter
public class HDF5LibraryInitializer {
  private static final AtomicBoolean hdf5LibraryInitialized = new AtomicBoolean(false);
  private static final LinkedHashMap<String, String> libraryNames = new LinkedHashMap<>();
  private static final JniExtractor defaultJNIExtractor = NativeLoader.getJniExtractor();
  private static final PathCollectingJNIExtractor jniExtractor = new PathCollectingJNIExtractor(defaultJNIExtractor);

  static {
    libraryNames.put("z", "Z compression library (Windows-style naming)");
    libraryNames.put("libz", "Z compression library (Linux-style naming)");
    libraryNames.put("sz", "SZip decompression library (Windows-style naming)");
    libraryNames.put("libsz", "SZip decompression library (Linux-style naming)");
    libraryNames.put("hdf5", "HDF5 (Windows-style naming)");
    libraryNames.put("libhdf5", "HDF5 (Windows-style naming)");
    libraryNames.put("jhdf5", "jHDF5 (Windows-style naming)");
    libraryNames.put("libjhdf5", "jHDF5 (Linux-style naming)");
    libraryNames.put("hdf5_tools", "HDF5_tools");
    libraryNames.put("hdf5_java", "HDF5_java");
    libraryNames.put("libh5blosc", "HDF5 BLOSC filter plugin (Linux-style naming)");
    libraryNames.put("h5blosc", "HDF5 BLOSC filter plugin (Windows-style naming)");
    libraryNames.put("libh5bshuf", "HDF5 Shuffle filter plugin (Linux-style naming)");
    libraryNames.put("h5bshuf", "HDF5 Shuffle filter plugin (Windows-style naming)");
    libraryNames.put("libh5bz2", "HDF5 BZ2 filter plugin (Linux-style naming)");
    libraryNames.put("h5bz2", "HDF5 BZ2 filter plugin (Windows-style naming)");
    libraryNames.put("libh5lz4", "HDF5 LZ4 filter plugin (Linux-style naming)");
    libraryNames.put("h5lz4", "HDF5 LZ4 filter plugin (Windows-style naming)");
    libraryNames.put("libh5lzf", "HDF5 LZF filter plugin (Linux-style naming)");
    libraryNames.put("h5lzf", "HDF5 LZF filter plugin (Windows-style naming)");
    libraryNames.put("libh5zfp", "HDF5 ZFP filter plugin (Linux-style naming)");
    libraryNames.put("h5zfp", "HDF5 ZFP filter plugin (Windows-style naming)");
    libraryNames.put("libh5zstd", "HDF5 zSTD filter plugin (Linux-style naming)");
    libraryNames.put("h5zstd", "HDF5 zSTD filter plugin (Windows-style naming)");

    initializeHDF5Library();
  }

  public static synchronized void initializeHDF5Library() {
    if (hdf5LibraryInitialized.get()) {
      log.debug("HDF5 library is already initialized");
      return;
    }

    for (final var e : libraryNames.entrySet()) {
      final var lib = e.getKey();
      final var name = e.getValue();
      log.info("Loading " + name + " library");
      try {
        if (!NativeLibraryUtil.loadNativeLibrary(jniExtractor, lib)) {
          log.warn("Failed to load library " + lib + " with custom JNI Extractor, will try fallback method.");
          NativeLoader.loadLibrary(lib);
          log.warn("Fallback method succeeded but the library path won't be added to the H5 plugins search registry.");
        }
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
}
