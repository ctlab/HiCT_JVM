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
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.GradleException
import org.gradle.api.file.RelativePath
import org.gradle.api.tasks.testing.logging.TestLogEvent.*
import org.gradle.language.jvm.tasks.ProcessResources
import java.io.ByteArrayOutputStream
import java.net.URI
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.util.zip.ZipFile

plugins {
  java
  application
  id("com.gradleup.shadow") version "8.3.9"
  id("io.freefair.lombok") version "8.10.2"
}

group = "ru.itmo.ctlab.hict"

repositories {
  mavenCentral()
  maven {
    url = uri("https://maven.scijava.org/content/repositories/public/")
  }
  maven {
    url = uri("https://nexus.bioviz.org/repository/maven-releases/")
  }
}

val vertxVersion = "4.4.1"
val junitJupiterVersion = "5.9.1"
val junitPlatformVersion = "1.9.1"
val slf4jVersion = "1.7.36"
val logbackVersion = "1.2.13"

dependencyLocking {
  lockAllConfigurations()
}

val mainVerticleName = "ru.itmo.ctlab.hict.hict_server.MainVerticle"
val launcherClassName = "io.vertx.core.Launcher"

val watchForChange = "src/**/*"
val doOnChange = "${projectDir}/gradlew classes"

val versionFile = file("${project.projectDir}/version.txt")
val changelogFile = file("${project.projectDir}/CHANGELOG.md")
val webUiPackageJson = file("${project.projectDir}/../HiCT_WebUI/package.json")

val webUICloneDirectory = layout.buildDirectory.dir("webui").get()
val localWebUIRepositoryDirectory = layout.projectDirectory.dir("../HiCT_WebUI")
val remoteWebUIRepositoryDirectory = webUICloneDirectory.dir("HiCT_WebUI")
val webUIRepositoryDirectory =
  if (localWebUIRepositoryDirectory.asFile.exists()) localWebUIRepositoryDirectory else remoteWebUIRepositoryDirectory
val webUIRepositoryAddress = "https://github.com/ctlab/HiCT_WebUI.git"
val webUITargetDirectory = layout.projectDirectory.dir("src/main/resources/webui")
val bundledToolchainSourceDirectory = layout.projectDirectory.dir("toolchains-dist")
val webUIFallbackRef = "master"
val webUISameAsJvmRefToken = "same-as-jvm"
val webUIRefOverride = providers.gradleProperty("webuiRef").orNull ?: System.getenv("HICT_WEBUI_REF")
val npmExecutable = if (System.getProperty("os.name").lowercase().contains("windows")) "npm.cmd" else "npm"
val requireBundledWebUI =
  (providers.gradleProperty("requireBundledWebUI").orNull ?: System.getenv("HICT_REQUIRE_BUNDLED_WEBUI"))
    ?.let { it.equals("true", ignoreCase = true) || it == "1" || it.equals("yes", ignoreCase = true) }
    ?: false
val requireWebUIConverterDtoChecks =
  (providers.gradleProperty("requireWebUIConverterDtoChecks").orNull ?: System.getenv("HICT_REQUIRE_WEBUI_CONVERTER_CHECKS"))
    ?.let { it.equals("true", ignoreCase = true) || it == "1" || it.equals("yes", ignoreCase = true) }
    ?: false
val nativeProcessingLibraryBaseName = "hict_native"
val nativeProcessingSourceFile = layout.projectDirectory.file("src/main/native/hict_native.cpp")
val nativeProcessingResourceRoot = layout.buildDirectory.dir("native-processing/resources")

data class NativeProcessingVariant(
  val id: String,
  val taskSuffix: String,
  val libraryBaseName: String,
  val gnuCompileFlags: List<String>,
  val msvcCompileFlags: List<String>,
  val description: String
)

val nativeProcessingVariants = listOf(
  NativeProcessingVariant(
    id = "sse2",
    taskSuffix = "Sse2",
    libraryBaseName = "${nativeProcessingLibraryBaseName}_sse2",
    gnuCompileFlags = listOf("-msse2"),
    msvcCompileFlags = emptyList(),
    description = "x86-64 SSE2 baseline build"
  ),
  NativeProcessingVariant(
    id = "avx2",
    taskSuffix = "Avx2",
    libraryBaseName = nativeProcessingLibraryBaseName,
    gnuCompileFlags = listOf("-mavx2", "-mfma", "-msse4.2", "-mbmi", "-mbmi2"),
    msvcCompileFlags = listOf("/arch:AVX2"),
    description = "x86-64 AVX2/FMA/BMI2 build"
  ),
  NativeProcessingVariant(
    id = "avx512",
    taskSuffix = "Avx512",
    libraryBaseName = "${nativeProcessingLibraryBaseName}_avx512",
    gnuCompileFlags = listOf(
      "-mavx2",
      "-mfma",
      "-msse4.2",
      "-mbmi",
      "-mbmi2",
      "-mavx512f",
      "-mavx512dq",
      "-mavx512bw",
      "-mavx512vl",
      "-DHICT_NATIVE_AVX512=1"
    ),
    msvcCompileFlags = listOf("/arch:AVX512", "/DHICT_NATIVE_AVX512=1"),
    description = "AVX-512F/DQ/BW/VL build"
  )
)

val nativeProcessingOpenMpEnabled =
  (providers.gradleProperty("nativeOpenmp").orNull ?: System.getenv("HICT_NATIVE_OPENMP"))
    ?.let { it.equals("true", ignoreCase = true) || it == "1" || it.equals("yes", ignoreCase = true) }
    ?: false
val requireNativeProcessingVariants =
  (providers.gradleProperty("requireNativeProcessingVariants").orNull ?: System.getenv("HICT_REQUIRE_NATIVE_PROCESSING_VARIANTS"))
    ?.let { it.equals("true", ignoreCase = true) || it == "1" || it.equals("yes", ignoreCase = true) }
    ?: false

fun nativeProcessingPlatformDirectory(): String? {
  val os = System.getProperty("os.name").lowercase()
  val arch = System.getProperty("os.arch").lowercase()
  val is64Bit = arch.contains("64") || arch == "amd64" || arch == "x86_64"
  if (!is64Bit) {
    return null
  }
  return when {
    os.contains("linux") -> "linux_64"
    os.contains("win") -> "windows_64"
    os.contains("mac") || os.contains("darwin") -> "macos_64"
    else -> null
  }
}

fun nativeProcessingJniIncludeDirectory(): String? {
  val os = System.getProperty("os.name").lowercase()
  return when {
    os.contains("linux") -> "linux"
    os.contains("mac") || os.contains("darwin") -> "darwin"
    os.contains("win") -> "win32"
    else -> null
  }
}

fun executableOnPath(executableName: String): Boolean {
  val path = System.getenv("PATH") ?: return false
  return path.split(File.pathSeparator)
    .asSequence()
    .map { File(it, executableName) }
    .any { it.isFile && it.canExecute() }
}

fun nativeProcessingCompilerExecutable(): String? {
  val override = providers.gradleProperty("nativeCxx").orNull
    ?: System.getenv("HICT_NATIVE_CXX")
  if (!override.isNullOrBlank()) {
    val overrideFile = File(override)
    if (overrideFile.isAbsolute) {
      return override.takeIf { overrideFile.isFile && overrideFile.canExecute() }
    }
    return override.takeIf { executableOnPath(it) }
  }
  val os = System.getProperty("os.name").lowercase()
  val candidates = if (os.contains("win")) {
    listOf("cl.exe", "cl", "clang-cl.exe", "clang-cl", "g++", "clang++")
  } else {
    listOf("g++", "clang++")
  }
  return candidates.firstOrNull(::executableOnPath)
}

fun nativeProcessingCompilerFlavor(compilerExecutable: String?): String? {
  if (compilerExecutable.isNullOrBlank()) {
    return null
  }
  val executableName = File(compilerExecutable).name.lowercase().removeSuffix(".exe")
  return when {
    executableName == "cl" || executableName == "clang-cl" -> "msvc"
    executableName == "g++" || executableName == "clang++" || executableName.endsWith("-g++") -> "gnu"
    else -> null
  }
}

fun nativeProcessingCompilerSupportsCurrentOs(compilerExecutable: String? = nativeProcessingCompilerExecutable()): Boolean {
  val os = System.getProperty("os.name").lowercase()
  val compilerFlavor = nativeProcessingCompilerFlavor(compilerExecutable)
  return when {
    os.contains("win") -> compilerFlavor == "msvc" || compilerFlavor == "gnu"
    else -> compilerFlavor == "gnu"
  }
}

fun nativeProcessingOutputFile(variant: NativeProcessingVariant): File {
  val platformDirectory = nativeProcessingPlatformDirectory() ?: "unsupported"
  val mappedLibraryName = System.mapLibraryName(variant.libraryBaseName)
  val variantDirectory = nativeProcessingResourceVariantDirectory(variant.id)
  return nativeProcessingResourceRoot
    .map { it.file("natives/$platformDirectory/$variantDirectory/native/$mappedLibraryName") }
    .get()
    .asFile
}

fun nativeProcessingResourceVariantDirectory(variantId: String): String = when (variantId.lowercase()) {
  "sse2", "generic", "x86_64-v3" -> "generic"
  else -> variantId.lowercase()
}

fun handleMissingWebUI(message: String, cause: Throwable? = null) {
  if (requireBundledWebUI) {
    throw GradleException(message, cause)
  }
  if (cause == null) {
    logger.warn(message)
  } else {
    logger.warn(message, cause)
  }
}

fun emitCiWarning(message: String) {
  if (System.getenv("GITHUB_ACTIONS").equals("true", ignoreCase = true)) {
    println("::warning::${message.replace('\n', ' ')}")
  }
  logger.warn(message)
}


fun envOrProjectProperty(name: String): String? =
  (findProperty(name) as String?)?.takeIf { it.isNotBlank() }
    ?: System.getenv(name)?.takeIf { it.isNotBlank() }

fun selectedBundledToolchainPlatforms(): List<String> {
  val override = envOrProjectProperty("HICT_BUNDLED_TOOLCHAIN_PLATFORMS")
    ?: envOrProjectProperty("hictBundledToolchainPlatforms")
  if (!override.isNullOrBlank()) {
    if (override.equals("all", ignoreCase = true)) {
      val root = bundledToolchainSourceDirectory.asFile
      return if (root.isDirectory) {
        root.listFiles()
          ?.filter { it.isDirectory }
          ?.map { it.name }
          ?.sorted()
          ?: emptyList()
      } else {
        emptyList()
      }
    }
    return override.split(',', ';', ' ', '\n')
      .map { it.trim() }
      .filter { it.isNotBlank() }
      .distinct()
  }

  envOrProjectProperty("HICT_DARWIN_PLATFORM_DIR")?.let { return listOf(it) }

  val osName = System.getProperty("os.name").lowercase()
  val osArch = System.getProperty("os.arch").lowercase()
  return when {
    osName.contains("win") -> listOf("windows_x86_64")
    osName.contains("mac") || osName.contains("darwin") ->
      if (osArch.contains("aarch64") || osArch.contains("arm64")) listOf("darwin_arm64") else listOf("darwin_x86_64")
    osName.contains("linux") -> listOf("linux_x86_64")
    else -> emptyList()
  }
}

val bundledToolchainPlatforms = selectedBundledToolchainPlatforms()

val jhdf5SourceMode = (envOrProjectProperty("HICT_JHDF5_SOURCE_MODE")
  ?: envOrProjectProperty("hictJhdf5SourceMode")
  ?: "release").lowercase()
val useMavenJhdf5 = jhdf5SourceMode in setOf("maven", "maven-central", "published") ||
  envOrProjectProperty("HICT_USE_MAVEN_JHDF5")
    ?.let { it == "1" || it.equals("true", ignoreCase = true) || it.equals("yes", ignoreCase = true) }
    ?: false
val bundledJhdf5JarName = envOrProjectProperty("HICT_JHDF5_JAR_NAME")
  ?: "sis-jhdf5-19.04.1-slim.jar"
val bundledJhdf5FallbackJarName = envOrProjectProperty("HICT_JHDF5_FALLBACK_JAR_NAME")
  ?: "sis-jhdf5-19.04.1.jar"
val bundledJhdf5DefaultLocalJarPath = "src/main/resources/libs/$bundledJhdf5JarName"
val bundledJhdf5LocalJarPath = envOrProjectProperty("HICT_JHDF5_LOCAL_JAR")
  ?: envOrProjectProperty("hictJhdf5LocalJar")
  ?: bundledJhdf5DefaultLocalJarPath
val bundledJhdf5FallbackLocalJarPath = "src/main/resources/libs/$bundledJhdf5FallbackJarName"
val bundledJhdf5DownloadUrl = envOrProjectProperty("HICT_JHDF5_DOWNLOAD_URL")
  ?: envOrProjectProperty("hictJhdf5DownloadUrl")
  ?: "https://github.com/AxisAlexNT/jhdf5-with-plugins-configuration-snapshot/releases/download/release-artifacts/$bundledJhdf5JarName"
val bundledJhdf5FallbackDownloadUrl = envOrProjectProperty("HICT_JHDF5_FALLBACK_DOWNLOAD_URL")
  ?: "https://github.com/AxisAlexNT/jhdf5-with-plugins-configuration-snapshot/releases/download/release-artifacts/$bundledJhdf5FallbackJarName"
val bundledJhdf5NativesArchiveName = envOrProjectProperty("HICT_JHDF5_NATIVES_ARCHIVE_NAME")
  ?: "sis-jhdf5-19.04.1-natives.tar.gz"
val bundledJhdf5NativesArchivePath = envOrProjectProperty("HICT_JHDF5_NATIVES_ARCHIVE")
  ?: envOrProjectProperty("HICT_JHDF5_NATIVES_ARCHIVE_PATH")
  ?: "src/main/resources/libs/$bundledJhdf5NativesArchiveName"
val bundledJhdf5NativesArchiveDownloadUrl = envOrProjectProperty("HICT_JHDF5_NATIVES_ARCHIVE_URL")
  ?: "https://github.com/AxisAlexNT/jhdf5-with-plugins-configuration-snapshot/releases/download/release-artifacts/$bundledJhdf5NativesArchiveName"
val preferBundledJhdf5NativesArchive = bundledJhdf5JarName.contains("slim", ignoreCase = true) ||
  (envOrProjectProperty("HICT_REQUIRE_JHDF5_NATIVES_ARCHIVE")
    ?.let { it == "1" || it.equals("true", ignoreCase = true) || it.equals("yes", ignoreCase = true) }
    ?: false)
val requireBundledJhdf5 = !useMavenJhdf5 && (envOrProjectProperty("HICT_REQUIRE_BUNDLED_JHDF5")
  ?.let { it == "1" || it.equals("true", ignoreCase = true) || it.equals("yes", ignoreCase = true) }
  ?: false)

fun downloadFile(url: String, target: File, label: String): File? {
  if (target.isFile) {
    return target
  }
  return try {
    target.parentFile.mkdirs()
    URI(url).toURL().openStream().use { stream ->
      Files.copy(stream, target.toPath(), REPLACE_EXISTING)
    }
    logger.lifecycle("Downloaded $label from $url to ${target.absolutePath}")
    target.takeIf { it.isFile }
  } catch (err: Exception) {
    target.delete()
    logger.warn("Failed to download $label from $url: ${err.message}")
    null
  }
}

fun downloadedJhdf5JarFile(): File? {
  if (useMavenJhdf5 || jhdf5SourceMode == "local") {
    return null
  }
  val target = file(".gradle/jhdf5/$bundledJhdf5JarName")
  downloadFile(bundledJhdf5DownloadUrl, target, "bundled JHDF5 jar")?.let { return it }
  if (bundledJhdf5FallbackJarName != bundledJhdf5JarName) {
    val fallbackTarget = file(".gradle/jhdf5/$bundledJhdf5FallbackJarName")
    return downloadFile(bundledJhdf5FallbackDownloadUrl, fallbackTarget, "fallback bundled JHDF5 jar")
  }
  return null
}

fun bundledJhdf5LocalJarFile(): File? = if (useMavenJhdf5) {
  null
} else {
  sequenceOf(bundledJhdf5LocalJarPath, bundledJhdf5FallbackLocalJarPath)
    .distinct()
    .map { file(it) }
    .firstOrNull { it.isFile }
    ?: downloadedJhdf5JarFile()
}

fun downloadedJhdf5NativesArchiveFile(): File? {
  if (useMavenJhdf5 || jhdf5SourceMode == "local" || !preferBundledJhdf5NativesArchive) {
    return null
  }
  val target = file(".gradle/jhdf5/$bundledJhdf5NativesArchiveName")
  return downloadFile(bundledJhdf5NativesArchiveDownloadUrl, target, "bundled JHDF5 native archive")
}

fun bundledJhdf5NativesArchiveFile(): File? = if (useMavenJhdf5) {
  null
} else {
  file(bundledJhdf5NativesArchivePath)
    .takeIf { it.isFile }
    ?: downloadedJhdf5NativesArchiveFile()
}

val runtimeJhdf5NativesArchiveFile = layout.buildDirectory.file("jhdf5-runtime/$bundledJhdf5NativesArchiveName")

fun selectedRuntimeJhdf5NativePlatforms(): List<String> {
  val explicit = envOrProjectProperty("HICT_JHDF5_RUNTIME_PLATFORMS")
    ?: envOrProjectProperty("hictJhdf5RuntimePlatforms")
  if (!explicit.isNullOrBlank()) {
    return explicit.split(',', ';', '\n')
      .map { it.trim() }
      .filter { it.isNotBlank() }
      .distinct()
  }
  return when (envOrProjectProperty("HICT_DARWIN_PLATFORM_DIR")) {
    "darwin_arm64" -> listOf("aarch64-Mac OS X")
    "darwin_x86_64" -> listOf("x86_64-Mac OS X")
    else -> emptyList()
  }
}

val runtimeJhdf5NativePlatforms = selectedRuntimeJhdf5NativePlatforms()

fun selectedJhdf5VerificationScope(): String {
  val explicit = envOrProjectProperty("HICT_VERIFY_BUNDLED_JHDF5_SCOPE")
    ?: envOrProjectProperty("hictVerifyBundledJhdf5Scope")
  if (!explicit.isNullOrBlank()) {
    return explicit.lowercase()
  }
  return if (runtimeJhdf5NativePlatforms.isNotEmpty()) "runtime" else "universal"
}

val bundledJhdf5VerificationScope = selectedJhdf5VerificationScope()

fun isRuntimeJhdf5NativeFile(fileName: String): Boolean {
  val lower = fileName.lowercase()
  if (lower == "jhdf5.dll" || lower == "hdf5.dll" || lower == "blosc.dll") {
    return true
  }
  if (lower == "zlib1.dll" || lower == "zlib.dll" || lower == "zstd.dll" || lower == "lz4.dll" || lower == "bz2.dll" || lower == "bzip2.dll") {
    return true
  }
  if (lower == "libjhdf5.so" || lower == "libjhdf5.jnilib") {
    return true
  }
  if (Regex("""libhdf5\.so(?:\..*)?""").matches(lower)) {
    return true
  }
  if (Regex("""libhdf5(?:\.\d+)+\.dylib""").matches(lower) || lower == "libhdf5.dylib") {
    return true
  }
  if (Regex("""lib(?:z|zstd|lz4|bz2|jpeg|aec).*\.so(?:\..*)?""").matches(lower)) {
    return true
  }
  if (Regex("""lib(?:z|zstd|lz4|bz2|jpeg|aec).*\.dylib""").matches(lower)) {
    return true
  }
  if (Regex("""lib(?:zstd|lz4|bz2|jpeg|aec).*\.dll""").matches(lower)) {
    return true
  }
  return Regex("""(?:lib)?h5[a-z0-9_+-]*\.(?:dll|dylib|so(?:\..*)?)""").matches(lower)
}

fun runtimeJhdf5NativesArchiveOrSource(): File? =
  runtimeJhdf5NativesArchiveFile.get().asFile.takeIf { it.isFile }
    ?: bundledJhdf5NativesArchiveFile()

fun requiredJhdf5FilterPluginNames(platformDirectoryName: String): List<String> {
  val extension = when {
    platformDirectoryName.contains("Windows") -> "dll"
    platformDirectoryName.contains("Mac OS X") -> "dylib"
    else -> "so"
  }
  return listOf("bshuf", "lzf", "lz4", "zstd").map { "libh5$it.$extension" }
}

fun allRuntimeJhdf5NativePlatforms(): List<String> = listOf(
    "amd64-Linux",
    "amd64-Linux-avx2",
    "arm64-Linux",
    "amd64-Windows",
    "amd64-Windows-avx2",
    "aarch64-Mac OS X",
    "x86_64-Mac OS X"
  )

fun validateRequiredJhdf5FilterPlugins(nativeRoot: File, requiredPlatforms: List<String> = allRuntimeJhdf5NativePlatforms()) {
  val missing = mutableListOf<String>()
  for (platform in requiredPlatforms) {
    val platformDir = nativeRoot.resolve(platform)
    if (!platformDir.isDirectory) {
      missing += "$platform directory"
      continue
    }
    for (plugin in requiredJhdf5FilterPluginNames(platform)) {
      if (!platformDir.resolve(plugin).exists()) {
        missing += "$platform/$plugin"
      }
    }
  }
  if (missing.isNotEmpty()) {
    throw GradleException(
      "JHDF5 native archive is missing required HDF5 filter plugins needed by HiCT datasets: " +
        missing.joinToString(", ")
    )
  }
}

version = readVersion()

application {
  mainClass.set("ru.itmo.ctlab.hict.hict_server.tools.HictCli")
}

val lombokVersion = "1.18.34"

dependencies {
//  implementation(fileTree("src/main/resources/libs"))
//  runtimeOnly(fileTree("src/main/resources/libs/natives"))

  val localJhdf5Jar = bundledJhdf5LocalJarFile()
  if (localJhdf5Jar != null) {
    implementation(files(localJhdf5Jar))
    logger.lifecycle("Using bundled JHDF5 jar from ${localJhdf5Jar.absolutePath}")
  } else {
    val message = if (useMavenJhdf5) {
      "HICT_JHDF5_SOURCE_MODE=${jhdf5SourceMode}; using published cisd:jhdf5:19.04.1 from Maven repositories by explicit request."
    } else {
      "No bundled JHDF5 jar is available at ${bundledJhdf5LocalJarPath} and ${bundledJhdf5DownloadUrl} could not be downloaded; using published cisd:jhdf5:19.04.1 from Maven repositories."
    }
    if (requireBundledJhdf5) {
      throw GradleException(message)
    }
    logger.lifecycle(message)
    implementation("cisd:jhdf5:19.04.1")
  }


  // https://mvnrepository.com/artifact/cisd/base
  implementation("cisd:base:18.09.0")
  implementation("org.jetbrains:annotations:24.0.0")


  // https://mvnrepository.com/artifact/org.apache.bcel/bcel
  implementation("org.apache.bcel:bcel:6.7.0")

  compileOnly("org.projectlombok:lombok:$lombokVersion")
  annotationProcessor("org.projectlombok:lombok:$lombokVersion")
  testCompileOnly("org.projectlombok:lombok:$lombokVersion")
  testAnnotationProcessor("org.projectlombok:lombok:$lombokVersion")

  implementation("org.slf4j:slf4j-api:$slf4jVersion")
//  implementation("org.slf4j:slf4j-nop:1.7.+")
  implementation("ch.qos.logback:logback-classic:$logbackVersion")


  implementation(platform("io.vertx:vertx-stack-depchain:$vertxVersion"))
  implementation("io.vertx:vertx-core")
  implementation("io.vertx:vertx-web-client")
  implementation("io.vertx:vertx-web-validation")
  implementation("io.vertx:vertx-config")
  implementation("io.vertx:vertx-web")
  implementation("io.vertx:vertx-web-openapi")
  implementation("io.vertx:vertx-web-sstore-cookie")
  implementation("io.vertx:vertx-json-schema")
  implementation("io.vertx:vertx-shell")
  implementation("io.vertx:vertx-web-api-contract")
  implementation("io.vertx:vertx-uri-template")
  implementation("io.vertx:vertx-rx-java3")
  implementation("io.vertx:vertx-reactive-streams")
  testImplementation("io.vertx:vertx-junit5")
  testImplementation("org.junit.jupiter:junit-jupiter:$junitJupiterVersion")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher:$junitPlatformVersion")

  // https://mvnrepository.com/artifact/org.apache.commons/commons-lang3
  implementation("org.apache.commons:commons-lang3:3.12.0")

  // https://mvnrepository.com/artifact/org.apache.commons/commons-pool2
  implementation("org.apache.commons:commons-pool2:2.11.1")

// https://mvnrepository.com/artifact/org.apache.commons/commons-csv
  implementation("org.apache.commons:commons-csv:1.10.0")

  // https://mvnrepository.com/artifact/org.scijava/native-lib-loader
  implementation("org.scijava:native-lib-loader:2.4.0")
  implementation("info.picocli:picocli:4.7.6")
  implementation("com.github.samtools:htsjdk:4.1.3")
  implementation("org.broad.igv:bigwig:2.0.1")


}

java {
  sourceCompatibility = JavaVersion.VERSION_19
  targetCompatibility = JavaVersion.VERSION_19
}

tasks.withType<ShadowJar> {
  archiveClassifier.set("fat")
  manifest {
    attributes(
      mapOf(
        "Main-Verticle" to mainVerticleName,
        "Main-Class" to "ru.itmo.ctlab.hict.hict_server.tools.HictCli"
      )
    )
  }
  mergeServiceFiles()
}

tasks.withType<Test> {
  useJUnitPlatform()
  System.getProperty("hict.native.test.library")
    ?.takeIf { it.isNotBlank() }
    ?.let { systemProperty("hict.native.test.library", it) }
  testLogging {
    events = setOf(PASSED, SKIPPED, FAILED)
  }
}

nativeProcessingVariants.forEach { variant ->
  tasks.register<Exec>("compileNativeProcessing${variant.taskSuffix}") {
    group = "build"
    description = "Build the optional HiCT native processing JNI library (${variant.description}) for the current platform."
    val platformDirectory = nativeProcessingPlatformDirectory()
    val jniPlatformInclude = nativeProcessingJniIncludeDirectory()
    val outputFile = nativeProcessingOutputFile(variant)
    val javaHome = file(System.getProperty("java.home"))
    val javaInclude = javaHome.resolve("include")
    val javaPlatformInclude = jniPlatformInclude?.let { javaInclude.resolve(it) }
    val compilerExecutable = nativeProcessingCompilerExecutable()
    val compilerFlavor = nativeProcessingCompilerFlavor(compilerExecutable)

    inputs.file(nativeProcessingSourceFile)
    inputs.property("platformDirectory", platformDirectory ?: "unsupported")
    inputs.property("jniPlatformInclude", jniPlatformInclude ?: "unsupported")
    inputs.property("compilerFlavor", compilerFlavor ?: "unsupported")
    inputs.property("variantId", variant.id)
    inputs.property("gnuCompileFlags", variant.gnuCompileFlags.joinToString(" "))
    inputs.property("msvcCompileFlags", variant.msvcCompileFlags.joinToString(" "))
    inputs.property("nativeProcessingOpenMpEnabled", nativeProcessingOpenMpEnabled)
    outputs.file(outputFile)
    isIgnoreExitValue = true

    onlyIf {
      platformDirectory != null &&
        jniPlatformInclude != null &&
        compilerExecutable != null &&
        nativeProcessingCompilerSupportsCurrentOs(compilerExecutable) &&
        nativeProcessingSourceFile.asFile.isFile &&
        javaInclude.isDirectory &&
        javaPlatformInclude?.isDirectory == true
    }

    doFirst {
      outputFile.parentFile.mkdirs()
      logger.lifecycle("Building HiCT native processing ${variant.id} library: ${outputFile.absolutePath}")
    }

    val msvcOpenMpFlags = if (nativeProcessingOpenMpEnabled) listOf("/openmp") else listOf("/wd4068")
    val gnuOpenMpFlags = if (nativeProcessingOpenMpEnabled) listOf("-fopenmp") else emptyList()
    val gnuVariantFlags = if (platformDirectory == "macos_64") emptyList() else variant.gnuCompileFlags
    val command = when (compilerFlavor) {
      "msvc" -> listOfNotNull(
        compilerExecutable,
        "/nologo",
        "/std:c++17",
        "/O2",
        "/EHsc",
        "/LD",
        "/DNOMINMAX",
        "/DWIN32_LEAN_AND_MEAN",
        "/DHICT_NATIVE_VARIANT=\\\"${variant.id}\\\"",
        "/I${javaInclude.absolutePath}",
        "/I${javaPlatformInclude?.absolutePath ?: javaInclude.absolutePath}",
        nativeProcessingSourceFile.asFile.absolutePath,
        "/Fe:${outputFile.absolutePath}"
      ) + variant.msvcCompileFlags + msvcOpenMpFlags + listOf("/link", "/NOLOGO")
      else -> listOfNotNull(
        compilerExecutable,
        "-std=c++17",
        "-O3",
        "-fPIC",
        "-fvisibility=hidden",
        "-shared",
        "-DHICT_NATIVE_VARIANT=\"${variant.id}\"",
        "-I${javaInclude.absolutePath}",
        "-I${javaPlatformInclude?.absolutePath ?: javaInclude.absolutePath}",
        "-o",
        outputFile.absolutePath,
        nativeProcessingSourceFile.asFile.absolutePath
      ) + gnuVariantFlags + gnuOpenMpFlags
    }
    commandLine(command)

    doLast {
      val exitValue = executionResult.get().exitValue
      if (exitValue != 0) {
        outputFile.delete()
        emitCiWarning("HiCT native processing ${variant.id} build failed with exit code $exitValue; Java fallback remains available.")
      } else if (!outputFile.isFile) {
        emitCiWarning("HiCT native processing ${variant.id} build completed but did not produce ${outputFile.absolutePath}; Java fallback remains available.")
      }
    }
  }
}

tasks.register("compileNativeProcessing") {
  group = "build"
  description = "Build the optional HiCT native processing SSE2 baseline JNI library for the current platform."
  dependsOn("compileNativeProcessingSse2")
}

tasks.register("compileNativeProcessingBaseline") {
  group = "build"
  description = "Compatibility alias for compileNativeProcessingSse2."
  dependsOn("compileNativeProcessingSse2")
}

tasks.register("natives") {
  group = "build"
  description = "Build every optional HiCT native processing library supported by this machine and toolchain."
  dependsOn(nativeProcessingVariants.map { "compileNativeProcessing${it.taskSuffix}" })
}

tasks.register("verifyNativeProcessingBuild") {
  group = "verification"
  description = "Warn when optional HiCT native processing variants were not built for the current platform."
  dependsOn("natives")
  doLast {
    val platformDirectory = nativeProcessingPlatformDirectory()
    if (platformDirectory == null) {
      val message = "HiCT native processing is unsupported on this platform; Java fallback remains available."
      if (requireNativeProcessingVariants) {
        throw GradleException(message)
      }
      emitCiWarning(message)
      return@doLast
    }
    val missingVariants = mutableListOf<String>()
    nativeProcessingVariants.forEach { variant ->
      val outputFile = nativeProcessingOutputFile(variant)
      if (!outputFile.isFile) {
        val message = "HiCT native processing ${variant.id} library is missing at ${outputFile.absolutePath}; this package will use Java fallback for that variant."
        missingVariants += "${variant.id} (${outputFile.absolutePath})"
        emitCiWarning(message)
      } else {
        logger.lifecycle("HiCT native processing ${variant.id} library ready: ${outputFile.absolutePath}")
      }
    }
    if (requireNativeProcessingVariants && missingVariants.isNotEmpty()) {
      throw GradleException(
        "Required HiCT native processing variant(s) were not built: ${missingVariants.joinToString()}"
      )
    }
  }
}

tasks.register("describeNativeProcessing") {
  group = "help"
  description = "Describe optional HiCT native processing library build/load locations."
  doLast {
    val platformDirectory = nativeProcessingPlatformDirectory()
    if (platformDirectory == null) {
      println("Native processing platform is unsupported on this machine.")
      return@doLast
    }
    val compilerExecutable = nativeProcessingCompilerExecutable()
    println("Native processing source: ${nativeProcessingSourceFile.asFile.absolutePath}")
    println("Native processing compiler: ${compilerExecutable ?: "not found"}")
    println("Native processing compiler flavor: ${nativeProcessingCompilerFlavor(compilerExecutable) ?: "unsupported"}")
    println("Native processing compiler enabled on this OS: ${nativeProcessingCompilerSupportsCurrentOs(compilerExecutable)}")
    println("Native processing OpenMP: ${if (nativeProcessingOpenMpEnabled) "requested" else "disabled by default"}")
    nativeProcessingVariants.forEach { variant ->
      println("Native processing ${variant.id} output: ${nativeProcessingOutputFile(variant).absolutePath}")
    }
    println("Runtime overrides:")
    println("  HICT_NATIVE_PROCESSING=1")
    println("  HICT_NATIVE_VARIANT=auto|generic|avx2|avx512")
    println("  HICT_NATIVE_VARIANT=baseline, sse2 and x86_64-v3 are accepted as aliases for generic")
    println("  HICT_NATIVE_LIBRARY_PATH=${nativeProcessingOutputFile(nativeProcessingVariants.first()).absolutePath}")
    println("  HICT_NATIVE_LIBRARY_DIR=${nativeProcessingOutputFile(nativeProcessingVariants.first()).parentFile.absolutePath}")
  }
}



tasks.register<JavaExec>("runConversionCli") {
  group = "application"
  description = "Run conversion CLI (hict-to-mcool / mcool-to-hict subcommands)"
  classpath = sourceSets["main"].runtimeClasspath
  mainClass.set("ru.itmo.ctlab.hict.hict_server.tools.HictCli")
}

nativeProcessingVariants.forEach { variant ->
  tasks.register<JavaExec>("benchmarkNativeProcessing${variant.taskSuffix}") {
    group = "verification"
    description = "Benchmark Java tile processing against the ${variant.description} native backend."
    dependsOn("natives", "compileJava")
    onlyIf { nativeProcessingOutputFile(variant).isFile }
    classpath = sourceSets["main"].output.classesDirs + sourceSets["main"].compileClasspath
    mainClass.set("ru.itmo.ctlab.hict.hict_library.nativeprocessing.NativeProcessingBenchmark")
    systemProperty("hict.native.library.dir", nativeProcessingOutputFile(variant).parentFile.absolutePath)
    systemProperty("hict.native.variant", variant.id)
    listOf(
      "hict.native.benchmark.rows",
      "hict.native.benchmark.columns",
      "hict.native.benchmark.warmup",
      "hict.native.benchmark.iterations"
    ).forEach { propertyName ->
      System.getProperty(propertyName)?.let { systemProperty(propertyName, it) }
    }
  }
}

tasks.register("benchmarkNativeProcessingBaseline") {
  group = "verification"
  description = "Compatibility alias for benchmarkNativeProcessingAvx2."
  dependsOn("benchmarkNativeProcessingAvx2")
}

tasks.register("benchmarkNativeProcessing") {
  group = "verification"
  description = "Benchmark Java tile processing against every native backend built on this machine."
  dependsOn(nativeProcessingVariants.map { "benchmarkNativeProcessing${it.taskSuffix}" })
}

tasks.register<JavaExec>("benchmarkNativeProcessingReport") {
  group = "verification"
  description = "Create a Java/SSE2/AVX2/AVX-512 native processing requests/sec benchmark report with SVG plots."
  dependsOn("natives", "compileJava")
  classpath = sourceSets["main"].output.classesDirs + sourceSets["main"].compileClasspath
  mainClass.set("ru.itmo.ctlab.hict.hict_library.nativeprocessing.NativeProcessingBenchmarkReport")
  systemProperty("hict.native.library.dir", nativeProcessingOutputFile(nativeProcessingVariants.first()).parentFile.absolutePath)
  systemProperty("hict.native.benchmark.reportDir", layout.buildDirectory.dir("reports/hict-native-benchmark").get().asFile.absolutePath)
  listOf(
    "hict.native.benchmark.rows",
    "hict.native.benchmark.columns",
    "hict.native.benchmark.warmup",
    "hict.native.benchmark.iterations",
    "hict.native.benchmark.variants"
  ).forEach { propertyName ->
    System.getProperty(propertyName)?.let { systemProperty(propertyName, it) }
  }
}

tasks.register("benchmark") {
  group = "verification"
  description = "Run HiCT performance benchmarks available on this machine."
  dependsOn("benchmarkNativeProcessingReport")
}


tasks.withType<JavaExec>().configureEach {
  doFirst {
    environment(
      "LD_LIBRARY_PATH",
      "\$LD_LIBRARY_PATH:/home/${System.getenv("USER")}/hdf/HDF5-1.14.1-Linux/HDF_Group/HDF5/1.14.1/lib:/home/${
        System.getenv(
          "USER"
        )
      }/hdf/HDF5-1.14.1-Linux/HDF_Group/HDF5/1.14.1/lib/plugin"
    )
    environment(
      "HDF5_PLUGIN_PATH",
      "/home/${System.getenv("USER")}/hdf/HDF5-1.14.1-Linux/HDF_Group/HDF5/1.14.1/lib/plugin"
    )
    environment("VERTXWEB_ENVIRONMENT", "dev")
  }
}





fun readVersion(): String {
  return if (versionFile.exists()) {
    versionFile.readText().trim()
  } else {
    "0.0.0"
  }
}

fun writeVersion(version: String) {
  versionFile.writeText(version)
}

fun incrementPatchVersion(currentVersion: String): String {
  val gitHash = getGitHash(layout.projectDirectory.asFile)
  val webuiVer = if (webUIRepositoryDirectory.asFile.exists()) {
    val webuiGitHash = getGitHash(webUIRepositoryDirectory.asFile)
    "webui_$webuiGitHash"
  } else "nowebui"
  val semver = currentVersion.substringBefore("-")
  val (major, minor, patch) = semver.split(".")
  val newPatch = patch.toInt() + 1
  return "$major.$minor.$newPatch-$gitHash-$webuiVer"
}

fun getGitHash(repositoryDir: File): String {
  val byteOut = ByteArrayOutputStream()
  project.exec {
    commandLine("git", "rev-parse", "--short=7", "HEAD")
    standardOutput = byteOut
    workingDir = repositoryDir
  }
  return String(byteOut.toByteArray()).trim()
}

fun getCurrentJvmRefName(): String? {
  val githubRefName = System.getenv("GITHUB_REF_NAME")
  if (!githubRefName.isNullOrBlank()) {
    return githubRefName.trim()
  }

  val byteOut = ByteArrayOutputStream()
  val result = project.exec {
    commandLine("git", "rev-parse", "--abbrev-ref", "HEAD")
    standardOutput = byteOut
    workingDir = layout.projectDirectory.asFile
    isIgnoreExitValue = true
  }
  if (result.exitValue != 0) {
    return null
  }
  val branch = String(byteOut.toByteArray()).trim()
  return branch.takeIf { it.isNotBlank() && it != "HEAD" }
}

fun resolveWebUIRef(): String {
  val override = webUIRefOverride?.trim()
  if (!override.isNullOrBlank() && !override.equals(webUISameAsJvmRefToken, ignoreCase = true)) {
    return override
  }
  return getCurrentJvmRefName() ?: webUIFallbackRef
}

fun verifyWebUIConverterDtoRegression(webUIDir: File) {
  val requestSource = webUIDir.resolve("src/app/core/net/api/request.ts")
  val requestDtoSource = webUIDir.resolve("src/app/core/net/dto/requestDTO.ts")
  val browserAssetsDir = webUIDir.resolve("dist/assets")

  val sourceChecks = listOf(
    requestSource to listOf(
      "class ListConvertibleMatrixFilesRequest",
      "requestPath = \"/list_convertible_matrices\""
    ),
    requestDtoSource to listOf(
      "class ListConvertibleMatrixFilesRequestDTO",
      "instanceof ListConvertibleMatrixFilesRequest",
      "case \"/list_convertible_matrices\"",
      "class EmptyRequestDTO",
      "\"options\" in entity"
    )
  )

  val failures = mutableListOf<String>()
  for ((file, requiredSnippets) in sourceChecks) {
    if (!file.isFile) {
      failures += "missing ${file.relativeToOrSelf(webUIDir)}"
      continue
    }
    val text = file.readText()
    for (snippet in requiredSnippets) {
      if (!text.contains(snippet)) {
        failures += "${file.relativeToOrSelf(webUIDir)} does not contain '$snippet'"
      }
    }
  }

  val browserBundles = if (browserAssetsDir.isDirectory) {
    browserAssetsDir.walkTopDown().filter { it.isFile && it.extension == "js" }.toList()
  } else {
    emptyList()
  }
  if (browserBundles.none { it.readText().contains("/list_convertible_matrices") }) {
    failures += "browser bundle does not contain /list_convertible_matrices request support"
  }

  if (failures.isNotEmpty()) {
    val message =
      "HiCT_WebUI converter DTO regression detected; refusing to embed a WebUI bundle that cannot open the converter dialog:\n" +
        failures.joinToString(separator = "\n") { "- $it" }
    if (requireWebUIConverterDtoChecks) {
      throw GradleException(message)
    }
    logger.warn(message)
    logger.warn("Set -PrequireWebUIConverterDtoChecks=true to fail CI on this compatibility check.")
  }
}

val currentVersion: String by lazy { readVersion() }

version = currentVersion

tasks.register("incrementPatchVersion") {
  doLast {
    val newVersion = incrementPatchVersion(currentVersion)
    writeVersion(newVersion)
    project.version = newVersion
    println("[Patch] Version incremented to $newVersion")
  }
}

tasks.register("cleanWebUI") {
  doLast {
    delete(webUIRepositoryDirectory.dir("dist"))
    delete(webUITargetDirectory)
  }
}

tasks.register("buildWebUI") {
  dependsOn("cleanWebUI")
  doLast {
    try {
      val requestedWebUIRef = resolveWebUIRef()
      if (localWebUIRepositoryDirectory.asFile.exists()) {
        println(
          "Using local HiCT_WebUI checkout at ${localWebUIRepositoryDirectory.asFile.absolutePath} " +
            "(branch/working tree will not be modified by Gradle; requested ref '${requestedWebUIRef}' is ignored)"
        )
        project.exec {
          commandLine(npmExecutable, "install", "--no-audit", "--no-fund")
          workingDir = localWebUIRepositoryDirectory.asFile
          standardOutput = System.out
        }
        project.exec {
          commandLine(npmExecutable, "run", "build")
          workingDir = localWebUIRepositoryDirectory.asFile
          standardOutput = System.out
        }
        verifyWebUIConverterDtoRegression(localWebUIRepositoryDirectory.asFile)
        return@doLast
      }

      println("Preparing HiCT_WebUI ref '${requestedWebUIRef}' (fallback '${webUIFallbackRef}')")
      Files.createDirectories(webUICloneDirectory.asFile.toPath())
      val cloneResult = project.exec {
        commandLine("git", "clone", webUIRepositoryAddress)
        workingDir = webUICloneDirectory.asFile
        standardOutput = System.out
        isIgnoreExitValue = true
      }


      if (cloneResult.exitValue != 0) {
        if (!webUIRepositoryDirectory.asFile.resolve(".git").exists()) {
          handleMissingWebUI("Failed to clone WebUI repository. Proceeding without baked-in WebUI.")
          return@doLast
        }
        println("HiCT_WebUI clone already exists; fetching requested ref.")
      }

      project.exec {
        commandLine("git", "fetch", "origin", "--tags", "--prune")
        workingDir = webUIRepositoryDirectory.asFile
        standardOutput = System.out
        isIgnoreExitValue = true
      }

      var checkedOutRef = requestedWebUIRef
      var checkOutResult = project.exec {
        commandLine("git", "checkout", requestedWebUIRef)
        workingDir = webUIRepositoryDirectory.asFile
        standardOutput = System.out
        isIgnoreExitValue = true
      }
      if (checkOutResult.exitValue != 0) {
        println("Failed to checkout HiCT_WebUI ref '${requestedWebUIRef}', trying fallback '${webUIFallbackRef}'.")
        checkedOutRef = webUIFallbackRef
        checkOutResult = project.exec {
          commandLine("git", "checkout", webUIFallbackRef)
          workingDir = webUIRepositoryDirectory.asFile
          standardOutput = System.out
          isIgnoreExitValue = true
        }
        if (checkOutResult.exitValue != 0) {
          handleMissingWebUI("Failed to checkout HiCT_WebUI ref '${requestedWebUIRef}' or fallback '${webUIFallbackRef}'. Proceeding without baked-in WebUI.")
          return@doLast
        }
      }

      println("Using HiCT_WebUI ref '${checkedOutRef}'.")
      project.exec {
        commandLine("git", "pull", "--ff-only")
        workingDir = webUIRepositoryDirectory.asFile
        standardOutput = System.out
        isIgnoreExitValue = true
      }


      project.exec {
        commandLine(npmExecutable, "install", "--no-audit", "--no-fund")
        workingDir = webUIRepositoryDirectory.asFile
        standardOutput = System.out
      }

      project.exec {
        commandLine(npmExecutable, "run", "build")
        workingDir = webUIRepositoryDirectory.asFile
        standardOutput = System.out
      }
      verifyWebUIConverterDtoRegression(webUIRepositoryDirectory.asFile)
    } catch (e: Exception) {
      if (e is GradleException && e.message?.startsWith("HiCT_WebUI converter DTO regression detected") == true) {
        throw e
      }
      handleMissingWebUI("Caught an exception during building WebUI.", e)
      return@doLast
    }
  }
}

tasks.register<Copy>("copyWebUI") {
  dependsOn("buildWebUI")
  doLast {
    Files.createDirectories(webUITargetDirectory.asFile.toPath())
  }
  from(webUIRepositoryDirectory.dir("dist"))
  into(webUITargetDirectory)
}

tasks.named("clean") {
  dependsOn("cleanWebUI")
}

tasks.named<ProcessResources>("processResources") {
  dependsOn("copyWebUI")
  dependsOn("verifyNativeProcessingBuild")
  dependsOn("prepareRuntimeJhdf5NativesArchive")
  duplicatesStrategy = org.gradle.api.file.DuplicatesStrategy.EXCLUDE
  exclude(
    "libs/$bundledJhdf5JarName",
    "libs/$bundledJhdf5FallbackJarName",
    "libs/$bundledJhdf5NativesArchiveName",
  )
  from(nativeProcessingResourceRoot) {
    into("")
  }
  from(provider {
    runtimeJhdf5NativesArchiveFile.get().asFile
      .takeIf { it.isFile }
      ?.let { listOf(it) }
      ?: emptyList<File>()
  }) {
    into("libs")
  }

  // The custom JHDF5 snapshot jar stores macOS natives under the original
  // SIS/JHDF5 layout, e.g. `native/jhdf5/x86_64-Mac OS X/*.dylib` and
  // `libs/native/jhdf5/aarch64-Mac OS X/*.dylib`.  HiCT's runtime loader,
  // however, looks for normalized `resources/libs/osx_64` and
  // `resources/libs/osx_arm64` directories.  Expand and alias the relevant
  // dylibs while processing resources so the final fat jar is self-contained.
  bundledJhdf5LocalJarFile()?.let { jhdf5Jar ->
    val macJhdf5AliasNames = mutableSetOf<String>()
    fun addMacJhdf5Tree(sourcePrefix: String, targetDir: String) {
      from(zipTree(jhdf5Jar)) {
        include("$sourcePrefix/*.dylib")
        eachFile { relativePath = RelativePath(true, name) }
        includeEmptyDirs = false
        into("resources/libs/$targetDir")
      }
    }

    fun addMacJhdf5Aliases(sourcePrefix: String, targetDir: String) {
      addMacJhdf5Tree(sourcePrefix, targetDir)
      from(zipTree(jhdf5Jar)) {
        include("$sourcePrefix/*.dylib", "$sourcePrefix/*.jnilib")
        includeEmptyDirs = false
        eachFile {
          val alias = when {
            name.matches(Regex("^libhdf5\\.\\d+(?:\\.\\d+)*\\.dylib$")) -> "libhdf5.dylib"
            name.matches(Regex("^libhdf5_hl\\.\\d+(?:\\.\\d+)*\\.dylib$")) -> "libhdf5_hl.dylib"
            name.matches(Regex("^libhdf5_java\\.\\d+(?:\\.\\d+)*\\.dylib$")) -> "libhdf5_java.dylib"
            name.matches(Regex("^libhdf5_tools\\.\\d+(?:\\.\\d+)*\\.dylib$")) -> "libhdf5_tools.dylib"
            name == "libjhdf5.jnilib" -> "libjhdf5.jnilib"
            else -> null
          }
          if (alias == null) {
            exclude()
          } else {
            val aliasKey = "$targetDir/$alias"
            if (macJhdf5AliasNames.add(aliasKey)) {
              relativePath = RelativePath(true, alias)
            } else {
              exclude()
            }
          }
        }
        into("resources/libs/$targetDir")
      }
    }

    addMacJhdf5Aliases("native/jhdf5/x86_64-Mac OS X", "osx_64")
    addMacJhdf5Aliases("libs/native/jhdf5/x86_64-Mac OS X", "osx_64")
    addMacJhdf5Aliases("native/jhdf5/x86_64-Mac OS X", "macos_64")
    addMacJhdf5Aliases("libs/native/jhdf5/x86_64-Mac OS X", "macos_64")
    addMacJhdf5Aliases("native/jhdf5/aarch64-Mac OS X", "osx_arm64")
    addMacJhdf5Aliases("libs/native/jhdf5/aarch64-Mac OS X", "osx_arm64")
    addMacJhdf5Aliases("native/jhdf5/aarch64-Mac OS X", "macos_arm64")
    addMacJhdf5Aliases("libs/native/jhdf5/aarch64-Mac OS X", "macos_arm64")
  }
  // New platform naming in loader code uses "macos_64"; keep aliasing from legacy "osx_64".
  from("src/main/resources/natives/osx_64") {
    into("resources/libs/osx_64")
    include("**/*.dylib", "**/*.jnilib")
  }
  from("src/main/resources/natives/osx_64") {
    into("resources/libs/osx_64")
    include("**/*.so")
    rename { it.replace(".so", ".dylib") }
  }
  from("src/main/resources/natives/osx_64") {
    into("resources/libs/macos_64")
    include("**/*.dylib", "**/*.jnilib")
  }
  from("src/main/resources/natives/osx_64") {
    into("resources/libs/macos_64")
    include("**/*.so")
    rename { it.replace(".so", ".dylib") }
  }
  from("src/main/resources/natives/macos_64") {
    into("resources/libs/macos_64")
    include("**/*.dylib", "**/*.jnilib")
  }
  from("src/main/resources/natives/macos_64") {
    into("resources/libs/macos_64")
    include("**/*.so")
    rename { it.replace(".so", ".dylib") }
  }
  from("src/main/resources/natives/osx_arm64") {
    into("resources/libs/osx_arm64")
    include("**/*.dylib", "**/*.jnilib")
  }
  from("src/main/resources/natives/osx_arm64") {
    into("resources/libs/osx_arm64")
    include("**/*.so")
    rename { it.replace(".so", ".dylib") }
  }
  bundledToolchainPlatforms.forEach { platform ->
    from(bundledToolchainSourceDirectory.dir(platform)) {
      into("toolchains/$platform")
    }
  }
  doLast {
    Files.copy(
      versionFile.toPath(),
      layout.buildDirectory.file("resources/main/version.txt").get().asFile.toPath(),
      StandardCopyOption.REPLACE_EXISTING
    )
    if (changelogFile.exists()) {
      Files.copy(
        changelogFile.toPath(),
        layout.buildDirectory.file("resources/main/CHANGELOG.md").get().asFile.toPath(),
        StandardCopyOption.REPLACE_EXISTING
      )
    }
    if (webUiPackageJson.exists()) {
      Files.copy(
        webUiPackageJson.toPath(),
        layout.buildDirectory.file("resources/main/webui-package.json").get().asFile.toPath(),
        StandardCopyOption.REPLACE_EXISTING
      )
    }
  }
}

tasks.register("describeBundledToolchains") {
  group = "distribution"
  description = "List locally prepared external toolchain payloads that will be embedded into the fat JAR."
  doLast {
    val root = bundledToolchainSourceDirectory.asFile
    if (!root.exists()) {
      println("No bundled toolchains found in ${root.absolutePath}")
      return@doLast
    }

    root.walkTopDown()
      .filter { it.isFile && it.name == "manifest.json" }
      .sortedBy { it.absolutePath }
      .forEach { println("Bundled toolchain manifest: ${it.absolutePath}") }
  }
}

tasks.named("build") {
  dependsOn("copyWebUI")
  dependsOn("incrementPatchVersion")
}

tasks.named("jar") {
  dependsOn("copyWebUI")
  dependsOn("incrementPatchVersion")
  dependsOn("shadowJar")
}

val prepareRuntimeJhdf5NativesArchive = tasks.register("prepareRuntimeJhdf5NativesArchive") {
  group = "build"
  description = "Create the smaller JHDF5 native sidecar archive used by HiCT portable releases."
  inputs.property("runtimeJhdf5NativePlatforms", runtimeJhdf5NativePlatforms.joinToString(","))
  outputs.file(runtimeJhdf5NativesArchiveFile)
  doLast {
    val sourceArchive = bundledJhdf5NativesArchiveFile()
    if (sourceArchive == null || !sourceArchive.isFile) {
      if (preferBundledJhdf5NativesArchive || requireBundledJhdf5) {
        throw GradleException("JHDF5 native archive is required but was not found at $bundledJhdf5NativesArchivePath")
      }
      logger.lifecycle("Skipping runtime JHDF5 native archive preparation because no native archive is configured.")
      return@doLast
    }

    val extractRoot = layout.buildDirectory.dir("jhdf5-runtime/extracted").get().asFile
    val targetArchive = runtimeJhdf5NativesArchiveFile.get().asFile
    delete(extractRoot)
    extractRoot.mkdirs()
    targetArchive.parentFile.mkdirs()

    exec {
      commandLine("tar", "-xzf", sourceArchive.absolutePath, "-C", extractRoot.absolutePath)
    }

    val nativeRoot = extractRoot.resolve("native/jhdf5")
    if (!nativeRoot.isDirectory) {
      throw GradleException("JHDF5 native archive ${sourceArchive.absolutePath} does not contain native/jhdf5")
    }

    if (runtimeJhdf5NativePlatforms.isNotEmpty()) {
      val selectedPlatforms = runtimeJhdf5NativePlatforms.toSet()
      nativeRoot.listFiles()
        ?.filter { it.name !in selectedPlatforms }
        ?.forEach {
          if (Files.isSymbolicLink(it.toPath()) || it.isFile) {
            Files.deleteIfExists(it.toPath())
          } else {
            it.deleteRecursively()
          }
        }
      logger.lifecycle("Preparing runtime JHDF5 native archive for platform directories: ${selectedPlatforms.joinToString(", ")}")
    }

    nativeRoot.walkTopDown()
      .filter { it.isFile || Files.isSymbolicLink(it.toPath()) }
      .filter { !isRuntimeJhdf5NativeFile(it.name) }
      .forEach { Files.deleteIfExists(it.toPath()) }

    nativeRoot.walkBottomUp()
      .filter { it.isDirectory && it.list()?.isEmpty() == true }
      .filter { it != nativeRoot }
      .forEach { Files.deleteIfExists(it.toPath()) }

    validateRequiredJhdf5FilterPlugins(
      nativeRoot,
      runtimeJhdf5NativePlatforms.ifEmpty { allRuntimeJhdf5NativePlatforms() }
    )

    targetArchive.delete()
    exec {
      commandLine("tar", "-czf", targetArchive.absolutePath, "-C", extractRoot.absolutePath, "native/jhdf5")
    }
    logger.lifecycle("Prepared runtime JHDF5 native archive at ${targetArchive.absolutePath} from ${sourceArchive.absolutePath}")
  }
}


tasks.register("verifyBundledJhdf5Payload") {
  group = "verification"
  description = "Verify that the selected JHDF5 jar/fat JAR carries the native payloads needed by portable releases."
  dependsOn("shadowJar")
  doLast {
    if (useMavenJhdf5) {
      emitCiWarning("Skipping bundled JHDF5 native-payload verification because HICT_JHDF5_SOURCE_MODE=maven / HICT_USE_MAVEN_JHDF5 is active.")
      return@doLast
    }
    val fatJar = tasks.named<ShadowJar>("shadowJar").get().archiveFile.get().asFile
    if (!fatJar.isFile) {
      throw GradleException("Fat JAR was not produced: ${fatJar.absolutePath}")
    }
    val entries = ZipFile(fatJar).use { zip: ZipFile -> zip.entries().asSequence().map { entry -> entry.name }.toSet() }
    val nativesArchiveEntries = runtimeJhdf5NativesArchiveOrSource()
      ?.takeIf { it.isFile }
      ?.let { archive ->
        ByteArrayOutputStream().use { out ->
          exec {
            commandLine("tar", "-tzf", archive.absolutePath)
            standardOutput = out
          }
          out.toString().lineSequence().filter { it.isNotBlank() }.toSet()
        }
      }
      ?: emptySet()
    fun hasVersionedMacHdf5Dylib(platformDirectory: String): Boolean {
      val regex = Regex("^resources/libs/$platformDirectory/libhdf5\\.\\d+(?:\\.\\d+)*\\.dylib$")
      return entries.any { it == "resources/libs/$platformDirectory/libhdf5.dylib" || regex.matches(it) }
    }
    fun hasJhdf5Entry(path: String): Boolean =
      entries.contains(path) || nativesArchiveEntries.contains(path.removePrefix("libs/"))

    val universalRequirements = listOf(
      "Embedded JHDF5 native archive" to { set: Set<String> ->
        runtimeJhdf5NativesArchiveOrSource()?.isFile != true ||
          set.contains("libs/$bundledJhdf5NativesArchiveName")
      },
      "Linux amd64 JHDF5 JNI" to { set: Set<String> ->
        set.contains("native/jhdf5/amd64-Linux/libjhdf5.so") || hasJhdf5Entry("libs/native/jhdf5/amd64-Linux/libjhdf5.so")
      },
      "Linux arm64 JHDF5 JNI" to { set: Set<String> ->
        set.contains("native/jhdf5/arm64-Linux/libjhdf5.so") || hasJhdf5Entry("libs/native/jhdf5/arm64-Linux/libjhdf5.so")
      },
      "Windows amd64 JHDF5 JNI" to { set: Set<String> ->
        set.contains("native/jhdf5/amd64-Windows/jhdf5.dll") || hasJhdf5Entry("libs/native/jhdf5/amd64-Windows/jhdf5.dll")
      },
      "macOS arm64 JHDF5 JNI" to { set: Set<String> ->
        set.contains("native/jhdf5/aarch64-Mac OS X/libjhdf5.jnilib") || hasJhdf5Entry("libs/native/jhdf5/aarch64-Mac OS X/libjhdf5.jnilib")
      },
      "macOS x86_64 JHDF5 JNI" to { set: Set<String> ->
        set.contains("native/jhdf5/x86_64-Mac OS X/libjhdf5.jnilib") || hasJhdf5Entry("libs/native/jhdf5/x86_64-Mac OS X/libjhdf5.jnilib")
      },
      "macOS arm64 HDF5 dylib" to { entriesSet: Set<String> ->
        hasVersionedMacHdf5Dylib("osx_arm64") ||
          hasVersionedMacHdf5Dylib("macos_arm64") ||
          hasVersionedMacHdf5Dylib("darwin_arm64") ||
          nativesArchiveEntries.any { it.matches(Regex("^native/jhdf5/aarch64-Mac OS X/libhdf5(\\.[0-9]+(\\.[0-9]+)*)?\\.dylib$")) }
      },
      "macOS x86_64 HDF5 dylib" to { entriesSet: Set<String> ->
        hasVersionedMacHdf5Dylib("osx_64") ||
          hasVersionedMacHdf5Dylib("macos_64") ||
          hasVersionedMacHdf5Dylib("darwin_x86_64") ||
          nativesArchiveEntries.any { it.matches(Regex("^native/jhdf5/x86_64-Mac OS X/libhdf5(\\.[0-9]+(\\.[0-9]+)*)?\\.dylib$")) }
      },
      // Current JHDF5 macOS package provides the core HDF5/JHDF5 dylib tree.
      // Plugins are validated by the producing JHDF5 workflow when present, but
      // HiCT packaging must not reject a valid core macOS runtime because optional
      // plugin dylibs are absent for Darwin.
    )
    val runtimeRequirements = when (runtimeJhdf5NativePlatforms.toSet()) {
      setOf("aarch64-Mac OS X") -> listOf(
        "Embedded JHDF5 native archive" to { set: Set<String> ->
          runtimeJhdf5NativesArchiveOrSource()?.isFile != true ||
            set.contains("libs/$bundledJhdf5NativesArchiveName")
        },
        "macOS arm64 JHDF5 JNI" to { set: Set<String> ->
          set.contains("native/jhdf5/aarch64-Mac OS X/libjhdf5.jnilib") ||
            hasJhdf5Entry("libs/native/jhdf5/aarch64-Mac OS X/libjhdf5.jnilib")
        },
        "macOS arm64 HDF5 dylib" to { _: Set<String> ->
          hasVersionedMacHdf5Dylib("osx_arm64") ||
            hasVersionedMacHdf5Dylib("macos_arm64") ||
            hasVersionedMacHdf5Dylib("darwin_arm64") ||
            nativesArchiveEntries.any { it.matches(Regex("^native/jhdf5/aarch64-Mac OS X/libhdf5(\\.[0-9]+(\\.[0-9]+)*)?\\.dylib$")) }
        },
      )
      setOf("x86_64-Mac OS X") -> listOf(
        "Embedded JHDF5 native archive" to { set: Set<String> ->
          runtimeJhdf5NativesArchiveOrSource()?.isFile != true ||
            set.contains("libs/$bundledJhdf5NativesArchiveName")
        },
        "macOS x86_64 JHDF5 JNI" to { set: Set<String> ->
          set.contains("native/jhdf5/x86_64-Mac OS X/libjhdf5.jnilib") ||
            hasJhdf5Entry("libs/native/jhdf5/x86_64-Mac OS X/libjhdf5.jnilib")
        },
        "macOS x86_64 HDF5 dylib" to { _: Set<String> ->
          hasVersionedMacHdf5Dylib("osx_64") ||
            hasVersionedMacHdf5Dylib("macos_64") ||
            hasVersionedMacHdf5Dylib("darwin_x86_64") ||
            nativesArchiveEntries.any { it.matches(Regex("^native/jhdf5/x86_64-Mac OS X/libhdf5(\\.[0-9]+(\\.[0-9]+)*)?\\.dylib$")) }
        },
      )
      else -> universalRequirements
    }
    val requiredAnyOf = when (bundledJhdf5VerificationScope) {
      "runtime", "platform", "portable" -> runtimeRequirements
      "universal", "all" -> universalRequirements
      else -> throw GradleException(
        "Unsupported HICT_VERIFY_BUNDLED_JHDF5_SCOPE=$bundledJhdf5VerificationScope; expected runtime or universal."
      )
    }
    val missing = requiredAnyOf.filter { (_, checker) -> !checker(entries) }
    if (missing.isNotEmpty()) {
      val message = buildString {
        appendLine(
          when (bundledJhdf5VerificationScope) {
            "runtime", "platform", "portable" ->
              "The fat JAR/runtime sidecar pair does not contain the required bundled JHDF5/HDF5 payloads for the selected portable target:"
            else ->
              "The fat JAR does not contain the required bundled JHDF5/HDF5 native payloads:"
          }
        )
        missing.forEach { (label, _) -> appendLine("- $label") }
        appendLine("Set HICT_JHDF5_LOCAL_JAR to the packaged sis-jhdf5 jar from jhdf5-with-plugins-configuration-snapshot, and set HICT_REQUIRE_BUNDLED_JHDF5=1 in release builds. To intentionally use Maven, set HICT_JHDF5_SOURCE_MODE=maven or -PhictJhdf5SourceMode=maven.")
      }
      if (requireBundledJhdf5) {
        throw GradleException(message)
      }
      emitCiWarning(message)
    } else {
      logger.lifecycle("Verified bundled JHDF5/HDF5 native payloads in ${fatJar.absolutePath}")
    }
  }
}

tasks.named<ShadowJar>("shadowJar") {
  dependsOn("verifyNativeProcessingBuild")
  dependsOn(prepareRuntimeJhdf5NativesArchive)
  doFirst {
    archiveFile.get().asFile.parentFile
      ?.listFiles { file -> file.isFile && file.name.matches(Regex("""sis-jhdf5-.*-natives\.tar\.gz""")) }
      ?.forEach { Files.deleteIfExists(it.toPath()) }
  }
}


if (requireBundledJhdf5) {
  tasks.named("build") {
    dependsOn("verifyBundledJhdf5Payload")
  }
}
