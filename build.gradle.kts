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
import org.gradle.api.tasks.testing.logging.TestLogEvent.*
import org.gradle.language.jvm.tasks.ProcessResources
import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.StandardCopyOption.REPLACE_EXISTING

plugins {
  java
  application
  id("com.gradleup.shadow") version "8.3.9"
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

version = readVersion()

application {
  mainClass.set("ru.itmo.ctlab.hict.hict_server.tools.HictCli")
}

val lombokVersion = "1.18.42"

dependencies {
//  implementation(fileTree("src/main/resources/libs"))
//  runtimeOnly(fileTree("src/main/resources/libs/natives"))

  implementation("cisd:jhdf5:19.04.1")


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
  implementation("org.broad.igv:bigwig:3.0.0")


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
  testLogging {
    events = setOf(PASSED, SKIPPED, FAILED)
  }
}



tasks.register<JavaExec>("runConversionCli") {
  group = "application"
  description = "Run conversion CLI (hict-to-mcool / mcool-to-hict subcommands)"
  classpath = sourceSets["main"].runtimeClasspath
  mainClass.set("ru.itmo.ctlab.hict.hict_server.tools.HictCli")
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
    throw GradleException(
      "HiCT_WebUI converter DTO regression detected; refusing to embed a WebUI bundle that cannot open the converter dialog:\n" +
        failures.joinToString(separator = "\n") { "- $it" }
    )
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
          commandLine(npmExecutable, "install")
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
        commandLine(npmExecutable, "install")
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
  from(bundledToolchainSourceDirectory) {
    into("toolchains")
  }
  doLast {
    Files.copy(
      versionFile.toPath(),
      layout.buildDirectory.file("resources/main/version.txt").get().asFile.toPath(),
      StandardCopyOption.REPLACE_EXISTING
    )
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
}
