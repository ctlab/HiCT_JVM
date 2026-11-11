package ru.itmo.ctlab.hict.hict_server.examples;

import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

/** Select and rename explicitly mapped reference records, preserving their bases. */
final class ExampleFastaPreparation {
  private ExampleFastaPreparation() { }

  static String prepare(Path folder, JsonObject example, AtomicBoolean cancelled) throws IOException {
    var recipe = example.getJsonObject("openingRecipe");
    var preparation = recipe.getJsonObject("fastaPreparation");
    if (preparation == null) return null;
    var records = preparation.getJsonArray("records");
    var expectedLengths = new HashMap<String, Integer>();
    for (var entry : records) {
      var record = (JsonObject) entry;
      expectedLengths.put(record.getString("source"), record.getInteger("length"));
    }
    String sourceName = null;
    for (var entry : example.getJsonArray("files")) {
      var file = (JsonObject) entry;
      if (file.getString("id").equals(recipe.getString("fasta"))) sourceName = file.getString("filename");
    }
    if (sourceName == null) throw new IOException("Reference preparation has no FASTA source");
    var sequences = new HashMap<String, StringBuilder>();
    var seen = new HashSet<String>();
    try (var raw = Files.newInputStream(folder.resolve(sourceName));
         var decoded = sourceName.endsWith(".gz") ? new GZIPInputStream(raw) : raw;
         var reader = new java.io.BufferedReader(new java.io.InputStreamReader(decoded, StandardCharsets.US_ASCII))) {
      StringBuilder selected = null;
      int expected = 0;
      for (String line; (line = reader.readLine()) != null;) {
        checkCancelled(cancelled);
        if (line.startsWith(">")) {
          var id = line.substring(1).trim().split("\\s+", 2)[0];
          selected = null;
          if (expectedLengths.containsKey(id)) {
            if (!seen.add(id)) throw new IOException("Duplicate reference record " + id);
            expected = expectedLengths.get(id);
            selected = new StringBuilder(expected);
            sequences.put(id, selected);
          }
        } else if (selected != null) {
          var bases = line.strip();
          if (bases.chars().anyMatch(Character::isWhitespace) || selected.length() + (long) bases.length() > expected)
            throw new IOException("Invalid reference sequence length or whitespace");
          selected.append(bases);
        }
      }
    }
    var filename = preparation.getString("filename");
    var target = folder.resolve(filename);
    var temporary = Files.createTempFile(folder, "reference-", ".partial");
    try {
      try (var writer = Files.newBufferedWriter(temporary, StandardCharsets.US_ASCII)) {
        for (var entry : records) {
          checkCancelled(cancelled);
          var record = (JsonObject) entry;
          var sequence = sequences.get(record.getString("source"));
          if (sequence == null || sequence.length() != record.getInteger("length"))
            throw new IOException("Missing or mismatched reference record " + record.getString("source"));
          writer.write(">" + record.getString("target") + "\n");
          for (int offset = 0; offset < sequence.length(); offset += 60) {
            checkCancelled(cancelled);
            writer.write(sequence.substring(offset, Math.min(offset + 60, sequence.length())));
            writer.write('\n');
          }
        }
      }
      final MessageDigest digest;
      try { digest = MessageDigest.getInstance("SHA-256"); }
      catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
      try (var input = Files.newInputStream(temporary)) {
        byte[] buffer = new byte[65536];
        for (int count; (count = input.read(buffer)) != -1;) {
          checkCancelled(cancelled);
          digest.update(buffer, 0, count);
        }
      }
      if (!HexFormat.of().formatHex(digest.digest()).equals(preparation.getString("sha256")))
        throw new IOException("Prepared reference checksum differs from the catalog");
      checkCancelled(cancelled);
      Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      return filename;
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static void checkCancelled(AtomicBoolean cancelled) {
    if (cancelled.get() || Thread.currentThread().isInterrupted()) throw new CancellationException("Reference preparation cancelled");
  }
}
