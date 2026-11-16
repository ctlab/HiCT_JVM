package ru.itmo.ctlab.hict.hict_server.examples;

import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_server.handlers.conversion.ConversionDirection;

import java.nio.file.Path;

/** A conversion result returns to its catalog recipe rather than losing its reference context. */
public record ExampleConversionContext(String id, String title) {
  public static ExampleConversionContext resolve(Path root, Path source, ConversionDirection direction, String id) {
    if (id == null || id.isBlank()) return null;
    var example = ExamplesCatalog.load(root).find(id);
    if (example == null) throw new IllegalArgumentException("The example is no longer in the catalog");
    return forExample(root, source, direction, example);
  }

  static ExampleConversionContext forExample(Path root, Path source, ConversionDirection direction,
                                             ExamplesCatalog.Example example) {
    if (direction != ConversionDirection.MCOOL_TO_HICT && direction != ConversionDirection.HIC_TO_HICT)
      throw new IllegalArgumentException("Example opening requires conversion to HiCT");
    var body = example.body();
    var recipe = body.getJsonObject("openingRecipe");
    if (!recipe.getBoolean("quickStart", false))
      throw new IllegalArgumentException("This example opens through the wizard");
    JsonObject matrix = null;
    for (var entry : body.getJsonArray("files")) {
      var file = (JsonObject) entry;
      if (file.getString("id").equals(recipe.getString("matrix"))) matrix = file;
    }
    var expected = matrix == null ? null : root.resolve("examples").resolve(example.id())
      .resolve(matrix.getString("filename")).toAbsolutePath().normalize();
    if (!source.toAbsolutePath().normalize().equals(expected))
      throw new IllegalArgumentException("The conversion source does not match the example recipe");
    if (!ExampleCache.available(root, body))
      throw new IllegalArgumentException("The example files or catalog changed. Verify them through Open Examples first.");
    return new ExampleConversionContext(example.id(), body.getString("title", example.id()));
  }
}
