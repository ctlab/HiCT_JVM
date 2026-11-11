package ru.itmo.ctlab.hict.hict_server.handlers.conversion;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.HttpException;
import java.nio.file.Path;

/** Readiness and submission use the same resolver; missing optional tools are expected errors. */
public final class SequenceToolAvailability {
  private SequenceToolAvailability() { }
  public static Path require(String command) {
    try { return new ExternalToolchainManager().requireSequenceCommand(command); }
    catch (java.io.IOException | IllegalStateException e) { throw new HttpException(409, explanation(command), e); }
  }
  public static Path requireAligner() {
    try { return new ExternalToolchainManager().requireSequenceAligner(); }
    catch (IllegalStateException e) { throw new HttpException(409,
      "Alignment dotplots need minimap2 or mm2-plus. Install the complete HiCT portable package, or rebuild with ./gradlew natives shadowJar.", e); }
  }
  public static JsonObject inspect() {
    var tools = new JsonObject();
    for (String command : new String[]{"seqkit", "moddotplot", "hict-moddotplot", "blastn"}) {
      try { require(command); tools.put(command, new JsonObject().put("available", true)); }
      catch (HttpException e) { tools.put(command, new JsonObject().put("available", false).put("reason", e.getPayload())); }
    }
    var alignment = tools.getJsonObject("hict-moddotplot").copy();
    if (alignment.getBoolean("available")) {
      try { requireAligner(); }
      catch (HttpException e) { alignment = new JsonObject().put("available", false).put("reason", e.getPayload()); }
    }
    tools.put("dotplot", alignment);
    return tools;
  }
  private static String explanation(String command) {
    String feature = switch (command) {
      case "seqkit" -> "Exact and substitution search need SeqKit";
      case "blastn" -> "Gapped alignment search needs BLAST+";
      case "moddotplot" -> "The upstream ModDotPlot plotting command is unavailable";
      default -> "Dotplot generation needs HiCT's sequence-analysis helper";
    };
    return feature + ", which is unavailable in this installation. Install the complete current portable package or rebuild with ./gradlew natives shadowJar."
      + (command.equals("blastn") ? " Bundled BLAST is supported on Linux x86_64; other platforms can configure HICT_BLASTN_BIN." : "");
  }
}
