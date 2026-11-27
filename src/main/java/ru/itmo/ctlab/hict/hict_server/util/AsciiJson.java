package ru.itmo.ctlab.hict.hict_server.util;

import io.vertx.core.json.JsonObject;
import java.util.HexFormat;

/** Encode a JSON document that can be read even with a legacy platform encoding. */
public final class AsciiJson {
  private AsciiJson() { }

  public static String encode(JsonObject object, boolean pretty) {
    String json = pretty ? object.encodePrettily() : object.encode();
    var result = new StringBuilder(json.length());
    var hex = HexFormat.of();
    for (int i = 0; i < json.length(); i++) {
      char unit = json.charAt(i);
      if (unit <= 0x7F) result.append(unit);
      else result.append("\\u").append(hex.toHexDigits(unit, 4));
    }
    return result.toString();
  }
}
