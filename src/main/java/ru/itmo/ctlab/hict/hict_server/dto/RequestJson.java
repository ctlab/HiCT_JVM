package ru.itmo.ctlab.hict.hict_server.dto;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import java.math.BigDecimal;

/** Validates JSON fields before passing a request to the engine. */
public final class RequestJson {
  private RequestJson() {}

  public static JsonObject requireObject(RoutingContext ctx) {
    try {
      final var body = ctx.body().asJsonObject();
      if (body != null) return body;
    } catch (DecodeException | ClassCastException malformed) {
      throw new IllegalArgumentException("Expected a JSON request object");
    }
    throw new IllegalArgumentException("Expected a JSON request object");
  }

  public static long requireLong(JsonObject json, String name) {
    if (json.getValue(name) instanceof Number value) {
      try {
        return new BigDecimal(value.toString()).longValueExact();
      } catch (ArithmeticException | NumberFormatException invalidNumber) {
        throw invalidField(name);
      }
    }
    throw invalidField(name);
  }

  public static int requireInt(JsonObject json, String name) {
    final long value = requireLong(json, name);
    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) throw invalidField(name);
    return (int) value;
  }

  public static String requireString(JsonObject json, String name) {
    if (json.getValue(name) instanceof String value && !value.isBlank()) return value;
    throw invalidField(name);
  }

  public static Long optionalLong(JsonObject json, String name) {
    return json.getValue(name) == null ? null : requireLong(json, name);
  }

  public static String optionalString(JsonObject json, String name, String fallback) {
    if (json.getValue(name) == null) return fallback;
    if (json.getValue(name) instanceof String value) return value;
    throw invalidField(name);
  }

  private static IllegalArgumentException invalidField(String name) {
    return new IllegalArgumentException("Missing or invalid field: " + name);
  }
}
