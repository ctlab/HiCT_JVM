package ru.itmo.ctlab.hict.hict_server.handlers.fileop;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SecondaryResolutionCompatibilityTest {
  @Test void selectedResolutionUsesItsOwnDimensions() {
    assertTrue(SecondaryResolutionCompatibility.matchingSubset(
      new long[]{1000, 500, 100}, new long[]{4, 8, 40}, new long[]{500}, new long[]{8}));
  }
  @Test void orderingDoesNotChangeCompatibility() {
    assertTrue(SecondaryResolutionCompatibility.matchingSubset(
      new long[]{1000, 500, 100}, new long[]{4, 8, 40}, new long[]{100, 1000}, new long[]{40, 4}));
  }
  @Test void equalArrayPositionCannotHideWrongResolutionOrSize() {
    assertFalse(SecondaryResolutionCompatibility.matchingSubset(
      new long[]{1000, 500}, new long[]{4, 8}, new long[]{500}, new long[]{4}));
    assertFalse(SecondaryResolutionCompatibility.matchingSubset(
      new long[]{1000, 500}, new long[]{4, 8}, new long[]{250}, new long[]{4}));
    assertFalse(SecondaryResolutionCompatibility.matchingSubset(
      new long[]{1000, 500}, new long[]{4, 8}, new long[]{}, new long[]{}));
  }
}
