package ru.itmo.ctlab.hict.hict_library.domain;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ATUDescriptorOrderTest {
  private static List<Integer> bins(List<ATUDescriptor> atus) {
    var result = new ArrayList<Integer>();
    for (var atu : atus) {
      for (int i = atu.getStartIndexInStripeIncl(); i < atu.getEndIndexInStripeExcl(); i++) {
        int bin = atu.getDirection() == ATUDirection.FORWARD ? i : atu.getEndIndexInStripeExcl() - 1 - (i - atu.getStartIndexInStripeIncl());
        result.add(100 * atu.getStripeDescriptor().stripeId() + bin);
      }
    }
    return result;
  }

  @Test void reductionPreservesOrderedBinsForEveryPair() {
    // Includes both adjacency orders, gaps, overlaps, different stripes and directions.
    for (int stripe = 0; stripe < 2; stripe++)
      for (int a = 0; a < 3; a++) for (int b = 0; b < 3; b++)
        for (var da : ATUDirection.values()) for (var db : ATUDirection.values()) {
          var first = new ATUDescriptor(new StripeDescriptor(0, 10, null), a * 2, a * 2 + 2, da);
          var second = new ATUDescriptor(new StripeDescriptor(stripe, 10, null), b * 2, b * 2 + 2, db);
          var input = List.of(first, second);
          assertEquals(bins(input), bins(ATUDescriptor.reduce(input)));
          boolean contiguous = stripe == 0 && da == db &&
            (da == ATUDirection.FORWARD ? a + 1 == b : b + 1 == a);
          assertEquals(contiguous ? 1 : 2, ATUDescriptor.reduce(input).size());
        }
  }

  @Test void reportedReversedTranslocationPreservesOrder() {
    var stripe = new StripeDescriptor(0, 40, null);
    var input = List.of(new ATUDescriptor(stripe, 30, 40, ATUDirection.REVERSED),
      new ATUDescriptor(stripe, 0, 10, ATUDirection.REVERSED),
      new ATUDescriptor(stripe, 10, 20, ATUDirection.REVERSED),
      new ATUDescriptor(stripe, 20, 30, ATUDirection.FORWARD));
    assertEquals(bins(input), bins(ATUDescriptor.reduce(input)));
  }

  public static void main(String[] args) {
    var test = new ATUDescriptorOrderTest();
    test.reductionPreservesOrderedBinsForEveryPair();
    test.reportedReversedTranslocationPreservesOrder();
  }
}
