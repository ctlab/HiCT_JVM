package ru.itmo.ctlab.hict.hict_library.domain;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

@Getter
@RequiredArgsConstructor
@Builder
@EqualsAndHashCode
public class ATUDescriptor {
  final StripeDescriptor stripeDescriptor;
  final int startIndexInStripeIncl;
  final int endIndexInStripeExcl;
  final ATUDirection direction;

  public static MergeResult merge(final ATUDescriptor d1, final ATUDescriptor d2) {
    if (d1.stripeDescriptor.stripeId() == d2.stripeDescriptor.stripeId() && d1.direction == d2.direction) {
      if (d1.endIndexInStripeExcl == d2.startIndexInStripeIncl) {
        assert (
          d1.startIndexInStripeIncl < d2.endIndexInStripeExcl
        ) : "L start < R end??";
        return new MergeResult(new ATUDescriptor(
          d1.stripeDescriptor,
          d1.startIndexInStripeIncl,
          d2.endIndexInStripeExcl,
          d1.direction
        ), null);
      } else if (d2.endIndexInStripeExcl == d1.startIndexInStripeIncl) {
        return ATUDescriptor.merge(d2, d1);
      }
    }
    return new MergeResult(d1, d2);
  }

  public static List<ATUDescriptor> reduce(final List<ATUDescriptor> atus) {
    if (atus.isEmpty()) {
      return List.of();
    }

    final List<ATUDescriptor> result = new ArrayList<>();

    var lastAtu = atus.get(0);

    for (var i = 1; i < atus.size(); ++i) {
      final var merged = ATUDescriptor.merge(lastAtu, atus.get(i));
      if (merged.d2 != null) {
        result.add(merged.d1);
        lastAtu = merged.d2;
      } else {
        lastAtu = merged.d1;
      }
    }

    result.add(lastAtu);

    return result;
  }

  public @NotNull ATUDescriptor reversed() {
    return new ATUDescriptor(this.stripeDescriptor, this.startIndexInStripeIncl, this.endIndexInStripeExcl, this.direction.inverse());
  }

  public @NotNull ATUDescriptor copy() {
    return new ATUDescriptor(this.stripeDescriptor, this.startIndexInStripeIncl, this.endIndexInStripeExcl, this.direction);
  }

  public int getLength() {
    return this.endIndexInStripeExcl - this.startIndexInStripeIncl;
  }

  public record MergeResult(ATUDescriptor d1, ATUDescriptor d2) {
  }
}
