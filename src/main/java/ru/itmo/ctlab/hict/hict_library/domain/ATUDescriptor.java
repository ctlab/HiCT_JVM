package ru.itmo.ctlab.hict.hict_library.domain;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Getter
@RequiredArgsConstructor
@Builder
public class ATUDescriptor {
    final @NonNull StripeDescriptor stripe_descriptor;
    final long start_index_in_stripe_incl;
    final long end_index_in_stripe_excl;
    final @NonNull ATUDirection direction;

    public static MergeResult merge(final @NonNull ATUDescriptor d1, final @NonNull ATUDescriptor d2) {
        if (d1.stripe_descriptor.stripe_id() == d2.stripe_descriptor.stripe_id() && d1.direction == d2.direction) {
            if (d1.end_index_in_stripe_excl == d2.start_index_in_stripe_incl) {
                assert (
                        d1.start_index_in_stripe_incl < d2.end_index_in_stripe_excl
                ) : "L start < R end??";
                return new MergeResult(new ATUDescriptor(
                        d1.stripe_descriptor,
                        d1.start_index_in_stripe_incl,
                        d2.end_index_in_stripe_excl,
                        d1.direction
                ), null);
            } else if (d2.end_index_in_stripe_excl == d1.start_index_in_stripe_incl) {
                return ATUDescriptor.merge(d2, d1);
            }
        }
        return new MergeResult(d1, d2);
    }

    public static List<@NonNull ATUDescriptor> reduce(final @NonNull List<@NonNull ATUDescriptor> atus) {
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

    public record MergeResult(@NonNull ATUDescriptor d1, ATUDescriptor d2) {
    }
}
