package ru.itmo.ctlab.hict.hict_library.domain;

public record StripeDescriptor(long stripe_id, long stripe_length_bins, long[] bin_weights) {
}
