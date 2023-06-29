package ru.itmo.ctlab.hict.hict_library.domain;

public record StripeDescriptor(int stripeId, long stripeLengthBins, double[] bin_weights) {
}
