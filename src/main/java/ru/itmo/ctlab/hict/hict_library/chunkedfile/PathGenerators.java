package ru.itmo.ctlab.hict.hict_library.chunkedfile;

import lombok.NonNull;
import org.jetbrains.annotations.NotNull;

public class PathGenerators {
  public static @NotNull @NonNull String getBlockLengthDatasetPath(final long resolution) {
    return String.format("/resolutions/%d/treap_coo/block_length", resolution);
  }

  public static @NotNull @NonNull String getBlockOffsetDatasetPath(final long resolution) {
    return String.format("/resolutions/%d/treap_coo/block_offset", resolution);
  }

  public static @NotNull @NonNull String getBlockColsDatasetPath(final long resolution) {
    return String.format("/resolutions/%d/treap_coo/block_cols", resolution);
  }

  public static @NotNull @NonNull String getBlockRowsDatasetPath(final long resolution) {
    return String.format("/resolutions/%d/treap_coo/block_rows", resolution);
  }

  public static @NotNull @NonNull String getBlockValuesDatasetPath(final long resolution) {
    return String.format("/resolutions/%d/treap_coo/block_vals", resolution);
  }

  public static @NotNull @NonNull String getDenseBlockDatasetPath(final long resolution) {
    return String.format("/resolutions/%d/treap_coo/dense_blocks", resolution);
  }

  public static @NotNull @NonNull String getStripeLengthsBinsDatasetPath(final long resolution) {
    return String.format("/resolutions/%d/stripes/stripe_length_bins", resolution);
  }

  public static @NotNull @NonNull String getStripeBinWeightsDatasetPath(final long resolution) {
    return String.format("/resolutions/%d/stripes/stripes_bin_weights", resolution);
  }

  public static @NotNull @NonNull String getBasisATUDatasetPath(final long resolution) {
    return String.format("/resolutions/%d/atl/basis_atu", resolution);
  }

  public static @NotNull @NonNull String getContigsATLDatasetPath(final long resolution) {
    return String.format("/resolutions/%d/contigs/atl", resolution);
  }

  public static @NotNull @NonNull String getContigHideTypeDatasetPath(final long resolution) {
    return String.format("/resolutions/%d/contigs/contig_hide_type", resolution);
  }

  public static @NotNull @NonNull String getContigLengthBinsDatasetPath(final long resolution) {
    return String.format("/resolutions/%d/contigs/contig_length_bins", resolution);
  }

  public static @NotNull @NonNull String getContigDirectionDatasetPath() {
    return "/contig_info/contig_direction";
  }

  public static @NotNull @NonNull String getContigLengthBpDatasetPath() {
    return "/contig_info/contig_length_bp";
  }

  public static @NotNull @NonNull String getContigNameDatasetPath() {
    return "/contig_info/contig_name";
  }

  public static @NotNull @NonNull String getContigOrderDatasetPath() {
    return "/contig_info/ordered_contig_ids";
  }
}
