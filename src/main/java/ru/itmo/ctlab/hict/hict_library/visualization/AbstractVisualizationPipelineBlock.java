package ru.itmo.ctlab.hict.hict_library.visualization;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

public abstract class AbstractVisualizationPipelineBlock {

  protected final @NotNull Map<@NotNull String, Object> globalParameters;

  public AbstractVisualizationPipelineBlock(final @NotNull Map<@NotNull String, Object> globalParameters) {
    this.globalParameters = globalParameters;
  }

  public abstract void forward(final long[][] input, final long[][] output);

  public abstract void forward(final double[][] input, final double[][] output);

  public abstract void forward(final double[][] input, final long[][] output);

  public abstract void forward(final long[][] input, final double[][] output);


}
