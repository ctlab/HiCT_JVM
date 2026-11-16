# Applying visualization changes

Color thresholds, endpoint colors, signal transforms and scaling preview after
350 ms without another edit. **Apply** sends the current valid choices immediately.
The sidebar shows **Preview pending…**, **Applying visualization changes…**, or
**Changes pending. Apply to retry.** Invalid ranges stay editable and are not sent.

Changes belong to the source and colormap selected when you make them. Switching
between Primary and Secondary does not redirect a pending edit. Expected and O/E
use their own primary palettes; Secondary keeps its independent graph settings.
An older read or acknowledgement cannot replace a newer typed value.

Normalization may fit a default upper threshold. A newer manual color or threshold
choice takes priority. If you change only the transform while normalization is
running, the final fit uses the resulting transformed signal. Opening Rendering
Pipeline and saving a session wait for scheduled visualization changes, so their
snapshot includes the resulting committed options and graph.

Presets replace their documented scope. A source preset affects that source;
a whole-graph preset applies its complete saved graph and primary options.
Later edits take priority over a pending preset. Failed graph reads or rejected
writes preserve the last committed map and retain your draft for retry. A failed
write has not been saved merely because its typed values remain visible.

Weight preparation still selects shared file/coverage vectors for the backend;
enabling or disabling them in a source branch is source-specific. Custom graph
inputs remain intact when changing only colors. See also
[Viewport histogram and levels](viewport-histogram.md) and
[Expected and O/E palettes](observed-expected-palettes.md).
