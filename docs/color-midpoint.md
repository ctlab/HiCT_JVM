# Color midpoint

The small diamond above the color-range slider marks the signal receiving a
halfway blend of the endpoint RGBA colors. Drag it left to give weaker signals
more of the upper color; drag it right to give them less. The two round handles
still select the lower and upper thresholds. **Reset midpoint** restores a linear
color transition without changing thresholds. **Focus range** spreads the selected
range across the slider for finer control.

Focus the diamond with Tab and use arrow keys to adjust it. Shift makes smaller
steps; Page Up/Down makes larger steps. Home/End move near the lower/upper end of
the selected range. When thresholds are close together, the diamond gets its own
vertical space. The tooltip and accessible value report the signal at half color.

Midpoint changes preview through the usual pending-change workflow and belong to
the selected source/colormap. They leave contact values, signal transforms,
normalization, histogram samples and thresholds unchanged. **Slider scale → Log**
changes adjustment sensitivity; the diamond still refers to the actual signal
and the rail previews the color curve on that axis.

Ordinary Observed and Expected gradients support this control. Expected remembers
its own curve separately from Observed. O/E keeps its neutral white point at 1,
and percentage identity palettes keep their fixed color mapping; neither offers
the ordinary midpoint diamond. See [Expected and O/E palettes](observed-expected-palettes.md).

Session saves/restores, user presets and portable graph presets retain the curve.
Saving a user preset waits for pending preview changes. A source preset may replace
that source's midpoint with its saved/default curve; choosing a Secondary source
preset also stays scoped to Secondary in the single-map layout. Full-graph presets
replace their documented whole-graph scope. Rendering Pipeline exposes the same
parameter as **gamma (linear)** on ordinary Colormap nodes.
Preset background and track styles remain shared scene settings.

For reproducibility, ordinary color interpolation uses `t = clamp((signal - lower)
/ (upper - lower), 0, 1)` and the RGBA blend fraction `t^gamma`. The diamond is at
`lower + (upper - lower) × 0.5^(1/gamma)`. Gamma is finite in 0.05–100; old payloads
without it mean 1. The native path applies this color transfer to its own buffer,
leaving numeric signal arrays intact. Export metadata includes non-default gamma.
Gamma does not affect O/E or identity palette mappings.
