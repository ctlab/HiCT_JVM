# Reinforcement fixtures

These are synthetic test inputs, not bundled scientific examples or downloadable
catalog entries. `example.org` URLs are placeholders and must never be fetched by
tests. No citation here represents a real publication.

- `contracts.json` and `assembly.fa`: independent numerical, coordinate and
  alignment oracles used by the initial foundation tests.
- `custom-pipeline.json`: standalone custom graph with log2 above the diagonal
  and a literal multiplier of seven below it. Preserve its expression tree.
- `legacy-presets.json`: the existing `visualizationOptions` export envelope,
  with a pre-log base of 10 and disabled post-log sentinel -1. W2 migration must
  preserve numerical meaning rather than negating the sentinel into base 1.
- `legacy-session-v2.json`: the current v2 settings/session envelope. It does
  **not** preserve edited assembly, tracks or the complete processing graph.
  W1 must migrate it with an explicit limitation notice, never label it a full
  assembly restoration. The paths refer to disposable generated tiny fixtures.
- `catalog-complete.json`: a complete synthetic example for the W15 target
  contract, including a data-defined section, citation, attribution, typed files,
  reference identity and opening recipe with a track. This is a schema-design
  input, not a claim that the production catalog validator already exists.
- `catalog-invalid.json`: full independent catalog documents with one intended
  defect each, plus the expected JSON pointer and reason. A standards-compliant
  schema and semantic validator must consume these in W15. Cross-reference,
  uniqueness and recipe role checks cannot be replaced by structural validation.
- `historical-control-signatures.json`, `control-contracts.json` and
  `menu-surfaces.json`: the interface audit ledger and browser surface contracts.

The fixture catalog's enum and recipe spellings are proposed concrete values for
the target contract. When implementing W15, finalize the schema and update these
inputs together; do not silently weaken negative cases to make validation pass.
No catalog runtime, session migration or preset-application success is established
by JSON parsing alone.
