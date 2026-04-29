# ADR NNNN — Title

- **Status:** Proposed | Accepted | Deprecated | Superseded by ADR-NNNN
- **Date:** YYYY-MM-DD
- **Phase:** Phase 0 — Discovery & Spike (or later)
- **Deciders:** name(s)

## Context

What is the issue we're seeing that motivates this decision?

What constraints, prior art, or upstream behaviour bound the solution space?
Cite the DuckLake spec, prior ADRs, and design principles by anchor wherever
possible. If this ADR depends on another ADR's columns / SQL / shapes, name
that ADR explicitly here; drift between ADRs is the failure mode this template
is meant to catch.

## Decision

The decision, stated in the active voice and as concretely as the level of
the ADR allows. For ADRs that lock SQL, JSON shapes, or column definitions,
the canonical form lives **in this ADR** and downstream phases match it
byte-for-byte; do not paraphrase it elsewhere.

## Consequences

What becomes easier? What becomes harder? What are the upgrade-path
implications for users and for downstream phases?

Call out:

- **Phase impact** — which later phases pick this up, and which sections of
  their phase docs must reference this ADR rather than re-deriving the
  decision.
- **Reversibility** — is this a one-way door (e.g. on-disk schema columns,
  public API names) or a two-way door (e.g. a default value)? One-way doors
  must say so loudly.
- **Open questions** — anything we're explicitly deferring. Link to the
  follow-up phase / ADR.

## Alternatives considered

Brief, honest summaries of the options we rejected and *why*. The reader
should be able to reconstruct why the chosen option won. Bare lists of
discarded options without rationale are not useful here.

## References

- DuckLake spec sections cited
- Other ADRs (`docs/decisions/00NN-...md`)
- Project design principles (`README.md`, `docs/performance.md`)
- External material (issues, RFCs, blog posts)
