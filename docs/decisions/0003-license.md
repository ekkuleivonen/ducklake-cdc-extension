# ADR 0003 — License: Apache-2.0

- **Status:** Proposed
- **Date:** 2026-04-28
- **Phase:** Phase 0 — Discovery & Spike
- **Deciders:** ekku

## Context

The project ships with `LICENSE` containing the full Apache License 2.0
text and the README's "License" line says `Apache-2.0`. This ADR
ratifies that choice with the rationale, locks the locking discipline,
and removes any lingering "MIT alignment is being reconsidered"
ambiguity (already absent from the README; this ADR keeps it absent).

The three license options that mattered for this decision:

- **Apache-2.0** — current. Permissive license with an **explicit
  patent grant**: contributors grant a royalty-free patent license on
  any patents that read on their contributions. The grant terminates
  if the recipient sues a contributor (or any other recipient) over
  the same patents (the "patent retaliation" clause). The trade-off
  is that downstream users get patent peace of mind; the project
  takes on a slightly heavier per-file boilerplate (NOTICE file,
  per-file headers per the standard recommendations) and the longer
  license header.
- **MIT** — the DuckDB / DuckLake ecosystem default. Lighter file
  surface (single short license; no NOTICE; no per-file boilerplate
  expected); explicit patent grant is **absent**. Wider usage in
  small OSS projects.
- **BSD-3-Clause / 0BSD** — equivalent permissive without patent
  grant; not seriously contested for this project.

The directional pressure runs both ways:

- **For MIT:** the broader DuckDB ecosystem leans MIT. A community
  extension submission may face soft-pressure to align with the
  ecosystem default. New contributors familiar with the ecosystem
  expect MIT.
- **For Apache-2.0:** the explicit patent grant matters once the
  project is widely deployed (private maintainer notes § 11 explicitly defers the
  commercial decision; an Apache-2.0 base preserves the option
  without committing to it). And the conversion path is asymmetric:

| Direction | Cost |
| --- | --- |
| **Apache-2.0 → MIT** | One-way easy. The Apache-2.0 license is **strictly more restrictive** than MIT (it adds the NOTICE requirement, the patent grant, the per-file header recommendation). Going from Apache-2.0 to MIT is a relaxation. Contributors who agreed to Apache-2.0 terms have implicitly agreed to a superset of MIT's terms. The relicensing PR is mostly cosmetic. |
| **MIT → Apache-2.0** | Hard. MIT is more permissive; tightening to Apache-2.0 requires re-licensing every contribution from every contributor (or proving every contribution is by a contributor who has signed a CLA with relicensing rights). For a project with even 5-10 external contributors, this is a months-long process. |

The **default to Apache-2.0** therefore wins on the conservative-default
heuristic: if we're not sure, the option that preserves more downstream
choices later is the right one. The reverse (default to MIT, hope we
never need patent grant or commercial flexibility) commits to a path
that cannot be reversed without significant cost.

## Decision

**License: Apache-2.0.** The `LICENSE` file at repo root contains the
full Apache 2.0 text (already in place). The `README.md` License
section says "Apache-2.0 (see `LICENSE`)" and links the file.

### What lands and what doesn't

- ✅ `LICENSE` contains the full Apache 2.0 text.
- ✅ `README.md` § "License" says "Apache-2.0".
- ✅ Per-file headers are **not required** for v0.1 — Apache 2.0
  recommends them but does not require them. Phase 1 may adopt them
  if the maintainer prefers; the recommendation is to keep the
  surface light and rely on the `LICENSE` at repo root.
- ✅ A `NOTICE` file is **not currently required** because we don't
  yet bundle third-party works that demand attribution. Phase 1 work
  item: when the extension links any third-party C++ library beyond
  what DuckDB already bundles, add the relevant attribution to
  `NOTICE`.
- ❌ No CLA. Contributions are accepted under the project's
  Apache-2.0 license via the standard
  ["inbound = outbound"](https://www.apache.org/licenses/contributor-agreements.html)
  GitHub norm: opening a PR represents that the contribution is
  licensed under the project's license. A CLA would slow first
  contributions for marginal benefit at the project's current
  stage.
- ❌ No DCO (Developer Certificate of Origin) sign-off requirement.
  The same "inbound = outbound" reasoning applies; sign-off is
  optional. Re-evaluate at Phase 5 launch if any contributor base
  shifts toward enterprise users who prefer DCO.

### When this decision is reversible

Apache-2.0 → MIT is reversible at minimal cost. The reversal is
warranted **only** in response to an explicit request from the DuckDB
team to align the project's license with the ecosystem for a specific
reason — for example, a community-extensions listing requirement that
mandates MIT, or a maintainer-side preference expressed during the
extension submission review. **In the absence of that request, ship
Apache-2.0**.

A unilateral relicensing to MIT (without the upstream signal) loses the
patent-grant safety net for no measurable benefit; a relicensing in
response to a clear ecosystem request preserves the project's
relationship with its primary distribution channel and is cheap.

### Removed framing

The early-Phase-0 wording "MIT alignment is being reconsidered" is
**dead** as of this ADR. Pillar-12 reconciliation rules
(`docs/roadmap/`) apply: this ADR and the README must say the
same thing about the license. Drift between this ADR and the README's
License line is a release blocker.

### Per-binding license carry-over

The Phase 3 (Python) and Phase 4 (Go) clients ship under the same
license as the extension — Apache-2.0 — unless the package ecosystem
imposes a binding-specific override. PyPI accepts Apache-2.0 cleanly;
Go modules carry the license file with the source. The per-binding
LICENSE files are a **copy** of the root `LICENSE`, not an
independently chosen license; this is the locking discipline that
prevents drift across the project's deliverables.

### Third-party dependencies

Apache-2.0 is **license-compatible** with MIT, BSD, ISC, and most
permissive licenses we may need to depend on. We are
**not license-compatible** with GPL-family licenses; any proposed
dependency under GPL / LGPL / AGPL is reviewed against this ADR and
rejected unless the maintainer explicitly amends this ADR. (The
likelihood of needing a GPL dependency for a DuckDB extension is low;
DuckDB itself is MIT and avoids GPL transitive deps.)

## Consequences

- **Phase impact.**
  - **Phase 1** does no relicensing work; the existing `LICENSE` and
    `README.md` § "License" line are correct as of this ADR. New
    files added in Phase 1 do **not** need per-file Apache 2.0
    headers (recommendation deferred to a future polish pass).
  - **Phase 2** (catalog matrix) does not change the license. New
    backend integrations inherit Apache-2.0.
  - **Phase 3 (Python)** and **Phase 4 (Go)** ship per-binding
    `LICENSE` files that copy the root `LICENSE` verbatim.
  - **Phase 5** community-extensions submission carries the
    Apache-2.0 license. If the submission review surfaces a soft
    request to relicense to MIT, the maintainer's call (with this
    ADR as the cost-of-change context).
- **Reversibility.**
  - **Apache-2.0 → MIT:** reversible at low cost. One-way-easy:
    contributions accepted under Apache-2.0 are implicitly accepted
    under MIT (Apache 2.0 is a superset of MIT in restriction terms,
    so dropping the patent grant and NOTICE requirement is a
    relaxation that does not require re-consent).
  - **MIT → Apache-2.0:** would be hard. By starting with
    Apache-2.0, the project never has to take the hard path.
  - **Apache-2.0 → GPL or other copyleft:** strictly forbidden
    without explicit unanimous re-consent from every contributor.
    The asymmetry is the same as MIT → Apache, multiplied: copyleft
    is a stricter superset that requires every contribution to be
    re-licensable upward.
- **Open questions deferred.**
  - **CLA adoption.** Reconsider at Phase 5 launch if the
    contributor base shifts toward enterprises that prefer signed
    CLAs. The "inbound = outbound" GitHub norm is sufficient for
    v0.1.
  - **Per-file headers.** Reconsider if the project's deployment
    model ever ships subsets of the source separately (per-file
    headers help downstream users identify the license for a single
    extracted file). Not relevant for the extension's monolithic
    `.duckdb_extension` build.
  - **NOTICE file population.** Becomes required if Phase 1 adds any
    third-party dependency requiring attribution. Tracked as a
    Phase 1 polish item.
  - **Trademark policy.** Not in scope for the LICENSE choice.
    Project name and logo (when one exists) get their own treatment;
    Apache-2.0 § 6 explicitly excludes trademarks from the license
    grant, which is what the project would want. Revisit if a
    trademark conflict ever surfaces; not a Phase 0 / Phase 1 worry.

## Alternatives considered

- **MIT.** Ecosystem-aligned (DuckDB, DuckLake, most DuckDB
  extensions), shorter license file, simpler per-file practice.
  Rejected as the **default** because the relicensing path
  (MIT → Apache-2.0 if ever needed for patent-grant or commercial
  reasons) is hard. Adopted only if the DuckDB team requests
  alignment for a specific reason (community-extensions listing
  requirement, etc.). The conditional "switch to MIT on request"
  path is documented above.
- **BSD-3-Clause.** Equivalent permissive license without patent
  grant. Rejected for the same patent-grant reason as MIT, plus
  BSD-3-Clause is less ecosystem-aligned (neither DuckDB-default
  nor patent-protected; falls between two stools).
- **0BSD / Unlicense.** Maximum permissiveness; no patent grant,
  no attribution requirement. Rejected: no patent peace of mind, and
  the no-attribution path makes accepting third-party contributions
  awkward (contributors usually want their copyright preserved).
- **MPL-2.0 (Mozilla Public License 2.0).** File-level copyleft —
  modifications to existing files must be open-sourced; new files
  can be under any compatible license. Rejected: introduces a
  per-file licensing question every PR must answer (which file did
  you modify? does that file's MPL-2.0 obligation transfer?), which
  is not the friction the project wants in early days. The
  patent-grant is also weaker than Apache-2.0's.
- **Dual MIT / Apache-2.0** (the Rust-ecosystem norm). Rejected:
  more legal complexity than v0.1 needs; downstream users have to
  pick which license they're consuming under, and the project
  doesn't gain anything from offering both.
- **Future commercial / source-available license** (e.g., BSL,
  ELv2). Rejected for v0.1 in line with the private maintainer notes § 11's deferral
  of the commercial question. Apache-2.0 is the broadest
  open-source default; if the project ever takes on a commercial
  arm, that's a separate decision tree (and the Apache-2.0 patent
  grant survives the source-fork that would precede any
  commercial-license fork).

## References

- `docs/roadmap/` — the
  license-choice work item this ADR formalises, including the
  default-to-Apache-2.0 rationale and the "switch on explicit
  request only" rule.
- `docs/roadmap/README.md` — pillar 12 (the
  multi-language commitment that interacts with the patent-grant
  argument; Apache-2.0 is more inviting to enterprise consumers
  than MIT for the same reason it's more cumbersome).
- ADR 0001 — Extension implementation language (the C++ codebase
  this license covers).
- ADR 0005 — Extension vs SQL-helper library (the form that the
  per-binding `LICENSE` carry-over rule applies to).
- `LICENSE` — the canonical Apache 2.0 text shipped at repo root.
- `README.md` § "License" — the user-visible license declaration
  this ADR ratifies.
- the private maintainer notes § 11 (private; not in public tree) — the commercial
  question this ADR keeps an option-preserving stance on.
- Apache Software Foundation
  ["inbound = outbound"](https://www.apache.org/licenses/contributor-agreements.html)
  — the contribution-acceptance norm this ADR adopts.
- Apache License 2.0 — <https://www.apache.org/licenses/LICENSE-2.0>
