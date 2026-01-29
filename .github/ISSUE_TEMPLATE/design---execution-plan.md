---
name: Design / Execution Plan
about: Describe thiExecution plan for a pipeline component or architectural changes
  issue template's purpose here.
title: "[design]"
labels: ''
assignees: ''

---

## Goal
What is the objective of this component or change?

Be explicit about *what this does* and *what it does not do*.

---

## Context
Why does this exist?
Where does it fit in the pipeline?

- Layer: ingestion / transform / load / orchestration
- Data source or system involved
- Constraints (volume, format, frequency)

---

## Scope

### In scope
- What this issue is responsible for
- Expected inputs
- Expected outputs
- Entry point(s)

### Out of scope
- Explicitly list what is NOT handled here
- (Transformations, validation, business logic, etc.)

---

## Design
High-level design decisions.

- Data layout (folders, tables, partitions)
- Interfaces (CLI args, functions, modules)
- Idempotency strategy
- Failure behavior

Example (if applicable)

---

## Execution Plan
Concrete steps to implement this.

- [ ] Step 1
- [ ] Step 2
- [ ] Step 3

Each step should be small enough to map to commits or PRs.

---

## Acceptance Criteria (Definition of Done)
This issue is complete when:

- [ ] Component runs end-to-end
- [ ] Re-runs are safe (idempotent)
- [ ] Logs clearly describe behavior
- [ ] Tests pass locally and in CI
- [ ] Documentation updated if needed

---

## Tests
What should be tested?

- Unit tests
- Failure cases
- Edge cases (empty input, partial data, retries)

---

## Notes / Tradeoffs
- Decisions made and why
- Alternatives considered
- Known limitations
