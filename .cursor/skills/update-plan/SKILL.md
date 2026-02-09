---
name: update-plan
description: Maintain the plan/ directory with design decision documents. Use proactively at the end of any agent session that introduces, changes, or supersedes a design decision. Also use when the user explicitly asks to document a design decision.
---

# Update Plan

## When to Trigger

At the end of any session where you have:

- Introduced a new design pattern, protocol change, or architectural decision
- Changed existing behaviour that is already documented in `plan/`
- Superseded or deprecated a previously documented design
- Resolved an open issue listed in an existing plan document

Check `plan/` for existing documents that overlap with your changes before writing anything new.

## Workflow

1. **List `plan/`** to see existing documents.
2. **Read any related documents** — match by topic, not just filename.
3. **Decide**: update an existing document, or create a new one.
   - If the change modifies or extends an existing design, update that document in place.
   - If the change is a new, self-contained topic, create a new file.
4. **Write the document** following the format below.
5. **Update cross-references** — if `DESIGN-REVIEW.md` or other documents reference the topic, update those too.

## File Conventions

- Location: `plan/` at repo root
- Naming: `UPPER-KEBAB-CASE.md` (e.g., `COMMIT-CATCHUP.md`)
- Link between plan documents using relative markdown links: `[text](OTHER-DOC.md)`

## Document Format

```markdown
# Title

**Status**: Implemented | In Progress | Proposed | Superseded by [OTHER.md](OTHER.md)

## Problem

What issue or gap this addresses. Include previous behaviour if replacing something.

## Current Design

How it works now. Include:
- Key types, functions, or modules involved
- Important parameters or constants
- Relevant code snippets (short, illustrative — not full listings)

## Not Yet Implemented

Optional. Open items or known gaps.
```

Keep documents factual and concise. No filler. Omit sections that don't apply.

## Updating an Existing Document

- Preserve the existing structure. Don't rewrite sections unrelated to your changes.
- Move old behaviour to a "Previous Behaviour" subsection under "Problem" if it's being replaced.
- Update the **Status** field if it has changed.
- If a document is fully superseded, change its status and link to the replacement.
