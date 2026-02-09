---
name: rust-lint-fmt
description: Run cargo clippy and nightly rustfmt to lint and format Rust code. Use when the user asks to lint, format, check code quality, or run CI checks on Rust code.
---

# Rust Lint & Format

## Clippy

Run clippy including test code:

```bash
cargo clippy --tests
```

### Fixing clippy warnings

- **Actually resolve** each warning by fixing the underlying code.
- **Never** suppress warnings with `#[allow(...)]`, `#[expect(...)]`, or `#![allow(...)]` attributes unless the user explicitly asks for it.
- If a warning seems like a false positive, ask the user before suppressing it.

## Rustfmt

Run nightly rustfmt to check which files need formatting:

```bash
cargo +nightly fmt -- -l --config imports_granularity=Module,group_imports=StdExternalCrate,reorder_imports=true
```

If files are listed in the output, format them by running:

```bash
cargo +nightly fmt -- --config imports_granularity=Module,group_imports=StdExternalCrate,reorder_imports=true
```

## Workflow

1. Run clippy. Fix all warnings (do not suppress with allow attributes). Re-run clippy until clean.
2. Run rustfmt in list mode. If files are listed, run rustfmt to apply formatting. Verify no files remain.
