# Extract Faster N-Quads Parser And K-Way Chunking

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` will be kept up to date as work proceeds.

This document follows `PLANS.md` from the repository root.

## Purpose / Big Picture

After this change, the current branch will include the faster N-Quads parsing path and the chunked k-way merge architecture from `branch-with-faster-code`, while intentionally excluding compressor implementation changes (for example `SectionCompressor` and sibling compressor classes). The result will be visible through focused parser/merge tests that pass on this branch.

## Progress

- [x] (2026-02-09 13:38Z) Read repo-level planning and workflow rules.
- [x] (2026-02-09 13:40Z) Confirm source snapshot exists at `branch-with-faster-code/`.
- [x] (2026-02-09 13:44Z) Map exact parser + merge files to transplant; define compressor exclusion list.
- [x] (2026-02-09 13:45Z) Create selective patch set from source snapshot.
- [x] (2026-02-09 13:46Z) Apply production code changes for parser and merge.
- [x] (2026-02-09 13:46Z) Apply relevant tests/resources only for transplanted behavior.
- [x] (2026-02-09 13:51Z) Run install + targeted verifies; adjust minimal compile/test fallout.
- [x] (2026-02-09 13:53Z) Finalize plan log and handoff summary.

## Surprises & Discoveries

- Observation: `branch-with-faster-code/` is a plain source snapshot, not a git branch with commit metadata.
  Evidence: `git -C branch-with-faster-code status --short` produced no output and no `.git` directory is present in listing.

- Observation: `NTriplesChunkedSource` could not compile until parser-oriented `TripleString` additions were transplanted (`ComponentDecoderChars` and byte-string decode entry points).
  Evidence: compile error during root install referenced missing symbol `TripleString.ComponentDecoderChars` in `NTriplesChunkedSource`.

- Observation: full `qendpoint-core` verify succeeded after selective transplant, without applying compressor implementation diffs.
  Evidence: Maven summary reported `Tests run: 2744, Failures: 0, Errors: 0, Skipped: 24` and `BUILD SUCCESS`.

## Decision Log

- Decision: Use file-level diff (`diff -qr` and targeted `diff -u`) between repo root and `branch-with-faster-code/` to isolate transferable changes.
  Rationale: No commit history in source snapshot, so selective transplant must be path-and-hunk based.
  Date/Author: 2026-02-09 / Codex

- Decision: Exclude all changes under `qendpoint-core/src/main/java/com/the_qa_company/qendpoint/core/hdt/impl/diskimport/*Compressor*` unless a compile blocker proves an unavoidable API dependency.
  Rationale: User explicitly requested excluding compressor changes, especially `SectionCompressor`.
  Date/Author: 2026-02-09 / Codex

- Decision: Bring parser-adjacent utility dependencies (`UnicodeEscape`, `ByteStringInterner`, `BigMappedByteBuffer#close`, `TripleString` byte-string decoding support, and `LexerFixer`) when required by parser/chunking code.
  Rationale: These dependencies are structural prerequisites for the faster N-Triples/N-Quads chunked parser path and RIOT parallel parser support.
  Date/Author: 2026-02-09 / Codex

## Outcomes & Retrospective

Selective extraction completed. Parser fast-path and RIOT parallel parser support were transplanted, plus k-way chunked merge architecture classes and corresponding tests/resources. Compressor implementation files were intentionally left unchanged, matching requested scope. The final `qendpoint-core` verification run passed.

## Context and Orientation

The source snapshot is at `branch-with-faster-code/`. The target codebase is repository root.

Relevant areas expected for this extraction:

- Parser pipeline: `qendpoint-core/src/main/java/com/the_qa_company/qendpoint/core/rdf/` and `qendpoint-core/src/main/java/com/the_qa_company/qendpoint/core/rdf/parsers/`.
- K-way merge chunking architecture: `qendpoint-core/src/main/java/com/the_qa_company/qendpoint/core/util/concurrent/` and `qendpoint-core/src/main/java/com/the_qa_company/qendpoint/core/iterator/utils/`.
- Tests and test resources: `qendpoint-core/src/test/java/com/the_qa_company/qendpoint/core/rdf/`, `qendpoint-core/src/test/java/com/the_qa_company/qendpoint/core/rdf/parsers/`, `qendpoint-core/src/test/java/com/the_qa_company/qendpoint/core/util/concurrent/`, `qendpoint-core/src/test/java/com/the_qa_company/qendpoint/core/iterator/utils/`, and relevant `qendpoint-core/src/test/resources/`.

Explicitly out of scope:

- Compressor implementation changes, especially under `qendpoint-core/src/main/java/com/the_qa_company/qendpoint/core/hdt/impl/diskimport/` for classes such as `SectionCompressor`, `MultiSectionSectionCompressor`, and language-prefix compressor variants.

## Plan of Work

First, build a scoped inventory of changed files by comparing target and source snapshot in parser and merge-related packages. Then inspect each diff to decide if it belongs to parser speedups or merge chunking architecture. Keep compressor paths filtered out.

Next, apply only selected changes into the target repository, preferring full-file copies when the file exists only in source and hunk-level patches when existing files contain mixed concerns.

After production changes, copy matching tests/resources that validate imported behavior without bringing compressor-only tests.

Finally, run required install/build and targeted module verifies. If a compile/test failure shows a missing dependency created by excluded compressor changes, resolve by minimal neutral adaptation in non-compressor code or by dropping the dependent transplant piece.

## Concrete Steps

From repository root:

1. Inventory scoped file diffs.
2. Produce selected file list for parser and merge.
3. Apply selected production files/hunks.
4. Apply selected tests/resources.
5. Run:
   `mvn -T 1C -o -Dmaven.repo.local=.m2_repo -DskipTests clean install | tail -200`
6. Run focused verifies (no `-am`, no `-q`):
   `mvn -o -Dmaven.repo.local=.m2_repo -pl qendpoint-core -Dtest=<scoped classes> verify | tail -500`
7. Run module verify if focused tests pass:
   `mvn -o -Dmaven.repo.local=.m2_repo -pl qendpoint-core verify | tail -500`

## Validation and Acceptance

Acceptance criteria:

- Parser tests for N-Quads and parser factory/parallel support added by this extraction pass.
- K-way merge chunking tests added by this extraction pass.
- `qendpoint-core` verify passes without importing compressor implementation changes.

Behavioral proof:

- Newly imported tests fail if transplanted parser/merge logic is reverted and pass with extracted code.

## Idempotence and Recovery

Steps are idempotent: re-copying selected files or reapplying same hunks should produce no change.

If a selected file brings unwanted compressor coupling, revert only that file/hunk and re-run focused tests to retain requested scope boundaries.

## Artifacts and Notes

- Key commands run:
  - `mvn -T 1C -o -Dmaven.repo.local=.m2_repo -DskipTests clean install | tail -200`
  - `mvn -o -Dmaven.repo.local=.m2_repo -pl qendpoint-core -Dtest=RDFParserFactoryDefaultTest,...,PipedCopyIteratorClosePipeTest verify | tail -500`
  - `mvn -o -Dmaven.repo.local=.m2_repo -pl qendpoint-core verify | tail -500`
- Verification outcomes:
  - Focused transplanted test set: passed (`35` tests, `0` failures).
  - Full `qendpoint-core` verify: passed (`2744` tests, `0` failures, `0` errors).

## Interfaces and Dependencies

No new external dependencies are planned. This extraction should reuse existing module dependencies already present in the source snapshot and current branch.

Revision note (2026-02-09): Initial ExecPlan created to guide selective transplant of parser and k-way chunking changes from source snapshot.
Revision note (2026-02-09): Updated with completed implementation progress, compile-discovery notes, final verification results, and scope confirmation for compressor exclusion.
