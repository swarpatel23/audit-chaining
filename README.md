# Reladomo Audit & Bitemporal Chaining Sample

Small Maven project that demonstrates **temporal “chaining” semantics** in two ways:

- **Reladomo-managed** audit-only and bitemporal objects (via Mithra/Reladomo XML mappings + generated code)
- A **direct SQL** reference implementation in `TemporalSqlStore` that models the same ideas with plain JDBC + H2

## Prerequisites

- Java **11**
- Maven **3.x**

## Build & run tests

Run everything (includes Reladomo code generation during the build):

```bash
mvn test
```

Optional:

```bash
mvn clean test
```

## What’s in here

### Reladomo mappings (source of truth)

Reladomo object definitions live in:

- `src/main/resources/mithra/AuditAccount.xml`: **audit-only** (processing-time) mapping using `IN_Z` / `OUT_Z`
- `src/main/resources/mithra/BitemporalAccount.xml`: **bitemporal** mapping using:
  - business time: `FROM_Z` / `THRU_Z`
  - processing time: `IN_Z` / `OUT_Z`
- `src/main/resources/mithra/MithraClassList.xml`: list of objects to generate

Generated sources are written to:

- `target/generated-sources/reladomo/...` (do not edit by hand)

### Test runtime configuration

The Reladomo test harness is configured by:

- `src/test/resources/testconfig/ReladomoTestRuntimeConfiguration.xml` (resource name: `test_db`)
- `src/test/resources/testdata/ReladomoTestData.txt` (minimal schema seed used by the test util)

The base test setup is in `src/test/java/com/example/reladomo/ReladomoTestBase.java`.

### Tests to read

- `AuditOnlyChainingTest`: shows audit-only history where each update “closes” the prior row (`OUT_Z` becomes the next row’s `IN_Z`)
- `BitemporalChainingTest`: shows business-time corrections (splits/rewrites along business date while preserving processing history)
- `sql/DirectSqlChainingTest`: runs the same scenarios via `TemporalSqlStore` (no Reladomo), useful for understanding the raw SQL mechanics

## Temporal model (quick mental model)

- **Infinity**: “current” rows use a sentinel timestamp (Reladomo uses `DefaultInfinityTimestamp`; `TemporalSqlStore` uses `9999-12-31 23:59:59`).
- **Audit-only**: a single timeline (processing-time). Each change:
  1) updates the current row’s `OUT_Z` to “now”
  2) inserts a new row with `IN_Z = now` and `OUT_Z = infinity`
- **Bitemporal**: two timelines:
  - business validity: `[FROM_Z, THRU_Z)`
  - processing validity: `[IN_Z, OUT_Z)`

Business-time corrections are implemented as “rewrite the current business slices, then insert a new processing version”.

## Direct SQL reference (`TemporalSqlStore`)

`src/main/java/com/example/reladomo/sql/TemporalSqlStore.java` provides a plain-JDBC implementation against H2 that:

- runs each operation in a single JDBC transaction (`autoCommit=false` + commit/rollback)
- uses `SELECT ... FOR UPDATE` to lock the current rows for an account during bitemporal rewrites
- rewrites business slices in memory (split → merge → validate), then inserts the next processing version

## Notes / troubleshooting

- **SLF4J “StaticLoggerBinder” warning during tests**: Reladomo test dependencies can run without a logging backend; the warning is typically harmless for this sample.
- **Git push error `src refspec main does not match any`**: you likely haven’t created the first commit yet. Run:

```bash
git add .
git commit -m "Initial commit"
git push -u origin main
```

