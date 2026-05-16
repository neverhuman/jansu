# jankurai Repo Score

- Standard: `jankurai`
- Auditor: `1.3.0`
- Schema: `1.9.0`
- Paper edition: `2026.05-ed8`
- Target stack ID: `rust-ts-vite-react-postgres-bounded-python`
- Target stack: `Rust core + TypeScript/React/Vite + PostgreSQL + generated contracts + exception-only Python AI/data service`
- Repo: `.`
- Run ID: `1778971088`
- Started at: `1778971088`
- Elapsed: `5614` ms
- Scope: `full`
- Raw score: `85`
- Final score: `85`
- Decision: `advisory`
- Minimum score: `85`
- Caps applied: `none`

## Hard Rule Caps

| Rule | Max Score | Applied |
| --- | ---: | --- |
| `no-root-agent-instructions` | 75 | no |
| `no-one-command-setup-or-validation` | 70 | no |
| `no-deterministic-fast-lane` | 65 | no |
| `no-security-lane-on-high-risk-repo` | 60 | no |
| `generated-contracts-or-public-api-drift-untested` | 80 | no |
| `python-direct-product-truth-or-db-ownership` | 72 | no |
| `no-secret-or-dependency-scanning-in-ci` | 78 | no |
| `no-jankurai-audit-lane-in-ci` | 82 | no |
| `jankurai-required-tool-ci-evidence-gap` | 88 | no |
| `non-optimal-product-language-found` | 74 | no |
| `too-much-python-in-product-surface` | 72 | no |
| `boundary-reclassification-evidence-gap` | 72 | no |
| `vibe-placeholders-in-product-code` | 68 | no |
| `fallback-soup-in-product-code` | 70 | no |
| `future-hostile-dead-language-in-product-code` | 64 | no |
| `severe-duplication-in-product-code` | 70 | no |
| `generated-zone-mutation-risk` | 76 | no |
| `direct-db-access-from-wrong-layer` | 66 | no |
| `missing-web-e2e-lane` | 82 | no |
| `missing-rendered-ux-qa-lane` | 84 | no |
| `prompt-injection-risk` | 78 | no |
| `overbroad-agent-agency` | 65 | no |
| `secret-like-content-detected` | 60 | no |
| `false-green-test-risk` | 76 | no |
| `destructive-migration-risk` | 70 | no |
| `authz-or-data-isolation-gap` | 78 | no |
| `input-boundary-gap` | 78 | no |
| `agent-tool-supply-chain-gap` | 78 | no |
| `release-readiness-gap` | 80 | no |
| `missing-rust-property-or-integration-tests` | 82 | no |
| `no-agent-friendly-exception-pattern` | 76 | no |
| `missing-agent-readable-docs` | 80 | no |
| `streaming-runtime-drift` | 78 | no |
| `rust-bad-behavior` | 72 | no |
| `sql-bad-behavior` | 72 | no |
| `typescript-bad-behavior` | 72 | no |
| `docker-bad-behavior` | 72 | no |
| `python-bad-behavior` | 72 | no |
| `ci-bad-behavior` | 70 | no |
| `git-bad-behavior` | 70 | no |
| `gittools-bad-behavior` | 70 | no |
| `release-bad-behavior` | 70 | no |
| `web-security-bad-behavior` | 68 | no |
| `repo-rot-bad-behavior` | 88 | no |
| `comment-hygiene-dangerous-residue` | 72 | no |
| `ci-local-parity` | 70 | no |

## Copy-Code Redundancy

- Status: `review` hard=`0` warning=`119` files=`260`
- Policy: min-lines=`10` min-tokens=`100` max-findings=`50` include-tests=`false` strict=`false`
- Duplicate volume: lines=`527` tokens=`2363` bytes=`17654`

- Notes:
  - hard classes are limited to exact active-source file matches and substantial exact same-name units
  - warning classes include same-body different-name units and token/block duplication
  - tests, fixtures, stories, config, Docker, and migrations are omitted unless --include-tests is set
  - showing the top 50 classes and omitting 69 lower-ranked classes

| Kind | Severity | Language | Lines | Tokens | Instances | Reason |
| --- | --- | --- | ---: | ---: | --- | --- |
| `ExactUnitDifferentName` | `Warning` | `rust` | 3 | 4 | `jansu-sans-io/src/de_part2.rs:440-443, jansu-sans-io/src/primitive/tagged/de.rs:85-88, jansu-sans-io/src/primitive/tagged/de.rs:92-95, jansu-sans-io/src/primitive/tagged/de.rs:104-107, jansu-sans-io/src/primitive/tagged/de.rs:116-119, jansu-sans-io/src/primitive/tagged/de.rs:128-131, jansu-sans-io/src/primitive/tagged/de.rs:140-143, jansu-sans-io/src/primitive/tagged/de.rs:152-155, jansu-sans-io/src/primitive/tagged/de.rs:164-167, jansu-sans-io/src/primitive/tagged/de.rs:176-179, jansu-sans-io/src/primitive/tagged/de.rs:188-191, jansu-sans-io/src/primitive/tagged/de.rs:200-203, jansu-sans-io/src/primitive/tagged/de.rs:212-215, jansu-sans-io/src/primitive/tagged/de.rs:224-227, jansu-sans-io/src/primitive/tagged/de.rs:231-234, jansu-sans-io/src/primitive/tagged/de.rs:252-255, jansu-sans-io/src/primitive/tagged/de.rs:276-279, jansu-sans-io/src/primitive/tagged/de.rs:283-286, jansu-sans-io/src/primitive/tagged/de.rs:290-293, jansu-sans-io/src/primitive/tagged/de.rs:304-307, jansu-sans-io/src/primitive/tagged/de.rs:337-340, jansu-sans-io/src/primitive/tagged/de.rs:347-350, jansu-sans-io/src/primitive/tagged/de.rs:372-375, jansu-sans-io/src/primitive/tagged/de.rs:407-410, jansu-sans-io/src/primitive/tagged/de.rs:414-417` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 4 | 5 | `jansu-sans-io/src/primitive/tagged/ser.rs:181-185, jansu-sans-io/src/primitive/tagged/ser.rs:310-314, jansu-sans-io/src/primitive/tagged/ser.rs:329-333, jansu-sans-io/src/primitive/tagged/ser.rs:348-352, jansu-sans-io/src/primitive/tagged/ser.rs:367-371, jansu-sans-io/src/primitive/tagged/ser.rs:386-390, jansu-sans-io/src/primitive/tagged/ser.rs:395-399, jansu-sans-io/src/primitive/tagged/ser.rs:413-417, jansu-sans-io/src/primitive/tagged/ser.rs:432-436, jansu-sans-io/src/ser.rs:574-578, jansu-sans-io/src/ser.rs:782-786, jansu-sans-io/src/ser.rs:801-805, jansu-sans-io/src/ser.rs:820-824, jansu-sans-io/src/ser.rs:839-843, jansu-sans-io/src/ser.rs:858-862, jansu-sans-io/src/ser.rs:867-871, jansu-sans-io/src/ser.rs:885-889, jansu-sans-io/src/ser.rs:927-931` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 55 | 111 | `jansu-storage/src/limbo/tests.rs:197-252, jansu-storage/src/lite/tests.rs:197-252` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 48 | 113 | `jansu-storage/src/limbo/tests.rs:255-303, jansu-storage/src/lite/tests.rs:255-303` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 23 | 58 | `jansu-storage/src/dynostore/opticon/tests.rs:29-52, jansu-storage/src/sql.rs:517-540` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 23 | 58 | `jansu-storage/src/limbo/tests.rs:21-44, jansu-storage/src/lite/tests.rs:21-44` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 22 | 49 | `jansu-client/src/lib.rs:836-858, jansu-proxy/src/lib.rs:368-390` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 3 | 5 | `jansu-broker/src/lib.rs:255-258, jansu-cat/src/lib.rs:43-46, jansu-generator/src/lib.rs:96-99, jansu-model/src/error.rs:58-61, jansu-perf/src/lib.rs:98-101, jansu-topic/src/lib.rs:41-44` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 3 | `jansu-sans-io/src/record/codec.rs:573-574, jansu-sans-io/src/record/codec.rs:582-583, jansu-sans-io/src/record/codec.rs:591-592, jansu-sans-io/src/record/codec.rs:600-601, jansu-sans-io/src/record/codec.rs:609-610, jansu-schema/src/lib.rs:700-701, jansu-schema/src/lib.rs:732-733, jansu-schema/src/lib.rs:749-750, jansu-schema/src/lib.rs:771-772, jansu-schema/src/lib.rs:787-788, jansu-storage/src/dynostore/metadata/tests.rs:157-158, jansu-storage/src/dynostore/metadata/tests.rs:198-199, jansu-storage/src/dynostore/metadata/tests.rs:246-247, jansu-storage/src/dynostore/metadata/tests.rs:290-291, jansu-storage/src/gcs/limit.rs:236-237` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 14 | 31 | `jansu-sans-io/src/ser_part2.rs:333-347, jansu-sans-io/src/ser_part2.rs:372-386` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 2 | 3 | `jansu-cat/src/lib.rs:49-51, jansu-cli/src/lib.rs:67-69, jansu-client/src/lib.rs:147-149, jansu-generator/src/lib.rs:102-104, jansu-perf/src/lib.rs:104-106, jansu-proxy/src/lib.rs:80-82, jansu-service/src/lib.rs:289-291, jansu-topic/src/lib.rs:53-55` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 12 | 21 | `jansu-cat/src/consume.rs:182-194, jansu-cat/src/produce.rs:137-149` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 5 | `jansu-sans-io/src/primitive/tagged/ser.rs:348-352, jansu-sans-io/src/primitive/tagged/ser.rs:367-371, jansu-sans-io/src/ser.rs:820-824, jansu-sans-io/src/ser.rs:839-843` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 5 | `jansu-sans-io/src/primitive/tagged/ser.rs:310-314, jansu-sans-io/src/primitive/tagged/ser.rs:329-333, jansu-sans-io/src/ser.rs:782-786, jansu-sans-io/src/ser.rs:801-805` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 5 | `jansu-sans-io/src/primitive/tagged/ser.rs:413-417, jansu-sans-io/src/primitive/tagged/ser.rs:432-436, jansu-sans-io/src/ser.rs:885-889, jansu-sans-io/src/ser.rs:927-931` | `same-name semantic unit copied across multiple files` |
| `TokenBlock` | `Warning` | `rust` | 10 | 280 | `fuzz/fuzz_targets/generate_seeds.rs:480-489, jansu-sans-io/src/bin/bench/api_versions.rs:66-75` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 275 | `fuzz/fuzz_targets/generate_seeds.rs:485-494, jansu-sans-io/src/bin/bench/api_versions.rs:71-80` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 271 | `fuzz/fuzz_targets/generate_seeds.rs:486-495, jansu-sans-io/src/bin/bench/api_versions.rs:72-81` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 262 | `fuzz/fuzz_targets/generate_seeds.rs:487-496, jansu-sans-io/src/bin/bench/api_versions.rs:73-82` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 220 | `jansu-sans-io/src/bin/bench/fetch.rs:250-259, jansu-sans-io/src/bin/bench/produce_large.rs:27-36` | `strict token/block duplication exceeded the configured threshold` |
| `ExactUnitSameName` | `Warning` | `rust` | 5 | 11 | `jansu-storage/src/dynostore/metadata.rs:154-159, jansu-storage/src/dynostore/metron.rs:78-83, jansu-storage/src/gcs/limit.rs:133-138` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 10 | 21 | `jansu-sans-io/src/primitive/tagged/ser.rs:225-235, jansu-sans-io/src/ser.rs:677-687` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 5 | 10 | `jansu-storage/src/dynostore/metadata.rs:391-396, jansu-storage/src/dynostore/metron.rs:256-261, jansu-storage/src/gcs/limit.rs:186-191` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 3 | `jansu-model/src/wv.rs:51-52, jansu-model/src/wv.rs:60-61, jansu-model/src/wv.rs:80-81, jansu-model/src/wv.rs:131-132, jansu-model/src/wv.rs:139-140, jansu-model/src/wv.rs:147-148, jansu-model/src/wv.rs:161-162, jansu-model/src/wv.rs:169-170, jansu-model/src/wv.rs:177-178, jansu-model/src/wv.rs:203-204` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 9 | 20 | `jansu-sans-io/src/protocol_types.rs:496-505, jansu-sans-io/src/protocol_types.rs:509-518` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 3 | 3 | `jansu-sans-io/src/primitive/varint.rs:407-410, jansu-sans-io/src/primitive/varint.rs:585-588, jansu-sans-io/src/record/codec.rs:374-377, jansu-sans-io/src/record/codec.rs:497-500` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 3 | 2 | `jansu-sans-io/src/primitive/varint.rs:44-47, jansu-storage/src/limbo/timestamp.rs:23-26, jansu-storage/src/lite/lite_timestamp.rs:23-26, jansu-storage/src/sql.rs:35-38` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 4 | `jansu-sans-io/src/primitive/tagged/ser.rs:91-92, jansu-sans-io/src/primitive/tagged/ser.rs:97-98, jansu-sans-io/src/primitive/tagged/ser.rs:111-112, jansu-sans-io/src/primitive/tagged/ser.rs:117-118, jansu-sans-io/src/primitive/tagged/ser.rs:123-124, jansu-sans-io/src/primitive/tagged/ser.rs:129-130, jansu-sans-io/src/primitive/tagged/ser.rs:135-136, jansu-sans-io/src/primitive/tagged/ser.rs:141-142, jansu-sans-io/src/primitive/tagged/ser.rs:147-148` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 11 | `jansu-storage/src/dynostore/metadata.rs:187-191, jansu-storage/src/dynostore/metron.rs:114-118, jansu-storage/src/gcs/limit.rs:144-148` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 9 | `jansu-storage/src/dynostore/metadata.rs:215-219, jansu-storage/src/dynostore/metron.rs:146-150, jansu-storage/src/gcs/limit.rs:154-158` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 8 | 16 | `jansu-sans-io/src/primitive/tagged/ser.rs:212-220, jansu-sans-io/src/ser.rs:664-672` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 1 | `jansu-sans-io/src/ser.rs:93-94, jansu-sans-io/src/ser.rs:132-133, jansu-sans-io/src/ser.rs:144-145, jansu-sans-io/src/ser.rs:168-169, jansu-schema/src/lib.rs:284-285, jansu-schema/src/lib.rs:429-430, jansu-schema/src/lib.rs:439-440, jansu-schema/src/lib.rs:446-447, jansu-service/src/frame.rs:543-544` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 7 | 15 | `jansu-sans-io/src/primitive/tagged/de.rs:311-318, jansu-sans-io/src/primitive/tagged/de.rs:325-332` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 1 | `jansu-perf/src/lib.rs:754-755, jansu-perf/src/lib.rs:759-760, jansu-sans-io/src/primitive/tagged/ser.rs:318-319, jansu-sans-io/src/primitive/tagged/ser.rs:337-338, jansu-sans-io/src/primitive/tagged/ser.rs:422-423, jansu-sans-io/src/primitive/tagged/ser.rs:441-442, jansu-sans-io/src/ser.rs:790-791, jansu-sans-io/src/ser.rs:809-810` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 6 | `jansu-sans-io/src/primitive/tagged.rs:406-407, jansu-sans-io/src/primitive/tagged.rs:436-437, jansu-sans-io/src/primitive/tagged.rs:463-464, jansu-sans-io/src/primitive/tagged.rs:488-489, jansu-sans-io/src/primitive/tagged.rs:518-519, jansu-sans-io/src/primitive/tagged.rs:545-546, jansu-sans-io/src/primitive/tagged.rs:570-571` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 3 | 14 | `jansu-storage/src/dynostore/metadata.rs:338-341, jansu-storage/src/dynostore/metron.rs:181-184, jansu-storage/src/gcs/limit.rs:162-165` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 3 | 10 | `jansu-storage/src/dynostore/metadata.rs:358-361, jansu-storage/src/dynostore/metron.rs:209-212, jansu-storage/src/gcs/limit.rs:170-173` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 3 | 8 | `jansu-storage/src/dynostore/metadata.rs:368-371, jansu-storage/src/dynostore/metron.rs:218-221, jansu-storage/src/gcs/limit.rs:178-181` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 6 | 16 | `jansu-sans-io/src/primitive/tagged/ser.rs:293-299, jansu-sans-io/src/ser.rs:758-764` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 6 | 16 | `jansu-sans-io/src/primitive/tagged/ser.rs:268-274, jansu-sans-io/src/ser.rs:726-732` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 2 | 5 | `jansu-auth/src/lib.rs:56-58, jansu-client/src/lib.rs:153-155, jansu-proxy/src/lib.rs:86-88, jansu-service/src/lib.rs:295-297` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 2 | 5 | `jansu-client/src/lib.rs:168-170, jansu-proxy/src/lib.rs:110-112, jansu-service/src/lib.rs:301-303, jansu-storage/src/error.rs:226-228` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 6 | 12 | `jansu-schema/src/lib.rs:318-324, jansu-schema/src/lib.rs:610-616` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 2 | 3 | `jansu-auth/src/lib.rs:50-52, jansu-otel/src/lib.rs:40-42, jansu-schema/src/lib.rs:189-191, jansu-storage/src/error.rs:165-167` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 3 | 4 | `jansu-sans-io/src/de_part2.rs:432-435, jansu-sans-io/src/primitive/tagged/de.rs:437-440, jansu-sans-io/src/primitive/tagged/de.rs:474-477` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 3 | 4 | `jansu-sans-io/src/primitive/varint.rs:416-419, jansu-sans-io/src/primitive/varint.rs:594-597, jansu-sans-io/src/record/codec.rs:511-514` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 6 | 7 | `jansu-service/src/channel.rs:38-44, jansu-storage/src/service/channel_request.rs:17-23` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 3 | 2 | `jansu-service/src/channel.rs:30-33, jansu-service/src/stream.rs:47-50, jansu-storage/src/service/channel_request.rs:9-12` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 3 | 2 | `jansu-broker/src/lib.rs:261-264, jansu-generator/src/lib.rs:84-87, jansu-perf/src/lib.rs:92-95` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 5 | `jansu-sans-io/src/primitive/tagged/ser.rs:190-191, jansu-sans-io/src/primitive/tagged/ser.rs:356-357, jansu-sans-io/src/ser.rs:642-643, jansu-sans-io/src/ser.rs:828-829, jansu-sans-io/src/ser.rs:847-848, jansu-sans-io/src/ser.rs:875-876` | `same body appears under different names across files` |

## Dimensions

| Dimension | Weight | Score | Weighted | Evidence |
| --- | ---: | ---: | ---: | --- |
| Ownership and navigation surface | 13 | 100 | 13.00 | root `AGENTS.md` present; owner map present |
| Contract and boundary integrity | 13 | 98 | 12.74 | contract surface found; generated contract artifacts found |
| Proof lanes and test routing | 12 | 100 | 12.00 | one-command setup/validation lane found; deterministic fast lane found |
| Security and supply-chain posture | 12 | 74 | 8.88 | secret or dependency scan tooling found; provenance/SBOM tooling found |
| Code shape and semantic surface | 12 | 55 | 6.60 | largest authored code file: jansu-sans-io/src/ser.rs (970 LOC); code file exceeds 500 LOC |
| Data truth and workflow safety | 8 | 95 | 7.60 | database surface present; structured db boundary manifest present |
| Observability and repair evidence | 8 | 90 | 7.20 | observability libraries or patterns found; diagnostic shaping hints found |
| Context economy and agent instructions | 7 | 100 | 7.00 | root `AGENTS.md` present; root `AGENTS.md` stays short |
| Jankurai tool adoption and CI replacement | 7 | 30 | 2.10 | control-plane files present; applicable=16 |
| Python containment and polyglot hygiene | 4 | 100 | 4.00 | no Python files in scope |
| Build speed signals | 4 | 85 | 3.40 | build acceleration markers found; targeted test/build commands found |

## Reference Profile Structure

- Applicable cells: `9` canonical=`9` noncanonical=`0` guidance missing=`0`

| Cell | Status | Canonical | Detected | Aliases | Guidance | Owner | Proof lane | Agent fix |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `web` | `not_applicable` | `apps/web/` | `-` | `frontend/, ui/, packages/web/, packages/ui/` | `not_required` | `apps/web` | `rendered UX / Playwright` | `no action` |
| `api` | `canonical` | `apps/api/` | `apps/api` | `api/, server/, backend/` | `present` | `apps/api` | `edge handler / contract tests` | `keep `apps/api/AGENTS.md` aligned with owns / forbidden / proof lane guidance` |
| `domain` | `canonical` | `crates/domain/` | `crates/domain` | `domain/, core/` | `present` | `crates/domain` | `unit / property tests` | `keep `crates/domain/AGENTS.md` aligned with owns / forbidden / proof lane guidance` |
| `application` | `canonical` | `crates/application/` | `crates/application` | `application/, usecases/, use-cases/` | `present` | `crates/application` | `use-case / authz tests` | `keep `crates/application/AGENTS.md` aligned with owns / forbidden / proof lane guidance` |
| `adapters` | `canonical` | `crates/adapters/` | `crates/adapters` | `adapters/, infra/, integrations/` | `present` | `crates/adapters` | `adapter integration tests` | `keep `crates/adapters/AGENTS.md` aligned with owns / forbidden / proof lane guidance` |
| `workers` | `canonical` | `crates/workers/` | `crates/workers` | `workers/, jobs/, scheduler/, queue/` | `present` | `crates/workers` | `workflow / replay tests` | `keep `crates/workers/AGENTS.md` aligned with owns / forbidden / proof lane guidance` |
| `contracts` | `canonical` | `contracts/` | `contracts` | `openapi/, protobuf/, json-schema/, generated/` | `present` | `contracts` | `generation / drift checks` | `keep `contracts/AGENTS.md` aligned with owns / forbidden / proof lane guidance` |
| `db` | `canonical` | `db/` | `db` | `migrations/, constraints/, sql/` | `present` | `db` | `migration / constraint tests` | `keep `db/AGENTS.md` aligned with owns / forbidden / proof lane guidance` |
| `python-ai` | `canonical` | `python/ai-service/` | `python, python/ai-service` | `python/, ai-service/, evals/, embeddings/, model/` | `present` | `python/ai-service` | `eval / contract tests` | `keep `python/ai-service/AGENTS.md` aligned with owns / forbidden / proof lane guidance` |
| `ops` | `canonical` | `ops/` | `.github, .github/workflows, ops` | `.github/, .github/workflows/, ci/, release/, observability/, security/` | `present` | `ops` | `security lane / workflow lint` | `keep `ops/AGENTS.md` aligned with owns / forbidden / proof lane guidance` |

## Rendered UX QA

- Web surface: `false`
- Layered UX lane: `true`
- Missing: `none`

## Tool Adoption

- Control plane present: `true`
- Applicable tools: `16`
- Configured: `16`
- CI evidence: `0`
- Artifact verified: `0`
- Replaced count: `0`
- Missing CI evidence: `audit-ci, proof-routing, proofbind, proofmark-rust, copy-code, security, ci-bad-behavior, git-bad-behavior, release-bad-behavior, contract-drift, rust-witness, authz-matrix, input-boundary, agent-tool-supply, release-readiness, cost-budget`

| Tool | Category | Mode | Status | Replaced | Artifacts |
| --- | --- | --- | --- | --- | --- |
| `audit-ci` | `audit` | `auto` | `configured` | `manual repo scoring, ad hoc score gates` | `agent/repo-score.json, agent/repo-score.md` |
| `proof-routing` | `proof` | `auto` | `configured` | `ad hoc proof lane selection, manual proof receipts` | `agent/repo-score.json, agent/repo-score.md, target/jankurai/repair-queue.jsonl` |
| `proofbind` | `proof` | `auto` | `configured` | `manual changed-surface routing, ad hoc proof obligation lists` | `target/jankurai/proofbind/surface-witness.json, target/jankurai/proofbind/obligations.json` |
| `proofmark-rust` | `proof` | `auto` | `configured` | `line-only coverage review, manual in-diff mutation review` | `target/jankurai/proofmark/proofmark-receipt.json, target/jankurai/proofmark/proof-receipt.json` |
| `copy-code` | `audit` | `auto` | `configured` | `ad hoc copy-code review, manual duplication triage` | `target/jankurai/copy-code.json, target/jankurai/copy-code.md` |
| `security` | `security` | `auto` | `configured` | `gitleaks, dependency review, SBOM/provenance` | `target/jankurai/security/evidence.json` |
| `ci-bad-behavior` | `security` | `auto` | `configured` | `mutable workflow refs, secret echo/debug workflow checks, non-blocking security scans` | `target/jankurai/language-bad-behavior.log` |
| `git-bad-behavior` | `audit` | `auto` | `configured` | `destructive git automation, force-push release scripts, hidden stash-based state` | `target/jankurai/language-bad-behavior.log` |
| `release-bad-behavior` | `release` | `auto` | `configured` | `manual release checklist, ad hoc tag and artifact review, manual provenance review` | `target/jankurai/language-bad-behavior.log` |
| `ux-qa` | `ux` | `auto` | `not_applicable` | `playwright, axe-core, visual baselines` | `target/jankurai/ux-qa.json` |
| `db-migration-analyze` | `db` | `auto` | `not_applicable` | `manual migration review` | `target/jankurai/migration-report.json` |
| `contract-drift` | `contract` | `auto` | `configured` | `handwritten contract drift checks, openapi diff` | `agent/repo-score.json, agent/repo-score.md` |
| `rust-witness` | `rust` | `auto` | `configured` | `manual witness graphing` | `target/jankurai/rust/witness-graph.json` |
| `vibe-coverage` | `audit` | `auto` | `not_applicable` | `manual vibe-coding coverage spreadsheet` | `target/jankurai/vibe-coverage.json, target/jankurai/vibe-coverage.md` |
| `coverage-evidence` | `proof` | `auto` | `not_applicable` | `manual coverage report review, ad hoc mutation survivor review` | `target/jankurai/coverage/coverage-audit.json, target/jankurai/coverage/coverage-audit.md` |
| `authz-matrix` | `security` | `auto` | `configured` | `manual authz matrix review` | `agent/repo-score.json, agent/repo-score.md` |
| `input-boundary` | `security` | `auto` | `configured` | `manual unsafe sink review` | `agent/repo-score.json, agent/repo-score.md` |
| `agent-tool-supply` | `security` | `auto` | `configured` | `manual MCP/tool trust review` | `agent/repo-score.json, agent/repo-score.md` |
| `release-readiness` | `release` | `auto` | `configured` | `manual launch checklist` | `agent/repo-score.json, agent/repo-score.md` |
| `cost-budget` | `release` | `auto` | `configured` | `manual spend review` | `agent/repo-score.json, agent/repo-score.md` |

## Security evidence (ingested)

- Source: `target/jankurai/security/evidence.json`
- Envelope exit code: `0` · elapsed: `92976` ms · strict: `false`
- Commands — ran: `1`, skipped: `0`, failed: `0`
- Generated at: `1778971035`
- Git HEAD (envelope): `60b4d058668e7d36b7792681c770b2e93a2d6943`

## Boundary manifest (ingested)

- Path: `agent/boundaries.toml`
- Stack: `rust-postgres-kafka-native` · version: `0.5.0`
- Queue path counts — adapter: `2`, event_contract: `1`, generated_type: `1`, client_marker: `8`, streaming_exception: `2`
- Content fingerprint: `sha256:90988d9de819c8fe4d700c2706e385226a283cf83573462a84c7694034f78fdf`

## Boundary Reclassifications

No audited runtime boundary reclassifications declared.

## Findings

1. `medium` `shape` `.`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:shape` `soft` confidence `0.76`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: `Code shape and semantic surface` scored 55 below the standard floor of 85
   Fix: split large or ambiguous authored code into smaller semantic modules with focused tests
   Rerun: `just fast`
   Fingerprint: `sha256:925bdf46e321f42733fec6388bd35b933c37f2f20b793878627df0ca58ff0c5f`
   Evidence: largest authored code file: jansu-sans-io/src/ser.rs (970 LOC), code file exceeds 500 LOC, copy-code advisory classes found: 119 (advisory only, no score impact), rust bad-behavior advisory signals: 1099
2. `medium` `security` `.github/workflows/jankurai.yml`
   Rule: `HLT-016-SUPPLY-CHAIN-DRIFT`
   Check: `HLT-016-SUPPLY-CHAIN-DRIFT:security` `soft` confidence `0.76`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/audit-rubric.md#top-level-risk-mapping`
   Reason: `Security and supply-chain posture` scored 74 below the standard floor of 85
   Fix: wire secret, dependency, provenance, and workflow scans into an operational CI lane
   Rerun: `just security`
   Fingerprint: `sha256:113b64f53ea1ab1b676b502b23f5b6ea736adbd7993914eecc88c7b99fe047ab`
   Evidence: secret or dependency scan tooling found, provenance/SBOM tooling found, workflow linting tooling found, security lane present

## Policy

- Policy file: `./agent/audit-policy.toml`
- Minimum score: `85`
- Fail on: `critical, high`

## Agent Fix Queue

1. `medium` `HLT-001-DEAD-MARKER` `.` - split large or ambiguous authored code into smaller semantic modules with focused tests
   Route: `Entropy`/`fast`
2. `medium` `HLT-016-SUPPLY-CHAIN-DRIFT` `.github/workflows/jankurai.yml` - wire secret, dependency, provenance, and workflow scans into an operational CI lane
   Route: `Security, secrets, agency`/`security`
