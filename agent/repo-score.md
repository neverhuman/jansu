# jankurai Repo Score

- Standard: `jankurai`
- Auditor: `1.3.0`
- Schema: `1.9.0`
- Paper edition: `2026.05-ed8`
- Target stack ID: `rust-ts-vite-react-postgres-bounded-python`
- Target stack: `Rust core + TypeScript/React/Vite + PostgreSQL + generated contracts + exception-only Python AI/data service`
- Repo: `.`
- Run ID: `1778969795`
- Started at: `1778969795`
- Elapsed: `9514` ms
- Scope: `full`
- Raw score: `82`
- Final score: `80`
- Decision: `fail`
- Minimum score: `85`
- Caps applied: `release-readiness-gap`

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
| `release-readiness-gap` | 80 | yes |
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

- Status: `review` hard=`0` warning=`217` files=`269`
- Policy: min-lines=`10` min-tokens=`100` max-findings=`50` include-tests=`false` strict=`false`
- Duplicate volume: lines=`1154` tokens=`11711` bytes=`56151`

- Notes:
  - hard classes are limited to exact active-source file matches and substantial exact same-name units
  - warning classes include same-body different-name units and token/block duplication
  - tests, fixtures, stories, config, Docker, and migrations are omitted unless --include-tests is set
  - showing the top 50 classes and omitting 167 lower-ranked classes

| Kind | Severity | Language | Lines | Tokens | Instances | Reason |
| --- | --- | --- | ---: | ---: | --- | --- |
| `ExactUnitDifferentName` | `Warning` | `rust` | 3 | 4 | `jansu-sans-io/src/de.rs:352-355, jansu-sans-io/src/de.rs:359-362, jansu-sans-io/src/de.rs:375-378, jansu-sans-io/src/de.rs:391-394, jansu-sans-io/src/de.rs:437-440, jansu-sans-io/src/de.rs:453-456, jansu-sans-io/src/de.rs:469-472, jansu-sans-io/src/de.rs:485-488, jansu-sans-io/src/de.rs:501-504, jansu-sans-io/src/de.rs:517-520, jansu-sans-io/src/de.rs:533-536, jansu-sans-io/src/de.rs:549-552, jansu-sans-io/src/de.rs:565-568, jansu-sans-io/src/de.rs:575-578, jansu-sans-io/src/de.rs:593-596, jansu-sans-io/src/de.rs:627-630, jansu-sans-io/src/de.rs:650-653, jansu-sans-io/src/de.rs:678-681, jansu-sans-io/src/de.rs:793-796, jansu-sans-io/src/de.rs:839-842, jansu-sans-io/src/de.rs:886-889, jansu-sans-io/src/de.rs:914-917, jansu-sans-io/src/de.rs:1001-1004, jansu-sans-io/src/de.rs:1038-1041, jansu-sans-io/src/de.rs:1108-1111, jansu-sans-io/src/de.rs:1115-1118, jansu-sans-io/src/de.rs:1122-1125, jansu-sans-io/src/de.rs:1129-1132, jansu-sans-io/src/de.rs:1136-1139, jansu-sans-io/src/de.rs:1143-1146, jansu-sans-io/src/de.rs:1150-1153, jansu-sans-io/src/de.rs:1157-1160, jansu-sans-io/src/de.rs:1164-1167, jansu-sans-io/src/de.rs:1171-1174, jansu-sans-io/src/de.rs:1178-1181, jansu-sans-io/src/de.rs:1185-1188, jansu-sans-io/src/de.rs:1192-1195, jansu-sans-io/src/de.rs:1199-1202, jansu-sans-io/src/de.rs:1206-1209, jansu-sans-io/src/de.rs:1213-1216, jansu-sans-io/src/de.rs:1220-1223, jansu-sans-io/src/de.rs:1227-1230, jansu-sans-io/src/de.rs:1234-1237, jansu-sans-io/src/de.rs:1269-1272, jansu-sans-io/src/de.rs:1305-1308, jansu-sans-io/src/de.rs:1342-1345, jansu-sans-io/src/de.rs:1349-1352, jansu-sans-io/src/de.rs:1485-1488, jansu-sans-io/src/primitive/tagged/de.rs:85-88, jansu-sans-io/src/primitive/tagged/de.rs:92-95, jansu-sans-io/src/primitive/tagged/de.rs:104-107, jansu-sans-io/src/primitive/tagged/de.rs:116-119, jansu-sans-io/src/primitive/tagged/de.rs:128-131, jansu-sans-io/src/primitive/tagged/de.rs:140-143, jansu-sans-io/src/primitive/tagged/de.rs:152-155, jansu-sans-io/src/primitive/tagged/de.rs:164-167, jansu-sans-io/src/primitive/tagged/de.rs:176-179, jansu-sans-io/src/primitive/tagged/de.rs:188-191, jansu-sans-io/src/primitive/tagged/de.rs:200-203, jansu-sans-io/src/primitive/tagged/de.rs:212-215, jansu-sans-io/src/primitive/tagged/de.rs:224-227, jansu-sans-io/src/primitive/tagged/de.rs:231-234, jansu-sans-io/src/primitive/tagged/de.rs:252-255, jansu-sans-io/src/primitive/tagged/de.rs:276-279, jansu-sans-io/src/primitive/tagged/de.rs:283-286, jansu-sans-io/src/primitive/tagged/de.rs:290-293, jansu-sans-io/src/primitive/tagged/de.rs:304-307, jansu-sans-io/src/primitive/tagged/de.rs:337-340, jansu-sans-io/src/primitive/tagged/de.rs:347-350, jansu-sans-io/src/primitive/tagged/de.rs:372-375, jansu-sans-io/src/primitive/tagged/de.rs:407-410, jansu-sans-io/src/primitive/tagged/de.rs:414-417` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 4 | 5 | `jansu-sans-io/src/primitive/tagged/ser.rs:181-185, jansu-sans-io/src/primitive/tagged/ser.rs:310-314, jansu-sans-io/src/primitive/tagged/ser.rs:329-333, jansu-sans-io/src/primitive/tagged/ser.rs:348-352, jansu-sans-io/src/primitive/tagged/ser.rs:367-371, jansu-sans-io/src/primitive/tagged/ser.rs:386-390, jansu-sans-io/src/primitive/tagged/ser.rs:395-399, jansu-sans-io/src/primitive/tagged/ser.rs:413-417, jansu-sans-io/src/primitive/tagged/ser.rs:432-436, jansu-sans-io/src/ser.rs:574-578, jansu-sans-io/src/ser.rs:782-786, jansu-sans-io/src/ser.rs:801-805, jansu-sans-io/src/ser.rs:820-824, jansu-sans-io/src/ser.rs:839-843, jansu-sans-io/src/ser.rs:858-862, jansu-sans-io/src/ser.rs:867-871, jansu-sans-io/src/ser.rs:885-889, jansu-sans-io/src/ser.rs:927-931` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 55 | 111 | `jansu-storage/src/limbo/tests.rs:197-252, jansu-storage/src/lite/tests.rs:197-252` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 48 | 113 | `jansu-storage/src/limbo/tests.rs:255-303, jansu-storage/src/lite/tests.rs:255-303` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 3 | `jansu-proxy/src/produce.rs:860-861, jansu-proxy/src/produce.rs:981-982, jansu-proxy/src/produce.rs:1186-1187, jansu-sans-io/src/record/codec.rs:573-574, jansu-sans-io/src/record/codec.rs:582-583, jansu-sans-io/src/record/codec.rs:591-592, jansu-sans-io/src/record/codec.rs:600-601, jansu-sans-io/src/record/codec.rs:609-610, jansu-schema/src/lib.rs:700-701, jansu-schema/src/lib.rs:732-733, jansu-schema/src/lib.rs:749-750, jansu-schema/src/lib.rs:771-772, jansu-schema/src/lib.rs:787-788, jansu-schema/src/proto.rs:903-904, jansu-schema/src/proto.rs:946-947, jansu-schema/src/proto.rs:996-997, jansu-schema/src/proto.rs:1046-1047, jansu-schema/src/proto.rs:1068-1069, jansu-schema/src/proto.rs:1097-1098, jansu-schema/src/proto.rs:1136-1137, jansu-schema/src/proto.rs:1173-1174, jansu-schema/src/proto.rs:1220-1221, jansu-schema/src/proto.rs:1258-1259, jansu-schema/src/proto.rs:1278-1279, jansu-schema/src/proto.rs:1303-1304, jansu-schema/src/proto.rs:1329-1330, jansu-schema/src/proto.rs:1356-1357, jansu-storage/src/dynostore/metadata/tests.rs:157-158, jansu-storage/src/dynostore/metadata/tests.rs:198-199, jansu-storage/src/dynostore/metadata/tests.rs:246-247, jansu-storage/src/dynostore/metadata/tests.rs:290-291, jansu-storage/src/gcs/limit.rs:236-237` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 23 | 58 | `jansu-storage/src/dynostore/opticon/tests.rs:29-52, jansu-storage/src/sql.rs:517-540` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 23 | 58 | `jansu-storage/src/limbo/tests.rs:21-44, jansu-storage/src/lite/tests.rs:21-44` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 22 | 49 | `jansu-client/src/lib.rs:836-858, jansu-proxy/src/lib.rs:368-390` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 7 | 15 | `jansu-sans-io/src/de.rs:804-811, jansu-sans-io/src/de.rs:823-830, jansu-sans-io/src/primitive/tagged/de.rs:311-318, jansu-sans-io/src/primitive/tagged/de.rs:325-332` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 3 | 4 | `jansu-sans-io/src/ser.rs:1098-1101, jansu-sans-io/src/ser.rs:1225-1228, jansu-sans-io/src/ser.rs:1242-1245, jansu-sans-io/src/ser.rs:1259-1262, jansu-sans-io/src/ser.rs:1276-1279, jansu-sans-io/src/ser.rs:1283-1286, jansu-sans-io/src/ser.rs:1322-1325` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 2 | 3 | `jansu-cat/src/lib.rs:49-51, jansu-cli/src/lib.rs:67-69, jansu-client/src/lib.rs:147-149, jansu-generator/src/lib.rs:102-104, jansu-perf/src/lib.rs:104-106, jansu-proxy/src/lib.rs:80-82, jansu-sans-io/build.rs:47-49, jansu-service/src/lib.rs:289-291, jansu-topic/src/lib.rs:53-55` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 3 | 5 | `jansu-broker/src/lib.rs:255-258, jansu-cat/src/lib.rs:43-46, jansu-generator/src/lib.rs:96-99, jansu-model/src/error.rs:58-61, jansu-perf/src/lib.rs:98-101, jansu-topic/src/lib.rs:41-44` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 3 | 4 | `jansu-sans-io/src/de.rs:1059-1062, jansu-sans-io/src/de.rs:1372-1375, jansu-sans-io/src/de.rs:1417-1420, jansu-sans-io/src/de.rs:1477-1480, jansu-sans-io/src/primitive/tagged/de.rs:437-440, jansu-sans-io/src/primitive/tagged/de.rs:474-477` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 3 | 2 | `jansu-proxy/src/produce.rs:509-512, jansu-proxy/src/produce.rs:535-538, jansu-sans-io/src/primitive/varint.rs:44-47, jansu-storage/src/limbo/timestamp.rs:23-26, jansu-storage/src/lite/lite_timestamp.rs:23-26, jansu-storage/src/sql.rs:35-38` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 1 | `jansu-sans-io/src/de.rs:106-107, jansu-sans-io/src/de.rs:117-118, jansu-sans-io/src/de.rs:134-135, jansu-sans-io/src/de.rs:151-152, jansu-sans-io/src/de.rs:1404-1405, jansu-sans-io/src/ser.rs:93-94, jansu-sans-io/src/ser.rs:132-133, jansu-sans-io/src/ser.rs:144-145, jansu-sans-io/src/ser.rs:168-169, jansu-schema/src/lib.rs:284-285, jansu-schema/src/lib.rs:429-430, jansu-schema/src/lib.rs:439-440, jansu-schema/src/lib.rs:446-447, jansu-service/src/frame.rs:543-544` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 12 | 21 | `jansu-cat/src/consume.rs:182-194, jansu-cat/src/produce.rs:137-149` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 3 | 4 | `jansu-sans-io/src/de.rs:1059-1062, jansu-sans-io/src/de.rs:1372-1375, jansu-sans-io/src/de.rs:1417-1420, jansu-sans-io/src/primitive/tagged/de.rs:437-440, jansu-sans-io/src/primitive/tagged/de.rs:474-477` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 5 | `jansu-sans-io/src/primitive/tagged/ser.rs:348-352, jansu-sans-io/src/primitive/tagged/ser.rs:367-371, jansu-sans-io/src/ser.rs:820-824, jansu-sans-io/src/ser.rs:839-843` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 5 | `jansu-sans-io/src/primitive/tagged/ser.rs:310-314, jansu-sans-io/src/primitive/tagged/ser.rs:329-333, jansu-sans-io/src/ser.rs:782-786, jansu-sans-io/src/ser.rs:801-805` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 5 | `jansu-sans-io/src/primitive/tagged/ser.rs:413-417, jansu-sans-io/src/primitive/tagged/ser.rs:432-436, jansu-sans-io/src/ser.rs:885-889, jansu-sans-io/src/ser.rs:927-931` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 1 | `jansu-perf/src/lib.rs:754-755, jansu-perf/src/lib.rs:759-760, jansu-sans-io/src/primitive/tagged/ser.rs:318-319, jansu-sans-io/src/primitive/tagged/ser.rs:337-338, jansu-sans-io/src/primitive/tagged/ser.rs:422-423, jansu-sans-io/src/primitive/tagged/ser.rs:441-442, jansu-sans-io/src/ser.rs:790-791, jansu-sans-io/src/ser.rs:809-810, jansu-sans-io/src/ser.rs:1093-1094, jansu-sans-io/src/ser.rs:1232-1233, jansu-sans-io/src/ser.rs:1312-1313, jansu-sans-io/src/ser.rs:1351-1352` | `same body appears under different names across files` |
| `TokenBlock` | `Warning` | `rust` | 10 | 280 | `fuzz/fuzz_targets/generate_seeds.rs:480-489, jansu-sans-io/src/bin/bench/api_versions.rs:66-75` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 275 | `fuzz/fuzz_targets/generate_seeds.rs:485-494, jansu-sans-io/src/bin/bench/api_versions.rs:71-80` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 271 | `fuzz/fuzz_targets/generate_seeds.rs:486-495, jansu-sans-io/src/bin/bench/api_versions.rs:72-81` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 262 | `fuzz/fuzz_targets/generate_seeds.rs:487-496, jansu-sans-io/src/bin/bench/api_versions.rs:73-82` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 220 | `jansu-sans-io/src/bin/bench/fetch.rs:250-259, jansu-sans-io/src/bin/bench/produce_large.rs:27-36` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 203 | `fuzz/fuzz_targets/generate_seeds.rs:650-659, jansu-sans-io/src/record/deflated.rs:682-691` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 201 | `fuzz/fuzz_targets/generate_seeds.rs:623-632, jansu-sans-io/src/record/deflated.rs:595-604` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 197 | `fuzz/fuzz_targets/generate_seeds.rs:677-686, jansu-sans-io/src/record/deflated.rs:858-867` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 197 | `fuzz/fuzz_targets/generate_seeds.rs:711-720, jansu-sans-io/src/record/deflated.rs:768-777` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 196 | `fuzz/fuzz_targets/generate_seeds.rs:651-660, jansu-sans-io/src/record/deflated.rs:683-692` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 195 | `fuzz/fuzz_targets/generate_seeds.rs:652-661, jansu-sans-io/src/record/deflated.rs:684-693` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 194 | `fuzz/fuzz_targets/generate_seeds.rs:655-664, jansu-sans-io/src/record/deflated.rs:687-696` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 194 | `fuzz/fuzz_targets/generate_seeds.rs:653-662, jansu-sans-io/src/record/deflated.rs:685-694` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 194 | `fuzz/fuzz_targets/generate_seeds.rs:658-667, jansu-sans-io/src/record/deflated.rs:690-699` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 194 | `fuzz/fuzz_targets/generate_seeds.rs:654-663, jansu-sans-io/src/record/deflated.rs:686-695` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 193 | `fuzz/fuzz_targets/generate_seeds.rs:657-666, jansu-sans-io/src/record/deflated.rs:689-698` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 193 | `fuzz/fuzz_targets/generate_seeds.rs:656-665, jansu-sans-io/src/record/deflated.rs:688-697` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 192 | `fuzz/fuzz_targets/generate_seeds.rs:624-633, jansu-sans-io/src/record/deflated.rs:596-605` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 191 | `fuzz/fuzz_targets/generate_seeds.rs:625-634, jansu-sans-io/src/record/deflated.rs:597-606` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 190 | `fuzz/fuzz_targets/generate_seeds.rs:678-687, jansu-sans-io/src/record/deflated.rs:859-868` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 190 | `fuzz/fuzz_targets/generate_seeds.rs:690-699, jansu-sans-io/src/record/deflated.rs:871-880` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 190 | `fuzz/fuzz_targets/generate_seeds.rs:712-721, jansu-sans-io/src/record/deflated.rs:769-778` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 189 | `fuzz/fuzz_targets/generate_seeds.rs:691-700, jansu-sans-io/src/record/deflated.rs:872-881` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 189 | `fuzz/fuzz_targets/generate_seeds.rs:689-698, jansu-sans-io/src/record/deflated.rs:870-879` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 189 | `fuzz/fuzz_targets/generate_seeds.rs:721-730, jansu-sans-io/src/record/deflated.rs:778-787` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 189 | `fuzz/fuzz_targets/generate_seeds.rs:720-729, jansu-sans-io/src/record/deflated.rs:777-786` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 189 | `fuzz/fuzz_targets/generate_seeds.rs:692-701, jansu-sans-io/src/record/deflated.rs:873-882` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 188 | `fuzz/fuzz_targets/generate_seeds.rs:685-694, jansu-sans-io/src/record/deflated.rs:866-875` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 188 | `fuzz/fuzz_targets/generate_seeds.rs:687-696, jansu-sans-io/src/record/deflated.rs:868-877` | `strict token/block duplication exceeded the configured threshold` |

## Dimensions

| Dimension | Weight | Score | Weighted | Evidence |
| --- | ---: | ---: | ---: | --- |
| Ownership and navigation surface | 13 | 100 | 13.00 | root `AGENTS.md` present; owner map present |
| Contract and boundary integrity | 13 | 98 | 12.74 | contract surface found; generated contract artifacts found |
| Proof lanes and test routing | 12 | 100 | 12.00 | one-command setup/validation lane found; deterministic fast lane found |
| Security and supply-chain posture | 12 | 74 | 8.88 | secret or dependency scan tooling found; provenance/SBOM tooling found |
| Code shape and semantic surface | 12 | 35 | 4.20 | largest authored code file: jansu-sans-io/src/de.rs (1510 LOC); code file exceeds 500 LOC |
| Data truth and workflow safety | 8 | 95 | 7.60 | database surface present; structured db boundary manifest present |
| Observability and repair evidence | 8 | 90 | 7.20 | observability libraries or patterns found; diagnostic shaping hints found |
| Context economy and agent instructions | 7 | 100 | 7.00 | root `AGENTS.md` present; root `AGENTS.md` stays short |
| Jankurai tool adoption and CI replacement | 7 | 30 | 2.10 | control-plane files present; applicable=16 |
| Python containment and polyglot hygiene | 4 | 100 | 4.00 | no Python files in scope |
| Build speed signals | 4 | 70 | 2.80 | build acceleration markers found; targeted test/build commands found |

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
   Reason: `Code shape and semantic surface` scored 35 below the standard floor of 85
   Fix: split large or ambiguous authored code into smaller semantic modules with focused tests
   Rerun: `just fast`
   Fingerprint: `sha256:314cff91e999a23d4e7dca4b44a33090029e1025b6faa32b6ba12e4b5e106abe`
   Evidence: largest authored code file: jansu-sans-io/src/de.rs (1510 LOC), code file exceeds 500 LOC, code file exceeds 1000 LOC, copy-code advisory classes found: 217 (advisory only, no score impact)
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
3. `medium` `proof` `Justfile`
   Rule: `HLT-018-PERF-CONCURRENCY-DRIFT`
   Check: `HLT-018-PERF-CONCURRENCY-DRIFT:proof` `soft` confidence `0.76`
   Route: TLR `Verification`, lane `fast`, owner `workspace`
   Docs: `docs/testing.md`
   Reason: `Build speed signals` scored 70 below the standard floor of 85
   Fix: add fast deterministic build/test targets, caches, and narrow proof lanes for agent iteration
   Rerun: `just fast`
   Fingerprint: `sha256:1ab579f1e82b68096d7993539cc68cbf5c5cf22e4aadf4d2e4515aa7279b5b22`
   Evidence: build acceleration markers found, targeted test/build commands found, CI cache hint found, explicit cache marker plus narrow per-package target found
4. `high` `release` `docs/testing.md`
   Rule: `HLT-025-RELEASE-READINESS-GAP`
   Check: `HLT-025-RELEASE-READINESS-GAP:release` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `release`, owner `standard`
   Docs: `docs/testing.md`
   Matched term: `release readiness`
   Reason: launch gates need artifact-backed release evidence
   Fix: add launch-gate evidence for security, backups, monitoring, rollback, and abuse controls
   Rerun: `just check`
   Fingerprint: `sha256:0f21fe749a193e4585d996dc6d063b41115e23a172c09ba76bba9b549301861b`
   Evidence: release language found without full launch-gate evidence

## Policy

- Policy file: `./agent/audit-policy.toml`
- Minimum score: `85`
- Fail on: `critical, high`

## Agent Fix Queue

1. `high` `HLT-025-RELEASE-READINESS-GAP` `docs/testing.md` - add launch-gate evidence for security, backups, monitoring, rollback, and abuse controls
   Route: `Verification`/`release`
2. `medium` `HLT-018-PERF-CONCURRENCY-DRIFT` `Justfile` - add fast deterministic build/test targets, caches, and narrow proof lanes for agent iteration
   Route: `Verification`/`fast`
3. `medium` `HLT-001-DEAD-MARKER` `.` - split large or ambiguous authored code into smaller semantic modules with focused tests
   Route: `Entropy`/`fast`
4. `medium` `HLT-016-SUPPLY-CHAIN-DRIFT` `.github/workflows/jankurai.yml` - wire secret, dependency, provenance, and workflow scans into an operational CI lane
   Route: `Security, secrets, agency`/`security`
