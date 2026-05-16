# jankurai Repo Score

- Standard: `jankurai`
- Auditor: `0.7.0`
- Schema: `1.5.0`
- Paper edition: `2026.05-ed7`
- Target stack ID: `rust-ts-vite-react-postgres-bounded-python`
- Target stack: `Rust core + TypeScript/React/Vite + PostgreSQL + generated contracts + exception-only Python AI/data service`
- Repo: `.`
- Run ID: `1778950148`
- Started at: `1778950148`
- Elapsed: `8154` ms
- Scope: `full`
- Raw score: `75`
- Final score: `70`
- Decision: `advisory`
- Minimum score: `85`
- Caps applied: `fallback-soup-in-product-code, release-readiness-gap`

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
| `fallback-soup-in-product-code` | 70 | yes |
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

- Status: `review` hard=`0` warning=`233` files=`238`
- Policy: min-lines=`10` min-tokens=`100` max-findings=`50` include-tests=`false` strict=`false`
- Duplicate volume: lines=`1164` tokens=`11546` bytes=`56079`

- Notes:
  - hard classes are limited to exact active-source file matches and substantial exact same-name units
  - warning classes include same-body different-name units and token/block duplication
  - tests, fixtures, stories, config, Docker, and migrations are omitted unless --include-tests is set
  - showing the top 50 classes and omitting 183 lower-ranked classes

| Kind | Severity | Language | Lines | Tokens | Instances | Reason |
| --- | --- | --- | ---: | ---: | --- | --- |
| `ExactUnitDifferentName` | `Warning` | `rust` | 3 | 4 | `jansu-sans-io/src/de.rs:352-355, jansu-sans-io/src/de.rs:359-362, jansu-sans-io/src/de.rs:375-378, jansu-sans-io/src/de.rs:391-394, jansu-sans-io/src/de.rs:437-440, jansu-sans-io/src/de.rs:453-456, jansu-sans-io/src/de.rs:469-472, jansu-sans-io/src/de.rs:485-488, jansu-sans-io/src/de.rs:501-504, jansu-sans-io/src/de.rs:517-520, jansu-sans-io/src/de.rs:533-536, jansu-sans-io/src/de.rs:549-552, jansu-sans-io/src/de.rs:565-568, jansu-sans-io/src/de.rs:575-578, jansu-sans-io/src/de.rs:593-596, jansu-sans-io/src/de.rs:627-630, jansu-sans-io/src/de.rs:650-653, jansu-sans-io/src/de.rs:678-681, jansu-sans-io/src/de.rs:793-796, jansu-sans-io/src/de.rs:839-842, jansu-sans-io/src/de.rs:886-889, jansu-sans-io/src/de.rs:914-917, jansu-sans-io/src/de.rs:1001-1004, jansu-sans-io/src/de.rs:1038-1041, jansu-sans-io/src/de.rs:1108-1111, jansu-sans-io/src/de.rs:1115-1118, jansu-sans-io/src/de.rs:1122-1125, jansu-sans-io/src/de.rs:1129-1132, jansu-sans-io/src/de.rs:1136-1139, jansu-sans-io/src/de.rs:1143-1146, jansu-sans-io/src/de.rs:1150-1153, jansu-sans-io/src/de.rs:1157-1160, jansu-sans-io/src/de.rs:1164-1167, jansu-sans-io/src/de.rs:1171-1174, jansu-sans-io/src/de.rs:1178-1181, jansu-sans-io/src/de.rs:1185-1188, jansu-sans-io/src/de.rs:1192-1195, jansu-sans-io/src/de.rs:1199-1202, jansu-sans-io/src/de.rs:1206-1209, jansu-sans-io/src/de.rs:1213-1216, jansu-sans-io/src/de.rs:1220-1223, jansu-sans-io/src/de.rs:1227-1230, jansu-sans-io/src/de.rs:1234-1237, jansu-sans-io/src/de.rs:1269-1272, jansu-sans-io/src/de.rs:1305-1308, jansu-sans-io/src/de.rs:1342-1345, jansu-sans-io/src/de.rs:1349-1352, jansu-sans-io/src/de.rs:1485-1488, jansu-sans-io/src/primitive/tagged/de.rs:85-88, jansu-sans-io/src/primitive/tagged/de.rs:92-95, jansu-sans-io/src/primitive/tagged/de.rs:104-107, jansu-sans-io/src/primitive/tagged/de.rs:116-119, jansu-sans-io/src/primitive/tagged/de.rs:128-131, jansu-sans-io/src/primitive/tagged/de.rs:140-143, jansu-sans-io/src/primitive/tagged/de.rs:152-155, jansu-sans-io/src/primitive/tagged/de.rs:164-167, jansu-sans-io/src/primitive/tagged/de.rs:176-179, jansu-sans-io/src/primitive/tagged/de.rs:188-191, jansu-sans-io/src/primitive/tagged/de.rs:200-203, jansu-sans-io/src/primitive/tagged/de.rs:212-215, jansu-sans-io/src/primitive/tagged/de.rs:224-227, jansu-sans-io/src/primitive/tagged/de.rs:231-234, jansu-sans-io/src/primitive/tagged/de.rs:255-258, jansu-sans-io/src/primitive/tagged/de.rs:284-287, jansu-sans-io/src/primitive/tagged/de.rs:291-294, jansu-sans-io/src/primitive/tagged/de.rs:298-301, jansu-sans-io/src/primitive/tagged/de.rs:312-315, jansu-sans-io/src/primitive/tagged/de.rs:345-348, jansu-sans-io/src/primitive/tagged/de.rs:355-358, jansu-sans-io/src/primitive/tagged/de.rs:380-383, jansu-sans-io/src/primitive/tagged/de.rs:415-418, jansu-sans-io/src/primitive/tagged/de.rs:422-425` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 4 | 5 | `jansu-sans-io/src/primitive/tagged/ser.rs:181-185, jansu-sans-io/src/primitive/tagged/ser.rs:310-314, jansu-sans-io/src/primitive/tagged/ser.rs:329-333, jansu-sans-io/src/primitive/tagged/ser.rs:348-352, jansu-sans-io/src/primitive/tagged/ser.rs:367-371, jansu-sans-io/src/primitive/tagged/ser.rs:386-390, jansu-sans-io/src/primitive/tagged/ser.rs:395-399, jansu-sans-io/src/primitive/tagged/ser.rs:413-417, jansu-sans-io/src/primitive/tagged/ser.rs:432-436, jansu-sans-io/src/ser.rs:574-578, jansu-sans-io/src/ser.rs:782-786, jansu-sans-io/src/ser.rs:801-805, jansu-sans-io/src/ser.rs:820-824, jansu-sans-io/src/ser.rs:839-843, jansu-sans-io/src/ser.rs:858-862, jansu-sans-io/src/ser.rs:867-871, jansu-sans-io/src/ser.rs:885-889, jansu-sans-io/src/ser.rs:927-931` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 55 | 111 | `jansu-storage/src/limbo/tests.rs:197-252, jansu-storage/src/lite/tests.rs:197-252` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 48 | 113 | `jansu-storage/src/limbo/tests.rs:255-303, jansu-storage/src/lite/tests.rs:255-303` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 22 | 49 | `jansu-schema/src/avro.rs:702-724, jansu-schema/src/lake/delta.rs:854-876, jansu-schema/src/proto/arrow.rs:1335-1357` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 3 | `jansu-proxy/src/produce.rs:857-858, jansu-proxy/src/produce.rs:978-979, jansu-proxy/src/produce.rs:1183-1184, jansu-sans-io/src/record/codec.rs:578-579, jansu-sans-io/src/record/codec.rs:587-588, jansu-sans-io/src/record/codec.rs:596-597, jansu-sans-io/src/record/codec.rs:605-606, jansu-sans-io/src/record/codec.rs:614-615, jansu-schema/src/lib.rs:700-701, jansu-schema/src/lib.rs:732-733, jansu-schema/src/lib.rs:749-750, jansu-schema/src/lib.rs:771-772, jansu-schema/src/lib.rs:787-788, jansu-schema/src/proto.rs:899-900, jansu-schema/src/proto.rs:942-943, jansu-schema/src/proto.rs:992-993, jansu-schema/src/proto.rs:1042-1043, jansu-schema/src/proto.rs:1064-1065, jansu-schema/src/proto.rs:1093-1094, jansu-schema/src/proto.rs:1132-1133, jansu-schema/src/proto.rs:1169-1170, jansu-schema/src/proto.rs:1216-1217, jansu-schema/src/proto.rs:1254-1255, jansu-schema/src/proto.rs:1274-1275, jansu-schema/src/proto.rs:1299-1300, jansu-schema/src/proto.rs:1325-1326, jansu-schema/src/proto.rs:1352-1353, jansu-storage/src/dynostore/metadata/tests.rs:157-158, jansu-storage/src/dynostore/metadata/tests.rs:198-199, jansu-storage/src/dynostore/metadata/tests.rs:246-247, jansu-storage/src/dynostore/metadata/tests.rs:290-291, jansu-storage/src/gcs/limit.rs:236-237` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 2 | `jansu-sans-io/src/lib.rs:777-778, jansu-sans-io/src/lib.rs:913-914, jansu-sans-io/src/lib.rs:1539-1540, jansu-sans-io/src/lib.rs:1555-1556, jansu-sans-io/src/lib.rs:1578-1579, jansu-sans-io/src/lib.rs:1590-1591, jansu-sans-io/src/lib.rs:1623-1624, jansu-sans-io/src/lib.rs:1658-1659, jansu-sans-io/src/lib.rs:1917-1918, jansu-sans-io/src/lib.rs:1927-1928, jansu-sans-io/src/lib.rs:1946-1947, jansu-sans-io/src/lib.rs:1968-1969, jansu-sans-io/src/lib.rs:1981-1982, jansu-sans-io/src/lib.rs:1991-1992, jansu-sans-io/src/lib.rs:2004-2005, jansu-sans-io/src/lib.rs:2033-2034, jansu-sans-io/src/lib.rs:2050-2051, jansu-sans-io/src/lib.rs:2081-2082, jansu-sans-io/src/lib.rs:2097-2098, jansu-sans-io/src/lib.rs:2123-2124, jansu-sans-io/src/lib.rs:2135-2136, jansu-sans-io/src/lib.rs:2180-2181, jansu-sans-io/src/lib.rs:2192-2193, jansu-sans-io/src/lib.rs:2222-2223, jansu-sans-io/src/lib.rs:2232-2233, jansu-sans-io/src/lib.rs:2241-2242` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 23 | 58 | `jansu-storage/src/dynostore/opticon/tests.rs:29-52, jansu-storage/src/sql.rs:517-540` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 23 | 58 | `jansu-storage/src/limbo/tests.rs:21-44, jansu-storage/src/lite/tests.rs:21-44` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 22 | 49 | `jansu-client/src/lib.rs:836-858, jansu-proxy/src/lib.rs:368-390` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 7 | 15 | `jansu-sans-io/src/de.rs:804-811, jansu-sans-io/src/de.rs:823-830, jansu-sans-io/src/primitive/tagged/de.rs:319-326, jansu-sans-io/src/primitive/tagged/de.rs:333-340` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 2 | 5 | `jansu-storage/src/service.rs:1598-1600, jansu-storage/src/service.rs:1616-1618, jansu-storage/src/service.rs:1620-1622, jansu-storage/src/service.rs:1645-1647, jansu-storage/src/service.rs:1699-1701, jansu-storage/src/service.rs:1737-1739, jansu-storage/src/service.rs:1818-1820, jansu-storage/src/service.rs:1822-1824, jansu-storage/src/service.rs:1826-1828, jansu-storage/src/service.rs:1830-1832` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 3 | 4 | `jansu-sans-io/src/ser.rs:1098-1101, jansu-sans-io/src/ser.rs:1225-1228, jansu-sans-io/src/ser.rs:1242-1245, jansu-sans-io/src/ser.rs:1259-1262, jansu-sans-io/src/ser.rs:1276-1279, jansu-sans-io/src/ser.rs:1283-1286, jansu-sans-io/src/ser.rs:1322-1325` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 1 | `jansu-sans-io/src/de.rs:106-107, jansu-sans-io/src/de.rs:117-118, jansu-sans-io/src/de.rs:134-135, jansu-sans-io/src/de.rs:151-152, jansu-sans-io/src/de.rs:1404-1405, jansu-sans-io/src/lib.rs:1741-1742, jansu-sans-io/src/lib.rs:1752-1753, jansu-sans-io/src/lib.rs:1763-1764, jansu-sans-io/src/lib.rs:1838-1839, jansu-sans-io/src/lib.rs:1845-1846, jansu-sans-io/src/ser.rs:93-94, jansu-sans-io/src/ser.rs:132-133, jansu-sans-io/src/ser.rs:144-145, jansu-sans-io/src/ser.rs:168-169, jansu-schema/src/lib.rs:284-285, jansu-schema/src/lib.rs:429-430, jansu-schema/src/lib.rs:439-440, jansu-schema/src/lib.rs:446-447, jansu-service/src/frame.rs:543-544` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 2 | 3 | `jansu-cat/src/lib.rs:49-51, jansu-cli/src/lib.rs:67-69, jansu-client/src/lib.rs:147-149, jansu-generator/src/lib.rs:102-104, jansu-perf/src/lib.rs:104-106, jansu-proxy/src/lib.rs:80-82, jansu-sans-io/build.rs:47-49, jansu-service/src/lib.rs:289-291, jansu-topic/src/lib.rs:53-55` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 3 | 5 | `jansu-broker/src/lib.rs:255-258, jansu-cat/src/lib.rs:43-46, jansu-generator/src/lib.rs:96-99, jansu-model/src/error.rs:58-61, jansu-perf/src/lib.rs:98-101, jansu-topic/src/lib.rs:41-44` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 3 | 4 | `jansu-sans-io/src/de.rs:1059-1062, jansu-sans-io/src/de.rs:1372-1375, jansu-sans-io/src/de.rs:1417-1420, jansu-sans-io/src/de.rs:1477-1480, jansu-sans-io/src/primitive/tagged/de.rs:445-448, jansu-sans-io/src/primitive/tagged/de.rs:482-485` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 3 | 2 | `jansu-proxy/src/produce.rs:509-512, jansu-proxy/src/produce.rs:535-538, jansu-sans-io/src/primitive/varint.rs:44-47, jansu-storage/src/limbo/timestamp.rs:23-26, jansu-storage/src/lite/lite_timestamp.rs:23-26, jansu-storage/src/sql.rs:35-38` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 12 | 21 | `jansu-cat/src/consume.rs:182-194, jansu-cat/src/produce.rs:137-149` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 3 | 4 | `jansu-sans-io/src/de.rs:1059-1062, jansu-sans-io/src/de.rs:1372-1375, jansu-sans-io/src/de.rs:1417-1420, jansu-sans-io/src/primitive/tagged/de.rs:445-448, jansu-sans-io/src/primitive/tagged/de.rs:482-485` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 5 | `jansu-sans-io/src/primitive/tagged/ser.rs:348-352, jansu-sans-io/src/primitive/tagged/ser.rs:367-371, jansu-sans-io/src/ser.rs:820-824, jansu-sans-io/src/ser.rs:839-843` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 5 | `jansu-sans-io/src/primitive/tagged/ser.rs:310-314, jansu-sans-io/src/primitive/tagged/ser.rs:329-333, jansu-sans-io/src/ser.rs:782-786, jansu-sans-io/src/ser.rs:801-805` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 5 | `jansu-sans-io/src/primitive/tagged/ser.rs:413-417, jansu-sans-io/src/primitive/tagged/ser.rs:432-436, jansu-sans-io/src/ser.rs:885-889, jansu-sans-io/src/ser.rs:927-931` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 1 | `jansu-perf/src/lib.rs:756-757, jansu-perf/src/lib.rs:761-762, jansu-sans-io/src/primitive/tagged/ser.rs:318-319, jansu-sans-io/src/primitive/tagged/ser.rs:337-338, jansu-sans-io/src/primitive/tagged/ser.rs:422-423, jansu-sans-io/src/primitive/tagged/ser.rs:441-442, jansu-sans-io/src/ser.rs:790-791, jansu-sans-io/src/ser.rs:809-810, jansu-sans-io/src/ser.rs:1093-1094, jansu-sans-io/src/ser.rs:1232-1233, jansu-sans-io/src/ser.rs:1312-1313, jansu-sans-io/src/ser.rs:1351-1352` | `same body appears under different names across files` |
| `TokenBlock` | `Warning` | `rust` | 10 | 280 | `fuzz/fuzz_targets/generate_seeds.rs:480-489, jansu-sans-io/src/bin/bench.rs:176-185` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 275 | `fuzz/fuzz_targets/generate_seeds.rs:485-494, jansu-sans-io/src/bin/bench.rs:181-190` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 271 | `fuzz/fuzz_targets/generate_seeds.rs:486-495, jansu-sans-io/src/bin/bench.rs:182-191` | `strict token/block duplication exceeded the configured threshold` |
| `TokenBlock` | `Warning` | `rust` | 10 | 262 | `fuzz/fuzz_targets/generate_seeds.rs:487-496, jansu-sans-io/src/bin/bench.rs:183-192` | `strict token/block duplication exceeded the configured threshold` |
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

## Dimensions

| Dimension | Weight | Score | Weighted | Evidence |
| --- | ---: | ---: | ---: | --- |
| Ownership and navigation surface | 13 | 83 | 10.79 | root `AGENTS.md` present; owner map present |
| Contract and boundary integrity | 13 | 88 | 11.44 | contract surface found; generated contract artifacts found |
| Proof lanes and test routing | 12 | 100 | 12.00 | one-command setup/validation lane found; deterministic fast lane found |
| Security and supply-chain posture | 12 | 66 | 7.92 | secret or dependency scan tooling found; provenance/SBOM tooling found |
| Code shape and semantic surface | 12 | 17 | 2.04 | largest authored code file: jansu-schema/src/lake/delta.rs (2558 LOC); code file exceeds 500 LOC |
| Data truth and workflow safety | 8 | 95 | 7.60 | database surface present; structured db boundary manifest present |
| Observability and repair evidence | 8 | 90 | 7.20 | observability libraries or patterns found; diagnostic shaping hints found |
| Context economy and agent instructions | 7 | 100 | 7.00 | root `AGENTS.md` present; root `AGENTS.md` stays short |
| Jankurai tool adoption and CI replacement | 7 | 29 | 2.03 | control-plane files present; applicable=16 |
| Python containment and polyglot hygiene | 4 | 100 | 4.00 | no Python files in scope |
| Build speed signals | 4 | 70 | 2.80 | build acceleration markers found; targeted test/build commands found |

## Reference Profile Structure

- Applicable cells: `3` canonical=`3` noncanonical=`0` guidance missing=`0`

| Cell | Status | Canonical | Detected | Aliases | Guidance | Owner | Proof lane | Agent fix |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `web` | `not_applicable` | `apps/web/` | `-` | `frontend/, ui/, packages/web/, packages/ui/` | `not_required` | `apps/web` | `rendered UX / Playwright` | `no action` |
| `api` | `not_applicable` | `apps/api/` | `-` | `api/, server/, backend/` | `not_required` | `apps/api` | `edge handler / contract tests` | `no action` |
| `domain` | `not_applicable` | `crates/domain/` | `-` | `domain/, core/` | `not_required` | `crates/domain` | `unit / property tests` | `no action` |
| `application` | `not_applicable` | `crates/application/` | `-` | `application/, usecases/, use-cases/` | `not_required` | `crates/application` | `use-case / authz tests` | `no action` |
| `adapters` | `not_applicable` | `crates/adapters/` | `-` | `adapters/, infra/, integrations/` | `not_required` | `crates/adapters` | `adapter integration tests` | `no action` |
| `workers` | `not_applicable` | `crates/workers/` | `-` | `workers/, jobs/, scheduler/, queue/` | `not_required` | `crates/workers` | `workflow / replay tests` | `no action` |
| `contracts` | `canonical` | `contracts/` | `contracts` | `openapi/, protobuf/, json-schema/, generated/` | `present` | `contracts` | `generation / drift checks` | `keep `contracts/AGENTS.md` aligned with owns / forbidden / proof lane guidance` |
| `db` | `canonical` | `db/` | `db` | `migrations/, constraints/, sql/` | `present` | `db` | `migration / constraint tests` | `keep `db/AGENTS.md` aligned with owns / forbidden / proof lane guidance` |
| `python-ai` | `not_applicable` | `python/ai-service/` | `-` | `python/, ai-service/, evals/, embeddings/, model/` | `not_required` | `python/ai-service` | `eval / contract tests` | `no action` |
| `ops` | `canonical` | `ops/` | `.github, .github/workflows, ops` | `.github/, .github/workflows/, ci/, release/, observability/, security/` | `present` | `ops` | `security lane / workflow lint` | `keep `ops/AGENTS.md` aligned with owns / forbidden / proof lane guidance` |

## Rendered UX QA

- Web surface: `false`
- Layered UX lane: `true`
- Missing: `none`

## Tool Adoption

- Control plane present: `true`
- Applicable tools: `16`
- Configured: `15`
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
| `copy-code` | `audit` | `auto` | `missing` | `ad hoc copy-code review, manual duplication triage` | `target/jankurai/copy-code.json, target/jankurai/copy-code.md` |
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
- Queue path counts — adapter: `2`, event_contract: `1`, generated_type: `1`, client_marker: `7`, streaming_exception: `1`
- Content fingerprint: `sha256:8cd3bd74f97a072251bc6a8bcfb50730e563a470a7b069f8ff735590f7c28104`

## Boundary Reclassifications

No audited runtime boundary reclassifications declared.

## Findings

1. `medium` `shape` `.`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:shape` `soft` confidence `0.76`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: `Code shape and semantic surface` scored 17 below the standard floor of 85
   Fix: split large or ambiguous authored code into smaller semantic modules with focused tests
   Rerun: `just fast`
   Fingerprint: `sha256:26fe38e251f1f913bee2b9f35aac5268332097ab3418abe2021199fcf0a09bcf`
   Evidence: largest authored code file: jansu-schema/src/lake/delta.rs (2558 LOC), code file exceeds 500 LOC, code file exceeds 1000 LOC, copy-code advisory classes found: 233 (advisory only, no score impact)
2. `medium` `security` `.github/workflows/jankurai.yml`
   Rule: `HLT-016-SUPPLY-CHAIN-DRIFT`
   Check: `HLT-016-SUPPLY-CHAIN-DRIFT:security` `soft` confidence `0.76`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/audit-rubric.md#top-level-risk-mapping`
   Reason: `Security and supply-chain posture` scored 66 below the standard floor of 85
   Fix: wire secret, dependency, provenance, and workflow scans into an operational CI lane
   Rerun: `just security`
   Fingerprint: `sha256:eb6acb55678ae606bd1e3575fe3aa79da832aa766b88714c480c15225d48b3aa`
   Evidence: secret or dependency scan tooling found, provenance/SBOM tooling found, security lane present, canonical security lane wrapper present
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
4. `medium` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `soft` confidence `0.76`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: `Ownership and navigation surface` scored 83 below the standard floor of 85
   Fix: tighten owner/test maps and root routing until agents can localize ownership without inference
   Rerun: `just fast`
   Fingerprint: `sha256:f22331131a2d75b4ff814289d01473fb3ab281f4b3c8ac366b63eb97cfe85438`
   Evidence: root `AGENTS.md` present, owner map present, test/proof routing map present, local `AGENTS.md` file(s)
5. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `etc/differential/compose.kafka-4.2.yaml` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:f3efff127876e61853fa16570c26d6e15413ba98e57ba06b48a08cabecf69774`
   Evidence: etc/differential/compose.kafka-4.2.yaml
6. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `etc/grafana/dashboards/home.json` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:5c80ebf89be157834d4e554a66c722f82f2218727bbb4a3ab7034f2f4802710a`
   Evidence: etc/grafana/dashboards/home.json
7. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `etc/grafana/provisioning/dashboards/jansu.yaml` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:5cfdcce905ca5b70829d8df8c18735e9e480661c82af816008fcca356256ff46`
   Evidence: etc/grafana/provisioning/dashboards/jansu.yaml
8. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `etc/grafana/provisioning/datasources/prometheus.yaml` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:67716f165fab38b7cdc015726f62be7b64b61d22b11cb37eb7c4bd1c2750ac72`
   Evidence: etc/grafana/provisioning/datasources/prometheus.yaml
9. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `etc/initdb.d/010-schema.sql` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:4d066204f99d053f4b5927a92d6805758f4e9bfe57c7cda57199ecc35892e531`
   Evidence: etc/initdb.d/010-schema.sql
10. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `etc/initdb.d/011-offset-retention-patch.sql` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:344bdef78c8f5daa5331bb2da2caf5716ee7626c098817ee6c56ad960650abca`
   Evidence: etc/initdb.d/011-offset-retention-patch.sql
11. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `etc/lakekeeper/create-default-warehouse.json` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:d40956054164a4fc0c7a412ab5dce98536ef8d6d0db585f2230fe56e8f10575d`
   Evidence: etc/lakekeeper/create-default-warehouse.json
12. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `etc/prometheus.yaml` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:732dc1cd530510aa3296ae84c1bdc2e23f37b220a5e91f20b9154e0c50ec6ef2`
   Evidence: etc/prometheus.yaml
13. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `etc/schema/customer.proto` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:f7f1d67df2dfd5fa89ab0e8bd4069aecd212fd2cf16c5bf84c3e4042c0488e68`
   Evidence: etc/schema/customer.proto
14. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `etc/schema/employee.proto` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:b33478873c1fa03cf9739b9ddef0bd2dd48e108d7117cc4c74c4fb1d13feb9bd`
   Evidence: etc/schema/employee.proto
15. `medium` `proof` `agent/repo-score.md:371`
   Rule: `HLT-027-HUMAN-REVIEW-EVIDENCE-GAP`
   Check: `HLT-027-HUMAN-REVIEW-EVIDENCE-GAP:proof` `soft` confidence `0.88`
   Route: TLR `Repair`, lane `audit`, owner `agent`
   Docs: `docs/testing.md`
   Matched term: `review evidence`
   Reason: proof and review claims need receipts
   Fix: attach raw CI logs, review receipts, and replayable commands instead of accepting claims or summaries
   Rerun: `just score`
   Fingerprint: `sha256:9242a8c12235f5da083d0f18902ee310492f9ebebefd60f5618aa31e7c9d3f71`
   Evidence: Evidence: Evidence: "\"\\\"Evidence: \\\\\\\"about\\\\\\\": \\\\\\\"The principal filter, or null to accept all principals.\\\\\\\" },\\\"\""
16. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `etc/differential/compose.kafka-4.2.yaml` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:b8209ca393d46ce6d9771f5d7c38c54ba0519af4635cedaecfb31e8374844349`
   Evidence: etc/differential/compose.kafka-4.2.yaml
17. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `etc/grafana/dashboards/home.json` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:8887a8f715e629bc33e9c1564b0d9f90d8d0d50bdc34b6bdc958b3b50c4e7cf7`
   Evidence: etc/grafana/dashboards/home.json
18. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `etc/grafana/provisioning/dashboards/jansu.yaml` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:357cd0ad82d8809e3720da1cb299df6af99357068ae3d5cbe9d421e447243f29`
   Evidence: etc/grafana/provisioning/dashboards/jansu.yaml
19. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `etc/grafana/provisioning/datasources/prometheus.yaml` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:5548e326e4148f2bfb9bcfd2660c0a096d9790cde1df51720d8fbaa8aae42b0e`
   Evidence: etc/grafana/provisioning/datasources/prometheus.yaml
20. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `etc/initdb.d/010-schema.sql` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:973e231b073279916fede7a1e6601a35257db7d74d86e44e93e7287d967c3714`
   Evidence: etc/initdb.d/010-schema.sql
21. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `etc/initdb.d/011-offset-retention-patch.sql` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:c065ff42d17446576c0b7f2dd02678d453d527ffe4d3ad3539d31ad5e5fd8511`
   Evidence: etc/initdb.d/011-offset-retention-patch.sql
22. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `etc/lakekeeper/create-default-warehouse.json` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:6707cf7b9d23dc402b2f52a06552c9f2278fb74ba145a3302856421b5a725486`
   Evidence: etc/lakekeeper/create-default-warehouse.json
23. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `etc/prometheus.yaml` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:2c75d4bca5563e597b39574ec3aa7f73e8ede804ce314fa7e8a64503cbd03a9c`
   Evidence: etc/prometheus.yaml
24. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `etc/schema/customer.proto` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:c3c3821f0e173c12c0ab0b10ce97ad96efb3f4eadd14e0ab72d3c3d76ccaaa72`
   Evidence: etc/schema/customer.proto
25. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `etc/schema/employee.proto` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:40a9cea6ebe9bf4239d2f3f6adb34e0ae6932d4a99d917ecb60eb958dad2a240`
   Evidence: etc/schema/employee.proto
26. `medium` `release` `docs/testing.md`
   Rule: `HLT-026-COST-BUDGET-GAP`
   Check: `HLT-026-COST-BUDGET-GAP:release` `soft` confidence `0.88`
   Route: TLR `Verification`, lane `release`, owner `standard`
   Docs: `docs/testing.md`
   Matched term: `budget`
   Reason: unbounded paid work needs budgets and stop conditions
   Fix: add explicit budgets, quotas, stop conditions, and kill-switch evidence for paid or unbounded operations
   Rerun: `just check`
   Fingerprint: `sha256:edd248b7afc24b644107205fa5b84a88103ac4b622009ff9f19b779de8798f59`
   Evidence: cost surface found without budget/stop-condition policy
27. `high` `release` `docs/testing.md`
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
28. `high` `vibe` `jansu-model/src/lib.rs:788`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: fallback soup detected in product code
   Fix: collapse fallback chains into explicit typed states with bounded retry policy, telemetry, and documented repair guidance
   Rerun: `just fast`
   Fingerprint: `sha256:6bddc7407b75163f4c77159ec6deae23de88ad61daeeba25aaa0e64742dee8cb`
   Evidence: jansu-model/src/lib.rs:788 syn::parse_str::<Type>(&self.name).unwrap_or_else(|_| panic!("not a type: {self:?}"))

## Policy

- Policy file: `./agent/audit-policy.toml`
- Minimum score: `85`
- Fail on: `critical, high`

## Agent Fix Queue

1. `high` `HLT-004-UNMAPPED-PROOF` `agent/test-map.json` - add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Route: `Verification`/`fast`
2. `high` `HLT-025-RELEASE-READINESS-GAP` `docs/testing.md` - add launch-gate evidence for security, backups, monitoring, rollback, and abuse controls
   Route: `Verification`/`release`
3. `medium` `HLT-018-PERF-CONCURRENCY-DRIFT` `Justfile` - add fast deterministic build/test targets, caches, and narrow proof lanes for agent iteration
   Route: `Verification`/`fast`
4. `medium` `HLT-026-COST-BUDGET-GAP` `docs/testing.md` - add explicit budgets, quotas, stop conditions, and kill-switch evidence for paid or unbounded operations
   Route: `Verification`/`release`
5. `medium` `HLT-027-HUMAN-REVIEW-EVIDENCE-GAP` `agent/repo-score.md` - attach raw CI logs, review receipts, and replayable commands instead of accepting claims or summaries
   Route: `Repair`/`audit`
6. `high` `HLT-003-OWNERLESS-PATH` `agent/owner-map.json` - add the narrowest stable prefix for this path to `agent/owner-map.json`
   Route: `Context/setup`/`fast`
7. `medium` `HLT-003-OWNERLESS-PATH` `agent/owner-map.json` - tighten owner/test maps and root routing until agents can localize ownership without inference
   Route: `Context/setup`/`fast`
8. `high` `HLT-001-DEAD-MARKER` `jansu-model/src/lib.rs` - collapse fallback chains into explicit typed states with bounded retry policy, telemetry, and documented repair guidance
   Route: `Entropy`/`fast`
9. `medium` `HLT-001-DEAD-MARKER` `.` - split large or ambiguous authored code into smaller semantic modules with focused tests
   Route: `Entropy`/`fast`
10. `medium` `HLT-016-SUPPLY-CHAIN-DRIFT` `.github/workflows/jankurai.yml` - wire secret, dependency, provenance, and workflow scans into an operational CI lane
   Route: `Security, secrets, agency`/`security`
