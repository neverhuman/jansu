# jankurai Repo Score

- Standard: `jankurai`
- Auditor: `0.7.0`
- Schema: `1.5.0`
- Paper edition: `2026.05-ed7`
- Target stack ID: `rust-ts-vite-react-postgres-bounded-python`
- Target stack: `Rust core + TypeScript/React/Vite + PostgreSQL + generated contracts + exception-only Python AI/data service`
- Repo: `.`
- Run ID: `1778939775`
- Started at: `1778939775`
- Elapsed: `10246` ms
- Scope: `full`
- Raw score: `73`
- Final score: `60`
- Decision: `advisory`
- Minimum score: `85`
- Caps applied: `vibe-placeholders-in-product-code, fallback-soup-in-product-code, future-hostile-dead-language-in-product-code, severe-duplication-in-product-code, secret-like-content-detected, authz-or-data-isolation-gap, input-boundary-gap, agent-tool-supply-chain-gap, release-readiness-gap, rust-bad-behavior, sql-bad-behavior, docker-bad-behavior, ci-bad-behavior, release-bad-behavior, ci-local-parity`

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
| `vibe-placeholders-in-product-code` | 68 | yes |
| `fallback-soup-in-product-code` | 70 | yes |
| `future-hostile-dead-language-in-product-code` | 64 | yes |
| `severe-duplication-in-product-code` | 70 | yes |
| `generated-zone-mutation-risk` | 76 | no |
| `direct-db-access-from-wrong-layer` | 66 | no |
| `missing-web-e2e-lane` | 82 | no |
| `missing-rendered-ux-qa-lane` | 84 | no |
| `prompt-injection-risk` | 78 | no |
| `overbroad-agent-agency` | 65 | no |
| `secret-like-content-detected` | 60 | yes |
| `false-green-test-risk` | 76 | no |
| `destructive-migration-risk` | 70 | no |
| `authz-or-data-isolation-gap` | 78 | yes |
| `input-boundary-gap` | 78 | yes |
| `agent-tool-supply-chain-gap` | 78 | yes |
| `release-readiness-gap` | 80 | yes |
| `missing-rust-property-or-integration-tests` | 82 | no |
| `no-agent-friendly-exception-pattern` | 76 | no |
| `missing-agent-readable-docs` | 80 | no |
| `streaming-runtime-drift` | 78 | no |
| `rust-bad-behavior` | 72 | yes |
| `sql-bad-behavior` | 72 | yes |
| `typescript-bad-behavior` | 72 | no |
| `docker-bad-behavior` | 72 | yes |
| `python-bad-behavior` | 72 | no |
| `ci-bad-behavior` | 70 | yes |
| `git-bad-behavior` | 70 | no |
| `gittools-bad-behavior` | 70 | no |
| `release-bad-behavior` | 70 | yes |
| `web-security-bad-behavior` | 68 | no |
| `repo-rot-bad-behavior` | 88 | no |
| `comment-hygiene-dangerous-residue` | 72 | no |
| `ci-local-parity` | 70 | yes |

## Copy-Code Redundancy

- Status: `review` hard=`2` warning=`269` files=`219`
- Policy: min-lines=`10` min-tokens=`100` max-findings=`50` include-tests=`false` strict=`false`
- Duplicate volume: lines=`1278` tokens=`11824` bytes=`59799`

- Notes:
  - hard classes are limited to exact active-source file matches and substantial exact same-name units
  - warning classes include same-body different-name units and token/block duplication
  - tests, fixtures, stories, config, Docker, and migrations are omitted unless --include-tests is set
  - showing the top 50 classes and omitting 221 lower-ranked classes

| Kind | Severity | Language | Lines | Tokens | Instances | Reason |
| --- | --- | --- | ---: | ---: | --- | --- |
| `ExactUnitSameName` | `Hard` | `rust` | 55 | 111 | `jansu-storage/src/limbo/tests.rs:197-252, jansu-storage/src/lite/tests.rs:197-252` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Hard` | `rust` | 48 | 113 | `jansu-storage/src/limbo/tests.rs:255-303, jansu-storage/src/lite/tests.rs:255-303` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 3 | 4 | `jansu-sans-io/src/de.rs:352-355, jansu-sans-io/src/de.rs:359-362, jansu-sans-io/src/de.rs:375-378, jansu-sans-io/src/de.rs:391-394, jansu-sans-io/src/de.rs:437-440, jansu-sans-io/src/de.rs:453-456, jansu-sans-io/src/de.rs:469-472, jansu-sans-io/src/de.rs:485-488, jansu-sans-io/src/de.rs:501-504, jansu-sans-io/src/de.rs:517-520, jansu-sans-io/src/de.rs:533-536, jansu-sans-io/src/de.rs:549-552, jansu-sans-io/src/de.rs:565-568, jansu-sans-io/src/de.rs:575-578, jansu-sans-io/src/de.rs:593-596, jansu-sans-io/src/de.rs:627-630, jansu-sans-io/src/de.rs:650-653, jansu-sans-io/src/de.rs:678-681, jansu-sans-io/src/de.rs:793-796, jansu-sans-io/src/de.rs:839-842, jansu-sans-io/src/de.rs:886-889, jansu-sans-io/src/de.rs:914-917, jansu-sans-io/src/de.rs:1001-1004, jansu-sans-io/src/de.rs:1038-1041, jansu-sans-io/src/de.rs:1108-1111, jansu-sans-io/src/de.rs:1115-1118, jansu-sans-io/src/de.rs:1122-1125, jansu-sans-io/src/de.rs:1129-1132, jansu-sans-io/src/de.rs:1136-1139, jansu-sans-io/src/de.rs:1143-1146, jansu-sans-io/src/de.rs:1150-1153, jansu-sans-io/src/de.rs:1157-1160, jansu-sans-io/src/de.rs:1164-1167, jansu-sans-io/src/de.rs:1171-1174, jansu-sans-io/src/de.rs:1178-1181, jansu-sans-io/src/de.rs:1185-1188, jansu-sans-io/src/de.rs:1192-1195, jansu-sans-io/src/de.rs:1199-1202, jansu-sans-io/src/de.rs:1206-1209, jansu-sans-io/src/de.rs:1213-1216, jansu-sans-io/src/de.rs:1220-1223, jansu-sans-io/src/de.rs:1227-1230, jansu-sans-io/src/de.rs:1234-1237, jansu-sans-io/src/de.rs:1269-1272, jansu-sans-io/src/de.rs:1305-1308, jansu-sans-io/src/de.rs:1342-1345, jansu-sans-io/src/de.rs:1349-1352, jansu-sans-io/src/de.rs:1485-1488, jansu-sans-io/src/primitive/tagged/de.rs:85-88, jansu-sans-io/src/primitive/tagged/de.rs:92-95, jansu-sans-io/src/primitive/tagged/de.rs:104-107, jansu-sans-io/src/primitive/tagged/de.rs:116-119, jansu-sans-io/src/primitive/tagged/de.rs:128-131, jansu-sans-io/src/primitive/tagged/de.rs:140-143, jansu-sans-io/src/primitive/tagged/de.rs:152-155, jansu-sans-io/src/primitive/tagged/de.rs:164-167, jansu-sans-io/src/primitive/tagged/de.rs:176-179, jansu-sans-io/src/primitive/tagged/de.rs:188-191, jansu-sans-io/src/primitive/tagged/de.rs:200-203, jansu-sans-io/src/primitive/tagged/de.rs:212-215, jansu-sans-io/src/primitive/tagged/de.rs:224-227, jansu-sans-io/src/primitive/tagged/de.rs:231-234, jansu-sans-io/src/primitive/tagged/de.rs:255-258, jansu-sans-io/src/primitive/tagged/de.rs:284-287, jansu-sans-io/src/primitive/tagged/de.rs:291-294, jansu-sans-io/src/primitive/tagged/de.rs:298-301, jansu-sans-io/src/primitive/tagged/de.rs:312-315, jansu-sans-io/src/primitive/tagged/de.rs:345-348, jansu-sans-io/src/primitive/tagged/de.rs:355-358, jansu-sans-io/src/primitive/tagged/de.rs:380-383, jansu-sans-io/src/primitive/tagged/de.rs:415-418, jansu-sans-io/src/primitive/tagged/de.rs:422-425` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 4 | 5 | `jansu-sans-io/src/primitive/tagged/ser.rs:181-185, jansu-sans-io/src/primitive/tagged/ser.rs:310-314, jansu-sans-io/src/primitive/tagged/ser.rs:329-333, jansu-sans-io/src/primitive/tagged/ser.rs:348-352, jansu-sans-io/src/primitive/tagged/ser.rs:367-371, jansu-sans-io/src/primitive/tagged/ser.rs:386-390, jansu-sans-io/src/primitive/tagged/ser.rs:395-399, jansu-sans-io/src/primitive/tagged/ser.rs:413-417, jansu-sans-io/src/primitive/tagged/ser.rs:432-436, jansu-sans-io/src/ser.rs:574-578, jansu-sans-io/src/ser.rs:782-786, jansu-sans-io/src/ser.rs:801-805, jansu-sans-io/src/ser.rs:820-824, jansu-sans-io/src/ser.rs:839-843, jansu-sans-io/src/ser.rs:858-862, jansu-sans-io/src/ser.rs:867-871, jansu-sans-io/src/ser.rs:885-889, jansu-sans-io/src/ser.rs:927-931` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 22 | 49 | `jansu-schema/src/avro.rs:702-724, jansu-schema/src/lake/delta.rs:793-815, jansu-schema/src/proto/arrow.rs:1312-1334` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 3 | `jansu-proxy/src/produce.rs:857-858, jansu-proxy/src/produce.rs:978-979, jansu-proxy/src/produce.rs:1183-1184, jansu-sans-io/src/record/codec.rs:578-579, jansu-sans-io/src/record/codec.rs:587-588, jansu-sans-io/src/record/codec.rs:596-597, jansu-sans-io/src/record/codec.rs:605-606, jansu-sans-io/src/record/codec.rs:614-615, jansu-schema/src/lib.rs:700-701, jansu-schema/src/lib.rs:732-733, jansu-schema/src/lib.rs:749-750, jansu-schema/src/lib.rs:771-772, jansu-schema/src/lib.rs:787-788, jansu-schema/src/proto.rs:899-900, jansu-schema/src/proto.rs:942-943, jansu-schema/src/proto.rs:992-993, jansu-schema/src/proto.rs:1042-1043, jansu-schema/src/proto.rs:1064-1065, jansu-schema/src/proto.rs:1093-1094, jansu-schema/src/proto.rs:1132-1133, jansu-schema/src/proto.rs:1169-1170, jansu-schema/src/proto.rs:1216-1217, jansu-schema/src/proto.rs:1254-1255, jansu-schema/src/proto.rs:1274-1275, jansu-schema/src/proto.rs:1299-1300, jansu-schema/src/proto.rs:1325-1326, jansu-schema/src/proto.rs:1352-1353, jansu-storage/src/dynostore/metadata/tests.rs:157-158, jansu-storage/src/dynostore/metadata/tests.rs:198-199, jansu-storage/src/dynostore/metadata/tests.rs:246-247, jansu-storage/src/dynostore/metadata/tests.rs:290-291, jansu-storage/src/gcs/limit.rs:236-237` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 2 | `jansu-sans-io/src/lib.rs:777-778, jansu-sans-io/src/lib.rs:913-914, jansu-sans-io/src/lib.rs:1539-1540, jansu-sans-io/src/lib.rs:1555-1556, jansu-sans-io/src/lib.rs:1578-1579, jansu-sans-io/src/lib.rs:1590-1591, jansu-sans-io/src/lib.rs:1623-1624, jansu-sans-io/src/lib.rs:1658-1659, jansu-sans-io/src/lib.rs:1917-1918, jansu-sans-io/src/lib.rs:1927-1928, jansu-sans-io/src/lib.rs:1946-1947, jansu-sans-io/src/lib.rs:1968-1969, jansu-sans-io/src/lib.rs:1981-1982, jansu-sans-io/src/lib.rs:1991-1992, jansu-sans-io/src/lib.rs:2004-2005, jansu-sans-io/src/lib.rs:2033-2034, jansu-sans-io/src/lib.rs:2050-2051, jansu-sans-io/src/lib.rs:2081-2082, jansu-sans-io/src/lib.rs:2097-2098, jansu-sans-io/src/lib.rs:2123-2124, jansu-sans-io/src/lib.rs:2135-2136, jansu-sans-io/src/lib.rs:2180-2181, jansu-sans-io/src/lib.rs:2192-2193, jansu-sans-io/src/lib.rs:2222-2223, jansu-sans-io/src/lib.rs:2232-2233, jansu-sans-io/src/lib.rs:2241-2242` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 23 | 58 | `jansu-storage/src/dynostore/opticon/tests.rs:29-52, jansu-storage/src/sql.rs:517-540` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 23 | 58 | `jansu-storage/src/limbo/tests.rs:21-44, jansu-storage/src/lite/tests.rs:21-44` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 22 | 49 | `jansu-client/src/lib.rs:836-858, jansu-proxy/src/lib.rs:368-390` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 7 | 16 | `jansu-storage/src/lib.rs:1010-1017, jansu-storage/src/lib.rs:1293-1300, jansu-storage/src/lib.rs:2361-2368, jansu-storage/src/service.rs:624-631` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 7 | 15 | `jansu-sans-io/src/de.rs:804-811, jansu-sans-io/src/de.rs:823-830, jansu-sans-io/src/primitive/tagged/de.rs:319-326, jansu-sans-io/src/primitive/tagged/de.rs:333-340` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 1 | 1 | `jansu-sans-io/src/de.rs:106-107, jansu-sans-io/src/de.rs:117-118, jansu-sans-io/src/de.rs:134-135, jansu-sans-io/src/de.rs:151-152, jansu-sans-io/src/de.rs:1404-1405, jansu-sans-io/src/lib.rs:1741-1742, jansu-sans-io/src/lib.rs:1752-1753, jansu-sans-io/src/lib.rs:1763-1764, jansu-sans-io/src/lib.rs:1838-1839, jansu-sans-io/src/lib.rs:1845-1846, jansu-sans-io/src/ser.rs:93-94, jansu-sans-io/src/ser.rs:132-133, jansu-sans-io/src/ser.rs:144-145, jansu-sans-io/src/ser.rs:168-169, jansu-schema/src/lib.rs:284-285, jansu-schema/src/lib.rs:429-430, jansu-schema/src/lib.rs:439-440, jansu-schema/src/lib.rs:446-447, jansu-service/src/frame.rs:543-544, jansu-storage/src/lib.rs:1734-1735` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 6 | 14 | `jansu-storage/src/lib.rs:1169-1175, jansu-storage/src/lib.rs:1452-1458, jansu-storage/src/lib.rs:2889-2895, jansu-storage/src/service.rs:979-985` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 6 | 11 | `jansu-storage/src/lib.rs:1212-1218, jansu-storage/src/lib.rs:1495-1501, jansu-storage/src/lib.rs:3073-3079, jansu-storage/src/service.rs:1074-1080` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 6 | 11 | `jansu-storage/src/lib.rs:1186-1192, jansu-storage/src/lib.rs:1469-1475, jansu-storage/src/lib.rs:2956-2962, jansu-storage/src/service.rs:1009-1015` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 3 | 4 | `jansu-sans-io/src/ser.rs:1098-1101, jansu-sans-io/src/ser.rs:1225-1228, jansu-sans-io/src/ser.rs:1242-1245, jansu-sans-io/src/ser.rs:1259-1262, jansu-sans-io/src/ser.rs:1276-1279, jansu-sans-io/src/ser.rs:1283-1286, jansu-sans-io/src/ser.rs:1322-1325` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 2 | 1 | `jansu-storage/src/service.rs:1598-1600, jansu-storage/src/service.rs:1616-1618, jansu-storage/src/service.rs:1620-1622, jansu-storage/src/service.rs:1645-1647, jansu-storage/src/service.rs:1699-1701, jansu-storage/src/service.rs:1737-1739, jansu-storage/src/service.rs:1818-1820, jansu-storage/src/service.rs:1822-1824, jansu-storage/src/service.rs:1826-1828, jansu-storage/src/service.rs:1830-1832` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 2 | 3 | `jansu-cat/src/lib.rs:49-51, jansu-cli/src/lib.rs:67-69, jansu-client/src/lib.rs:147-149, jansu-generator/src/lib.rs:102-104, jansu-perf/src/lib.rs:104-106, jansu-proxy/src/lib.rs:80-82, jansu-sans-io/build.rs:47-49, jansu-service/src/lib.rs:289-291, jansu-topic/src/lib.rs:53-55` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 5 | 13 | `jansu-storage/src/lib.rs:1035-1040, jansu-storage/src/lib.rs:1318-1323, jansu-storage/src/lib.rs:2469-2474, jansu-storage/src/service.rs:699-704` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 5 | 13 | `jansu-storage/src/lib.rs:1057-1062, jansu-storage/src/lib.rs:1340-1345, jansu-storage/src/lib.rs:2126-2131, jansu-storage/src/service.rs:764-769` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 5 | 13 | `jansu-storage/src/lib.rs:1046-1051, jansu-storage/src/lib.rs:1329-1334, jansu-storage/src/lib.rs:2536-2541, jansu-storage/src/service.rs:747-752` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 5 | 12 | `jansu-storage/src/lib.rs:1149-1154, jansu-storage/src/lib.rs:1432-1437, jansu-storage/src/lib.rs:2705-2710, jansu-storage/src/service.rs:862-867` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 5 | 12 | `jansu-storage/src/lib.rs:1160-1165, jansu-storage/src/lib.rs:1443-1448, jansu-storage/src/lib.rs:2853-2858, jansu-storage/src/service.rs:951-956` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 5 | 11 | `jansu-storage/src/lib.rs:1001-1006, jansu-storage/src/lib.rs:1284-1289, jansu-storage/src/lib.rs:2325-2330, jansu-storage/src/service.rs:595-600` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 5 | 10 | `jansu-storage/src/lib.rs:1119-1124, jansu-storage/src/lib.rs:1402-1407, jansu-storage/src/lib.rs:2669-2674, jansu-storage/src/service.rs:833-838` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 3 | 5 | `jansu-broker/src/lib.rs:255-258, jansu-cat/src/lib.rs:43-46, jansu-generator/src/lib.rs:96-99, jansu-model/src/error.rs:58-61, jansu-perf/src/lib.rs:98-101, jansu-topic/src/lib.rs:41-44` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 5 | 8 | `jansu-storage/src/lib.rs:1090-1095, jansu-storage/src/lib.rs:1373-1378, jansu-storage/src/lib.rs:3245-3250, jansu-storage/src/service.rs:1183-1188` | `same-name semantic unit copied across multiple files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 3 | 4 | `jansu-sans-io/src/de.rs:1059-1062, jansu-sans-io/src/de.rs:1372-1375, jansu-sans-io/src/de.rs:1417-1420, jansu-sans-io/src/de.rs:1477-1480, jansu-sans-io/src/primitive/tagged/de.rs:445-448, jansu-sans-io/src/primitive/tagged/de.rs:482-485` | `same body appears under different names across files` |
| `ExactUnitDifferentName` | `Warning` | `rust` | 3 | 2 | `jansu-proxy/src/produce.rs:509-512, jansu-proxy/src/produce.rs:535-538, jansu-sans-io/src/primitive/varint.rs:44-47, jansu-storage/src/limbo/timestamp.rs:23-26, jansu-storage/src/lite/lite_timestamp.rs:23-26, jansu-storage/src/sql.rs:35-38` | `same body appears under different names across files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 10 | `jansu-storage/src/lib.rs:1027-1031, jansu-storage/src/lib.rs:1310-1314, jansu-storage/src/lib.rs:2434-2438, jansu-storage/src/service.rs:673-677` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 9 | `jansu-storage/src/lib.rs:1139-1143, jansu-storage/src/lib.rs:1422-1426, jansu-storage/src/lib.rs:2814-2818, jansu-storage/src/service.rs:925-929` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 9 | `jansu-storage/src/lib.rs:1068-1072, jansu-storage/src/lib.rs:1351-1355, jansu-storage/src/lib.rs:2572-2576, jansu-storage/src/service.rs:793-797` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 8 | `jansu-storage/src/lib.rs:1111-1115, jansu-storage/src/lib.rs:1394-1398, jansu-storage/src/lib.rs:3295-3299, jansu-storage/src/service.rs:1211-1215` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 12 | 21 | `jansu-cat/src/consume.rs:182-194, jansu-cat/src/produce.rs:137-149` | `same-name semantic unit copied across multiple files` |
| `ExactUnitSameName` | `Warning` | `rust` | 4 | 6 | `jansu-storage/src/lib.rs:1101-1105, jansu-storage/src/lib.rs:1384-1388, jansu-storage/src/lib.rs:3220-3224, jansu-storage/src/service.rs:1160-1164` | `same-name semantic unit copied across multiple files` |
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

## Dimensions

| Dimension | Weight | Score | Weighted | Evidence |
| --- | ---: | ---: | ---: | --- |
| Ownership and navigation surface | 13 | 83 | 10.79 | root `AGENTS.md` present; owner map present |
| Contract and boundary integrity | 13 | 88 | 11.44 | contract surface found; generated contract artifacts found |
| Proof lanes and test routing | 12 | 100 | 12.00 | one-command setup/validation lane found; deterministic fast lane found |
| Security and supply-chain posture | 12 | 66 | 7.92 | secret or dependency scan tooling found; provenance/SBOM tooling found |
| Code shape and semantic surface | 12 | 0 | 0.00 | largest authored code file: jansu-storage/src/lib.rs (3371 LOC); code file exceeds 500 LOC |
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
   Reason: `Code shape and semantic surface` scored 0 below the standard floor of 85
   Fix: split large or ambiguous authored code into smaller semantic modules with focused tests
   Rerun: `just fast`
   Fingerprint: `sha256:9d7b045da9638adf8c1194ec51f57a06cf2e7f4e03a597714e08215a62420702`
   Evidence: largest authored code file: jansu-storage/src/lib.rs (3371 LOC), code file exceeds 500 LOC, code file exceeds 1000 LOC, copy-code inexcusable classes found: 2 (exact file or same-name function copy)
2. `high` `ci` `.github/workflows/ci.yml:1`
   Rule: `HLT-042-CI-LOCAL-PARITY`
   Check: `HLT-042-CI-LOCAL-PARITY:ci` `hard` confidence `0.95`
   Route: TLR `Verification`, lane `fast`, owner `ops`
   Docs: `docs/ci-local.md`
   Matched term: `ci.local-parity.lib-missing`
   Reason: ops/ci/lib.sh is the shared helper module (artifact assertions, tool pins) every lane sources
   Fix: add ops/ci/lib.sh defining shared helpers and tool version pins
   Rerun: `just fast`
   Fingerprint: `sha256:f9426bff22d503a91f398d05e8692f626ef28fae31c299094095a46139e67dcd`
   Evidence: detector=ci.local-parity.lib-missing, path=.github/workflows/ci.yml, line=1, proof_window=None, snippet=---
3. `high` `ci` `.github/workflows/ci.yml:1`
   Rule: `HLT-042-CI-LOCAL-PARITY`
   Check: `HLT-042-CI-LOCAL-PARITY:ci` `hard` confidence `0.95`
   Route: TLR `Verification`, lane `fast`, owner `ops`
   Docs: `docs/ci-local.md`
   Matched term: `ci.local-parity.pre-push-hook-missing`
   Reason: without a mandatory pre-push gate, broken code can be pushed and CI is the first place a failure shows up
   Fix: add ops/git-hooks/pre-push that runs `bash ops/ci/quality-gates.sh` and wire it via `git config core.hooksPath ops/git-hooks`
   Rerun: `just fast`
   Fingerprint: `sha256:0b1dec1bcff8cac6af5980a4cb7be666d62f917fbeb874cff3e82a9c8179c2f7`
   Evidence: detector=ci.local-parity.pre-push-hook-missing, path=.github/workflows/ci.yml, line=1, proof_window=None, snippet=---
4. `high` `ci` `.github/workflows/ci.yml:1`
   Rule: `HLT-042-CI-LOCAL-PARITY`
   Check: `HLT-042-CI-LOCAL-PARITY:ci` `hard` confidence `0.95`
   Route: TLR `Verification`, lane `fast`, owner `ops`
   Docs: `docs/ci-local.md`
   Matched term: `ci.local-parity.doctor-missing`
   Reason: without a doctor script, developers cannot confirm their local environment matches CI
   Fix: add scripts/ci-doctor.sh listing every tool the ops/ci scripts depend on
   Rerun: `just fast`
   Fingerprint: `sha256:81b4c6f9757e71710483dd2e815382d8d4b6580963f23e6157b5da52d5b56b06`
   Evidence: detector=ci.local-parity.doctor-missing, path=.github/workflows/ci.yml, line=1, proof_window=None, snippet=---
5. `high` `ci` `.github/workflows/ci.yml:1`
   Rule: `HLT-042-CI-LOCAL-PARITY`
   Check: `HLT-042-CI-LOCAL-PARITY:ci` `hard` confidence `0.95`
   Route: TLR `Verification`, lane `fast`, owner `ops`
   Docs: `docs/ci-local.md`
   Matched term: `ci.local-parity.runner-missing`
   Reason: scripts/ci-local.sh is the local entry point that delegates to the same ops/ci scripts the workflows call
   Fix: add scripts/ci-local.sh exposing each CI lane locally
   Rerun: `just fast`
   Fingerprint: `sha256:439f613bb966671a73caabc94bd33bc2dc553df2c0cb4d8867c61ef4673c6291`
   Evidence: detector=ci.local-parity.runner-missing, path=.github/workflows/ci.yml, line=1, proof_window=None, snippet=---
6. `high` `security` `.github/workflows/ci.yml:1`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.concurrency.missing`
   Reason: workflow can run duplicate stale audits for the same ref
   Fix: add workflow-level concurrency with cancel-in-progress
   Rerun: `just security`
   Fingerprint: `sha256:1759b857f9322a8a07ee73ebfd51913a1bc3a5c47b3af3d62f138da10fa1abe0`
   Evidence: detector=ci.concurrency.missing, path=.github/workflows/ci.yml, line=1, proof_window=None, snippet=---
7. `high` `security` `.github/workflows/ci.yml:1`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.timeout.missing`
   Reason: workflow can run without a checked time bound
   Fix: set an explicit timeout-minutes on each job
   Rerun: `just security`
   Fingerprint: `sha256:9ba217a6f37451562bbe228da9ae7bb1c1517de628cf170cb438601eff720d9d`
   Evidence: detector=ci.timeout.missing, path=.github/workflows/ci.yml, line=1, proof_window=None, snippet=---
8. `high` `ci` `.github/workflows/ci.yml:17`
   Rule: `HLT-042-CI-LOCAL-PARITY`
   Check: `HLT-042-CI-LOCAL-PARITY:ci` `hard` confidence `0.95`
   Route: TLR `Verification`, lane `fast`, owner `ops`
   Docs: `docs/ci-local.md`
   Matched term: `ci.local-parity.workflow-not-thin`
   Reason: without a single source of truth, local runs drift from CI and breakage is only visible after push
   Fix: extract the workflow steps into ops/ci/<lane>.sh and call them with `bash ops/ci/<lane>.sh`
   Rerun: `just fast`
   Fingerprint: `sha256:2b96675d5f06b2f3e21b366ed6a00bf357f44ce019099126dc1b7963747691c1`
   Evidence: detector=ci.local-parity.workflow-not-thin, path=.github/workflows/ci.yml, line=17, proof_window=None, snippet=jobs:
9. `high` `security` `.github/workflows/ci.yml:56`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:13132a3dc3182578cacaf1568447c0b1b0486150f4614c06a0ee5c942eb0b9c9`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=56, proof_window=None, snippet=- uses: actions/checkout@v6
10. `high` `security` `.github/workflows/ci.yml:57`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:07850f5d088db23f3272aa679de02f669be1b846d97896861c0157284e4a219f`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=57, proof_window=None, snippet=- uses: extractions/setup-just@v3
11. `high` `security` `.github/workflows/ci.yml:60`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:6150d7449cb2d6f4ca5120fe2ff0b1a7e4c3993c27415d34cbac876c5439efd9`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=60, proof_window=None, snippet=- uses: actions-rust-lang/setup-rust-toolchain@v1
12. `high` `security` `.github/workflows/ci.yml:73`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:d86a48a62dd9d023b59bc9240c0999cb42264d853d9a5a3e4bc1ae746e7335b8`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=73, proof_window=None, snippet=- uses: actions/upload-artifact@v7
13. `high` `security` `.github/workflows/ci.yml:89`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:4fecce987782804d27b9b5a6be8311a672989cb4c51c434d56a896ec4895420e`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=89, proof_window=None, snippet=- uses: actions/checkout@v6
14. `high` `security` `.github/workflows/ci.yml:90`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:93c252f51be60340bd9499d1ce9bd62a7a2d5e799fb1a024cd0105419c17c406`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=90, proof_window=None, snippet=- uses: extractions/setup-just@v3
15. `high` `security` `.github/workflows/ci.yml:91`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:85194c6ec9b552cc0559492b8fb7587ecfde18897c351c7bcc27b12f9499da60`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=91, proof_window=None, snippet=- uses: actions-rust-lang/setup-rust-toolchain@v1
16. `high` `security` `.github/workflows/ci.yml:104`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:0efef07b766895b2af3738c5ebf90f5a181c478109e1e141a7010d96adf0f7df`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=104, proof_window=None, snippet=- uses: actions/checkout@v6
17. `high` `security` `.github/workflows/ci.yml:105`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:7214fe98ffcdb538ed8011731f1343e87a9c57931a8805cd9781a1cf60885cd3`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=105, proof_window=None, snippet=- uses: extractions/setup-just@v3
18. `high` `security` `.github/workflows/ci.yml:106`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:4c5839d638a821fca26e597590901b588286f9ede60368781c07ce205f1225b3`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=106, proof_window=None, snippet=- uses: actions-rust-lang/setup-rust-toolchain@v1
19. `high` `security` `.github/workflows/ci.yml:118`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:87ff61fae8c3c0301df66a7699d189a8cd5bfab8858fe217b4e57facb69d03c8`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=118, proof_window=None, snippet=- uses: actions/checkout@v6
20. `high` `security` `.github/workflows/ci.yml:119`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:7bad2f275e218930fde97059256631b4fad97307aeceab7996a0a2ca746ec2da`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=119, proof_window=None, snippet=- uses: extractions/setup-just@v3
21. `high` `security` `.github/workflows/ci.yml:120`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:018afc2f7b3d2f040751d23957da9f8598c8492877d498506c31f8d37772cdad`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=120, proof_window=None, snippet=- uses: actions-rust-lang/setup-rust-toolchain@v1
22. `high` `security` `.github/workflows/ci.yml:138`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:f32b02ab01c50365d6b8c6028dac3fc1a680d633489cfc93fe6ba47cd7e40dbc`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=138, proof_window=None, snippet=- uses: actions/checkout@v6
23. `high` `security` `.github/workflows/ci.yml:139`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:41627218d1e67ebfdc5b4e384b6ef58f6aa904833a5ce0699ce012f1018acc24`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=139, proof_window=None, snippet=- uses: actions-rust-lang/setup-rust-toolchain@v1
24. `high` `security` `.github/workflows/ci.yml:162`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:c8587c6013807c253e1d9e3b94b13b13738999bfaa438ec54d7c4cc3f2a61719`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=162, proof_window=None, snippet=- uses: actions/checkout@v6
25. `high` `security` `.github/workflows/ci.yml:163`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:c3f1998e411606e9f6ea07b5248ed4e2ce4bff1b21fe62add83b2078bb8c05c2`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=163, proof_window=None, snippet=- uses: actions-rust-lang/setup-rust-toolchain@v1
26. `high` `security` `.github/workflows/ci.yml:177`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:1268d531fda9fb7dde2eea361edf6c6dff3f5e8de78a840ca0b1585918be6bdb`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=177, proof_window=None, snippet=- uses: actions/checkout@v6
27. `high` `security` `.github/workflows/ci.yml:178`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:30b5a1ddeb637a1882e20a31ba245cf22e9a93aced48278b9d7a4b5f425bbc9b`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=178, proof_window=None, snippet=- uses: actions-rust-lang/setup-rust-toolchain@v1
28. `high` `release` `.github/workflows/ci.yml:180`
   Rule: `HLT-037-RELEASE-BAD-BEHAVIOR`
   Check: `HLT-037-RELEASE-BAD-BEHAVIOR:release` `hard` confidence `0.95`
   Route: TLR `Verification`, lane `release`, owner `ops`
   Docs: `docs/BAD_release.md`
   Matched term: `release.integrity.missing`
   Reason: published artifacts need machine-checkable integrity and supply-chain receipts
   Fix: attach checksums plus SBOM/provenance/signature/attestation evidence to the release witness
   Rerun: `just check`
   Fingerprint: `sha256:c90e5c96b2c54701e1864959d85dc6f8d006cf6e9fe27ece8fda93ebf77de79e`
   Evidence: detector=release.integrity.missing, path=.github/workflows/ci.yml, line=180, proof_window=None, snippet=- run: cargo publish --dry-run --workspace
29. `high` `security` `.github/workflows/ci.yml:184`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:9cfe2f1783fb9c8cf68f8d4387cba07a3e148d432ee108dd4a98e22d6ecdb4ef`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=184, proof_window=None, snippet=- uses: actions/checkout@v6
30. `high` `security` `.github/workflows/ci.yml:185`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:47c788f86a24c7383329d5dae60b17100c3a349ac96090ea31b3c97c4894ec1d`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=185, proof_window=None, snippet=- uses: crate-ci/typos@v1.44.0
31. `high` `security` `.github/workflows/ci.yml:191`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:88c036aef1e0fad247c56b9506fe293e6ea9a52ec0fe451cb5cfed44d22e0339`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=191, proof_window=None, snippet=- uses: actions/checkout@v6
32. `high` `security` `.github/workflows/ci.yml:194`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:e79c56bfba984a57dabf219aa514d4caa0e614013807d42616b095de59df5bc5`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=194, proof_window=None, snippet=- uses: actions/upload-artifact@v7
33. `high` `security` `.github/workflows/ci.yml:210`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:0769df6f6b6300137870d0b1e8a3dcb90e898bac2d56e940e55295201bfcb8cd`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=210, proof_window=None, snippet=- uses: actions/checkout@v6
34. `high` `security` `.github/workflows/ci.yml:211`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:cb45ff24c14dc14cc083087fb0463b40ce80864d68a32958fae74bd2a88c1e40`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=211, proof_window=None, snippet=- uses: docker/metadata-action@v6
35. `high` `security` `.github/workflows/ci.yml:220`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:54fc09640f82a8d22a2a3854209f1d1417132e7b6548e1235917e5111abe7ab8`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=220, proof_window=None, snippet=- uses: docker/setup-buildx-action@v4
36. `high` `security` `.github/workflows/ci.yml:223`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:5ddde0e4d8ea6f807d00f232096c656053930e741baf0a6614255be01c2b282d`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=223, proof_window=None, snippet=- uses: docker/login-action@v4
37. `high` `security` `.github/workflows/ci.yml:228`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:200d19637e767ef25999343d50cbe54032a4078dae9c7908a58d340d9046c81d`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=228, proof_window=None, snippet=- uses: docker/build-push-action@v7
38. `high` `security` `.github/workflows/ci.yml:267`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:892ae7bacdda0372fc93770ac4edd63ecb559f98c4f7cb62b933e9725c80d4a0`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=267, proof_window=None, snippet=- uses: docker/metadata-action@v6
39. `high` `security` `.github/workflows/ci.yml:276`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:7092b648607afe45ce71cef41569761d71a6305ecef4fbe5abca8928cbc54d06`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=276, proof_window=None, snippet=- uses: actions/checkout@v6
40. `high` `security` `.github/workflows/ci.yml:280`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:f659735d03bca71eb4fdb41e7fc99d76d0f183f4f31af59575dbfd1e2b4b853f`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=280, proof_window=None, snippet=- uses: actions/checkout@v6
41. `high` `security` `.github/workflows/ci.yml:322`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:5fc746948a4599a34bc108c7f80ddbf2264689bde735ea29cd978b4f32730f18`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/ci.yml, line=322, proof_window=None, snippet=- uses: actions/checkout@v6
42. `high` `security` `.github/workflows/differential-kafka-lab.yml:1`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.concurrency.missing`
   Reason: workflow can run duplicate stale audits for the same ref
   Fix: add workflow-level concurrency with cancel-in-progress
   Rerun: `just security`
   Fingerprint: `sha256:26f1ad2824d07aeaa38a2e1f351a8aefad4170adbfaeef6c34227c18c3ae9de7`
   Evidence: detector=ci.concurrency.missing, path=.github/workflows/differential-kafka-lab.yml, line=1, proof_window=None, snippet=name: differential-kafka-lab
43. `high` `security` `.github/workflows/differential-kafka-lab.yml:1`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.permissions.missing`
   Reason: workflow permissions default is not pinned in source
   Fix: add top-level `permissions: contents: read` and job-specific write scopes only where needed
   Rerun: `just security`
   Fingerprint: `sha256:18dc4ce98d9155d2307ab493da0a48ab7ff7b658c958b207e555617754724dbd`
   Evidence: detector=ci.permissions.missing, path=.github/workflows/differential-kafka-lab.yml, line=1, proof_window=None, snippet=name: differential-kafka-lab
44. `high` `ci` `.github/workflows/differential-kafka-lab.yml:17`
   Rule: `HLT-042-CI-LOCAL-PARITY`
   Check: `HLT-042-CI-LOCAL-PARITY:ci` `hard` confidence `0.95`
   Route: TLR `Verification`, lane `fast`, owner `ops`
   Docs: `docs/ci-local.md`
   Matched term: `ci.local-parity.workflow-not-thin`
   Reason: without a single source of truth, local runs drift from CI and breakage is only visible after push
   Fix: extract the workflow steps into ops/ci/<lane>.sh and call them with `bash ops/ci/<lane>.sh`
   Rerun: `just fast`
   Fingerprint: `sha256:1f65a2ec1960d1e88b4be3c213dac0cf71620a2aa7c1e7e842e92ea6b23cd4ce`
   Evidence: detector=ci.local-parity.workflow-not-thin, path=.github/workflows/differential-kafka-lab.yml, line=17, proof_window=None, snippet=jobs:
45. `high` `security` `.github/workflows/differential-kafka-lab.yml:23`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:30b67d3da1017e69072104b8c6768a739e615b30fa48afa8dbbb2f3ae384139e`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/differential-kafka-lab.yml, line=23, proof_window=None, snippet=- uses: actions/checkout@v4
46. `high` `security` `.github/workflows/differential-kafka-lab.yml:26`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:c3ac39e936333a18666f22e1e704d6a71071a11fd479abe8b2abb5d8b1557a05`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/differential-kafka-lab.yml, line=26, proof_window=None, snippet=uses: dtolnay/rust-toolchain@stable
47. `high` `security` `.github/workflows/differential-kafka-lab.yml:45`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:14843d4ea5d6fe7be65b238b0857f1a79f0d5b2935f82375502502744ff0acf7`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/differential-kafka-lab.yml, line=45, proof_window=None, snippet=uses: actions/upload-artifact@v4
48. `medium` `security` `.github/workflows/jankurai.yml`
   Rule: `HLT-016-SUPPLY-CHAIN-DRIFT`
   Check: `HLT-016-SUPPLY-CHAIN-DRIFT:security` `soft` confidence `0.76`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/audit-rubric.md#top-level-risk-mapping`
   Reason: `Security and supply-chain posture` scored 66 below the standard floor of 85
   Fix: wire secret, dependency, provenance, and workflow scans into an operational CI lane
   Rerun: `just security`
   Fingerprint: `sha256:eb6acb55678ae606bd1e3575fe3aa79da832aa766b88714c480c15225d48b3aa`
   Evidence: secret or dependency scan tooling found, provenance/SBOM tooling found, security lane present, canonical security lane wrapper present
49. `high` `security` `.github/workflows/jankurai.yml:1`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.concurrency.missing`
   Reason: workflow can run duplicate stale audits for the same ref
   Fix: add workflow-level concurrency with cancel-in-progress
   Rerun: `just security`
   Fingerprint: `sha256:130d79c121d03f9780d3348a86c4a7eac6308bf578a32a416f722babe7df892e`
   Evidence: detector=ci.concurrency.missing, path=.github/workflows/jankurai.yml, line=1, proof_window=None, snippet=name: jankurai
50. `high` `security` `.github/workflows/jankurai.yml:1`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.timeout.missing`
   Reason: workflow can run without a checked time bound
   Fix: set an explicit timeout-minutes on each job
   Rerun: `just security`
   Fingerprint: `sha256:9e961161af32933baacaf16de5b2469db25a682f6a9e5e311b8985bcef32daa6`
   Evidence: detector=ci.timeout.missing, path=.github/workflows/jankurai.yml, line=1, proof_window=None, snippet=name: jankurai
51. `high` `ci` `.github/workflows/jankurai.yml:8`
   Rule: `HLT-042-CI-LOCAL-PARITY`
   Check: `HLT-042-CI-LOCAL-PARITY:ci` `hard` confidence `0.95`
   Route: TLR `Verification`, lane `fast`, owner `ops`
   Docs: `docs/ci-local.md`
   Matched term: `ci.local-parity.workflow-not-thin`
   Reason: without a single source of truth, local runs drift from CI and breakage is only visible after push
   Fix: extract the workflow steps into ops/ci/<lane>.sh and call them with `bash ops/ci/<lane>.sh`
   Rerun: `just fast`
   Fingerprint: `sha256:c9eb825f09bef3c8366348ce9a2b405e7ed97ee8ba5b52cbadda7e2442063404`
   Evidence: detector=ci.local-parity.workflow-not-thin, path=.github/workflows/jankurai.yml, line=8, proof_window=None, snippet=jobs:
52. `high` `security` `.github/workflows/jankurai.yml:14`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:ece7e0192e1775fd82b811272664eda84968b42a29b7481f095901c97e637a6b`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/jankurai.yml, line=14, proof_window=None, snippet=- uses: actions/checkout@v4
53. `high` `security` `.github/workflows/jankurai.yml:15`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:39ce6d784cbbec4317ca6e4d517cd5df0b2cbce52a53bbbc56f4cd5779d83552`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/jankurai.yml, line=15, proof_window=None, snippet=- uses: dtolnay/rust-toolchain@stable
54. `high` `security` `.github/workflows/jankurai.yml:20`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.sarif.not-uploaded`
   Reason: SARIF evidence is not published to code scanning
   Fix: upload the SARIF artifact with github/codeql-action/upload-sarif pinned to a full SHA
   Rerun: `just security`
   Fingerprint: `sha256:ea548606f9dd9de657a9d5044306086d36ef7e6b76b93b855562e1ef2a825fc5`
   Evidence: detector=ci.sarif.not-uploaded, path=.github/workflows/jankurai.yml, line=20, proof_window=None, snippet=run: jankurai audit . --mode advisory --json target/jankurai/repo-score.json --md target/jankurai/repo-score.md --sarif target/jankurai/jankurai.sarif --github-
55. `high` `security` `.github/workflows/jankurai.yml:22`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.security-scan.nonblocking`
   Reason: security or proof job is explicitly non-blocking
   Fix: remove the non-blocking override so scan failures stop the pipeline
   Rerun: `just security`
   Fingerprint: `sha256:e29e9474c9a5266b2d1562b90efc2b47e8ab6fc619874efda61c3d4d6df7b681`
   Evidence: detector=ci.security-scan.nonblocking, path=.github/workflows/jankurai.yml, line=22, proof_window=None, snippet=run: just db-doctor || true
56. `high` `security` `.github/workflows/jankurai.yml:25`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:25bd3a08e047adbbf28e72abe810fbb4aa22753485ccce49dffb6d3542178db8`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/jankurai.yml, line=25, proof_window=None, snippet=- uses: actions/upload-artifact@v4
57. `high` `security` `.github/workflows/release.yml:1`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.concurrency.missing`
   Reason: workflow can run duplicate stale audits for the same ref
   Fix: add workflow-level concurrency with cancel-in-progress
   Rerun: `just security`
   Fingerprint: `sha256:7725e14981add5a159fbb01af3b00b549313d169c6ab6b7756c01d7251e5b56b`
   Evidence: detector=ci.concurrency.missing, path=.github/workflows/release.yml, line=1, proof_window=None, snippet=name: release
58. `high` `security` `.github/workflows/release.yml:1`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.timeout.missing`
   Reason: workflow can run without a checked time bound
   Fix: set an explicit timeout-minutes on each job
   Rerun: `just security`
   Fingerprint: `sha256:b5473434887edda5182b96e1663f1eb3dbfc83a356126131371cd1ceb46a28c6`
   Evidence: detector=ci.timeout.missing, path=.github/workflows/release.yml, line=1, proof_window=None, snippet=name: release
59. `high` `ci` `.github/workflows/release.yml:21`
   Rule: `HLT-042-CI-LOCAL-PARITY`
   Check: `HLT-042-CI-LOCAL-PARITY:ci` `hard` confidence `0.95`
   Route: TLR `Verification`, lane `fast`, owner `ops`
   Docs: `docs/ci-local.md`
   Matched term: `ci.local-parity.workflow-not-thin`
   Reason: without a single source of truth, local runs drift from CI and breakage is only visible after push
   Fix: extract the workflow steps into ops/ci/<lane>.sh and call them with `bash ops/ci/<lane>.sh`
   Rerun: `just fast`
   Fingerprint: `sha256:7365dc3fedcdc2e53a26be9e69e40a7a8b856f73ffe3363d23d859495b2e4af3`
   Evidence: detector=ci.local-parity.workflow-not-thin, path=.github/workflows/release.yml, line=21, proof_window=None, snippet=jobs:
60. `high` `security` `.github/workflows/release.yml:28`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:aa4598a50fb6dc057829fb92cc1e547c26d2efc3bf67e2cf7fd0910c485bfe55`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/release.yml, line=28, proof_window=None, snippet=- uses: actions/checkout@v6
61. `high` `security` `.github/workflows/release.yml:32`
   Rule: `HLT-034-CI-BAD-BEHAVIOR`
   Check: `HLT-034-CI-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `ci.action.not-full-sha`
   Reason: tag or branch refs can change without review
   Fix: pin every external action to a 40-character commit SHA
   Rerun: `just security`
   Fingerprint: `sha256:133782223ae9f893a921aaec0bd8dec9b1d4f727fdaa97eabc5d349afde0049b`
   Evidence: detector=ci.action.not-full-sha, path=.github/workflows/release.yml, line=32, proof_window=None, snippet=- uses: extractions/setup-just@v3
62. `medium` `proof` `Justfile`
   Rule: `HLT-018-PERF-CONCURRENCY-DRIFT`
   Check: `HLT-018-PERF-CONCURRENCY-DRIFT:proof` `soft` confidence `0.76`
   Route: TLR `Verification`, lane `fast`, owner `workspace`
   Docs: `docs/testing.md`
   Reason: `Build speed signals` scored 70 below the standard floor of 85
   Fix: add fast deterministic build/test targets, caches, and narrow proof lanes for agent iteration
   Rerun: `just fast`
   Fingerprint: `sha256:1ab579f1e82b68096d7993539cc68cbf5c5cf22e4aadf4d2e4515aa7279b5b22`
   Evidence: build acceleration markers found, targeted test/build commands found, CI cache hint found, explicit cache marker plus narrow per-package target found
63. `high` `security` `agent/agent-tool-supply-evidence.md:7`
   Rule: `HLT-024-AGENT-TOOL-SUPPLY-GAP`
   Check: `HLT-024-AGENT-TOOL-SUPPLY-GAP:security` `hard` confidence `0.88`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `agent`
   Docs: `docs/audit-rubric.md#top-level-risk-mapping`
   Matched term: `agent tool supply`
   Reason: agent tool supply-chain changes alter execution authority
   Fix: pin and review agent tools, MCP servers, hooks, and rule files; keep untrusted tool output separate from trusted policy
   Rerun: `just security`
   Fingerprint: `sha256:e82fa99b0edfc9e2e9565d2fc335bd1f6758b8114ea59de3f94508ecbfe0c4e8`
   Evidence: checks that nothing has slipped in unpinned.
64. `medium` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `soft` confidence `0.76`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: `Ownership and navigation surface` scored 83 below the standard floor of 85
   Fix: tighten owner/test maps and root routing until agents can localize ownership without inference
   Rerun: `just fast`
   Fingerprint: `sha256:f22331131a2d75b4ff814289d01473fb3ab281f4b3c8ac366b63eb97cfe85438`
   Evidence: root `AGENTS.md` present, owner map present, test/proof routing map present, local `AGENTS.md` file(s)
65. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `.cursor/rules/jansu-master-plan.mdc` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:115c66db5f596afa9da57da6b76f2bf98a17de0d2fb39f61a950963fd2b5fa28`
   Evidence: .cursor/rules/jansu-master-plan.mdc
66. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `CHANGELOG.md` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:2911a47f9548e75233bc304c284818b5b7fb6c72ecde5d7bc5c97be0635cd18a`
   Evidence: CHANGELOG.md
67. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `FEATURE_GAPS.md` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:2372d1d68ea2f342f127a3533021ca0165f023c2f5e2f5c395fc0c532e58561d`
   Evidence: FEATURE_GAPS.md
68. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `compose.yaml` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:6476456e2b7a7ea1ed6401886bd8eea08e291b20efd14a7209f78deba40ec8bc`
   Evidence: compose.yaml
69. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `data/.gitignore` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:85e38d07f0c90ee86ab2f6a848ae5a574b448547f7f5f5b79fff96de7e5691e5`
   Evidence: data/.gitignore
70. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `demo/2026-qcon-london-01.tape` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:6d6d30979757a5a3a4c4ad2c5a0e75400527bb477ff89ca0c6ca79be2f12acac`
   Evidence: demo/2026-qcon-london-01.tape
71. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `duckdb-init.sql` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:c98891363f3d4c2552f5a5af97eebbce0559fe9938625affbce4c8eea0619d8c`
   Evidence: duckdb-init.sql
72. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `etc/certs/.gitignore` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:80f752b57523b9005da44356f266c46bc53e5e62a4b75563f125da4627c0eabe`
   Evidence: etc/certs/.gitignore
73. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `etc/data/employees.json` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:78ff21afddb2516e4e8ec0d654cfae2f65fe660b85808421575450dd1f954370`
   Evidence: etc/data/employees.json
74. `high` `context` `agent/owner-map.json`
   Rule: `HLT-003-OWNERLESS-PATH`
   Check: `HLT-003-OWNERLESS-PATH:context` `hard` confidence `0.88`
   Route: TLR `Context/setup`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#ownership-boundaries`
   Reason: path `etc/data/grades.json` has no owner-map route
   Fix: add the narrowest stable prefix for this path to `agent/owner-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:e6a1e68027c47c30baed155fd1e8d4eaa07fcf9cb6098c6ed050ca9d0ca02e1e`
   Evidence: etc/data/grades.json
75. `medium` `proof` `agent/repo-score.json:1592`
   Rule: `HLT-027-HUMAN-REVIEW-EVIDENCE-GAP`
   Check: `HLT-027-HUMAN-REVIEW-EVIDENCE-GAP:proof` `soft` confidence `0.88`
   Route: TLR `Repair`, lane `audit`, owner `agent`
   Docs: `docs/testing.md`
   Matched term: `review evidence`
   Reason: proof and review claims need receipts
   Fix: attach raw CI logs, review receipts, and replayable commands instead of accepting claims or summaries
   Rerun: `just score`
   Fingerprint: `sha256:811256f49bff78233aee80d61d79a64b7578ac14cad48ad549ac393b21229928`
   Evidence: "\"\\\"Evidence: \\\\\\\"about\\\\\\\": \\\\\\\"The principal filter, or null to accept all principals.\\\\\\\" },\\\"\""
76. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `.gitignore` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:8fe7edc74ce7873d59c9b6ccb57a19bca48fe9589c015113fb5028c2cc245992`
   Evidence: .gitignore
77. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `CHANGELOG.md` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:eaa40b784b5bb02ef321df1133743a622cda79939f89a05728e47c374dc90555`
   Evidence: CHANGELOG.md
78. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `FEATURE_GAPS.md` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:9f40a4804dd8b778d8da8f5f94c2df3c71aaa6630be6f60fdb2df7ea049df952`
   Evidence: FEATURE_GAPS.md
79. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `README.md` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:c4a25759c16b05da63ece6c953b801c311e0df2a29f91df6cc92c21e69fa6cc7`
   Evidence: README.md
80. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `compose.yaml` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:85dff384f1e0e7475491bdb74f5e2dcdc1bfda543a27805361d678daad4e9b35`
   Evidence: compose.yaml
81. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `data/.gitignore` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:bf2e3b11a16c0b1b6ae9eaae44f1c553eac803a8446688825dff1c947b0363c3`
   Evidence: data/.gitignore
82. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `demo/2026-qcon-london-01.tape` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:815e5c7e54820649d59d775b79f2137f7fe2a2016a1c6835f9dd012b9a32db16`
   Evidence: demo/2026-qcon-london-01.tape
83. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `duckdb-init.sql` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:8379fe57ba0cbdc950d001d6c6af7fd4bc5aa948941098e35c7c4f7b2af3d770`
   Evidence: duckdb-init.sql
84. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `etc/certs/.gitignore` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:335ce812057eeded1e560449bca40eda8aabb9886e4cb67a932205e09253fc39`
   Evidence: etc/certs/.gitignore
85. `high` `proof` `agent/test-map.json`
   Rule: `HLT-004-UNMAPPED-PROOF`
   Check: `HLT-004-UNMAPPED-PROOF:proof` `hard` confidence `0.88`
   Route: TLR `Verification`, lane `fast`, owner `agent`
   Docs: `agent/JANKURAI_STANDARD.md#proof-lanes`
   Reason: path `etc/data/employees.json` has no test-map proof route
   Fix: add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Rerun: `just fast`
   Fingerprint: `sha256:d4f9f108d88a46c4a328457a13c523bd20107301e03898ed5de0655fd9ab1c06`
   Evidence: etc/data/employees.json
86. `high` `security` `compose.yaml:97`
   Rule: `HLT-032-DOCKER-BAD-BEHAVIOR`
   Check: `HLT-032-DOCKER-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `docker.port.public-db-admin`
   Reason: the port mapping is publicly reachable without a local bind
   Fix: bind the port to localhost or keep it on an internal-only network
   Rerun: `just security`
   Fingerprint: `sha256:0ee732caa0bb38efb95069a404edd6c02777dc350a1ee8fdcbe4e272ff594fe5`
   Evidence: detector=docker.port.public-db-admin, path=compose.yaml, line=97, proof_window=None, snippet=- LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@db:5432/postgres
87. `high` `security` `compose.yaml:98`
   Rule: `HLT-032-DOCKER-BAD-BEHAVIOR`
   Check: `HLT-032-DOCKER-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `docker.port.public-db-admin`
   Reason: the port mapping is publicly reachable without a local bind
   Fix: bind the port to localhost or keep it on an internal-only network
   Rerun: `just security`
   Fingerprint: `sha256:f7345b96cf712bb97ef59c4441f19974646c35d860397e0a02f61865f79926d1`
   Evidence: detector=docker.port.public-db-admin, path=compose.yaml, line=98, proof_window=None, snippet=- LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@db:5432/postgres
88. `high` `security` `compose.yaml:120`
   Rule: `HLT-032-DOCKER-BAD-BEHAVIOR`
   Check: `HLT-032-DOCKER-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `docker.port.public-db-admin`
   Reason: the port mapping is publicly reachable without a local bind
   Fix: bind the port to localhost or keep it on an internal-only network
   Rerun: `just security`
   Fingerprint: `sha256:7584765832ae47043a5d45a51da95f8375690a5b902b9906a16a9293a36b4d49`
   Evidence: detector=docker.port.public-db-admin, path=compose.yaml, line=120, proof_window=None, snippet=- LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@db:5432/postgres
89. `high` `security` `compose.yaml:121`
   Rule: `HLT-032-DOCKER-BAD-BEHAVIOR`
   Check: `HLT-032-DOCKER-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `ops`
   Docs: `docs/testing.md`
   Matched term: `docker.port.public-db-admin`
   Reason: the port mapping is publicly reachable without a local bind
   Fix: bind the port to localhost or keep it on an internal-only network
   Rerun: `just security`
   Fingerprint: `sha256:ab67b0a956fb0b0ca50058292cb0cd8608e24db34a655710d789000f2b7003d3`
   Evidence: detector=docker.port.public-db-admin, path=compose.yaml, line=121, proof_window=None, snippet=- LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@db:5432/postgres
90. `medium` `release` `docs/testing.md`
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
91. `high` `release` `docs/testing.md`
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
92. `high` `vibe` `jansu-auth/src/lib.rs:111`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: fallback soup detected in product code
   Fix: collapse fallback chains into explicit typed states with bounded retry policy, telemetry, and documented repair guidance
   Rerun: `just fast`
   Fingerprint: `sha256:9cd6138ad65ebf74395e19d8c51f9f5e19cdfe97350f98ca9576fb06723642d7`
   Evidence: jansu-auth/src/lib.rs:111 .unwrap_or_default()
93. `high` `security` `jansu-broker/src/broker.rs:19`
   Rule: `HLT-022-AUTHZ-ISOLATION-GAP`
   Check: `HLT-022-AUTHZ-ISOLATION-GAP:security` `hard` confidence `0.88`
   Route: TLR `Business truth`, lane `db`, owner `tools`
   Docs: `docs/audit-rubric.md#top-level-risk-mapping`
   Matched term: `admin`
   Reason: authz/data isolation requires negative proof evidence
   Fix: add owner/non-owner authorization tests or RLS evidence for the touched data boundary
   Rerun: `just fast`
   Fingerprint: `sha256:63644684c7e674d6214f28f84d08185f54a001676139805373a4c35ef0c4a14f`
   Evidence: // proof: jansu-broker/src/coordinator/group/administrator/tests.rs::{heartbeat_from_unknown_member_returns_error,leave_unknown_member_returns_per_member_error,lifecycle}
94. `high` `vibe` `jansu-cli/src/cli/perf.rs:103`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:28d6eeb8107b5cff46177c5091679a4a6e15603bfb974ea13d065d9bbb79bc07`
   Evidence: jansu-cli/src/cli/perf.rs:103, future-hostile/dead-language term `todo` appears
95. `high` `vibe` `jansu-cli/src/cli/perf.rs:103`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: product code contains TODO/stub/unimplemented/unreachable placeholder markers
   Fix: replace placeholders with implemented behavior, typed unsupported-state errors, or a tracked exception record with docs
   Rerun: `just fast`
   Fingerprint: `sha256:36f9733d6c2bfe560955117df3c14da8396e5b9d5484e86490ccc565c712331f`
   Evidence: jansu-cli/src/cli/perf.rs:103 Command::Consume => todo!(),
96. `high` `security` `jansu-cli/src/cli/user.rs:83`
   Rule: `HLT-029-RUST-BAD-BEHAVIOR`
   Check: `HLT-029-RUST-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#top-level-risk-mapping`
   Matched term: `rust.unsafe.zeroed`
   Reason: all-zero validity was not proven
   Fix: construct the type with a valid initializer instead of zeroing it
   Rerun: `just fast`
   Fingerprint: `sha256:d474ee3c5cc8815f1908f3e00fbb39dc51ffaa7332b454bb251b6a02e8dd1f19`
   Evidence: detector=zeroed, proof-window=NearbySafetyComment, snippet=let mut buf = BytesMut::zeroed(32);
97. `high` `security` `jansu-cli/src/cli/user.rs:88`
   Rule: `HLT-029-RUST-BAD-BEHAVIOR`
   Check: `HLT-029-RUST-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#top-level-risk-mapping`
   Matched term: `rust.unsafe.zeroed`
   Reason: all-zero validity was not proven
   Fix: construct the type with a valid initializer instead of zeroing it
   Rerun: `just fast`
   Fingerprint: `sha256:2db32e4b761c181c90c1dd3953584c87ca743a0a4fa6c0648481d7a132a4b537`
   Evidence: detector=zeroed, proof-window=NearbySafetyComment, snippet=let mut buf = BytesMut::zeroed(64);
98. `high` `security` `jansu-cli/src/cli/user.rs:125`
   Rule: `HLT-029-RUST-BAD-BEHAVIOR`
   Check: `HLT-029-RUST-BAD-BEHAVIOR:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#top-level-risk-mapping`
   Matched term: `rust.unsafe.zeroed`
   Reason: all-zero validity was not proven
   Fix: construct the type with a valid initializer instead of zeroing it
   Rerun: `just fast`
   Fingerprint: `sha256:6bc45337c0ccbdacb9c8100031ee17b6b376b47f9ac4514b0c48e40ef60b50d7`
   Evidence: detector=zeroed, proof-window=NearbySafetyComment, snippet=let mut salt = BytesMut::zeroed(Self::DEFAULT_SALT_LEN);
99. `critical` `security` `jansu-cli/src/cli/user.rs:191`
   Rule: `HLT-010-SECRET-SPRAWL`
   Check: `HLT-010-SECRET-SPRAWL:security` `hard` confidence `0.95`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `tools`
   Docs: `docs/audit-rubric.md#top-level-risk-mapping`
   Reason: secret-like value or credential material appears in repository text
   Fix: remove and rotate the credential, add local and CI secret scanning, and scan transcripts/artifacts/MCP config for related exposure
   Rerun: `just security`
   Fingerprint: `sha256:9a2c333d4601141e63887b4e1f27d998fa0f896d73602e34fcb72e1f08f273ad`
   Evidence: let password = "password";
100. `high` `security` `jansu-schema/src/lake/delta.rs:442`
   Rule: `HLT-023-INPUT-BOUNDARY-GAP`
   Check: `HLT-023-INPUT-BOUNDARY-GAP:security` `hard` confidence `0.88`
   Route: TLR `Security, secrets, agency`, lane `security`, owner `tools`
   Docs: `docs/audit-rubric.md#top-level-risk-mapping`
   Matched term: `string sql`
   Reason: input handling risk needs deterministic negative tests
   Fix: replace unsafe sinks with typed schemas, parameterized APIs, allowlists, or sandboxed execution plus negative tests
   Rerun: `just security`
   Fingerprint: `sha256:94031e72569c4157da82611b3c0b0c3cb569e5ee1198736f2575b5f43e9b3276`
   Evidence: let sql = format!("SELECT {} FROM t", all_cols);
101. `high` `vibe` `jansu-storage/src/dynostore/batch.rs:116`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:152d30d914ab637c611f89cc991988c27dd162416a7cb142754deb454ea4a1fa`
   Evidence: jansu-storage/src/dynostore/batch.rs:116, future-hostile/dead-language term `todo` appears
102. `high` `vibe` `jansu-storage/src/dynostore/batch.rs:367`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:0dd9e1b7e1a5b10444be29da8ff3c3e792f752b851d8324d54d33bd48bb6200c`
   Evidence: jansu-storage/src/dynostore/batch.rs:367, future-hostile/dead-language term `todo` appears
103. `high` `vibe` `jansu-storage/src/dynostore/describe.rs:257`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:9aa6ed688298c56fd71b102028f9906d169485ae8195f7a9aee37bb806d7c1eb`
   Evidence: jansu-storage/src/dynostore/describe.rs:257, future-hostile/dead-language term `todo` appears
104. `high` `vibe` `jansu-storage/src/dynostore/features.rs:161`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:0dec23e6c4febce74ce386c3701773a6cb3d518ea297cca7ba0902e3284b8400`
   Evidence: jansu-storage/src/dynostore/features.rs:161, future-hostile/dead-language term `todo` appears
105. `high` `vibe` `jansu-storage/src/dynostore/mod.rs:256`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:c35da02b771072325ee35dead0cfa7fec35c935d0c2e43f1f8a9cea902caf81b`
   Evidence: jansu-storage/src/dynostore/mod.rs:256, future-hostile/dead-language term `todo` appears
106. `high` `vibe` `jansu-storage/src/dynostore/mod.rs:257`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:83451d4795e59faaaf13f5c6a69bd49e44e5b88a63f53e94fe709dcdbf37839b`
   Evidence: jansu-storage/src/dynostore/mod.rs:257, future-hostile/dead-language term `todo` appears
107. `high` `vibe` `jansu-storage/src/limbo/engine.rs:32`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:95de5b3a88d73d2f1912ec75998616e61b78755efbd9a745c94edfcaba5999cc`
   Evidence: jansu-storage/src/limbo/engine.rs:32, future-hostile/dead-language term `todo` appears
108. `high` `vibe` `jansu-storage/src/limbo/storage_broker.rs:147`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:7d7c94867670f625876ac3bc216495310ec2e68e2a1614e14ff0be8da9a6892c`
   Evidence: jansu-storage/src/limbo/storage_broker.rs:147, future-hostile/dead-language term `todo` appears
109. `high` `vibe` `jansu-storage/src/limbo/storage_broker.rs:289`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:ff749d6fa461dc9a8747eb6a52e32d77974057a4d292eb8c6c658ae99fdbdd99`
   Evidence: jansu-storage/src/limbo/storage_broker.rs:289, future-hostile/dead-language term `todo` appears
110. `high` `vibe` `jansu-storage/src/limbo/storage_broker.rs:290`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:9e78e9bfa58e1539b2c583d214647c0775cc6cc0ad606d0bd417905a71eb7939`
   Evidence: jansu-storage/src/limbo/storage_broker.rs:290, future-hostile/dead-language term `todo` appears
111. `high` `vibe` `jansu-storage/src/limbo/storage_end.rs:182`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:6989406641e03ccf785715d75f993615b1b45276e41fa5a9a44e3c9c64d926a5`
   Evidence: jansu-storage/src/limbo/storage_end.rs:182, future-hostile/dead-language term `todo` appears
112. `high` `vibe` `jansu-storage/src/limbo/storage_end.rs:191`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:7ac94512d72dd1e94f4f04116cb8428f30d5bba86aaae430ae2130a30b1d22d9`
   Evidence: jansu-storage/src/limbo/storage_end.rs:191, future-hostile/dead-language term `todo` appears
113. `high` `vibe` `jansu-storage/src/limbo/storage_end.rs:199`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:c1fde91132c57b25e481a966e00b30a5f3e957a501be81e345a5d61101ba78e2`
   Evidence: jansu-storage/src/limbo/storage_end.rs:199, future-hostile/dead-language term `todo` appears
114. `high` `vibe` `jansu-storage/src/limbo/storage_txn.rs:327`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:80073815bfa4e9327b588d0fdecf9c935c91ad2a9ad03e3dc8dd0c8a995dc72c`
   Evidence: jansu-storage/src/limbo/storage_txn.rs:327, future-hostile/dead-language term `todo` appears
115. `high` `vibe` `jansu-storage/src/limbo/storage_txn.rs:436`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:4ac7f41aa40e4abfa3b67cc907d5a590b6d6f8e406351a1e0a9b08a2e9a66660`
   Evidence: jansu-storage/src/limbo/storage_txn.rs:436, future-hostile/dead-language term `todo` appears
116. `high` `copy-code` `jansu-storage/src/limbo/tests.rs:197`
   Rule: `HLT-043-COPY-PASTE-BAD-BEHAVIOR`
   Check: `HLT-043-COPY-PASTE-BAD-BEHAVIOR:copy-code` `hard` confidence `0.95`
   Route: TLR `Maintainability entropy`, lane `copy-code`, owner `tools`
   Docs: `docs/BAD_COPY.md`
   Matched term: `create_topic`
   Reason: same-name semantic unit copied across multiple files
   Fix: keep the named unit in one owner and call it from the other sites
   Rerun: `cargo run -p jankurai -- copy-code . --json target/jankurai/copy-code.json --md target/jankurai/copy-code.md`
   Fingerprint: `sha256:be230947fe9311d0a1e0d9095a03047fec17f756a2223df452a94129f81d6c8b`
   Evidence: kind=ExactUnitSameName, language=rust, duplicate_lines=55, duplicate_tokens=111, duplicate_bytes=1360, instances=jansu-storage/src/limbo/tests.rs:197-252, jansu-storage/src/lite/tests.rs:197-252
117. `high` `copy-code` `jansu-storage/src/limbo/tests.rs:255`
   Rule: `HLT-043-COPY-PASTE-BAD-BEHAVIOR`
   Check: `HLT-043-COPY-PASTE-BAD-BEHAVIOR:copy-code` `hard` confidence `0.95`
   Route: TLR `Maintainability entropy`, lane `copy-code`, owner `tools`
   Docs: `docs/BAD_COPY.md`
   Matched term: `create_topic_in_tx`
   Reason: same-name semantic unit copied across multiple files
   Fix: keep the named unit in one owner and call it from the other sites
   Rerun: `cargo run -p jankurai -- copy-code . --json target/jankurai/copy-code.json --md target/jankurai/copy-code.md`
   Fingerprint: `sha256:3153b308c5c8d4343c383dd9ccaf7e8be63cda212107ff5f7a43cb8953fe2e46`
   Evidence: kind=ExactUnitSameName, language=rust, duplicate_lines=48, duplicate_tokens=113, duplicate_bytes=1239, instances=jansu-storage/src/limbo/tests.rs:255-303, jansu-storage/src/lite/tests.rs:255-303
118. `high` `vibe` `jansu-storage/src/lite/delegate_compaction.rs:183`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `temporary` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:ed7773a2f5dcb48eff9447ed39ade9cc34468a678e7acb33fcfdb165875ea184`
   Evidence: jansu-storage/src/lite/delegate_compaction.rs:183, future-hostile/dead-language term `temporary` appears
119. `high` `vibe` `jansu-storage/src/lite/delegate_compaction.rs:184`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `temporary` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:5d0da1423c145b166fcd815eefb44b71bb38cfe85df80fcfca6260092a13dce6`
   Evidence: jansu-storage/src/lite/delegate_compaction.rs:184, future-hostile/dead-language term `temporary` appears
120. `high` `vibe` `jansu-storage/src/lite/delegate_compaction.rs:185`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `temporary` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:44f17249d246134f5050d544187c1fd9ea44ccb558a7bb9612e10af4e03cf77b`
   Evidence: jansu-storage/src/lite/delegate_compaction.rs:185, future-hostile/dead-language term `temporary` appears
121. `high` `vibe` `jansu-storage/src/lite/delegate_compaction.rs:187`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `temporary` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:a0abd8d1f5533e74316027450be925068f0b0f52fe7cc57ba6bda6afa27f61c9`
   Evidence: jansu-storage/src/lite/delegate_compaction.rs:187, future-hostile/dead-language term `temporary` appears
122. `high` `vibe` `jansu-storage/src/lite/delegate_compaction.rs:193`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `temporary` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:046888fe43308145b9ddbb2ebef3fee521b5619067a19aad2ba59cce9e4d7c23`
   Evidence: jansu-storage/src/lite/delegate_compaction.rs:193, future-hostile/dead-language term `temporary` appears
123. `high` `data` `jansu-storage/src/lite/policy_compact_delete.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:ba1294c71defe1af5cde3d43868682e51acdddd066030b6ec7270538b73947a9`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from record
124. `high` `data` `jansu-storage/src/lite/policy_delete.sql:51`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:1bf0911bbade1a46ff0a24303d8d1c0c2133a9c01dc4c726067f97c98a9d94dd`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from record
125. `high` `vibe` `jansu-storage/src/lite/storage_admin.rs:171`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:de7dcb22a6cb231313bd96299f9afdf1342fd9c49b3eff0a57dca5b37ce8b9b9`
   Evidence: jansu-storage/src/lite/storage_admin.rs:171, future-hostile/dead-language term `todo` appears
126. `high` `vibe` `jansu-storage/src/lite/storage_admin.rs:306`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:0e680197c24e93039c73cae32f69e27918bb78fb3b62b928439242e68cc891d5`
   Evidence: jansu-storage/src/lite/storage_admin.rs:306, future-hostile/dead-language term `todo` appears
127. `high` `vibe` `jansu-storage/src/lite/storage_admin.rs:307`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:0d47dbc7c7ed07db735ca26bfde32faf7b84d7271b3a9b6b77e6a3fd3fd59bfd`
   Evidence: jansu-storage/src/lite/storage_admin.rs:307, future-hostile/dead-language term `todo` appears
128. `high` `vibe` `jansu-storage/src/lite/storage_producer.rs:268`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:9360438a473c96bba31fa75ebde407d5f5b55b8720c265d8b23d7a0574139544`
   Evidence: jansu-storage/src/lite/storage_producer.rs:268, future-hostile/dead-language term `todo` appears
129. `high` `vibe` `jansu-storage/src/lite/storage_transactions.rs:113`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:64ea004544d067d5abe1954d57b6f22e705e34b5a01153f09ea232d575c58d38`
   Evidence: jansu-storage/src/lite/storage_transactions.rs:113, future-hostile/dead-language term `todo` appears
130. `high` `vibe` `jansu-storage/src/service.rs:1595`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:377cea3db75643e30439273d6c11e824c50db716444776746768d0216e993875`
   Evidence: jansu-storage/src/service.rs:1595, future-hostile/dead-language term `todo` appears
131. `high` `vibe` `jansu-storage/src/service.rs:1599`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:b1e8578e6980a51f90e37e93a9faaebe528d7cccf009eb60bc28f76613bbf168`
   Evidence: jansu-storage/src/service.rs:1599, future-hostile/dead-language term `todo` appears
132. `high` `vibe` `jansu-storage/src/service.rs:1606`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:7ddc4602b55c7394d8d2cb57f9c07c71d22baec764044bd2258e1bbfac1f1de8`
   Evidence: jansu-storage/src/service.rs:1606, future-hostile/dead-language term `todo` appears
133. `high` `vibe` `jansu-storage/src/service.rs:1613`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:d7bf133501f86f450b5d3ad4bf1f070095281f243d7c245af7cdb532d6b32fed`
   Evidence: jansu-storage/src/service.rs:1613, future-hostile/dead-language term `todo` appears
134. `high` `vibe` `jansu-storage/src/service.rs:1617`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:613898c0379bd2fc3b4ca593f2924c0882f0ed2f6ff226f33f5950d90fe7c966`
   Evidence: jansu-storage/src/service.rs:1617, future-hostile/dead-language term `todo` appears
135. `high` `vibe` `jansu-storage/src/service.rs:1621`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:baec921c749dfc1ec4ce571a7da7252bfb34ba74f3160366125aeadcd7e8c0d8`
   Evidence: jansu-storage/src/service.rs:1621, future-hostile/dead-language term `todo` appears
136. `high` `vibe` `jansu-storage/src/service.rs:1630`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:fd937d28a9bdaadf5cb0db618d5a8606b13b2885f29a36fa9158e1f62332b7e3`
   Evidence: jansu-storage/src/service.rs:1630, future-hostile/dead-language term `todo` appears
137. `high` `vibe` `jansu-storage/src/service.rs:1646`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:502cccfa5fa3945aeefd9d4f367a542ef747b2a1af742c04a08543909183553c`
   Evidence: jansu-storage/src/service.rs:1646, future-hostile/dead-language term `todo` appears
138. `high` `vibe` `jansu-storage/src/service.rs:1654`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:3a786d0dd8cd5c78f482eb763408d49c9ad5ed9cf7374ab3f78c5b123a543f90`
   Evidence: jansu-storage/src/service.rs:1654, future-hostile/dead-language term `todo` appears
139. `high` `vibe` `jansu-storage/src/service.rs:1663`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:555fcaa1a0c112da08ad69faf558025b9466600f8b67df22317292cc14bed9eb`
   Evidence: jansu-storage/src/service.rs:1663, future-hostile/dead-language term `todo` appears
140. `high` `vibe` `jansu-storage/src/service.rs:1671`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:b2bae1c12fa1c07f459f0aebd8b2eabe59eedb3b6ba4bf692b35796de6166621`
   Evidence: jansu-storage/src/service.rs:1671, future-hostile/dead-language term `todo` appears
141. `high` `vibe` `jansu-storage/src/service.rs:1680`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:b4a54cf2fcc631c826e773d3c6d98462e1c41a12b94001cf5a49ba1064870feb`
   Evidence: jansu-storage/src/service.rs:1680, future-hostile/dead-language term `todo` appears
142. `high` `vibe` `jansu-storage/src/service.rs:1689`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:06119403f333def66e170f799fb725ce7cfae07ea0699f06cd9dbcd885437050`
   Evidence: jansu-storage/src/service.rs:1689, future-hostile/dead-language term `todo` appears
143. `high` `vibe` `jansu-storage/src/service.rs:1696`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:4668930481e3f5a87849fc5c30f8d8cc3884a5840db6e2bc0d09410fc21d1244`
   Evidence: jansu-storage/src/service.rs:1696, future-hostile/dead-language term `todo` appears
144. `high` `vibe` `jansu-storage/src/service.rs:1700`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:69e4acb93d11b9fce1f9657e0cc72f1fbbc15cb5df7ff655d6828298704f7f4c`
   Evidence: jansu-storage/src/service.rs:1700, future-hostile/dead-language term `todo` appears
145. `high` `vibe` `jansu-storage/src/service.rs:1709`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:c7c037ad6133402aeced34c575fe9e49567cc415cc5071836f0c59bacde85811`
   Evidence: jansu-storage/src/service.rs:1709, future-hostile/dead-language term `todo` appears
146. `high` `vibe` `jansu-storage/src/service.rs:1717`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:6d6434ec687ac10a87a1c0001c05e98bc96e579dfd42fbffc675933636df600b`
   Evidence: jansu-storage/src/service.rs:1717, future-hostile/dead-language term `todo` appears
147. `high` `vibe` `jansu-storage/src/service.rs:1725`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:15303e1c14cb8b42a985a46a195c06ff7c9b7d9fe22c9def75f248848e4c5f77`
   Evidence: jansu-storage/src/service.rs:1725, future-hostile/dead-language term `todo` appears
148. `high` `vibe` `jansu-storage/src/service.rs:1734`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:a043db49b2ac538322239b0b43fbcd79a9433d794d92e5cb7c235cf171100a55`
   Evidence: jansu-storage/src/service.rs:1734, future-hostile/dead-language term `todo` appears
149. `high` `vibe` `jansu-storage/src/service.rs:1738`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:7ea897dd474948cb3c0d403ec567e10624e5925771a570d26bcef350a64dc797`
   Evidence: jansu-storage/src/service.rs:1738, future-hostile/dead-language term `todo` appears
150. `high` `vibe` `jansu-storage/src/service.rs:1745`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:d9a01eed4940f5fe168c01798b7603b7ee895b547f745e56ce7c48ecaf06a432`
   Evidence: jansu-storage/src/service.rs:1745, future-hostile/dead-language term `todo` appears
151. `high` `vibe` `jansu-storage/src/service.rs:1753`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:827dbf24b7108f6d56494207d35144d67d79b88f7de4256bcbfcc2f99bfa485d`
   Evidence: jansu-storage/src/service.rs:1753, future-hostile/dead-language term `todo` appears
152. `high` `vibe` `jansu-storage/src/service.rs:1762`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:9c8704571a75517260ea33e49bc1f55ea733aa5e107b62569ea433c9a06c95c4`
   Evidence: jansu-storage/src/service.rs:1762, future-hostile/dead-language term `todo` appears
153. `high` `vibe` `jansu-storage/src/service.rs:1771`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:7c03772c5893c658bf7a43ffedd3b23fad4724320e63cdfd2a19f90c8942be71`
   Evidence: jansu-storage/src/service.rs:1771, future-hostile/dead-language term `todo` appears
154. `high` `vibe` `jansu-storage/src/service.rs:1781`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:5277e55c789885c1e10dec425c483a41829c2097a7998cd2fd82fc5d120a4410`
   Evidence: jansu-storage/src/service.rs:1781, future-hostile/dead-language term `todo` appears
155. `high` `vibe` `jansu-storage/src/service.rs:1791`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:bbdcc5c4e44fcd5a06c08004ef72b1f181d9ac997d2aad6e11dbeb0d25090658`
   Evidence: jansu-storage/src/service.rs:1791, future-hostile/dead-language term `todo` appears
156. `high` `vibe` `jansu-storage/src/service.rs:1798`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:f31e0ee440d05a874857b14f84e3afe42186dff464845614688f8ad74bfb0461`
   Evidence: jansu-storage/src/service.rs:1798, future-hostile/dead-language term `todo` appears
157. `high` `vibe` `jansu-storage/src/service.rs:1805`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:34207f5875010d8af590d94a3167e22b8f909c914f2aeb19b4299bc8883ed310`
   Evidence: jansu-storage/src/service.rs:1805, future-hostile/dead-language term `todo` appears
158. `high` `vibe` `jansu-storage/src/service.rs:1815`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:b4025c9fa2dd5eebb9fd7b555a86c1e0e5cd9ea4a0d7ca855ace746893304c7e`
   Evidence: jansu-storage/src/service.rs:1815, future-hostile/dead-language term `todo` appears
159. `high` `vibe` `jansu-storage/src/service.rs:1819`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:f5aacc5364721e4ca44ac444e5c91cf06a4ff7bedc52dbff8a09efefa4667eb4`
   Evidence: jansu-storage/src/service.rs:1819, future-hostile/dead-language term `todo` appears
160. `high` `vibe` `jansu-storage/src/service.rs:1823`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:6a71c8217e8c1368ac19faa5514a2ad4af34db959344b271ca84174208bb76ea`
   Evidence: jansu-storage/src/service.rs:1823, future-hostile/dead-language term `todo` appears
161. `high` `vibe` `jansu-storage/src/service.rs:1827`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:51c19a0ce05775594faece08209fc2cd1aa9416842c0cbd8b80020f38ab4839f`
   Evidence: jansu-storage/src/service.rs:1827, future-hostile/dead-language term `todo` appears
162. `high` `vibe` `jansu-storage/src/service.rs:1831`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:caeb68e9654a6716f8367bc16bc9eaeb39fbb5f0c205ed21322d4d0a6eb23390`
   Evidence: jansu-storage/src/service.rs:1831, future-hostile/dead-language term `todo` appears
163. `high` `vibe` `jansu-storage/src/slate/engine.rs:324`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `old` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:495aa1ebd54ab7d4a099a8f538d3a87fbbe288e7900e4a78867906e6256d6495`
   Evidence: jansu-storage/src/slate/engine.rs:324, future-hostile/dead-language term `old` appears
164. `high` `vibe` `jansu-storage/src/slate/engine.rs:325`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `old` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:621f72dc5232a64ffe685dd3ba8388c58f1b35ef9eef0b657a8ab24d87f9d4b0`
   Evidence: jansu-storage/src/slate/engine.rs:325, future-hostile/dead-language term `old` appears
165. `high` `vibe` `jansu-storage/src/slate/engine.rs:339`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `old` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:02971bfe521dbe7a66e3d206b35b572dc1ae3a3183c9fe56ad64637b29a51e8b`
   Evidence: jansu-storage/src/slate/engine.rs:339, future-hostile/dead-language term `old` appears
166. `high` `vibe` `jansu-storage/src/slate/engine.rs:375`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `old` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:84b46f0f6634dfc96a3e5ca69508f412bb6fb4f0b3d82e0086e19a5d81654c17`
   Evidence: jansu-storage/src/slate/engine.rs:375, future-hostile/dead-language term `old` appears
167. `high` `vibe` `jansu-storage/src/slate/storage.rs:158`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:47868af9ecf79ee4cdb7d4f5fbf093d3c4558baa7b9f46bc8cf077647a57e0dc`
   Evidence: jansu-storage/src/slate/storage.rs:158, future-hostile/dead-language term `todo` appears
168. `high` `vibe` `jansu-storage/src/slate/storage.rs:698`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:db2de448769a11c283e810529e06812cb5ed8eed70461fa102ebb5f88513462e`
   Evidence: jansu-storage/src/slate/storage.rs:698, future-hostile/dead-language term `todo` appears
169. `high` `vibe` `jansu-storage/src/slate/storage.rs:791`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:5c771a0c2d4abda3381a842e89fab628605037ceb583a4a5f27b1e93006e79f9`
   Evidence: jansu-storage/src/slate/storage.rs:791, future-hostile/dead-language term `todo` appears
170. `high` `vibe` `jansu-storage/src/slate/storage.rs:1356`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:96c86e6115d2eb3520eca1928c273f598d98da738d21de2cbb4cc58402a35eea`
   Evidence: jansu-storage/src/slate/storage.rs:1356, future-hostile/dead-language term `todo` appears
171. `high` `vibe` `jansu-storage/src/slate/storage.rs:1431`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:ee29706d0b0f850ebb1d7d9432541c54d9f7660563d0f777b7ec96deb156f1af`
   Evidence: jansu-storage/src/slate/storage.rs:1431, future-hostile/dead-language term `todo` appears
172. `high` `vibe` `jansu-storage/src/slate/storage.rs:1520`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:db676be740429ab00346154d9e33821ca17402529b855810b85435ea9f4ecab1`
   Evidence: jansu-storage/src/slate/storage.rs:1520, future-hostile/dead-language term `todo` appears
173. `high` `vibe` `jansu-storage/src/slate/storage.rs:1610`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:96c96c5e12106880c2639222219d4a8678e9d4c4c1b4ccbebe412b4296ac673b`
   Evidence: jansu-storage/src/slate/storage.rs:1610, future-hostile/dead-language term `todo` appears
174. `high` `vibe` `jansu-storage/src/slate/storage.rs:1927`
   Rule: `HLT-001-DEAD-MARKER`
   Check: `HLT-001-DEAD-MARKER:vibe` `hard` confidence `0.88`
   Route: TLR `Entropy`, lane `fast`, owner `tools`
   Docs: `docs/audit-rubric.md#future-hostile-language-rule`
   Reason: future-hostile/dead-language term `todo` appears in product/runtime code
   Fix: remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Rerun: `just fast`
   Fingerprint: `sha256:c25f2320dcfb4fdbdd8c1434420e77eb21e055cac42e5ce08599d85dc589010c`
   Evidence: jansu-storage/src/slate/storage.rs:1927, future-hostile/dead-language term `todo` appears
175. `high` `data` `jansu-storage/src/sql/consumer_group_delete.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:b5cfd55c45fe01dc5c713716028d38c054198cd209129f96764b51d1167fc047`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from consumer_group
176. `high` `data` `jansu-storage/src/sql/consumer_group_detail_delete_by_cg.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:d45e618c6e1e38eb30c1868a28e9e6773c1951d3166825b40d1a3c213bb54e13`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from consumer_group_detail
177. `high` `data` `jansu-storage/src/sql/consumer_offset_delete_by_cg.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:03d29138964876f4a58cee06d10d1c6b37f83cca304d67cf4ddacb7d3a1d5814`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from consumer_offset
178. `high` `data` `jansu-storage/src/sql/consumer_offset_delete_by_topic.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:a6df48098e64565172f5291671db3c990a89f7c123d4e0b27563bcad96313c3c`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from consumer_offset
179. `high` `data` `jansu-storage/src/sql/consumer_offset_delete_expired.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:77a9666cc237e3cab3f23280df8487757cd77dc7dda80256225ddb372c8d13d6`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from consumer_offset
180. `high` `data` `jansu-storage/src/sql/header_delete_by_topic.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:e198e522cbef0ec41e3d12885415bc67ce616fd8e839943ccdf4215fad2acfd1`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from header
181. `high` `data` `jansu-storage/src/sql/policy_compact.sql:41`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:5e7f478668a60a4a36cbb7e8b35ddbb8209ab7a641095b66199f76746c838292`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from record
182. `high` `data` `jansu-storage/src/sql/policy_delete.sql:51`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:277052cba0dd24e943e72a470ff3eb8188d73577e24662da5a9282b4e422862b`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from record
183. `high` `data` `jansu-storage/src/sql/producer_detail_delete_by_topic.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:d98cdc7fdbf52744ad8b4b0839f12c016ce74e0b98015d3c371b8135627339c4`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from producer_detail
184. `high` `data` `jansu-storage/src/sql/record_delete_by_topic.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:569ece75279a2a431c947c24e1f1adf3913b7e4daf79a9f64cf09eee9afcdf14`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from record
185. `high` `data` `jansu-storage/src/sql/scram_credential_delete.sql:14`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:ff59f6329206b3cc255d831714ab4cfc1ef2ab8b24da57f6de4d447833cc0204`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from scram_credential
186. `high` `data` `jansu-storage/src/sql/topic_configuration_delete.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:3cd62a68482774f70deb5a135aae987d99913a1691c2ac7578daff5e1f83e42b`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from topic_configuration
187. `high` `data` `jansu-storage/src/sql/topic_configuration_delete_by_topic.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:7c4b9d1e45d087f200a9b208fac43740515419d23dc97c7dc658324afce1bd93`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from topic_configuration
188. `high` `data` `jansu-storage/src/sql/topic_delete_by.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:c59f81977050b41f65b316821ee8fc3977d94f41bbadae06ea3445f21b0e80ad`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from topic
189. `high` `data` `jansu-storage/src/sql/topition_delete_by_topic.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:bb4ddffee46d9e171a269b3c6c7de21fd13d11e00d856f75ccb9d8c8de6f12cd`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from topition
190. `high` `data` `jansu-storage/src/sql/txn_offset_commit_delete_by_txn.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:fe3adb6297868646de2d6898f00b95dbf1e584bf67d8d6a1376816eff5721667`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from txn_offset_commit
191. `high` `data` `jansu-storage/src/sql/txn_offset_commit_tp_delete_by_topic.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:4f7f4378ae7b0af7fe730ba74f37ab14915eb697aae772e591f1d868b2b4cbfe`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from txn_offset_commit_tp
192. `high` `data` `jansu-storage/src/sql/txn_offset_commit_tp_delete_by_txn.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:dd24affd16235ec6ef8e91c938827aca5e23a330c952c6b3c4d4d0e6e3080e2d`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from txn_offset_commit_tp
193. `high` `data` `jansu-storage/src/sql/txn_produce_offset_delete_by_topic.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:8d8181cd53f5838b45028b98618f06ba61a07c985bf57e7ae6d90389e9154799`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from txn_produce_offset
194. `high` `data` `jansu-storage/src/sql/txn_produce_offset_delete_by_txn.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:eaa2da354e511dfec1dab5543bdcbc6b0b2cb424549ce99963ac82466245f8d3`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from txn_produce_offset
195. `high` `data` `jansu-storage/src/sql/txn_topition_delete_by_topic.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:ad38f5fe2852ee74d5a06a5a3d30f79c1a2abe6b4683afd452dc73ba6387201b`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from txn_topition
196. `high` `data` `jansu-storage/src/sql/txn_topition_delete_by_txn.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:13df00a40f53eb882c753e15e76d8d9e297f5a63162877388c82d81d08302cba`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from txn_topition
197. `high` `data` `jansu-storage/src/sql/watermark_delete_by_topic.sql:16`
   Rule: `HLT-030-SQL-BAD-BEHAVIOR`
   Check: `HLT-030-SQL-BAD-BEHAVIOR:data` `hard` confidence `0.95`
   Route: TLR `Contracts/data`, lane `db`, owner `tools`
   Docs: `docs/BAD_SQL.md`
   Matched term: `update/delete`
   Reason: the statement reaches a whole-table write path without a row filter
   Fix: add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Rerun: `just fast`
   Fingerprint: `sha256:a20c0d99c81dbd8083ce042983869b9ad72e4aff855a551289ffc6a0244e5d6a`
   Evidence: detector=sql.query.full-table-write, proof-window=where-clause, snippet=delete from watermark

## Policy

- Policy file: `./agent/audit-policy.toml`
- Minimum score: `85`
- Fail on: `critical, high`

## Agent Fix Queue

1. `high` `HLT-022-AUTHZ-ISOLATION-GAP` `jansu-broker/src/broker.rs` - add owner/non-owner authorization tests or RLS evidence for the touched data boundary
   Route: `Business truth`/`db`
2. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/lite/policy_compact_delete.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
3. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/lite/policy_delete.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
4. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/consumer_group_delete.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
5. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/consumer_group_detail_delete_by_cg.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
6. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/consumer_offset_delete_by_cg.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
7. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/consumer_offset_delete_by_topic.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
8. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/consumer_offset_delete_expired.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
9. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/header_delete_by_topic.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
10. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/policy_compact.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
11. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/policy_delete.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
12. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/producer_detail_delete_by_topic.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
13. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/record_delete_by_topic.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
14. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/scram_credential_delete.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
15. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/topic_configuration_delete.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
16. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/topic_configuration_delete_by_topic.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
17. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/topic_delete_by.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
18. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/topition_delete_by_topic.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
19. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/txn_offset_commit_delete_by_txn.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
20. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/txn_offset_commit_tp_delete_by_topic.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
21. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/txn_offset_commit_tp_delete_by_txn.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
22. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/txn_produce_offset_delete_by_topic.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
23. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/txn_produce_offset_delete_by_txn.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
24. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/txn_topition_delete_by_topic.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
25. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/txn_topition_delete_by_txn.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
26. `high` `HLT-030-SQL-BAD-BEHAVIOR` `jansu-storage/src/sql/watermark_delete_by_topic.sql` - add a WHERE clause or prove the full-table rewrite with a local migration receipt
   Route: `Contracts/data`/`db`
27. `high` `HLT-042-CI-LOCAL-PARITY` `.github/workflows/ci.yml` - add ops/ci/lib.sh defining shared helpers and tool version pins
   Route: `Verification`/`fast`
28. `high` `HLT-042-CI-LOCAL-PARITY` `.github/workflows/ci.yml` - add ops/git-hooks/pre-push that runs `bash ops/ci/quality-gates.sh` and wire it via `git config core.hooksPath ops/git-hooks`
   Route: `Verification`/`fast`
29. `high` `HLT-042-CI-LOCAL-PARITY` `.github/workflows/ci.yml` - add scripts/ci-doctor.sh listing every tool the ops/ci scripts depend on
   Route: `Verification`/`fast`
30. `high` `HLT-042-CI-LOCAL-PARITY` `.github/workflows/ci.yml` - add scripts/ci-local.sh exposing each CI lane locally
   Route: `Verification`/`fast`
31. `high` `HLT-042-CI-LOCAL-PARITY` `.github/workflows/ci.yml` - extract the workflow steps into ops/ci/<lane>.sh and call them with `bash ops/ci/<lane>.sh`
   Route: `Verification`/`fast`
32. `high` `HLT-037-RELEASE-BAD-BEHAVIOR` `.github/workflows/ci.yml` - attach checksums plus SBOM/provenance/signature/attestation evidence to the release witness
   Route: `Verification`/`release`
33. `high` `HLT-042-CI-LOCAL-PARITY` `.github/workflows/differential-kafka-lab.yml` - extract the workflow steps into ops/ci/<lane>.sh and call them with `bash ops/ci/<lane>.sh`
   Route: `Verification`/`fast`
34. `high` `HLT-042-CI-LOCAL-PARITY` `.github/workflows/jankurai.yml` - extract the workflow steps into ops/ci/<lane>.sh and call them with `bash ops/ci/<lane>.sh`
   Route: `Verification`/`fast`
35. `high` `HLT-042-CI-LOCAL-PARITY` `.github/workflows/release.yml` - extract the workflow steps into ops/ci/<lane>.sh and call them with `bash ops/ci/<lane>.sh`
   Route: `Verification`/`fast`
36. `high` `HLT-004-UNMAPPED-PROOF` `agent/test-map.json` - add the narrowest stable prefix and runnable proof command to `agent/test-map.json`
   Route: `Verification`/`fast`
37. `high` `HLT-025-RELEASE-READINESS-GAP` `docs/testing.md` - add launch-gate evidence for security, backups, monitoring, rollback, and abuse controls
   Route: `Verification`/`release`
38. `medium` `HLT-018-PERF-CONCURRENCY-DRIFT` `Justfile` - add fast deterministic build/test targets, caches, and narrow proof lanes for agent iteration
   Route: `Verification`/`fast`
39. `medium` `HLT-026-COST-BUDGET-GAP` `docs/testing.md` - add explicit budgets, quotas, stop conditions, and kill-switch evidence for paid or unbounded operations
   Route: `Verification`/`release`
40. `medium` `HLT-027-HUMAN-REVIEW-EVIDENCE-GAP` `agent/repo-score.json` - attach raw CI logs, review receipts, and replayable commands instead of accepting claims or summaries
   Route: `Repair`/`audit`
41. `high` `HLT-003-OWNERLESS-PATH` `agent/owner-map.json` - add the narrowest stable prefix for this path to `agent/owner-map.json`
   Route: `Context/setup`/`fast`
42. `medium` `HLT-003-OWNERLESS-PATH` `agent/owner-map.json` - tighten owner/test maps and root routing until agents can localize ownership without inference
   Route: `Context/setup`/`fast`
43. `critical` `HLT-010-SECRET-SPRAWL` `jansu-cli/src/cli/user.rs` - remove and rotate the credential, add local and CI secret scanning, and scan transcripts/artifacts/MCP config for related exposure
   Route: `Security, secrets, agency`/`security`
44. `high` `HLT-034-CI-BAD-BEHAVIOR` `.github/workflows/ci.yml` - add workflow-level concurrency with cancel-in-progress
   Route: `Security, secrets, agency`/`security`
45. `high` `HLT-034-CI-BAD-BEHAVIOR` `.github/workflows/ci.yml` - set an explicit timeout-minutes on each job
   Route: `Security, secrets, agency`/`security`
46. `high` `HLT-034-CI-BAD-BEHAVIOR` `.github/workflows/ci.yml` - pin every external action to a 40-character commit SHA
   Route: `Security, secrets, agency`/`security`
47. `high` `HLT-034-CI-BAD-BEHAVIOR` `.github/workflows/differential-kafka-lab.yml` - add workflow-level concurrency with cancel-in-progress
   Route: `Security, secrets, agency`/`security`
48. `high` `HLT-034-CI-BAD-BEHAVIOR` `.github/workflows/differential-kafka-lab.yml` - add top-level `permissions: contents: read` and job-specific write scopes only where needed
   Route: `Security, secrets, agency`/`security`
49. `high` `HLT-034-CI-BAD-BEHAVIOR` `.github/workflows/differential-kafka-lab.yml` - pin every external action to a 40-character commit SHA
   Route: `Security, secrets, agency`/`security`
50. `high` `HLT-034-CI-BAD-BEHAVIOR` `.github/workflows/jankurai.yml` - add workflow-level concurrency with cancel-in-progress
   Route: `Security, secrets, agency`/`security`
51. `high` `HLT-034-CI-BAD-BEHAVIOR` `.github/workflows/jankurai.yml` - set an explicit timeout-minutes on each job
   Route: `Security, secrets, agency`/`security`
52. `high` `HLT-034-CI-BAD-BEHAVIOR` `.github/workflows/jankurai.yml` - pin every external action to a 40-character commit SHA
   Route: `Security, secrets, agency`/`security`
53. `high` `HLT-034-CI-BAD-BEHAVIOR` `.github/workflows/jankurai.yml` - upload the SARIF artifact with github/codeql-action/upload-sarif pinned to a full SHA
   Route: `Security, secrets, agency`/`security`
54. `high` `HLT-034-CI-BAD-BEHAVIOR` `.github/workflows/jankurai.yml` - remove the non-blocking override so scan failures stop the pipeline
   Route: `Security, secrets, agency`/`security`
55. `high` `HLT-034-CI-BAD-BEHAVIOR` `.github/workflows/release.yml` - add workflow-level concurrency with cancel-in-progress
   Route: `Security, secrets, agency`/`security`
56. `high` `HLT-034-CI-BAD-BEHAVIOR` `.github/workflows/release.yml` - set an explicit timeout-minutes on each job
   Route: `Security, secrets, agency`/`security`
57. `high` `HLT-034-CI-BAD-BEHAVIOR` `.github/workflows/release.yml` - pin every external action to a 40-character commit SHA
   Route: `Security, secrets, agency`/`security`
58. `high` `HLT-024-AGENT-TOOL-SUPPLY-GAP` `agent/agent-tool-supply-evidence.md` - pin and review agent tools, MCP servers, hooks, and rule files; keep untrusted tool output separate from trusted policy
   Route: `Security, secrets, agency`/`security`
59. `high` `HLT-032-DOCKER-BAD-BEHAVIOR` `compose.yaml` - bind the port to localhost or keep it on an internal-only network
   Route: `Security, secrets, agency`/`security`
60. `high` `HLT-001-DEAD-MARKER` `jansu-auth/src/lib.rs` - collapse fallback chains into explicit typed states with bounded retry policy, telemetry, and documented repair guidance
   Route: `Entropy`/`fast`
61. `high` `HLT-001-DEAD-MARKER` `jansu-cli/src/cli/perf.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
62. `high` `HLT-001-DEAD-MARKER` `jansu-cli/src/cli/perf.rs` - replace placeholders with implemented behavior, typed unsupported-state errors, or a tracked exception record with docs
   Route: `Entropy`/`fast`
63. `high` `HLT-029-RUST-BAD-BEHAVIOR` `jansu-cli/src/cli/user.rs` - construct the type with a valid initializer instead of zeroing it
   Route: `Security, secrets, agency`/`fast`
64. `high` `HLT-023-INPUT-BOUNDARY-GAP` `jansu-schema/src/lake/delta.rs` - replace unsafe sinks with typed schemas, parameterized APIs, allowlists, or sandboxed execution plus negative tests
   Route: `Security, secrets, agency`/`security`
65. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/dynostore/batch.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
66. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/dynostore/describe.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
67. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/dynostore/features.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
68. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/dynostore/mod.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
69. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/limbo/engine.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
70. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/limbo/storage_broker.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
71. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/limbo/storage_end.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
72. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/limbo/storage_txn.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
73. `high` `HLT-043-COPY-PASTE-BAD-BEHAVIOR` `jansu-storage/src/limbo/tests.rs` - keep the named unit in one owner and call it from the other sites
   Route: `Maintainability entropy`/`copy-code`
74. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/lite/delegate_compaction.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
75. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/lite/storage_admin.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
76. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/lite/storage_producer.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
77. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/lite/storage_transactions.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
78. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/service.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
79. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/slate/engine.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
80. `high` `HLT-001-DEAD-MARKER` `jansu-storage/src/slate/storage.rs` - remove or rename the marker, implement the intended behavior, model a typed unsupported state, or move docs/generated/vendor/product-copy text into an allowlisted context
   Route: `Entropy`/`fast`
81. `medium` `HLT-001-DEAD-MARKER` `.` - split large or ambiguous authored code into smaller semantic modules with focused tests
   Route: `Entropy`/`fast`
82. `medium` `HLT-016-SUPPLY-CHAIN-DRIFT` `.github/workflows/jankurai.yml` - wire secret, dependency, provenance, and workflow scans into an operational CI lane
   Route: `Security, secrets, agency`/`security`
