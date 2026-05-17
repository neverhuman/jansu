-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ 2024-2026 Peter Morgan <peter.morgan@jansu.io>
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

select
r.topition,
r.offset_id,
r.timestamp,
retention.value

from record r
join topition tp on tp.id = r.topition
join topic t on t.id = tp.topic
join cluster c on c.id = t.cluster
join topic_configuration cleanup on cleanup.topic = t.id
left join topic_configuration retention on retention.topic = t.id
    and retention.name = 'retention.ms'

where c.name = $1
and cleanup.name = 'cleanup.policy'
and cleanup.value like '%delete%'
