select t.uuid, t.name, is_internal, partitions, replication_factor

from

cluster c
join topic t on t.cluster = c.id

where

c.name = $1
and t.uuid = $2;
