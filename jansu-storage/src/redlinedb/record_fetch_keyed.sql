select

r.offset_id,
r.attributes,
r.timestamp,
r.k,
r.v,
coalesce(length(r.k), 0) + coalesce(length(r.v), 0) as bytes,
r.producer_id,
r.producer_epoch

from

cluster c
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join record r on r.topition = tp.id

where

c.name = $1
and t.name = $2
and tp.partition = $3
and r.offset_id >= $4
and r.offset_id < $6
and r.k = $7
and coalesce(length(r.k), 0) + coalesce(length(r.v), 0) < $5

order by r.offset_id;
