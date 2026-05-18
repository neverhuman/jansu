insert into producer_epoch (producer, epoch)

select

p.id,
coalesce(max(pe.epoch) + 1, 0) as epoch

from

cluster c
join producer p on p.cluster = c.id
left join producer_epoch pe on pe.producer = p.id

where

c.name = $1
and p.id = $2

group by p.id

returning epoch;
