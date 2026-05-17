update txn_detail

set

started_at = $1,
status = 'BEGIN'

where txn_detail.id in (

select txn_d.id

from

cluster c
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d."transaction" = txn.id and txn_d.producer_epoch = pe.id

where

c.name = $2
and txn.name = $3
and p.id = $4
and pe.epoch = $5
and txn_d.started_at is null
and txn_d.status is null

);
