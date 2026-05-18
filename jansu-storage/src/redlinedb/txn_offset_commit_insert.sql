insert into txn_offset_commit
(txn_detail, consumer_group, generation_id, member_id)

select
txn_d.id,
cg.id,
$1,
$2

from

cluster c
join consumer_group cg on cg.cluster = c.id
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d."transaction" = txn.id and txn_d.producer_epoch = pe.id

where

c.name = $3
and txn.name = $4
and cg.name = $5
and p.id = $6
and pe.epoch = $7;
