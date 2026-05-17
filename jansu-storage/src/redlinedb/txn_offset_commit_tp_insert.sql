insert into txn_offset_commit_tp
(offset_commit, topition, committed_offset, leader_epoch, metadata)

select

oc.id,
tp.id,
$1,
$2,
$3

from

cluster c
join consumer_group cg on cg.cluster = c.id
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d."transaction" = txn.id and txn_d.producer_epoch = pe.id
join txn_offset_commit oc on oc.txn_detail = txn_d.id and oc.consumer_group = cg.id

where

c.name = $4
and txn.name = $5
and cg.name = $6
and p.id = $7
and pe.epoch = $8
and t.name = $9
and tp.partition = $10;
