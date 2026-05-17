select r.offset_id, r.timestamp

from

cluster c
join topic t on t.cluster = c.id
join topition tp on tp.topic = t.id
join txn on txn.cluster = c.id
join txn_detail txn_d on txn_d."transaction" = txn.id
join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id and txn_tp.topition = tp.id
join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id
join record r on r.topition = tp.id and r.offset_id = txn_po.offset_start

where

c.name = $1
and t.name = $2
and tp.partition = $3
and (txn_d.status = 'PREPARE_COMMIT' or txn_d.status = 'PREPARE_ABORT' or txn_d.status = 'BEGIN')

order by r.offset_id asc
limit 1;
