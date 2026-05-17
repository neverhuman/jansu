select txn_oc.id

from cluster c
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d."transaction" = txn.id and txn_d.producer_epoch = pe.id
join txn_offset_commit txn_oc on txn_oc.txn_detail = txn_d.id

where

c.name = $1
and txn.name = $2
and p.id = $3
and pe.epoch = $4;
