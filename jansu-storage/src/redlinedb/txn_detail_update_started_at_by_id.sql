update txn_detail

set

started_at = $1,
status = 'BEGIN'

where id = $2;
