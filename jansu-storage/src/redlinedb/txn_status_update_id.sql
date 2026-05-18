update txn_detail

set

status = $1,
last_updated = $2

where id = $3;
