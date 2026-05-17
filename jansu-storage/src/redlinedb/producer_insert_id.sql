insert into producer (id, cluster)

select $2, c.id

from cluster c

where c.name = $1;
