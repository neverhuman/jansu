select cg.id
from cluster c
join consumer_group cg on cg.cluster = c.id
where c.name = $1
and cg.name = $2;
