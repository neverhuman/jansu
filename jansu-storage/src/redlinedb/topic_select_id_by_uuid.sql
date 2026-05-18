select t.id, t.name
from cluster c
join topic t on t.cluster = c.id
where c.name = $1
and t.uuid = $2;
