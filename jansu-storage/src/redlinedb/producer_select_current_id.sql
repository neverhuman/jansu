select p.id

from

cluster c
join producer p on p.cluster = c.id

where c.name = $1

order by p.id desc

limit 1;
