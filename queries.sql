-- name: users_all
select
    *
from
    users;

-- name: users_first
select
    *
from
    users
order by
    id desc
limit 1;

