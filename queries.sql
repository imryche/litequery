-- name: users_all
select
    *
from
    users;

-- name: users_first^
select
    *
from
    users
order by
    id desc
limit 1;

-- name: users_insert<!
insert into users (name)
    values (:name);

-- name: users_delete_all
delete from users;

