-- name: get_all_users
select
    *
from
    users;

-- name: get_user_by_id^
select
    *
from
    users
where
    id = :id;

-- name: get_last_user_id$
select
    id
from
    users
order by
    id desc;

-- name: insert_user<!
insert into users (name, email)
    values (:name, :email);

-- name: delete_all_users!
delete from users;

