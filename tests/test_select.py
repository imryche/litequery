from dataclasses import dataclass
from datetime import datetime


@dataclass
class User:
    id: int
    name: str
    email: str
    created_at: datetime


def test_select(lq):
    users = lq.get_all_users()

    assert len(users) == 2
    assert users[0].name == "Alice"
    assert isinstance(users[0].created_at, datetime)
    assert users[1].name == "Bob"
    assert isinstance(users[1].created_at, datetime)


def test_select_one(lq):
    user = lq.get_user_by_id(id=1)
    assert user.email == "alice@example.com"


def test_select_one_into(lq):
    user = lq.get_user_by_id(id=1).into(User)
    assert isinstance(user, User)
    assert user.name == "Alice"


def test_select_into(lq):
    users = lq.get_all_users().into(User)
    assert isinstance(users[0], User)
    assert users[0].name == "Alice"


def test_select_value(lq):
    user_id = lq.get_last_user_id()
    assert user_id == 2
