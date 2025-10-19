import pytest


def test_transaction_rollback(lq):
    with pytest.raises(Exception):
        with lq.transaction():
            lq.insert_user(name="Charlie", email="charlie@example.com")
            raise Exception("Force rollback")
            lq.insert_user(name="Eve", email="eve@example.com")

    users = lq.get_all_users()
    assert len(users) == 2
