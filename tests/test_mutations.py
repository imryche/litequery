def test_insert_returning(lq):
    user_id = lq.insert_user(name="Eve", email="eve@example.com")
    assert user_id == 3

    user = lq.get_user_by_id(id=user_id)
    assert user.name == "Eve"
    assert user.email == "eve@example.com"


def test_delete(lq):
    users = lq.get_all_users()
    assert len(users) == 2

    rows_deleted = lq.delete_all_users()
    assert rows_deleted == 2

    users = lq.get_all_users()
    assert len(users) == 0
