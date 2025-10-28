def test_insert_returning(lq):
    user_id = lq.insert_user(name="Dave", email="dave@example.com")
    assert user_id == 4

    user = lq.get_user_by_id(id=user_id)
    assert user.name == "Dave"
    assert user.email == "dave@example.com"


def test_delete(lq):
    users = lq.get_all_users()
    assert len(users) == 3

    rows_deleted = lq.delete_all_users()
    assert rows_deleted == 3

    users = lq.get_all_users()
    assert len(users) == 0
