from datetime import UTC, datetime, timedelta, timezone


def test_naive_datetime_roundtrip(lq):
    dt = datetime(2025, 10, 24, 9, 30, 0, 0)
    row = lq.raw_one("select :dt as 'dt [datetime]'", dt=dt)

    assert isinstance(row.dt, datetime)
    assert row.dt.tzinfo is None
    assert row.dt == dt


def test_aware_datetime_converts_to_utc_naive(lq):
    tz = timezone(timedelta(hours=5))
    dt = datetime(2024, 1, 2, 10, 0, 0, tzinfo=tz)
    row = lq.raw_one("select :dt as 'dt [datetime]'", dt=dt)

    dt_normalized = dt.astimezone(UTC).replace(tzinfo=None)
    assert row.dt.tzinfo is None
    assert row.dt == dt_normalized


def test_datetime_column_is_naive(lq):
    user = lq.get_user_by_id(id=1)

    assert isinstance(user.created_at, datetime)
    assert user.created_at.tzinfo is None
