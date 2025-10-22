def test_foreign_keys_enabled(lq):
    events = lq.get_all_events()
    assert len(events) == 2

    lq.delete_all_users()

    events = lq.get_all_events()
    assert len(events) == 0


def test_pragmas_configured(lq):
    expected_pragmas = [
        ("journal_mode", "wal"),
        ("foreign_keys", 1),
        ("synchronous", 1),
        ("mmap_size", 134217728),  # 128 Mb
        ("journal_size_limit", 67108864),  # 64 Mb
        ("cache_size", 2000),
        ("busy_timeout", 30000),
    ]

    for pragma, expected_value in expected_pragmas:
        value = lq.raw_value(f"pragma {pragma}")
        assert value == expected_value
