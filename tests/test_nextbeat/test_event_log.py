from nextbeat.event_log import Event, EventLog, Timestamp


def test_append_event() -> None:
    log = EventLog()
    log.append_job_event("A", "waiting")
    actual = list(log.events(0, 1))
    expected = [Event(0, "A", "waiting")]
    assert expected == actual

    log.append_job_event("B", "waiting")
    actual = list(log.events(0, 2))
    assert len(actual) == 2

    actual = list(log.events(1, 2))
    expected = [Event(1, "B", "waiting")]
    assert expected == actual


def test_events_and_state() -> None:
    log = EventLog()
    events = [
        Event(0, "A", "waiting"),
        Event(1, "B", "waiting"),
        Event(2, "B", "running"),
        Event(3, "B", "succeeded"),
    ]
    for event in events:
        log.append_job_event(event.name, event.payload)

    actual = list(log.events_and_state("A", 0, 1))
    assert events[0:1] == actual

    actual = list(log.events_and_state("B", 0, 2))
    assert events[1:2] == actual

    actual = list(log.events_and_state("A", 1, 2))
    assert events[0:1] == actual


def test_subscribers() -> None:
    log = EventLog()
    called = False

    def call(low: Timestamp, high: Timestamp) -> None:
        nonlocal called
        called = True
        assert low == 1
        assert high == 2

    log.subscribe(["A"], call)

    log.append_job_event("B", "waiting")
    assert log.all_subscriptions_executed()

    log.execute_subscriptions()
    assert not log.all_subscriptions_executed()
    assert called == False

    log.append_job_event("A", "waiting")
    log.execute_subscriptions()
    assert called == True