import asyncio
import time

from transponder.health import ErrorKind, Health, Notification


class CollectingNotifier:
    name = "collect"

    def __init__(self):
        self.sent: list[Notification] = []

    async def send(self, client, n):
        self.sent.append(n)


async def deliver(health: Health) -> None:
    task = asyncio.create_task(health.run())
    await health.stop()
    await task


async def test_transient_failures_need_threshold():
    n = CollectingNotifier()
    h = Health([n], min_consecutive_failures=3, hostname="host")
    h.report_failure("rt:a:vp", ErrorKind.NETWORK, "timeout")
    h.report_failure("rt:a:vp", ErrorKind.NETWORK, "timeout")
    await deliver(h)
    assert n.sent == []
    h.report_failure("rt:a:vp", ErrorKind.NETWORK, "timeout again")
    await deliver(h)
    assert len(n.sent) == 1
    assert n.sent[0].title == "[host] rt:a:vp: network"
    assert n.sent[0].message == "timeout again"
    assert n.sent[0].priority == 0


async def test_non_transient_alerts_immediately_with_priority():
    n = CollectingNotifier()
    h = Health([n], hostname="host")
    h.report_failure("keys:a", ErrorKind.ALL_KEYS_EXHAUSTED, "all gone")
    await deliver(h)
    assert [x.priority for x in n.sent] == [1]


async def test_recovery_and_repeat():
    n = CollectingNotifier()
    h = Health([n], repeat_after=100, hostname="host")
    h.report_failure("db", ErrorKind.DB_UNAVAILABLE, "down")
    h.report_failure("db", ErrorKind.DB_UNAVAILABLE, "still down")
    await deliver(h)
    assert len(n.sent) == 1  # de-duplicated

    # Fake the clock past repeat_after: the ongoing issue is re-sent once.
    issue = h.active_issues()[0]
    issue.notified_at -= 101
    h.report_failure("db", ErrorKind.DB_UNAVAILABLE, "still down")
    await deliver(h)
    assert len(n.sent) == 2 and "(still)" in n.sent[1].title and "Ongoing for" in n.sent[1].message

    h.report_ok("db")
    await deliver(h)
    assert len(n.sent) == 3
    assert n.sent[2].title == "[host] db: recovered from db unavailable"
    assert n.sent[2].priority == -1
    assert h.active_issues() == []

    # Recovery is only announced for issues that were actually notified.
    h.report_failure("rt:x", ErrorKind.NETWORK, "blip")
    h.report_ok("rt:x")
    await deliver(h)
    assert len(n.sent) == 3


async def test_condition_survives_report_ok():
    n = CollectingNotifier()
    h = Health([n], hostname="host")
    h.set_condition("rt:a:vp", ErrorKind.FEED_STALE, True, "stale")
    h.report_ok("rt:a:vp")  # the poll succeeded, but the data is still stale
    assert [i.kind for i in h.active_issues()] == [ErrorKind.FEED_STALE]
    h.set_condition("rt:a:vp", ErrorKind.FEED_STALE, False)
    await deliver(h)
    assert [x.title for x in n.sent] == ["[host] rt:a:vp: feed stale", "[host] rt:a:vp: recovered from feed stale"]


async def test_notice_is_throttled():
    n = CollectingNotifier()
    h = Health([n], repeat_after=1000, hostname="host")
    h.notice("keys:a:K1", ErrorKind.QUOTA_EXHAUSTED, "benched")
    h.notice("keys:a:K1", ErrorKind.QUOTA_EXHAUSTED, "benched again")
    h.notice("keys:a:K2", ErrorKind.QUOTA_EXHAUSTED, "other key")
    await deliver(h)
    assert [x.message for x in n.sent] == ["benched", "other key"]


async def test_failed_notifier_does_not_break_delivery():
    class Broken:
        name = "broken"

        async def send(self, client, n):
            raise RuntimeError("boom")

    good = CollectingNotifier()
    h = Health([Broken(), good], hostname="host")
    h.notify("hello", "world")
    await deliver(h)
    assert len(good.sent) == 1 and h.sent["collect"] == 1 and h.sent["broken"] == 0


async def test_no_notifiers_is_log_only():
    h = Health([], hostname="host")
    h.report_failure("db", ErrorKind.DB_UNAVAILABLE, "down")
    h.notify("x", "y")
    assert h._queue.empty()
