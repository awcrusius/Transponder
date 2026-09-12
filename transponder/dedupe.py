"""Drop re-observations before they reach the writer.

Agencies republish their whole trip-update snapshot every poll with a fresh
header timestamp, so 80-90 % of stop_time_updates / trip_updates rows repeat
the previous poll byte for byte and the unique index never catches them. The
`Deduper` remembers, per observed thing (see `TableSpec.identity`), a hash of
the last row it let through and when. A row identical to that one within the
window is dropped; a changed row, a new thing, or one whose last write is older
than the window goes through. So every distinct value is written the first time
it is seen and an unchanged value is re-asserted once per window (a heartbeat,
so "still like this" and "no longer reported" stay distinguishable).

A stored row's `time` therefore means "observed like this at this poll, and
unchanged since the previous row for the same thing". The state at instant T is
the latest row at or before T per thing.

Cost: one dict lookup and one tuple hash per row, and about 100 bytes of memory
per thing currently being reported. Each Poller owns one Deduper and calls it
from its decode thread, so no locking is needed.
"""

from __future__ import annotations

import dataclasses
from collections import Counter
from collections.abc import Callable
from typing import TYPE_CHECKING

from transponder.tables import RT_TABLES, TableSpec

if TYPE_CHECKING:
    from transponder.rt import Batch

_Key = tuple
_Row = tuple


def _key_fn(spec: TableSpec) -> Callable[[_Row], _Key]:
    parts: list[tuple[int, ...]] = []
    for ident in spec.identity:
        names = (ident,) if isinstance(ident, str) else ident
        parts.append(tuple(spec.columns.index(n) for n in names))

    def key(row: _Row) -> _Key:
        return tuple(next((row[i] for i in idxs if row[i] is not None), None) for idxs in parts)

    return key


def _payload_fn(spec: TableSpec) -> Callable[[_Row], int]:
    skip = set(spec.observed_at)
    for ident in spec.identity:
        skip.update((ident,) if isinstance(ident, str) else ident)
    idxs = tuple(i for i, c in enumerate(spec.columns) if c not in skip)

    def payload(row: _Row) -> int:
        return hash(tuple(row[i] for i in idxs))

    return payload


class Deduper:
    def __init__(self, window: float = 3600.0) -> None:
        self.window = window
        self.suppressed: Counter[str] = Counter()
        self._seen: dict[str, dict[_Key, tuple[int, float]]] = {}
        self._fns: dict[str, tuple[Callable[[_Row], _Key], Callable[[_Row], int]]] = {}
        self._last_sweep: float = 0.0

    def filter(self, batch: "Batch", now: float) -> "Batch":
        """Return `batch` without rows identical to one written in the last `window` seconds.

        `now` is a POSIX timestamp (the fetch time); the window is measured in
        wall-clock time rather than feed time so a feed with a frozen timestamp
        cannot suppress itself forever.
        """
        spec = RT_TABLES.get(batch.table)
        if spec is None or not spec.identity or self.window <= 0:
            return batch
        if batch.table not in self._fns:
            self._fns[batch.table] = (_key_fn(spec), _payload_fn(spec))
        key_of, payload_of = self._fns[batch.table]
        seen = self._seen.setdefault(batch.table, {})
        cutoff = now - self.window

        kept: list[_Row] = []
        for row in batch.rows:
            key = key_of(row)
            digest = payload_of(row)
            last = seen.get(key)
            if last is not None and last[0] == digest and last[1] > cutoff:
                continue
            seen[key] = (digest, now)
            kept.append(row)
        dropped = len(batch.rows) - len(kept)
        if dropped:
            self.suppressed[batch.table] += dropped

        if now - self._last_sweep > self.window / 4:
            self._sweep(cutoff)
            self._last_sweep = now
        return batch if not dropped else dataclasses.replace(batch, rows=kept)

    def _sweep(self, cutoff: float) -> None:
        # Anything written before the cutoff would be re-written on its next
        # sighting anyway, so forgetting it changes nothing.
        for seen in self._seen.values():
            stale = [k for k, (_, written) in seen.items() if written <= cutoff]
            for k in stale:
                del seen[k]

    def size(self) -> int:
        return sum(len(s) for s in self._seen.values())
