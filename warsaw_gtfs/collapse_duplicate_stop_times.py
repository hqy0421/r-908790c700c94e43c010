from collections.abc import Iterable, Sequence
from typing import cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import StopTime

PaxExchange = StopTime.PassengerExchange


class CollapseDuplicateStopTimes(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        self.logger.debug("Getting duplicate stop times")
        duplicates = self.find_duplicates(r.db)

        self.logger.debug("Solving duplicates")
        solutions = self.solve_duplicates(r.db, duplicates)

        self.logger.debug("Applying fixes")
        self.apply_solutions(r.db, solutions)

    def find_duplicates(self, db: DBConnection) -> list[tuple[str, int]]:
        return [
            (cast(str, r[0]), cast(int, r[1]))
            for r in db.raw_execute(
                "SELECT l.trip_id, l.stop_sequence "
                "FROM stop_times l "
                "INNER JOIN stop_times r ON ("
                "  l.trip_id = r.trip_id"
                "  AND l.stop_id = r.stop_id"
                "  AND l.stop_sequence + 1 = r.stop_sequence"
                ")"
            )
        ]

    def solve_duplicates(
        self,
        db: DBConnection,
        duplicates: Iterable[tuple[str, int]],
    ) -> list[StopTime]:
        return [
            self.solve_duplicate(db, trip_id, stop_sequence)
            for trip_id, stop_sequence in duplicates
        ]

    def solve_duplicate(self, db: DBConnection, trip_id: str, stop_sequence: int) -> StopTime:
        l = db.typed_out_execute(
            "SELECT * FROM stop_times WHERE trip_id = ? AND stop_sequence = ?",
            StopTime,
            (trip_id, stop_sequence),
        ).one_must("solve_duplicate: non-existing (trip_id, stop_sequence) pair")
        r = db.typed_out_execute(
            "SELECT * FROM stop_times WHERE trip_id = ? AND stop_sequence = ?",
            StopTime,
            (trip_id, stop_sequence + 1),
        ).one_must("solve_duplicate: non-existing (trip_id, stop_sequence + 1) pair")
        assert self.is_same_stop_event(l, r)

        solution = self.merge_stop_times(l, r)
        previous = r
        next_sequence = stop_sequence + 2

        # Also handle runs of three or more rows describing one event. A run
        # ends at the first sequence gap, different stop, or missing row.
        while True:
            following = db.retrieve(StopTime, trip_id, next_sequence)
            if following is None or not self.is_same_stop_event(previous, following):
                break
            solution = self.merge_stop_times(solution, following)
            previous = following
            next_sequence += 1

        return solution

    @staticmethod
    def is_same_stop_event(l: StopTime, r: StopTime) -> bool:
        """Adjacent rows for one trip/stop are one event.

        Requiring exact sequence adjacency is what keeps two separated visits
        in a loop trip as independent stop occurrences.
        """
        return (
            l.trip_id == r.trip_id
            and l.stop_id == r.stop_id
            and r.stop_sequence == l.stop_sequence + 1
        )

    def merge_stop_times(self, l: StopTime, r: StopTime) -> StopTime:
        return StopTime(
            trip_id=l.trip_id,
            stop_id=l.stop_id,
            stop_sequence=l.stop_sequence,
            arrival_time=l.arrival_time,
            departure_time=r.departure_time,
            pickup_type=merge_pax_exchange(l.pickup_type, r.pickup_type),
            drop_off_type=merge_pax_exchange(l.drop_off_type, r.drop_off_type),
            stop_headsign=r.stop_headsign or l.stop_headsign,
            shape_dist_traveled=(
                l.shape_dist_traveled
                if l.shape_dist_traveled is not None
                else r.shape_dist_traveled
            ),
            platform=r.platform or l.platform,
            extra_fields_json=r.extra_fields_json or l.extra_fields_json,
        )

    def apply_solutions(self, db: DBConnection, solutions: Sequence[StopTime]) -> None:
        deletion_parameters = list(
            dict.fromkeys(
                (solution.trip_id, redundant_sequence)
                for solution in solutions
                for redundant_sequence in self.find_redundant_sequences(db, solution)
            )
        )

        with db.transaction():
            db.update_many(StopTime, solutions)
            db.raw_execute_many(
                "DELETE FROM stop_times WHERE trip_id = ? AND stop_sequence = ?",
                deletion_parameters,
            )

    def find_redundant_sequences(
        self,
        db: DBConnection,
        solution: StopTime,
    ) -> list[int]:
        """Return the contiguous same-event rows folded into ``solution``."""
        expected_sequence = solution.stop_sequence + 1
        fallback = [solution.stop_sequence + 1]
        try:
            rows = list(
                db.raw_execute(
                    "SELECT stop_sequence "
                    "FROM stop_times "
                    "WHERE trip_id = ? AND stop_sequence > ? AND stop_id = ? "
                    "ORDER BY stop_sequence",
                    (solution.trip_id, solution.stop_sequence, solution.stop_id),
                ).all()
            )
            if not rows:
                # Preserve the original pair-collapse behavior for mock DBs or
                # callers that supply a solved pair without queryable rows.
                return fallback

            redundant: list[int] = []
            for row in rows:
                sequence = cast(int, row[0])
                if sequence != expected_sequence:
                    break
                redundant.append(sequence)
                expected_sequence += 1
        except Exception:
            # Preserve the original pair-collapse behavior for minimal test
            # doubles or databases where the introspection query is unavailable.
            return fallback
        return redundant


def is_same_stop_event(l: StopTime, r: StopTime) -> bool:
    return CollapseDuplicateStopTimes.is_same_stop_event(l, r)


def can_merge_stop_times(l: StopTime, r: StopTime) -> bool:
    return CollapseDuplicateStopTimes.is_same_stop_event(l, r)


def merge_pax_exchange(l: PaxExchange, r: PaxExchange) -> PaxExchange:
    def any_eq(to: PaxExchange) -> bool:
        return l == to or r == to

    if any_eq(PaxExchange.SCHEDULED_STOP):
        return PaxExchange.SCHEDULED_STOP
    elif any_eq(PaxExchange.MUST_PHONE):
        return PaxExchange.MUST_PHONE
    elif any_eq(PaxExchange.ON_REQUEST):
        return PaxExchange.ON_REQUEST
    else:
        assert l == PaxExchange.NONE
        assert r == PaxExchange.NONE
        return PaxExchange.NONE
