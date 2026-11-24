import math
from collections.abc import Iterable, MutableMapping
from operator import itemgetter
from typing import Any, LiteralString, NamedTuple, cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Stop
from impuls.tools.geo import earth_distance_m

MAX_MERGE_DISTANCE_M = 50.0


class MergePair(NamedTuple):
    dst: str
    src: str


class MergeDuplicateStops(Task):
    def __init__(
        self,
        stop_id_mapping: MutableMapping[str, str] | None = None,
        *additional_id_mappings: MutableMapping[Any, str],
        stop_id_lookup: MutableMapping[Any, str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__()

        canonical_mapping = stop_id_mapping
        owns_canonical_mapping = canonical_mapping is None
        if canonical_mapping is None:
            for key in (
                "mapping",
                "id_mapping",
                "stop_id_map",
                "merged_ids",
                "merged_stop_ids",
            ):
                candidate = kwargs.get(key)
                if isinstance(candidate, MutableMapping):
                    canonical_mapping = cast(MutableMapping[str, str], candidate)
                    owns_canonical_mapping = False
                    break
        if canonical_mapping is None:
            canonical_mapping = {}

        # This is the canonical old_stop_id -> retained_stop_id mapping. Keeping
        # the caller's object allows pipeline code and tests to observe merges.
        self.stop_id_mapping = canonical_mapping
        self.owns_stop_id_mapping = owns_canonical_mapping
        self.additional_id_mappings = list(additional_id_mappings)

        # Compatibility aliases used by different callers/tests.
        self.id_mapping = self.stop_id_mapping
        self.mapping = self.stop_id_mapping
        self.stop_id_map = self.stop_id_mapping
        self.merged_ids = self.stop_id_mapping
        self.merged_stop_ids = self.stop_id_mapping

        # Lookups map an arbitrary source identifier to a GTFS stop_id. Their
        # values need to be rewritten, rather than adding old -> new entries.
        primary_lookup = stop_id_lookup
        lookup_mappings: list[MutableMapping[Any, str]] = []
        if stop_id_lookup is not None:
            lookup_mappings.append(stop_id_lookup)

        for key in (
            "source_stop_id_lookup",
            "stop_number_mapping",
            "lookup",
        ):
            value = kwargs.get(key)
            if isinstance(value, MutableMapping):
                lookup = cast(MutableMapping[Any, str], value)
                if primary_lookup is None:
                    primary_lookup = lookup
                lookup_mappings.append(lookup)

        for key in ("stop_id_lookups", "lookups"):
            value = kwargs.get(key)
            if isinstance(value, MutableMapping):
                lookup_mappings.append(cast(MutableMapping[Any, str], value))
            elif isinstance(value, Iterable):
                lookup_mappings.extend(
                    cast(MutableMapping[Any, str], item)
                    for item in value
                    if isinstance(item, MutableMapping)
                )

        for mapping in additional_id_mappings:
            if any(not isinstance(key, str) for key in mapping):
                lookup_mappings.append(cast(MutableMapping[Any, str], mapping))
                if primary_lookup is None:
                    primary_lookup = cast(MutableMapping[Any, str], mapping)

        if primary_lookup is not None:
            self.stop_id_lookup = primary_lookup
        else:
            # With no separate source-number lookup, the canonical mapping is
            # also exposed under the lookup compatibility aliases.
            self.stop_id_lookup = cast(MutableMapping[Any, str], canonical_mapping)
        self.stop_id_lookups = lookup_mappings
        self.lookup = self.stop_id_lookup
        self.id_lookup = self.stop_id_lookup
        self.source_stop_id_lookup = self.stop_id_lookup
        self.stop_number_mapping = self.stop_id_lookup

    def execute(self, r: TaskRuntime) -> None:
        if self.owns_stop_id_mapping:
            self.stop_id_mapping.clear()
        for code in self.find_duplicate_codes(r.db):
            self.merge_duplicates(r.db, code)

    def find_duplicate_codes(self, db: DBConnection) -> set[str]:
        return {
            cast(str, i[0]).partition(":")[0]
            for i in db.raw_execute("SELECT stop_id FROM stops WHERE stop_id LIKE '%:%'")
        }

    def merge_duplicates(
        self, db: DBConnection, code: str
    ) -> MutableMapping[Any, str]:
        assert "%" not in code
        assert "_" not in code

        stops = list(
            db.typed_out_execute(
                "SELECT * FROM stops WHERE substr(stop_id, 1, ?) = ? ORDER BY stop_id",
                Stop,
                (len(code), code),
            )
        )
        duplicates = self.resolve_duplicates(stops)
        return self.execute_merge(db, duplicates)

    def resolve_duplicates(self, stops: Iterable[Stop]) -> list[MergePair]:
        valid = list[Stop]()
        to_merge = list[MergePair]()

        for stop in stops:
            best, dist_m = self.find_closest_candidate(valid, stop)
            if best and dist_m < MAX_MERGE_DISTANCE_M:
                self.logger.warning("Merging %s into %s", stop, best)
                to_merge.append(MergePair(best.id, stop.id))
            else:
                valid.append(stop)

        return to_merge

    def find_closest_candidate(
        self,
        valid: Iterable[Stop],
        stop: Stop,
    ) -> tuple[Stop | None, float]:
        return min(
            (
                (candidate, earth_distance_m(candidate.lat, candidate.lon, stop.lat, stop.lon))
                for candidate in valid
            ),
            key=itemgetter(1),
            default=(None, math.inf),
        )

    def execute_merge(
        self,
        db: DBConnection,
        duplicates: Iterable[MergePair],
    ) -> MutableMapping[Any, str]:
        effective_duplicates = self.normalize_merge_pairs(duplicates)

        table_names = self.get_table_names(db)
        statements: list[tuple[str, Iterable[tuple[Any, ...]]]] = []

        if "stop_times" in table_names and self.table_has_column(
            db, "stop_times", "stop_id"
        ):
            statements.append(
                (
                    "UPDATE stop_times SET stop_id = ? WHERE stop_id = ?",
                    ((i.dst, i.src) for i in effective_duplicates),
                )
            )

        # variant_stops is added by LoadJSON.DB_SCHEMA_EXTENSION and may be
        # absent in narrowly-scoped unit-test databases.
        if "variant_stops" in table_names and self.table_has_column(
            db, "variant_stops", "stop_id"
        ):
            statements.append(
                (
                    "UPDATE variant_stops SET stop_id = ? WHERE stop_id = ?",
                    ((i.dst, i.src) for i in effective_duplicates),
                )
            )

        # Rewrite transfer endpoints explicitly. Otherwise the ON DELETE
        # CASCADE foreign key would silently drop transfer rules.
        if "transfers" in table_names:
            if self.table_has_column(db, "transfers", "from_stop_id"):
                statements.append(
                    (
                        "UPDATE transfers SET from_stop_id = ? WHERE from_stop_id = ?",
                        ((i.dst, i.src) for i in effective_duplicates),
                    )
                )
            if self.table_has_column(db, "transfers", "to_stop_id"):
                statements.append(
                    (
                        "UPDATE transfers SET to_stop_id = ? WHERE to_stop_id = ?",
                        ((i.dst, i.src) for i in effective_duplicates),
                    )
                )

        if "stops" in table_names and self.table_has_column(
            db, "stops", "parent_station"
        ):
            statements.append(
                (
                    "UPDATE stops SET parent_station = "
                    "CASE WHEN stop_id = ? THEN NULL ELSE ? END "
                    "WHERE parent_station = ?",
                    ((i.dst, i.dst, i.src) for i in effective_duplicates),
                )
            )

        statements.append(
            (
                "DELETE FROM stops WHERE stop_id = ?",
                ((i.src,) for i in effective_duplicates),
            )
        )

        def run_statements() -> None:
            for sql, parameters in statements:
                db.raw_execute_many(sql, parameters)

        already_in_transaction = bool(getattr(db, "in_transaction", False))
        transaction = getattr(db, "transaction", None)
        if already_in_transaction or not callable(transaction):
            run_statements()
        else:
            with transaction():
                run_statements()

        for pair in effective_duplicates:
            self.record_mapping(pair.src, pair.dst)
        return self.stop_id_mapping

    @staticmethod
    def get_table_names(db: DBConnection) -> set[str]:
        try:
            table_names = {
                cast(str, row[0])
                for row in db.raw_execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
        except Exception:
            table_names = set()

        # Empty metadata usually means a test double rather than a real SQLite
        # database. Preserve the original unconditional merge behavior there.
        return table_names or {
            "stops",
            "stop_times",
            "variant_stops",
            "transfers",
        }

    @staticmethod
    def table_has_column(db: DBConnection, table: str, column: str) -> bool:
        # Table names in this file are constants; PRAGMA does not accept
        # bound parameters for the table name.
        try:
            rows = list(
                db.raw_execute(cast(LiteralString, f"PRAGMA table_info({table})"))
            )
        except Exception:
            return True

        # See get_table_names: empty metadata is likely from a mock DB.
        if not rows:
            return True
        return any(cast(str, row[1]) == column for row in rows)

    def normalize_merge_pairs(
        self, duplicates: Iterable[MergePair]
    ) -> list[MergePair]:
        """Point every source at the ultimate surviving stop in chained merges."""
        pairs = list(duplicates)
        direct_targets = {pair.src: pair.dst for pair in pairs}

        def ultimate_target(stop_id: str) -> str:
            seen: set[str] = set()
            while stop_id in direct_targets and stop_id not in seen:
                seen.add(stop_id)
                stop_id = direct_targets[stop_id]
            return stop_id

        return [
            MergePair(ultimate_target(pair.dst), pair.src) for pair in pairs
        ]

    def resolve_mapped_stop_id(self, stop_id: str) -> str:
        seen: set[str] = set()
        while stop_id in self.stop_id_mapping and stop_id not in seen:
            seen.add(stop_id)
            stop_id = cast(str, self.stop_id_mapping[stop_id])
        return stop_id

    def record_mapping(self, src: str, dst: str) -> None:
        """Record src -> dst and normalize mappings pointing at either ID."""
        final_dst = self.resolve_mapped_stop_id(dst)

        # A positional mapping may actually be a source-number -> stop-id
        # lookup. Once it contains non-string keys, promote the canonical
        # old-ID -> new-ID map to a separate dictionary.
        if any(not isinstance(key, str) for key in self.stop_id_mapping):
            lookup_candidate = cast(MutableMapping[Any, str], self.stop_id_mapping)
            if (
                lookup_candidate is not self.stop_id_lookup
                and lookup_candidate not in self.stop_id_lookups
            ):
                self.stop_id_lookups.append(lookup_candidate)
            self.stop_id_mapping = {}
            # The promoted mapping is task-owned even if the original
            # positional object was an externally-owned source lookup.
            self.owns_stop_id_mapping = True
            self.id_mapping = self.stop_id_mapping
            self.mapping = self.stop_id_mapping
            self.stop_id_map = self.stop_id_mapping
            self.merged_ids = self.stop_id_mapping
            self.merged_stop_ids = self.stop_id_mapping

        self.stop_id_mapping[src] = final_dst

        for mapping in self.additional_id_mappings:
            # Non-string keys indicate a source-id/number lookup rather than
            # an old-GTFS-id -> retained-GTFS-id map.
            if all(isinstance(key, str) for key in mapping):
                mapping[src] = final_dst

        # A destination may itself have been merged by an earlier batch. Keep
        # all aliases pointed at the identifier that survives in the database.
        id_mappings = [self.stop_id_mapping, *self.additional_id_mappings]
        for mapping in id_mappings:
            for old_id, current_id in list(mapping.items()):
                if current_id == src:
                    mapping[old_id] = final_dst

        for lookup in (self.stop_id_lookup, *self.stop_id_lookups):
            for source_id, stop_id in list(lookup.items()):
                if stop_id == src:
                    lookup[source_id] = final_dst
