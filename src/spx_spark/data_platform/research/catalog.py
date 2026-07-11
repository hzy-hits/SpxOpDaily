"""Rebuildable DuckDB catalog over the SPX Spark research lake.

The catalog owns only views.  Parquet and the optional read-only SQLite ledger
remain the sources of truth, so an in-memory catalog is the safe default and a
file-backed catalog can be deleted and rebuilt at any time.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Sequence

import duckdb

from .schemas import DATASET_SCHEMAS, Column, DatasetSchema


PUBLIC_VIEWS = frozenset(
    {
        "research_strategy_outcome_v1",
        "put_call_bias_audit_v1",
        "session_data_quality_v1",
        "research_quotes_v1",
    }
)


class ResearchCatalogError(RuntimeError):
    """Raised when a research catalog cannot be built safely."""


@dataclass(frozen=True)
class ResearchCatalogConfig:
    """Physical sources for a disposable DuckDB research catalog."""

    data_root: Path
    database: Path | str = ":memory:"
    sqlite_ledger: Path | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "data_root", Path(self.data_root).expanduser().resolve())
        if self.sqlite_ledger is not None:
            object.__setattr__(
                self,
                "sqlite_ledger",
                Path(self.sqlite_ledger).expanduser().resolve(),
            )
        if self.database != ":memory:":
            object.__setattr__(
                self,
                "database",
                Path(self.database).expanduser().resolve(),
            )

    @property
    def lake_root(self) -> Path:
        if self.data_root.name == "lake":
            return self.data_root
        return self.data_root / "lake"

    @property
    def manifests_root(self) -> Path:
        base = self.data_root.parent if self.data_root.name == "lake" else self.data_root
        return base / "manifests"


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _empty_projection(columns: Sequence[Column]) -> str:
    return ",\n        ".join(
        f"CAST(NULL AS {column.sql_type}) AS {_quote_identifier(column.name)}"
        for column in columns
    )


def _normalized_projection(columns: Sequence[Column], available: set[str]) -> str:
    expressions: list[str] = []
    by_folded_name = {name.casefold(): name for name in available}
    for column in columns:
        source_names: list[str] = []
        for candidate in (column.name, *column.aliases):
            actual = by_folded_name.get(candidate.casefold())
            if actual is not None and actual not in source_names:
                source_names.append(actual)
        if not source_names:
            expression = f"CAST(NULL AS {column.sql_type})"
        else:
            casts = [
                f"TRY_CAST({_quote_identifier(name)} AS {column.sql_type})"
                for name in source_names
            ]
            expression = casts[0] if len(casts) == 1 else f"COALESCE({', '.join(casts)})"
        expressions.append(f"{expression} AS {_quote_identifier(column.name)}")
    return ",\n        ".join(expressions)


class ResearchCatalog:
    """Build and query a versioned, read-only research surface.

    No general ``execute`` method is exposed.  DDL is restricted to catalog
    construction, while consumers receive :class:`DuckDBResearchReader`, which
    can query only the allowlisted versioned views.
    """

    def __init__(self, config: ResearchCatalogConfig):
        self.config = config
        self._lock = threading.RLock()
        database = config.database
        if database != ":memory:":
            assert isinstance(database, Path)
            database.parent.mkdir(parents=True, exist_ok=True)
        self._connection = duckdb.connect(str(database))
        # TIMESTAMPTZ values must not change with the host running a replay.
        self._connection.execute("SET TimeZone = 'UTC'")
        self._closed = False
        self.rebuild()

    @classmethod
    def in_memory(
        cls,
        data_root: str | Path,
        *,
        sqlite_ledger: str | Path | None = None,
    ) -> "ResearchCatalog":
        return cls(
            ResearchCatalogConfig(
                data_root=Path(data_root),
                sqlite_ledger=Path(sqlite_ledger) if sqlite_ledger is not None else None,
            )
        )

    def __enter__(self) -> "ResearchCatalog":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def reader(self) -> "DuckDBResearchReader":
        self._require_open()
        return DuckDBResearchReader(self)

    def rebuild(self) -> None:
        """Recreate every source and public view from the durable stores."""

        with self._lock:
            self._require_open()
            self._attach_sqlite_read_only()
            sqlite_tables = self._sqlite_tables()
            for schema in DATASET_SCHEMAS:
                self._build_dataset_views(schema, sqlite_tables)
            self._build_public_views()

    def _require_open(self) -> None:
        if self._closed:
            raise ResearchCatalogError("research catalog is closed")

    def _attach_sqlite_read_only(self) -> None:
        attached = {
            str(row[1])
            for row in self._connection.execute("PRAGMA database_list").fetchall()
        }
        if "ledger" in attached:
            self._connection.execute("DETACH ledger")
        ledger = self.config.sqlite_ledger
        if ledger is None:
            return
        if not ledger.is_file():
            raise ResearchCatalogError(f"SQLite ledger does not exist: {ledger}")
        try:
            self._connection.execute(
                f"ATTACH {_quote_literal(ledger)} AS ledger (TYPE SQLITE, READ_ONLY)"
            )
        except duckdb.Error as exc:
            raise ResearchCatalogError(f"cannot attach SQLite ledger read-only: {ledger}") from exc

    def _sqlite_tables(self) -> set[str]:
        if self.config.sqlite_ledger is None:
            return set()
        rows = self._connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_catalog = 'ledger' AND table_schema = 'main'
            """
        ).fetchall()
        return {str(row[0]) for row in rows}

    def _parquet_files(self, schema: DatasetSchema) -> list[Path]:
        lake = self.config.lake_root
        patterns = (
            f"facts/{schema.name}/**/*.parquet",
            f"facts/dataset={schema.name}/**/*.parquet",
            f"{schema.name}/**/*.parquet",
            f"facts/**/{schema.name}*.parquet",
            *schema.parquet_patterns,
        )
        files: set[Path] = set()
        for pattern in patterns:
            files.update(path.resolve() for path in lake.glob(pattern) if path.is_file())
        return sorted(files)

    def _json_manifest_files(self, schema: DatasetSchema) -> list[Path]:
        if schema.name != "session_manifests":
            return []
        return sorted(
            path.resolve()
            for path in self.config.manifests_root.glob("**/*.json")
            if path.is_file()
        )

    def _relation_columns(self, relation: str) -> set[str]:
        rows = self._connection.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
        return {str(row[0]) for row in rows}

    def _create_empty_view(self, name: str, schema: DatasetSchema) -> None:
        self._connection.execute(
            f"""
            CREATE OR REPLACE VIEW {_quote_identifier(name)} AS
            SELECT
                {_empty_projection(schema.columns)}
            WHERE FALSE
            """
        )

    def _create_normalized_view(
        self,
        *,
        name: str,
        raw_relation: str,
        schema: DatasetSchema,
    ) -> None:
        available = self._relation_columns(raw_relation)
        self._connection.execute(
            f"""
            CREATE OR REPLACE VIEW {_quote_identifier(name)} AS
            SELECT
                {_normalized_projection(schema.columns, available)}
            FROM {raw_relation}
            """
        )

    def _build_parquet_view(self, schema: DatasetSchema) -> str:
        normalized = f"_parquet_{schema.name}_v1"
        raw = f"_raw_parquet_{schema.name}_v1"
        files = self._parquet_files(schema)
        if not files:
            self._create_empty_view(normalized, schema)
            return normalized
        paths = ", ".join(_quote_literal(path) for path in files)
        try:
            self._connection.execute(
                f"""
                CREATE OR REPLACE VIEW {_quote_identifier(raw)} AS
                SELECT *
                FROM read_parquet(
                    [{paths}],
                    union_by_name = TRUE,
                    hive_partitioning = TRUE,
                    filename = TRUE
                )
                """
            )
            self._create_normalized_view(
                name=normalized,
                raw_relation=_quote_identifier(raw),
                schema=schema,
            )
        except duckdb.Error as exc:
            raise ResearchCatalogError(
                f"cannot build Parquet research view for {schema.name}"
            ) from exc
        return normalized

    def _build_json_manifest_view(self, schema: DatasetSchema) -> str:
        normalized = f"_json_{schema.name}_v1"
        raw = f"_raw_json_{schema.name}_v1"
        files = self._json_manifest_files(schema)
        if not files:
            self._create_empty_view(normalized, schema)
            return normalized
        paths = ", ".join(_quote_literal(path) for path in files)
        try:
            self._connection.execute(
                f"""
                CREATE OR REPLACE VIEW {_quote_identifier(raw)} AS
                SELECT *
                FROM read_json_auto([{paths}], union_by_name = TRUE, filename = TRUE)
                """
            )
            self._create_normalized_view(
                name=normalized,
                raw_relation=_quote_identifier(raw),
                schema=schema,
            )
        except duckdb.Error as exc:
            raise ResearchCatalogError("cannot build compaction manifest view") from exc
        return normalized

    def _build_sqlite_view(
        self,
        schema: DatasetSchema,
        sqlite_tables: set[str],
    ) -> str:
        normalized = f"_sqlite_{schema.name}_v1"
        table = next((name for name in schema.sqlite_tables if name in sqlite_tables), None)
        if table is None:
            self._create_empty_view(normalized, schema)
            return normalized
        relation = ".".join(
            (_quote_identifier("ledger"), _quote_identifier("main"), _quote_identifier(table))
        )
        self._create_normalized_view(name=normalized, raw_relation=relation, schema=schema)
        return normalized

    def _build_dataset_views(
        self,
        schema: DatasetSchema,
        sqlite_tables: set[str],
    ) -> None:
        sources = [self._build_parquet_view(schema)]
        if schema.name == "session_manifests":
            sources.append(self._build_json_manifest_view(schema))
        if schema.sqlite_tables:
            sources.append(self._build_sqlite_view(schema, sqlite_tables))

        union_parts = [
            f"SELECT *, {priority} AS _source_priority "
            f"FROM {_quote_identifier(source)}"
            for priority, source in enumerate(sources, start=1)
        ]
        union_sql = "\nUNION ALL\n".join(union_parts)
        canonical = f"_research_source_{schema.name}_v1"
        key = schema.primary_key
        if key is None:
            sql = f"""
                CREATE OR REPLACE VIEW {_quote_identifier(canonical)} AS
                SELECT * EXCLUDE (_source_priority)
                FROM ({union_sql})
            """
        else:
            keys = (key,) if isinstance(key, str) else key
            key_identifiers = tuple(_quote_identifier(name) for name in keys)
            partition_by = ", ".join(key_identifiers)
            missing_key = " OR ".join(f"{name} IS NULL" for name in key_identifiers)
            sql = f"""
                CREATE OR REPLACE VIEW {_quote_identifier(canonical)} AS
                WITH combined AS (
                    {union_sql}
                ), ranked AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY {partition_by}
                            ORDER BY _source_priority DESC
                        ) AS _source_row
                    FROM combined
                )
                SELECT * EXCLUDE (_source_priority, _source_row)
                FROM ranked
                WHERE {missing_key} OR _source_row = 1
            """
        self._connection.execute(sql)

    def _build_public_views(self) -> None:
        views_path = Path(__file__).with_name("views")
        for path in sorted(views_path.glob("*.sql")):
            sql = path.read_text(encoding="utf-8")
            try:
                self._connection.execute(sql)
            except duckdb.Error as exc:
                raise ResearchCatalogError(f"cannot build research view from {path.name}") from exc

    def _query_view(
        self,
        view: str,
        *,
        predicates: Sequence[str] = (),
        parameters: Sequence[object] = (),
        order_by: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, object]]:
        if view not in PUBLIC_VIEWS:
            raise ValueError(f"research view is not allowlisted: {view}")
        if limit is not None and not 1 <= limit <= 100_000:
            raise ValueError("limit must be between 1 and 100000")
        sql = f"SELECT * FROM {_quote_identifier(view)}"
        if predicates:
            sql += " WHERE " + " AND ".join(predicates)
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit is not None:
            sql += " LIMIT ?"
            parameters = (*parameters, limit)
        with self._lock:
            self._require_open()
            cursor = self._connection.execute(sql, list(parameters))
            names = [item[0] for item in cursor.description]
            return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


class DuckDBResearchReader:
    """Query-only facade for the fixed, versioned research views."""

    def __init__(self, catalog: ResearchCatalog):
        self._catalog = catalog

    def strategy_outcomes(
        self,
        query: object | None = None,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        strategy_name: str | None = None,
        side: str | None = None,
        limit: int = 10_000,
    ) -> list[dict[str, object]]:
        """Return decision/outcome rows that passed the anti-lookahead gate."""

        # Accept a port-level query object without coupling this adapter to its
        # concrete dataclass. Explicit keyword arguments remain useful to tools.
        if query is not None:
            start_date = getattr(query, "start_date", start_date)
            end_date = getattr(query, "end_date", end_date)
            strategy_name = getattr(query, "strategy_name", strategy_name)
            side = getattr(query, "side", side)
            limit = getattr(query, "limit", limit)
        predicates: list[str] = []
        parameters: list[object] = []
        if start_date is not None:
            predicates.append("session_date >= ?")
            parameters.append(start_date)
        if end_date is not None:
            predicates.append("session_date <= ?")
            parameters.append(end_date)
        if strategy_name is not None:
            predicates.append("strategy_name = ?")
            parameters.append(strategy_name)
        if side is not None:
            predicates.append("option_side = upper(?)")
            parameters.append(side)
        return self._catalog._query_view(
            "research_strategy_outcome_v1",
            predicates=predicates,
            parameters=parameters,
            order_by="decision_at, decision_id, horizon_minutes NULLS LAST",
            limit=limit,
        )

    def put_call_bias(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 10_000,
    ) -> list[dict[str, object]]:
        predicates: list[str] = []
        parameters: list[object] = []
        if start_date is not None:
            predicates.append("session_date >= ?")
            parameters.append(start_date)
        if end_date is not None:
            predicates.append("session_date <= ?")
            parameters.append(end_date)
        return self._catalog._query_view(
            "put_call_bias_audit_v1",
            predicates=predicates,
            parameters=parameters,
            order_by=(
                "session_date, strategy_name, strategy_version, option_side, "
                "horizon_minutes NULLS LAST"
            ),
            limit=limit,
        )

    def session_data_quality(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        provider: str | None = None,
        dataset: str | None = None,
        limit: int = 10_000,
    ) -> list[dict[str, object]]:
        predicates: list[str] = []
        parameters: list[object] = []
        if start_date is not None:
            predicates.append("session_date >= ?")
            parameters.append(start_date)
        if end_date is not None:
            predicates.append("session_date <= ?")
            parameters.append(end_date)
        if provider is not None:
            predicates.append("provider = ?")
            parameters.append(provider)
        if dataset is not None:
            predicates.append("dataset = ?")
            parameters.append(dataset)
        return self._catalog._query_view(
            "session_data_quality_v1",
            predicates=predicates,
            parameters=parameters,
            order_by="session_date, provider, dataset",
            limit=limit,
        )

    def quotes(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        provider: str | None = None,
        limit: int = 10_000,
    ) -> list[dict[str, object]]:
        predicates: list[str] = []
        parameters: list[object] = []
        if start_date is not None:
            predicates.append("session_date >= ?")
            parameters.append(start_date)
        if end_date is not None:
            predicates.append("session_date <= ?")
            parameters.append(end_date)
        if provider is not None:
            predicates.append("provider = ?")
            parameters.append(provider)
        return self._catalog._query_view(
            "research_quotes_v1",
            predicates=predicates,
            parameters=parameters,
            order_by="source_at, received_at, instrument_id",
            limit=limit,
        )


def build_research_catalog(
    data_root: str | Path,
    *,
    database: str | Path = ":memory:",
    sqlite_ledger: str | Path | None = None,
) -> ResearchCatalog:
    """Build a disposable research catalog from durable data sources."""

    return ResearchCatalog(
        ResearchCatalogConfig(
            data_root=Path(data_root),
            database=Path(database) if database != ":memory:" else database,
            sqlite_ledger=Path(sqlite_ledger) if sqlite_ledger is not None else None,
        )
    )
