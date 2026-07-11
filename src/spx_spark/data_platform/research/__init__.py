"""DuckDB-backed research catalog and versioned read-only views."""

from .catalog import (
    DuckDBResearchReader,
    ResearchCatalog,
    ResearchCatalogConfig,
    ResearchCatalogError,
    build_research_catalog,
)

__all__ = [
    "DuckDBResearchReader",
    "ResearchCatalog",
    "ResearchCatalogConfig",
    "ResearchCatalogError",
    "build_research_catalog",
]
