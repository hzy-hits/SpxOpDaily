"""Minimal settings entrypoint for the simplified runtime."""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SPX_",
        frozen=True,
        extra="forbid",
        toml_file=("config/defaults.toml", "config/production.toml"),
    )

    data_root: Path = Path("/srv/data/spx-spark")
    log_level: str = "INFO"
    core_lock_root: Path = Path("/tmp")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        del dotenv_settings
        return (
            init_settings,
            env_settings,
            TomlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )


@lru_cache
def get_settings() -> AppSettings:
    return AppSettings()
