# Frozen runtime defaults for unit/architecture tests.
#
# Copied from config/runtime.toml at Phase 0 baseline freeze. Unit tests must
# load this file via SPX_SPARK_RUNTIME_CONFIG (see tests/conftest.py) so that
# workspace deployment edits to config/runtime.toml or a local .env cannot
# change assertions. When intentionally changing product defaults, update both
# config/runtime.toml and this fixture in the same change.
