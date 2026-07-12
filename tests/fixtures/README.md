# Frozen runtime defaults for unit/architecture tests.
#
# Copied from config/runtime.yaml at Phase 0 baseline freeze. Unit tests must
# load this file via SPX_SPARK_RUNTIME_CONFIG (see tests/conftest.py) so that
# workspace deployment edits to config/runtime.yaml or a local .env cannot
# change assertions. When intentionally changing product defaults, update both
# config/runtime.yaml and this fixture in the same change.
