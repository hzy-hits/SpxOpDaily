"""Compatibility entrypoint for architecture guards.

Canonical location: tests/architecture/test_module_registry.py
"""

from __future__ import annotations

from architecture.test_module_registry import (  # noqa: F401
    test_all_production_modules_are_classified,
    test_domain_has_stdlib_only,
    test_env_helper_defaults_are_not_literals,
    test_layer_import_rules,
)
