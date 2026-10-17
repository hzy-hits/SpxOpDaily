"""Release trust boundary and transactional cutover behavior, without a live host."""
import importlib.util
import io
import json
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    'cloud_release', Path(__file__).parents[1] / 'scripts/deploy-release.py'
)
release = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(release)
SHA = 'a' * 40
DIGEST = 'b' * 64


@pytest.mark.parametrize('value', [
    'sh', 'deploy master ' + DIGEST, 'deploy ' + SHA + ' ../../x',
    'deploy ' + SHA + ' ' + DIGEST + '; id', 'deploy ' + SHA,
])
def test_ssh_receiver_rejects_untrusted_commands(value):
    with pytest.raises(ValueError):
        release.command(value)


def test_ssh_receiver_accepts_only_exact_release_identity():
    assert release.command(f'deploy {SHA} {DIGEST}') == [SHA, DIGEST]


@pytest.fixture
def host(tmp_path, monkeypatch):
    root, legacy, rust, units = [tmp_path / p for p in ('runtime', 'legacy', 'rust', 'units')]
    for path in (root, legacy, rust, units):
        path.mkdir()
    for key, path in [('ROOT', root), ('LEGACY', legacy), ('RUST', rust), ('UNITS', units)]:
        monkeypatch.setattr(release, key, path)
    target = root / 'releases' / SHA
    target.mkdir(parents=True)
    (target / 'archive.sha256').write_text(DIGEST)
    old_rust = rust / 'old'
    old_rust.mkdir()
    (rust / 'current').symlink_to(old_rust)
    (units / 'spx-core.service').write_text(f'[Service]\nExecStart={legacy}/.venv/bin/spx core run\n')
    calls = []

    def run(*args, **kwargs):
        calls.append(args)
        return 'aarch64' if args == ('uname', '-m') else '100'

    monkeypatch.setattr(release, 'run', run)
    monkeypatch.setattr(release, 'urlopen', lambda *a, **kw: io.BytesIO(json.dumps({'sha': SHA}).encode()))
    monkeypatch.setattr(release.time, 'sleep', lambda seconds: None)
    monkeypatch.setattr(release, 'HEALTH_FILES', [])
    monkeypatch.setattr(release, 'active', lambda user, names: (
        list(names) if list(names) in [release.PYTHON_UNITS, release.RUST_UNITS] else []))
    return root, legacy, target, units, calls


def test_wrong_digest_rejected_before_service_mutation(host):
    with pytest.raises(ValueError, match='checksum differs'):
        release.install(SHA, 'c' * 64)
    assert host[-1] == [('uname', '-m')]


def test_migration_requires_separate_cutover_before_stopping_services(host):
    target = host[2]
    (target / 'alembic').mkdir()
    (target / 'alembic/new.py').write_text('new schema')
    with pytest.raises(ValueError, match='migration changed'):
        release.install(SHA, DIGEST)
    assert host[-1] == [('uname', '-m')]


def test_failed_validation_restores_previous_runtime_and_local_unit(host, monkeypatch):
    root, legacy, target, units, calls = host
    count = 0

    def active(user, names):
        nonlocal count
        if names == release.PYTHON_UNITS:
            count += 1
            return names if count == 1 else []
        return names if names == release.RUST_UNITS else []

    monkeypatch.setattr(release, 'active', active)
    with pytest.raises(RuntimeError, match='validation failed'):
        release.install(SHA, DIGEST)
    assert not (root / 'current').is_symlink()
    assert not (units / 'spx-core.service.d/90-cloud-release.conf').exists()
    assert ('systemctl', '--user', 'start', *release.PYTHON_UNITS) in calls
    assert not (root / 'previous.json').exists()


def test_success_records_version_and_keeps_host_credentials_outside_bundle(host):
    root, legacy, target, units, calls = host
    release.install(SHA, DIGEST)
    assert (root / 'current').resolve() == target
    receipt = json.loads((root / 'previous.json').read_text())
    assert receipt['python'] == str(legacy)
    assert receipt['release'] == SHA
    assert not (target / '.env').exists()
    assert ('systemctl', '--user', 'disable', '--now', 'spx-spark-backtest-weekly.timer') in calls


def test_stale_health_is_not_mistaken_for_success(tmp_path, monkeypatch):
    path = tmp_path / 'health.json'
    path.write_text(json.dumps({'updated_at': '2000-01-01T00:00:00Z', 'phase': 'ready'}))
    monkeypatch.setattr(release, 'HEALTH_FILES', [path])
    with pytest.raises(RuntimeError, match='health did not advance'):
        release.verify_health(1700000000)


def test_repeated_deploy_preserves_the_original_rollback_point(host):
    root, legacy, target, units, calls = host
    release.install(SHA, DIGEST)
    before = (root / 'previous.json').read_bytes()
    calls.clear()
    release.install(SHA, DIGEST)
    assert (root / 'previous.json').read_bytes() == before
    assert calls == [('uname', '-m')]
