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
    assert calls == [
        ('uname', '-m'),
        ('systemctl', '--user', 'show', 'spx-spark-backtest-weekly.timer', '-p', 'LoadState', '--value'),
        ('systemctl', '--user', 'disable', '--now', 'spx-spark-backtest-weekly.timer'),
    ]


def test_cold_start_waits_for_real_readiness_without_weakening_health(monkeypatch):
    now = [0]
    attempts = []
    monkeypatch.setattr(release.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(release.time, 'sleep', lambda seconds: now.__setitem__(0, now[0] + seconds))
    monkeypatch.setattr(release, 'run', lambda *args: '100')

    def health(started):
        attempts.append(started)
        if len(attempts) == 1:
            raise RuntimeError('Normalized bridge has not reconnected')

    monkeypatch.setattr(release, 'verify_health', health)
    release.wait_ready(0, {('pid',): '100'}, timeout=5)
    assert len(attempts) == 2
    assert now[0] == 2


def test_never_ready_still_fails_after_bounded_wait(monkeypatch):
    now = [0]
    monkeypatch.setattr(release.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(release.time, 'sleep', lambda seconds: now.__setitem__(0, now[0] + seconds))
    monkeypatch.setattr(release, 'run', lambda *args: '100')

    def health(started):
        raise RuntimeError('Normalized bridge has not reconnected')

    monkeypatch.setattr(release, 'verify_health', health)
    with pytest.raises(RuntimeError, match='not reconnected'):
        release.wait_ready(0, {('pid',): '100'}, timeout=4)
    assert now[0] == 4


def test_crashing_process_is_not_tolerated_during_readiness_wait(monkeypatch):
    monkeypatch.setattr(release, 'run', lambda *args: '101')
    with pytest.raises(RuntimeError, match='process restarted'):
        release.wait_ready(0, {('pid',): '100'})


@pytest.fixture
def migration_host(tmp_path, monkeypatch):
    import os
    import sqlite3
    import subprocess
    import sys

    target = Path(__file__).parents[1]
    database = tmp_path / "spx.sqlite"
    monkeypatch.setattr(release, "ROOT", tmp_path)
    monkeypatch.setattr(release, "DATABASE", database)
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", release.OLD_REVISION],
                   cwd=target, env=dict(os.environ, SPX_DATA_ROOT=str(tmp_path)),
                   check=True, capture_output=True)
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE review_preserved (value TEXT)")
        connection.execute("INSERT INTO review_preserved VALUES ('before')")
    return target, database


def test_reviewed_addition_does_not_allow_modified_or_extra_migrations(tmp_path):
    import shutil

    before, after = tmp_path / "before", tmp_path / "after"
    (before / "alembic").mkdir(parents=True)
    (after / "alembic/versions").mkdir(parents=True)
    (before / "alembic/env.py").write_text("old")
    (after / "alembic/env.py").write_text("old")
    shutil.copyfile(Path(__file__).parents[1] / release.RETENTION_MIGRATION,
                    after / release.RETENTION_MIGRATION)
    assert release.reviewed_migration(before, after)
    (after / "alembic/env.py").write_text("changed")
    with pytest.raises(ValueError, match="separate migration review"):
        release.reviewed_migration(before, after)


def test_additive_migration_keeps_old_rows_and_verified_backup(migration_host):
    import sqlite3

    target, database = migration_host
    backup = Path(release.migrate_retention_indexes(target))
    receipt = json.loads(backup.with_suffix(".json").read_text())
    assert receipt["sha256"] == release.checksum(backup)
    with sqlite3.connect(backup) as saved:
        assert release.database_revision(saved) == release.OLD_REVISION
    with sqlite3.connect(database) as connection:
        assert release.database_revision(connection) == release.NEW_REVISION
        assert connection.execute("SELECT value FROM review_preserved").fetchall() == [("before",)]
        # The previous writer's explicit column insert is still legal after migration.
        connection.execute("""INSERT INTO notification_events
            (idempotency_key, logical_event_id, source, kind, lane, channel,
             payload_json, payload_sha256)
            VALUES ('old','old','core','test','ops','test','{}','hash')""")
        assert connection.execute("SELECT correlation_id FROM notification_events").fetchall() == [(None,)]
    assert release.migrate_retention_indexes(target) is None


def test_partial_migration_failure_restores_database(migration_host, monkeypatch):
    import sqlite3

    target, database = migration_host

    def broken(*args, **kwargs):
        with sqlite3.connect(database) as connection:
            connection.execute("CREATE INDEX ix_outcomes_decision ON outcomes(decision_id)")
        raise RuntimeError("migration interrupted")

    monkeypatch.setattr(release, "run", broken)
    with pytest.raises(RuntimeError, match="migration interrupted"):
        release.migrate_retention_indexes(target)
    with sqlite3.connect(database) as connection:
        assert release.database_revision(connection) == release.OLD_REVISION
        assert "ix_outcomes_decision" not in {row[1] for row in connection.execute("PRAGMA index_list(outcomes)")}
        assert connection.execute("SELECT value FROM review_preserved").fetchall() == [("before",)]
        assert connection.execute("PRAGMA quick_check").fetchall() == [("ok",)]


def test_application_rollback_preserves_new_rows_and_old_worker_can_boot(migration_host, tmp_path):
    import os
    import shutil
    import sqlite3
    import subprocess
    import sys

    target, database = migration_host
    release.migrate_retention_indexes(target)
    with sqlite3.connect(database) as connection:
        connection.execute("INSERT INTO review_preserved VALUES ('written after migration')")
    release.migrate_retention_indexes(target, rollback=True)
    old = tmp_path / 'old-runtime'
    shutil.copytree(target / 'alembic', old / 'alembic')
    (old / release.RETENTION_MIGRATION).unlink()
    shutil.copyfile(target / 'alembic.ini', old / 'alembic.ini')
    # Reproduce the old Worker's actual startup migration, not merely old SQL inserts.
    subprocess.run([sys.executable, '-m', 'alembic', 'upgrade', 'head'], cwd=old,
                   env=dict(os.environ, SPX_DATA_ROOT=str(database.parent)),
                   check=True, capture_output=True)
    with sqlite3.connect(database) as connection:
        assert release.database_revision(connection) == release.OLD_REVISION
        assert connection.execute('SELECT value FROM review_preserved').fetchall() == [
            ('before',), ('written after migration',)]
        assert not connection.execute("SELECT name FROM sqlite_master WHERE name IN "
                                      "('ix_decisions_feature_snapshot','ix_outcomes_decision')").fetchall()


def test_failed_cutover_downgrades_before_old_worker_starts(host, monkeypatch):
    original = release.run
    revision = [release.OLD_REVISION]
    monkeypatch.setattr(release, 'reviewed_migration', lambda *args: True)

    def migrate(target, *, rollback=False):
        revision[0] = release.OLD_REVISION if rollback else release.NEW_REVISION

    def run(*args, **kwargs):
        if args == ('systemctl', '--user', 'start', *release.PYTHON_UNITS):
            selected = (host[0] / 'current').resolve()
            expected = release.NEW_REVISION if selected == host[2] else release.OLD_REVISION
            assert revision[0] == expected
        return original(*args, **kwargs)

    monkeypatch.setattr(release, 'migrate_retention_indexes', migrate)
    monkeypatch.setattr(release, 'run', run)
    monkeypatch.setattr(release, 'wait_ready', lambda *args: (_ for _ in ()).throw(RuntimeError('not ready')))
    with pytest.raises(RuntimeError, match='not ready'):
        release.install(SHA, DIGEST)
    assert revision[0] == release.OLD_REVISION
    assert not (host[0] / 'current').is_symlink()


def test_python_restart_failure_does_not_leave_rust_stopped(host, monkeypatch):
    original = release.run

    def run(*args, **kwargs):
        if args == ('systemctl', '--user', 'start', *release.PYTHON_UNITS):
            raise RuntimeError('Python restart failed')
        return original(*args, **kwargs)

    monkeypatch.setattr(release, 'run', run)
    with pytest.raises(RuntimeError, match='Python restart failed'):
        release.install(SHA, DIGEST)
    assert ('sudo', '-n', 'systemctl', 'start', *release.RUST_UNITS) in host[-1]


def test_bounded_cold_start_can_exceed_two_minutes(monkeypatch):
    now = [0]
    monkeypatch.setattr(release.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(release.time, 'sleep', lambda seconds: now.__setitem__(0, now[0] + seconds))
    monkeypatch.setattr(release, 'run', lambda *args: '100')

    def health(started):
        if now[0] < 150:
            raise RuntimeError('ledger startup still reading')

    monkeypatch.setattr(release, 'verify_health', health)
    release.wait_ready(0, {('pid',): '100'})
    assert now[0] == 150


@pytest.mark.parametrize("state", ["not-found", "masked"])
def test_retired_research_timer_does_not_fail_successful_release(host, monkeypatch, state):
    original = release.run

    def run(*args, **kwargs):
        if args[-3:] == ("-p", "LoadState", "--value"):
            return state
        return original(*args, **kwargs)

    monkeypatch.setattr(release, "run", run)
    release.install(SHA, DIGEST)
    assert json.loads((host[0] / "previous.json").read_text())["release"] == SHA
    assert not any("disable" in call for call in host[-1])


def test_real_timer_cleanup_failure_still_rolls_back_before_success_receipt(host, monkeypatch):
    original = release.run

    def run(*args, **kwargs):
        if "disable" in args:
            raise RuntimeError("timer cleanup failed")
        return original(*args, **kwargs)

    monkeypatch.setattr(release, "run", run)
    with pytest.raises(RuntimeError, match="timer cleanup failed"):
        release.install(SHA, DIGEST)
    assert not (host[0] / "current").is_symlink()
    assert not (host[0] / "previous.json").exists()


def test_release_retention_keeps_current_rollback_and_unknown_paths(tmp_path, monkeypatch):
    root, rust = tmp_path/'python', tmp_path/'rust'
    monkeypatch.setattr(release,'ROOT',root)
    monkeypatch.setattr(release,'RUST',rust)
    for parent in (root,rust):
        for sha in ('a'*40,'b'*40,'c'*40,'not-a-release'):
            (parent/'releases'/sha).mkdir(parents=True)
        (parent/'current').symlink_to(parent/'releases'/('a'*40))
        (parent/'releases'/('d'*40)).symlink_to(tmp_path)
    calls=[]
    monkeypatch.setattr(release,'run',lambda *args: calls.append(args))
    removed=release.retire_old_releases(root/'releases'/('b'*40),rust/'releases'/('b'*40))
    assert set(removed)=={str(parent/'releases'/('c'*40)) for parent in (root,rust)}
    assert (root/'releases'/('a'*40)).is_dir()
    assert (root/'releases'/('b'*40)).is_dir()
    assert (root/'releases'/'not-a-release').is_dir()
    assert (root/'releases'/('d'*40)).is_symlink()
    assert calls==[('sudo','-n','rm','-rf','--',str(rust/'releases'/('c'*40)))]
