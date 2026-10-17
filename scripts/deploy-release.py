#!/usr/bin/env python3
"""Restricted Oracle release receiver; no checkout, dependency install or compilation."""
from contextlib import closing
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
from urllib.request import urlopen

ROOT = Path('/home/ubuntu/spx-runtime')
LEGACY = Path('/home/ubuntu/spx-spark')
RUST = Path('/opt/spx-spark-core-shadow')
UNITS = Path('/home/ubuntu/.config/systemd/user')
PYTHON_UNITS = ['spx-core', 'spx-worker', 'spx-spark-ibkr-stream',
                'spx-spark-schwab-marketdata', 'spx-spark-schwab-oauth']
RUST_UNITS = ['spx-rust-core-shadow', 'spx-rust-normalized-bridge',
              'spx-rust-report', 'spx-rust-delivery']
HEALTH_FILES = [Path('/var/lib/spx-spark-bridge-shadow/health.json'),
                Path('/var/lib/spx-spark-report-shadow/health.json')]
BASE_URL = 'https://github.com/hzy-hits/SpxOpDaily/releases/download'
DATABASE = Path('/srv/data/spx-spark/spx.sqlite')
# Reviewed additive migration only. Any other schema change still stops cutover.
CORRELATION_MIGRATION = 'alembic/versions/0005_notification_correlation.py'
CORRELATION_SHA256 = '3968710f27ca2350c65c3235eacc821bdbe0614a7141c48483d4ba667434d9fe'
OLD_REVISION = '0004_session_card_cover'
NEW_REVISION = '0005_notification_correlation'


def run(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True, **kwargs).stdout.strip()


def command(value):
    args = shlex.split(value)
    if (len(args) != 3 or args[0] != 'deploy'
            or not re.fullmatch(r'[0-9a-f]{40}', args[1])
            or not re.fullmatch(r'[0-9a-f]{64}', args[2])):
        raise ValueError('Only deploy <full commit SHA> <archive SHA256> is accepted')
    return args[1:]


def checksum(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def atomic_link(target, path):
    temporary = path.with_name(path.name + '.next')
    temporary.unlink(missing_ok=True)
    temporary.symlink_to(target)
    temporary.replace(path)


def active(user, units):
    prefix = ['systemctl', '--user'] if user else ['systemctl']
    return [u for u in units if subprocess.run(prefix + ['is-active', '--quiet', u],
                                             check=False).returncode == 0]


def schema_files(root):
    return {str(p.relative_to(root)): checksum(p) for p in (root / 'alembic').rglob('*.py')}


def reviewed_migration(previous, target):
    before, after = schema_files(previous), schema_files(target)
    if before == after:
        return False
    if (CORRELATION_MIGRATION not in before
            and after == {**before, CORRELATION_MIGRATION: CORRELATION_SHA256}):
        return True
    raise ValueError('Database migration changed; separate migration review required')


def database_revision(connection):
    rows = connection.execute('SELECT version_num FROM alembic_version').fetchall()
    if len(rows) != 1:
        raise ValueError('Expected exactly one database revision')
    return rows[0][0]


def migrate_correlation(target):
    """Run the reviewed Alembic revision with all writers stopped and a verified backup.

    An application rollback keeps the additive nullable column/index. If Alembic
    itself fails, restore the pre-migration database before restarting old writers.
    """
    with closing(sqlite3.connect(f'file:{DATABASE}?mode=rw', uri=True)) as source:
        revision = database_revision(source)
        if revision == NEW_REVISION:
            columns = {row[1] for row in source.execute('PRAGMA table_info(notification_events)')}
            indexes = {row[1] for row in source.execute('PRAGMA index_list(notification_events)')}
            if 'correlation_id' not in columns or 'ix_notification_events_correlation' not in indexes:
                raise ValueError('Database revision does not match correlation schema')
            return None
        if revision != OLD_REVISION:
            raise ValueError('Unexpected database baseline; migration refused')
        backups = ROOT / 'backups'
        backups.mkdir(exist_ok=True, mode=0o700)
        if shutil.disk_usage(backups).free < DATABASE.stat().st_size + 3 * 1024**3:
            raise ValueError('Insufficient space for database backup plus 3 GiB reserve')
        backup = backups / f'{target.name}-{time.time_ns()}.sqlite'
        backup.touch(mode=0o600, exist_ok=False)
        with closing(sqlite3.connect(backup)) as saved:
            source.backup(saved)
            if saved.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
                raise RuntimeError('Pre-migration database backup failed integrity check')
        receipt = {'source': str(DATABASE), 'backup': str(backup),
                   'sha256': checksum(backup), 'from_revision': revision,
                   'to_revision': NEW_REVISION, 'release': target.name}
        backup.with_suffix('.json').write_text(json.dumps(receipt, indent=2) + '\n')
        env = dict(os.environ, PYTHONPATH=str(target / 'src'), SPX_DATA_ROOT=str(DATABASE.parent),
                   PYTHONDONTWRITEBYTECODE='1', SPX_SPARK_DISABLE_RUNTIME_OVERRIDES='1')
        try:
            run(str(target / '.venv/bin/python'), '-m', 'alembic', 'upgrade', NEW_REVISION,
                cwd=target, env=env)
            if database_revision(source) != NEW_REVISION:
                raise RuntimeError('Database migration did not reach reviewed revision')
            if source.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
                raise RuntimeError('Post-migration database integrity check failed')
        except BaseException:
            with closing(sqlite3.connect(backup)) as saved:
                saved.backup(source)
            raise
        return str(backup)


def verify_health(started):
    for path in HEALTH_FILES:
        payload = json.loads(path.read_text())
        updated = datetime.fromisoformat(payload['updated_at'].replace('Z', '+00:00'))
        if updated.timestamp() < started or payload.get('phase') == 'halted':
            raise RuntimeError('Runtime health did not advance after deployment')
        if 'socket_connected' in payload and not payload['socket_connected']:
            raise RuntimeError('Normalized bridge has not reconnected')


def wait_ready(started, processes, timeout=105):
    """Allow cold replay/resync while rejecting crashes and a stale health file."""
    deadline = time.monotonic() + timeout
    while True:
        if any(pid == '0' or run(*args) != pid for args, pid in processes.items()):
            raise RuntimeError('A production process restarted during validation')
        try:
            verify_health(started)
            return
        except (RuntimeError, OSError, ValueError, KeyError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(2)


def install(sha, digest):
    if run('uname', '-m') != 'aarch64':
        raise ValueError('This receiver requires Ubuntu 24.04 ARM64')
    if shutil.disk_usage(ROOT).free < 3 * 1024**3:
        raise ValueError('At least 3 GiB free space required before staging')
    with urlopen('https://api.github.com/repos/hzy-hits/SpxOpDaily/commits/master',
                 timeout=30) as response:
        if json.load(response)['sha'] != sha:
            raise ValueError('Release is not current master; refusing stale or unmerged deployment')
    target = ROOT / 'releases' / sha
    if target.exists():
        if (target / 'archive.sha256').read_text().strip() != digest:
            raise ValueError('Existing release checksum differs; immutable releases cannot be replaced')
    else:
        with tempfile.TemporaryDirectory(prefix='stage-', dir=ROOT) as temporary:
            stage = Path(temporary)
            archive = stage / 'runtime.tar.gz'
            with urlopen(f'{BASE_URL}/runtime-{sha}/spx-runtime-arm64.tar.gz', timeout=120) as src:
                with archive.open('wb') as out:
                    shutil.copyfileobj(src, out)
            if checksum(archive) != digest:
                raise ValueError('Archive checksum mismatch')
            payload = stage / 'payload'
            with tarfile.open(archive) as package:
                package.extractall(payload, filter='data')
            if (payload / 'REVISION').read_text().strip() != sha:
                raise ValueError('Embedded revision mismatch')
            env = dict(os.environ, PYTHONPATH=str(payload / 'src'),
                       PYTHONDONTWRITEBYTECODE='1', SPX_SPARK_DISABLE_RUNTIME_OVERRIDES='1')
            run(str(payload / '.venv/bin/spx'), '--help', cwd=payload, env=env)
            for binary in ('spx-core', 'spx-bridge', 'spx-report', 'spx-delivery'):
                run(str(payload / 'bin' / binary), '--help')
            (payload / 'archive.sha256').write_text(digest + '\n')
            target.parent.mkdir(exist_ok=True)
            payload.rename(target)
    current = ROOT / 'current'
    previous = current.resolve() if current.is_symlink() else LEGACY
    if previous == target:
        if (set(active(True, PYTHON_UNITS)) != set(PYTHON_UNITS)
                or set(active(False, RUST_UNITS)) != set(RUST_UNITS)):
            raise RuntimeError('Installed release is unhealthy; operator recovery required')
        verify_health(time.time() - 60)
        print(json.dumps({'status': 'already_deployed', 'revision': sha}))
        return
    needs_migration = reviewed_migration(previous, target)
    old_rust = RUST / 'current'
    previous_rust = old_rust.resolve()
    # Preserve machine configuration, existing environment files, data and local unit hardening.
    for directory in ('data', 'logs'):
        if (LEGACY / directory).exists() and not (target / directory).exists():
            (target / directory).symlink_to(LEGACY / directory)
    units = sorted(p for p in UNITS.glob('spx*.service')
                   if '/home/ubuntu/spx-spark' in p.read_text())
    timers = active(True, [p.stem + '.timer' for p in units])
    running_python = active(True, PYTHON_UNITS)
    running_rust = active(False, RUST_UNITS)
    if set(running_python) != set(PYTHON_UNITS) or set(running_rust) != set(RUST_UNITS):
        raise ValueError('Baseline services are not all running; investigate before deployment')
    if active(True, [p.name for p in units if p.stem not in PYTHON_UNITS]):
        raise ValueError('A scheduled production job is running; retry after it finishes')
    changes = {}
    succeeded = False
    database_backup = None
    rust_target = RUST / 'releases' / sha
    run('sudo', '-n', 'mkdir', '-p', str(rust_target))
    run('sudo', '-n', 'cp', '-a', str(target / 'bin'), str(rust_target))
    started = datetime.now(timezone.utc).timestamp()
    try:
        if timers:
            run('systemctl', '--user', 'stop', *timers)
        run('systemctl', '--user', 'stop', *running_python)
        run('sudo', '-n', 'systemctl', 'stop', *running_rust)
        if needs_migration:
            database_backup = migrate_correlation(target)
        for unit in units:
            entries = [line.replace(str(LEGACY), str(current)) for line in unit.read_text().splitlines()
                       if line.startswith(('ExecStart=', 'ExecStartPre='))]
            reset = '[Service]\nWorkingDirectory=' + str(current) + '\nExecStart=\nExecStartPre=\n'
            reset += '\n'.join(entries) + '\n'
            reset += f'Environment=PYTHONPATH={current}/src SPX_SPARK_GIT_SHA={sha} PYTHONDONTWRITEBYTECODE=1 UV_NO_SYNC=1 UV_OFFLINE=1\n'
            reset += f'Environment=SPX_SPARK_RUNTIME_OVERRIDES={LEGACY}/config/runtime.local.toml\n'
            path = UNITS / (unit.name + '.d') / '90-cloud-release.conf'
            changes[path] = path.read_bytes() if path.exists() else None
            path.parent.mkdir(exist_ok=True)
            path.write_text(reset)
        atomic_link(target, current)
        run('sudo', '-n', 'ln', '-sfn', str(rust_target), str(old_rust))
        run('systemctl', '--user', 'daemon-reload')
        run('systemctl', '--user', 'start', *running_python)
        run('sudo', '-n', 'systemctl', 'start', *running_rust)
        processes = {}
        for prefix, names in [(['systemctl', '--user'], running_python),
                              (['systemctl'], running_rust)]:
            for name in names:
                args = (*prefix, 'show', name, '-p', 'MainPID', '--value')
                processes[args] = run(*args)
        time.sleep(15)
        if (set(active(True, running_python)) != set(running_python)
                or set(active(False, running_rust)) != set(running_rust)):
            raise RuntimeError('Post-deployment service validation failed')
        wait_ready(started, processes)
        ROOT.joinpath('previous.json').write_text(json.dumps({
            'python': str(previous), 'rust': str(previous_rust), 'release': sha,
            'database_backup': database_backup,
            'dropins': {str(p): v.decode() if v is not None else None for p, v in changes.items()}
        }, indent=2) + '\n')
        succeeded = True
    except BaseException:
        run('systemctl', '--user', 'stop', *running_python)
        run('sudo', '-n', 'systemctl', 'stop', *running_rust)
        for path, content in changes.items():
            if content is None:
                path.unlink(missing_ok=True)
            else:
                path.write_bytes(content)
        if previous != LEGACY:
            atomic_link(previous, current)
        else:
            current.unlink(missing_ok=True)
        run('sudo', '-n', 'ln', '-sfn', str(previous_rust), str(old_rust))
        run('systemctl', '--user', 'daemon-reload')
        run('systemctl', '--user', 'start', *running_python)
        run('sudo', '-n', 'systemctl', 'start', *running_rust)
        raise
    finally:
        if succeeded:
            timers = [t for t in timers if t != 'spx-spark-backtest-weekly.timer']
        if timers:
            run('systemctl', '--user', 'start', *timers)
    run('systemctl', '--user', 'disable', '--now', 'spx-spark-backtest-weekly.timer')
    print(json.dumps({'status': 'deployed', 'revision': sha, 'archive_sha256': digest,
                      'previous_python': str(previous), 'previous_rust': str(previous_rust),
                      'cutover_seconds': round(time.time() - started, 1)}))


if __name__ == '__main__':
    try:
        revision, archive_digest = command(os.environ.get('SSH_ORIGINAL_COMMAND', ' '.join(sys.argv[1:])))
        ROOT.mkdir(exist_ok=True, mode=0o700)
        with (ROOT / 'deploy.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            install(revision, archive_digest)
    except Exception as error:
        # Never echo captured service output, URLs with credentials or environment contents.
        print(json.dumps({'status': 'failed', 'reason': str(error)}), file=sys.stderr)
        sys.exit(1)
