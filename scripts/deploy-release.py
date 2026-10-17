#!/usr/bin/env python3
"""Restricted Oracle release receiver; no checkout, dependency install or compilation."""
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
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


def verify_health(started):
    for path in HEALTH_FILES:
        payload = json.loads(path.read_text())
        updated = datetime.fromisoformat(payload['updated_at'].replace('Z', '+00:00'))
        if updated.timestamp() < started or payload.get('phase') == 'halted':
            raise RuntimeError('Runtime health did not advance after deployment')
        if 'socket_connected' in payload and not payload['socket_connected']:
            raise RuntimeError('Normalized bridge has not reconnected')


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
    if schema_files(previous) != schema_files(target):
        raise ValueError('Database migration changed; separate migration review required')
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
    rust_target = RUST / 'releases' / sha
    run('sudo', '-n', 'mkdir', '-p', str(rust_target))
    run('sudo', '-n', 'cp', '-a', str(target / 'bin'), str(rust_target))
    started = datetime.now(timezone.utc).timestamp()
    try:
        if timers:
            run('systemctl', '--user', 'stop', *timers)
        run('systemctl', '--user', 'stop', *running_python)
        run('sudo', '-n', 'systemctl', 'stop', *running_rust)
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
        if any(pid == '0' or run(*args) != pid for args, pid in processes.items()):
            raise RuntimeError('A production process restarted during validation')
        if (set(active(True, running_python)) != set(running_python)
                or set(active(False, running_rust)) != set(running_rust)):
            raise RuntimeError('Post-deployment service validation failed')
        verify_health(started)
        ROOT.joinpath('previous.json').write_text(json.dumps({
            'python': str(previous), 'rust': str(previous_rust), 'release': sha,
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
                      'previous_python': str(previous), 'previous_rust': str(previous_rust)}))


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
