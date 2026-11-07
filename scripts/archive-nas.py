#!/usr/bin/env python3
"""Archive cold SPX diagnostics/frames and verified operational backups to NAS."""
import argparse
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import stat
import subprocess
import tempfile
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone

SOURCE = Path('/srv/data/spx-spark/rust-core-shadow/archive')
FRAMES = SOURCE.parent / 'frames'
DEST = '/mnt/nas/spx-spark/oracle/rust-core-shadow/archive'
HOST = 'ivena@100.109.146.30'
FEATURES = Path('/srv/data/spx-spark/data/features')
FEATURE_NAMES = ('level_decision_health', 'spring_gamma_v3', 'trade_intents',
                 'level_trigger_repricing', 'gth_detector_health')
FEATURE_DEST = '/mnt/nas/spx-spark/oracle/data/features'
RECEIPTS = Path('/srv/data/spx-spark/nas-feature-receipts')
BACKUPS = Path('/srv/data/spx-spark/backups')
RELEASE_BACKUPS = Path('/home/ubuntu/spx-runtime/backups')
COLD_DEST = '/mnt/nas/spx-spark/oracle/backups'
SSH = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
       '-o', 'ControlMaster=auto', '-o', 'ControlPersist=600',
       '-o', 'ControlPath=/home/ubuntu/spx-nas-archive/ssh-%C',
       '-o', 'ConnectTimeout=60', '-o', 'IPQoS=none', '-o', 'ServerAliveInterval=15',
       '-o', 'ServerAliveCountMax=3', '-i', '/home/ubuntu/.ssh/id_ed25519_quant_pi']
MOUNT_GATE = ('findmnt -rn -M /mnt/nas -t nfs,nfs4 '
              '-S 192.168.1.3:/volume1/RaspberryPi_Data >/dev/null')


def file_size(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f'Not a regular file: {path}')
    return info.st_size


@contextmanager
def lock(path, kind):
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, kind | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


def eligible(root, today):
    cutoff = today - timedelta(days=30)
    result = []
    for folder in sorted(root.glob('date=*')):
        day = date.fromisoformat(folder.name.removeprefix('date='))
        if day >= cutoff:
            continue
        if folder.is_symlink() or not folder.is_dir():
            raise ValueError(f'Unsafe directory: {folder}')
        if (folder / 'frames.ndjson.zst').exists():
            result.append(folder)
    return result


def inspect(folder):
    if folder.resolve() != folder:
        raise ValueError('Symlinked archive path')
    manifest_path = folder / 'manifest.json'
    manifest_size = file_size(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    if (manifest.get('schema_version') != 'spx_raw_frame_archive.v1'
            or manifest.get('status') != 'verified'
            or manifest.get('utc_date') != folder.name[5:]
            or manifest.get('archive_file') != 'frames.ndjson.zst'):
        raise ValueError(f'Unverified manifest: {folder}')
    archive = folder / 'frames.ndjson.zst'
    if file_size(archive) != manifest['archive_size_bytes']:
        raise ValueError(f'Local size mismatch: {folder}')
    return {'manifest.json': manifest_size,
            'frames.ndjson.zst': manifest['archive_size_bytes']}


def remote(command, timeout=180):
    return subprocess.run(SSH + [HOST, command], check=True, text=True,
                          capture_output=True, timeout=timeout).stdout


def copy_and_verify(folder, expected, target=None):
    target = target or DEST + '/' + folder.name
    # Confirm the actual NAS export is mounted, never fall back to Pi SD storage.
    prepare = """import os, pathlib, sys
p = pathlib.Path(sys.argv[1])
if p.resolve() != p: raise RuntimeError('Symlinked NAS destination')
p.mkdir(parents=True, exist_ok=True, mode=0o700)
if p.resolve() != p: raise RuntimeError('Symlinked NAS destination')
"""
    remote(MOUNT_GATE + ' && python3 -c ' + shlex.quote(prepare) + ' ' + shlex.quote(target))
    subprocess.run(['rsync', '-rt', '--checksum', '--fsync',
                    '--partial-dir=.partial', '--timeout=120', '--bwlimit=8192',
                    '-e', shlex.join(SSH), '--rsync-path=' + MOUNT_GATE + ' && rsync',
                    *[str(folder / name) for name in expected],
                    HOST + ':' + target + '/'], check=True, timeout=21600)
    hashes = {name: digest(folder / name) for name in expected}
    verify = """import hashlib, json, pathlib, stat, sys
p = pathlib.Path(sys.argv[1])
if p.resolve() != p: raise RuntimeError('Symlinked NAS destination')
expected = json.loads(sys.argv[2])
hashes = json.loads(sys.argv[3])
for name, wanted in expected.items():
    info = (p / name).lstat()
    if not stat.S_ISREG(info.st_mode): raise RuntimeError('Not regular')
    if info.st_size != wanted: raise RuntimeError('NAS size mismatch: '+name)
    with (p / name).open('rb') as stream:
        if hashlib.file_digest(stream, 'sha256').hexdigest() != hashes[name]:
            raise RuntimeError('NAS hash mismatch: '+name)
print('VERIFIED')
"""
    output = remote(MOUNT_GATE + ' && python3 -c ' + shlex.quote(verify) + ' '
                    + shlex.quote(target) + ' ' + shlex.quote(json.dumps(expected)) + ' '
                    + shlex.quote(json.dumps(hashes)), timeout=900)
    if output.strip() != 'VERIFIED':
        raise RuntimeError('Missing NAS verification acknowledgment')
    return hashes


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def migrate(folder, transport=copy_and_verify):
    day = folder.name[5:]
    with lock(FRAMES / '.spx-raw-log.lock', fcntl.LOCK_SH), \
            lock(FRAMES / f'.spx-raw-log-date-{day}.lock', fcntl.LOCK_EX):
        # Never remove the archive barrier while any corresponding raw segments remain.
        if any(FRAMES.glob(day + '*.ndjson')):
            raise RuntimeError(f'Raw segments still present for {day}')
        expected = inspect(folder)
        archive = folder / 'frames.ndjson.zst'
        before = archive.stat()
        hashes = transport(folder, expected)
        after = archive.stat()
        identity_fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        if (inspect(folder) != expected or
                any(getattr(after, key) != getattr(before, key) for key in identity_fields)):
            raise RuntimeError(f'Source changed during copy: {day}')
        receipt = {'verified_at': datetime.now(timezone.utc).isoformat(),
                   'destination': HOST + ':' + DEST + '/' + folder.name,
                   'verification': 'remote_sha256_and_file_sizes', 'sha256': hashes,
                   'file_sizes': expected, 'bytes': before.st_size}
        pending = folder / '.nas-receipt.tmp'
        with pending.open('w') as stream:
            json.dump(receipt, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        pending.replace(folder / 'nas-receipt.json')
        fd = os.open(folder, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
            archive.unlink()
            os.fsync(fd)
        finally:
            os.close(fd)
        print(json.dumps({'moved_day': day, 'freed_bytes': before.st_size}), flush=True)
        return before.st_size


def identity(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f'Not a regular file: {path}')
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


def feature_candidates(today):
    cutoff = today - timedelta(days=30)
    quiet_before = datetime.now(timezone.utc).timestamp() - 48 * 3600
    result = []
    for dataset in FEATURE_NAMES:
        for folder in sorted((FEATURES / dataset).glob('date=*')):
            if date.fromisoformat(folder.name[5:]) >= cutoff:
                continue
            if folder.resolve() != folder or not folder.is_dir():
                raise ValueError(f'Unsafe feature directory: {folder}')
            files = list(folder.glob('*.jsonl'))
            if files and all(file_size(p) >= 0 and p.stat().st_mtime < quiet_before for p in files):
                result.append(folder)
    return result


def save_receipt(path, receipt):
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_suffix('.tmp')
    with pending.open('w') as stream:
        json.dump(receipt, stream, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    pending.replace(path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def migrate_features(folder, transport=copy_and_verify):
    dataset, day = folder.parent.name, folder.name
    if dataset not in FEATURE_NAMES or folder.resolve() != folder:
        raise ValueError('Unexpected feature archive path')
    # These five producers append to current sessions only. Require old date and
    # 48 hours without changes; do not take their live, global writer locks.
    if folder not in feature_candidates(datetime.now(timezone.utc).date()):
        raise ValueError('Feature partition is recent or changed recently')
    files = sorted(folder.glob('*.jsonl'))
    before = {p.name: identity(p) for p in files}
    target = FEATURE_DEST + '/' + dataset + '/' + day
    with tempfile.TemporaryDirectory(prefix='spx-nas-feature-') as temp:
        stage = Path(temp)
        for path in files:
            with path.open('rb') as src, (stage / (path.name + '.gz')).open('wb') as packed:
                # Stable bytes let rsync reuse the prior partial transfer.
                with gzip.GzipFile(filename='', fileobj=packed, mode='wb', compresslevel=1, mtime=0) as dst:
                    shutil.copyfileobj(src, dst, 1024 * 1024)
        packed = {p.name: file_size(p) for p in stage.iterdir()}
        hashes = transport(stage, packed, target)
        if before != {p.name: identity(p) for p in folder.glob('*.jsonl')}:
            raise RuntimeError(f'Feature source changed during transfer: {folder}')
        receipt = {'dataset': dataset, 'date': day[5:], 'destination': target,
                   'completed_at': datetime.now(timezone.utc).isoformat(),
                   'verification': 'remote_sha256_and_file_sizes', 'sha256': hashes, 'compression': 'gzip-1',
                   'source_sizes': {name: info[2] for name, info in before.items()},
                   'compressed_sizes': packed}
        save_receipt(RECEIPTS / dataset / (day + '.json'), receipt)
        for path in files:
            if identity(path) != before[path.name]:
                raise RuntimeError(f'Feature source changed before removal: {path}')
            path.unlink()
        fd = os.open(folder, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        total = sum(receipt['source_sizes'].values())
        print(json.dumps({'feature': dataset, 'moved_day': day[5:], 'freed_bytes': total,
                          'nas_compressed_bytes': sum(packed.values())}), flush=True)
        return total


def restore_features(day):
    if day != 'all':
        date.fromisoformat(day)
    receipts = sorted(RECEIPTS.glob('*/date=*.json' if day == 'all' else f'*/date={day}.json'))
    if not receipts:
        raise ValueError('No matching completed feature archive receipts')
    for path in receipts:
        receipt = json.loads(path.read_text())
        dataset = receipt['dataset']
        date.fromisoformat(receipt['date'])
        if dataset not in FEATURE_NAMES:
            raise ValueError('Unexpected receipt dataset')
        target = FEATURE_DEST + '/' + dataset + '/date=' + receipt['date']
        folder = FEATURES / dataset / ('date=' + receipt['date'])
        if folder.resolve() != folder:
            raise ValueError('Symlinked restore destination')
        folder.mkdir(parents=True, exist_ok=True)
        for name, size in receipt['source_sizes'].items():
            if Path(name).name != name or not name.endswith('.jsonl'):
                raise ValueError('Unsafe receipt filename')
            output = folder / name
            if output.exists():
                if file_size(output) != size:
                    raise RuntimeError(f'Restore would overwrite different local file: {output}')
                continue
            # Staging is on the same filesystem, allowing atomic no-overwrite publication.
            with tempfile.TemporaryDirectory(prefix='.nas-restore-', dir=folder) as temp:
                stage = Path(temp)
                subprocess.run(['rsync', '-rt', '--whole-file', '--fsync', '--timeout=120',
                                '-e', shlex.join(SSH), '--rsync-path=' + MOUNT_GATE + ' && rsync',
                                HOST + ':' + target + '/' + name + '.gz', str(stage) + '/'],
                               check=True, timeout=21600)
                packed = stage / (name + '.gz')
                if file_size(packed) != receipt['compressed_sizes'][name + '.gz']:
                    raise RuntimeError('Compressed restore file size mismatch')
                expected_hash = (receipt.get('sha256') or {}).get(name + '.gz')
                if expected_hash and digest(packed) != expected_hash:
                    raise RuntimeError('Compressed restore content hash mismatch')
                pending = stage / name
                with gzip.open(packed, 'rb') as src, pending.open('wb') as dst:
                    shutil.copyfileobj(src, dst, 1024 * 1024)
                    dst.flush()
                    os.fsync(dst.fileno())
                if file_size(pending) != size:
                    raise RuntimeError('Restored file size mismatch')
                os.link(pending, output)
            print(json.dumps({'restored': str(output), 'bytes': size}), flush=True)


def cold_candidates():
    # Only finalized archives owned by operational retention/verified DB
    # compression. Never walk the live database, lake or arbitrary backups.
    files = list((BACKUPS / 'operational-history').glob('*.json.gz'))
    files += [p for p in BACKUPS.glob('sqlite-before-retention-*/spx.sqlite.zst')
              if p.with_suffix('.zst.json').is_file()]
    files += [p for p in RELEASE_BACKUPS.glob('*.sqlite.zst')
              if p.with_suffix('.zst.json').is_file()]
    return sorted(p for p in files if p.resolve() == p and p.is_file())


def migrate_cold_file(path, transport=copy_and_verify):
    if path not in cold_candidates():
        raise ValueError('Not a finalized operational archive')
    before = identity(path)
    expected = {path.name: before[2]}
    if path.name.endswith('.json.gz'):
        with gzip.open(path, 'rb') as stream:
            if hashlib.file_digest(stream, 'sha256').hexdigest() != path.name[:-8]:
                raise ValueError('Operational archive content hash mismatch')
    else:
        proof_path = path.with_suffix(path.suffix + '.json')
        proof = json.loads(proof_path.read_text())
        if (proof.get('verification') != 'decompressed_sha256_and_size'
                or proof.get('archive_bytes') != before[2]
                or proof.get('archive') != str(path)):
            raise ValueError('Missing verified database compression receipt')
        expected[proof_path.name] = file_size(proof_path)
    relative = ('releases/' + path.name if path.parent == RELEASE_BACKUPS
                else str(path.relative_to(BACKUPS)))
    target = COLD_DEST + '/' + str(Path(relative).parent)
    receipt_path = path.with_suffix(path.suffix + '.nas-receipt.json')
    receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else {}
    # Reuse only our immutable transfer receipt, tied to the source identity.
    recent = datetime.now(timezone.utc).timestamp() - before[3] / 1e9 < 7 * 86400
    if not recent or receipt.get('source_identity') != before or receipt.get('destination') != target:
        hashes = transport(path.parent, expected, target)
        if identity(path) != before:
            raise RuntimeError('Cold archive changed during transfer')
        receipt = {'source_identity': before, 'destination': target,
                   'verified_at': datetime.now(timezone.utc).isoformat(),
                   'verification': 'remote_sha256_and_file_sizes', 'sha256': hashes,
                   'file_sizes': expected}
        save_receipt(receipt_path, receipt)
    # Keep a week of compressed rollback copies; upload immediately, retire
    # only after verified transfer and the local grace. Payloads remain on NAS.
    if recent:
        return 0
    if identity(path) != before:
        raise RuntimeError('Cold archive changed before removal')
    path.unlink()
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    print(json.dumps({'cold_archive': relative, 'freed_bytes': before[2]}), flush=True)
    return before[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true', help='Copy, verify, then remove local archive payloads')
    parser.add_argument('--limit', type=int, default=0, help='Limit dates per run; zero means all')
    parser.add_argument('--features-only', action='store_true', help='Process only the five diagnostic datasets')
    parser.add_argument('--restore-features', metavar='DATE_OR_ALL', help='Restore archived JSONL to original report paths')
    args = parser.parse_args()
    with lock(Path('/home/ubuntu/spx-nas-archive/archive.lock'), fcntl.LOCK_EX):
        if args.restore_features:
            restore_features(args.restore_features)
            return
        today = datetime.now(timezone.utc).date()
        folders = [] if args.features_only else eligible(SOURCE, today)
        feature_folders = feature_candidates(today)
        cold_files = [] if args.features_only else cold_candidates()
        if args.limit:
            folders = folders[:args.limit]
            feature_folders = feature_folders[:args.limit]
            cold_files = cold_files[:args.limit]
        print(json.dumps({'execute': args.execute, 'utc_cutoff_exclusive': str(today - timedelta(days=30)),
                          'days': len(folders), 'bytes': sum((p / 'frames.ndjson.zst').stat().st_size for p in folders)}), flush=True)
        total = 0
        print(json.dumps({'feature_partitions': len(feature_folders),
                          'feature_bytes': sum(file_size(p) for f in feature_folders for p in f.glob('*.jsonl'))}), flush=True)
        for folder in feature_folders:
            if args.execute:
                total += migrate_features(folder)
        for folder in folders:
            if args.execute:
                total += migrate(folder)
            else:
                inspect(folder)
        print(json.dumps({'cold_archives': len(cold_files)}), flush=True)
        for path in cold_files:
            if args.execute:
                total += migrate_cold_file(path)
        print(json.dumps({'success': True, 'freed_bytes': total}), flush=True)


if __name__ == '__main__':
    main()
