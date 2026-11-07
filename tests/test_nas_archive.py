import json
from pathlib import Path
import tempfile
import gzip
import os
import shutil
import time
import unittest
from datetime import date, timedelta
from unittest.mock import patch
import importlib.util

_spec = importlib.util.spec_from_file_location('nas_archive', Path(__file__).parents[1] / 'scripts/archive-nas.py')
archive = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(archive)


class ArchiveSafetyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.frames = self.root / 'frames'
        self.frames.mkdir()
        self.patcher = patch.object(archive, 'FRAMES', self.frames)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.folder = self.root / 'date=2026-08-01'
        self.folder.mkdir()
        self.payload = self.folder / 'frames.ndjson.zst'
        self.payload.write_bytes(b'archive bytes')
        self.manifest = self.folder / 'manifest.json'
        self.manifest.write_text(json.dumps({
            'schema_version': 'spx_raw_frame_archive.v1', 'status': 'verified',
            'utc_date': '2026-08-01', 'archive_file': self.payload.name,
            'archive_size_bytes': self.payload.stat().st_size}))

    def test_verified_copy_removes_payload_keeps_receipt_and_manifest(self):
        destination = self.root / 'nas'
        destination.mkdir()
        def transfer(folder, expected):
            for name, size in expected.items():
                (destination / name).write_bytes((folder / name).read_bytes())
                self.assertEqual(archive.file_size(destination / name), size)
        archive.migrate(self.folder, transfer)
        self.assertFalse(self.payload.exists())
        self.assertTrue(self.manifest.exists())
        self.assertTrue((self.folder / 'nas-receipt.json').exists())
        self.assertEqual(archive.eligible(self.root, date(2026, 10, 5)), [])

    def test_nas_failure_keeps_source(self):
        def fail(*args):
            raise RuntimeError('NAS unavailable or size mismatch')
        with self.assertRaises(RuntimeError):
            archive.migrate(self.folder, fail)
        self.assertTrue(self.payload.exists())
        self.assertFalse((self.folder / 'nas-receipt.json').exists())

    def test_local_size_mismatch_is_never_transferred(self):
        self.payload.write_bytes(b'corrupt')
        with self.assertRaises(ValueError):
            archive.migrate(self.folder, lambda *args: self.fail('Must not transfer'))
        self.assertTrue(self.payload.exists())

    def test_changed_source_not_deleted(self):
        def change(*args):
            self.payload.write_bytes(b'new data')
        with self.assertRaises(ValueError):
            archive.migrate(self.folder, change)
        self.assertEqual(self.payload.read_bytes(), b'new data')

    def test_raw_source_protects_archive(self):
        (self.frames / '2026-08-01.0000.ndjson').write_text('raw')
        with self.assertRaises(RuntimeError):
            archive.migrate(self.folder, lambda *args: self.fail('Must not transfer'))
        self.assertTrue(self.payload.exists())

    def test_retention_boundary_and_symlink(self):
        self.assertEqual(archive.eligible(self.root, date(2026, 8, 31)), [])
        self.assertEqual(archive.eligible(self.root, date(2026, 9, 1)), [self.folder])
        self.payload.unlink()
        self.payload.symlink_to(self.manifest)
        with self.assertRaises((OSError, ValueError)):
            archive.inspect(self.folder)


class FeatureArchiveTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.features = self.root / 'features'
        self.receipts = self.root / 'receipts'
        for key, value in [('FEATURES', self.features), ('RECEIPTS', self.receipts)]:
            patcher = patch.object(archive, key, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.day = str(date.today() - timedelta(days=60))
        self.folder = self.features / 'level_decision_health' / ('date=' + self.day)
        self.folder.mkdir(parents=True)
        self.source = self.folder / 'samples.jsonl'
        self.content = b'{"health":"ready"}\n' * 100
        self.source.write_bytes(self.content)
        os.utime(self.source, (time.time()-72*3600, time.time()-72*3600))
        self.nas = self.root / 'nas'
        self.nas.mkdir()

    def transfer(self, folder, expected, target):
        for name, size in expected.items():
            shutil.copyfile(folder / name, self.nas / name)
            self.assertEqual((self.nas / name).stat().st_size, size)

    def test_compressed_archive_and_restore_at_original_report_path(self):
        archive.migrate_features(self.folder, self.transfer)
        self.assertFalse(self.source.exists())
        self.assertEqual(gzip.decompress((self.nas / 'samples.jsonl.gz').read_bytes()), self.content)
        def download(command, **kwargs):
            shutil.copyfile(self.nas / 'samples.jsonl.gz', Path(command[-1]) / 'samples.jsonl.gz')
        with patch.object(archive.subprocess, 'run', download):
            archive.restore_features(self.day)
        self.assertEqual(self.source.read_bytes(), self.content)

    def test_failed_transfer_does_not_delete_feature(self):
        def fail(*args):
            raise RuntimeError('NAS unavailable')
        with self.assertRaises(RuntimeError):
            archive.migrate_features(self.folder, fail)
        self.assertTrue(self.source.exists())
        self.assertFalse(self.receipts.exists())

    def test_recently_changed_old_partition_is_not_archived(self):
        self.source.touch()
        self.assertEqual(archive.feature_candidates(date.today()), [])

    def test_changed_feature_during_copy_is_not_deleted(self):
        def change(*args):
            self.source.write_bytes(b'new data')
        with self.assertRaises(RuntimeError):
            archive.migrate_features(self.folder, change)
        self.assertEqual(self.source.read_bytes(), b'new data')


if __name__ == '__main__':
    unittest.main()


class ColdArchiveTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.backups = root / 'backups'
        self.folder = self.backups / 'operational-history'
        self.folder.mkdir(parents=True)
        self.payload = b'{"table":"decisions","rows":[],"decision_legs":[]}'
        name = archive.hashlib.sha256(self.payload).hexdigest() + '.json.gz'
        self.source = self.folder / name
        self.source.write_bytes(gzip.compress(self.payload))
        for key, value in [('BACKUPS', self.backups), ('RELEASE_BACKUPS', root / 'release-backups')]:
            patcher = patch.object(archive, key, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_failed_remote_copy_preserves_source(self):
        def fail(*args):
            raise RuntimeError('interrupted')
        with self.assertRaises(RuntimeError):
            archive.migrate_cold_file(self.source, fail)
        self.assertTrue(self.source.exists())
        self.assertFalse(self.source.with_suffix('.gz.nas-receipt.json').exists())

    def test_cold_payload_upload_then_grace_retirement_rechecks_remote(self):
        calls = []
        def transfer(folder, expected, target):
            calls.append(expected)
            return {name: archive.digest(folder / name) for name in expected}
        assert archive.migrate_cold_file(self.source, transfer) == 0
        assert self.source.exists()
        assert archive.migrate_cold_file(self.source, transfer) == 0
        assert len(calls) == 1
        old = time.time() - 8 * 86400
        os.utime(self.source, (old, old))
        assert archive.migrate_cold_file(self.source, transfer) > 0
        assert len(calls) == 2
        assert not self.source.exists()
        assert self.source.with_suffix('.gz.nas-receipt.json').exists()

    def test_wrong_content_address_cannot_be_migrated(self):
        self.source.write_bytes(gzip.compress(b'different rows'))
        with self.assertRaises(ValueError):
            archive.migrate_cold_file(self.source, lambda *args: self.fail('must not transfer'))
        self.assertTrue(self.source.exists())

    def test_unverified_database_and_live_database_are_excluded(self):
        retired = self.backups / 'sqlite-before-retention-example'
        retired.mkdir()
        (retired / 'spx.sqlite.zst').write_bytes(b'unverified')
        (self.backups / 'spx.sqlite').write_bytes(b'live')
        assert archive.cold_candidates() == [self.source]


def test_transport_detects_same_size_remote_corruption(tmp_path, monkeypatch):
    import shlex
    import subprocess
    source = tmp_path / 'source'
    target = tmp_path / 'nas'
    source.mkdir()
    (source / 'payload').write_bytes(b'good')
    real_run = subprocess.run
    def remote(command, timeout=180):
        # Run the actual remote preparation/hash verifier against an isolated
        # directory, replacing only SSH and the NAS mount boundary.
        return real_run(shlex.split(command.split(' && ', 1)[1]), check=True,
                        text=True, capture_output=True).stdout
    def transfer(command, **kwargs):
        assert '--whole-file' not in command
        assert '--partial-dir=.partial' in command
        (target / 'payload').write_bytes(b'evil')
    monkeypatch.setattr(archive, 'remote', remote)
    monkeypatch.setattr(archive.subprocess, 'run', transfer)
    with unittest.TestCase().assertRaises(subprocess.CalledProcessError):
        archive.copy_and_verify(source, {'payload': 4}, str(target))
    assert (source / 'payload').read_bytes() == b'good'
