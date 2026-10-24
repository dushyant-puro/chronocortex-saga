"""Bounded SQLite backup bundles and in-memory restore drills; never activate."""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import struct
import tempfile
import time

from scripts.recovery_preflight import inspect, pairs, reject_constant, require


MAGIC = b'CCS-JOURNAL-BACKUP-1\n'
MAX_BYTES = 64 * 1024 * 1024
MAX_MANIFEST = 4096


def private_directory(path):
    """Windows requires an administrator-protected inherited ACL (see runbook)."""
    path = Path(path).absolute()
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and not path.is_symlink())
    for ancestor in (path, *path.parents):
        require(not (ancestor / '.git').exists())
        attributes = getattr(ancestor.lstat(), 'st_file_attributes', 0)
        require(not ancestor.is_symlink() and not attributes & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0))
    if os.name != 'nt':
        require(info.st_uid == os.geteuid() and not info.st_mode & 0o077)
    return path.resolve(strict=True)


def header(path):
    require(path.is_file() and path.stat().st_size <= MAX_BYTES)
    with path.open('rb') as stream:
        value = stream.read(20)
    require(value[:16] == b'SQLite format 3\x00' and value[18:20] == b'\x01\x01')


def copy_database(source, target):
    """Pin one read transaction; provider/task clocks are copied, never reset."""
    deadline = time.monotonic() + 10
    def progress(status, remaining, total):
        require(time.monotonic() < deadline)
    header(source)
    with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True, timeout=.25,
                                isolation_level=None)) as reader:
        reader.execute('PRAGMA query_only=ON')
        reader.execute('PRAGMA trusted_schema=OFF')
        reader.execute('BEGIN')
        require(reader.execute('PRAGMA application_id').fetchone()[0] == 0x43435341)
        require(reader.execute('PRAGMA user_version').fetchone()[0] == 6)
        require(reader.execute('PRAGMA journal_mode').fetchone()[0] == 'delete')
        require(reader.execute('PRAGMA page_count').fetchone()[0] *
                reader.execute('PRAGMA page_size').fetchone()[0] <= MAX_BYTES)
        reader.backup(target, pages=64, progress=progress, sleep=.01)
        reader.rollback()


def new_file(path):
    return os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                            getattr(os, 'O_BINARY', 0), 0o600), 'wb')


def verdict(operation):
    return {'schema': 1, 'operation': operation, 'passed': False,
            'restore_authorized': False, 'dispatch_authorized': False,
            'freshness_verified': False, 'provider_reconciled': False,
            'authenticity_verified': False}


def publish_no_clobber(pending, destination):
    """Publish a flushed file without replacing evidence; no copy fallback."""
    os.link(pending, destination)
    if os.name != 'nt':
        descriptor = os.open(destination.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def capture(journal, bundle):
    report = verdict('CAPTURE')
    try:
        source = Path(journal).resolve(strict=True)
        destination = Path(bundle).absolute()
        parent = private_directory(destination.parent)
        destination = parent / destination.name
        require(not os.path.lexists(destination))
        require(destination not in {source, *(Path(str(source) + suffix) for suffix in ('-journal', '-wal', '-shm'))})
        with tempfile.TemporaryDirectory(prefix='.ccs-backup-', dir=parent) as directory:
            snapshot = Path(directory) / 'snapshot.db'
            with new_file(snapshot):
                pass
            with closing(sqlite3.connect(snapshot)) as writer:
                writer.execute('PRAGMA synchronous=EXTRA')
                copy_database(source, writer)
            evidence = inspect(snapshot)
            require(evidence['passed'])
            require(snapshot.stat().st_size <= MAX_BYTES)
            raw = snapshot.read_bytes()
            digest = hashlib.sha256(raw).hexdigest()
            manifest = {'schema': 1, 'journal_schema': 6, 'sha256': digest, 'bytes': len(raw),
                        'captured_at': datetime.now(timezone.utc).isoformat()}
            metadata = json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode('ascii')
            pending = Path(directory) / 'bundle.partial'
            with new_file(pending) as stream:
                stream.write(MAGIC + struct.pack('>I', len(metadata)) + metadata + raw)
                stream.flush()
                os.fsync(stream.fileno())
            # Atomic no-clobber publication on supported local filesystems.
            # Never fall back to replace(), which could destroy existing evidence.
            publish_no_clobber(pending, destination)
            report.update(passed=True, snapshot_sha256=digest, snapshot_bytes=len(raw),
                          counts=evidence['counts'], uncertain_actions=evidence['uncertain_actions'])
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError, RecursionError):
        report['passed'] = False
        report['failure'] = 'BACKUP_UNVERIFIED'
    return report


def read_bundle(path):
    # Bound allocation and reject trailing/truncated data before opening SQLite.
    require(stat.S_ISREG(os.lstat(path).st_mode))
    flags = os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
    with os.fdopen(os.open(path, flags), 'rb') as stream:
        require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode))
        require(os.fstat(stream.fileno()).st_size <= MAX_BYTES + MAX_MANIFEST + len(MAGIC) + 4)
        require(stream.read(len(MAGIC)) == MAGIC)
        length = stream.read(4)
        require(len(length) == 4)
        size = struct.unpack('>I', length)[0]
        require(0 < size <= MAX_MANIFEST)
        manifest = json.loads(stream.read(size).decode('ascii'), object_pairs_hook=pairs,
                              parse_constant=reject_constant)
        require(type(manifest) is dict and set(manifest) == {'schema', 'journal_schema', 'sha256', 'bytes', 'captured_at'})
        require(type(manifest['schema']) is int and manifest['schema'] == 1)
        require(type(manifest['journal_schema']) is int and manifest['journal_schema'] == 6)
        require(type(manifest['bytes']) is int and 0 < manifest['bytes'] <= MAX_BYTES)
        require(type(manifest['sha256']) is str and len(manifest['sha256']) == 64)
        require(type(manifest['captured_at']) is str and len(manifest['captured_at']) <= 64)
        require(datetime.fromisoformat(manifest['captured_at']).utcoffset() is not None)
        raw = stream.read(manifest['bytes'] + 1)
        require(len(raw) == manifest['bytes'] and hashlib.sha256(raw).hexdigest() == manifest['sha256'])
    return manifest, raw


def logical_digest(db):
    deadline = time.monotonic() + 10
    db.execute('PRAGMA trusted_schema=OFF')
    db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
    result, size = hashlib.sha256(), 0
    for statement in db.iterdump():
        encoded = statement.encode('utf-8')
        size += len(encoded)
        require(size <= MAX_BYTES * 4 and time.monotonic() < deadline)
        result.update(struct.pack('>Q', len(encoded)))
        result.update(encoded)
    return result.digest()


def drill(bundle, workspace):
    report = verdict('RESTORE_DRILL')
    try:
        parent = private_directory(workspace)
        manifest, raw = read_bundle(bundle)
        with tempfile.TemporaryDirectory(prefix='.ccs-drill-', dir=parent) as directory:
            snapshot = Path(directory) / 'snapshot.db'
            with new_file(snapshot) as stream:
                stream.write(raw)
            evidence = inspect(snapshot)
            require(evidence['passed'])
            with closing(sqlite3.connect(':memory:')) as restored:
                copy_database(snapshot, restored)
                restored.execute('PRAGMA query_only=ON')
                require(restored.execute('PRAGMA integrity_check(1)').fetchall() == [('ok',)])
                require(restored.execute('PRAGMA foreign_key_check').fetchone() is None)
                # Full logical rows, schema and sequence values survive restoration.
                with closing(sqlite3.connect(snapshot.as_uri() + '?mode=ro', uri=True)) as expected:
                    require(logical_digest(restored) == logical_digest(expected))
            report.update(passed=True, snapshot_sha256=manifest['sha256'],
                          counts=evidence['counts'], uncertain_actions=evidence['uncertain_actions'],
                          restored_to_memory=True)
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError, RecursionError):
        report['passed'] = False
        report['failure'] = 'RESTORE_DRILL_UNVERIFIED'
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    commands = parser.add_subparsers(dest='command', required=True)
    backup = commands.add_parser('capture', allow_abbrev=False)
    backup.add_argument('--journal', required=True)
    backup.add_argument('--bundle', required=True)
    restore = commands.add_parser('drill', allow_abbrev=False)
    restore.add_argument('--bundle', required=True)
    restore.add_argument('--workspace', required=True)
    args = parser.parse_args()
    result = capture(args.journal, args.bundle) if args.command == 'capture' else drill(args.bundle, args.workspace)
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    raise SystemExit(0 if result['passed'] else 2)


if __name__ == '__main__':
    main()
