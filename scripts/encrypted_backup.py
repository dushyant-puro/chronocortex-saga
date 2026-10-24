"""Authenticated encrypted journal bundles; keys stay outside backup storage."""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import stat
import tempfile

from cryptography.fernet import Fernet, InvalidToken
from cryptography.exceptions import UnsupportedAlgorithm

from runtime.credential_input import read_backup_key_file
from scripts import journal_backup as plain
from scripts.recovery_preflight import require


MAGIC = b'CCS-ENCRYPTED-BACKUP-1\n'
DOMAIN = b'CCS-JOURNAL-FERNET-1\x00'
MAX_BYTES = (plain.MAX_BYTES + plain.MAX_MANIFEST + 256) * 2


def cipher(key_file, storage):
    key = Path(key_file).absolute()
    parent = plain.private_directory(key.parent)
    key = parent / key.name
    require(not key.is_relative_to(storage))
    return Fernet(read_backup_key_file(key))


def create_key(key_file):
    report = plain.verdict('CREATE_BACKUP_KEY')
    try:
        key = Path(key_file).absolute()
        key = plain.private_directory(key.parent) / key.name
        with plain.new_file(key) as stream:
            stream.write(Fernet.generate_key())
            stream.flush()
            os.fsync(stream.fileno())
        report['passed'] = True
    except (OSError, ValueError, TypeError):
        report['failure'] = 'KEY_CREATION_UNVERIFIED'
    return report


def read_ciphertext(bundle):
    require(stat.S_ISREG(os.lstat(bundle).st_mode))
    flags = os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
    with os.fdopen(os.open(bundle, flags), 'rb') as stream:
        require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode))
        require(os.fstat(stream.fileno()).st_size <= MAX_BYTES)
        require(stream.read(len(MAGIC)) == MAGIC)
        token = stream.read(MAX_BYTES + 1)
    require(0 < len(token) <= MAX_BYTES - len(MAGIC))
    # Fernet's base64 decoder accepts some noncanonical inputs. Refuse trailing
    # junk, alternate padding and whitespace at this envelope boundary.
    require(re.fullmatch(rb'[A-Za-z0-9_-]+={0,2}', token) is not None)
    decoded = base64.b64decode(token, altchars=b'-_', validate=True)
    require(base64.urlsafe_b64encode(decoded) == token)
    return token


def capture(journal, bundle, key_file):
    report = plain.verdict('ENCRYPTED_CAPTURE')
    report['encrypted'] = False
    try:
        destination = Path(bundle).absolute()
        parent = plain.private_directory(destination.parent)
        destination = parent / destination.name
        source = Path(journal).resolve(strict=True)
        require(not os.path.lexists(destination))
        require(destination not in {source, *(Path(str(source)+suffix) for suffix in ('-journal','-wal','-shm'))})
        encryption = cipher(key_file, parent)
        with tempfile.TemporaryDirectory(prefix='.ccs-encrypted-', dir=parent) as directory:
            inner = Path(directory) / 'snapshot.ccsb'
            captured = plain.capture(source, inner)
            require(captured['passed'])
            token = encryption.encrypt(DOMAIN + inner.read_bytes())
            require(len(MAGIC) + len(token) <= MAX_BYTES)
            pending = Path(directory) / 'encrypted.partial'
            with plain.new_file(pending) as stream:
                stream.write(MAGIC + token)
                stream.flush()
                os.fsync(stream.fileno())
            plain.publish_no_clobber(pending, destination)
            report.update(passed=True, encrypted=True, counts=captured['counts'],
                          uncertain_actions=captured['uncertain_actions'])
    except (OSError, ValueError, TypeError, KeyError, InvalidToken, UnsupportedAlgorithm):
        report['passed'] = False
        report['failure'] = 'ENCRYPTED_BACKUP_UNVERIFIED'
    return report


def drill(bundle, workspace, key_file):
    report = plain.verdict('ENCRYPTED_RESTORE_DRILL')
    report['ciphertext_authenticated'] = False
    try:
        parent = plain.private_directory(workspace)
        decryption = cipher(key_file, parent)
        raw = decryption.decrypt(read_ciphertext(bundle))
        require(raw.startswith(DOMAIN + plain.MAGIC))
        require(len(raw) <= plain.MAX_BYTES + plain.MAX_MANIFEST + len(plain.MAGIC) + 4 + len(DOMAIN))
        # Authentication and protocol-domain checks precede plaintext writes.
        with tempfile.TemporaryDirectory(prefix='.ccs-decrypted-', dir=parent) as directory:
            inner = Path(directory) / 'snapshot.ccsb'
            with plain.new_file(inner) as stream:
                stream.write(raw[len(DOMAIN):])
            restored = plain.drill(inner, directory)
            require(restored['passed'])
            report.update(passed=True, ciphertext_authenticated=True,
                          counts=restored['counts'], uncertain_actions=restored['uncertain_actions'],
                          restored_to_memory=True)
    except (OSError, ValueError, TypeError, KeyError, InvalidToken, UnsupportedAlgorithm):
        report['passed'] = False
        report['failure'] = 'ENCRYPTED_RESTORE_UNVERIFIED'
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    commands = parser.add_subparsers(dest='command', required=True)
    key = commands.add_parser('create-key', allow_abbrev=False)
    key.add_argument('--key-file', required=True)
    backup = commands.add_parser('capture', allow_abbrev=False)
    backup.add_argument('--journal', required=True)
    backup.add_argument('--bundle', required=True)
    backup.add_argument('--key-file', required=True)
    restore = commands.add_parser('drill', allow_abbrev=False)
    restore.add_argument('--bundle', required=True)
    restore.add_argument('--workspace', required=True)
    restore.add_argument('--key-file', required=True)
    args = parser.parse_args()
    if args.command == 'create-key': result = create_key(args.key_file)
    elif args.command == 'capture': result = capture(args.journal, args.bundle, args.key_file)
    else: result = drill(args.bundle, args.workspace, args.key_file)
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    raise SystemExit(0 if result['passed'] else 2)


if __name__ == '__main__':
    main()
