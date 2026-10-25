"""Authenticated encrypted journal bundles; keys stay outside backup storage."""
import argparse
import base64
import hmac
import json
import os
from pathlib import Path
import re
import stat
import tempfile

from cryptography.fernet import Fernet, InvalidToken, MultiFernet
from cryptography.exceptions import UnsupportedAlgorithm

from runtime.credential_input import read_backup_key_file
from scripts import journal_backup as plain
from scripts.recovery_preflight import require


MAGIC = b'CCS-ENCRYPTED-BACKUP-1\n'
DOMAIN = b'CCS-JOURNAL-FERNET-1\x00'
MAX_BYTES = (plain.MAX_BYTES + plain.MAX_MANIFEST + 256) * 2


def key_material(key_file, *storage):
    key = Path(key_file).absolute()
    parent = plain.private_directory(key.parent)
    key = parent / key.name
    require(all(not key.is_relative_to(directory) for directory in storage))
    return read_backup_key_file(key)


def cipher(key_file, storage):
    return Fernet(key_material(key_file, storage))


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


def authenticated_payload(token, decryption):
    raw = decryption.decrypt(token)
    require(raw.startswith(DOMAIN + plain.MAGIC))
    require(len(raw) <= plain.MAX_BYTES + plain.MAX_MANIFEST + len(plain.MAGIC) + 4 + len(DOMAIN))
    return raw


def drill_payload(raw, workspace):
    # Only called with authenticated, bounded protocol payloads. No live path.
    with tempfile.TemporaryDirectory(prefix='.ccs-decrypted-', dir=workspace) as directory:
        inner = Path(directory) / 'snapshot.ccsb'
        with plain.new_file(inner) as stream:
            stream.write(raw[len(DOMAIN):])
        restored = plain.drill(inner, directory)
        require(restored['passed'])
    return restored


def drill(bundle, workspace, key_file):
    report = plain.verdict('ENCRYPTED_RESTORE_DRILL')
    report['ciphertext_authenticated'] = False
    try:
        parent = plain.private_directory(workspace)
        decryption = cipher(key_file, parent)
        raw = authenticated_payload(read_ciphertext(bundle), decryption)
        # Authentication and protocol-domain checks precede plaintext writes.
        restored = drill_payload(raw, parent)
        report.update(passed=True, ciphertext_authenticated=True,
                      counts=restored['counts'], uncertain_actions=restored['uncertain_actions'],
                      restored_to_memory=True)
    except (OSError, ValueError, TypeError, KeyError, InvalidToken, UnsupportedAlgorithm):
        report['passed'] = False
        report['failure'] = 'ENCRYPTED_RESTORE_UNVERIFIED'
    return report


def rotate(bundle, destination, workspace, old_key_file, new_key_file):
    """Re-encrypt one validated snapshot; retain its exact payload and age."""
    report = plain.verdict('ROTATE_BACKUP_KEY')
    report.update(ciphertext_authenticated=False, encrypted=False)
    try:
        source = Path(bundle).absolute()
        source = plain.private_directory(source.parent) / source.name
        target = Path(destination).absolute()
        parent = plain.private_directory(target.parent)
        target = parent / target.name
        private = plain.private_directory(workspace)
        require(source != target and not os.path.lexists(target))
        old_bytes = key_material(old_key_file, source.parent, parent, private)
        new_bytes = key_material(new_key_file, source.parent, parent, private)
        require(not hmac.compare_digest(base64.urlsafe_b64decode(old_bytes),
                                        base64.urlsafe_b64decode(new_bytes)))
        old, new = Fernet(old_bytes), Fernet(new_bytes)
        token = read_ciphertext(source)
        # The explicitly named OLD key must authenticate; no key discovery or
        # acceptance of a bundle already encrypted under only the new key.
        raw = authenticated_payload(token, old)
        restored = drill_payload(raw, private)
        rotated = MultiFernet([new, old]).rotate(token)
        require(len(MAGIC) + len(rotated) <= MAX_BYTES)
        require(hmac.compare_digest(authenticated_payload(rotated, new), raw))
        require(old.extract_timestamp(token) == new.extract_timestamp(rotated))
        with tempfile.TemporaryDirectory(prefix='.ccs-rotation-', dir=parent) as directory:
            pending = Path(directory) / 'encrypted.partial'
            with plain.new_file(pending) as stream:
                stream.write(MAGIC + rotated)
                stream.flush()
                os.fsync(stream.fileno())
            require(hmac.compare_digest(read_ciphertext(pending), rotated))
            plain.publish_no_clobber(pending, target)
        report.update(passed=True, ciphertext_authenticated=True, encrypted=True,
                      payload_preserved=True, token_timestamp_preserved=True,
                      counts=restored['counts'], uncertain_actions=restored['uncertain_actions'])
    except (OSError, ValueError, TypeError, KeyError, InvalidToken, UnsupportedAlgorithm):
        report['passed'] = False
        report['failure'] = 'KEY_ROTATION_UNVERIFIED'
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
    rotation = commands.add_parser('rotate', allow_abbrev=False)
    rotation.add_argument('--bundle', required=True)
    rotation.add_argument('--destination', required=True)
    rotation.add_argument('--workspace', required=True)
    rotation.add_argument('--old-key-file', required=True)
    rotation.add_argument('--new-key-file', required=True)
    args = parser.parse_args()
    if args.command == 'create-key': result = create_key(args.key_file)
    elif args.command == 'capture': result = capture(args.journal, args.bundle, args.key_file)
    elif args.command == 'drill': result = drill(args.bundle, args.workspace, args.key_file)
    else: result = rotate(args.bundle, args.destination, args.workspace, args.old_key_file, args.new_key_file)
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    raise SystemExit(0 if result['passed'] else 2)


if __name__ == '__main__':
    main()
