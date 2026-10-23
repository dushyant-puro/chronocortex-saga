"""Offline provisioning only. Protect the journal and secret directory first.

No secret is printed. On Windows the new file inherits its directory's ACL;
this utility does not certify an ACL, encryption, or a human identity.
"""
import argparse
import os
from pathlib import Path

from runtime.action_journal import SQLiteActionJournal


def provision(journal, principal, permissions, credential_file, *, console_access=True):
    """Create a NEW principal and exclusively deliver its credential to disk.

    The administrator owns directory/ACL setup. POSIX creation mode is 0600.
    Existing paths (including symlinks) are refused; nothing is overwritten.
    A known delivery failure revokes the new credential. A process crash before
    delivery may leave an unusable credential; revoke it offline before reuse.
    """
    path = Path(credential_file)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    credential = None
    try:
        with os.fdopen(descriptor, "w", encoding="ascii", newline="") as stream:
            journal.security.provision_principal(principal, set(permissions) | ({"ccs:observe"} if console_access else set()))
            credential = journal.security.issue_credential(principal)
            stream.write(credential.token)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        try:
            if credential is not None:
                journal.security.revoke_credential(credential.id)
        finally:
            path.unlink(missing_ok=True)
        raise
    return credential.id


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--principal", required=True)
    parser.add_argument("--permission", action="append", default=[])
    parser.add_argument("--credential-file", required=True)
    parser.add_argument("--no-console-access", action="store_true", help="Grant only explicit permissions, for a separate worker principal")
    args = parser.parse_args()
    if not Path(args.journal).is_file():
        parser.error("An existing journal is required; initialize/migrate offline first")
    try:
        credential_id = provision(SQLiteActionJournal(args.journal), args.principal,
                                  args.permission, args.credential_file, console_access=not args.no_console_access)
    except Exception:
        # Avoid paths, exception payloads and bearer secrets in normal output.
        parser.exit(1, "Provisioning failed. Check offline authority, protected paths and journal state; existing files are never overwritten.\n")
    print(f"Principal provisioned; credential ID {credential_id}. Secret delivered to the specified file. Expires within 24 hours.")


if __name__ == "__main__":
    main()
