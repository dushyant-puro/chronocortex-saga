"""Bounded, exclusive regular-file credential injection for trusted hosts."""
import os
import re
import stat


class CredentialInputError(ValueError):
    pass


def read_credential_file(path):
    return _read_private_secret(path, rb"[0-9a-f]{32}\.[A-Za-z0-9_-]{43}", 128)


def read_stripe_key_file(path):
    """Accept restricted test keys only, without whitespace or live-key fallback."""
    return _read_private_secret(path, rb"rk_test_[A-Za-z0-9]{16,256}", 264)


def _read_private_secret(path, pattern, max_bytes):
    """Windows inherits the protected directory ACL; it is not certified here.

    The administrator must protect the parent against path replacement. POSIX
    additionally requires a single-link file owned by this user with owner-only permissions.
    No environment, transcript, provider payload or fallback credential is used.
    """
    descriptor = None
    try:
        if not stat.S_ISREG(os.lstat(path).st_mode):
            raise ValueError()
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        descriptor = os.open(path, flags)
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > max_bytes:
            raise ValueError()
        if os.name != "nt" and (info.st_uid != os.geteuid() or info.st_mode & 0o077):
            raise ValueError()
        raw = os.read(descriptor, max_bytes + 1)
        if re.fullmatch(pattern, raw) is None:
            raise ValueError()
        return raw.decode("ascii")
    except (OSError, ValueError):
        raise CredentialInputError("Credential input is unavailable, malformed or insufficiently protected") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
