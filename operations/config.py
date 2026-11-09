"""Bounded configuration for one loopback recovery-only host."""
from dataclasses import dataclass, fields
import json
import os
from pathlib import Path
import stat

from runtime.worker_supervisor import WorkerSettings


class ConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class RecoveryConfig:
    journal: Path
    worker_credential_file: Path
    port: int = 8877
    settings: WorkerSettings = WorkerSettings()

    def __post_init__(self):
        if (type(self.port) is not int or not 1024 <= self.port <= 65535 or
                type(self.settings) is not WorkerSettings):
            raise ConfigurationError('Invalid recovery configuration')
        self.validate_paths()

    def validate_paths(self):
        for path in (self.journal, self.worker_credential_file):
            if (not isinstance(path, Path) or not path.is_absolute() or not path.is_file() or
                    path.is_symlink() or path.anchor.startswith('\\\\')):
                raise ConfigurationError('Existing absolute regular-file paths are required')
        if self.journal.resolve() == self.worker_credential_file.resolve():
            raise ConfigurationError('Journal and worker credential must be separate files')

    @property
    def origin(self):
        return f'http://127.0.0.1:{self.port}'


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ConfigurationError('Duplicate configuration field')
        result[key] = value
    return result


def _constant(_):
    raise ValueError('Nonfinite configuration number')


def load(path):
    """No env interpolation, credential values, provider overrides or fallback.

    Windows requires an administrator-protected parent ACL; native ACL custody
    is not certified by this Python reader. POSIX additionally checks owner/mode.
    """
    descriptor = None
    try:
        if not stat.S_ISREG(os.lstat(path).st_mode):
            raise ValueError()
        descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_BINARY', 0) |
                             getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 16384:
            raise ValueError()
        if os.name != 'nt' and (info.st_uid != os.geteuid() or info.st_mode & 0o077):
            raise ValueError()
        raw = os.read(descriptor, 16385)
        if len(raw) > 16384:
            raise ValueError()
        data = json.loads(raw.decode('utf-8-sig'), object_pairs_hook=_object, parse_constant=_constant)
        required = {'schema', 'mode', 'journal', 'worker_credential_file', 'port'}
        if (type(data) is not dict or not required <= set(data) or
                set(data) - required - {'worker_settings'} or
                type(data['schema']) is not int or data['schema'] != 1 or data['mode'] != 'RECOVERY_ONLY'):
            raise ValueError()
        settings = data.get('worker_settings', {})
        if type(settings) is not dict or set(settings) - {f.name for f in fields(WorkerSettings)}:
            raise ValueError()
        if any(type(data[key]) is not str or not data[key] or '\x00' in data[key]
               for key in ('journal', 'worker_credential_file')):
            raise ValueError()
        return RecoveryConfig(Path(data['journal']), Path(data['worker_credential_file']),
                              data['port'], WorkerSettings(**settings))
    except (OSError, ValueError, TypeError, RecursionError):
        raise ConfigurationError('Recovery configuration is unavailable, malformed or insufficiently protected') from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
