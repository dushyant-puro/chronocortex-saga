"""Test-only Windows PowerShell runner; never use real credentials in source."""
import os
import shutil
import subprocess


TIMEOUT_SECONDS = 20
PREFIX = "CCS_KEY_SETUP_STAGE:"
STAGES = frozenset({"PROCESS_STARTED", "VALIDATE_PATH", "CREATE_DIRECTORY", "APPLY_ACL", "VERIFY_ACL",
                    "READ_SECRET", "VALIDATE_SECRET", "CREATE_FILE", "WRITE_FILE", "FLUSH_FILE", "CLEANUP", "COMPLETE"})


def setup_stages(stderr):
    if type(stderr) is bytes:
        stderr = stderr[-8192:].decode("utf-8", errors="replace")
    elif type(stderr) is str:
        stderr = stderr[-8192:]
    else:
        return []
    return [line[len(PREFIX):] for line in stderr.splitlines()
            if line.startswith(PREFIX) and line[len(PREFIX):] in STAGES][-64:]


class WindowsShellFailure(RuntimeError):
    def __init__(self, reason, stage="UNOBSERVED"):
        self.reason = reason if type(reason) is str and reason in {"TIMEOUT", "UNAVAILABLE", "LAUNCH_FAILED"} else "UNCLASSIFIED"
        self.stage = stage if type(stage) is str and stage in STAGES else "UNOBSERVED"
        super().__init__(f"Windows PowerShell test failure: reason={self.reason} last_reported_stage={self.stage} budget_seconds={TIMEOUT_SECONDS}")


def run_shell(source):
    executable = shutil.which("powershell.exe")
    if executable is None:
        raise WindowsShellFailure("UNAVAILABLE")
    # Preserve the existing standard-module isolation and total test budget.
    environment = {k: v for k, v in os.environ.items() if k.upper() != "PSMODULEPATH"}
    wrapped = f"[Console]::Error.WriteLine('{PREFIX}PROCESS_STARTED'); " + source
    try:
        result = subprocess.run([executable, "-NoProfile", "-NonInteractive", "-Command", wrapped],
                                capture_output=True, text=True, timeout=TIMEOUT_SECONDS, env=environment,
                                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    except subprocess.TimeoutExpired as error:
        stages = setup_stages(error.stderr)
        raise WindowsShellFailure("TIMEOUT", stages[-1] if stages else "UNOBSERVED") from None
    except OSError:
        raise WindowsShellFailure("LAUNCH_FAILED") from None
    return subprocess.CompletedProcess(["powershell.exe", "<test source omitted>"], result.returncode, result.stdout, result.stderr)
