"""Real pinned pytest watchdog: a traceback is observation, not cancellation."""
import os
from pathlib import Path
import queue
import shlex
import subprocess
import sys
import threading
import time

import pytest
import yaml


SOURCE = '''import sys, threading
def test_blocked_owned_work():
    private_value = "PRIVATE_VALUE_MUST_NOT_APPEAR_IN_THREAD_DUMP"
    released = threading.Event()
    def accept_release():
        if sys.stdin.readline() == "release\\n":
            released.set()
    threading.Thread(target=accept_release, daemon=True).start()
    print("CCS_WATCHDOG_READY", flush=True)
    released.wait()
    assert private_value
    print("CCS_WATCHDOG_RELEASED", flush=True)
    {result}
'''


def run_case(tmp_path, *, exit_on_timeout, fail_after_release=False):
    workflow = yaml.safe_load((Path(__file__).resolve().parents[1] / '.github/workflows/tests.yml').read_text(encoding='utf-8'))
    commands = [step['run'] for step in workflow['jobs']['test']['steps']
                if '-m pytest' in step.get('run', '') and '--junitxml=results/runtime-tests.xml' in step['run']]
    assert len(commands) == 1
    arguments = shlex.split(commands[0].replace('${{ matrix.python }}', 'python'))
    overrides = dict(arguments[index + 1].split('=', 1) for index, value in enumerate(arguments) if value == '-o')
    assert float(overrides['faulthandler_timeout']) > 0
    # Exercise the checked-in CI exit policy, rather than a duplicate false
    # constant. Only the synthetic diagnostic interval is shortened to 1s.
    policy = 'true' if exit_on_timeout else overrides['faulthandler_exit_on_timeout']
    case = tmp_path / 'test_watchdog_case.py'
    case.write_text(SOURCE.format(result="raise AssertionError('controlled failure after release')" if fail_after_release else 'pass'), encoding='utf-8')
    # Isolate the synthetic child from unrelated third-party plugin autoload;
    # pytest's built-in faulthandler still runs on the actual installed version.
    environment = {**os.environ, 'PYTEST_DISABLE_PLUGIN_AUTOLOAD': '1'}
    environment.pop('PYTEST_ADDOPTS', None)
    environment.pop('PYTEST_PLUGINS', None)
    child = subprocess.Popen([sys.executable, '-m', 'pytest', '-q', '-s', '--tb=no', '--noconftest', '-p', 'no:cacheprovider',
                              '-o', 'faulthandler_timeout=1',
                              '-o', f'faulthandler_exit_on_timeout={policy}', str(case)],
                             cwd=tmp_path, env=environment, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                             creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    events = queue.Queue()
    output = {'stdout': [], 'stderr': []}
    def read(name, stream):
        for line in stream:
            # The child is our fixed controlled fixture. Bound retained output
            # and never include captured commands/streams in failure messages.
            if sum(map(len, output[name])) + len(line) <= 65536:
                output[name].append(line)
            events.put((name, line[:2048]))
    readers = [threading.Thread(target=read, args=(name, stream), daemon=True)
               for name, stream in (('stdout', child.stdout), ('stderr', child.stderr))]
    for reader in readers:
        reader.start()
    ready = dumped = False
    # One budget for coordination + normal exit, replacing separate 10s/5s
    # allowances. No automatic retry and no increase to their combined bound.
    deadline = time.monotonic() + 15
    def remaining():
        return max(.001, deadline - time.monotonic())
    def finished():
        try:
            return child.wait(timeout=remaining())
        except subprocess.TimeoutExpired:
            raise AssertionError('Watchdog fixture did not exit within its shared budget') from None
    try:
        while not (ready and dumped):
            assert time.monotonic() < deadline, 'Watchdog fixture did not reach its diagnostic barrier'
            try:
                name, line = events.get(timeout=remaining())
            except queue.Empty:
                pytest.fail('Watchdog fixture did not reach its diagnostic barrier')
            ready |= name == 'stdout' and 'CCS_WATCHDOG_READY' in line
            dumped |= name == 'stderr' and 'test_blocked_owned_work' in line
        if exit_on_timeout:
            # Negative control: terminating watchdog policy loses the blocked
            # work. CI must explicitly choose the nonterminating policy.
            code = finished()
            assert code != 0
        else:
            assert child.poll() is None, 'Observation incorrectly terminated blocked work'
            child.stdin.write('release\n')
            child.stdin.flush()
            code = finished()
            assert code == (1 if fail_after_release else 0)
        for reader in readers:
            reader.join(timeout=5)
            assert not reader.is_alive()
        trace = ''.join(output['stderr'])
        assert 'Timeout (' in trace and 'test_blocked_owned_work' in trace
        assert 'PRIVATE_VALUE_MUST_NOT_APPEAR_IN_THREAD_DUMP' not in trace
        released = 'CCS_WATCHDOG_RELEASED' in ''.join(output['stdout'])
        assert released is not exit_on_timeout
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
        child.stdin.close()
        child.stdout.close()
        child.stderr.close()
        for reader in readers:
            reader.join(timeout=5)


@pytest.mark.parametrize('fail_after_release', [False, True])
def test_watchdog_dump_does_not_cancel_work_or_replace_final_test_status(tmp_path, fail_after_release):
    run_case(tmp_path, exit_on_timeout=False, fail_after_release=fail_after_release)


def test_terminating_watchdog_negative_control_exposes_policy_difference(tmp_path):
    run_case(tmp_path, exit_on_timeout=True)
