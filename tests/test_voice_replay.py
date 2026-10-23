import copy
import json
import subprocess
import sys

import pytest

from eval.voice_replay import DEFAULT_FIXTURE, load_fixture, score


def test_bundled_oracle_is_deterministic_and_labeled_simulated():
    fixture = load_fixture(DEFAULT_FIXTURE)
    report = score(fixture)
    assert report == score(copy.deepcopy(fixture))
    assert report["checks_passed"] == report["case_count"] == 10 and report["checks_failed"] == 0
    assert report["label"] == "SIMULATED" and report["real_audio_or_provider"] == "NOT_RUN"
    assert "Chennai" not in json.dumps(report) and "Bengaluru" not in json.dumps(report)


def test_incorrect_independent_expectation_produces_failed_check():
    fixture = load_fixture(DEFAULT_FIXTURE)
    fixture["cases"][1]["expected"]["grounded"] = True
    report = score(fixture)
    assert report["checks_failed"] == 1 and report["checks_passed"] == 9


@pytest.mark.parametrize("change", ["label", "schema", "duplicate-id", "unknown-event", "unknown-key", "unbounded"])
def test_replay_cannot_silently_relabel_or_skip_invalid_scenarios(change):
    fixture = load_fixture(DEFAULT_FIXTURE)
    if change == "label":
        fixture["label"] = "REAL_PROVIDER"
    elif change == "schema":
        fixture["schema"] = True
    elif change == "duplicate-id":
        fixture["cases"][1]["id"] = fixture["cases"][0]["id"]
    elif change == "unknown-event":
        fixture["cases"][0]["events"][0]["type"] = "silently-pass"
    elif change == "unknown-key":
        fixture["cases"][0]["events"][0]["extra"] = "ignored"
    else:
        fixture["cases"][0]["events"] *= 100
    with pytest.raises(ValueError):
        score(fixture)


def test_cli_reports_and_failure_exit_are_reproducible(tmp_path):
    output = tmp_path / "scorecard.json"
    command = [sys.executable, "-m", "eval.voice_replay", "--output", str(output)]
    result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0 and output.is_file()
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["checks_passed"] == 10 and json.loads(result.stdout)["label"] == "SIMULATED"
    fixture = load_fixture(DEFAULT_FIXTURE)
    fixture["cases"][0]["expected"]["text"] = "wrong oracle"
    path = tmp_path / "fixture.json"
    path.write_text(json.dumps(fixture), encoding="utf-8")
    result = subprocess.run(command + ["--fixture", str(path)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 1 and json.loads(result.stdout)["checks_failed"] == 1


@pytest.mark.parametrize("raw", ['{"schema":1,"schema":1}', '{"secret":"PRIVATE-DO-NOT-LOG",', '{"value":NaN}'])
def test_fixture_parse_failure_does_not_echo_payload(tmp_path, raw):
    fixture, output = tmp_path / "bad.json", tmp_path / "no-scorecard.json"
    fixture.write_text(raw, encoding="utf-8")
    result = subprocess.run([sys.executable, "-m", "eval.voice_replay", "--fixture", str(fixture), "--output", str(output)],
                             capture_output=True, text=True, timeout=30)
    assert result.returncode == 2 and "PRIVATE-DO-NOT-LOG" not in result.stdout + result.stderr
    assert not output.exists()
