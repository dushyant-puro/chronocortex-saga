"""Deterministic SIMULATED SDK protocol replay, never an ASR quality benchmark."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile

from livekit.agents import stt
from livekit.agents.types import TimedString

from voice.livekit_observer import LiveKitEvidenceStream
from runtime.voice_ingress import VoiceIngress, bounded_text


DEFAULT_FIXTURE = Path(__file__).parent / "fixtures" / "voice_evidence_cases.json"


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("Duplicate fixture key")
        result[key] = value
    return result


def _constant(_):
    raise ValueError("Nonfinite fixture value")


def load_fixture(path):
    with Path(path).open("rb") as source:
        raw = source.read(1_048_577)
    if len(raw) > 1_048_576:
        raise ValueError("Fixture exceeds capacity")
    data = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant)
    validate_fixture(data)
    return data


def validate_fixture(data):
    if (type(data) is not dict or set(data) != {"schema", "label", "cases"} or
            type(data["schema"]) is not int or data["schema"] != 1 or data["label"] != "SIMULATED" or
            type(data["cases"]) is not list or not 1 <= len(data["cases"]) <= 128):
        raise ValueError("Unsupported simulated fixture")
    seen = set()
    for case in data["cases"]:
        if (type(case) is not dict or set(case) != {"id", "events", "expected"} or
                type(case["id"]) is not str or re.fullmatch(r"[a-z0-9_]{1,80}", case["id"]) is None or case["id"] in seen):
            raise ValueError("Invalid case identity")
        seen.add(case["id"])
        expected = case["expected"]
        if (type(expected) is not dict or set(expected) != {"grounded", "text"} or type(expected["grounded"]) is not bool):
            raise ValueError("Explicit independent case expectations required")
        if expected["text"] is not None:
            bounded_text(expected["text"])
        if type(case["events"]) is not list or not 1 <= len(case["events"]) <= 128:
            raise ValueError("Event capacity exceeded")
        for event in case["events"]:
            if type(event) is not dict or type(event.get("type")) is not str:
                raise ValueError("Invalid event")
            kind = event["type"]
            if kind in {"start", "end", "restart", "old_close"}:
                if set(event) != {"type"}:
                    raise ValueError("Invalid control event")
            elif kind == "complete":
                if set(event) != {"type", "text"}:
                    raise ValueError("Invalid completion event")
                bounded_text(event["text"])
            elif kind in {"interim", "final", "old_final"}:
                if not {"type", "text", "scored"} <= set(event) <= {"type", "text", "scored", "timed", "confidence", "offset"}:
                    raise ValueError("Invalid transcript event")
                bounded_text(event["text"])
                if type(event["scored"]) is not bool or type(event.get("timed", True)) is not bool:
                    raise ValueError("Explicit metadata availability required")
                for key in ("confidence", "offset"):
                    if key in event and (type(event[key]) not in (int, float) or not math.isfinite(event[key])):
                        raise ValueError("Finite synthetic numeric data required")
            else:
                raise ValueError("Unknown event type")


def _speech(event):
    if event["type"] in {"start", "end"}:
        kind = stt.SpeechEventType.START_OF_SPEECH if event["type"] == "start" else stt.SpeechEventType.END_OF_SPEECH
        return stt.SpeechEvent(type=kind)
    words = []
    for index, text in enumerate(event["text"].split()):
        options = {}
        if event["scored"]:
            options["confidence"] = event.get("confidence", .95)
        if event.get("timed", True):
            options.update(start_time=event.get("offset", 0.) + index * .5,
                           end_time=event.get("offset", 0.) + index * .5 + .4)
        words.append(TimedString(text, **options))
    kind = stt.SpeechEventType.INTERIM_TRANSCRIPT if event["type"] == "interim" else stt.SpeechEventType.FINAL_TRANSCRIPT
    return stt.SpeechEvent(type=kind, alternatives=[stt.SpeechData(language="en", text=event["text"], words=words)])


def score(data):
    validate_fixture(data)
    digest = hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    checks = []
    for case in data["cases"]:
        ingress = VoiceIngress()
        current, old = LiveKitEvidenceStream(ingress), None
        for index, event in enumerate(case["events"]):
            kind = event["type"]
            if kind == "restart":
                old, current = current, LiveKitEvidenceStream(ingress)
            elif kind == "old_close":
                if old is None:
                    raise ValueError("Old stream required")
                old.close()
            elif kind == "old_final":
                if old is None:
                    raise ValueError("Old stream required")
                old.feed(_speech(event))
            elif kind == "complete":
                current.complete(event["text"], f"{case['id']}:{index}")
            else:
                current.feed(_speech(event))
        proposal = ingress.latest
        grounded, text = (proposal.grounded, proposal.text) if proposal is not None else (False, None)
        matches = text == case["expected"]["text"]
        passed = matches and grounded == case["expected"]["grounded"]
        checks.append({"id": case["id"], "passed": passed, "grounded": grounded,
                       "text_matches_expected": matches,
                       "reason": proposal.reason if proposal is not None else "NO_PROPOSAL",
                       "token_count": len(proposal.tokens) if proposal is not None else 0})
    return {"schema": 1, "label": "SIMULATED", "fixture_sha256": digest,
            "case_count": len(checks), "checks_passed": sum(c["passed"] for c in checks),
            "checks_failed": sum(not c["passed"] for c in checks), "checks": checks,
            "real_audio_or_provider": "NOT_RUN", "performance_claim": "NONE"}


def write_report(path, report):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as stream:
            temporary = stream.name
            json.dump(report, stream, sort_keys=True, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None and os.path.exists(temporary):
            os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", default=str(DEFAULT_FIXTURE))
    parser.add_argument("--output", default="results/voice-evidence-scorecard.json")
    args = parser.parse_args()
    try:
        report = score(load_fixture(args.fixture))
        write_report(args.output, report)
    except (OSError, ValueError, TypeError, RecursionError):
        parser.exit(2, "Offline voice replay failed: invalid fixture or unavailable output.\n")
    print(json.dumps({key: report[key] for key in ("label", "fixture_sha256", "case_count", "checks_passed", "checks_failed")}))
    raise SystemExit(0 if report["checks_failed"] == 0 else 1)


if __name__ == "__main__":
    main()
