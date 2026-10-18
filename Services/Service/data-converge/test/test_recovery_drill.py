"""Run the offline crash drill and verify durable effects, not script text."""

import json
import os
import subprocess
import sys

import pytest

from .paths import SOURCE_ROOT, TEST_ROOT


def run_drill(tmp_path):
    env = dict(os.environ, PYTHONPATH=str(SOURCE_ROOT), TMPDIR=str(tmp_path))
    result = subprocess.run(
        [sys.executable, str(TEST_ROOT / "recovery_drill.py")],
        capture_output=True, text=True, timeout=45, env=env)
    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stderr == ""
    assert list(tmp_path.iterdir()) == []
    return json.loads(result.stdout)


@pytest.fixture
def drill_report(tmp_path):
    return run_drill(tmp_path)


@pytest.mark.parametrize("name, recovered, status, attempts, received", [
    ("prepared_before_dispatch", 1, "PENDING", 0, 0),
    ("dispatch_before_receiver", 1, "UNKNOWN", 1, 0),
    ("accepted_before_finish", 1, "UNKNOWN", 1, 1),
    ("success_before_replay", 0, "SUCCESS", 1, 1),
])
def test_crash_classification_matches_persisted_boundaries(
        drill_report, name, recovered, status, attempts, received):
    case = next(row for row in drill_report["cases"] if row["name"] == name)
    observed = case["observed"]
    assert case["passed"] is True
    assert observed == case["expected"]
    assert observed["exit_code"] == 91
    assert observed["recovered"] == recovered
    assert observed["after_recovery"] == {
        "status": status, "attempts": attempts, "receiver_count": received,
        "history_statuses": [] if attempts == 0 else [status]}
    assert observed["second_recovered"] == 0


def test_safe_replay_uses_new_query_and_dispatches_only_once(drill_report):
    case = drill_report["cases"][0]
    assert case["name"] == "prepared_before_dispatch"
    evidence, observed = case["evidence"], case["observed"]
    assert observed["after_replay"] == {
        "status": "SUCCESS", "attempts": 1, "receiver_count": 1,
        "history_statuses": ["SUCCESS"]}
    assert observed["after_second_replay"] == observed["after_replay"]
    assert evidence["crash_query_id"] != evidence["current_query_id"]
    assert [row["query_id"] for row in evidence["receiver"]] == [
        evidence["current_query_id"]]
    assert [row["query_id"] for row in evidence["attempts"]] == [
        evidence["current_query_id"]]
    assert evidence["attempts"][0]["attempt_number"] == 1


def test_unknown_and_success_replay_preserve_external_acceptance(drill_report):
    for case in drill_report["cases"][1:]:
        observed, evidence = case["observed"], case["evidence"]
        assert observed["after_second_replay"] == observed["after_recovery"]
        assert observed["after_replay"] == observed["after_recovery"]
        assert evidence["crash_query_id"] == evidence["current_query_id"]
        assert [row["query_id"] for row in evidence["attempts"]] == [
            evidence["crash_query_id"]]
        assert all(row["query_id"] == evidence["crash_query_id"]
                   for row in evidence["receiver"])


def test_drill_evidence_is_ordered_and_excludes_production_claims(drill_report):
    assert drill_report["scope"] == "offline_protocol_demonstration"
    assert drill_report["clickhouse_cluster_validated"] is False
    assert drill_report["temporary_files_removed"] is True
    assert drill_report["passed"] is True
    for case in drill_report["cases"]:
        events = case["evidence"]["events"]
        assert events
        ids = [row["event_id"] for row in events]
        assert ids == sorted(set(ids))
        assert all(row["query_id"] in {case["evidence"]["crash_query_id"],
                                       case["evidence"]["current_query_id"]}
                   for row in events)


@pytest.mark.parametrize("name, event_types", [
    ("prepared_before_dispatch", ["PREPARED", "RECOVERED_UNSUBMITTED", "PREPARED",
                                  "DISPATCHED", "SUCCEEDED"]),
    ("dispatch_before_receiver", ["PREPARED", "DISPATCHED", "RECOVERY_REQUIRED"]),
    ("accepted_before_finish", ["PREPARED", "DISPATCHED", "RECOVERY_REQUIRED"]),
    ("success_before_replay", ["PREPARED", "DISPATCHED", "SUCCEEDED"]),
])
def test_drill_compares_audited_events_with_fixed_expectations(drill_report, name, event_types):
    case = next(row for row in drill_report["cases"] if row["name"] == name)
    assert case["observed"]["event_types"] == event_types
    assert case["expected"]["event_types"] == event_types
    assert [row["event_type"] for row in case["evidence"]["events"]] == event_types


def test_drill_can_run_again_without_reusing_prior_state(tmp_path):
    first, second = run_drill(tmp_path), run_drill(tmp_path)
    assert [row["observed"] for row in first["cases"]] == [
        row["observed"] for row in second["cases"]]
    first_ids = {row["evidence"]["crash_query_id"] for row in first["cases"]}
    second_ids = {row["evidence"]["crash_query_id"] for row in second["cases"]}
    assert first_ids.isdisjoint(second_ids)
