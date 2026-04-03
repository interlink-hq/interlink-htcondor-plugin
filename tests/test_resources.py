"""
Unit tests for the cluster-resource parsing functions in handles.py.

These tests exercise :func:`parse_cluster_resources_from_json` and
:func:`parse_cluster_resources_from_text` without running a live HTCondor
binary, and verify the :func:`get_cluster_resources` ping-path integration
via the Flask test client.  The expected response format is aligned with
interlink-hq/interLink#516 (PingResponse).
"""

import json as _json
import os
import sys
import unittest.mock as mock


def _make_handles_module():
    """Import handles with mocked globals so top-level parse code doesn't fail."""
    with mock.patch("sys.argv", ["handles.py"]):
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if repo_root not in sys.path:
            sys.path.insert(0, repo_root)

        orig_dir = os.getcwd()
        os.chdir(repo_root)
        try:
            import handles as h
        finally:
            os.chdir(orig_dir)
    return h


handles = _make_handles_module()


def _flask_test_client():
    handles.app.config["TESTING"] = True
    return handles.app.test_client()


# ---------------------------------------------------------------------------
# parse_cluster_resources_from_json
# ---------------------------------------------------------------------------


class TestParseClusterResourcesFromJSON:
    """Tests for parse_cluster_resources_from_json (condor_status --json path)."""

    def _make_slot(self, cpus, memory_mb, state="Unclaimed", dynamic=False):
        slot = {"Cpus": cpus, "Memory": memory_mb, "State": state}
        if dynamic:
            slot["DynamicSlot"] = True
        return slot

    def test_happy_path_two_unclaimed_slots(self):
        # node01: 16 CPUs, 128 000 MB  (Unclaimed)
        # node02: 32 CPUs,  64 000 MB  (Unclaimed)
        slots = [
            self._make_slot(16, 128000, "Unclaimed"),
            self._make_slot(32, 64000, "Unclaimed"),
        ]
        resp = handles.parse_cluster_resources_from_json(_json.dumps(slots))
        assert resp["status"] == "ok"
        assert resp["resources"]["cpu"] == "48"
        assert resp["resources"]["memory"] == "192000Mi"

    def test_claimed_slots_are_excluded(self):
        slots = [
            self._make_slot(16, 128000, "Unclaimed"),
            self._make_slot(8, 32000, "Claimed"),
        ]
        resp = handles.parse_cluster_resources_from_json(_json.dumps(slots))
        assert resp["resources"]["cpu"] == "16"
        assert resp["resources"]["memory"] == "128000Mi"

    def test_dynamic_child_slots_are_excluded(self):
        # Partitionable parent (Unclaimed, 8 CPUs remaining) + dynamic child
        # (Claimed, 8 CPUs allocated).  Only the parent should be counted.
        slots = [
            self._make_slot(8, 64000, "Unclaimed"),   # p-slot
            self._make_slot(8, 64000, "Claimed", dynamic=True),  # d-slot
        ]
        resp = handles.parse_cluster_resources_from_json(_json.dumps(slots))
        assert resp["resources"]["cpu"] == "8"
        assert resp["resources"]["memory"] == "64000Mi"

    def test_all_slots_claimed_returns_zero(self):
        slots = [self._make_slot(16, 128000, "Claimed")]
        resp = handles.parse_cluster_resources_from_json(_json.dumps(slots))
        assert resp["resources"]["cpu"] == "0"
        assert resp["resources"]["memory"] == "0Mi"

    def test_invalid_json_raises_value_error(self):
        import pytest
        with pytest.raises(ValueError, match="parse error"):
            handles.parse_cluster_resources_from_json("not valid json")

    def test_empty_list_raises_value_error(self):
        import pytest
        with pytest.raises(ValueError, match="no slots"):
            handles.parse_cluster_resources_from_json("[]")

    def test_response_has_status_ok(self):
        slots = [self._make_slot(4, 8000, "Unclaimed")]
        resp = handles.parse_cluster_resources_from_json(_json.dumps(slots))
        assert resp["status"] == "ok"

    def test_response_has_resources_key(self):
        slots = [self._make_slot(4, 8000, "Unclaimed")]
        resp = handles.parse_cluster_resources_from_json(_json.dumps(slots))
        assert "resources" in resp
        assert "cpu" in resp["resources"]
        assert "memory" in resp["resources"]


# ---------------------------------------------------------------------------
# parse_cluster_resources_from_text
# ---------------------------------------------------------------------------


class TestParseClusterResourcesFromText:
    """Tests for parse_cluster_resources_from_text (condor_status -autoformat path)."""

    def test_happy_path_two_lines(self):
        # node01: 16 CPUs, 128 000 MB
        # node02: 32 CPUs,  64 000 MB
        stdout = "16 128000\n32 64000\n"
        resp = handles.parse_cluster_resources_from_text(stdout)
        assert resp["status"] == "ok"
        assert resp["resources"]["cpu"] == "48"
        assert resp["resources"]["memory"] == "192000Mi"

    def test_skips_invalid_lines(self):
        # Mix valid and invalid lines.
        stdout = "8 32000\nbadline\n4 N/A\n4 16000\n"
        resp = handles.parse_cluster_resources_from_text(stdout)
        # Only first (8) and last (4) lines contribute.
        assert resp["resources"]["cpu"] == "12"
        assert resp["resources"]["memory"] == "48000Mi"

    def test_empty_output_returns_zeros(self):
        resp = handles.parse_cluster_resources_from_text("")
        assert resp["status"] == "ok"
        assert resp["resources"]["cpu"] == "0"
        assert resp["resources"]["memory"] == "0Mi"

    def test_whitespace_only_output_returns_zeros(self):
        resp = handles.parse_cluster_resources_from_text("   \n\n  \n")
        assert resp["resources"]["cpu"] == "0"
        assert resp["resources"]["memory"] == "0Mi"

    def test_single_slot(self):
        resp = handles.parse_cluster_resources_from_text("4 8192\n")
        assert resp["resources"]["cpu"] == "4"
        assert resp["resources"]["memory"] == "8192Mi"

    def test_response_has_status_ok(self):
        resp = handles.parse_cluster_resources_from_text("2 1024\n")
        assert resp["status"] == "ok"

    def test_response_has_resources_key(self):
        resp = handles.parse_cluster_resources_from_text("2 1024\n")
        assert "resources" in resp
        assert "cpu" in resp["resources"]
        assert "memory" in resp["resources"]


# ---------------------------------------------------------------------------
# PingResponse serialisation
# ---------------------------------------------------------------------------


class TestPingResponseSerialization:
    """Verify the PingResponse dict serialises to the interLink#516 JSON schema."""

    def test_full_response_has_expected_keys(self):
        resp = {
            "status": "ok",
            "resources": {
                "cpu": "128",
                "memory": "512000Mi",
            },
        }
        serialised = _json.dumps(resp)
        decoded = _json.loads(serialised)
        assert decoded["status"] == "ok"
        assert decoded["resources"]["cpu"] == "128"
        assert decoded["resources"]["memory"] == "512000Mi"

    def test_round_trip_preserves_values(self):
        original = {
            "status": "ok",
            "resources": {"cpu": "64", "memory": "256000Mi"},
        }
        assert _json.loads(_json.dumps(original)) == original


# ---------------------------------------------------------------------------
# Ping path integration (/status with empty pod list)
# ---------------------------------------------------------------------------


class TestPingPathIntegration:
    """/status with empty array should return PingResponse JSON (interLink#516)."""

    @staticmethod
    def _make_fake_popen(json_output):
        """Return a fake os.popen that yields *json_output* for condor_status."""

        def fake_popen(cmd):
            class FakeProc:
                def read(self):
                    return json_output

                def close(self):
                    pass

            return FakeProc()

        return fake_popen

    def test_ping_returns_200(self, monkeypatch):
        slots = [{"Cpus": 4, "Memory": 8192, "State": "Unclaimed"}]
        monkeypatch.setattr(os, "popen", self._make_fake_popen(_json.dumps(slots)))
        monkeypatch.setattr(handles, "args", mock.MagicMock(proxy=""))
        resp = _flask_test_client().get(
            "/status", data=_json.dumps([]), content_type="application/json"
        )
        assert resp.status_code == 200

    def test_ping_returns_json_content_type(self, monkeypatch):
        slots = [{"Cpus": 4, "Memory": 8192, "State": "Unclaimed"}]
        monkeypatch.setattr(os, "popen", self._make_fake_popen(_json.dumps(slots)))
        monkeypatch.setattr(handles, "args", mock.MagicMock(proxy=""))
        resp = _flask_test_client().get(
            "/status", data=_json.dumps([]), content_type="application/json"
        )
        assert "application/json" in resp.content_type

    def test_ping_response_has_status_and_resources(self, monkeypatch):
        slots = [{"Cpus": 4, "Memory": 8192, "State": "Unclaimed"}]
        monkeypatch.setattr(os, "popen", self._make_fake_popen(_json.dumps(slots)))
        monkeypatch.setattr(handles, "args", mock.MagicMock(proxy=""))
        resp = _flask_test_client().get(
            "/status", data=_json.dumps([]), content_type="application/json"
        )
        data = _json.loads(resp.data)
        assert data["status"] == "ok"
        assert "resources" in data
        assert data["resources"]["cpu"] == "4"
        assert data["resources"]["memory"] == "8192Mi"

    def test_ping_returns_ok_when_condor_status_fails(self, monkeypatch):
        """When condor_status is unavailable the handler returns status:ok with no resources."""

        def raise_oserror(cmd):
            raise OSError("condor_status not found")

        monkeypatch.setattr(os, "popen", raise_oserror)
        monkeypatch.setattr(handles, "args", mock.MagicMock(proxy=""))
        resp = _flask_test_client().get(
            "/status", data=_json.dumps([]), content_type="application/json"
        )
        assert resp.status_code == 200
        data = _json.loads(resp.data)
        assert data["status"] == "ok"

    def test_ping_returns_503_when_proxy_missing(self, monkeypatch, tmp_path):
        """When a proxy path is configured but the file does not exist, return 503."""
        missing_proxy = str(tmp_path / "nonexistent.proxy")
        monkeypatch.setattr(handles, "args", mock.MagicMock(proxy=missing_proxy))
        resp = _flask_test_client().get(
            "/status", data=_json.dumps([]), content_type="application/json"
        )
        assert resp.status_code == 503

    def test_ping_succeeds_when_proxy_file_exists(self, monkeypatch, tmp_path):
        proxy_file = tmp_path / "valid.proxy"
        proxy_file.write_text("proxy-data")
        slots = [{"Cpus": 8, "Memory": 16384, "State": "Unclaimed"}]
        monkeypatch.setattr(os, "popen", self._make_fake_popen(_json.dumps(slots)))
        monkeypatch.setattr(handles, "args", mock.MagicMock(proxy=str(proxy_file)))
        resp = _flask_test_client().get(
            "/status", data=_json.dumps([]), content_type="application/json"
        )
        assert resp.status_code == 200
        data = _json.loads(resp.data)
        assert data["resources"]["cpu"] == "8"
