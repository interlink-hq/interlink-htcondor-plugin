"""
Unit tests for the prepare_probes function and related prepare-* logic
in handles.py.

prepare_probes follows the same pattern as prepare_env_file and
prepare_mounts: it is called once per container inside SubmitHandler and
returns (probe_script, cleanup_script) strings ready to be injected into
the job executable by produce_htcondor_singularity_script.
"""

import json as _json
import os
import sys
import tempfile
import unittest.mock as mock


def _make_handles_module():
    """Import handles with mocked globals so the top-level code doesn't fail."""

    # Patch sys.argv so argparse doesn't consume pytest arguments.
    with mock.patch("sys.argv", ["handles.py"]):
        # Ensure the repo root is on sys.path for the relative import of probes.
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if repo_root not in sys.path:
            sys.path.insert(0, repo_root)

        # We need a real SidecarConfig.yaml – use the one in the repo root.
        orig_dir = os.getcwd()
        os.chdir(repo_root)
        try:
            import handles as h
        finally:
            os.chdir(orig_dir)
    return h


# Import once for the whole module.
handles = _make_handles_module()
prepare_probes = handles.prepare_probes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_METADATA = {
    "name": "test-pod",
    "uid": "abc-123",
    "namespace": "default",
    "annotations": {},
}


def _container(name="c1", image="busybox:latest", **probes):
    c = {"name": name, "image": image}
    c.update(probes)
    return c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPrepareProbesNoProbes:
    def test_returns_empty_strings_when_no_probes(self):
        container = _container()
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert probe_script == ""
        assert cleanup_script == ""

    def test_returns_empty_strings_when_probes_null(self):
        container = _container(
            livenessProbe=None, readinessProbe=None, startupProbe=None
        )
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert probe_script == ""
        assert cleanup_script == ""


class TestPrepareProbesHTTP:
    def test_http_liveness_probe(self):
        container = _container(
            livenessProbe={"httpGet": {"path": "/live", "port": 8080}}
        )
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert probe_script != ""
        assert '"liveness"' in probe_script
        assert "executeHTTPProbe()" in probe_script

    def test_http_readiness_probe(self):
        container = _container(
            readinessProbe={"httpGet": {"path": "/ready", "port": 9090}}
        )
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert probe_script != ""
        assert '"readiness"' in probe_script
        assert "9090" in probe_script

    def test_http_startup_probe(self):
        container = _container(
            startupProbe={"httpGet": {"path": "/start", "port": 3000}}
        )
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert probe_script != ""
        assert '"startup"' in probe_script

    def test_all_three_http_probes(self):
        container = _container(
            livenessProbe={"httpGet": {"path": "/live", "port": 8080}},
            readinessProbe={"httpGet": {"path": "/ready", "port": 8080}},
            startupProbe={"httpGet": {"path": "/start", "port": 8080}},
        )
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert '"liveness"' in probe_script
        assert '"readiness"' in probe_script
        assert '"startup"' in probe_script

    def test_cleanup_script_contains_trap(self):
        container = _container(
            livenessProbe={"httpGet": {"path": "/live", "port": 8080}}
        )
        _, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert "trap cleanup_probes EXIT" in cleanup_script

    def test_cleanup_script_contains_pid_var(self):
        container = _container(
            readinessProbe={"httpGet": {"path": "/ready", "port": 8080}}
        )
        _, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert "READINESS_PROBE_c1_0_PID" in cleanup_script


class TestPrepareProbesExec:
    def test_exec_liveness_probe(self):
        container = _container(
            livenessProbe={"exec": {"command": ["/bin/sh", "-c", "echo ok"]}}
        )
        probe_script, _ = prepare_probes(container, _BASE_METADATA)
        assert "executeExecProbe()" in probe_script
        assert '"liveness"' in probe_script

    def test_exec_readiness_probe(self):
        container = _container(
            readinessProbe={"exec": {"command": ["cat", "/tmp/ready"]}}
        )
        probe_script, _ = prepare_probes(container, _BASE_METADATA)
        assert '"readiness"' in probe_script
        assert '"cat"' in probe_script


class TestPrepareProbesImageHandling:
    def test_plain_image_gets_docker_prefix(self):
        container = _container(
            image="myrepo/myimage:latest",
            livenessProbe={"httpGet": {"port": 8080}},
        )
        probe_script, _ = prepare_probes(container, _BASE_METADATA)
        assert '"docker://myrepo/myimage:latest"' in probe_script

    def test_docker_image_not_double_prefixed(self):
        container = _container(
            image="docker://busybox:latest",
            livenessProbe={"httpGet": {"port": 8080}},
        )
        probe_script, _ = prepare_probes(container, _BASE_METADATA)
        assert '"docker://busybox:latest"' in probe_script
        assert "docker://docker://" not in probe_script

    def test_cvmfs_image_not_prefixed(self):
        container = _container(
            image="/cvmfs/atlas.cern.ch/repo/containers/fs/singularity/x86_64-centos7",
            livenessProbe={"httpGet": {"port": 8080}},
        )
        probe_script, _ = prepare_probes(container, _BASE_METADATA)
        assert "docker://" not in probe_script


class TestPrepareProbesAnnotations:
    def test_singularity_options_from_annotation(self):
        metadata = {
            **_BASE_METADATA,
            "annotations": {
                "slurm-job.vk.io/singularity-options": "--nv --bind /scratch"
            },
        }
        container = _container(livenessProbe={"exec": {"command": ["true"]}})
        probe_script, _ = prepare_probes(container, metadata)
        assert '"--nv"' in probe_script
        assert '"--bind"' in probe_script

    def test_custom_singularity_path_from_config(self):
        orig = handles.InterLinkConfigInst.get("SingularityPath")
        singularity_path = "/opt/singularity/bin/singularity"
        handles.InterLinkConfigInst["SingularityPath"] = singularity_path
        try:
            container = _container(livenessProbe={"exec": {"command": ["true"]}})
            probe_script, _ = prepare_probes(container, _BASE_METADATA)
            assert '"/opt/singularity/bin/singularity"' in probe_script
        finally:
            if orig is None:
                handles.InterLinkConfigInst.pop("SingularityPath", None)
            else:
                handles.InterLinkConfigInst["SingularityPath"] = orig

    def test_no_singularity_path_defaults_to_singularity(self):
        orig = handles.InterLinkConfigInst.pop("SingularityPath", None)
        try:
            container = _container(livenessProbe={"exec": {"command": ["true"]}})
            probe_script, _ = prepare_probes(container, _BASE_METADATA)
            assert '"singularity"' in probe_script
        finally:
            if orig is not None:
                handles.InterLinkConfigInst["SingularityPath"] = orig


class TestPrepareProbesContainerNameNormalisation:
    def test_dashes_replaced_in_pid_var(self):
        container = _container(
            name="my-fancy-container",
            livenessProbe={"httpGet": {"port": 8080}},
        )
        _, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert "LIVENESS_PROBE_my_fancy_container_0_PID" in cleanup_script

    def test_probe_script_uses_normalised_var(self):
        container = _container(
            name="foo-bar",
            readinessProbe={"httpGet": {"port": 9000}},
        )
        probe_script, _ = prepare_probes(container, _BASE_METADATA)
        assert "READINESS_PROBE_foo_bar_0_PID" in probe_script


class TestPrepareProbesReturnTypes:
    def test_returns_tuple_of_two_strings(self):
        container = _container(livenessProbe={"httpGet": {"port": 8080}})
        result = prepare_probes(container, _BASE_METADATA)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], str)
        assert isinstance(result[1], str)

    def test_both_strings_nonempty_when_probe_defined(self):
        container = _container(readinessProbe={"exec": {"command": ["true"]}})
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert probe_script
        assert cleanup_script


# ---------------------------------------------------------------------------
# produce_htcondor_singularity_script — runCtn / multi-container tests
# ---------------------------------------------------------------------------


def _fake_metadata(name="test-pod", uid="uid-123", annotations=None):
    return {
        "name": name,
        "uid": uid,
        "namespace": "default",
        "annotations": annotations or {},
    }


def _make_script(
    containers,
    container_commands,
    metadata=None,
    input_files=None,
    probe_scripts=None,
    cleanup_scripts=None,
    data_root=None,
    prestop_trap=None,
    poststart_hooks=None,
):
    """Call produce_htcondor_singularity_script in a temp dir and return the
    generated bash script content."""
    if metadata is None:
        metadata = _fake_metadata()
    if input_files is None:
        input_files = []

    orig_dir = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        orig_dr = handles.InterLinkConfigInst.get("DataRootFolder")
        handles.InterLinkConfigInst["DataRootFolder"] = ""
        # Ensure the executable path resolves in the tmpdir
        try:
            handles.produce_htcondor_singularity_script(
                containers,
                metadata,
                container_commands,
                input_files,
                probe_scripts=probe_scripts,
                cleanup_scripts=cleanup_scripts,
                prestop_trap=prestop_trap,
                poststart_hooks=poststart_hooks,
            )
        finally:
            if orig_dr is None:
                handles.InterLinkConfigInst.pop("DataRootFolder", None)
            else:
                handles.InterLinkConfigInst["DataRootFolder"] = orig_dr
            os.chdir(orig_dir)

        # Read the generated .sh file — produce_htcondor_singularity_script
        # places it in a per-pod subdirectory: {name}-{uid}/{name}-{uid}.sh
        job_dir = os.path.join(tmpdir, f"{metadata['name']}-{metadata['uid']}")
        sh_path = os.path.join(job_dir, f"{metadata['name']}-{metadata['uid']}.sh")
        with open(sh_path) as fh:
            return fh.read()


_ONE_CONTAINER = [_container("c1", "docker://busybox:latest")]
_TWO_CONTAINERS = [
    _container("c1", "docker://busybox:latest"),
    _container("c2", "docker://alpine:latest"),
]


class TestRunCtnHelpers:
    """Script always includes the runCtn/waitCtns/endScript bash helpers."""

    def test_shebang_present(self):
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest"])],
        )
        assert script.startswith("#!/bin/bash")

    def test_runctn_function_defined(self):
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest"])],
        )
        assert "runCtn()" in script

    def test_waitctns_function_defined(self):
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest"])],
        )
        assert "waitCtns()" in script

    def test_endscript_function_defined(self):
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest"])],
        )
        assert "endScript()" in script

    def test_highestexitcode_initialized(self):
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest"])],
        )
        assert "highestExitCode=0" in script

    def test_pidctns_initialized(self):
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest"])],
        )
        assert 'pidCtns=""' in script

    def test_workingpath_exported(self):
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest"])],
        )
        assert "workingPath=$(pwd)" in script

    def test_waitctns_called_at_end(self):
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest"])],
        )
        assert "waitCtns" in script
        # The bare "\nwaitCtns\n" call must appear after the runCtn call
        waitctns_call_pos = script.index("\nwaitCtns\n")
        assert waitctns_call_pos > script.index("runCtn c1")

    def test_endscript_called_at_end(self):
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest"])],
        )
        assert "endScript" in script
        assert script.index("endScript\n") > script.index("waitCtns\n")


class TestRunCtnSingleContainer:
    """Single-container pod still uses runCtn (background execution)."""

    def test_runctn_invocation_present(self):
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest", "sh"])],
        )
        assert "runCtn c1" in script

    def test_runctn_carries_singularity_command(self):
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest", "sh"])],
        )
        assert "runCtn c1 singularity exec docker://busybox:latest sh" in script

    def test_no_direct_singularity_exec_outside_runctn(self):
        """The singularity exec should only appear inside the runCtn call, not
        as a bare top-level command."""
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest", "sh"])],
        )
        lines = script.splitlines()
        bare = [ln for ln in lines if ln.strip().startswith("singularity exec")]
        assert bare == [], f"Bare singularity exec lines found: {bare}"


class TestRunCtnMultiContainer:
    """Multiple containers each get their own runCtn call; all run in background."""

    def test_both_containers_have_runctn(self):
        script = _make_script(
            _TWO_CONTAINERS,
            [
                ("c1", ["singularity", "exec", "docker://busybox:latest", "sh"]),
                ("c2", ["singularity", "exec", "docker://alpine:latest", "sh"]),
            ],
        )
        assert "runCtn c1" in script
        assert "runCtn c2" in script

    def test_container_order_preserved(self):
        script = _make_script(
            _TWO_CONTAINERS,
            [
                ("c1", ["singularity", "exec", "docker://busybox:latest", "sh"]),
                ("c2", ["singularity", "exec", "docker://alpine:latest", "sh"]),
            ],
        )
        assert script.index("runCtn c1") < script.index("runCtn c2")

    def test_waitctns_after_both_runctn_calls(self):
        script = _make_script(
            _TWO_CONTAINERS,
            [
                ("c1", ["singularity", "exec", "docker://busybox:latest", "sh"]),
                ("c2", ["singularity", "exec", "docker://alpine:latest", "sh"]),
            ],
        )
        # Find the waitCtns *call* (the bare line "\nwaitCtns\n"), not its
        # function definition which also contains "waitCtns".
        waitctns_call_pos = script.index("\nwaitCtns\n")
        assert waitctns_call_pos > script.index("runCtn c2")

    def test_three_containers(self):
        containers = [
            _container("a", "docker://img1:latest"),
            _container("b", "docker://img2:latest"),
            _container("c", "docker://img3:latest"),
        ]
        commands = [
            ("a", ["singularity", "exec", "docker://img1:latest"]),
            ("b", ["singularity", "exec", "docker://img2:latest"]),
            ("c", ["singularity", "exec", "docker://img3:latest"]),
        ]
        script = _make_script(containers, commands)
        for name in ("a", "b", "c"):
            assert f"runCtn {name}" in script
        assert script.index("runCtn a") < script.index("runCtn b")
        assert script.index("runCtn b") < script.index("runCtn c")

    def test_each_container_gets_correct_image(self):
        script = _make_script(
            _TWO_CONTAINERS,
            [
                ("c1", ["singularity", "exec", "docker://busybox:latest"]),
                ("c2", ["singularity", "exec", "docker://alpine:latest"]),
            ],
        )
        assert "runCtn c1 singularity exec docker://busybox:latest" in script
        assert "runCtn c2 singularity exec docker://alpine:latest" in script


class TestRunCtnOutputRedirection:
    """runCtn redirects output to a per-container .out file."""

    def test_runctn_body_redirects_to_workingpath(self):
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest"])],
        )
        # Output is written to {podname}-{poduid}-{ctn}.out in the sandbox
        assert '"${_IL_POD_NAME}-${_IL_POD_UID}-${ctn}.out"' in script

    def test_background_ampersand_in_runctn(self):
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest"])],
        )
        # The & must appear inside the runCtn function body
        runctn_start = script.index("runCtn()")
        runctn_end = script.index("waitCtns()")
        runctn_body = script[runctn_start:runctn_end]
        assert " &" in runctn_body

    def test_waitctns_writes_status_file(self):
        script = _make_script(
            _ONE_CONTAINER,
            [("c1", ["singularity", "exec", "docker://busybox:latest"])],
        )
        assert "run-${ctn}.status" in script


class TestRunCtnWithProbes:
    """Probe cleanup traps appear before helpers; probe sub-shells before runCtn."""

    def _make_probe_script(self):
        """Build probe_script and cleanup_script for a liveness probe container."""
        container = _container(
            "c1",
            "docker://busybox:latest",
            livenessProbe={"httpGet": {"port": 8080}},
        )
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        return container, probe_script, cleanup_script

    def test_cleanup_before_runctn_helpers(self):
        container, probe_script, cleanup_script = self._make_probe_script()
        script = _make_script(
            [container],
            [("c1", ["singularity", "exec", "docker://busybox:latest"])],
            probe_scripts=[probe_script],
            cleanup_scripts=[cleanup_script],
        )
        cleanup_pos = script.index("cleanup_probes")
        helpers_pos = script.index("runCtn()")
        assert cleanup_pos < helpers_pos

    def test_probe_subshell_before_runctn_call(self):
        container, probe_script, cleanup_script = self._make_probe_script()
        script = _make_script(
            [container],
            [("c1", ["singularity", "exec", "docker://busybox:latest"])],
            probe_scripts=[probe_script],
            cleanup_scripts=[cleanup_script],
        )
        probe_pos = script.index("waitForProbes")
        runctn_call_pos = script.index("runCtn c1")
        assert probe_pos < runctn_call_pos


class TestCleanCommandTokens:
    """Unit tests for the _clean_command_tokens helper."""

    def test_basic_join(self):
        result = handles._clean_command_tokens(["singularity", "exec", "image"])
        assert result == "singularity exec image"

    def test_empty_tokens_removed(self):
        result = handles._clean_command_tokens(["singularity", '""', "exec", "image"])
        assert '""' not in result

    def test_c_flag_quotes_next_token(self):
        result = handles._clean_command_tokens(["sh", "-c", "echo hello"])
        assert "'echo hello'" in result

    def test_extra_spaces_collapsed(self):
        result = handles._clean_command_tokens(["a", "", "b"])
        assert "  " not in result

    def test_literal_empty_string_inside_c_script_is_preserved(self):
        script = "python - <<'EOF'\nprint((\"\", 8080))\nEOF"
        result = handles._clean_command_tokens(["sh", "-c", script])
        assert '("", 8080)' in result


class TestPrepareEnvFile:
    def test_prepare_env_file_writes_export_lines(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            handles,
            "InterLinkConfigInst",
            {"DataRootFolder": str(tmp_path) + "/"},
        )
        env_file_name, env_path = handles.prepare_env_file(
            {
                "name": "c1",
                "env": [
                    {"name": "BACKTICKS", "value": "Run `command` here"},
                    {"name": "SINGLE_QUOTES", "value": "It's working"},
                ],
            },
            {"name": "pod-a", "uid": "uid-a"},
        )
        assert env_file_name == "pod-a-uid-a_env.env"
        content = open(env_path).read()
        assert "export BACKTICKS='Run `command` here'" in content
        assert "export SINGLE_QUOTES='It'\"'\"'s working'" in content

    def test_wrap_command_with_env_injects_shell_wrapper(self):
        wrapped = handles._wrap_command_with_env(
            ["python", "-c", "print('ok')"], "env.env"
        )
        assert wrapped[:4] == ["/bin/sh", "-c", '. ./env.env && exec "$@"', "sh"]
        assert wrapped[4:] == ["python", "-c", "print('ok')"]


# ---------------------------------------------------------------------------
# Lifecycle hook tests — _translate_lifecycle_hook, generate_prestop_trap,
# prepare_lifecycle_hooks, and script injection via produce_htcondor_singularity_script
# ---------------------------------------------------------------------------


class TestTranslateLifecycleHook:
    """Unit tests for _translate_lifecycle_hook."""

    def test_none_returns_none(self):
        assert handles._translate_lifecycle_hook(None) is None

    def test_empty_dict_returns_none(self):
        assert handles._translate_lifecycle_hook({}) is None

    def test_exec_hook_basic(self):
        handler = {"exec": {"command": ["/bin/sh", "-c", "echo done"]}}
        result = handles._translate_lifecycle_hook(handler)
        assert result is not None
        assert result["type"] == "exec"
        assert result["command"] == ["/bin/sh", "-c", "echo done"]

    def test_exec_hook_empty_command_returns_none(self):
        handler = {"exec": {"command": []}}
        result = handles._translate_lifecycle_hook(handler)
        assert result is None

    def test_httpget_hook_basic(self):
        handler = {"httpGet": {"path": "/stop", "port": 8080, "scheme": "HTTP"}}
        result = handles._translate_lifecycle_hook(handler)
        assert result is not None
        assert result["type"] == "httpget"
        assert result["path"] == "/stop"
        assert result["port"] == 8080
        assert result["scheme"] == "http"
        assert result["host"] == "localhost"

    def test_httpget_hook_defaults(self):
        handler = {"httpGet": {"port": 9090}}
        result = handles._translate_lifecycle_hook(handler)
        assert result is not None
        assert result["path"] == "/"
        assert result["scheme"] == "http"
        assert result["host"] == "localhost"

    def test_httpget_named_port_returns_none(self, caplog):
        import logging

        handler = {"httpGet": {"port": "http", "path": "/stop"}}
        with caplog.at_level(logging.WARNING):
            result = handles._translate_lifecycle_hook(handler)
        assert result is None
        assert "named port" in caplog.text

    def test_unsupported_type_returns_none(self, caplog):
        import logging

        handler = {"tcpSocket": {"port": 8080}}
        with caplog.at_level(logging.WARNING):
            result = handles._translate_lifecycle_hook(handler)
        assert result is None


class TestGeneratePreStopTrap:
    """Unit tests for generate_prestop_trap."""

    def _base_metadata(self, annotations=None):
        return {
            "name": "test-pod",
            "uid": "abc-123",
            "annotations": annotations or {},
        }

    def test_no_lifecycle_returns_empty(self):
        containers = [{"name": "c1", "image": "busybox:latest"}]
        result = handles.generate_prestop_trap(containers, self._base_metadata())
        assert result == ""

    def test_null_lifecycle_returns_empty(self):
        containers = [{"name": "c1", "image": "busybox:latest", "lifecycle": None}]
        result = handles.generate_prestop_trap(containers, self._base_metadata())
        assert result == ""

    def test_empty_lifecycle_returns_empty(self):
        containers = [{"name": "c1", "image": "busybox:latest", "lifecycle": {}}]
        result = handles.generate_prestop_trap(containers, self._base_metadata())
        assert result == ""

    def test_exec_prestop_hook_defines_function(self):
        containers = [
            {
                "name": "c1",
                "image": "busybox:latest",
                "lifecycle": {
                    "preStop": {"exec": {"command": ["/bin/sh", "-c", "nginx -s quit"]}}
                },
            }
        ]
        result = handles.generate_prestop_trap(containers, self._base_metadata())
        assert "preStopTrap()" in result

    def test_exec_prestop_registers_sigterm_trap(self):
        containers = [
            {
                "name": "c1",
                "image": "busybox:latest",
                "lifecycle": {
                    "preStop": {"exec": {"command": ["/bin/sh", "-c", "nginx -s quit"]}}
                },
            }
        ]
        result = handles.generate_prestop_trap(containers, self._base_metadata())
        assert "trap preStopTrap SIGTERM" in result

    def test_exec_prestop_runs_command_in_singularity(self):
        containers = [
            {
                "name": "c1",
                "image": "docker://nginx:latest",
                "lifecycle": {
                    "preStop": {"exec": {"command": ["/bin/sh", "-c", "nginx -s quit"]}}
                },
            }
        ]
        result = handles.generate_prestop_trap(containers, self._base_metadata())
        assert "singularity" in result
        assert "docker://nginx:latest" in result
        assert "timeout" in result

    def test_exec_prestop_plain_image_gets_docker_prefix(self):
        containers = [
            {
                "name": "c1",
                "image": "nginx:latest",
                "lifecycle": {
                    "preStop": {"exec": {"command": ["/usr/sbin/nginx", "-s", "quit"]}}
                },
            }
        ]
        result = handles.generate_prestop_trap(containers, self._base_metadata())
        assert "docker://nginx:latest" in result

    def test_httpget_prestop_uses_curl(self):
        containers = [
            {
                "name": "c1",
                "image": "busybox:latest",
                "lifecycle": {
                    "preStop": {"httpGet": {"path": "/shutdown", "port": 8080}}
                },
            }
        ]
        result = handles.generate_prestop_trap(containers, self._base_metadata())
        assert "curl" in result
        assert "http://localhost:8080/shutdown" in result

    def test_prestop_terminates_pidctns(self):
        containers = [
            {
                "name": "c1",
                "image": "busybox:latest",
                "lifecycle": {
                    "preStop": {"exec": {"command": ["/bin/sh", "-c", "cleanup"]}}
                },
            }
        ]
        result = handles.generate_prestop_trap(containers, self._base_metadata())
        # Must iterate over pidCtns and kill each container
        assert "pidCtns" in result
        assert 'kill "${pid}"' in result

    def test_prestop_waits_after_termination(self):
        containers = [
            {
                "name": "c1",
                "image": "busybox:latest",
                "lifecycle": {
                    "preStop": {"exec": {"command": ["true"]}}
                },
            }
        ]
        result = handles.generate_prestop_trap(containers, self._base_metadata())
        assert "wait" in result

    def test_multiple_containers_all_hooks_present(self):
        containers = [
            {
                "name": "c1",
                "image": "busybox:latest",
                "lifecycle": {"preStop": {"exec": {"command": ["stop-c1"]}}},
            },
            {
                "name": "c2",
                "image": "alpine:latest",
                "lifecycle": {"preStop": {"exec": {"command": ["stop-c2"]}}},
            },
        ]
        result = handles.generate_prestop_trap(containers, self._base_metadata())
        assert "c1" in result
        assert "c2" in result
        # Only one SIGTERM trap should be registered
        assert result.count("trap preStopTrap SIGTERM") == 1

    def test_singularity_options_from_annotation(self):
        containers = [
            {
                "name": "c1",
                "image": "busybox:latest",
                "lifecycle": {
                    "preStop": {"exec": {"command": ["/bin/sh", "-c", "cleanup"]}}
                },
            }
        ]
        metadata = self._base_metadata(
            annotations={"slurm-job.vk.io/singularity-options": "--nv"}
        )
        result = handles.generate_prestop_trap(containers, metadata)
        assert "--nv" in result

    def test_output_file_uses_workingpath(self):
        containers = [
            {
                "name": "my-app",
                "image": "busybox:latest",
                "lifecycle": {
                    "preStop": {"exec": {"command": ["/bin/sh", "-c", "quit"]}}
                },
            }
        ]
        result = handles.generate_prestop_trap(containers, self._base_metadata())
        assert "prestop-my-app.out" in result
        assert "${workingPath}" in result


class TestPreStopTrapInScript:
    """Integration tests: preStop trap is injected into the generated script."""

    def _container_with_prestop(self, name="c1", image="docker://busybox:latest", cmd=None):
        if cmd is None:
            cmd = ["/bin/sh", "-c", "cleanup"]
        return {
            "name": name,
            "image": image,
            "lifecycle": {"preStop": {"exec": {"command": cmd}}},
        }

    def test_trap_in_script_when_prestop_defined(self):
        container = self._container_with_prestop()
        prestop_trap = handles.generate_prestop_trap(
            [container], _fake_metadata()
        )
        script = _make_script(
            [container],
            [("c1", ["singularity", "exec", "docker://busybox:latest", "sh"])],
            prestop_trap=prestop_trap,
        )
        assert "trap preStopTrap SIGTERM" in script

    def test_no_trap_when_no_prestop(self):
        container = _container("c1", "docker://busybox:latest")
        script = _make_script(
            [container],
            [("c1", ["singularity", "exec", "docker://busybox:latest", "sh"])],
        )
        assert "trap preStopTrap SIGTERM" not in script

    def test_trap_defined_before_runctn_call(self):
        container = self._container_with_prestop()
        prestop_trap = handles.generate_prestop_trap([container], _fake_metadata())
        script = _make_script(
            [container],
            [("c1", ["singularity", "exec", "docker://busybox:latest", "sh"])],
            prestop_trap=prestop_trap,
        )
        trap_pos = script.index("trap preStopTrap SIGTERM")
        runctn_pos = script.index("runCtn c1")
        assert trap_pos < runctn_pos

    def test_trap_defined_after_runctn_helpers(self):
        container = self._container_with_prestop()
        prestop_trap = handles.generate_prestop_trap([container], _fake_metadata())
        script = _make_script(
            [container],
            [("c1", ["singularity", "exec", "docker://busybox:latest", "sh"])],
            prestop_trap=prestop_trap,
        )
        helpers_pos = script.index("runCtn()")
        trap_pos = script.index("trap preStopTrap SIGTERM")
        assert helpers_pos < trap_pos


class TestPrepareLifecycleHooks:
    """Unit tests for prepare_lifecycle_hooks."""

    def test_returns_empty_when_no_lifecycle(self):
        containers = [{"name": "c1", "image": "busybox:latest"}]
        result = handles.prepare_lifecycle_hooks(containers, _BASE_METADATA)
        assert result == ""

    def test_returns_trap_when_prestop_defined(self):
        containers = [
            {
                "name": "c1",
                "image": "busybox:latest",
                "lifecycle": {
                    "preStop": {"exec": {"command": ["/bin/sh", "-c", "quit"]}}
                },
            }
        ]
        result = handles.prepare_lifecycle_hooks(containers, _BASE_METADATA)
        assert "preStopTrap" in result
        assert "trap preStopTrap SIGTERM" in result

    def test_returns_string(self):
        containers = [{"name": "c1", "image": "busybox:latest"}]
        result = handles.prepare_lifecycle_hooks(containers, _BASE_METADATA)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# postStart lifecycle hook tests
# ---------------------------------------------------------------------------


class TestPostStartHelpers:
    """Unit tests for _find_tmp_bind_in_tokens, _find_image_in_tokens,
    _inject_hook_tmp_into_cmd."""

    def test_find_tmp_bind_not_present(self):
        tokens = ["singularity", "exec", "docker://busybox:latest", "sh"]
        assert handles._find_tmp_bind_in_tokens(tokens) is None

    def test_find_tmp_bind_present(self):
        tokens = [
            "singularity",
            "exec",
            "--bind",
            "/host/tmp:/tmp",
            "docker://busybox:latest",
            "sh",
        ]
        assert handles._find_tmp_bind_in_tokens(tokens) == "/host/tmp"

    def test_find_tmp_bind_in_comma_spec(self):
        tokens = ["singularity", "exec", "--bind", "/a:/b,/h/t:/tmp", "docker://img", "sh"]
        assert handles._find_tmp_bind_in_tokens(tokens) == "/h/t"

    def test_find_image_docker_prefix(self):
        tokens = ["singularity", "exec", "docker://busybox:latest", "sh"]
        assert handles._find_image_in_tokens(tokens) == "docker://busybox:latest"

    def test_find_image_cvmfs(self):
        tokens = ["singularity", "exec", "/cvmfs/img.sif", "sh"]
        assert handles._find_image_in_tokens(tokens) == "/cvmfs/img.sif"

    def test_find_image_not_found(self):
        tokens = ["singularity", "exec", "sh"]
        assert handles._find_image_in_tokens(tokens) == ""

    def test_inject_hook_tmp_adds_bind_before_image(self):
        tokens = ["singularity", "exec", "docker://busybox:latest", "sh"]
        result = handles._inject_hook_tmp_into_cmd(tokens)
        img_pos = result.index("docker://busybox:latest")
        assert result[img_pos - 2] == "--bind"
        assert "${workingPath}/hook-tmp:/tmp" in result[img_pos - 1]

    def test_inject_hook_tmp_preserves_other_tokens(self):
        tokens = ["singularity", "exec", "--bind", "src:/dst", "docker://img", "cmd"]
        result = handles._inject_hook_tmp_into_cmd(tokens)
        assert "singularity" in result
        assert "docker://img" in result
        assert "cmd" in result


class TestGeneratePostStartFragment:
    """Unit tests for _generate_poststart_fragment."""

    def _base_metadata(self):
        return {"name": "p", "uid": "u", "annotations": {}}

    def test_exec_hook_runs_in_singularity(self):
        hook = {"type": "exec", "command": ["/bin/sh", "-c", "echo hi"]}
        tokens = ["singularity", "exec", "docker://busybox:latest", "sh"]
        result = handles._generate_poststart_fragment(
            "c1", hook, '"${workingPath}/hook-tmp:/tmp"', "singularity", "", tokens
        )
        assert "singularity" in result
        assert "docker://busybox:latest" in result
        assert "timeout" in result

    def test_exec_hook_logs_to_container_out_file(self):
        hook = {"type": "exec", "command": ["/bin/sh", "-c", "echo hi"]}
        tokens = ["singularity", "exec", "docker://busybox:latest", "sh"]
        result = handles._generate_poststart_fragment(
            "c1", hook, '"${workingPath}/hook-tmp:/tmp"', "singularity", "", tokens
        )
        assert "${_IL_POD_NAME}-${_IL_POD_UID}-c1.out" in result

    def test_httpget_hook_uses_curl(self):
        hook = {
            "type": "httpget",
            "scheme": "http",
            "host": "localhost",
            "port": 8080,
            "path": "/ready",
        }
        tokens = ["singularity", "exec", "docker://busybox:latest", "sh"]
        result = handles._generate_poststart_fragment(
            "c1", hook, "", "singularity", "", tokens
        )
        assert "curl" in result
        assert "http://localhost:8080/ready" in result

    def test_hook_tmp_bind_included_in_singularity_call(self):
        hook = {"type": "exec", "command": ["true"]}
        tokens = ["singularity", "exec", "docker://busybox:latest", "sh"]
        result = handles._generate_poststart_fragment(
            "c1",
            hook,
            '"${workingPath}/hook-tmp:/tmp"',
            "singularity",
            "",
            tokens,
        )
        assert "${workingPath}/hook-tmp:/tmp" in result

    def test_singularity_options_included(self):
        hook = {"type": "exec", "command": ["true"]}
        tokens = ["singularity", "exec", "docker://img", "sh"]
        result = handles._generate_poststart_fragment(
            "c1", hook, "", "singularity", "--nv", tokens
        )
        assert "--nv" in result

    def test_completion_message_present(self):
        hook = {"type": "exec", "command": ["true"]}
        tokens = ["singularity", "exec", "docker://img", "sh"]
        result = handles._generate_poststart_fragment(
            "c1", hook, "", "singularity", "", tokens
        )
        assert "postStart hook for container c1 completed" in result


class TestPostStartInScript:
    """Integration tests: postStart hook is injected into the generated script."""

    def _container_with_poststart(self, name="c1", image="docker://busybox:latest"):
        return {
            "name": name,
            "image": image,
            "lifecycle": {
                "postStart": {"exec": {"command": ["/bin/sh", "-c", "echo started"]}}
            },
        }

    def _poststart_hooks(self, container):
        lifecycle = container.get("lifecycle") or {}
        ps = lifecycle.get("postStart")
        return {container["name"]: handles._translate_lifecycle_hook(ps) if ps else None}

    def test_no_poststart_when_not_defined(self):
        container = _container("c1", "docker://busybox:latest")
        script = _make_script(
            [container],
            [("c1", ["singularity", "exec", "docker://busybox:latest", "sh"])],
        )
        assert "postStart hook" not in script

    def test_poststart_fragment_injected(self):
        container = self._container_with_poststart()
        hooks = self._poststart_hooks(container)
        script = _make_script(
            [container],
            [("c1", ["singularity", "exec", "docker://busybox:latest", "sh"])],
            poststart_hooks=hooks,
        )
        assert "postStart hook for container c1" in script

    def test_poststart_runs_before_runctn(self):
        container = self._container_with_poststart()
        hooks = self._poststart_hooks(container)
        script = _make_script(
            [container],
            [("c1", ["singularity", "exec", "docker://busybox:latest", "sh"])],
            poststart_hooks=hooks,
        )
        ps_pos = script.index("postStart hook for container c1")
        runctn_pos = script.index("runCtn c1")
        assert ps_pos < runctn_pos

    def test_hook_tmp_dir_created_when_no_tmp_mount(self):
        container = self._container_with_poststart()
        hooks = self._poststart_hooks(container)
        script = _make_script(
            [container],
            [("c1", ["singularity", "exec", "docker://busybox:latest", "sh"])],
            poststart_hooks=hooks,
        )
        assert 'mkdir -p "${workingPath}/hook-tmp"' in script

    def test_hook_tmp_bind_injected_into_runctn_command(self):
        container = self._container_with_poststart()
        hooks = self._poststart_hooks(container)
        script = _make_script(
            [container],
            [("c1", ["singularity", "exec", "docker://busybox:latest", "sh"])],
            poststart_hooks=hooks,
        )
        # The runCtn call for c1 must include the hook-tmp bind
        runctn_line = next(
            ln for ln in script.splitlines() if ln.startswith("runCtn c1")
        )
        assert "${workingPath}/hook-tmp:/tmp" in runctn_line

    def test_no_extra_hook_tmp_when_tmp_already_bound(self):
        container = self._container_with_poststart()
        hooks = self._poststart_hooks(container)
        # cmd_tokens already has a /tmp bind
        tokens = [
            "singularity",
            "exec",
            "--bind",
            "/host/tmp:/tmp",
            "docker://busybox:latest",
            "sh",
        ]
        script = _make_script(
            [container],
            [("c1", tokens)],
            poststart_hooks=hooks,
        )
        # hook-tmp directory should NOT be created
        assert 'mkdir -p "${workingPath}/hook-tmp"' not in script


# ---------------------------------------------------------------------------
# API compatibility tests — interlink 0.6.1
# ---------------------------------------------------------------------------


def _flask_test_client():
    """Return a Flask test client with the handles app."""
    handles.app.config["TESTING"] = True
    return handles.app.test_client()


def _make_pod(name="test-pod", uid="uid-123", namespace="default", containers=None):
    if containers is None:
        containers = [{"name": "c1", "image": "busybox:latest"}]
    return {
        "metadata": {"name": name, "uid": uid, "namespace": namespace},
        "spec": {"containers": containers},
        "status": {"phase": "Running"},
    }


class TestCreateResponseFormat:
    """/create must return HTTP 200 with {PodUID, PodJID} (interlink 0.6.1 contract)."""

    def _call_create(self, tmp_path, monkeypatch):
        """Stub HTCondor dependencies and POST to /create; return the response."""
        monkeypatch.setattr(
            handles,
            "InterLinkConfigInst",
            {"DataRootFolder": str(tmp_path) + "/"},
        )
        monkeypatch.setattr(handles, "htcondor_batch_submit", lambda path: "123.0")
        monkeypatch.setattr(handles, "handle_jid", lambda jid, pod: None)
        job_dir = tmp_path / "test-pod-uid-123"
        job_dir.mkdir()
        (job_dir / "test-pod-uid-123.jid").write_text("123.0")
        monkeypatch.setattr(
            handles,
            "produce_htcondor_singularity_script",
            lambda *a, **kw: str(tmp_path / "fake.jdl"),
        )
        payload = _json.dumps({"pod": _make_pod(), "container": []})
        return _flask_test_client().post(
            "/create", data=payload, content_type="application/json"
        )

    def test_create_returns_200_not_201(self, tmp_path, monkeypatch):
        resp = self._call_create(tmp_path, monkeypatch)
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"

    def test_create_response_has_poduid_and_podjid(self, tmp_path, monkeypatch):
        resp = self._call_create(tmp_path, monkeypatch)
        data = _json.loads(resp.data)
        assert "PodUID" in data
        assert "PodJID" in data
        assert data["PodUID"] == "uid-123"
        assert data["PodJID"] == "123.0"

    def test_create_response_has_no_extra_metadata_field(self, tmp_path, monkeypatch):
        resp = self._call_create(tmp_path, monkeypatch)
        data = _json.loads(resp.data)
        # interlink 0.6.1 CreateStruct only has PodUID and PodJID
        assert "metadata" not in data


class TestStatusHandlerMultiPod:
    """/status must return statuses for ALL pods in the request array."""

    def _make_jid_file(self, tmp_path, pod_name, pod_uid, jid):
        job_dir = tmp_path / f"{pod_name}-{pod_uid}"
        job_dir.mkdir()
        (job_dir / f"{pod_name}-{pod_uid}.jid").write_text(jid)

    def _setup_config(self, tmp_path, monkeypatch):
        """Patch InterLinkConfigInst to use tmp_path as data root."""
        monkeypatch.setattr(
            handles,
            "InterLinkConfigInst",
            {"DataRootFolder": str(tmp_path) + "/"},
        )

    def _fake_condor_output(self, monkeypatch, jid, job_status=2):
        """Patch os.popen so condor_q returns a minimal JSON job record."""
        job_record = _json.dumps(
            [{"JobStatus": job_status, "ClusterId": int(jid.split(".")[0])}]
        )

        def fake_popen(cmd):
            class FakeProc:
                def read(self):
                    if f"condor_q {jid}" in cmd or f"condor_history {jid}" in cmd:
                        return job_record
                    return ""

                def close(self):
                    pass

            return FakeProc()

        monkeypatch.setattr(os, "popen", fake_popen)

    def _get_statuses(self, pods):
        """POST a /status request and return the parsed JSON list."""
        client = _flask_test_client()
        resp = client.get(
            "/status", data=_json.dumps(pods), content_type="application/json"
        )
        assert resp.status_code == 200
        return _json.loads(resp.data)

    def test_single_pod_returns_one_status(self, tmp_path, monkeypatch):
        self._setup_config(tmp_path, monkeypatch)
        self._make_jid_file(tmp_path, "pod-a", "uid-a", "100.0")
        self._fake_condor_output(monkeypatch, "100.0", job_status=2)
        statuses = self._get_statuses([_make_pod("pod-a", "uid-a")])
        assert len(statuses) == 1
        assert statuses[0]["name"] == "pod-a"

    def test_multiple_pods_returns_all_statuses(self, tmp_path, monkeypatch):
        self._setup_config(tmp_path, monkeypatch)
        self._make_jid_file(tmp_path, "pod-a", "uid-a", "100.0")
        self._make_jid_file(tmp_path, "pod-b", "uid-b", "101.0")

        job_records = {
            "100.0": _json.dumps([{"JobStatus": 2}]),
            "101.0": _json.dumps([{"JobStatus": 1}]),
        }

        def fake_popen(cmd):
            class FakeProc:
                def read(self):
                    for jid, record in job_records.items():
                        if jid in cmd:
                            return record
                    return ""

                def close(self):
                    pass

            return FakeProc()

        monkeypatch.setattr(os, "popen", fake_popen)
        pods = [_make_pod("pod-a", "uid-a"), _make_pod("pod-b", "uid-b")]
        statuses = self._get_statuses(pods)
        assert len(statuses) == 2
        names = {s["name"] for s in statuses}
        assert "pod-a" in names
        assert "pod-b" in names

    def test_status_response_has_init_containers_field(self, tmp_path, monkeypatch):
        self._setup_config(tmp_path, monkeypatch)
        self._make_jid_file(tmp_path, "pod-a", "uid-a", "100.0")
        self._fake_condor_output(monkeypatch, "100.0", job_status=2)
        statuses = self._get_statuses([_make_pod("pod-a", "uid-a")])
        assert "initContainers" in statuses[0]

    def test_status_response_has_jid_field(self, tmp_path, monkeypatch):
        self._setup_config(tmp_path, monkeypatch)
        self._make_jid_file(tmp_path, "pod-a", "uid-a", "100.0")
        self._fake_condor_output(monkeypatch, "100.0", job_status=2)
        statuses = self._get_statuses([_make_pod("pod-a", "uid-a")])
        assert statuses[0]["JID"] == "100.0"
        assert statuses[0]["UID"] == "uid-a"
        assert statuses[0]["namespace"] == "default"

    def test_missing_pod_skipped_not_fatal(self, tmp_path, monkeypatch):
        """If one pod's JID file doesn't exist, others still get status."""
        self._setup_config(tmp_path, monkeypatch)
        # Only pod-a has a JID file; pod-b does not
        self._make_jid_file(tmp_path, "pod-a", "uid-a", "100.0")
        self._fake_condor_output(monkeypatch, "100.0", job_status=2)
        pods = [_make_pod("pod-a", "uid-a"), _make_pod("pod-b", "uid-b")]
        statuses = self._get_statuses(pods)
        # Only pod-a should be in the response
        assert len(statuses) == 1
        assert statuses[0]["name"] == "pod-a"


class TestLogsHandler:
    def test_getlogs_uses_sandbox_filename_for_condor_tail(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            handles,
            "InterLinkConfigInst",
            {"DataRootFolder": str(tmp_path) + "/"},
        )

        job_dir = tmp_path / "pod-a-uid-a"
        job_dir.mkdir()
        (job_dir / "pod-a-uid-a.jid").write_text("100")

        seen = {}

        def fake_run(cmd, capture_output, text, timeout):
            seen["cmd"] = cmd

            class Result:
                returncode = 1
                stdout = "probe log line\n"
                stderr = ""

            return Result()

        monkeypatch.setattr(handles.subprocess, "run", fake_run)

        resp = _flask_test_client().get(
            "/getLogs",
            data=_json.dumps(
                {
                    "PodName": "pod-a",
                    "PodUID": "uid-a",
                    "ContainerName": "main",
                }
            ),
            content_type="application/json",
        )

        assert resp.status_code == 200
        assert resp.data.decode() == "probe log line\n"
        assert seen["cmd"][-1] == "pod-a-uid-a-main.out"


class TestSystemInfoEndpoint:
    """/system-info must return JSON with status and htcondor_connected fields."""

    @staticmethod
    def _make_condor_popen(output):
        """Return a fake os.popen that reads the given string."""

        def fake_popen(cmd):
            class FakeProc:
                def read(self):
                    return output

                def close(self):
                    pass

            return FakeProc()

        return fake_popen

    def test_system_info_returns_200(self, monkeypatch):
        monkeypatch.setattr(os, "popen", self._make_condor_popen("TotalMachines=10\n"))
        resp = _flask_test_client().get("/system-info")
        assert resp.status_code == 200

    def test_system_info_returns_json(self, monkeypatch):
        monkeypatch.setattr(os, "popen", self._make_condor_popen("TotalMachines=10\n"))
        resp = _flask_test_client().get("/system-info")
        data = _json.loads(resp.data)
        assert "status" in data
        assert "htcondor_connected" in data
        assert "timestamp" in data

    def test_system_info_connected_when_condor_responds(self, monkeypatch):
        monkeypatch.setattr(
            os, "popen", self._make_condor_popen("TotalMachines=5 TotalCPUs=20\n")
        )
        resp = _flask_test_client().get("/system-info")
        data = _json.loads(resp.data)
        assert data["htcondor_connected"] is True
        assert data["status"] == "ok"

    def test_system_info_not_connected_on_error(self, monkeypatch):
        def fake_popen(cmd):
            raise OSError("condor not found")

        monkeypatch.setattr(os, "popen", fake_popen)
        resp = _flask_test_client().get("/system-info")
        assert resp.status_code == 200  # endpoint itself always returns 200
        data = _json.loads(resp.data)
        assert data["htcondor_connected"] is False
        assert data["status"] == "warning"
