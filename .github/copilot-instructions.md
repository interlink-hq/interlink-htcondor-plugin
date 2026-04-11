# Copilot instructions for interlink-htcondor-plugin

This file is intended to help Copilot-style assistants (and contributors) understand and work with this repository quickly.

1) Build, test, and lint commands

- Install runtime deps (used by README):
  - pip3 install flask pyyaml

- Run the plugin server (development):
  - python3 handles.py --port 8000
  - Common flags (see README): --schedd-host, --collector-host, --condor-config, --auth-method, --proxy, --cadir, --certfile, --keyfile, --debug, --dummy-job

- Docker image (optional):
  - docker build -t interlink-htcondor-plugin -f docker/Dockerfile .
  - docker run --rm -p 4000:8000 -v /etc/grid-security:/etc/grid-security:ro -v /tmp:/tmp interlink-htcondor-plugin

- Tests (pytest):
  - Run all tests: pytest -v
  - Run a single test function: pytest tests/test_probes.py::test_http_probe_basic -q
  - Run a single test file: pytest tests/test_handles.py -q

- Linting / static checks:
  - Flake8 configuration provided in .github/linters/.flake8 (max-line-length 88, ignores E203,W503). Note: probes.py intentionally contains embedded bash strings and is excluded from E501 checks.

2) High-level architecture (big picture)

- Purpose: This repository implements an InterLink sidecar plugin that converts Kubernetes Pod specs (from InterLink) into HTCondor job submissions using Singularity/Apptainer or host-script execution.

- Main components:
  - handles.py: Flask HTTP server + request handlers. Implements /create, /delete, /status, /getLogs, /system-info endpoints, and most of the logic to prepare jobs, generate shell scripts and JDL files, and submit to HTCondor.
  - probes.py: Translates Kubernetes probes (liveness/readiness/startup) into portable bash probe snippets that run alongside containers inside the job script. Provides generation and cleanup functions used by handles.py.
  - tests/: Unit tests for handles.py and probes.py that capture expected script fragments and translation behavior.
  - SidecarConfig.yaml: runtime configuration (CommandPrefix, ExportPodData, DataRootFolder, SingularityPath, etc.). This config is loaded at module import time by handles.py.
  - docker/: Dockerfile and related config for running a self-contained HTCondor mini-schedd environment for integration testing.

- Job generation flow (high level):
  1. POST /create with an InterLink CreateStruct (pod + container resolved objects).
  2. handles.py validates the pod, prepares env files, mounts, and probe scripts (prepare_env_file, prepare_mounts, prepare_probes).
  3. produce_htcondor_singularity_script writes an executable shell script and a JDL (.jdl) submit file. It injects helpers (runCtn/waitCtns/endScript) and probe sub-shells.
  4. htcondor_batch_submit runs condor_submit (either locally or to a specified pool) and parses the returned cluster ID.
  5. Job output and per-container logs are transferred back and retrieved by LogsHandler (via condor_tail) or from the DataRootFolder.

3) Key conventions and repository-specific patterns

- DataRootFolder and file layout:
  - SidecarConfig.yaml.DataRootFolder (default ".interlink/") is the canonical root for job executables, JDLs, env files, and transferred Output/Log/Error directories. Handles.py resolves this early and uses absolute paths to ensure safety.
  - All generated files follow the naming convention: {pod-name}-{pod-uid}.sh, .jdl, .jid and per-container output files {pod-name}-{pod-uid}-{container}.out.

- runCtn / waitCtns / endScript pattern:
  - Multi-container pods are implemented by launching each container command in background via runCtn(), collecting pids in pidCtns, then waitCtns() waits for each pid and computes the highest exit code; endScript() exits with that highest code. This mirrors the SLURM interlink plugin pattern and is relied upon throughout tests.

- Probes translation and execution:
  - Only httpGet and exec probe types are supported. probes.py translates Kubernetes probe dicts to ProbeCommand objects and emits bash functions and orchestration sub-shells.
  - Probe helper functions (executeHTTPProbe, executeExecProbe, runProbe, runStartupProbe, waitForProbes) are injected into the generated job script before the main singularity exec to ensure probes run concurrently and can terminate containers on failures.
  - Cleanup trap: generate_probe_cleanup_script emits cleanup_probes() and a trap cleanup_probes EXIT so probe PIDs are killed when the job exits.

- Singularity / image handling:
  - Images starting with "/cvmfs" or "docker://" are used as-is; other images are prefixed with "docker://" by prepare_probes / prepare_mounts.
  - Host-mode execution: an image name containing the literal substring "host" triggers host-script mode (produce_htcondor_host_script) where the container args are treated as host commands rather than a singularity exec.
  - Singularity options may be provided via pod annotation "slurm-job.vk.io/singularity-options" and are included when generating exec commands for exec-type probes and container commands.

- Environment files and secrets handling:
  - prepare_env_file writes KEY=VALUE lines to an env file (mode 0644). Important: values must NOT be shell-quoted — Singularity reads the file literally. The code intentionally avoids shlex.quote for env file values.
  - envFrom (configMaps / secrets) are expanded into the same env file when resolved and provided in the create request.
  - ExportPodData controls whether ConfigMaps/Secrets are written to DataRootFolder and bound into the container via --bind.

- HTCondor submit details:
  - produce_htcondor_singularity_script sets InitialDir to the absolute DataRootFolder so HTCondor transfers output files back to the submit node in that directory.
  - transfer_input_files and transfer_output_files are computed from mounts/env files and per-container output files; when_to_transfer_output = ON_EXIT_OR_EVICT is used.
  - htcondor_batch_submit accepts optional --pool/-remote flags when collector and schedd are provided; otherwise submits locally.

- Parsing and rounding rules:
  - parse_cpu accepts plain and millicore ("100m") representations and rounds up to an integer number of CPUs (minimum 1) suitable for HTCondor RequestCpus.
  - parse_string_with_suffix converts common memory suffixes (Mi, Gi, etc.) into MB for RequestMemory.

- Safety checks and path validation:
  - htcondor_batch_submit verifies the JDL path is inside the configured DataRootFolder (prevents path escape) using os.path.realpath checks.
  - produce_htcondor_singularity_script ensures DataRootFolder subdirs (log/out/err) exist and are mode 1777 to match HTCondor transfer expectations.

- Tests and expectations:
  - Tests exercise probe translation, script fragments, and the script-generation helpers. Many tests assert that specific helper functions, PID variables and probe orchestration sub-shells are present in the generated scripts.
  - When writing code that affects script output (handles.py / probes.py), update or review these tests carefully.

4) Files to consult for more detail

- README.md — usage, quick start, endpoints, and examples (primary user-facing doc).
- SidecarConfig.yaml — runtime config read by handles.py on import.
- probes.py — probe translation and bash-generation logic (contains long embedded bash strings; intentionally exempt from E501 in flake8 config).
- handles.py — main server logic and job/script/JDL generation.
- tests/ — unit tests that document expected script fragments and behavior.
- docker/Dockerfile — helpful for running a contained HTCondor test environment.
- test/vk-test-set/CLAUDE.md — guidance for vk-test-set tests if using Virtual Kubelet tests.

5) Existing AI assistant configs to merge

- test/vk-test-set/CLAUDE.md contains useful test-run and environment notes for the vk-test-set (pytest commands, KUBECONFIG/VKTEST_CONFIG), include those when running Virtual Kubelet tests.

---

If desired, configure MCP server integrations (e.g., Playwright) for this repository. Would you like me to configure any MCP servers for this project now?

Summary: created .github/copilot-instructions.md with build/test/lint commands, architecture overview, and repository-specific conventions; ask if adjustments or additional coverage are needed.
