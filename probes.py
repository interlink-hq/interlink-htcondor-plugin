"""
Kubernetes probe support for the HTCondor interLink plugin.

Translates Kubernetes liveness, readiness, and startup probe specifications
into bash script snippets that are injected into the HTCondor job script,
mirroring the approach used by the interlink-slurm-plugin.
"""

import logging

# ---------------------------------------------------------------------------
# Internal probe representation
# ---------------------------------------------------------------------------

PROBE_TYPE_HTTP = "http"
PROBE_TYPE_EXEC = "exec"


class HTTPGetAction:
    def __init__(self, scheme="HTTP", host="localhost", port=80, path="/"):
        self.scheme = scheme or "HTTP"
        self.host = host or "localhost"
        self.port = int(port) if port else 80
        self.path = path or "/"


class ExecAction:
    def __init__(self, command=None):
        self.command = command or []


class ProbeCommand:
    def __init__(
        self,
        probe_type,
        initial_delay_seconds=0,
        period_seconds=10,
        timeout_seconds=1,
        success_threshold=1,
        failure_threshold=3,
        http_get=None,
        exec_action=None,
    ):
        self.type = probe_type
        self.initial_delay_seconds = initial_delay_seconds
        self.period_seconds = period_seconds if period_seconds else 10
        self.timeout_seconds = timeout_seconds if timeout_seconds else 1
        self.success_threshold = success_threshold if success_threshold else 1
        self.failure_threshold = failure_threshold if failure_threshold else 3
        self.http_get = http_get  # HTTPGetAction | None
        self.exec_action = exec_action  # ExecAction | None


# ---------------------------------------------------------------------------
# Translation helpers
# ---------------------------------------------------------------------------


def _translate_single_probe(k8s_probe):
    """Convert a single Kubernetes probe dict to a ProbeCommand, or None."""
    if k8s_probe is None:
        return None

    probe = ProbeCommand(
        probe_type=None,
        initial_delay_seconds=k8s_probe.get("initialDelaySeconds", 0),
        period_seconds=k8s_probe.get("periodSeconds", 10),
        timeout_seconds=k8s_probe.get("timeoutSeconds", 1),
        success_threshold=k8s_probe.get("successThreshold", 1),
        failure_threshold=k8s_probe.get("failureThreshold", 3),
    )

    if "httpGet" in k8s_probe and k8s_probe["httpGet"] is not None:
        http = k8s_probe["httpGet"]
        probe.type = PROBE_TYPE_HTTP
        probe.http_get = HTTPGetAction(
            scheme=http.get("scheme", "HTTP"),
            host=http.get("host", "localhost"),
            port=http.get("port", 80),
            path=http.get("path", "/"),
        )
        return probe

    if "exec" in k8s_probe and k8s_probe["exec"] is not None:
        probe.type = PROBE_TYPE_EXEC
        probe.exec_action = ExecAction(command=k8s_probe["exec"].get("command", []))
        return probe

    logging.warning(
        "Unsupported probe type (only HTTP and Exec are supported); probe ignored."
    )
    return None


def translate_kubernetes_probes(container):
    """
    Parse Kubernetes probe specs from a container dict.

    Returns a tuple (readiness_probes, liveness_probes, startup_probes), each
    a list of ProbeCommand objects (typically 0 or 1 element).
    """
    readiness_probes = []
    liveness_probes = []
    startup_probes = []

    if container.get("startupProbe"):
        probe = _translate_single_probe(container["startupProbe"])
        if probe is not None:
            startup_probes.append(probe)
            logging.debug(
                "Translated startup probe for container %s", container.get("name")
            )

    if container.get("readinessProbe"):
        probe = _translate_single_probe(container["readinessProbe"])
        if probe is not None:
            readiness_probes.append(probe)
            logging.debug(
                "Translated readiness probe for container %s", container.get("name")
            )

    if container.get("livenessProbe"):
        probe = _translate_single_probe(container["livenessProbe"])
        if probe is not None:
            liveness_probes.append(probe)
            logging.debug(
                "Translated liveness probe for container %s", container.get("name")
            )

    return readiness_probes, liveness_probes, startup_probes


# ---------------------------------------------------------------------------
# Bash script generation
# ---------------------------------------------------------------------------


def _build_probe_args(probe):
    """Return the positional argument string for executeHTTPProbe / executeExecProbe."""
    if probe.type == PROBE_TYPE_HTTP:
        h = probe.http_get
        return f'"{h.scheme}" "{h.host}" {h.port} "{h.path}" {probe.timeout_seconds}'
    if probe.type == PROBE_TYPE_EXEC:
        return " ".join(f'"{c}"' for c in probe.exec_action.command)
    return ""


def generate_probe_script(
    container_name,
    image_name,
    readiness_probes,
    liveness_probes,
    startup_probes,
    singularity_path="singularity",
    singularity_options=None,
):
    """
    Generate the bash probe script section for a single container.

    Returns an empty string if no probes are defined.
    The returned script is intended to be appended to the HTCondor job
    executable *before* the main singularity exec command so that the
    probe sub-shell runs alongside the container in the background.
    """
    if not readiness_probes and not liveness_probes and not startup_probes:
        return ""

    if singularity_options is None:
        singularity_options = []

    # Build the singularity exec prefix for exec probes
    singularity_exec_prefix = f'"{singularity_path}" exec'
    for opt in singularity_options:
        singularity_exec_prefix += f' "{opt}"'
    singularity_exec_prefix += f' "{image_name}"'

    lines = []

    # ------------------------------------------------------------------ #
    # Helper function definitions                                          #
    # ------------------------------------------------------------------ #
    lines.append("""
# ---- Probe helper functions ----
executeHTTPProbe() {
    local scheme="$1"
    local host="$2"
    local port="$3"
    local path="$4"
    local timeout="$5"

    if [ -z "$host" ] || [ "$host" = "localhost" ] || [ "$host" = "127.0.0.1" ]; then
        host="localhost"
    fi

    local url="${scheme,,}://${host}:${port}${path}"
    timeout "${timeout}" curl -f -s "$url" &>/dev/null
    return $?
}
""")

    lines.append(f"""executeExecProbe() {{
    local timeout="$1"
    shift
    local command=("$@")
    timeout "${{timeout}}" {singularity_exec_prefix} "${{command[@]}}"
    return $?
}}
""")

    lines.append(f"""
workingPath="${{workingPath:-/tmp}}"

shutDownContainersOnProbeFail() {{
    printf "%s\\n" "$(date -Is --utc) Probe failure detected for container {container_name} – terminating job."
    for pidCtn in ${{pidCtns:-}}; do
        pid="${{pidCtn%:*}}"
        ctn="${{pidCtn#*:}}"
        printf "%s\\n" "$(date -Is --utc) Killing container ${{ctn}} (pid ${{pid}})."
        kill "${{pid}}" 2>/dev/null || true
        printf "1\\n" > "${{workingPath}}/run-${{ctn}}.status"
    done
}}

runProbe() {{
    local probe_type="$1"
    local container_name="$2"
    local initial_delay="$3"
    local period="$4"
    local timeout="$5"
    local success_threshold="$6"
    local failure_threshold="$7"
    local probe_name="$8"
    local probe_index="$9"
    shift 9
    local probe_args=("$@")

    local probe_status_file="${{workingPath}}/${{probe_name}}-probe-${{container_name}}-${{probe_index}}.status"
    local probe_timestamp_file="${{workingPath}}/${{probe_name}}-probe-${{container_name}}-${{probe_index}}.timestamp"

    printf "%s\\n" "$(date -Is --utc) Starting ${{probe_name}} probe for container ${{container_name}}..."
    echo "UNKNOWN" > "$probe_status_file"
    date -Is --utc > "$probe_timestamp_file"

    if [ "$initial_delay" -gt 0 ]; then
        printf "%s\\n" "$(date -Is --utc) Waiting ${{initial_delay}}s before starting ${{probe_name}} probe..."
        sleep "$initial_delay"
    fi

    local consecutive_successes=0
    local consecutive_failures=0
    local probe_ready=false

    while true; do
        date -Is --utc > "$probe_timestamp_file"

        if [ "$probe_type" = "http" ]; then
            executeHTTPProbe "${{probe_args[@]}}"
        elif [ "$probe_type" = "exec" ]; then
            executeExecProbe "$timeout" "${{probe_args[@]}}"
        fi

        local exit_code=$?

        if [ $exit_code -eq 0 ]; then
            consecutive_successes=$((consecutive_successes + 1))
            consecutive_failures=0

            if [ $consecutive_successes -ge $success_threshold ]; then
                if [ "$probe_name" = "readiness" ]; then
                    printf "%s\\n" "$(date -Is --utc) ${{probe_name}} probe succeeded for ${{container_name}} (${{success_threshold}} times). Container is healthy."
                elif [ "$probe_name" = "liveness" ]; then
                    if [ "$probe_ready" = false ]; then
                        printf "%s\\n" "$(date -Is --utc) ${{probe_name}} probe succeeded for ${{container_name}} (${{success_threshold}} times). Container is alive."
                    fi
                fi
                echo "SUCCESS" > "$probe_status_file"
                probe_ready=true
                if [ "$probe_name" = "readiness" ]; then
                    return 0
                fi
            fi
        else
            consecutive_failures=$((consecutive_failures + 1))
            consecutive_successes=0
            printf "%s\\n" "$(date -Is --utc) ${{probe_name}} probe failed for ${{container_name}} (${{consecutive_failures}}/${{failure_threshold}})"
            echo "FAILURE" > "$probe_status_file"
            probe_ready=false

            if [ $consecutive_failures -ge $failure_threshold ]; then
                printf "%s\\n" "$(date -Is --utc) ${{probe_name}} probe failed for ${{container_name}} after ${{failure_threshold}} attempts" >&2
                echo "FAILED_THRESHOLD" > "$probe_status_file"
                if [ "$probe_name" = "readiness" ]; then
                    exit 1
                fi
            fi
        fi

        sleep "$period"
    done
    return 0
}}

runStartupProbe() {{
    local probe_type="$1"
    local container_name="$2"
    local initial_delay="$3"
    local period="$4"
    local timeout="$5"
    local success_threshold="$6"
    local failure_threshold="$7"
    local probe_name="$8"
    local probe_index="$9"
    shift 9
    local probe_args=("$@")

    local probe_status_file="${{workingPath}}/${{probe_name}}-probe-${{container_name}}-${{probe_index}}.status"

    printf "%s\\n" "$(date -Is --utc) Starting ${{probe_name}} probe for container ${{container_name}}..."
    echo "RUNNING" > "$probe_status_file"

    if [ "$initial_delay" -gt 0 ]; then
        printf "%s\\n" "$(date -Is --utc) Waiting ${{initial_delay}}s before starting ${{probe_name}} probe..."
        sleep "$initial_delay"
    fi

    local consecutive_successes=0
    local consecutive_failures=0

    while true; do
        if [ "$probe_type" = "http" ]; then
            executeHTTPProbe "${{probe_args[@]}}"
        elif [ "$probe_type" = "exec" ]; then
            executeExecProbe "$timeout" "${{probe_args[@]}}"
        fi

        local exit_code=$?

        if [ $exit_code -eq 0 ]; then
            consecutive_successes=$((consecutive_successes + 1))
            consecutive_failures=0
            printf "%s\\n" "$(date -Is --utc) ${{probe_name}} probe succeeded for ${{container_name}} (${{consecutive_successes}}/${{success_threshold}})"

            if [ $consecutive_successes -ge $success_threshold ]; then
                printf "%s\\n" "$(date -Is --utc) ${{probe_name}} probe successful for ${{container_name}} – other probes can now start"
                echo "SUCCESS" > "$probe_status_file"
                return 0
            fi
        else
            consecutive_failures=$((consecutive_failures + 1))
            consecutive_successes=0
            printf "%s\\n" "$(date -Is --utc) ${{probe_name}} probe failed for ${{container_name}} (${{consecutive_failures}}/${{failure_threshold}})"

            if [ $consecutive_failures -ge $failure_threshold ]; then
                printf "%s\\n" "$(date -Is --utc) ${{probe_name}} probe failed for ${{container_name}} after ${{failure_threshold}} attempts – container should restart" >&2
                echo "FAILED_THRESHOLD" > "$probe_status_file"
                exit 1
            fi
        fi

        sleep "$period"
    done
}}

waitForProbes() {{
    local probe_name="$1"
    local container_name="$2"
    local probe_count="$3"

    if [ "$probe_count" -eq 0 ]; then
        return 0
    fi

    printf "%s\\n" "$(date -Is --utc) Waiting for ${{probe_name}} probes to succeed before starting other probes for ${{container_name}}..."

    while true; do
        local all_probes_successful=true

        for i in $(seq 0 $((probe_count - 1))); do
            local probe_status_file="${{workingPath}}/${{probe_name}}-probe-${{container_name}}-${{i}}.status"
            if [ ! -f "$probe_status_file" ]; then
                all_probes_successful=false
                break
            fi

            local status
            status=$(cat "$probe_status_file")
            if [ "$status" != "SUCCESS" ]; then
                if [ "$status" = "FAILED_THRESHOLD" ]; then
                    printf "%s\\n" "$(date -Is --utc) ${{probe_name}} probe failed for ${{container_name}} – exiting" >&2
                    return 1
                fi
                all_probes_successful=false
                break
            fi
        done

        if [ "$all_probes_successful" = true ]; then
            printf "%s\\n" "$(date -Is --utc) All ${{probe_name}} probes successful for ${{container_name}} – other probes can now start"
            return 0
        fi

        sleep 1
    done
}}
""")

    # ------------------------------------------------------------------ #
    # Probe invocation block (runs in a sub-shell in the background)       #
    # ------------------------------------------------------------------ #
    cvar = container_name.replace("-", "_")

    # Startup probe invocations
    for i, probe in enumerate(startup_probes):
        args = _build_probe_args(probe)
        lines.append(
            f'runStartupProbe "{probe.type}" "{container_name}" '
            f"{probe.initial_delay_seconds} {probe.period_seconds} "
            f"{probe.timeout_seconds} {probe.success_threshold} "
            f'{probe.failure_threshold} "startup" {i} {args} &\n'
            f"STARTUP_PROBE_{cvar}_{i}_PID=$!\n"
        )

    # Orchestration sub-shell
    if startup_probes:
        lines.append(
            f"(\n"
            f'    waitForProbes "startup" "{container_name}" {len(startup_probes)}\n'
            f"    if [ $? -eq 0 ]; then\n"
        )
    else:
        lines.append(
            "(\n"
            'echo "No startup probes defined, starting readiness/liveness probes directly."\n'
            "if true; then\n"
        )

    if readiness_probes:
        for i, probe in enumerate(readiness_probes):
            args = _build_probe_args(probe)
            lines.append(
                f'        runProbe "{probe.type}" "{container_name}" '
                f"{probe.initial_delay_seconds} {probe.period_seconds} "
                f"{probe.timeout_seconds} {probe.success_threshold} "
                f'{probe.failure_threshold} "readiness" {i} {args} &\n'
                f"        READINESS_PROBE_{cvar}_{i}_PID=$!\n"
            )
        lines.append(
            f'        waitForProbes "readiness" "{container_name}" {len(readiness_probes)}\n'
            "        if [ $? -eq 0 ]; then\n"
        )
    else:
        lines.append(
            '            echo "No readiness probes defined, starting liveness probes directly."\n'
            "            if true; then\n"
        )

    if liveness_probes:
        for i, probe in enumerate(liveness_probes):
            args = _build_probe_args(probe)
            lines.append(
                f'            runProbe "{probe.type}" "{container_name}" '
                f"{probe.initial_delay_seconds} {probe.period_seconds} "
                f"{probe.timeout_seconds} {probe.success_threshold} "
                f'{probe.failure_threshold} "liveness" {i} {args} &\n'
                f"            LIVENESS_PROBE_{cvar}_{i}_PID=$!\n"
            )
    else:
        lines.append(
            f'            printf "%s\\n" "$(date -Is --utc) No liveness probes defined for container {container_name}."\n'
        )

    lines.append(
        "        else\n"
        '            printf "%s\\n" "$(date -Is --utc) Readiness probes failed – not starting liveness probes" >&2\n'
        "            shutDownContainersOnProbeFail\n"
        "            exit 1\n"
        "        fi\n"
        "    else\n"
        '        printf "%s\\n" "$(date -Is --utc) Startup probes failed – not starting readiness probes" >&2\n'
        "        shutDownContainersOnProbeFail\n"
        "        exit 1\n"
        "    fi\n"
        ") &\n"
    )

    return "".join(lines)


def generate_probe_cleanup_script(
    container_name, readiness_probes, liveness_probes, startup_probes
):
    """
    Generate a bash cleanup function and EXIT trap that kills all probe processes.
    Returns an empty string if no probes are defined.
    """
    if not readiness_probes and not liveness_probes and not startup_probes:
        return ""

    cvar = container_name.replace("-", "_")
    lines = [
        "\n# ---- Probe cleanup ----\n",
        "cleanup_probes() {\n",
        '    printf "%s\\n" "$(date -Is --utc) Cleaning up probe processes..."\n',
    ]

    for i in range(len(startup_probes)):
        lines.append(
            f'    [ -n "${{STARTUP_PROBE_{cvar}_{i}_PID:-}}" ] && kill "$STARTUP_PROBE_{cvar}_{i}_PID" 2>/dev/null || true\n'
        )
    for i in range(len(readiness_probes)):
        lines.append(
            f'    [ -n "${{READINESS_PROBE_{cvar}_{i}_PID:-}}" ] && kill "$READINESS_PROBE_{cvar}_{i}_PID" 2>/dev/null || true\n'
        )
    for i in range(len(liveness_probes)):
        lines.append(
            f'    [ -n "${{LIVENESS_PROBE_{cvar}_{i}_PID:-}}" ] && kill "$LIVENESS_PROBE_{cvar}_{i}_PID" 2>/dev/null || true\n'
        )

    lines.append("}\ntrap cleanup_probes EXIT\n")
    return "".join(lines)
