import argparse
import base64
import json
import logging
import math
import os
import re
import shlex
import subprocess
from datetime import datetime

import yaml
from flask import Flask, jsonify, request
from probes import (
    generate_probe_cleanup_script,
    generate_probe_script,
    translate_kubernetes_probes,
)

parser = argparse.ArgumentParser()

parser.add_argument("--schedd-name", help="Schedd name", type=str, default="")
parser.add_argument("--schedd-host", help="Schedd host", type=str, default="")
parser.add_argument("--collector-host", help="Collector-host", type=str, default="")
parser.add_argument("--cadir", help="CA directory", type=str, default="")
parser.add_argument("--certfile", help="cert file", type=str, default="")
parser.add_argument("--keyfile", help="key file", type=str, default="")
parser.add_argument(
    "--auth-method", help="Default authentication methods", type=str, default=""
)
parser.add_argument("--debug", help="Debug level", type=str, default="")
parser.add_argument(
    "--condor-config", help="Path to condor_config file", type=str, default=""
)
parser.add_argument("--proxy", help="Path to proxy file", type=str, default="")
parser.add_argument(
    "--dummy-job",
    action="store_true",
    help="Whether the job should be a real job or a dummy sleep job",
)
parser.add_argument("--port", help="Server port", type=int, default=8000)

args = parser.parse_args()

if args.schedd_name != "":
    os.environ["_condor_SCHEDD_NAME"] = args.schedd_name
if args.schedd_host != "":
    os.environ["_condor_SCHEDD_HOST"] = args.schedd_host
if args.collector_host != "":
    os.environ["_condor_COLLECTOR_HOST"] = args.collector_host
if args.cadir != "":
    os.environ["_condor_AUTH_SSL_CLIENT_CADIR"] = args.cadir
if args.certfile != "":
    os.environ["_condor_AUTH_SSL_CLIENT_CERTFILE"] = args.certfile
if args.keyfile != "":
    os.environ["_condor_AUTH_SSL_CLIENT_KEYFILE"] = args.keyfile
if args.auth_method != "":
    os.environ["_condor_SEC_DEFAULT_AUTHENTICATION_METHODS"] = args.auth_method
if args.debug != "":
    os.environ["_condor_TOOL_DEBUG"] = args.debug
if args.condor_config != "":
    os.environ["CONDOR_CONFIG"] = args.condor_config
if args.proxy != "":
    os.environ["X509_USER_PROXY"] = args.proxy
if args.proxy != "":
    os.environ["X509_USER_CERT"] = args.proxy
dummy_job = args.dummy_job


global JID
JID = []

# Maximum bytes to retrieve per condor_tail call (10 MiB).
_CONDOR_TAIL_MAX_BYTES = 10 * 1024 * 1024


def read_yaml_file(file_path):
    with open(file_path, "r") as file:
        try:
            data = yaml.safe_load(file)
            return data
        except yaml.YAMLError as e:
            print("Error reading YAML file:", e)
            return None


global InterLinkConfigInst
interlink_config_path = "./SidecarConfig.yaml"
InterLinkConfigInst = read_yaml_file(interlink_config_path)
print("Interlink configuration info:", InterLinkConfigInst)


def error_response(message, status_code=500):
    """Create standardized error response"""
    return (
        jsonify({"error": message, "timestamp": datetime.utcnow().isoformat() + "Z"}),
        status_code,
    )


def success_response(data, status_code=200):
    """Create standardized success response"""
    return jsonify(data), status_code


def validate_pod_request(request_data):
    """Validate incoming pod request structure"""
    if not request_data:
        return False, "Empty request data"
    if not isinstance(request_data, dict):
        return False, "Request data must be a dictionary"
    if "metadata" not in request_data:
        return False, "Missing metadata in request"
    if "name" not in request_data.get("metadata", {}):
        return False, "Missing pod name in metadata"
    return True, "Valid request"


def prepare_envs(container):
    env = ""
    try:
        for env_var in container["env"]:
            if env_var.get("value") is not None:
                if env_var.get("value").startswith("["):
                    modified_value = '"' + env_var.get("value").replace('"', '"') + '"'
                    env += f"--env {env_var['name']}={modified_value} "
                else:
                    env += f"--env {env_var['name']}={env_var['value']} "
            else:
                env += f"--env {env_var['name']}= "
        return [env]
    except Exception as e:
        logging.info(f"There is some problem with your env variables: {e}")
        return [""]


def _shell_single_quote(val):
    """Format *val* as a POSIX shell single-quoted string."""
    return "'" + str(val).replace("'", "'\"'\"'") + "'"


def _wrap_command_with_env(command_tokens, env_file_name):
    """Source the generated env file inside the container, then exec the command."""
    if not env_file_name:
        return command_tokens
    return [
        "/bin/sh",
        "-c",
        f'. ./{env_file_name} && exec "$@"',
        "sh",
    ] + command_tokens


def prepare_env_file(container, metadata, container_standalone=None):
    """Write a sourceable env script for the given container and return its path.

    The file contains ``export`` statements and is sourced inside the container
    command wrapper instead of being passed via ``--env-file``. This avoids
    Apptainer re-parsing values like backticks or backslash-escaped quotes.

    ``envFrom`` entries (secretRef / configMapRef) are expanded from the
    ``container_standalone`` data supplied by the interLink sidecar so that all
    keys from the referenced Secrets / ConfigMaps are also injected.
    """
    env_file_name = f"{metadata['name']}-{metadata['uid']}_env.env"
    job_dir = os.path.join(
        os.path.realpath(InterLinkConfigInst["DataRootFolder"]),
        f"{metadata['name']}-{metadata['uid']}",
    )
    os.makedirs(job_dir, exist_ok=True)
    os.chmod(job_dir, 0o1777)
    env_file_path = os.path.join(job_dir, env_file_name)
    lines = []

    try:
        # --- individual env vars (already resolved by interLink) -------------
        for env_var in container.get("env", []):
            name = env_var["name"]
            raw_val = env_var.get("value") or ""
            lines.append(f"export {name}={_shell_single_quote(raw_val)}")

        # --- envFrom (bulk import from Secret or ConfigMap) ------------------
        if container_standalone is not None:
            secrets_list = container_standalone.get("secrets", [])
            configmaps_list = container_standalone.get("configMaps", [])
            for env_from in container.get("envFrom", []):
                if "secretRef" in env_from:
                    ref_name = env_from["secretRef"].get("name", "")
                    for secret in secrets_list:
                        if secret.get("metadata", {}).get("name") == ref_name:
                            for k, v in secret.get("data", {}).items():
                                lines.append(
                                    f"export {k}={_shell_single_quote(v or '')}"
                                )
                elif "configMapRef" in env_from:
                    ref_name = env_from["configMapRef"].get("name", "")
                    for cm in configmaps_list:
                        if cm.get("metadata", {}).get("name") == ref_name:
                            for k, v in cm.get("data", {}).items():
                                lines.append(
                                    f"export {k}={_shell_single_quote(v or '')}"
                                )

        # Env vars may include secret values resolved by the interLink sidecar.
        # Write them to a sourceable file and transfer it into the execute
        # sandbox; the generated container wrapper sources it inside the
        # container before exec'ing the real command.
        with open(env_file_path, "w") as fp:
            fp.write("\n".join(lines) + "\n")
        os.chmod(env_file_path, 0o644)
        logging.info(f"Wrote env file to {env_file_path}")

        return (env_file_name, env_file_path)

    except Exception as e:
        logging.error(f"Failed to write env file: {e}")
        return (None, None)


def prepare_mounts(pod, container_standalone):
    mounts = ["--bind"]
    mount_data = []
    pod_name = (
        container_standalone["name"].split("-")[:6]
        if len(container_standalone["name"].split("-")) > 6
        else container_standalone["name"].split("-")
    )
    pod_name_folder = os.path.join(
        os.path.realpath(InterLinkConfigInst["DataRootFolder"]), "-".join(pod_name[:-1])
    )
    all_containers = list(pod["spec"]["containers"]) + list(
        pod["spec"].get("initContainers", [])
    )
    for c in all_containers:
        if c["name"] == container_standalone["name"]:
            container = c
            try:
                os.makedirs(pod_name_folder, exist_ok=True)
                os.chmod(pod_name_folder, 0o1777)
                logging.info(f"Successfully created folder {pod_name_folder}")
            except Exception as e:
                logging.error(e)
            if "volumeMounts" in container.keys():
                for mount_var in container["volumeMounts"]:
                    path = ""
                    for vol in pod["spec"]["volumes"]:
                        if vol["name"] != mount_var["name"]:
                            continue
                        if "configMap" in vol.keys():
                            config_maps_paths = mountConfigMaps(
                                pod, container_standalone
                            )
                            # print("bind as configmap", mount_var["name"], vol["name"])
                            for i, path in enumerate(config_maps_paths):
                                mount_data.append(path)
                        elif "secret" in vol.keys():
                            secrets_paths = mountSecrets(pod, container_standalone)
                            # print("bind as secret", mount_var["name"], vol["name"])
                            for i, path in enumerate(secrets_paths):
                                mount_data.append(path)
                        elif "emptyDir" in vol.keys():
                            path = mount_empty_dir(
                                container,
                                pod,
                                vol["name"],
                                mount_var["mountPath"],
                                read_only=mount_var.get("readOnly", False),
                            )
                            mount_data.append(path)
                        elif "hostPath" in vol.keys():
                            host_path = vol["hostPath"]["path"]
                            mount_path = mount_var["mountPath"]
                            bind_path = f"{host_path}:{mount_path}"
                            mount_data.append(bind_path)
                        else:
                            # Implement logic for other volume types if required.
                            logging.info("\n*********\n*To be implemented*\n********")
            else:
                logging.info("Container has no volume mount")
                return [""]

            path_hardcoded = ""
            mount_data.append(path_hardcoded)
    mounts.append(",".join(mount_data))
    print("mounts are", mounts)
    if mounts[1] == "":
        mounts = [""]
    return mounts


def extract_container(pod, container_standalone):
    for c in pod["spec"]["containers"]:
        if c["name"] == container_standalone["name"]:
            return c
    raise ValueError(f"Container {container_standalone['name']} not found in pod")


def mountConfigMaps(pod, container_standalone):
    configMapNamePaths = []
    # for c in pod["spec"]["containers"]:
    #     if c["name"] == container_standalone["name"]:
    #       container = c
    container = extract_container(pod, container_standalone)
    if InterLinkConfigInst["ExportPodData"] and "volumeMounts" in container.keys():
        data_root_folder = InterLinkConfigInst["DataRootFolder"]
        # Clean and recreate per-job configMaps folder
        job_dir = os.path.join(
            os.getcwd(),
            data_root_folder,
            f"{pod['metadata']['name']}-{pod['metadata']['uid']}",
        )
        pod_configmaps_root = os.path.join(job_dir, "configMaps")
        cmd = ["-rf", pod_configmaps_root]
        shell = subprocess.Popen(
            [
                "rm",
            ]
            + cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _, err = shell.communicate()

        if err:
            logging.error("Unable to delete root folder")

        for mountSpec in container["volumeMounts"]:
            for vol in pod["spec"]["volumes"]:
                if vol["name"] != mountSpec["name"]:
                    continue
                if "configMap" in vol.keys():
                    print("container_standalone:", container_standalone)
                    cfgMaps = container_standalone["configMaps"]
                    for cfgMap in cfgMaps:
                        podConfigMapDir = os.path.join(
                            job_dir,
                            "configMaps",
                            vol["name"],
                        )
                        for key in cfgMap["data"].keys():
                            path = os.path.join(podConfigMapDir, key)
                            path += f":{mountSpec['mountPath']}/{key}"
                            configMapNamePaths.append(path)
                        cmd = ["-p", podConfigMapDir]
                        shell = subprocess.Popen(
                            ["mkdir"] + cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                        )
                        execReturn, _ = shell.communicate()
                        if execReturn:
                            logging.error(err)
                        else:
                            logging.debug(f"--- Created folder {podConfigMapDir}")
                        logging.debug("--- Writing ConfigMaps files")
                        for k, v in cfgMap["data"].items():
                            full_path = os.path.join(podConfigMapDir, k)
                            with open(full_path, "w") as f:
                                f.write(v)
                            os.chmod(full_path, vol["configMap"]["defaultMode"])
                            logging.debug(f"--- Written ConfigMap file {full_path}")
    return configMapNamePaths


def mountSecrets(pod, container_standalone):
    secret_name_paths = []
    # for c in pod["spec"]["containers"]:
    #     if c["name"] == container_standalone["name"]:
    #         container = c
    container = extract_container(pod, container_standalone)
    if InterLinkConfigInst["ExportPodData"] and "volumeMounts" in container.keys():
        data_root_folder = InterLinkConfigInst["DataRootFolder"]
        job_dir = os.path.join(
            os.getcwd(),
            data_root_folder,
            f"{pod['metadata']['name']}-{pod['metadata']['uid']}",
        )
        pod_secrets_root = os.path.join(job_dir, "secrets")
        cmd = ["-rf", pod_secrets_root]
        subprocess.run(["rm"] + cmd, check=True)
        for mountSpec in container["volumeMounts"]:
            for vol in pod["spec"]["volumes"]:
                if vol["name"] != mountSpec["name"]:
                    continue
                if "secret" in vol.keys():
                    secrets = container_standalone["secrets"]
                    for secret in secrets:
                        if secret["metadata"]["name"] != vol["secret"]["secretName"]:
                            continue
                        pod_secret_dir = os.path.join(
                            job_dir,
                            "secrets",
                            vol["name"],
                        )
                        for key in secret["data"]:
                            path = os.path.join(pod_secret_dir, key)
                            path += f":{mountSpec['mountPath']}/{key}"
                            secret_name_paths.append(path)
                        cmd = ["-p", pod_secret_dir]
                        subprocess.run(["mkdir"] + cmd, check=True)
                        logging.debug(f"--- Created folder {pod_secret_dir}")
                        logging.debug("--- Writing Secret files")
                        for k, v in secret["data"].items():
                            full_path = os.path.join(pod_secret_dir, k)
                            with open(full_path, "wb") as f:
                                f.write(base64.b64decode(v))
                            os.chmod(full_path, vol["secret"]["defaultMode"])
                            logging.debug(f"--- Written Secret file {full_path}")
    return secret_name_paths


def mount_empty_dir(container, pod, vol_name, mount_path, read_only=False):
    ed_path = None
    if InterLinkConfigInst["ExportPodData"] and "volumeMounts" in container.keys():
        job_dir = os.path.join(
            os.getcwd(),
            InterLinkConfigInst["DataRootFolder"],
            f"{pod['metadata']['name']}-{pod['metadata']['uid']}",
        )
        empty_dirs_root = os.path.join(job_dir, "emptyDirs")
        os.makedirs(empty_dirs_root, exist_ok=True)
        os.chmod(empty_dirs_root, 0o1777)
        ed_path = os.path.join(empty_dirs_root, vol_name)
        os.makedirs(ed_path, exist_ok=True)
        os.chmod(ed_path, 0o1777)
        ed_path += ":" + mount_path
        if read_only:
            ed_path += ":ro"

    return ed_path


def parse_cpu(value_str):
    """Parse a Kubernetes CPU value and return an integer number of CPUs (>=1).

    Kubernetes CPU can be expressed as:
      - Plain integer or float: "1", "2", "0.5"
      - Millicores: "100m", "500m", "1000m"

    HTCondor RequestCpus requires a whole number, so we round up with a
    minimum of 1.
    """
    value_str = str(value_str).strip()
    if value_str.endswith("m"):
        millicores = float(value_str[:-1])
        cpus = millicores / 1000.0
    else:
        cpus = float(value_str)
    return max(1, int(math.ceil(cpus)))


def parse_string_with_suffix(value_str):
    # should return MB because HTCondor wants MB
    suffixes = {
        "k": 1 / 10**3,
        "M": 1,
        "G": 10**3,
        "Ki": 1 / 1024,
        "Mi": 1,
        "Gi": 1024,
    }

    match = re.match(r"(\d+)([a-zA-Z]+)", value_str)
    if match:
        numeric_part = match.group(1)
        suffix = match.group(2)
        if suffix in suffixes:
            numeric_value = int(float(numeric_part) * suffixes[suffix])
            return numeric_value
        else:
            return 1
    else:
        print("Unrecognized memory value, setting it to 1 MB")
        return 1


# Maximum number of seconds to allow a preStop or postStart lifecycle hook to run.
_LIFECYCLE_HOOK_TIMEOUT_SECONDS = 30

# Regex to detect an existing --bind spec whose destination is /tmp.
_RE_TMP_BIND = re.compile(r"([^,:\s]+):/tmp(?::|,|\s|$)")


def _find_tmp_bind_in_tokens(cmd_tokens):
    """Scan a list of singularity command tokens for a --bind spec with /tmp.

    Returns the host-side path if found, or None if /tmp is not already bound.
    This is used to decide whether the lifecycle hook needs to inject its own
    ``hook-tmp`` directory.
    """
    for tok in cmd_tokens:
        m = _RE_TMP_BIND.search(tok)
        if m:
            return m.group(1)
    return None


def _find_image_in_tokens(cmd_tokens):
    """Return the first token that looks like a container image, or empty string."""
    for tok in cmd_tokens:
        if tok.startswith("docker://") or tok.startswith("/cvmfs"):
            return tok
    return ""


def _inject_hook_tmp_into_cmd(cmd_tokens):
    """Insert ``--bind "${workingPath}/hook-tmp:/tmp"`` before the image token.

    The bind value uses a shell variable (``${workingPath}``) so that it
    expands at script runtime rather than at script-generation time.

    Returns a new list; does NOT modify the original.
    """
    bind_val = '"${workingPath}/hook-tmp:/tmp"'
    for i, tok in enumerate(cmd_tokens):
        if tok.startswith("docker://") or tok.startswith("/cvmfs"):
            new = list(cmd_tokens)
            new.insert(i, bind_val)
            new.insert(i, "--bind")
            return new
    # Image token not found — return unchanged and let the hook run without
    # an explicit /tmp injection.
    logging.warning(
        "_inject_hook_tmp_into_cmd: image token not found in command tokens; "
        "skipping /tmp injection for lifecycle hook"
    )
    return list(cmd_tokens)


def _generate_poststart_fragment(
    ctn_name,
    hook_spec,
    hook_tmp_bind,
    singularity_path,
    singularity_options,
    cmd_tokens,
):
    """Generate a bash fragment that runs a postStart lifecycle hook.

    The fragment runs *synchronously* before the ``runCtn`` call so that the
    hook's side-effects (e.g. creating ``/tmp/poststart-marker``) are visible
    to the container immediately when it starts.

    Parameters
    ----------
    ctn_name:
        Container name (used for comments and output file naming).
    hook_spec:
        Parsed hook dict produced by ``_translate_lifecycle_hook``.
    hook_tmp_bind:
        The ``--bind`` spec string for /tmp sharing (e.g.
        ``'"${workingPath}/hook-tmp:/tmp"'``).  May be empty if the caller
        already injected the bind into ``cmd_tokens``.
    singularity_path:
        Path to the singularity binary.
    singularity_options:
        Extra singularity flags from the pod annotation.
    cmd_tokens:
        The (potentially modified) container command tokens; used to extract
        the container image.

    Returns
    -------
    str
        Bash script fragment (no trailing newline).
    """
    image = _find_image_in_tokens(cmd_tokens)
    out_file = f'"${{_IL_POD_NAME}}-${{_IL_POD_UID}}-{ctn_name}.out"'

    lines = [f"# postStart hook for container {ctn_name}\n"]
    lines.append(
        f'printf "%s\\n" "$(date -Is --utc) Running postStart hook for container {ctn_name}..." >> {out_file} 2>&1\n'  # noqa: E501
    )

    if hook_spec["type"] == "exec":
        quoted_args = [shlex.quote(a) for a in hook_spec["command"]]
        if image and singularity_path:
            parts = [shlex.quote(singularity_path), "exec"]
            if singularity_options:
                parts.extend(shlex.quote(o) for o in singularity_options.split())
            if hook_tmp_bind:
                parts.extend(["--bind", hook_tmp_bind])
            parts.append(shlex.quote(image))
            parts.extend(["timeout", str(_LIFECYCLE_HOOK_TIMEOUT_SECONDS)])
            parts.extend(quoted_args)
            lines.append(f'{" ".join(parts)} >> {out_file} 2>&1 || true\n')
        else:
            lines.append(
                f'timeout {_LIFECYCLE_HOOK_TIMEOUT_SECONDS} {" ".join(quoted_args)} >> {out_file} 2>&1 || true\n'  # noqa: E501
            )

    elif hook_spec["type"] == "httpget":
        url = (
            f'{hook_spec["scheme"]}://{hook_spec["host"]}'
            f':{hook_spec["port"]}{hook_spec["path"]}'
        )
        lines.append(
            f"curl -f -s --max-time 10 {shlex.quote(url)} >> {out_file} 2>&1 || true\n"
        )

    lines.append(
        f'printf "%s\\n" "$(date -Is --utc) postStart hook for container {ctn_name} completed." >> {out_file} 2>&1\n'  # noqa: E501
    )
    return "".join(lines)


def _translate_lifecycle_hook(handler):
    """Translate a Kubernetes lifecycle handler dict to an internal spec.

    Supports exec and httpGet handler types.  Returns None if the handler is
    None, empty, or uses an unsupported type (e.g. tcpSocket).

    For httpGet, named ports (non-numeric string) cannot be resolved outside
    the container runtime and are skipped with a warning.

    Returns a dict with keys:
      - ``type``: ``"exec"`` or ``"httpget"``
      - ``command``: list of strings (exec only)
      - ``scheme``, ``host``, ``port``, ``path``: strings/int (httpGet only)
    """
    if not handler:
        return None

    if handler.get("exec"):
        cmd = handler["exec"].get("command") or []
        if cmd:
            return {"type": "exec", "command": cmd}
        return None

    if handler.get("httpGet"):
        http = handler["httpGet"]
        port = http.get("port", 80)
        if isinstance(port, str) and not port.isdigit():
            logging.warning(
                "preStop httpGet hook uses a named port (%r) which cannot be "
                "resolved in this context; hook will be skipped",
                port,
            )
            return None
        return {
            "type": "httpget",
            "scheme": (http.get("scheme") or "HTTP").lower(),
            "host": http.get("host") or "localhost",
            "port": int(port),
            "path": http.get("path") or "/",
        }

    logging.warning(
        "Unsupported lifecycle hook type in handler %r; hook will be skipped", handler
    )
    return None


def generate_prestop_trap(containers, metadata):
    """Generate a bash SIGTERM trap that runs preStop lifecycle hooks.

    When HTCondor terminates a job (condor_rm), it sends SIGTERM to the job
    script process.  This trap intercepts SIGTERM, runs each container's
    preStop hook (exec via ``singularity exec`` or httpGet via ``curl``), then
    forwards SIGTERM to the running container processes (tracked in
    ``pidCtns``) before waiting for them to exit.

    Only containers in *containers* that have a ``lifecycle.preStop`` spec are
    processed; init containers are not included.

    Parameters
    ----------
    containers:
        List of main container dicts from pod["spec"]["containers"].
    metadata:
        Pod metadata dict (used for annotations such as singularity-options).

    Returns
    -------
    str
        Bash script fragment that defines ``preStopTrap()`` and registers it
        as the SIGTERM trap.  Empty string if no container has a preStop hook.
    """
    singularity_path = InterLinkConfigInst.get("SingularityPath", "singularity")
    annotations = metadata.get("annotations", {})
    singularity_options = annotations.get("slurm-job.vk.io/singularity-options", "")

    entries = []
    for container in containers:
        lifecycle = container.get("lifecycle") or {}
        prestop = lifecycle.get("preStop")
        if not prestop:
            continue
        hook = _translate_lifecycle_hook(prestop)
        if hook is None:
            continue
        image = container.get("image", "")
        if not (image.startswith("/cvmfs") or image.startswith("docker://")):
            image = "docker://" + image
        entries.append({"name": container["name"], "hook": hook, "image": image})

    if not entries:
        return ""

    lines = [
        "\n# PreStop lifecycle hooks — executed when the job receives SIGTERM\n",
        "preStopTrap() {\n",
        '  printf "%s\\n" "$(date -Is --utc) Received SIGTERM: running preStop lifecycle hooks..."\n',  # noqa: E501
    ]

    for entry in entries:
        name = entry["name"]
        hook = entry["hook"]
        image = entry["image"]
        out_file = f'"${{workingPath}}/prestop-{name}.out"'

        lines.append(
            f'  printf "%s\\n" "$(date -Is --utc) Running preStop hook for container {name}..."\n'  # noqa: E501
        )

        if hook["type"] == "exec":
            quoted_args = [shlex.quote(a) for a in hook["command"]]
            if image and singularity_path:
                # Run the hook inside the container via singularity exec.
                # `timeout` is placed after the image name so it executes inside
                # the container (consistent with the SLURM plugin reference
                # implementation), limiting how long the hook command may run.
                parts = [shlex.quote(singularity_path), "exec"]
                if singularity_options:
                    parts.extend(shlex.quote(o) for o in singularity_options.split())
                parts.append(shlex.quote(image))
                parts.extend(["timeout", str(_LIFECYCLE_HOOK_TIMEOUT_SECONDS)])
                parts.extend(quoted_args)
                lines.append(f'  {" ".join(parts)} >> {out_file} 2>&1 || true\n')
            else:
                lines.append(
                    f'  timeout {_LIFECYCLE_HOOK_TIMEOUT_SECONDS} {" ".join(quoted_args)} >> {out_file} 2>&1 || true\n'  # noqa: E501
                )

        elif hook["type"] == "httpget":
            url = f'{hook["scheme"]}://{hook["host"]}:{hook["port"]}{hook["path"]}'
            lines.append(
                f"  curl -f -s --max-time 10 {shlex.quote(url)} >> {out_file} 2>&1 || true\n"  # noqa: E501
            )

    lines += [
        '  printf "%s\\n" "$(date -Is --utc) preStop hooks completed, terminating containers..."\n',  # noqa: E501
        "  for pidCtn in ${pidCtns} ; do\n",
        '    pid="${pidCtn%:*}"\n',
        '    ctn="${pidCtn#*:}"\n',
        '    printf "%s\\n" "$(date -Is --utc) Sending SIGTERM to container ${ctn} pid ${pid}..."\n',  # noqa: E501
        '    kill "${pid}" 2>/dev/null || true\n',
        "  done\n",
        "  wait\n",
        '  printf "%s\\n" "$(date -Is --utc) All containers terminated."\n',
        "}\n",
        "trap preStopTrap SIGTERM\n",
    ]

    return "".join(lines)


def prepare_lifecycle_hooks(containers, metadata):
    """Build the preStop trap script for a pod's main containers.

    Follows the same prepare-* pattern as prepare_probes.  Called once per
    pod (for all main containers together) inside SubmitHandler; the returned
    script is passed to produce_htcondor_singularity_script.

    Parameters
    ----------
    containers:
        List of main container dicts (not init containers).
    metadata:
        Pod metadata dict.

    Returns
    -------
    str
        Bash script fragment for the SIGTERM trap, or empty string if no
        container defines a preStop hook.
    """
    prestop_trap = generate_prestop_trap(containers, metadata)
    if prestop_trap:
        logging.info(
            "Prepared preStop lifecycle hooks for %d container(s)",
            sum(1 for c in containers if (c.get("lifecycle") or {}).get("preStop")),
        )
    return prestop_trap


def prepare_probes(container, metadata):
    """Translate Kubernetes probe specs for a container into bash script snippets.

    Follows the same prepare-* pattern as prepare_env_file and prepare_mounts.
    Called once per container inside SubmitHandler; the returned scripts are
    collected and later passed to produce_htcondor_singularity_script.

    Returns:
        tuple[str, str]: (probe_script, cleanup_script). Both strings are
        empty when no probes are defined for the container.
    """
    annotations = metadata.get("annotations", {})
    singularity_options = annotations.get("slurm-job.vk.io/singularity-options", "")
    singularity_path = InterLinkConfigInst.get("SingularityPath", "singularity")

    readiness, liveness, startup = translate_kubernetes_probes(container)

    if not readiness and not liveness and not startup:
        return "", ""

    image = container.get("image", "")
    if not (image.startswith("/cvmfs") or image.startswith("docker://")):
        image = "docker://" + image
    opts = singularity_options.split() if singularity_options else []

    probe_script = generate_probe_script(
        container_name=container["name"],
        image_name=image,
        readiness_probes=readiness,
        liveness_probes=liveness,
        startup_probes=startup,
        singularity_path=singularity_path,
        singularity_options=opts,
    )
    cleanup_script = generate_probe_cleanup_script(
        container_name=container["name"],
        readiness_probes=readiness,
        liveness_probes=liveness,
        startup_probes=startup,
    )

    logging.info(
        f"Prepared probes for container {container['name']}: "
        f"readiness={len(readiness)}, liveness={len(liveness)}, startup={len(startup)}"
    )
    return probe_script, cleanup_script


def _is_main_command_line(stripped):
    """Return True if *stripped* is a non-preamble, non-probe line.

    Used to find the insertion point for the probe sub-shell block: we skip
    the shebang, blank lines, comment lines, export statements and probe
    cleanup trap/function lines so that the probe background processes are
    launched just before the actual singularity exec command.
    """
    if not stripped:
        return False
    if stripped.startswith("#"):
        return False
    if stripped.startswith("export "):
        return False
    if "cleanup_probes" in stripped:
        return False
    if stripped.startswith("trap "):
        return False
    return True


# Bash helper functions injected into every multi-container job script.
# These implement the SLURM-plugin runCtn/waitCtns/endScript pattern so that
# each Singularity container runs in the background and all exit codes are
# collected before the job terminates.
# _IL_POD_NAME and _IL_POD_UID are injected into each generated script.
# Per-container output files are written to the HTCondor execute sandbox as
# relative paths (e.g. "${_IL_POD_NAME}-${_IL_POD_UID}-${ctn}.out") and
# retrieved by LogsHandler via condor_tail (no shared filesystem required).
_RUN_CTN_HELPERS = r"""
runCtn() {
  local ctn="$1"
  shift
  ( "$@" ) > "${_IL_POD_NAME}-${_IL_POD_UID}-${ctn}.out" 2>&1 &
  local pid="$!"
  printf '%s\n' "$(date -Is --utc) Running ${ctn} in background (pid ${pid})..."
  pidCtns="${pidCtns} ${pid}:${ctn}"
}

waitCtns() {
  for pidCtn in ${pidCtns}; do
    local pid="${pidCtn%:*}"
    local ctn="${pidCtn#*:}"
    printf '%s\n' "$(date -Is --utc) Waiting for ${ctn} (pid ${pid})..."
    wait "${pid}"
    local exitCode="$?"
    printf '%s\n' "${exitCode}" > "${workingPath}/run-${ctn}.status"
    printf '%s\n' "$(date -Is --utc) ${ctn} ended with status ${exitCode}."
  done
  for filestatus in "${workingPath}"/*.status; do
    [ -f "$filestatus" ] || continue
    local exitCode
    exitCode=$(cat "$filestatus")
    [ "${highestExitCode}" -lt "${exitCode}" ] && highestExitCode="${exitCode}"
  done
}

endScript() {
  printf '%s\n' "$(date -Is --utc) End of script, exit: ${highestExitCode}."
  exit "${highestExitCode}"
}
"""


def _extract_sandbox_bind_dirs(all_commands, input_files=None):
    """Return the set of relative (./...) bind-source dirs referenced in any
    command token list.  These are emptyDir directories that must be pre-created
    inside the HTCondor execute sandbox — HTCondor does not transfer empty
    directories, so without an explicit mkdir they will be absent and the bind
    mount will silently fail, leaving the container path read-only.

    ConfigMap and Secret sources are FILES that HTCondor transfers via
    transfer_input_files — they must NOT be pre-created as directories.
    We exclude any ./name whose basename matches a file in input_files."""
    transferred_basenames = set()
    if input_files:
        for f in input_files:
            transferred_basenames.add(os.path.basename(f))
    dirs = set()
    for _, tokens in all_commands:
        for i, tok in enumerate(tokens):
            if tok == "--bind" and i + 1 < len(tokens):
                for spec in tokens[i + 1].split(","):
                    if spec and ":" in spec:
                        src = spec.split(":")[0]
                        if src.startswith("./"):
                            basename = src[2:]  # strip "./"
                            if basename not in transferred_basenames:
                                dirs.add(src)
    return sorted(dirs)


def _clean_command_tokens(tokens):
    """Join and clean a list of singularity command tokens into a single string.

    Wraps the token that follows a ``-c`` flag with ``shlex.quote`` (so the
    shell does not re-split multi-line scripts, and single-quotes within the
    script content are safely escaped), then strips only standalone empty tokens.

    Note: we intentionally do NOT collapse multiple spaces here, because the
    quoted -c argument may contain Python code with meaningful indentation
    (multiple spaces).  Extra spaces from empty tokens such as pre_exec="" or
    singularity_options="" are harmless in a bash command line.
    """
    result = [token for token in tokens if token not in ("", '""')]
    for i in range(1, len(result)):
        if result[i - 1] == "-c":
            result[i] = shlex.quote(result[i])
    line = " ".join(result)
    return line.strip()


def produce_htcondor_singularity_script(
    containers,
    metadata,
    container_commands,
    input_files,
    probe_scripts=None,
    cleanup_scripts=None,
    init_container_commands=None,
    prestop_trap=None,
    poststart_hooks=None,
):
    """Write the HTCondor job executable and submit description file.

    Each container is launched in the background via a ``runCtn()`` bash
    helper, mirroring the SLURM plugin's pattern.  ``waitCtns()`` collects
    all exit codes, and ``endScript()`` exits with the highest one.

    Init containers (``init_container_commands``) are run sequentially and to
    completion *before* the main containers start, matching Kubernetes
    semantics.  If any init container exits non-zero the job aborts.

    Parameters
    ----------
    containers:
        List of container dicts from pod["spec"]["containers"].
    metadata:
        Pod metadata dict.
    container_commands:
        List of ``(container_name, [cmd_tokens])`` tuples, one per container,
        in the order they should be launched.  Each entry is produced by the
        SubmitHandler container loop.
    input_files:
        Files that HTCondor must transfer to the execute node (deduplicated
        across all containers by the caller).
    probe_scripts:
        Probe sub-shell snippets produced by prepare_probes(), one per
        container that defines probes.  Pass None (default) for no probes.
    cleanup_scripts:
        Cleanup trap snippets produced by prepare_probes(), matching
        probe_scripts.  Pass None (default) for no probes.
    init_container_commands:
        List of ``(container_name, [cmd_tokens])`` tuples for init containers.
        These run sequentially before the main containers.  Pass None (default)
        for no init containers.
    prestop_trap:
        Bash script fragment produced by prepare_lifecycle_hooks() that defines
        ``preStopTrap()`` and registers it as the SIGTERM trap.  Pass None
        (default) when no container has a preStop hook.
    poststart_hooks:
        Dict mapping container name to a parsed hook spec dict (the result of
        ``_translate_lifecycle_hook``), or ``None`` if the container has no
        postStart hook.  Pass None (default) when no container has a postStart
        hook.
    """
    if probe_scripts is None:
        probe_scripts = []
    if cleanup_scripts is None:
        cleanup_scripts = []
    if init_container_commands is None:
        init_container_commands = []
    if prestop_trap is None:
        prestop_trap = ""
    if poststart_hooks is None:
        poststart_hooks = {}

    datarootfolder = InterLinkConfigInst["DataRootFolder"]
    name = metadata["name"]
    uid = metadata["uid"]
    abs_dataroot = os.path.realpath(datarootfolder)
    # Create a unique job directory for all files related to this pod/job
    job_dir = os.path.join(abs_dataroot, f"{name}-{uid}")
    os.makedirs(job_dir, exist_ok=True)
    os.chmod(job_dir, 0o1777)
    executable_path = os.path.join(job_dir, f"{name}-{uid}.sh")
    sub_path = os.path.join(job_dir, f"{name}-{uid}.jdl")

    requested_cpus = 0
    requested_memory = 0
    for c in containers:
        if "resources" in c.keys():
            if "requests" in c["resources"].keys():
                if "cpu" in c["resources"]["requests"].keys():
                    requested_cpus += parse_cpu(c["resources"]["requests"]["cpu"])
                if "memory" in c["resources"]["requests"].keys():
                    requested_memory += parse_string_with_suffix(
                        c["resources"]["requests"]["memory"]
                    )
    if requested_cpus == 0:
        requested_cpus = 1
    if requested_memory == 0:
        requested_memory = 1

    annotations = metadata.get("annotations", {})
    prefix_ = ""

    # Export POD_IP from annotation
    pod_ip = annotations.get("interlink.eu/pod-ip", "")
    if pod_ip:
        prefix_ += f"\nexport POD_IP={pod_ip}\n"

    # CommandPrefix from config
    command_prefix = InterLinkConfigInst.get("CommandPrefix", "")
    if command_prefix:
        prefix_ += f"\n{command_prefix}"

    # Wstunnel client commands from annotation
    wstunnel_commands = annotations.get("interlink.eu/wstunnel-client-commands", "")
    if wstunnel_commands:
        prefix_ += f"\n{wstunnel_commands}\n"

    # Filter out empty probe/cleanup strings (containers with no probes return "")
    probe_scripts = [s for s in probe_scripts if s]
    cleanup_scripts = [s for s in cleanup_scripts if s]

    try:
        with open(executable_path, "w") as f:
            # ---- shebang + pod-specific variables -----------------------
            # _IL_POD_NAME / _IL_POD_UID are used by runCtn() to name the
            # per-container output files in the HTCondor execute sandbox.
            script_body = "#!/bin/bash\n"
            script_body += f"export _IL_POD_NAME={shlex.quote(name)}\n"
            script_body += f"export _IL_POD_UID={shlex.quote(uid)}\n"

            # ---- probe cleanup traps (must be defined before any trap) --
            for cs in cleanup_scripts:
                script_body += "\n" + cs + "\n"

            # ---- runCtn / waitCtns / endScript helpers ------------------
            script_body += _RUN_CTN_HELPERS

            # ---- preStop lifecycle hook trap (SIGTERM) ------------------
            if prestop_trap:
                script_body += "\n" + prestop_trap + "\n"

            # ---- preamble (exports, wstunnel, command prefix, etc.) -----
            if prefix_.strip():
                script_body += "\n" + prefix_.strip() + "\n"

            # ---- probe background sub-shells ----------------------------
            for ps in probe_scripts:
                script_body += "\n" + ps + "\n"

            # ---- pre-create emptyDir sandbox dirs (HTCondor skips empty dirs) -
            all_cmds = list(init_container_commands or []) + list(container_commands)
            sandbox_dirs = _extract_sandbox_bind_dirs(all_cmds, input_files)
            if sandbox_dirs:
                script_body += "\n# Pre-create emptyDir bind-source dirs in sandbox\n"
                for d in sandbox_dirs:
                    script_body += (
                        f"mkdir -p {shlex.quote(d)} && chmod 1777 {shlex.quote(d)}\n"
                    )

            # ---- init containers: run sequentially to completion ---------
            if init_container_commands:
                script_body += (
                    "\n# Init containers (run sequentially before main containers)\n"
                )
                for ctn_name, cmd_tokens in init_container_commands:
                    cleaned = _clean_command_tokens(cmd_tokens)
                    out_file = f'"${{_IL_POD_NAME}}-${{_IL_POD_UID}}-{ctn_name}.out"'
                    script_body += f"{cleaned} > {out_file} 2>&1\n"
                    fail_msg = f"Init container {ctn_name} failed with exit code"
                    script_body += (
                        "_init_rc=$?\n"
                        'if [ "$_init_rc" -ne 0 ]; then\n'
                        f'  printf "%s %s\\n" "{fail_msg}" "$_init_rc"\n'
                        '  exit "$_init_rc"\n'
                        "fi\n"
                    )
                script_body += "\n"

            # ---- main: run every container in background ----------------
            script_body += "\nhighestExitCode=0\n"
            script_body += 'pidCtns=""\n'
            script_body += "export workingPath=$(pwd)\n\n"

            # Singularity path/options needed for postStart hook generation.
            _sing_path = InterLinkConfigInst.get("SingularityPath", "singularity")
            _sing_opts = metadata.get("annotations", {}).get(
                "slurm-job.vk.io/singularity-options", ""
            )

            for ctn_name, cmd_tokens in container_commands:
                hook = poststart_hooks.get(ctn_name)
                if hook:
                    existing_tmp = _find_tmp_bind_in_tokens(cmd_tokens)
                    if existing_tmp:
                        # Reuse the user-supplied /tmp mount in the hook.
                        hook_tmp_bind = f'"{existing_tmp}:/tmp"'
                        final_tokens = cmd_tokens
                    else:
                        # No /tmp mount: create hook-tmp and inject the bind.
                        hook_tmp_bind = '"${workingPath}/hook-tmp:/tmp"'
                        script_body += 'mkdir -p "${workingPath}/hook-tmp"\n'
                        final_tokens = _inject_hook_tmp_into_cmd(cmd_tokens)

                    script_body += _generate_poststart_fragment(
                        ctn_name,
                        hook,
                        hook_tmp_bind,
                        _sing_path,
                        _sing_opts,
                        final_tokens,
                    )
                    cleaned = _clean_command_tokens(final_tokens)
                else:
                    cleaned = _clean_command_tokens(cmd_tokens)
                script_body += f"runCtn {ctn_name} {cleaned}\n"

            # ---- wait for all containers and exit -----------------------
            script_body += "\nwaitCtns\nendScript\n"

            f.write(script_body)
        logging.info("Generated job script for %s-%s at %s", name, uid, executable_path)
        logging.debug("Job script content:\n%s", script_body)

        # Ensure log/out/err subdirectories exist under the job directory so that
        # HTCondor can write the job's Log/Output/Error files there.
        for subdir in ("log", "out", "err"):
            subdir_path = os.path.join(job_dir, subdir)
            os.makedirs(subdir_path, exist_ok=True)
            os.chmod(subdir_path, 0o1777)

        transfer_input_line = (
            f"transfer_input_files = {','.join(input_files)}" if input_files else ""
        )

        # Build the list of per-container output files HTCondor should transfer back
        # from the execute sandbox to the job directory on the submit node.
        all_ctn_names = [ctn for ctn, _ in (init_container_commands or [])] + [
            ctn for ctn, _ in container_commands
        ]
        transfer_output_files = ",".join(
            f"{name}-{uid}-{ctn}.out" for ctn in all_ctn_names
        )
        transfer_output_line = (
            f"transfer_output_files = {transfer_output_files}"
            if transfer_output_files
            else 'transfer_output_files = ""'
        )

        job = f"""
Executable = {executable_path}
InitialDir = {job_dir}

Log        = log/mm_mul.$(Cluster).$(Process).log
Output     = out/mm_mul.out.$(Cluster).$(Process)
Error      = err/mm_mul.err.$(Cluster).$(Process)

{transfer_input_line}
{transfer_output_line}
should_transfer_files = YES
RequestCpus = {requested_cpus}
RequestMemory = {requested_memory}

# Retry if the job is held due to the permission error (Code 12, Subcode 13)
periodic_release = (HoldReasonCode == 12 && HoldReasonSubCode == 13)
when_to_transfer_output = ON_EXIT_OR_EVICT
+MaxWallTimeMins = 60

+WMAgent_AgentName = "whatever"

Queue 1
"""
        # print(job)
        with open(sub_path, "w") as f_:
            f_.write(job)
        os.chmod(executable_path, 0o0777)
    except Exception as e:
        logging.error(f"Unable to prepare the job: {e}")

    return sub_path


def produce_htcondor_host_script(container, metadata):
    datarootfolder = InterLinkConfigInst["DataRootFolder"]
    name = metadata["name"]
    uid = metadata["uid"]
    executable_path = f"{datarootfolder}{name}-{uid}.sh"
    sub_path = f"{datarootfolder}{name}-{uid}.jdl"
    try:
        with open(executable_path, "w") as f:
            shebang_line = f"#!{container['command'][-1]}\n"
            script_body = "\n".join(container["args"][-1].split("; "))
            batch_macros = shebang_line + script_body

            f.write(batch_macros)

        requested_cpu = parse_cpu(container["resources"]["requests"]["cpu"])
        # requested_memory = int(container['resources']['requests']['memory'])/1e6
        requested_memory = container["resources"]["requests"]["memory"]
        abs_dataroot = os.path.realpath(datarootfolder)
        for subdir in ("log", "out", "err"):
            subdir_path = os.path.join(abs_dataroot, subdir)
            os.makedirs(subdir_path, exist_ok=True)
            os.chmod(subdir_path, 0o1777)
        job = f"""
Executable = {executable_path}

Log        = log/mm_mul.$(Cluster).$(Process).log
Output     = out/mm_mul.out.$(Cluster).$(Process)
Error      = err/mm_mul.err.$(Cluster).$(Process)

should_transfer_files = YES
RequestCpus = {requested_cpu}
RequestMemory = {requested_memory}

# Retry if the job is held due to the permission error (Code 12, Subcode 13)
periodic_release = (HoldReasonCode == 12 && HoldReasonSubCode == 13)
when_to_transfer_output = ON_EXIT_OR_EVICT
+MaxWallTimeMins = 60

+WMAgent_AgentName = "whatever"

Queue 1
"""
        with open(sub_path, "w") as f_:
            f_.write(job)
        os.chmod(executable_path, 0o0777)
    except Exception as e:
        logging.error(f"Unable to prepare the job: {e}")

    return sub_path


def htcondor_batch_submit(job):
    logging.info("Submitting HTCondor job")

    # Resolve to an absolute path so the argument can never be confused with
    # a flag (e.g. a pod whose name begins with '-'), and validate that the
    # file stays inside the configured DataRootFolder.
    data_root = os.path.realpath(InterLinkConfigInst["DataRootFolder"])
    job_real = os.path.realpath(job)
    if not (job_real == data_root or job_real.startswith(data_root + os.sep)):
        raise ValueError(f"Submit file path escapes data root: {job!r}")

    collector = args.collector_host
    schedd = args.schedd_host
    if collector and schedd:
        # Remote submission: forward the job to a specific pool and schedd.
        cmd = [
            "condor_submit",
            "-pool",
            collector,
            "-remote",
            schedd,
            job_real,
            "-spool",
        ]
    else:
        # Local submission: use the schedd discovered from the local HTCondor pool.
        cmd = ["condor_submit", job_real]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"condor_submit failed (exit {result.returncode}): {result.stderr.strip()}"
        )

    preprocessed = result.stdout
    # Expected output: "1 job(s) submitted to cluster 12345."
    parts = preprocessed.strip().split(" ")
    if not parts:
        raise RuntimeError(f"Unexpected condor_submit output: {preprocessed!r}")
    jid = parts[-1].split(".")[0].strip()
    if not jid.isdigit():
        raise RuntimeError(
            f"Could not parse cluster ID from condor_submit output: {preprocessed!r}"
        )

    return jid


def delete_pod(pod):
    datarootfolder = InterLinkConfigInst["DataRootFolder"]
    name = pod["metadata"]["name"]
    uid = pod["metadata"]["uid"]

    logging.info(f"Deleting pod {pod['metadata']['name']}")
    job_dir = os.path.join(os.path.realpath(datarootfolder), f"{name}-{uid}")
    jid_path = os.path.join(job_dir, f"{name}-{uid}.jid")
    with open(jid_path) as f:
        data = f.read()
    jid = int(data.strip())
    process = os.popen(f"condor_rm {jid}")
    preprocessed = process.read()
    process.close()

    # Remove job directory contents
    try:
        os.remove(os.path.join(job_dir, f"{name}-{uid}.jid"))
    except FileNotFoundError:
        pass
    try:
        os.remove(os.path.join(job_dir, f"{name}-{uid}.sh"))
    except FileNotFoundError:
        pass
    try:
        os.remove(os.path.join(job_dir, f"{name}-{uid}.jdl"))
    except FileNotFoundError:
        pass
    try:
        os.remove(os.path.join(job_dir, f"{name}-{uid}_env.env"))
    except FileNotFoundError:
        pass

    # Clean up per-container log files transferred back by HTCondor inside job dir.
    try:
        with os.scandir(job_dir) as it:
            for entry in it:
                if entry.name.startswith(f"{name}-{uid}-") and entry.name.endswith(
                    ".out"
                ):
                    os.remove(entry.path)
    except OSError as e:
        logging.warning(f"Could not clean up log files for {name}-{uid}: {e}")

    # Optionally remove the job directory if empty
    try:
        os.rmdir(job_dir)
    except OSError:
        # Directory not empty or other error — leave it in place
        pass

    return preprocessed


def handle_jid(jid, pod):
    datarootfolder = InterLinkConfigInst["DataRootFolder"]
    name = pod["metadata"]["name"]
    uid = pod["metadata"]["uid"]

    job_dir = os.path.join(os.path.realpath(datarootfolder), f"{name}-{uid}")
    os.makedirs(job_dir, exist_ok=True)
    os.chmod(job_dir, 0o1777)
    jid_path = os.path.join(job_dir, f"{name}-{uid}.jid")
    with open(jid_path, "w") as f:
        f.write(str(jid))
    JID.append({"JID": jid, "pod": pod})
    logging.info(f"Job {jid} submitted successfully: {jid_path}")


def SubmitHandler():
    # READ THE REQUEST ###############
    logging.info("HTCondor Sidecar: received Submit call")

    try:
        request_data_string = request.data.decode("utf-8")
        logging.debug(f"Decoded request: {request_data_string}")

        # Parse the CreateStruct (InterLink API v0.5.0+ format)
        # Format: {"pod": {...}, "container": [...]}
        create_request = json.loads(request_data_string)

        # Validate that this is a CreateStruct
        if not isinstance(create_request, dict):
            return error_response("Request must be a CreateStruct object", 400)

        if "pod" not in create_request:
            return error_response("Missing 'pod' field in request", 400)

        pod = create_request["pod"]
        containers_standalone = create_request.get("container", [])

    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in request: {e}")
        return error_response("Invalid JSON format", 400)
    except Exception as e:
        logging.error(f"Error decoding request: {e}")
        return error_response("Error processing request", 400)

    # Validate Pod structure
    is_valid, validation_message = validate_pod_request(pod)
    if not is_valid:
        logging.error(f"Invalid Pod structure: {validation_message}")
        return error_response(f"Invalid Pod: {validation_message}", 400)

    # ELABORATE RESPONSE ###########
    # containers_standalone already extracted from create_request["container"]
    # print("Requested pod metadata name is: ", pod["metadata"]["name"])
    metadata = pod.get("metadata", {})
    containers = pod.get("spec", {}).get("containers", [])
    init_containers = pod.get("spec", {}).get("initContainers", [])

    # NORMAL CASE
    if "host" not in containers[0]["image"]:
        probe_scripts = []
        cleanup_scripts = []
        # container_commands collects (name, [tokens]) tuples for every container,
        # mirroring the SLURM plugin's runCtn pattern.
        container_commands = []
        init_container_commands = []
        # poststart_hooks maps container name → parsed hook spec (or None).
        poststart_hooks = {}
        # all_input_files is accumulated across all containers (deduped via seen set)
        all_input_files = []
        seen_input_files = set()

        # ---- init containers (run before main containers) -------------------
        for container in init_containers:
            logging.info(f"Building init-container command for {container['name']}")
            commstr1 = ["singularity", "exec"]
            image = ""
            mounts = [""]
            container_standalone = None
            singularity_options = metadata.get("annotations", {}).get(
                "slurm-job.vk.io/singularity-options", ""
            )
            pre_exec = metadata.get("annotations", {}).get(
                "slurm-job.vk.io/pre-exec", ""
            )
            if containers_standalone is not None:
                for c in containers_standalone:
                    if c["name"] == container["name"]:
                        container_standalone = c
                        mounts = prepare_mounts(pod, container_standalone)
                        break
            env_file_name, env_path = prepare_env_file(
                container, metadata, container_standalone
            )
            env_flags = ["--env-file", f"./{env_file_name}"] if env_file_name else []
            if container["image"].startswith("/cvmfs") or container["image"].startswith(
                "docker://"
            ):
                image = container["image"]
            else:
                image = "docker://" + container["image"]
            for mount in mounts[-1].split(","):
                if "/cvmfs" not in mount:
                    mount_src = mount.split(":")[0]
                    if mount_src and mount_src not in seen_input_files:
                        all_input_files.append(mount_src)
                        seen_input_files.add(mount_src)
            if env_path and env_path not in seen_input_files:
                all_input_files.append(env_path)
                seen_input_files.add(env_path)
            local_mounts = ["--bind", ""]
            for mount in (mounts[-1].split(","))[:-1]:
                if not mount or ":" not in mount:
                    continue
                parts = mount.split(":")
                if "/cvmfs" not in mount:
                    prefix_ = "./"
                else:
                    prefix_ = "/"
                local_src = prefix_ + parts[0].split("/")[-1]
                local_dst = parts[1]
                mount_opts = parts[2] if len(parts) > 2 else None
                if mount_opts:
                    local_mounts[1] += f"{local_src}:{local_dst}:{mount_opts},"
                else:
                    local_mounts[1] += f"{local_src}:{local_dst},"
            if local_mounts[-1] == "":
                local_mounts = [""]
            if "command" in container and "args" in container:
                container_entrypoint = _wrap_command_with_env(
                    container["command"] + container["args"], env_file_name
                )
                singularity_command = (
                    [pre_exec]
                    + commstr1
                    + [singularity_options]
                    + local_mounts
                    + [image]
                    + container_entrypoint
                )
            elif "command" in container:
                container_entrypoint = _wrap_command_with_env(
                    container["command"], env_file_name
                )
                singularity_command = (
                    [pre_exec]
                    + commstr1
                    + [singularity_options]
                    + local_mounts
                    + [image]
                    + container_entrypoint
                )
            elif "args" in container:
                container_entrypoint = _wrap_command_with_env(
                    container["args"], env_file_name
                )
                singularity_command = (
                    [pre_exec]
                    + commstr1
                    + [singularity_options]
                    + local_mounts
                    + [image]
                    + container_entrypoint
                )
            else:
                # No command and no args: use singularity run to invoke the
                # image's default ENTRYPOINT/CMD.  singularity exec without an
                # explicit command is not valid and would fail immediately.
                singularity_command = (
                    [pre_exec]
                    + ["singularity", "run"]
                    + [singularity_options]
                    + env_flags
                    + local_mounts
                    + [image]
                )
            init_container_commands.append((container["name"], singularity_command))

        for container in containers:
            logging.info(
                f"Beginning script generation for container {container['name']}"
            )
            commstr1 = ["singularity", "exec"]
            image = ""
            mounts = [""]
            container_standalone = None
            singularity_options = metadata.get("annotations", {}).get(
                "slurm-job.vk.io/singularity-options", ""
            )

            # flags = metadata.get("annotations", {}).get(
            #     "slurm-job.vk.io/flags", "")

            pre_exec = metadata.get("annotations", {}).get(
                "slurm-job.vk.io/pre-exec", ""
            )
            if containers_standalone is not None:
                for c in containers_standalone:
                    if c["name"] == container["name"]:
                        container_standalone = c
                        mounts = prepare_mounts(pod, container_standalone)
                        break
            # envs = prepare_envs(container)
            env_file_name, env_path = prepare_env_file(
                container, metadata, container_standalone
            )
            env_flags = ["--env-file", f"./{env_file_name}"] if env_file_name else []
            # if container["image"].startswith("/") or ".io" in container["image"]:
            # if container["image"].startswith("/") or "://" in container["image"]:
            #    image_uri = metadata.get("Annotations", {}).get(
            #        "htcondor-job.knoc.io/image-root", None
            #    )
            #    if image_uri:
            #        logging.info(image_uri)
            #        image = image_uri + container["image"]
            #    else:
            #        logging.warning(
            #            "image-uri not specified for path in remote filesystem"
            #        )
            if container["image"].startswith("/cvmfs") or container["image"].startswith(
                "docker://"
            ):
                image = container["image"]
            else:
                image = "docker://" + container["image"]
            # image = container["image"]
            logging.info("Appending all commands together...")
            for mount in mounts[-1].split(","):
                if "/cvmfs" not in mount:
                    mount_src = mount.split(":")[0]
                    if mount_src and mount_src not in seen_input_files:
                        all_input_files.append(mount_src)
                        seen_input_files.add(mount_src)
            if env_path and env_path not in seen_input_files:
                all_input_files.append(env_path)
                seen_input_files.add(env_path)
            local_mounts = ["--bind", ""]
            for mount in (mounts[-1].split(","))[:-1]:
                if not mount or ":" not in mount:
                    continue
                parts = mount.split(":")
                if "/cvmfs" not in mount:
                    prefix_ = "./"
                else:
                    prefix_ = "/"
                local_src = prefix_ + parts[0].split("/")[-1]
                local_dst = parts[1]
                mount_opts = parts[2] if len(parts) > 2 else None
                if mount_opts:
                    local_mounts[1] += f"{local_src}:{local_dst}:{mount_opts},"
                else:
                    local_mounts[1] += f"{local_src}:{local_dst},"
            if local_mounts[-1] == "":
                local_mounts = [""]

            probe_script, cleanup_script = prepare_probes(container, metadata)
            probe_scripts.append(probe_script)
            cleanup_scripts.append(cleanup_script)

            if "command" in container.keys() and "args" in container.keys():
                container_entrypoint = _wrap_command_with_env(
                    container["command"] + container["args"], env_file_name
                )
                singularity_command = (
                    [pre_exec]
                    + commstr1
                    + [singularity_options]
                    + local_mounts
                    + [image]
                    + container_entrypoint
                )
            elif "command" in container.keys():
                container_entrypoint = _wrap_command_with_env(
                    container["command"], env_file_name
                )
                singularity_command = (
                    [pre_exec]
                    + commstr1
                    + [singularity_options]
                    + local_mounts
                    + [image]
                    + container_entrypoint
                )
            elif "args" in container.keys():
                container_entrypoint = _wrap_command_with_env(
                    container["args"], env_file_name
                )
                singularity_command = (
                    [pre_exec]
                    + commstr1
                    + [singularity_options]
                    + local_mounts
                    + [image]
                    + container_entrypoint
                )
            else:
                # No command and no args: use singularity run to invoke the
                # image's default ENTRYPOINT/CMD.  singularity exec without an
                # explicit command is not valid and would fail immediately.
                singularity_command = (
                    [pre_exec]
                    + ["singularity", "run"]
                    + [singularity_options]
                    + env_flags
                    + local_mounts
                    + [image]
                )
            # Collect as (name, tokens) for runCtn pattern
            container_commands.append((container["name"], singularity_command))

            # Collect postStart hook (if defined) for this container.
            lifecycle = container.get("lifecycle") or {}
            poststart_raw = lifecycle.get("postStart")
            poststart_hooks[container["name"]] = (
                _translate_lifecycle_hook(poststart_raw) if poststart_raw else None
            )

        prestop_trap = prepare_lifecycle_hooks(containers, metadata)

        path = produce_htcondor_singularity_script(
            containers,
            metadata,
            container_commands,
            all_input_files,
            probe_scripts=probe_scripts,
            cleanup_scripts=cleanup_scripts,
            init_container_commands=init_container_commands,
            prestop_trap=prestop_trap,
            poststart_hooks=poststart_hooks,
        )

    else:
        # print("host keyword detected, ignoring other containers")
        sitename = containers[0]["image"].split(":")[-1]
        print(sitename)
        path = produce_htcondor_host_script(containers[0], metadata)

    try:
        out_jid = htcondor_batch_submit(path)
        logging.info(f"Job submitted with cluster id: {out_jid}")
        handle_jid(out_jid, pod)

        # Verify job submission: the JID file must live in the per-job directory
        job_dir = os.path.join(
            os.path.realpath(InterLinkConfigInst["DataRootFolder"]),
            f"{pod['metadata']['name']}-{pod['metadata']['uid']}",
        )
        jid_file = os.path.join(
            job_dir, f"{pod['metadata']['name']}-{pod['metadata']['uid']}.jid"
        )
        if not os.path.exists(jid_file):
            raise Exception("JID file was not created")

        resp = {
            "PodUID": pod["metadata"]["uid"],
            "PodJID": str(out_jid),
        }
        return success_response(resp, 200)
    except Exception as e:
        logging.error(f"Job submission failed: {e}")
        return error_response(f"Job submission failed: {str(e)}", 500)


def StopHandler():
    # READ THE REQUEST ######
    logging.info("HTCondor Sidecar: received Stop call")
    try:
        request_data_string = request.data.decode("utf-8")
        req = json.loads(request_data_string)
        # Validate request structure
        is_valid, validation_message = validate_pod_request(req)
        if not is_valid:
            logging.error(f"Invalid delete request: {validation_message}")
            return error_response(f"Invalid request: {validation_message}", 400)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in delete request: {e}")
        return error_response("Invalid JSON format", 400)
    except Exception as e:
        logging.error(f"Error processing delete request: {e}")
        return error_response("Error processing request", 400)

    # DELETE JOB RELATED TO REQUEST
    try:
        return_message = delete_pod(req)
        logging.info(f"Pod deletion result: {return_message}")
        # condor_rm returns "All jobs removed" on success, or a message like
        # "There are no jobs in the queue" / "Couldn't find/remove all jobs"
        # when the job already finished.  Both outcomes mean the job is gone.
        resp = {
            "message": "Pod successfully deleted",
            "podUID": req.get("metadata", {}).get("uid", ""),
            "podName": req.get("metadata", {}).get("name", ""),
        }
        return success_response(resp, 200)
    except FileNotFoundError as e:
        logging.error(f"Pod files not found during deletion: {e}")
        return error_response("Pod not found or already deleted", 404)
    except Exception as e:
        logging.error(f"Error deleting pod: {e}")
        return error_response(f"Deletion failed: {str(e)}", 500)


def StatusHandler():
    # READ THE REQUEST #####################
    logging.info("HTCondor Sidecar: received GetStatus call")
    try:
        request_data_string = request.data.decode("utf-8")
        req_list = json.loads(request_data_string)
        # Handle ping requests (empty array)
        if isinstance(req_list, list) and len(req_list) == 0:
            logging.info("Received ping request")
            # If no proxy path is configured (local/mini HTCondor), skip the
            # check entirely.  If a path is configured, verify the file exists.
            if args.proxy and not os.path.isfile(args.proxy):
                return error_response(
                    "HTCondor sidecar not ready - proxy file not available", 503
                )
            return success_response(
                {"message": "HTCondor sidecar is alive", "status": "healthy"}, 200
            )
        # Validate request format
        if not isinstance(req_list, list):
            return error_response("Status request must be an array", 400)
        if len(req_list) == 0:
            return error_response("Empty request array", 400)
        # Validate every pod in the list up-front
        for req in req_list:
            is_valid, validation_message = validate_pod_request(req)
            if not is_valid:
                logging.error(f"Invalid status request: {validation_message}")
                return error_response(f"Invalid request: {validation_message}", 400)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in status request: {e}")
        return error_response("Invalid JSON format", 400)
    except Exception as e:
        logging.error(f"Error processing status request: {e}")
        return error_response("Error processing request", 400)

    # ELABORATE RESPONSE — process ALL pods in the list #################
    resp = []
    for req in req_list:
        try:
            job_dir = os.path.join(
                os.path.realpath(InterLinkConfigInst["DataRootFolder"]),
                f"{req['metadata']['name']}-{req['metadata']['uid']}",
            )
            jid_file = os.path.join(
                job_dir, f"{req['metadata']['name']}-{req['metadata']['uid']}.jid"
            )
            with open(jid_file, "r") as f:
                jid_job = f.read().strip()
            podname = req["metadata"]["name"]
            podnamespace = req["metadata"].get("namespace", "default")
            poduid = req["metadata"]["uid"]
            # Query HTCondor for job status
            process = os.popen(f"condor_q {jid_job} --json")
            preprocessed = process.read()
            process.close()
            if not preprocessed.strip():
                # Job not found in queue, check history
                process = os.popen(f"condor_history {jid_job} --json")
                preprocessed = process.read()
                process.close()
            if not preprocessed.strip():
                logging.error(f"Job {jid_job} not found in HTCondor queue or history")
                continue
            job_data = json.loads(preprocessed)
            if not job_data:
                logging.error(f"No job data found for job {jid_job}")
                continue
            job = job_data[0]
            status = job.get("JobStatus", 0)
            # Get actual timestamps from HTCondor
            current_time = datetime.utcnow().isoformat() + "Z"
            start_time = (
                datetime.fromtimestamp(job.get("JobStartDate", 0)).isoformat() + "Z"
                if job.get("JobStartDate")
                else current_time
            )
            completion_time = (
                datetime.fromtimestamp(job.get("CompletionDate", 0)).isoformat() + "Z"
                if job.get("CompletionDate")
                else current_time
            )
            # Map HTCondor status to Kubernetes container states
            if status == 1:  # Idle
                state = {"waiting": {"reason": "ContainerCreating"}}
                readiness = False
            elif status == 2:  # Running
                state = {"running": {"startedAt": start_time}}
                readiness = True
            elif status == 4:  # Completed
                state = {
                    "terminated": {
                        "startedAt": start_time,
                        "finishedAt": completion_time,
                        "exitCode": job.get("ExitCode", 0),
                        "reason": "Completed",
                    }
                }
                readiness = False
            elif status == 3:  # Removed
                state = {
                    "terminated": {
                        "startedAt": start_time,
                        "finishedAt": completion_time,
                        "reason": "Cancelled",
                    }
                }
                readiness = False
            elif status == 5:  # Held
                state = {
                    "waiting": {
                        "reason": "JobHeld",
                        "message": job.get("HoldReason", "Job held by HTCondor"),
                    }
                }
                readiness = False
            else:
                state = {"waiting": {"reason": "Unknown"}}
                readiness = False
            # Build container status list
            containers = []
            for c in req["spec"]["containers"]:
                containers.append(
                    {
                        "name": c["name"],
                        "state": state,
                        "lastState": {},
                        "ready": readiness,
                        "restartCount": 0,
                        "image": c.get("image", "unknown"),
                        "imageID": c.get("image", "unknown"),
                    }
                )
            resp.append(
                {
                    "name": podname,
                    "UID": poduid,
                    "namespace": podnamespace,
                    "JID": jid_job,
                    "containers": containers,
                    "initContainers": [],
                }
            )
        except FileNotFoundError:
            logging.error(
                f"Job file not found for pod {req['metadata'].get('name', '?')}"
            )
        except json.JSONDecodeError as e:
            logging.error(
                "Error parsing HTCondor response for pod %s: %s",
                req["metadata"].get("name", "?"),
                e,
            )
        except Exception as e:
            logging.error(
                "Error retrieving status for pod %s: %s",
                req["metadata"].get("name", "?"),
                e,
            )
    return success_response(resp, 200)


def LogsHandler():
    logging.info("HTCondor Sidecar: received GetLogs call")
    try:
        request_data_string = request.data.decode("utf-8")
        req = json.loads(request_data_string)
        if req is None or not isinstance(req, dict):
            logging.error("Invalid request data")
            return "Invalid request data for getting logs", 400

        pod_name = req.get("PodName", "")
        pod_uid = req.get("PodUID", "")
        container_name = req.get("ContainerName", "")

        job_dir = os.path.join(
            os.path.realpath(InterLinkConfigInst["DataRootFolder"]),
            f"{pod_name}-{pod_uid}",
        )

        if not pod_name or not pod_uid or not container_name:
            logging.warning("GetLogs: missing PodName/PodUID/ContainerName in request")
            return "", 200

        datarootfolder = InterLinkConfigInst["DataRootFolder"]
        dataroot_real = os.path.realpath(datarootfolder)

        # Sanitize each name component.  os.path.basename strips embedded path
        # separators; the regex further limits characters to those allowed in
        # Kubernetes names (alphanumeric, hyphens, dots) plus UUID hyphens,
        # preventing null bytes and other unexpected characters.
        _safe = re.compile(r"^[a-zA-Z0-9._-]+$")
        parts = {
            "PodName": os.path.basename(pod_name),
            "PodUID": os.path.basename(pod_uid),
            "ContainerName": os.path.basename(container_name),
        }
        for field, value in parts.items():
            if not value or not _safe.match(value):
                logging.error(f"GetLogs: invalid {field} value: {value!r}")
                return "", 400

        # The per-container output file is written to the HTCondor execute sandbox
        # as a relative path by runCtn().  condor_tail must therefore use the
        # sandbox filename, while the post-transfer fallback reads the copy under
        # the per-job directory in the data root.
        sandbox_log_filename = (
            f"{parts['PodName']}-{parts['PodUID']}-{parts['ContainerName']}.out"
        )
        transferred_log_path = os.path.join(job_dir, sandbox_log_filename)

        opts = req.get("Opts", {})
        raw_tail = opts.get("Tail", 0) if isinstance(opts, dict) else 0
        tail = raw_tail if isinstance(raw_tail, int) and raw_tail > 0 else 0

        content = None

        # --- Try condor_tail first (no shared filesystem required) ---
        job_dir = os.path.join(
            os.path.realpath(datarootfolder), f"{parts['PodName']}-{parts['PodUID']}"
        )
        jid_file = os.path.join(job_dir, f"{parts['PodName']}-{parts['PodUID']}.jid")
        # Validate the jid_file path stays within the data root before opening.
        jid_file_real = os.path.realpath(jid_file)
        if os.path.exists(jid_file_real) and jid_file_real.startswith(
            dataroot_real + os.sep
        ):
            try:
                with open(jid_file_real, "r") as fh:
                    cluster_id = fh.read().strip()
                if cluster_id.isdigit():
                    proc_id = f"{cluster_id}.0"
                    # condor_tail retrieves the file from the execute sandbox via
                    # the HTCondor networking protocol; works for running jobs and
                    # recently-completed jobs whose sandbox has not yet been cleaned.
                    # proc_id is digits + ".0"; log_filename is validated by _safe.
                    collector = args.collector_host
                    schedd = args.schedd_host
                    if collector and schedd:
                        cmd = [
                            "condor_tail",
                            "-pool",
                            collector,
                            "-name",
                            schedd,
                            "-maxbytes",
                            str(_CONDOR_TAIL_MAX_BYTES),
                            proc_id,
                            sandbox_log_filename,
                        ]
                    else:
                        cmd = [
                            "condor_tail",
                            "-maxbytes",
                            str(_CONDOR_TAIL_MAX_BYTES),
                            proc_id,
                            sandbox_log_filename,
                        ]
                    result = subprocess.run(
                        cmd, capture_output=True, text=True, timeout=60
                    )
                    if result.stdout:
                        content = result.stdout
                        logging.info(
                            "GetLogs: retrieved via condor_tail for"
                            f" {proc_id} {sandbox_log_filename}"
                        )
                    else:
                        logging.info(
                            f"GetLogs: condor_tail rc={result.returncode}"
                            f" ({result.stderr.strip()!r}), falling back to file"
                        )
                else:
                    logging.warning(
                        f"GetLogs: invalid cluster_id in {jid_file}: {cluster_id!r}"
                    )
            except Exception as e:
                logging.info(f"GetLogs: condor_tail failed ({e}), falling back to file")

        # --- Fall back to the HTCondor-transferred copy in the data root ---
        # After the job completes, HTCondor transfers the sandbox file back to
        # InitialDir (abs_dataroot) via the standard file-transfer mechanism.
        if content is None:
            log_file_real = os.path.realpath(transferred_log_path)
            # After resolving symlinks, the file must live *inside* the data root
            # (not equal to it and not outside it).
            if not log_file_real.startswith(dataroot_real + os.sep):
                logging.error(
                    f"GetLogs: path traversal attempt blocked: {transferred_log_path!r}"
                )
                return "", 400
            logging.info(f"GetLogs: reading transferred file {log_file_real}")
            try:
                with open(log_file_real, "r", errors="replace") as fh:
                    content = fh.read()
            except FileNotFoundError:
                logging.info(f"GetLogs: log file not found yet: {log_file_real}")
                return "", 200

        if tail > 0 and content:
            lines = content.splitlines(keepends=True)
            content = "".join(lines[-tail:])
        return content or "", 200, {"Content-Type": "text/plain"}

    except Exception as e:
        logging.error(f"Error in LogsHandler: {e}")
        return "", 200


def SystemInfoHandler():
    """Health-check endpoint that reports HTCondor connectivity.

    Mirrors the /system-info endpoint in the SLURM plugin (see
    pkg/slurm/SystemInfo.go), adapted for HTCondor: runs ``condor_status -totals``
    to verify the schedd/collector is reachable and returns a JSON payload
    with status, timestamp, and the condensed condor_status output.
    """
    logging.info("HTCondor Sidecar: received SystemInfo call")

    response = {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "htcondor_connected": False,
    }

    try:
        process = os.popen("condor_status -totals 2>&1")
        output = process.read()
        process.close()
        if "TotalMachines" in output or "Machines" in output or "Slots" in output:
            response["htcondor_connected"] = True
            response["condor_status_output"] = output.strip()
        else:
            # condor_status ran but returned unexpected output — treat as warning
            response["status"] = "warning"
            response["htcondor_connected"] = False
            response["error"] = "condor_status returned unexpected output"
            response["condor_status_output"] = output.strip()
    except Exception as e:
        logging.warning(f"Failed to execute condor_status: {e}")
        response["status"] = "warning"
        response["htcondor_connected"] = False
        response["error"] = str(e)

    return jsonify(response), 200


app = Flask(__name__)
app.add_url_rule("/create", view_func=SubmitHandler, methods=["POST"])
app.add_url_rule("/delete", view_func=StopHandler, methods=["POST"])
app.add_url_rule("/status", view_func=StatusHandler, methods=["GET"])
app.add_url_rule("/getLogs", view_func=LogsHandler, methods=["GET"])
app.add_url_rule("/system-info", view_func=SystemInfoHandler, methods=["GET"])

if __name__ == "__main__":
    app.run(port=args.port, host="0.0.0.0", debug=True)
