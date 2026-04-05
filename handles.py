import argparse
import json
import logging
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


def prepare_env_file(container, metadata, env_file_name="jlab.env"):
    env_file_name = f"{metadata['name']}-{metadata['uid']}_env.env"
    env_file_path = os.path.join(InterLinkConfigInst["DataRootFolder"], env_file_name)
    lines = []

    try:
        for env_var in container.get("env", []):
            name = env_var["name"]
            raw_val = env_var.get("value") or ""
            safe_val = shlex.quote(raw_val)
            lines.append(f"{name}={safe_val}")

        with open(env_file_path, "w") as fp:
            fp.write("\n".join(lines) + "\n")
        os.chmod(env_file_path, 0o777)
        logging.info(f"Wrote env file to {env_file_path}")

        return (["--env-file", env_file_name], env_file_path)

    except Exception as e:
        logging.error(f"Failed to write env file: {e}")
        return ([], None)


def prepare_mounts(pod, container_standalone):
    mounts = ["--bind"]
    mount_data = []
    pod_name = (
        container_standalone["name"].split("-")[:6]
        if len(container_standalone["name"].split("-")) > 6
        else container_standalone["name"].split("-")
    )
    pod_name_folder = os.path.join(
        InterLinkConfigInst["DataRootFolder"], "-".join(pod_name[:-1])
    )
    for c in pod["spec"]["containers"]:
        if c["name"] == container_standalone["name"]:
            container = c
    try:
        os.makedirs(pod_name_folder, exist_ok=True)
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
                    config_maps_paths = mountConfigMaps(pod, container_standalone)
                    # print("bind as configmap", mount_var["name"], vol["name"])
                    for i, path in enumerate(config_maps_paths):
                        mount_data.append(path)
                elif "secret" in vol.keys():
                    secrets_paths = mountSecrets(pod, container_standalone)
                    # print("bind as secret", mount_var["name"], vol["name"])
                    for i, path in enumerate(secrets_paths):
                        mount_data.append(path)
                elif "emptyDir" in vol.keys():
                    path = mount_empty_dir(container, pod)
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
        cmd = ["-rf", os.path.join(os.getcwd(), data_root_folder, "configMaps")]
        shell = subprocess.Popen(
            ["rm"] + cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
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
                    namespace = pod["metadata"]["namespace"]
                    uid = pod["metadata"]["uid"]
                    for cfgMap in cfgMaps:
                        podConfigMapDir = os.path.join(
                            os.getcwd(),
                            data_root_folder,
                            f"{namespace}-{uid}/configMaps/",
                            vol["name"],
                        )
                        for key in cfgMap["data"].keys():
                            path = os.path.join(os.getcwd(), podConfigMapDir, key)
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
        cmd = ["-rf", os.path.join(os.getcwd(), data_root_folder, "secrets")]
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
                        namespace = pod["metadata"]["namespace"]
                        uid = pod["metadata"]["uid"]
                        pod_secret_dir = os.path.join(
                            os.getcwd(),
                            data_root_folder,
                            f"{namespace}-{uid}/secrets/",
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
                            with open(full_path, "w") as f:
                                f.write(v)
                            os.chmod(full_path, vol["secret"]["defaultMode"])
                            logging.debug(f"--- Written Secret file {full_path}")
    return secret_name_paths


def mount_empty_dir(container, pod):
    ed_path = None
    if InterLinkConfigInst["ExportPodData"] and "volumeMounts" in container.keys():
        cmd = ["-rf", os.path.join(InterLinkConfigInst["DataRootFolder"], "emptyDirs")]
        subprocess.run(["rm"] + cmd, check=True)
        for mount_spec in container["volumeMounts"]:
            pod_volume_spec = None
            for vol in pod["spec"]["volumes"]:
                if vol.name == mount_spec["name"]:
                    pod_volume_spec = vol["volumeSource"]
                    break
            if pod_volume_spec and pod_volume_spec["EmptyDir"]:
                ed_path = os.path.join(
                    InterLinkConfigInst["DataRootFolder"],
                    pod.namespace + "-" + str(pod.uid) + "/emptyDirs/" + vol.name,
                )
                cmd = ["-p", ed_path]
                subprocess.run(["mkdir"] + cmd, check=True)
                ed_path += (
                    ":" + mount_spec["mount_path"] + "/" + mount_spec["name"] + ","
                )

    return ed_path


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
_RUN_CTN_HELPERS = r"""
runCtn() {
  local ctn="$1"
  shift
  ( "$@" ) > "${workingPath}/run-${ctn}.out" 2>&1 &
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


def _clean_command_tokens(tokens):
    """Join and clean a list of singularity command tokens into a single string.

    Wraps the token that follows a ``-c`` flag in single quotes (so the shell
    does not re-split it), then strips empty double-quoted tokens and collapses
    extra whitespace.
    """
    result = list(tokens)
    for i in range(1, len(result)):
        if result[i - 1] == "-c":
            result[i] = "'" + result[i] + "'"
    line = " ".join(result)
    line = re.sub(r'\s*""\s*', " ", line)
    line = re.sub(r" {2,}", " ", line)
    return line.strip()


def produce_htcondor_singularity_script(
    containers,
    metadata,
    container_commands,
    input_files,
    probe_scripts=None,
    cleanup_scripts=None,
):
    """Write the HTCondor job executable and submit description file.

    Each container is launched in the background via a ``runCtn()`` bash
    helper, mirroring the SLURM plugin's pattern.  ``waitCtns()`` collects
    all exit codes, and ``endScript()`` exits with the highest one.

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
    """
    if probe_scripts is None:
        probe_scripts = []
    if cleanup_scripts is None:
        cleanup_scripts = []

    datarootfolder = InterLinkConfigInst["DataRootFolder"]
    name = metadata["name"]
    uid = metadata["uid"]
    executable_path = f"./{datarootfolder}/{name}-{uid}.sh"
    sub_path = f"./{datarootfolder}/{name}-{uid}.jdl"

    requested_cpus = 0
    requested_memory = 0
    for c in containers:
        if "resources" in c.keys():
            if "requests" in c["resources"].keys():
                if "cpu" in c["resources"]["requests"].keys():
                    requested_cpus += int(c["resources"]["requests"]["cpu"])
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
            # ---- shebang ------------------------------------------------
            script_body = "#!/bin/bash\n"

            # ---- probe cleanup traps (must be defined before any trap) --
            for cs in cleanup_scripts:
                script_body += "\n" + cs + "\n"

            # ---- runCtn / waitCtns / endScript helpers ------------------
            script_body += _RUN_CTN_HELPERS

            # ---- preamble (exports, wstunnel, command prefix, etc.) -----
            if prefix_.strip():
                script_body += "\n" + prefix_.strip() + "\n"

            # ---- probe background sub-shells ----------------------------
            for ps in probe_scripts:
                script_body += "\n" + ps + "\n"

            # ---- main: run every container in background ----------------
            script_body += "\nhighestExitCode=0\n"
            script_body += 'pidCtns=""\n'
            script_body += "export workingPath=$(pwd)\n\n"

            for ctn_name, cmd_tokens in container_commands:
                cleaned = _clean_command_tokens(cmd_tokens)
                script_body += f"runCtn {ctn_name} {cleaned}\n"

            # ---- wait for all containers and exit -----------------------
            script_body += "\nwaitCtns\nendScript\n"

            f.write(script_body)

        job = f"""
Executable = {executable_path}

Log        = log/mm_mul.$(Cluster).$(Process).log
Output     = out/mm_mul.out.$(Cluster).$(Process)
Error      = err/mm_mul.err.$(Cluster).$(Process)

transfer_input_files = {",".join(input_files)}
should_transfer_files = YES
RequestCpus = {requested_cpus}
RequestMemory = {requested_memory}

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
            batch_macros = f"""#!{container['command'][-1]}
""" + "\n".join(
                container["args"][-1].split("; ")
            )

            f.write(batch_macros)

        requested_cpu = container["resources"]["requests"]["cpu"]
        # requested_memory = int(container['resources']['requests']['memory'])/1e6
        requested_memory = container["resources"]["requests"]["memory"]
        job = f"""
Executable = {executable_path}

Log        = log/mm_mul.$(Cluster).$(Process).log
Output     = out/mm_mul.out.$(Cluster).$(Process)
Error      = err/mm_mul.err.$(Cluster).$(Process)

should_transfer_files = YES
RequestCpus = {requested_cpu}
RequestMemory = {requested_memory}

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
    collector = args.collector_host
    schedd = args.schedd_host
    process = os.popen(f"condor_submit -pool {collector} -remote {schedd} {job} -spool")
    preprocessed = process.read()
    process.close()
    jid = preprocessed.split(" ")[-1].split(".")[0]

    return jid


def delete_pod(pod):

    datarootfolder = InterLinkConfigInst["DataRootFolder"]
    name = pod["metadata"]["name"]
    uid = pod["metadata"]["uid"]

    logging.info(f"Deleting pod {pod['metadata']['name']}")
    with open(f"{datarootfolder}{name}-{uid}.jid") as f:
        data = f.read()
    jid = int(data.strip())
    process = os.popen(f"condor_rm {jid}")
    preprocessed = process.read()
    process.close()
    os.remove(f"{datarootfolder}{name}-{uid}.jid")
    os.remove(f"{datarootfolder}{name}-{uid}.sh")
    os.remove(f"{datarootfolder}{name}-{uid}.jdl")
    os.remove(f"{datarootfolder}{name}-{uid}_env.env")

    return preprocessed


def handle_jid(jid, pod):
    datarootfolder = InterLinkConfigInst["DataRootFolder"]
    name = pod["metadata"]["name"]
    uid = pod["metadata"]["uid"]

    with open(
        f"{datarootfolder}{name}-{uid}.jid",
        "w",
    ) as f:
        f.write(str(jid))
    JID.append({"JID": jid, "pod": pod})
    logging.info(
        f"Job {jid} submitted successfully",
        f"{datarootfolder}{name}-{uid}.jid",
    )


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

    # NORMAL CASE
    if "host" not in containers[0]["image"]:
        probe_scripts = []
        cleanup_scripts = []
        # container_commands collects (name, [tokens]) tuples for every container,
        # mirroring the SLURM plugin's runCtn pattern.
        container_commands = []
        # all_input_files is accumulated across all containers (deduped via seen set)
        all_input_files = []
        seen_input_files = set()

        for container in containers:
            logging.info(
                f"Beginning script generation for container {container['name']}"
            )
            commstr1 = ["singularity", "exec"]
            # envs = prepare_envs(container)
            env_flags, env_path = prepare_env_file(container, metadata)
            image = ""
            mounts = [""]
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
            else:
                mounts = [""]
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
                if "/cvmfs" not in mount:
                    prefix_ = "./"
                else:
                    prefix_ = "/"
                local_mounts[1] += (
                    prefix_
                    + (mount.split(":")[0]).split("/")[-1]
                    + ":"
                    + mount.split(":")[1]
                    + ","
                )
            if local_mounts[-1] == "":
                local_mounts = [""]

            probe_script, cleanup_script = prepare_probes(container, metadata)
            probe_scripts.append(probe_script)
            cleanup_scripts.append(cleanup_script)

            if "command" in container.keys() and "args" in container.keys():
                singularity_command = (
                    [pre_exec]
                    + commstr1
                    + [singularity_options]
                    + env_flags
                    + local_mounts
                    + [image]
                    + container["command"]
                    + container["args"]
                )
            elif "command" in container.keys():
                singularity_command = (
                    [pre_exec]
                    + commstr1
                    + [singularity_options]
                    + env_flags
                    + local_mounts
                    + [image]
                    + container["command"]
                )
            elif "args" in container.keys():
                singularity_command = (
                    [pre_exec]
                    + commstr1
                    + [singularity_options]
                    + env_flags
                    + local_mounts
                    + [image]
                    + container["args"]
                )
            else:
                singularity_command = (
                    [pre_exec] + commstr1 + env_flags + local_mounts + [image]
                )
            # Collect as (name, tokens) for runCtn pattern
            container_commands.append((container["name"], singularity_command))

        path = produce_htcondor_singularity_script(
            containers,
            metadata,
            container_commands,
            all_input_files,
            probe_scripts=probe_scripts,
            cleanup_scripts=cleanup_scripts,
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

        # Verify job submission was successful
        jid_file = (
            InterLinkConfigInst["DataRootFolder"]
            + pod["metadata"]["name"]
            + "-"
            + pod["metadata"]["uid"]
            + ".jid"
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
        # Check if deletion was successful
        if "All" in return_message or "removed" in return_message.lower():
            resp = {
                "message": "Pod successfully deleted",
                "podUID": req.get("metadata", {}).get("uid", ""),
                "podName": req.get("metadata", {}).get("name", ""),
            }
            return success_response(resp, 200)
        else:
            return error_response("Failed to delete pod from HTCondor", 500)
    except FileNotFoundError as e:
        logging.error(f"Pod files not found during deletion: {e}")
        return error_response("Pod not found or already deleted", 404)
    except Exception as e:
        logging.error(f"Error deleting pod: {e}")
        return error_response(f"Deletion failed: {str(e)}", 500)


def parse_cluster_resources_from_json(stdout):
    """Parse ``condor_status --json`` output into a PingResponse dict.

    Available resources are computed as the sum of ``Cpus`` and ``Memory``
    across all Unclaimed slots.  Dynamic child-slots (``DynamicSlot=True``)
    are excluded to avoid double-counting with their partitionable-slot
    parents, which already advertise the remaining (unclaimed) capacity.

    Aligned with the interlink-hq/interLink#516 PingResponse schema so the
    virtual kubelet can call ``updateNodeResources()`` on every heartbeat.

    Args:
        stdout: String output from ``condor_status --json``.

    Returns:
        dict with ``status`` and ``resources`` keys (cpu/memory as Kubernetes
        quantity strings, e.g. ``"24"`` and ``"96000Mi"``).

    Raises:
        ValueError: If *stdout* cannot be parsed or contains no slots.

    TODO: Replace the locally-defined PingResponse dict with the upstream
    commonIL.PingResponse type once interlink-hq/interLink#516 is merged and
    the interLink dependency is updated.
    """
    try:
        slots = json.loads(stdout)
    except json.JSONDecodeError as e:
        raise ValueError(f"condor_status --json parse error: {e}") from e

    if not isinstance(slots, list) or len(slots) == 0:
        raise ValueError("condor_status --json returned no slots")

    avail_cpus = 0
    avail_mem_mb = 0
    for slot in slots:
        # Skip dynamic child-slots to avoid double-counting with partitionable
        # slot parents which already advertise remaining (unclaimed) capacity.
        if slot.get("DynamicSlot", False):
            continue
        if slot.get("State", "") == "Unclaimed":
            avail_cpus += slot.get("Cpus", 0)
            avail_mem_mb += slot.get("Memory", 0)

    return {
        "status": "ok",
        "resources": {
            "cpu": str(avail_cpus),
            "memory": f"{avail_mem_mb}Mi",
        },
    }


def parse_cluster_resources_from_text(stdout):
    """Parse ``condor_status -autoformat Cpus Memory`` output into a PingResponse dict.

    Each non-empty line is expected to contain two whitespace-separated
    integers: CPUs and memory in MB.  Lines that cannot be parsed are
    silently skipped.

    NOTE: Unlike :func:`parse_cluster_resources_from_json` which reports
    *available* resources, this function sums the total installed CPUs and
    memory because plain-text ``condor_status`` output does not include
    per-slot allocation state.

    Args:
        stdout: String output from ``condor_status -autoformat Cpus Memory``.

    Returns:
        dict with ``status`` and ``resources`` keys (cpu/memory as Kubernetes
        quantity strings).
    """
    total_cpus = 0
    total_mem_mb = 0
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        try:
            cpu = int(parts[0])
            mem = int(parts[1])
        except ValueError:
            continue
        total_cpus += cpu
        total_mem_mb += mem

    return {
        "status": "ok",
        "resources": {
            "cpu": str(total_cpus),
            "memory": f"{total_mem_mb}Mi",
        },
    }


def get_taints_from_config():
    """Return the taint list from ``SidecarConfig.yaml``, or ``None`` if not configured.

    When the ``Taints`` key is present in the config (even as an empty list),
    the returned list is passed to the VK in the ping response so it can
    replace the node's non-system taints (interLink#516 behaviour).
    When the key is absent, ``None`` is returned and the ``taints`` field is
    omitted from the ping response, leaving the node's existing taints intact.

    Each taint dict must have a ``key`` and ``effect`` field (``value`` is
    optional).  Invalid entries are skipped with a warning.

    Returns:
        list of taint dicts, or None.
    """
    raw = InterLinkConfigInst.get("Taints")
    if raw is None:
        return None
    if not isinstance(raw, list):
        logging.warning(
            "SidecarConfig Taints must be a list; ignoring invalid value: %r", raw
        )
        return None

    taints = []
    for item in raw:
        if not isinstance(item, dict):
            logging.warning("Skipping non-dict taint entry: %r", item)
            continue
        key = item.get("key", "")
        effect = item.get("effect", "")
        if not key or not effect:
            logging.warning(
                "Skipping taint with missing key or effect: %r", item
            )
            continue
        taint = {"key": key, "effect": effect}
        if "value" in item:
            taint["value"] = item["value"]
        taints.append(taint)
    return taints


def get_cluster_resources():
    """Query the cluster for current resource availability.

    When ``ClusterResourcesScript`` is set in ``SidecarConfig.yaml``, that
    command is executed and its stdout is parsed as a JSON PingResponse dict
    (must contain at least a ``resources`` key).  This lets operators supply
    their own resource-reporting logic without modifying the plugin code.

    When ``ClusterResourcesScript`` is not set, the built-in HTCondor logic
    is used: tries ``condor_status --json`` first for accurate per-slot
    available CPU and memory data, then falls back to
    ``condor_status -autoformat Cpus Memory`` (total capacity) when JSON
    output is unavailable or cannot be parsed.

    Returns:
        dict aligned with the interLink#516 PingResponse schema.

    Raises:
        OSError: if the configured script cannot be executed.
        ValueError: if the script output cannot be parsed as JSON.
    """
    script = InterLinkConfigInst.get("ClusterResourcesScript", "").strip()
    if script:
        return _run_cluster_resources_script(script)

    # Built-in HTCondor path
    try:
        process = os.popen("condor_status --json 2>/dev/null")
        stdout = process.read()
        process.close()
        if stdout.strip():
            return parse_cluster_resources_from_json(stdout)
    except (OSError, ValueError) as e:
        logging.debug(
            f"condor_status --json unavailable ({e}), falling back to text parsing"
        )

    # Fallback: plain-text condor_status
    process = os.popen("condor_status -autoformat Cpus Memory 2>/dev/null")
    stdout = process.read()
    process.close()
    return parse_cluster_resources_from_text(stdout)


def _run_cluster_resources_script(script):
    """Execute *script* and parse its JSON stdout as a PingResponse dict.

    The script is responsible for printing a JSON object to stdout that
    follows the interLink#516 PingResponse schema, e.g.::

        {"status": "ok", "resources": {"cpu": "48", "memory": "192000Mi"}}

    The ``status`` field defaults to ``"ok"`` if the script omits it.
    Only ``resources`` (and optionally ``taints``) are propagated; any other
    fields in the script output are silently ignored.

    Args:
        script: Shell command string to execute (run via the shell so that
            paths, env vars and pipes work as expected).

    Returns:
        dict aligned with the interLink#516 PingResponse schema.

    Raises:
        OSError: if the script cannot be executed.
        ValueError: if the script output cannot be parsed as a JSON object.
    """
    logging.debug(f"Running ClusterResourcesScript: {script}")
    process = os.popen(f"{script} 2>/dev/null")
    stdout = process.read()
    process.close()

    if not stdout.strip():
        raise ValueError(
            f"ClusterResourcesScript produced no output: {script!r}"
        )

    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"ClusterResourcesScript output is not valid JSON: {e}"
        ) from e

    if not isinstance(data, dict):
        raise ValueError(
            f"ClusterResourcesScript output must be a JSON object, got: {type(data).__name__}"
        )

    result = {"status": data.get("status", "ok")}
    if "resources" in data:
        result["resources"] = data["resources"]
    return result


def StatusHandler():
    # READ THE REQUEST #####################
    logging.info("HTCondor Sidecar: received GetStatus call")
    try:
        request_data_string = request.data.decode("utf-8")
        req_list = json.loads(request_data_string)
        # Handle ping requests (empty array): return cluster resource availability
        # as JSON so the virtual kubelet can update the node's advertised capacity.
        # This path is triggered by the interlink-api ping call (interLink#516).
        if isinstance(req_list, list) and len(req_list) == 0:
            logging.info(
                "Received ping request (empty pod list), returning cluster resource availability"
            )
            if args.proxy and not os.path.isfile(args.proxy):
                return error_response(
                    "HTCondor sidecar not ready - proxy file not available", 503
                )
            try:
                ping_resp = get_cluster_resources()
            except (OSError, ValueError) as e:
                logging.warning(f"Failed to query cluster resources: {e}")
                ping_resp = {"status": "ok"}
            # Add taints from config if configured (interLink#516).
            # When present (even as []), the VK replaces non-system taints.
            # When absent, the VK leaves existing taints unchanged.
            taints = get_taints_from_config()
            if taints is not None:
                ping_resp["taints"] = taints
            return jsonify(ping_resp), 200
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
            jid_file = (
                InterLinkConfigInst["DataRootFolder"]
                + req["metadata"]["name"]
                + "-"
                + req["metadata"]["uid"]
                + ".jid"
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
    request_data_string = request.data.decode("utf-8")
    # print(request_data_string)
    req = json.loads(request_data_string)
    if req is None or not isinstance(req, dict):
        # print("Invalid logs request body is: ", req)
        logging.error("Invalid request data")
        return "Invalid request data for getting logs", 400

    resp = "NOT IMPLEMENTED"

    return json.dumps(resp), 200


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
