#!/bin/bash
# k3s-test-setup.sh — Set up an ephemeral interLink + HTCondor e2e environment.
#
# Usage: bash scripts/k3s-test-setup.sh
#
# What it does:
#   1. Installs K3s (the local Kubernetes cluster).
#   2. Builds the htcondor-sidecar Docker image from the repo source.
#   3. Downloads pre-built interLink API and Virtual Kubelet binaries.
#   4. Generates config files for all components.
#   5. Starts the htcondor-sidecar Docker container (HTCondor mini + plugin).
#   6. Runs a condor_submit smoke test inside the container.
#   7. Starts the interLink API binary as a host process.
#   8. Creates the Virtual Kubelet service account and RBAC.
#   9. Starts the Virtual Kubelet binary as a host process.
#  10. Waits for the virtual-kubelet node to become Ready and approves CSRs.
#
# Requirements: sudo access (for K3s), Docker, bash.

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== Setting up interLink + HTCondor e2e environment ==="
echo "Project root: ${PROJECT_ROOT}"

# ---------------------------------------------------------------------------
# Test directory
# ---------------------------------------------------------------------------
if [[ -n "${TEST_DIR:-}" ]]; then
  echo "Using existing TEST_DIR: ${TEST_DIR}"
else
  TEST_DIR=$(mktemp -d /tmp/interlink-test-XXXXXX)
  echo "Created TEST_DIR: ${TEST_DIR}"
fi

STATE_FILE="/tmp/interlink-test-dir.txt"
echo "${TEST_DIR}" > "${STATE_FILE}"
echo "State file: ${STATE_FILE}"

# ---------------------------------------------------------------------------
# Install K3s
# ---------------------------------------------------------------------------
echo ""
echo "=== Installing K3s ==="
K3S_VERSION="${K3S_VERSION:-v1.31.4+k3s1}"
echo "K3s version: ${K3S_VERSION}"

curl -sfL https://get.k3s.io | \
  sudo env INSTALL_K3S_VERSION="${K3S_VERSION}" sh -s - \
    --disable=traefik \
    --egress-selector-mode disabled \
  2>&1 | tee "${TEST_DIR}/k3s-install.log"

sudo chmod 644 /etc/rancher/k3s/k3s.yaml
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

echo "Waiting for K3s node to appear..."
node_appeared=0
for i in $(seq 1 30); do
  if kubectl get nodes 2>/dev/null | grep -q '.'; then
    node_appeared=1
    break
  fi
  echo "  Waiting for node... ($i/30)"
  sleep 5
done

if [ "${node_appeared}" -ne 1 ]; then
  echo "ERROR: No K3s node appeared within 150 s"
  cat "${TEST_DIR}/k3s-install.log"
  exit 1
fi

if ! kubectl wait --for=condition=Ready node --all --timeout=150s; then
  echo "ERROR: K3s did not become ready in time"
  kubectl get nodes || true
  cat "${TEST_DIR}/k3s-install.log"
  exit 1
fi

echo "✓ K3s is ready"
kubectl get nodes

# ---------------------------------------------------------------------------
# Initialise the vk-test-set submodule
# ---------------------------------------------------------------------------
echo ""
echo "=== Initialising vk-test-set submodule ==="
cd "${PROJECT_ROOT}"
git submodule update --init test/vk-test-set
echo "✓ vk-test-set submodule initialised"

# ---------------------------------------------------------------------------
# Build htcondor-sidecar Docker image
# ---------------------------------------------------------------------------
echo ""
echo "=== Building htcondor-sidecar Docker image ==="
docker build -f "${PROJECT_ROOT}/docker/Dockerfile" \
  -t htcondor-sidecar:local "${PROJECT_ROOT}" \
  2>&1 | tee "${TEST_DIR}/build-plugin.log"
echo "✓ htcondor-sidecar image built"

# ---------------------------------------------------------------------------
# Download interLink binaries
# ---------------------------------------------------------------------------
echo ""
echo "=== Downloading interLink binaries ==="
# 0.6.1-pre6 is the latest release that is compatible with this plugin
# (v0.6.1+ API).  Override INTERLINK_VERSION to pin a different release.
INTERLINK_VERSION="${INTERLINK_VERSION:-0.6.1-pre6}"
RELEASE_BASE="https://github.com/interlink-hq/interLink/releases/download/${INTERLINK_VERSION}"

curl -fsSL "${RELEASE_BASE}/interlink_Linux_x86_64" \
  -o "${TEST_DIR}/interlink"
chmod +x "${TEST_DIR}/interlink"

curl -fsSL "${RELEASE_BASE}/virtual-kubelet_Linux_x86_64" \
  -o "${TEST_DIR}/virtual-kubelet"
chmod +x "${TEST_DIR}/virtual-kubelet"

echo "✓ interLink binaries downloaded (version ${INTERLINK_VERSION})"

# ---------------------------------------------------------------------------
# Start htcondor-sidecar container
# ---------------------------------------------------------------------------
echo ""
echo "=== Starting htcondor-sidecar container ==="
docker run -d --name htcondor-sidecar \
  --privileged \
  -p 8000:8000 \
  htcondor-sidecar:local

sleep 5
if ! docker ps --filter "name=htcondor-sidecar" --filter "status=running" \
    | grep -q htcondor-sidecar; then
  echo "ERROR: htcondor-sidecar container failed to start"
  docker logs htcondor-sidecar 2>&1 || true
  exit 1
fi
echo "✓ htcondor-sidecar container started"

# Resolve the container's IP on the Docker bridge so the interLink API can
# reach the sidecar without triggering the SSRF guard (which blocks localhost).
SIDECAR_IP=$(docker inspect \
  -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
  htcondor-sidecar)
echo "  Sidecar IP: ${SIDECAR_IP}"

# ---------------------------------------------------------------------------
# Generate config files (after container start so we can use SIDECAR_IP)
# ---------------------------------------------------------------------------
mkdir -p "${TEST_DIR}/interlink-data"

# interLink API config (middleware between VK and the sidecar plugin)
cat > "${TEST_DIR}/interlink-config.yaml" <<EOF
InterlinkAddress: "http://0.0.0.0"
InterlinkPort: "3000"
SidecarURL: "http://${SIDECAR_IP}"
SidecarPort: "8000"
VerboseLogging: true
ErrorsOnlyLogging: false
DataRootFolder: "${TEST_DIR}/interlink-data/"
EOF

# Virtual Kubelet config
POD_IP=$(hostname -I | awk '{print $1}')
cat > "${TEST_DIR}/vk-config.yaml" <<EOF
InterlinkURL: "http://${POD_IP}"
InterlinkPort: "3000"
VerboseLogging: true
ErrorsOnlyLogging: false
ServiceAccount: "virtual-kubelet"
Namespace: "default"
VKTokenFile: ""
PodIP: "${POD_IP}"
DisableCSR: false
HTTP:
  Insecure: true
KubeletHTTP:
  Insecure: true
Resources:
  CPU: "100"
  Memory: "128Gi"
  Pods: "100"
EOF

echo "✓ Config files generated"

# Wait for HTCondor daemons to initialise inside the container
echo "Waiting for HTCondor daemons to initialise..."
condor_ready=0
for i in $(seq 1 30); do
  if docker exec htcondor-sidecar condor_status 2>/dev/null | grep -q "slot"; then
    condor_ready=1
    break
  fi
  echo "  Waiting for condor_status... ($i/30)"
  docker exec htcondor-sidecar condor_status 2>&1 || true
  sleep 5
done

if [ "${condor_ready}" -ne 1 ]; then
  echo "ERROR: condor_status not responding after 150 s — HTCondor failed to start"
  echo ""
  echo "=== condor_status -debug output ==="
  docker exec htcondor-sidecar condor_status -debug 2>&1 || true
  echo ""
  echo "=== htcondor-sidecar container stdout/stderr ==="
  docker logs htcondor-sidecar 2>&1 || true
  echo ""
  echo "=== HTCondor MasterLog ==="
  docker exec htcondor-sidecar cat /var/log/condor/MasterLog 2>/dev/null || true
  echo ""
  echo "=== HTCondor StartLog ==="
  docker exec htcondor-sidecar cat /var/log/condor/StartLog 2>/dev/null || true
  echo ""
  echo "=== HTCondor CollectorLog ==="
  docker exec htcondor-sidecar cat /var/log/condor/CollectorLog 2>/dev/null || true
  echo ""
  echo "=== HTCondor NegotiatorLog ==="
  docker exec htcondor-sidecar cat /var/log/condor/NegotiatorLog 2>/dev/null || true
  echo ""
  echo "=== HTCondor SchedLog ==="
  docker exec htcondor-sidecar cat /var/log/condor/SchedLog 2>/dev/null || true
  exit 1
fi
echo "✓ HTCondor daemons are ready"

# Wait for the plugin Flask server to respond on port 8000
echo "Waiting for plugin HTTP server to respond..."
plugin_ready=0
for i in $(seq 1 20); do
  if curl -sf -X GET http://localhost:8000/status \
      -H "Content-Type: application/json" \
      -d '[]' >/dev/null 2>&1; then
    plugin_ready=1
    break
  fi
  echo "  Waiting for plugin... ($i/20)"
  sleep 3
done

if [ "${plugin_ready}" -ne 1 ]; then
  echo "ERROR: plugin HTTP server did not respond in time"
  echo ""
  echo "=== htcondor-sidecar container logs ==="
  docker logs htcondor-sidecar 2>&1 || true
  exit 1
fi
echo "✓ Plugin HTTP server is ready"

# ---------------------------------------------------------------------------
# HTCondor + Apptainer smoke test
# ---------------------------------------------------------------------------
echo ""
echo "=== Running HTCondor + Apptainer smoke test ==="

# Write a minimal submit description that uses Apptainer to run alpine.
cat > "${TEST_DIR}/smoke-test.jdl" <<'JDLEOF'
Executable = /tmp/smoke-test.sh
Log        = /tmp/smoke-test.$(Cluster).$(Process).log
Output     = /tmp/smoke-test.$(Cluster).$(Process).out
Error      = /tmp/smoke-test.$(Cluster).$(Process).err
should_transfer_files = YES
when_to_transfer_output = ON_EXIT_OR_EVICT
Queue 1
JDLEOF

cat > "${TEST_DIR}/smoke-test.sh" <<'SHEOF'
#!/bin/bash
apptainer exec docker://alpine:3.20 echo "Apptainer smoke test passed"
SHEOF
chmod +x "${TEST_DIR}/smoke-test.sh"

docker cp "${TEST_DIR}/smoke-test.jdl" htcondor-sidecar:/tmp/smoke-test.jdl
docker cp "${TEST_DIR}/smoke-test.sh"  htcondor-sidecar:/tmp/smoke-test.sh

set +e
SUBMIT_OUT=$(docker exec htcondor-sidecar condor_submit /tmp/smoke-test.jdl 2>&1)
SUBMIT_STATUS=$?
set -e

if [ "${SUBMIT_STATUS}" -ne 0 ]; then
  echo "WARNING: HTCondor smoke test submission failed (HTCondor may still be starting):"
  echo "${SUBMIT_OUT}"
else
  SMOKE_JID=$(printf '%s\n' "${SUBMIT_OUT}" \
    | awk 'match($0, /cluster ([0-9]+)/, m) { print m[1]; exit }')
  if [[ "${SMOKE_JID}" =~ ^[0-9]+$ ]]; then
    echo "  Smoke test job submitted (cluster ID ${SMOKE_JID})"
    echo "  Waiting up to 3 min for smoke test job to finish..."
    for i in $(seq 1 18); do
      JOB_STATE=$(docker exec htcondor-sidecar \
        condor_q "${SMOKE_JID}" -format "%d\n" JobStatus 2>/dev/null || true)
      if [ -z "${JOB_STATE}" ]; then
        echo "  ✓ Smoke test job ${SMOKE_JID} completed"
        break
      fi
      echo "    Job status: ${JOB_STATE} (${i}/18)"
      sleep 10
    done
    docker exec htcondor-sidecar \
      cat "/tmp/smoke-test.${SMOKE_JID}.0.out" 2>/dev/null || true
    docker exec htcondor-sidecar \
      condor_history "${SMOKE_JID}" 2>/dev/null || true
  else
    echo "WARNING: Could not parse smoke test job ID from: ${SUBMIT_OUT}"
  fi
fi

# ---------------------------------------------------------------------------
# Diagnostic smoke tests: test apptainer as root vs condor user
# ---------------------------------------------------------------------------
echo ""
echo "=== Diagnostic: apptainer as root (docker exec) ==="
docker exec htcondor-sidecar \
  bash -c 'echo "TEST_DIAG=root_level" > /tmp/diag.env && singularity exec --env-file /tmp/diag.env docker://alpine:3.20 sh -c "echo apptainer-as-root-ok; echo TEST_DIAG=\$TEST_DIAG"' \
  2>&1 || echo "WARNING: apptainer as root FAILED (exit $?)"

echo ""
echo "=== Diagnostic: apptainer as condor user (via docker exec --user) ==="
docker exec --user condor htcondor-sidecar \
  bash -c 'echo "TEST_DIAG=condor_level" > /tmp/diag-condor.env && singularity exec --env-file /tmp/diag-condor.env docker://alpine:3.20 sh -c "echo apptainer-as-condor-ok; echo TEST_DIAG=\$TEST_DIAG"' \
  2>&1 || echo "WARNING: apptainer as condor user FAILED (exit $?)"

# ---------------------------------------------------------------------------
# Comprehensive condor smoke test: full plugin-style job with InitialDir,
# transfer_input_files, transfer_output_files, runCtn/waitCtns helpers.
# ---------------------------------------------------------------------------
echo ""
echo "=== Comprehensive condor+apptainer smoke test (plugin job structure) ==="

# Create a job directory inside the container via docker exec
docker exec htcondor-sidecar bash -c '
  mkdir -p /tmp/plugin-smoke
  chmod 1777 /tmp/plugin-smoke
  echo "PLUGIN_SMOKE_VAR=plugin_level_ok" > /tmp/plugin-smoke/plugin-smoke_env.env
'

# Write the job script (mimics the plugin runCtn/waitCtns pattern)
cat > "${TEST_DIR}/plugin-smoke.sh" << 'PLUGIN_SH_EOF'
#!/bin/bash
export _IL_POD_NAME=plugin-smoke
export _IL_POD_UID=smoke-test-uid

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

highestExitCode=0
pidCtns=""
export workingPath=$(pwd)

runCtn container singularity exec --env-file plugin-smoke_env.env docker://alpine:3.20 sh -c 'echo plugin-condor-apptainer-ok; echo PLUGIN_SMOKE_VAR=$PLUGIN_SMOKE_VAR'
waitCtns
endScript
PLUGIN_SH_EOF
chmod +x "${TEST_DIR}/plugin-smoke.sh"

cat > "${TEST_DIR}/plugin-smoke.jdl" << 'PLUGIN_JDL_EOF'
Executable = /tmp/plugin-smoke/plugin-smoke.sh
InitialDir = /tmp/plugin-smoke
Log        = /tmp/plugin-smoke/plugin-smoke.$(Cluster).$(Process).log
Output     = /tmp/plugin-smoke/plugin-smoke.$(Cluster).$(Process).out
Error      = /tmp/plugin-smoke/plugin-smoke.$(Cluster).$(Process).err
universe   = vanilla
should_transfer_files = YES
when_to_transfer_output = ON_EXIT_OR_EVICT
transfer_input_files = /tmp/plugin-smoke/plugin-smoke_env.env
transfer_output_files = plugin-smoke-smoke-test-uid-container.out
Queue 1
PLUGIN_JDL_EOF

docker cp "${TEST_DIR}/plugin-smoke.sh"  htcondor-sidecar:/tmp/plugin-smoke/plugin-smoke.sh
docker cp "${TEST_DIR}/plugin-smoke.jdl" htcondor-sidecar:/tmp/plugin-smoke/plugin-smoke.jdl

set +e
PLUGIN_SUBMIT_OUT=$(docker exec htcondor-sidecar condor_submit /tmp/plugin-smoke/plugin-smoke.jdl 2>&1)
PLUGIN_SUBMIT_STATUS=$?
set -e

if [ "${PLUGIN_SUBMIT_STATUS}" -ne 0 ]; then
  echo "WARNING: Plugin-style smoke test submission failed:"
  echo "${PLUGIN_SUBMIT_OUT}"
else
  PLUGIN_JID=$(printf '%s\n' "${PLUGIN_SUBMIT_OUT}" \
    | awk 'match($0, /cluster ([0-9]+)/, m) { print m[1]; exit }')
  if [[ "${PLUGIN_JID}" =~ ^[0-9]+$ ]]; then
    echo "  Plugin smoke test job submitted (cluster ID ${PLUGIN_JID})"
    echo "  Waiting up to 3 min for plugin smoke test job to finish..."
    for i in $(seq 1 18); do
      PLUGIN_JOB_STATE=$(docker exec htcondor-sidecar \
        condor_q "${PLUGIN_JID}" -format "%d\n" JobStatus 2>/dev/null || true)
      if [ -z "${PLUGIN_JOB_STATE}" ]; then
        echo "  ✓ Plugin smoke test job ${PLUGIN_JID} completed"
        break
      fi
      echo "    Job status: ${PLUGIN_JOB_STATE} (${i}/18)"
      sleep 10
    done
    echo "--- Plugin smoke test condor output ---"
    docker exec htcondor-sidecar \
      cat "/tmp/plugin-smoke/plugin-smoke.${PLUGIN_JID}.0.out" 2>/dev/null || true
    echo "--- Plugin smoke test job output (.out file from execute sandbox) ---"
    docker exec htcondor-sidecar \
      cat "/tmp/plugin-smoke/plugin-smoke-smoke-test-uid-container.out" 2>/dev/null \
      || echo "(output file not found - may indicate transfer_output_files failure)"
    echo "--- Plugin smoke test condor history ---"
    docker exec htcondor-sidecar \
      condor_history "${PLUGIN_JID}" -format "ExitCode=%d\n" ExitCode 2>/dev/null || true
  else
    echo "WARNING: Could not parse plugin smoke test job ID from: ${PLUGIN_SUBMIT_OUT}"
  fi
fi

# Stream plugin container logs in the background
docker logs -f htcondor-sidecar > "${TEST_DIR}/htcondor-sidecar.log" 2>&1 &
echo $! > "${TEST_DIR}/sidecar-log.pid"
echo "  Sidecar logs streaming to: ${TEST_DIR}/htcondor-sidecar.log"

# ---------------------------------------------------------------------------
# Start interLink API binary (host process)
# ---------------------------------------------------------------------------
echo ""
echo "=== Starting interLink API ==="
INTERLINKCONFIGPATH="${TEST_DIR}/interlink-config.yaml" \
  nohup "${TEST_DIR}/interlink" \
  > "${TEST_DIR}/interlink-api.log" 2>&1 &
INTERLINK_PID=$!
echo "${INTERLINK_PID}" > "${TEST_DIR}/interlink-api.pid"
echo "interLink API started (PID ${INTERLINK_PID})"

echo "Waiting for interLink API to respond..."
interlink_ready=0
for i in $(seq 1 20); do
  if curl -sf -X POST http://localhost:3000/pinglink >/dev/null 2>&1; then
    interlink_ready=1
    break
  fi
  if ! kill -0 "${INTERLINK_PID}" 2>/dev/null; then
    echo "ERROR: interLink API process died"
    cat "${TEST_DIR}/interlink-api.log" || true
    exit 1
  fi
  echo "  Waiting for interLink API... ($i/20)"
  sleep 3
done

if [ "${interlink_ready}" -ne 1 ]; then
  echo "ERROR: interLink API did not become ready in time"
  cat "${TEST_DIR}/interlink-api.log" || true
  exit 1
fi
echo "✓ interLink API is ready"

# ---------------------------------------------------------------------------
# Create Virtual Kubelet RBAC
# ---------------------------------------------------------------------------
echo ""
echo "=== Creating Virtual Kubelet RBAC ==="
kubectl apply -f - <<'YAML'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: virtual-kubelet
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: virtual-kubelet
rules:
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["update", "create", "get", "list", "watch", "patch"]
- apiGroups: [""]
  resources: ["configmaps", "secrets", "services", "serviceaccounts", "namespaces"]
  verbs: ["get", "list", "watch"]
- apiGroups: [""]
  resources: ["serviceaccounts/token"]
  verbs: ["create", "get", "list"]
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["delete", "get", "list", "watch", "patch"]
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["create", "get"]
- apiGroups: [""]
  resources: ["nodes/status"]
  verbs: ["update", "patch"]
- apiGroups: [""]
  resources: ["pods/status"]
  verbs: ["update", "patch"]
- apiGroups: [""]
  resources: ["events"]
  verbs: ["create", "patch"]
- apiGroups: ["certificates.k8s.io"]
  resources: ["certificatesigningrequests"]
  verbs: ["create", "get", "list", "watch", "delete"]
- apiGroups: ["certificates.k8s.io"]
  resources: ["certificatesigningrequests/approval"]
  verbs: ["update", "patch"]
- apiGroups: ["certificates.k8s.io"]
  resources: ["signers"]
  resourceNames: ["kubernetes.io/kubelet-serving"]
  verbs: ["approve"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: virtual-kubelet
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: virtual-kubelet
subjects:
- kind: ServiceAccount
  name: virtual-kubelet
  namespace: default
YAML
echo "✓ Service account and RBAC created"

# ---------------------------------------------------------------------------
# Build VK kubeconfig using a service account token
# ---------------------------------------------------------------------------
echo "Creating VK kubeconfig..."
VK_TOKEN=$(kubectl create token virtual-kubelet -n default --duration=24h)
K8S_SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
K8S_CA_DATA=$(kubectl config view --minify --raw \
  -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')

if [ -z "${K8S_CA_DATA}" ]; then
  K8S_CA_FILE=$(kubectl config view --minify --raw \
    -o jsonpath='{.clusters[0].cluster.certificate-authority}')
  if [ -n "${K8S_CA_FILE}" ] && [ -f "${K8S_CA_FILE}" ]; then
    K8S_CA_DATA=$(base64 -w 0 < "${K8S_CA_FILE}" 2>/dev/null || base64 < "${K8S_CA_FILE}")
  else
    echo "ERROR: Could not find Kubernetes CA certificate"
    exit 1
  fi
fi

cat > "${TEST_DIR}/vk-kubeconfig.yaml" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: default-cluster
  cluster:
    server: ${K8S_SERVER}
    certificate-authority-data: ${K8S_CA_DATA}
contexts:
- name: default-context
  context:
    cluster: default-cluster
    user: virtual-kubelet
    namespace: default
current-context: default-context
users:
- name: virtual-kubelet
  user:
    token: ${VK_TOKEN}
EOF
chmod 600 "${TEST_DIR}/vk-kubeconfig.yaml"
echo "✓ VK kubeconfig created"

# ---------------------------------------------------------------------------
# Start Virtual Kubelet binary
# ---------------------------------------------------------------------------
echo ""
echo "=== Starting Virtual Kubelet ==="
NODENAME=virtual-kubelet \
  KUBELET_PORT=10251 \
  KUBELET_URL=0.0.0.0 \
  POD_IP="${POD_IP}" \
  CONFIGPATH="${TEST_DIR}/vk-config.yaml" \
  KUBECONFIG="${TEST_DIR}/vk-kubeconfig.yaml" \
  nohup "${TEST_DIR}/virtual-kubelet" \
  > "${TEST_DIR}/vk.log" 2>&1 &
VK_PID=$!
echo "${VK_PID}" > "${TEST_DIR}/vk.pid"
echo "Virtual Kubelet started (PID ${VK_PID})"

# ---------------------------------------------------------------------------
# Wait for virtual-kubelet node to register
# ---------------------------------------------------------------------------
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

echo "Waiting for virtual-kubelet node to register..."
for i in $(seq 1 60); do
  if kubectl get node virtual-kubelet &>/dev/null; then
    echo "✓ virtual-kubelet node registered"
    break
  fi
  if ! kill -0 "${VK_PID}" 2>/dev/null; then
    echo "ERROR: Virtual Kubelet process died"
    tail -50 "${TEST_DIR}/vk.log" || true
    exit 1
  fi
  echo "  Waiting for VK node... ($i/60)"
  sleep 5
done

kubectl get node virtual-kubelet 2>/dev/null || {
  echo "ERROR: virtual-kubelet node did not register in time"
  tail -100 "${TEST_DIR}/vk.log" || true
  exit 1
}

echo "Waiting for virtual-kubelet node to become Ready..."
if ! kubectl wait --for=condition=Ready node/virtual-kubelet --timeout=300s; then
  echo "ERROR: virtual-kubelet node did not become Ready in time"
  kubectl describe node virtual-kubelet || true
  tail -100 "${TEST_DIR}/vk.log" || true
  exit 1
fi
echo "✓ virtual-kubelet node is Ready"

# ---------------------------------------------------------------------------
# Approve kubelet-serving CSRs (required for kubectl logs)
# ---------------------------------------------------------------------------
echo ""
echo "=== Checking and approving CSRs ==="

echo "Waiting for CSRs to appear..."
for i in $(seq 1 30); do
  if kubectl get csr 2>/dev/null | awk 'NR>1' | grep -q .; then
    break
  fi
  echo "  No CSRs yet... ($i/30)"
  sleep 2
done

PENDING_CSRS=$(kubectl get csr 2>/dev/null | awk 'NR>1 && /Pending/ {print $1}')
if [ -n "${PENDING_CSRS}" ]; then
  echo "  Approving pending CSRs: ${PENDING_CSRS}"
  echo "${PENDING_CSRS}" | xargs kubectl certificate approve
else
  echo "  No pending CSRs at this time"
fi

csr_issued=0
for i in $(seq 1 20); do
  NEW_PENDING=$(kubectl get csr 2>/dev/null | awk 'NR>1 && /Pending/ {print $1}')
  if [ -n "${NEW_PENDING}" ]; then
    echo "  Approving newly pending CSRs: ${NEW_PENDING}"
    echo "${NEW_PENDING}" | xargs kubectl certificate approve 2>/dev/null || true
  fi
  if kubectl get csr 2>/dev/null | grep -E "Approved,Issued" \
      | grep -qi "virtual-kubelet\|node:virtual-kubelet"; then
    csr_issued=1
    break
  fi
  echo "  Waiting for CSR issuance... ($i/20)"
  sleep 3
done

if [ "${csr_issued}" -ne 1 ]; then
  echo "WARNING: CSR issuance not confirmed — kubectl logs may not work"
  kubectl get csr 2>/dev/null || true
else
  echo "✓ CSRs approved and issued"
fi

echo ""
echo "=== interLink + HTCondor e2e environment is ready ==="
echo "  KUBECONFIG:     /etc/rancher/k3s/k3s.yaml"
echo "  Test dir:       ${TEST_DIR}"
echo "  VK PID:         ${VK_PID}"
echo "  API PID:        ${INTERLINK_PID}"
echo "  VK logs:        ${TEST_DIR}/vk.log"
echo "  API logs:       ${TEST_DIR}/interlink-api.log"
echo "  Sidecar logs:   ${TEST_DIR}/htcondor-sidecar.log"
