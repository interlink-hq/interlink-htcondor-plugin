#!/bin/bash
# k3s-test-run.sh — Run the HTCondor plugin e2e tests against the live K3s cluster.
#
# Usage: bash scripts/k3s-test-run.sh
#
# Expects k3s-test-setup.sh to have run successfully first.
# Reads TEST_DIR from /tmp/interlink-test-dir.txt.

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

# ---------------------------------------------------------------------------
# Locate test directory
# ---------------------------------------------------------------------------
if [[ -n "${TEST_DIR:-}" ]]; then
  echo "Using TEST_DIR from environment: ${TEST_DIR}"
elif [ -f /tmp/interlink-test-dir.txt ]; then
  TEST_DIR=$(cat /tmp/interlink-test-dir.txt)
  echo "Using TEST_DIR from state file: ${TEST_DIR}"
else
  echo "ERROR: TEST_DIR not set and /tmp/interlink-test-dir.txt not found"
  exit 1
fi

echo "=== Running interLink + HTCondor e2e tests ==="
echo "Project root: ${PROJECT_ROOT}"
echo "Test dir:     ${TEST_DIR}"

# ---------------------------------------------------------------------------
# Pre-flight: verify cluster and VK node are healthy
# ---------------------------------------------------------------------------
echo ""
echo "Checking cluster status..."
kubectl get nodes
kubectl get pods -A

echo "Waiting for virtual-kubelet node..."
for i in $(seq 1 30); do
  if kubectl get node virtual-kubelet &>/dev/null; then
    echo "✓ virtual-kubelet node found"
    break
  fi
  if [ "${i}" -eq 30 ]; then
    echo "ERROR: virtual-kubelet node not found"
    kubectl get nodes || true
    VK_PID_FILE="${TEST_DIR}/vk.pid"
    if [ -f "${VK_PID_FILE}" ]; then
      VK_PID=$(cat "${VK_PID_FILE}")
      echo "VK PID ${VK_PID} alive: $(kill -0 "${VK_PID}" 2>/dev/null && echo yes || echo no)"
      tail -50 "${TEST_DIR}/vk.log" || true
    fi
    exit 1
  fi
  echo "  Waiting... ($i/30)"
  sleep 5
done

echo "Waiting for virtual-kubelet node to be Ready..."
if ! kubectl wait --for=condition=Ready node/virtual-kubelet --timeout=120s; then
  echo "ERROR: virtual-kubelet node is not Ready"
  kubectl describe node virtual-kubelet || true
  tail -100 "${TEST_DIR}/vk.log" || true
  exit 1
fi
echo "✓ virtual-kubelet node is Ready"

# Approve any pending CSRs
kubectl get csr -o name | xargs -r kubectl certificate approve 2>/dev/null || true

# ---------------------------------------------------------------------------
# Apply test pod
# ---------------------------------------------------------------------------
echo ""
echo "=== Applying e2e test pod ==="

# Clean up any previous run
kubectl delete pod interlink-htcondor-test --ignore-not-found=true --wait=false

kubectl apply -f "${PROJECT_ROOT}/tests/e2e_test_pod.yaml"
echo "✓ Test pod submitted"

# ---------------------------------------------------------------------------
# Wait for test pod to complete
# ---------------------------------------------------------------------------
echo ""
echo "=== Waiting for test pod to complete ==="
POD_TIMEOUT="${POD_TIMEOUT:-600}"

echo "Waiting up to ${POD_TIMEOUT}s for the test pod to finish..."
pod_done=0
elapsed=0
while [ "${elapsed}" -lt "${POD_TIMEOUT}" ]; do
  PHASE=$(kubectl get pod interlink-htcondor-test \
    -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")

  case "${PHASE}" in
    Succeeded)
      echo "✓ Pod completed successfully (phase: ${PHASE})"
      pod_done=1
      break
      ;;
    Failed)
      echo "ERROR: Pod failed (phase: ${PHASE})"
      kubectl describe pod interlink-htcondor-test || true
      pod_done=2
      break
      ;;
    *)
      echo "  Pod phase: ${PHASE} (${elapsed}s / ${POD_TIMEOUT}s elapsed)"
      sleep 10
      elapsed=$((elapsed + 10))
      ;;
  esac
done

# ---------------------------------------------------------------------------
# Collect diagnostics
# ---------------------------------------------------------------------------
echo ""
echo "=== Pod status ==="
kubectl get pod interlink-htcondor-test -o wide || true
kubectl describe pod interlink-htcondor-test 2>/dev/null \
  | tee "${TEST_DIR}/pod-describe.txt" || true

echo ""
echo "=== Logs ==="
kubectl logs interlink-htcondor-test 2>/dev/null \
  | tee "${TEST_DIR}/pod-logs.txt" || true

echo ""
echo "=== interLink API logs (last 50 lines) ==="
tail -50 "${TEST_DIR}/interlink-api.log" 2>/dev/null || true

echo ""
echo "=== htcondor-sidecar container logs (last 50 lines) ==="
docker logs htcondor-sidecar --tail=50 2>/dev/null || true

# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------
echo ""
if [ "${pod_done}" -eq 1 ]; then
  echo "✓ e2e test PASSED"
  exit 0
elif [ "${pod_done}" -eq 2 ]; then
  echo "✗ e2e test FAILED — pod entered Failed phase"
  exit 1
else
  echo "✗ e2e test TIMED OUT — pod did not complete within ${POD_TIMEOUT}s"
  kubectl get pod interlink-htcondor-test || true
  exit 1
fi
