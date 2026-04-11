#!/bin/bash
# k3s-test-run.sh — Run the vk-test-set pytest suite against the live K3s cluster.
#
# Usage: bash scripts/k3s-test-run.sh
#
# This uses the interlink-hq/vk-test-set pytest suite (test/vk-test-set submodule),
# matching the approach in interlink-hq/interLink#514.
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
# Initialise the vk-test-set submodule (in case checkout didn't do it)
# ---------------------------------------------------------------------------
if [ ! -f "${PROJECT_ROOT}/test/vk-test-set/setup.py" ]; then
  echo "Initialising test/vk-test-set submodule..."
  cd "${PROJECT_ROOT}"
  git submodule update --init test/vk-test-set
fi

cd "${PROJECT_ROOT}/test/vk-test-set"

# ---------------------------------------------------------------------------
# Write vktest_config.yaml (HTCondor-specific)
# ---------------------------------------------------------------------------
echo "Creating test configuration..."
cat > vktest_config.yaml <<EOF
target_nodes:
  - virtual-kubelet

required_namespaces:
  - default
  - kube-system

timeout_multiplier: 10.
values:
  namespace: default

  annotations: {}

  tolerations:
    - key: virtual-node.interlink/no-schedule
      operator: Exists
      effect: NoSchedule
EOF

# ---------------------------------------------------------------------------
# Set up Python venv and install vk-test-set
# ---------------------------------------------------------------------------
echo "Setting up Python environment..."
python3 -m venv .venv
# shellcheck source=/dev/null
source .venv/bin/activate
pip3 install -e ./ || {
  echo "ERROR: Failed to install vk-test-set"
  exit 1
}
echo "✓ vk-test-set installed"

# ---------------------------------------------------------------------------
# Run pytest
# ---------------------------------------------------------------------------
echo ""
echo "Running integration tests..."
echo "========================================="

export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
pytest -v -k "not rclone and not limits and not stress and not multi-init and not fail" \
  2>&1 | tee "${TEST_DIR}/test-results.log"
TEST_EXIT_CODE=${PIPESTATUS[0]}

echo "========================================="
echo ""

if [ "${TEST_EXIT_CODE}" -eq 0 ]; then
  echo "✓ All tests passed!"
else
  echo "✗ Some tests failed (exit code: ${TEST_EXIT_CODE})"
  echo ""
  echo "Check logs for details:"
  echo "  - Test results:   ${TEST_DIR}/test-results.log"
  echo "  - VK:             ${TEST_DIR}/vk.log"
  echo "  - interLink API:  ${TEST_DIR}/interlink-api.log"
  echo "  - Sidecar:        ${TEST_DIR}/htcondor-sidecar.log"
fi

exit "${TEST_EXIT_CODE}"
